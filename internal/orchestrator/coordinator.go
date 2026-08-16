package orchestrator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/loadgen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AssignmentOptions defines the one run coordinated by this orchestrator.
type AssignmentOptions struct {
	Context  context.Context
	Config   config.Config
	RunID    string
	LeadTime time.Duration
}

type assignmentCoordinator struct {
	server   *Server
	ctx      context.Context
	config   config.Config
	runID    string
	leadTime time.Duration
	now      func() time.Time
	changed  chan struct{}
	started  chan struct{}
	window   RunWindow

	revision    uint64
	startsAt    time.Time
	deadline    time.Time
	activatesAt time.Time

	failureMu       sync.Mutex
	pendingFailures map[string]workerFailure
	delivered       map[string][]*loadtestv1.LoadAssignment
	recovery        map[string]workerRecovery
	restorations    []capacityRestoration
}

type workerFailure struct {
	workerID string
	reason   string
	detected time.Time
}

type workerRecovery struct {
	detected time.Time
	load     *loadtestv1.LoadSlice
}

type capacityRestoration struct {
	activatesAt time.Time
	providers   map[string]struct{}
	recovered   map[string]*loadtestv1.LoadSlice
	invalidated bool
}

func newAssignmentCoordinator(
	server *Server,
	options AssignmentOptions,
	now func() time.Time,
) (*assignmentCoordinator, error) {
	if options.Context == nil {
		return nil, errors.New("assignment context is required")
	}
	if strings.TrimSpace(options.RunID) == "" {
		return nil, errors.New("assignment run ID is required")
	}
	if options.LeadTime <= 0 {
		return nil, errors.New("assignment lead time must be greater than zero")
	}
	if options.Config.Fleet.MinWorkers <= 0 {
		return nil, errors.New("minimum workers must be greater than zero")
	}
	if options.Config.Load.Duration <= 0 {
		return nil, errors.New("load duration must be greater than zero")
	}
	return &assignmentCoordinator{
		server:          server,
		ctx:             options.Context,
		config:          options.Config,
		runID:           strings.TrimSpace(options.RunID),
		leadTime:        options.LeadTime,
		now:             now,
		changed:         make(chan struct{}, 1),
		started:         make(chan struct{}),
		pendingFailures: make(map[string]workerFailure),
		delivered:       make(map[string][]*loadtestv1.LoadAssignment),
		recovery:        make(map[string]workerRecovery),
	}, nil
}

// membershipChanged coalesces bursts because each reconciliation reads a fresh
// snapshot containing every worker registered before that point.
func (c *assignmentCoordinator) membershipChanged() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// workerFailed preserves failure details until the coordinator can reconcile
// them with its ordered assignment history.
func (c *assignmentCoordinator) workerFailed(workerID, reason string, detected time.Time) {
	c.failureMu.Lock()
	if _, exists := c.pendingFailures[workerID]; !exists {
		c.pendingFailures[workerID] = workerFailure{
			workerID: workerID,
			reason:   reason,
			detected: detected,
		}
	}
	c.failureMu.Unlock()
	c.membershipChanged()
}

func (c *assignmentCoordinator) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.changed:
			c.reconcile()
		}
	}
}

// reconcile creates one complete revision from the latest worker snapshot. It
// runs only on the coordinator goroutine, which keeps run timing and revisions ordered.
func (c *assignmentCoordinator) reconcile() {
	workers := c.server.activeSessions()
	c.processFailures()
	if c.revision == 0 && len(workers) < c.config.Fleet.MinWorkers {
		return
	}
	now := c.now()
	if c.revision == 0 {
		c.startsAt = now.Add(c.leadTime)
		c.deadline = c.startsAt.Add(c.config.Load.Duration)
		c.activatesAt = c.startsAt
		c.window = RunWindow{StartsAt: c.startsAt, Deadline: c.deadline}
		close(c.started)
	} else if !now.Before(c.deadline) {
		return
	} else {
		activationBase := now
		if c.activatesAt.After(activationBase) {
			activationBase = c.activatesAt
		}
		activatesAt := activationBase.Add(c.leadTime)
		if !activatesAt.Before(c.deadline) {
			return
		}
		c.activatesAt = activatesAt
	}

	workerIDs := make([]string, len(workers))
	for index, worker := range workers {
		workerIDs[index] = worker.state.ID
	}
	loadSlices := make(map[string]*loadtestv1.LoadSlice, len(workerIDs))
	if len(workerIDs) > 0 {
		var err error
		loadSlices, err = splitLoad(c.config.Load, workerIDs)
		if err != nil {
			c.server.logger.Printf("compute assignments: run_id=%s error=%v", c.runID, err)
			return
		}
	}

	c.revision++
	assignments := make(map[string]*loadtestv1.LoadAssignment, len(workers))
	for _, worker := range workers {
		assignments[worker.state.ID] = c.assignmentFor(loadSlices[worker.state.ID])
	}
	if c.server.metrics != nil {
		c.server.metrics.RecordAssignments(c.revision, assignments)
	}
	deliveredLoads := make([]*loadtestv1.LoadSlice, 0, len(workers))
	deliveredWorkers := make([]string, 0, len(workers))
	for _, worker := range workers {
		assignment := assignments[worker.state.ID]
		if err := worker.send(&loadtestv1.OrchestratorMessage{Payload: &loadtestv1.OrchestratorMessage_Assignment{
			Assignment: assignment,
		}}); err != nil {
			c.server.logger.Printf(
				"assignment delivery failed: worker_id=%s run_id=%s revision=%d error=%v",
				worker.state.ID,
				c.runID,
				c.revision,
				err,
			)
			c.server.removeSessionWithReason(worker.state.ID, worker, "assignment delivery failed")
			continue
		}
		c.recordDelivered(worker.state.ID, assignment)
		deliveredLoads = append(deliveredLoads, assignment.GetLoad())
		if isActiveSlice(assignment.GetLoad()) {
			deliveredWorkers = append(deliveredWorkers, worker.state.ID)
		}
	}
	if c.restoresConfiguredCapacity(deliveredLoads) {
		c.recordRecoveredDemand(c.activatesAt, deliveredWorkers)
	}
}

// restoresConfiguredCapacity prevents a partial assignment revision from
// closing recovery windows before the fleet can generate the requested load.
func (c *assignmentCoordinator) restoresConfiguredCapacity(loads []*loadtestv1.LoadSlice) bool {
	var virtualUsers uint64
	var rate uint64
	var maxInFlight uint64
	for _, load := range loads {
		virtualUsers += load.GetVirtualUsers()
		rate += load.GetRate()
		maxInFlight += load.GetMaxInFlight()
	}
	switch c.config.Load.Model {
	case config.LoadConstantVUs:
		return virtualUsers >= uint64(c.config.Load.VirtualUsers)
	case config.LoadConstantRate:
		return rate >= uint64(c.config.Load.Rate) &&
			maxInFlight >= uint64(c.config.Load.MaxInFlight)
	default:
		return false
	}
}

func (c *assignmentCoordinator) processFailures() {
	c.failureMu.Lock()
	failures := c.pendingFailures
	c.pendingFailures = make(map[string]workerFailure)
	c.failureMu.Unlock()

	for workerID, failure := range failures {
		c.invalidateRestorations(workerID, failure.detected)
		assignment := c.activeDeliveryAt(workerID, failure.detected)
		delete(c.delivered, workerID)
		if assignment == nil || c.revision == 0 || c.ctx.Err() != nil ||
			!failure.detected.Before(c.deadline) || !isActiveSlice(assignment.GetLoad()) {
			continue
		}
		if c.server.metrics.RecordWorkerFailure(workerID, failure.reason) &&
			assignment.GetLoad().GetModel() == loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE {
			c.addRecovery(workerID, workerRecovery{
				detected: failure.detected,
				load:     proto.Clone(assignment.GetLoad()).(*loadtestv1.LoadSlice),
			})
			c.server.logger.Printf(
				"worker recovery pending: worker_id=%s run_id=%s rate=%d detected_at=%s",
				workerID,
				c.runID,
				assignment.GetLoad().GetRate(),
				failure.detected.Format(time.RFC3339Nano),
			)
		}
	}
}

func (c *assignmentCoordinator) addRecovery(workerID string, recovery workerRecovery) {
	current, exists := c.recovery[workerID]
	if exists && !recovery.detected.Before(current.detected) {
		return
	}
	c.recovery[workerID] = recovery
}

// invalidateRestorations extends an optimistically credited recovery when one
// of its providers dies before the shared assignment activation.
func (c *assignmentCoordinator) invalidateRestorations(workerID string, detected time.Time) {
	for index := range c.restorations {
		restoration := &c.restorations[index]
		if restoration.invalidated || !detected.Before(restoration.activatesAt) {
			continue
		}
		if _, provides := restoration.providers[workerID]; !provides {
			continue
		}
		restoration.invalidated = true
		for recoveredID, load := range restoration.recovered {
			c.addRecovery(recoveredID, workerRecovery{
				detected: restoration.activatesAt,
				load:     proto.Clone(load).(*loadtestv1.LoadSlice),
			})
		}
	}
}

// recordDelivered retains pending revisions so a pre-activation failure can
// still be attributed to the assignment that was active at detection time.
func (c *assignmentCoordinator) recordDelivered(workerID string, assignment *loadtestv1.LoadAssignment) {
	if c.delivered == nil {
		c.delivered = make(map[string][]*loadtestv1.LoadAssignment)
	}
	c.delivered[workerID] = append(
		c.delivered[workerID],
		proto.Clone(assignment).(*loadtestv1.LoadAssignment),
	)
}

func (c *assignmentCoordinator) activeDeliveryAt(workerID string, at time.Time) *loadtestv1.LoadAssignment {
	history := c.delivered[workerID]
	for index := len(history) - 1; index >= 0; index-- {
		assignment := history[index]
		if !assignment.GetActivatesAt().AsTime().After(at) {
			return assignment
		}
	}
	return nil
}

// recordRecoveredDemand adds only arrivals whose absolute deadlines passed
// before a successful replacement assignment restored open-model capacity.
func (c *assignmentCoordinator) recordRecoveredDemand(restored time.Time, providerIDs []string) {
	providers := make(map[string]struct{}, len(providerIDs))
	for _, workerID := range providerIDs {
		providers[workerID] = struct{}{}
	}
	recovered := make(map[string]*loadtestv1.LoadSlice, len(c.recovery))
	for workerID, recovery := range c.recovery {
		records := c.recoveryArrivalRecords(recovery, restored)
		if err := c.server.metrics.addRecoveryUnmetDemandRecords(workerID, records); err != nil {
			c.server.logger.Printf("record recovery demand: worker_id=%s run_id=%s error=%v", workerID, c.runID, err)
			continue
		}
		var count uint64
		for _, record := range records {
			count += record.unmet
		}
		recovered[workerID] = proto.Clone(recovery.load).(*loadtestv1.LoadSlice)
		c.server.logger.Printf(
			"worker recovery accounted: worker_id=%s run_id=%s unmet_demand=%d restored_at=%s",
			workerID,
			c.runID,
			count,
			restored.Format(time.RFC3339Nano),
		)
		delete(c.recovery, workerID)
	}
	if len(recovered) > 0 {
		c.restorations = append(c.restorations, capacityRestoration{
			activatesAt: restored,
			providers:   providers,
			recovered:   recovered,
		})
	}
}

func (c *assignmentCoordinator) recoveryArrivalRecords(recovery workerRecovery, restored time.Time) []arrivalRecord {
	globalRate := int(recovery.load.GetGlobalRate())
	shardIndex := int(recovery.load.GetScheduleShardIndex())
	shardCount := int(recovery.load.GetScheduleShardCount())
	if globalRate == 0 && shardIndex == 0 && shardCount == 0 {
		globalRate = int(recovery.load.GetRate())
		shardCount = 1
	}
	start := recovery.detected
	if start.Before(c.startsAt) {
		start = c.startsAt
	}
	records := make([]arrivalRecord, 0)
	for start.Before(restored) {
		elapsed := start.Sub(c.startsAt)
		next := c.startsAt.Add((elapsed/time.Second + 1) * time.Second)
		end := next
		if restored.Before(end) {
			end = restored
		}
		count := loadgen.ScheduledShardArrivalsBetween(
			c.startsAt,
			start,
			end,
			globalRate,
			recovery.load.GetRampUp().AsDuration(),
			shardIndex,
			shardCount,
		)
		records = append(records, arrivalRecord{start: start, end: end, unmet: count})
		start = end
	}
	return records
}

// assignmentFor combines one worker's slice with the run-wide target and time window.
func (c *assignmentCoordinator) assignmentFor(load *loadtestv1.LoadSlice) *loadtestv1.LoadAssignment {
	headers := make(map[string]string, len(c.config.Target.Headers))
	for name, value := range c.config.Target.Headers {
		headers[name] = value
	}
	return &loadtestv1.LoadAssignment{
		RunId:    c.runID,
		Revision: c.revision,
		Target: &loadtestv1.Target{Protocol: &loadtestv1.Target_Http{
			Http: &loadtestv1.HttpTarget{
				Url:     c.config.Target.URL,
				Method:  c.config.Target.Method,
				Headers: headers,
				Body:    []byte(c.config.Target.Body),
			},
		}},
		Load:        load,
		StartsAt:    timestamppb.New(c.startsAt),
		Deadline:    timestamppb.New(c.deadline),
		ActivatesAt: timestamppb.New(c.activatesAt),
	}
}

func (s *Server) activeSessions() []*session {
	s.mu.RLock()
	workers := make([]*session, 0, len(s.sessions))
	for _, worker := range s.sessions {
		workers = append(workers, worker)
	}
	s.mu.RUnlock()
	slices.SortFunc(workers, func(left, right *session) int {
		return strings.Compare(left.state.ID, right.state.ID)
	})
	return workers
}
