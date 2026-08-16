package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/loadgen"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	defaultReportingInterval = time.Second
	defaultMetricsBufferSize = 65_536
)

type runControllerOptions struct {
	ReportingInterval time.Duration
	MetricsBufferSize int
	InitialSequence   uint64
	Now               func() time.Time
	NewTimer          func(time.Time) controllerTimer
	StartScheduler    func(*loadtestv1.LoadAssignment, *metrics.Collector) (context.CancelFunc, <-chan error, error)
}

type controllerTimer interface {
	C() <-chan time.Time
	Stop()
}

type realControllerTimer struct {
	timer *time.Timer
}

func newRealControllerTimer(at time.Time) controllerTimer {
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	return &realControllerTimer{timer: time.NewTimer(delay)}
}

func (t *realControllerTimer) C() <-chan time.Time { return t.timer.C }
func (t *realControllerTimer) Stop()               { t.timer.Stop() }

type runStatus struct {
	RunID      string
	Revision   uint64
	State      loadtestv1.WorkerState
	InFlight   uint64
	StartsAt   time.Time
	Deadline   time.Time
	activeLoad bool
}

type applyRunCommand struct {
	assignment *loadtestv1.LoadAssignment
	reply      chan error
}

type metricEmission struct {
	assignment    *loadtestv1.LoadAssignment
	intervalStart time.Time
	intervalEnd   time.Time
	snapshot      metrics.Snapshot
	final         bool
	ready         bool
}

type revisionScheduler struct {
	cancel    context.CancelFunc
	done      <-chan error
	collector *metrics.Collector
	drained   chan struct{}
	emission  *metricEmission
	completed bool
	snapshot  metrics.Snapshot
}

type revisionDrain struct {
	scheduler *revisionScheduler
	err       error
}

// runController serializes assignment transitions and metric boundaries for one
// worker stream while schedulers produce request results concurrently.
type runController struct {
	ctx      context.Context
	cancel   context.CancelFunc
	workerID string
	outbound chan<- *loadtestv1.WorkerMessage
	options  runControllerOptions
	commands chan applyRunCommand
	errors   chan error
	done     chan struct{}

	statusMu sync.RWMutex
	status   runStatus
	inFlight atomic.Uint64
	close    sync.Once
}

func newRunController(
	parent context.Context,
	workerID string,
	outbound chan<- *loadtestv1.WorkerMessage,
	options runControllerOptions,
) (*runController, error) {
	if parent == nil {
		return nil, errors.New("run controller context is required")
	}
	if workerID == "" {
		return nil, errors.New("run controller worker ID is required")
	}
	if outbound == nil {
		return nil, errors.New("run controller outbound queue is required")
	}
	if options.ReportingInterval == 0 {
		options.ReportingInterval = defaultReportingInterval
	}
	if options.MetricsBufferSize == 0 {
		options.MetricsBufferSize = defaultMetricsBufferSize
	}
	if options.InitialSequence == 0 {
		options.InitialSequence = 1
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.NewTimer == nil {
		options.NewTimer = newRealControllerTimer
	}
	if options.ReportingInterval <= 0 {
		return nil, errors.New("reporting interval must be greater than zero")
	}
	if options.MetricsBufferSize <= 0 {
		return nil, errors.New("metrics buffer size must be greater than zero")
	}
	ctx, cancel := context.WithCancel(parent)
	controller := &runController{
		ctx:      ctx,
		cancel:   cancel,
		workerID: workerID,
		outbound: outbound,
		options:  options,
		commands: make(chan applyRunCommand),
		errors:   make(chan error, 1),
		done:     make(chan struct{}),
		status:   runStatus{State: loadtestv1.WorkerState_WORKER_STATE_IDLE},
	}
	go controller.run()
	return controller, nil
}

func (c *runController) Apply(assignment *loadtestv1.LoadAssignment) error {
	if assignment == nil {
		return errors.New("load assignment is required")
	}
	command := applyRunCommand{
		assignment: proto.Clone(assignment).(*loadtestv1.LoadAssignment),
		reply:      make(chan error, 1),
	}
	select {
	case c.commands <- command:
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
	select {
	case err := <-command.reply:
		return err
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

func (c *runController) Status() runStatus {
	c.statusMu.RLock()
	status := c.status
	c.statusMu.RUnlock()
	status.InFlight = c.inFlight.Load()
	if status.activeLoad {
		now := c.options.Now()
		if !now.Before(status.StartsAt) && now.Before(status.Deadline) {
			status.State = loadtestv1.WorkerState_WORKER_STATE_RUNNING
		}
	}
	return status
}

func (c *runController) Errors() <-chan error {
	return c.errors
}

func (c *runController) Close() {
	c.close.Do(c.cancel)
	<-c.done
}

func (c *runController) run() {
	defer close(c.done)
	var (
		current       *loadtestv1.LoadAssignment
		pending       []*loadtestv1.LoadAssignment
		scheduler     *revisionScheduler
		schedulers    []*revisionScheduler
		emissions     []*metricEmission
		intervalStart time.Time
		metricAt      time.Time
		sequence      = c.options.InitialSequence - 1
		timer         controllerTimer
		timerC        <-chan time.Time
		timerAt       time.Time
	)
	drains := make(chan revisionDrain, 1)
	defer func() {
		stopControllerTimer(timer)
		for _, running := range schedulers {
			running.cancel()
		}
		for _, running := range schedulers {
			<-running.drained
			running.collector.Close()
		}
	}()

	resetTimer := func(next time.Time) {
		stopControllerTimer(timer)
		timerAt = next
		timer = c.options.NewTimer(next)
		timerC = timer.C()
	}
	scheduleNextTimer := func() {
		next := time.Time{}
		if len(pending) > 0 {
			next = pending[0].GetActivatesAt().AsTime()
		}
		if current != nil {
			deadline := current.GetDeadline().AsTime()
			if next.IsZero() || deadline.Before(next) {
				next = deadline
			}
			if !metricAt.IsZero() && metricAt.Before(next) {
				next = metricAt
			}
		}
		if next.IsZero() {
			stopControllerTimer(timer)
			timer, timerC, timerAt = nil, nil, time.Time{}
			return
		}
		resetTimer(next)
	}
	pumpEmissions := func() error {
		for {
			// Sequence snapshots by readiness, allowing a drained older revision
			// to follow newer metrics without blocking live observability.
			ready := -1
			for index, emission := range emissions {
				if emission.ready {
					ready = index
					break
				}
			}
			if ready < 0 {
				return nil
			}
			next := emissions[ready]
			if err := c.emitSnapshot(
				next.assignment,
				next.snapshot,
				&sequence,
				next.intervalStart,
				next.intervalEnd,
				next.final,
			); err != nil {
				return err
			}
			emissions = append(emissions[:ready], emissions[ready+1:]...)
		}
	}
	queueSnapshot := func(
		assignment *loadtestv1.LoadAssignment,
		snapshot metrics.Snapshot,
		start time.Time,
		end time.Time,
		final bool,
	) error {
		emissions = append(emissions, &metricEmission{
			assignment:    assignment,
			intervalStart: start,
			intervalEnd:   end,
			snapshot:      snapshot,
			final:         final,
			ready:         true,
		})
		return pumpEmissions()
	}
	startScheduler := func(assignment *loadtestv1.LoadAssignment) (*revisionScheduler, error) {
		collector, err := metrics.NewCollector(c.options.MetricsBufferSize)
		if err != nil {
			return nil, err
		}
		cancel, done, err := c.startAssignmentScheduler(assignment, collector)
		if err != nil {
			collector.Close()
			return nil, err
		}
		if cancel == nil || done == nil {
			if cancel != nil {
				cancel()
			}
			collector.Close()
			return nil, errors.New("scheduler must return cancellation and completion signals")
		}
		running := &revisionScheduler{
			cancel:    cancel,
			done:      done,
			collector: collector,
			drained:   make(chan struct{}),
		}
		schedulers = append(schedulers, running)
		go c.awaitRevisionDrain(running, drains)
		return running, nil
	}
	beginDrain := func(assignment *loadtestv1.LoadAssignment, running *revisionScheduler, start, end time.Time) {
		if running == nil {
			return
		}
		running.cancel()
		emission := &metricEmission{
			assignment:    assignment,
			intervalStart: start,
			intervalEnd:   end,
			final:         true,
		}
		if running.completed {
			emission.snapshot = running.snapshot
			emission.ready = true
		}
		running.emission = emission
		emissions = append(emissions, emission)
	}
	activatePending := func() error {
		next := pending[0]
		activation := next.GetActivatesAt().AsTime()
		if current != nil {
			if isActiveLoad(current.GetLoad()) && activation.After(intervalStart) {
				beginDrain(current, scheduler, intervalStart, activation)
			}
		}

		active := isActiveLoad(next.GetLoad()) && activation.Before(next.GetDeadline().AsTime())
		if active {
			var err error
			scheduler, err = startScheduler(next)
			if err != nil {
				return err
			}
			metricAt = nextMetricBoundary(next.GetStartsAt().AsTime(), activation, next.GetDeadline().AsTime(), c.options.ReportingInterval)
		} else {
			scheduler = nil
			metricAt = time.Time{}
		}
		intervalStart = activation
		current, pending = next, pending[1:]
		c.setStatus(next, active)
		return pumpEmissions()
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		case command := <-c.commands:
			latest := current
			if len(pending) > 0 {
				latest = pending[len(pending)-1]
			}
			duplicate, err := validateRunTransition(latest, command.assignment)
			if err != nil || duplicate {
				command.reply <- err
				continue
			}
			now := c.options.Now()
			if latest != nil && !command.assignment.GetActivatesAt().AsTime().After(now) {
				command.reply <- errors.New("replacement activation time must be in the future")
				continue
			}
			pending = append(pending, command.assignment)
			scheduleNextTimer()
			command.reply <- nil

		case <-timerC:
			if len(pending) > 0 && timerAt.Equal(pending[0].GetActivatesAt().AsTime()) {
				if err := activatePending(); err != nil {
					c.reportError(err)
					return
				}
				scheduleNextTimer()
				continue
			}
			if current == nil {
				timerC = nil
				continue
			}
			deadline := current.GetDeadline().AsTime()
			if !timerAt.Before(deadline) {
				if isActiveLoad(current.GetLoad()) && deadline.After(intervalStart) {
					beginDrain(current, scheduler, intervalStart, deadline)
				}
				scheduler = nil
				c.setStatus(current, false)
				metricAt = time.Time{}
				stopControllerTimer(timer)
				timer, timerC, timerAt = nil, nil, time.Time{}
				if err := pumpEmissions(); err != nil {
					c.reportError(err)
					return
				}
				continue
			}
			if isActiveLoad(current.GetLoad()) && scheduler != nil && timerAt.After(intervalStart) {
				snapshot := scheduler.collector.SnapshotAndReset()
				if scheduler.completed {
					snapshot = scheduler.snapshot
					scheduler.snapshot = metrics.Snapshot{}
				}
				if err := queueSnapshot(current, snapshot, intervalStart, timerAt, false); err != nil {
					c.reportError(err)
					return
				}
				intervalStart = timerAt
			}
			metricAt = nextMetricBoundary(current.GetStartsAt().AsTime(), timerAt, deadline, c.options.ReportingInterval)
			scheduleNextTimer()

		case drained := <-drains:
			snapshot := drained.scheduler.collector.Close()
			drained.scheduler.completed = true
			drained.scheduler.snapshot = snapshot
			if drained.scheduler.emission != nil {
				drained.scheduler.emission.snapshot = snapshot
				drained.scheduler.emission.ready = true
			}
			if drained.err != nil {
				c.reportError(fmt.Errorf("run assigned load: %w", drained.err))
				return
			}
			if err := pumpEmissions(); err != nil {
				c.reportError(err)
				return
			}
		}
	}
}

func (c *runController) startAssignmentScheduler(
	assignment *loadtestv1.LoadAssignment,
	collector *metrics.Collector,
) (context.CancelFunc, <-chan error, error) {
	if c.options.StartScheduler != nil {
		return c.options.StartScheduler(assignment, collector)
	}
	return c.startScheduler(assignment, collector)
}

func (c *runController) awaitRevisionDrain(running *revisionScheduler, drains chan<- revisionDrain) {
	defer close(running.drained)
	err := <-running.done
	select {
	case drains <- revisionDrain{scheduler: running, err: err}:
	case <-c.ctx.Done():
		running.collector.Close()
	}
}

func (c *runController) startScheduler(
	assignment *loadtestv1.LoadAssignment,
	collector *metrics.Collector,
) (context.CancelFunc, <-chan error, error) {
	httpTarget := assignment.GetTarget().GetHttp()
	executor, err := protocol.NewHTTP(config.Target{
		Protocol: config.ProtocolHTTP,
		URL:      httpTarget.GetUrl(),
		Method:   httpTarget.GetMethod(),
		Headers:  httpTarget.GetHeaders(),
		Body:     string(httpTarget.GetBody()),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("configure assigned HTTP target: %w", err)
	}
	tracked := &inFlightProtocol{next: executor, count: &c.inFlight}
	requestCtx, cancelRequests := context.WithDeadline(c.ctx, assignment.GetDeadline().AsTime())
	scheduleCtx, stopScheduling := context.WithCancel(requestCtx)
	done := make(chan error, 1)
	go func() {
		defer cancelRequests()
		load := assignment.GetLoad()
		switch load.GetModel() {
		case loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS:
			done <- loadgen.RunClosedWithRequestContext(scheduleCtx, requestCtx, tracked, collector, loadgen.ClosedOptions{
				VirtualUsers: int(load.GetVirtualUsers()),
				RampUp:       load.GetRampUp().AsDuration(),
				StartAt:      assignment.GetStartsAt().AsTime(),
			})
		case loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE:
			done <- loadgen.RunOpenWithRequestContext(scheduleCtx, requestCtx, tracked, collector, openOptionsForLoad(load, assignment.GetStartsAt().AsTime()))
		default:
			done <- fmt.Errorf("unsupported assigned load model %v", load.GetModel())
		}
	}()
	return stopScheduling, done, nil
}

// openOptionsForLoad preserves the local capacity allocation while forwarding
// the global sequence metadata that makes constant-rate workers disjoint.
func openOptionsForLoad(load *loadtestv1.LoadSlice, startsAt time.Time) loadgen.OpenOptions {
	return loadgen.OpenOptions{
		Rate:               int(load.GetRate()),
		GlobalRate:         int(load.GetGlobalRate()),
		ScheduleShardIndex: int(load.GetScheduleShardIndex()),
		ScheduleShardCount: int(load.GetScheduleShardCount()),
		MaxInFlight:        int(load.GetMaxInFlight()),
		RampUp:             load.GetRampUp().AsDuration(),
		StartAt:            startsAt,
	}
}

func (c *runController) emitSnapshot(
	assignment *loadtestv1.LoadAssignment,
	snapshot metrics.Snapshot,
	sequence *uint64,
	intervalStart time.Time,
	intervalEnd time.Time,
	finalForRevision bool,
) error {
	delta, err := newMetricsDelta(
		c.workerID,
		assignment.GetRunId(),
		assignment.GetRevision(),
		*sequence+1,
		intervalStart,
		intervalEnd,
		snapshot,
		finalForRevision,
	)
	if err != nil {
		return err
	}
	message := &loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Metrics{Metrics: delta}}
	select {
	case c.outbound <- message:
		*sequence++
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

func (c *runController) setStatus(assignment *loadtestv1.LoadAssignment, active bool) {
	c.statusMu.Lock()
	c.status = runStatus{
		RunID:      assignment.GetRunId(),
		Revision:   assignment.GetRevision(),
		State:      loadtestv1.WorkerState_WORKER_STATE_IDLE,
		StartsAt:   assignment.GetStartsAt().AsTime(),
		Deadline:   assignment.GetDeadline().AsTime(),
		activeLoad: active,
	}
	c.statusMu.Unlock()
}

func (c *runController) reportError(err error) {
	select {
	case c.errors <- err:
	default:
	}
}

func validateRunTransition(current, next *loadtestv1.LoadAssignment) (bool, error) {
	if err := validateAssignment(next); err != nil {
		return false, err
	}
	if next.GetActivatesAt() == nil {
		return false, errors.New("activation time is required")
	}
	if err := next.GetActivatesAt().CheckValid(); err != nil {
		return false, fmt.Errorf("invalid activation time: %w", err)
	}
	activation := next.GetActivatesAt().AsTime()
	if activation.Before(next.GetStartsAt().AsTime()) {
		return false, errors.New("activation time must not be before the run start")
	}
	if next.GetRevision() == 1 && !activation.Equal(next.GetStartsAt().AsTime()) {
		return false, errors.New("initial activation time must equal the run start")
	}
	if !activation.Before(next.GetDeadline().AsTime()) {
		return false, errors.New("activation time must be before the deadline")
	}
	if current == nil {
		return false, nil
	}
	if next.GetRunId() != current.GetRunId() {
		return false, fmt.Errorf("assignment run %q conflicts with active run %q", next.GetRunId(), current.GetRunId())
	}
	if next.GetRevision() < current.GetRevision() {
		return false, fmt.Errorf("assignment revision %d is older than %d", next.GetRevision(), current.GetRevision())
	}
	if next.GetRevision() == current.GetRevision() {
		if proto.Equal(next, current) {
			return true, nil
		}
		return false, fmt.Errorf("assignment revision %d conflicts with the applied revision", next.GetRevision())
	}
	if !next.GetStartsAt().AsTime().Equal(current.GetStartsAt().AsTime()) ||
		!next.GetDeadline().AsTime().Equal(current.GetDeadline().AsTime()) {
		return false, errors.New("replacement assignment must preserve the run window")
	}
	if !proto.Equal(next.GetTarget(), current.GetTarget()) {
		return false, errors.New("replacement assignment must preserve the target")
	}
	if !activation.After(current.GetActivatesAt().AsTime()) {
		return false, errors.New("replacement activation time must increase")
	}
	return false, nil
}

func isActiveLoad(load *loadtestv1.LoadSlice) bool {
	if load == nil {
		return false
	}
	switch load.GetModel() {
	case loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS:
		return load.GetVirtualUsers() > 0
	case loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE:
		return load.GetRate() > 0 && load.GetMaxInFlight() > 0
	default:
		return false
	}
}

func nextMetricBoundary(started, now, deadline time.Time, interval time.Duration) time.Time {
	if now.Before(started) {
		return minTime(started.Add(interval), deadline)
	}
	elapsed := now.Sub(started)
	next := started.Add((elapsed/interval + 1) * interval)
	return minTime(next, deadline)
}

func stopControllerTimer(timer controllerTimer) {
	if timer == nil {
		return
	}
	timer.Stop()
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

type inFlightProtocol struct {
	next  protocol.Protocol
	count *atomic.Uint64
}

func (p *inFlightProtocol) Execute(ctx context.Context) protocol.Result {
	p.count.Add(1)
	defer p.count.Add(^uint64(0))
	return p.next.Execute(ctx)
}
