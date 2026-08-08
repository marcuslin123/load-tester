package orchestrator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/config"
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
	config   config.Config
	runID    string
	leadTime time.Duration
	now      func() time.Time
	changed  chan struct{}
	started  chan runWindow

	revision uint64
	startsAt time.Time
	deadline time.Time
}

type runWindow struct {
	startsAt time.Time
	deadline time.Time
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
		server:   server,
		config:   options.Config,
		runID:    strings.TrimSpace(options.RunID),
		leadTime: options.LeadTime,
		now:      now,
		changed:  make(chan struct{}, 1),
		started:  make(chan runWindow, 1),
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
	if len(workers) < c.config.Fleet.MinWorkers {
		return
	}
	now := c.now()
	if c.revision == 0 {
		c.startsAt = now.Add(c.leadTime)
		c.deadline = c.startsAt.Add(c.config.Load.Duration)
		c.started <- runWindow{startsAt: c.startsAt, deadline: c.deadline}
	} else if !now.Before(c.deadline) {
		return
	}

	workerIDs := make([]string, len(workers))
	for index, worker := range workers {
		workerIDs[index] = worker.state.ID
	}
	loadSlices, err := splitLoad(c.config.Load, workerIDs)
	if err != nil {
		c.server.logger.Printf("compute assignments: run_id=%s error=%v", c.runID, err)
		return
	}

	c.revision++
	assignments := make(map[string]*loadtestv1.LoadAssignment, len(workers))
	for _, worker := range workers {
		assignments[worker.state.ID] = c.assignmentFor(loadSlices[worker.state.ID])
	}
	if c.server.metrics != nil {
		c.server.metrics.RecordAssignments(c.revision, assignments)
	}
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
			c.server.removeSession(worker.state.ID, worker)
		}
	}
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
		Load:     load,
		StartsAt: timestamppb.New(c.startsAt),
		Deadline: timestamppb.New(c.deadline),
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
