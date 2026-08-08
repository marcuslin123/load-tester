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
	Now               func() time.Time
	StopScheduler     func(context.CancelFunc, <-chan error)
}

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
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.StopScheduler == nil {
		options.StopScheduler = stopScheduler
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
		now := time.Now()
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
		current         *loadtestv1.LoadAssignment
		collector       *metrics.Collector
		schedulerCancel context.CancelFunc
		schedulerDone   <-chan error
		intervalStart   time.Time
		sequence        uint64
		timer           *time.Timer
		timerC          <-chan time.Time
		timerAt         time.Time
	)
	defer func() {
		stopTimer(timer)
		c.options.StopScheduler(schedulerCancel, schedulerDone)
		if collector != nil {
			collector.Close()
		}
	}()

	resetTimer := func(next time.Time) {
		stopTimer(timer)
		timerAt = next
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		timer = time.NewTimer(delay)
		timerC = timer.C
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		case command := <-c.commands:
			duplicate, err := validateRunTransition(current, command.assignment)
			if err != nil || duplicate {
				command.reply <- err
				continue
			}

			now := c.options.Now()
			if current != nil {
				c.options.StopScheduler(schedulerCancel, schedulerDone)
				schedulerCancel, schedulerDone = nil, nil
				now = c.options.Now()
				transitionEnd := minTime(now, current.GetDeadline().AsTime())
				if isActiveLoad(current.GetLoad()) && collector != nil {
					if transitionEnd.Sub(intervalStart) > c.options.ReportingInterval {
						err := errors.New("revision flush exceeded the reporting interval")
						command.reply <- err
						c.reportError(err)
						return
					}
					if transitionEnd.After(intervalStart) {
						if err := c.flush(current, collector, &sequence, intervalStart, transitionEnd, true); err != nil {
							command.reply <- err
							c.reportError(err)
							return
						}
					}
				}
			}

			next := command.assignment
			active := isActiveLoad(next.GetLoad()) && now.Before(next.GetDeadline().AsTime())
			if active && collector == nil {
				collector, err = metrics.NewCollector(c.options.MetricsBufferSize)
				if err != nil {
					command.reply <- err
					continue
				}
			}
			if active {
				schedulerCancel, schedulerDone, err = c.startScheduler(next, collector)
				if err != nil {
					command.reply <- err
					continue
				}
				intervalStart = maxTime(now, next.GetStartsAt().AsTime())
				resetTimer(nextMetricBoundary(next.GetStartsAt().AsTime(), now, next.GetDeadline().AsTime(), c.options.ReportingInterval))
			} else {
				intervalStart = next.GetStartsAt().AsTime()
				resetTimer(next.GetDeadline().AsTime())
			}
			current = next
			c.setStatus(next, active)
			command.reply <- nil

		case <-timerC:
			if current == nil {
				continue
			}
			deadline := current.GetDeadline().AsTime()
			if !timerAt.Before(deadline) {
				c.options.StopScheduler(schedulerCancel, schedulerDone)
				schedulerCancel, schedulerDone = nil, nil
				if isActiveLoad(current.GetLoad()) && collector != nil && deadline.After(intervalStart) {
					if err := c.flush(current, collector, &sequence, intervalStart, deadline, true); err != nil {
						c.reportError(err)
						return
					}
				}
				c.setStatus(current, false)
				timerC = nil
				continue
			}
			if isActiveLoad(current.GetLoad()) && collector != nil && timerAt.After(intervalStart) {
				if err := c.flush(current, collector, &sequence, intervalStart, timerAt, false); err != nil {
					c.reportError(err)
					return
				}
				intervalStart = timerAt
			}
			resetTimer(nextMetricBoundary(current.GetStartsAt().AsTime(), timerAt, deadline, c.options.ReportingInterval))

		case err := <-schedulerDone:
			schedulerCancel, schedulerDone = nil, nil
			if err != nil {
				c.reportError(fmt.Errorf("run assigned load: %w", err))
				return
			}
		}
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
	ctx, cancel := context.WithDeadline(c.ctx, assignment.GetDeadline().AsTime())
	done := make(chan error, 1)
	go func() {
		load := assignment.GetLoad()
		switch load.GetModel() {
		case loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS:
			done <- loadgen.RunClosed(ctx, tracked, collector, loadgen.ClosedOptions{
				VirtualUsers: int(load.GetVirtualUsers()),
				RampUp:       load.GetRampUp().AsDuration(),
				StartAt:      assignment.GetStartsAt().AsTime(),
			})
		case loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE:
			done <- loadgen.RunOpen(ctx, tracked, collector, loadgen.OpenOptions{
				Rate:        int(load.GetRate()),
				MaxInFlight: int(load.GetMaxInFlight()),
				RampUp:      load.GetRampUp().AsDuration(),
				StartAt:     assignment.GetStartsAt().AsTime(),
			})
		default:
			done <- fmt.Errorf("unsupported assigned load model %v", load.GetModel())
		}
	}()
	return cancel, done, nil
}

func (c *runController) flush(
	assignment *loadtestv1.LoadAssignment,
	collector *metrics.Collector,
	sequence *uint64,
	intervalStart time.Time,
	intervalEnd time.Time,
	finalForRevision bool,
) error {
	snapshot := collector.SnapshotAndReset()
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

func stopScheduler(cancel context.CancelFunc, done <-chan error) {
	if cancel == nil {
		return
	}
	cancel()
	if done != nil {
		<-done
	}
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
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
