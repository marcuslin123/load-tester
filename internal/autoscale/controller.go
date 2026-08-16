package autoscale

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/marcuslin123/load-tester/internal/fleet"
	"github.com/marcuslin123/load-tester/internal/loadgen"
	"github.com/marcuslin123/load-tester/internal/metrics"
)

const readinessPollInterval = 100 * time.Millisecond

// ArrivalWindow contains exact load-generation counters for one evaluation interval.
type ArrivalWindow struct {
	Admitted uint64
	Unmet    uint64
	Complete bool
}

// Source supplies exact load-generation windows without exposing gRPC internals.
type Source interface {
	ActiveWorkerIDs() []string
	ArrivalWindow(time.Time, time.Time) ArrivalWindow
	RecordInfrastructureViolation(string)
}

// RunWindow preserves the absolute schedule across fleet membership changes.
type RunWindow struct {
	StartsAt time.Time
	Deadline time.Time
}

// ControllerOptions configures one managed fleet and its evaluation cadence.
type ControllerOptions struct {
	Autoscale        bool
	MinWorkers       int
	MaxWorkers       int
	TargetRate       int
	RampUp           time.Duration
	StartupTimeout   time.Duration
	EvaluationPeriod time.Duration
	ObservationDelay time.Duration
	Policy           PolicyConfig
}

// Status is the latest immutable controller observation for logs and metrics.
type Status struct {
	Enabled            bool
	DesiredWorkers     int
	RegisteredWorkers  int
	ExpectedRate       float64
	AdmittedRate       float64
	SchedulerCoverage  float64
	WindowComplete     bool
	DeficientIntervals int
	ScaleActions       uint64
	FleetErrors        uint64
}

type controllerClock interface {
	Now() time.Time
	WaitUntil(context.Context, time.Time) bool
}

type realControllerClock struct{}

type controllerDependencies struct {
	clock  controllerClock
	logger *log.Logger
}

// Controller reconciles worker availability before applying capacity decisions.
type Controller struct {
	manager fleet.FleetManager
	source  Source
	options ControllerOptions
	clock   controllerClock
	logger  *log.Logger

	mu     sync.RWMutex
	status Status
	state  State

	// Registration history is reset whenever the provider creates a new worker generation.
	registeredWorkers map[string]struct{}
	unregisteredSince map[string]time.Time
}

// NewController constructs a provider-neutral worker fleet controller.
func NewController(manager fleet.FleetManager, source Source, options ControllerOptions, logger *log.Logger) (*Controller, error) {
	return newController(manager, source, options, controllerDependencies{
		clock:  realControllerClock{},
		logger: logger,
	})
}

func newController(
	manager fleet.FleetManager,
	source Source,
	options ControllerOptions,
	dependencies controllerDependencies,
) (*Controller, error) {
	if manager == nil {
		return nil, errors.New("fleet manager is required")
	}
	if source == nil {
		return nil, errors.New("autoscale source is required")
	}
	if options.MinWorkers <= 0 {
		return nil, errors.New("minimum workers must be greater than zero")
	}
	if options.MaxWorkers < options.MinWorkers {
		return nil, errors.New("maximum workers must be at least minimum workers")
	}
	if options.Autoscale && options.TargetRate <= 0 {
		return nil, errors.New("target rate must be greater than zero")
	}
	if options.TargetRate < 0 {
		return nil, errors.New("target rate must not be negative")
	}
	if options.RampUp < 0 {
		return nil, errors.New("ramp-up must not be negative")
	}
	if options.StartupTimeout <= 0 {
		return nil, errors.New("startup timeout must be greater than zero")
	}
	if options.EvaluationPeriod <= 0 {
		return nil, errors.New("evaluation period must be greater than zero")
	}
	if options.ObservationDelay < 0 {
		return nil, errors.New("observation delay must not be negative")
	}
	if options.Policy.MaxWorkers != options.MaxWorkers {
		return nil, errors.New("policy maximum workers must match controller maximum workers")
	}
	if dependencies.clock == nil {
		return nil, errors.New("controller clock is required")
	}
	logger := dependencies.logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Controller{
		manager: manager,
		source:  source,
		options: options,
		clock:   dependencies.clock,
		logger:  logger,
		status: Status{
			Enabled:        true,
			DesiredWorkers: options.MinWorkers,
		},
		registeredWorkers: make(map[string]struct{}),
		unregisteredSince: make(map[string]time.Time),
	}, nil
}

// Prepare establishes the minimum managed fleet before assignments may start.
func (c *Controller) Prepare(ctx context.Context) error {
	if err := c.manager.Scale(ctx, c.options.MinWorkers); err != nil {
		return fmt.Errorf("initial worker scale: %w", err)
	}
	deadline := c.clock.Now().Add(c.options.StartupTimeout)
	for {
		activeWorkerIDs := c.source.ActiveWorkerIDs()
		c.markRegistered(activeWorkerIDs)
		registered := len(activeWorkerIDs)
		c.updateStatus(func(status *Status) { status.RegisteredWorkers = registered })
		if registered >= c.options.MinWorkers {
			return nil
		}
		now := c.clock.Now()
		if !now.Before(deadline) {
			return fmt.Errorf("%d workers did not register within %s", c.options.MinWorkers, c.options.StartupTimeout)
		}
		wake := minTime(now.Add(readinessPollInterval), deadline)
		if !c.clock.WaitUntil(ctx, wake) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("%d workers did not register within %s", c.options.MinWorkers, c.options.StartupTimeout)
		}
	}
}

// Run evaluates complete schedule windows until the run ends or ctx is canceled.
func (c *Controller) Run(ctx context.Context, window RunWindow) error {
	if window.StartsAt.IsZero() || !window.Deadline.After(window.StartsAt) {
		return errors.New("valid autoscale run window is required")
	}
	if !c.clock.WaitUntil(ctx, window.StartsAt) {
		return nil
	}

	from := window.StartsAt
	for from.Before(window.Deadline) {
		to := minTime(from.Add(c.options.EvaluationPeriod), window.Deadline)
		if !c.clock.WaitUntil(ctx, to.Add(c.options.ObservationDelay)) {
			return nil
		}

		replacementAttempted, reconcileErr := c.reconcileFleet(ctx)
		if reconcileErr != nil {
			c.recordFleetError("reconcile worker fleet", reconcileErr)
		}

		arrivals := c.source.ArrivalWindow(from, to)
		expected := loadgen.ScheduledArrivalsBetween(
			window.StartsAt,
			from,
			to,
			c.options.TargetRate,
			c.options.RampUp,
		)
		registered := len(c.source.ActiveWorkerIDs())
		observation := Observation{
			At:                to,
			ExpectedArrivals:  expected,
			AdmittedArrivals:  arrivals.Admitted,
			UnmetDemand:       arrivals.Unmet,
			DesiredWorkers:    c.currentDesired(),
			RegisteredWorkers: registered,
		}
		periodSeconds := to.Sub(from).Seconds()
		if !arrivals.Complete {
			c.state.DeficientIntervals = 0
			coverage := schedulerCoverage(observation)
			c.updateStatus(func(status *Status) {
				status.RegisteredWorkers = registered
				status.ExpectedRate = float64(expected) / periodSeconds
				status.AdmittedRate = float64(arrivals.Admitted) / periodSeconds
				status.SchedulerCoverage = coverage
				status.WindowComplete = false
				status.DeficientIntervals = 0
			})
			c.logger.Printf(
				"autoscale evaluation: expected=%d admitted=%d unmet=%d coverage=%.4f complete=false deficits=0 desired=%d registered=%d scale=false reason=%q",
				expected,
				arrivals.Admitted,
				arrivals.Unmet,
				coverage,
				c.currentDesired(),
				registered,
				"arrival window incomplete",
			)
			from = to
			continue
		}

		decision, err := Evaluate(c.options.Policy, c.state, observation)
		if err != nil {
			return fmt.Errorf("evaluate autoscale policy: %w", err)
		}
		if !c.options.Autoscale {
			decision.Scale = false
			decision.DesiredWorkers = observation.DesiredWorkers
			decision.Reason = "fleet autoscaling disabled"
		}
		c.state = decision.State
		c.updateStatus(func(status *Status) {
			status.RegisteredWorkers = registered
			status.ExpectedRate = float64(expected) / periodSeconds
			status.AdmittedRate = float64(arrivals.Admitted) / periodSeconds
			status.SchedulerCoverage = decision.Coverage
			status.WindowComplete = true
			status.DeficientIntervals = decision.State.DeficientIntervals
		})

		scaleApplied := false
		if decision.Scale && !replacementAttempted && reconcileErr == nil && to.Before(window.Deadline) {
			if err := c.manager.Scale(ctx, decision.DesiredWorkers); err != nil {
				c.recordFleetError("scale worker fleet", err)
			} else {
				scaleApplied = true
				c.state = c.state.AfterScale(to)
				c.updateStatus(func(status *Status) {
					status.DesiredWorkers = decision.DesiredWorkers
					status.DeficientIntervals = 0
					status.ScaleActions++
				})
			}
		}

		status := c.Status()
		c.logger.Printf(
			"autoscale evaluation: expected=%d admitted=%d unmet=%d coverage=%.4f complete=true deficits=%d desired=%d registered=%d scale=%t reason=%q",
			expected,
			arrivals.Admitted,
			arrivals.Unmet,
			decision.Coverage,
			status.DeficientIntervals,
			status.DesiredWorkers,
			registered,
			scaleApplied,
			decision.Reason,
		)
		from = to
	}
	return nil
}

// reconcileFleet replaces disconnected running workers before changing capacity.
func (c *Controller) reconcileFleet(ctx context.Context) (bool, error) {
	workers, err := c.manager.List(ctx)
	if err != nil {
		return false, fmt.Errorf("list workers: %w", err)
	}
	activeWorkerIDs := c.source.ActiveWorkerIDs()
	c.markRegistered(activeWorkerIDs)
	active := make(map[string]struct{}, len(activeWorkerIDs))
	for _, workerID := range activeWorkerIDs {
		active[workerID] = struct{}{}
	}
	present := make(map[string]struct{}, len(workers))
	for _, worker := range workers {
		present[worker.WorkerID] = struct{}{}
		if worker.State != fleet.WorkerStateRunning {
			delete(c.registeredWorkers, worker.WorkerID)
			delete(c.unregisteredSince, worker.WorkerID)
		}
	}
	for workerID := range c.unregisteredSince {
		if _, exists := present[workerID]; !exists {
			delete(c.unregisteredSince, workerID)
		}
	}
	for workerID := range c.registeredWorkers {
		if _, exists := present[workerID]; !exists {
			delete(c.registeredWorkers, workerID)
		}
	}

	now := c.clock.Now()
	replaced := false
	var replacementErrors []error
	for _, worker := range workers {
		if worker.State != fleet.WorkerStateRunning {
			continue
		}
		if _, registered := active[worker.WorkerID]; registered {
			continue
		}
		if _, previouslyRegistered := c.registeredWorkers[worker.WorkerID]; !previouslyRegistered {
			firstObserved, exists := c.unregisteredSince[worker.WorkerID]
			if !exists {
				c.unregisteredSince[worker.WorkerID] = now
				continue
			}
			if now.Sub(firstObserved) < c.options.StartupTimeout {
				continue
			}
		}
		replaced = true
		if err := c.manager.Replace(ctx, worker.WorkerID); err != nil {
			replacementErrors = append(replacementErrors, fmt.Errorf("replace worker %q: %w", worker.WorkerID, err))
			continue
		}
		delete(c.registeredWorkers, worker.WorkerID)
		c.unregisteredSince[worker.WorkerID] = now
	}
	if replaced {
		return true, errors.Join(replacementErrors...)
	}
	return false, c.manager.Scale(ctx, c.currentDesired())
}

func (c *Controller) markRegistered(workerIDs []string) {
	for _, workerID := range workerIDs {
		c.registeredWorkers[workerID] = struct{}{}
		delete(c.unregisteredSince, workerID)
	}
}

// Status returns a race-free copy of the latest controller state.
func (c *Controller) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// AutoscalerMetrics adapts the latest controller status for Prometheus scrapes.
func (c *Controller) AutoscalerMetrics() metrics.AutoscalerView {
	status := c.Status()
	return metrics.AutoscalerView{
		Enabled:            status.Enabled,
		DesiredWorkers:     status.DesiredWorkers,
		ExpectedRate:       status.ExpectedRate,
		AdmittedRate:       status.AdmittedRate,
		SchedulerCoverage:  status.SchedulerCoverage,
		DeficientIntervals: status.DeficientIntervals,
		ScaleActions:       status.ScaleActions,
		Errors:             status.FleetErrors,
	}
}

func (c *Controller) currentDesired() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status.DesiredWorkers
}

func (c *Controller) updateStatus(update func(*Status)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update(&c.status)
}

func (c *Controller) recordFleetError(operation string, err error) {
	violation := fmt.Sprintf("%s: %v", operation, err)
	c.source.RecordInfrastructureViolation(violation)
	c.updateStatus(func(status *Status) { status.FleetErrors++ })
	c.logger.Printf("autoscale fleet error: %s", violation)
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func (realControllerClock) Now() time.Time {
	return time.Now()
}

func (realControllerClock) WaitUntil(ctx context.Context, deadline time.Time) bool {
	delay := time.Until(deadline)
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
