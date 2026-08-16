package autoscale

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/fleet"
	"github.com/marcuslin123/load-tester/internal/metrics"
)

func TestControllerPrepareScalesMinimumAndWaitsForRegistration(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{}
	clock := &scriptedControllerClock{now: started}
	clock.onWait = func(time.Time, int) bool {
		source.active = []string{"worker-a", "worker-b"}
		return true
	}
	manager := &fleetManagerStub{}
	controller := mustController(t, manager, source, clock, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       2,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})

	if err := controller.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if !reflect.DeepEqual(manager.calls, []int{2}) {
		t.Fatalf("Scale() calls = %v, want [2]", manager.calls)
	}
	if got := controller.Status(); got.DesiredWorkers != 2 || got.RegisteredWorkers != 2 {
		t.Fatalf("Status() = %+v, want desired and registered 2", got)
	}
}

func TestControllerPrepareFailsInitialScaleOrReadinessTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		manager *fleetManagerStub
		want    string
	}{
		{name: "initial scale", manager: &fleetManagerStub{errors: []error{errors.New("docker unavailable")}}, want: "initial worker scale"},
		{name: "registration timeout", manager: &fleetManagerStub{}, want: "register"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := &scriptedControllerClock{now: time.Unix(100, 0)}
			controller := mustController(t, test.manager, &controllerSourceStub{}, clock, ControllerOptions{
				MinWorkers:       1,
				MaxWorkers:       2,
				TargetRate:       100,
				StartupTimeout:   200 * time.Millisecond,
				EvaluationPeriod: 5 * time.Second,
				Policy:           testPolicyConfigWithMaximum(2),
			})

			err := controller.Prepare(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Prepare() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestControllerAutoscalerMetricsMapsStatus(t *testing.T) {
	t.Parallel()

	controller := mustController(t, &fleetManagerStub{}, &controllerSourceStub{}, &scriptedControllerClock{}, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       1,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})
	controller.updateStatus(func(status *Status) {
		status.DesiredWorkers = 3
		status.ExpectedRate = 1000
		status.AdmittedRate = 840
		status.SchedulerCoverage = 0.84
		status.DeficientIntervals = 2
		status.ScaleActions = 1
		status.FleetErrors = 4
	})

	want := metrics.AutoscalerView{
		Enabled:            true,
		DesiredWorkers:     3,
		ExpectedRate:       1000,
		AdmittedRate:       840,
		SchedulerCoverage:  0.84,
		DeficientIntervals: 2,
		ScaleActions:       1,
		Errors:             4,
	}
	if got := controller.AutoscalerMetrics(); got != want {
		t.Fatalf("AutoscalerMetrics() = %+v, want %+v", got, want)
	}
}

func TestControllerRunScalesAfterTwoDeficitsAndRecovers(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active: []string{"worker-a"},
		windows: []ArrivalWindow{
			{Admitted: 400, Complete: true},
			{Admitted: 400, Complete: true},
			{Admitted: 500, Complete: true},
		},
	}
	manager := &fleetManagerStub{}
	clock := &scriptedControllerClock{now: started}
	manager.onScale = func(desired int) {
		if desired == 2 {
			source.setActive("worker-a", "worker-b")
		}
	}
	clock.stopAfter = 3
	controller := mustController(t, manager, source, clock, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       1,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})

	err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(20 * time.Second)})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(manager.calls, []int{1, 1, 2, 2}) {
		t.Fatalf("Scale() calls = %v, want two evaluations, scale-up, then reconciliation", manager.calls)
	}
	status := controller.Status()
	if status.DesiredWorkers != 2 || status.RegisteredWorkers != 2 || status.ScaleActions != 1 {
		t.Fatalf("Status() = %+v, want recovered two-worker fleet", status)
	}
	if status.SchedulerCoverage != 1 || status.DeficientIntervals != 0 {
		t.Fatalf("Status() = %+v, want healthy final interval", status)
	}
}

func TestControllerReadsExactBoundaryAfterObservationDelay(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	boundary := started.Add(5 * time.Second)
	source := &controllerSourceStub{
		active:  []string{"worker-a"},
		windows: []ArrivalWindow{{Admitted: 500, Complete: true}},
	}
	clock := &scriptedControllerClock{now: started, stopAfter: 1}
	controller := mustController(t, &fleetManagerStub{}, source, clock, ControllerOptions{
		MinWorkers:       1,
		MaxWorkers:       1,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		ObservationDelay: time.Second,
		Policy:           testPolicyConfigWithMaximum(1),
	})

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: boundary}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := clock.now; !got.Equal(boundary.Add(time.Second)) {
		t.Fatalf("evaluation time = %s, want %s", got, boundary.Add(time.Second))
	}
	if !reflect.DeepEqual(source.windowRequests, []arrivalWindowRequest{{from: started, to: boundary}}) {
		t.Fatalf("ArrivalWindow() requests = %v, want [%s,%s)", source.windowRequests, started, boundary)
	}
	if got := controller.Status().AdmittedRate; got != 100 {
		t.Fatalf("admitted rate = %v, want 100", got)
	}
}

func TestControllerRunUsesExactRampWindow(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active:  []string{"worker-a"},
		windows: []ArrivalWindow{{Admitted: 624, Complete: true}},
	}
	clock := &scriptedControllerClock{now: started}
	clock.stopAfter = 1
	controller := mustController(t, &fleetManagerStub{}, source, clock, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       1,
		MaxWorkers:       4,
		TargetRate:       1000,
		RampUp:           20 * time.Second,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(20 * time.Second)}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	status := controller.Status()
	if status.ExpectedRate != 124.8 || status.AdmittedRate != 124.8 || status.SchedulerCoverage != 1 {
		t.Fatalf("Status() = %+v, want exact first ramp-window rates", status)
	}
}

func TestControllerRunRetriesFailedScaleWithoutCooldown(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active: []string{"worker-a"},
		windows: []ArrivalWindow{
			{Admitted: 400, Complete: true},
			{Admitted: 400, Complete: true},
			{Admitted: 400, Complete: true},
		},
	}
	manager := &fleetManagerStub{scaleErrorFor: map[int][]error{2: {errors.New("quota exceeded"), nil}}}
	manager.onScale = func(desired int) {
		if desired == 2 && countValue(manager.calls, 2) == 2 {
			source.setActive("worker-a", "worker-b")
		}
	}
	clock := &scriptedControllerClock{now: started}
	clock.stopAfter = 3
	controller := mustController(t, manager, source, clock, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       1,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(30 * time.Second)}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if countValue(manager.calls, 2) != 2 {
		t.Fatalf("Scale() calls = %v, want failed scale and immediate retry", manager.calls)
	}
	status := controller.Status()
	if status.FleetErrors != 1 || status.ScaleActions != 1 || status.DesiredWorkers != 2 {
		t.Fatalf("Status() = %+v, want one error followed by one successful scale", status)
	}
	if len(source.violations) != 1 || !strings.Contains(source.violations[0], "quota exceeded") {
		t.Fatalf("violations = %v, want fleet error", source.violations)
	}
}

func TestControllerRunLogsFailedScaleAsNotApplied(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active: []string{"worker-a"},
		windows: []ArrivalWindow{
			{Admitted: 400, Complete: true},
			{Admitted: 400, Complete: true},
		},
	}
	manager := &fleetManagerStub{scaleErrorFor: map[int][]error{2: {errors.New("quota exceeded")}}}
	var logs bytes.Buffer
	controller, err := newController(manager, source, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       1,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	}, controllerDependencies{
		clock:  &scriptedControllerClock{now: started, stopAfter: 2},
		logger: log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatalf("newController() error = %v", err)
	}

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(15 * time.Second)}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := logs.String(); !strings.Contains(got, `scale=false reason="scheduler coverage requires scale up"`) {
		t.Fatalf("controller log = %q, want failed scale reported as not applied", got)
	}
}

func TestControllerRunImmediatelyReplacesPreviouslyRegisteredWorkerBeforeScaling(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active:  []string{"worker-a", "worker-b"},
		windows: []ArrivalWindow{{Admitted: 100, Complete: true}},
	}
	clock := &scriptedControllerClock{now: started, stopAfter: 1}
	manager := &fleetManagerStub{workers: []fleet.WorkerInfo{
		{WorkerID: "worker-a", State: fleet.WorkerStateRunning},
		{WorkerID: "worker-b", State: fleet.WorkerStateRunning},
	}}
	controller := mustController(t, manager, source, clock, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       2,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})
	controller.state.DeficientIntervals = 1
	if err := controller.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	source.setActive("worker-a")

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(10 * time.Second)}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	status := controller.Status()
	if status.DesiredWorkers != 2 || status.RegisteredWorkers != 1 || status.ScaleActions != 0 || status.DeficientIntervals != 0 {
		t.Fatalf("Status() = %+v, want replacement wait without capacity scale", status)
	}
	if !reflect.DeepEqual(manager.replaceCalls, []string{"worker-b"}) {
		t.Fatalf("Replace() calls = %v, want [worker-b]", manager.replaceCalls)
	}
	if !reflect.DeepEqual(manager.calls, []int{2}) {
		t.Fatalf("Scale() calls = %v, want only initial Prepare scale", manager.calls)
	}
}

func TestControllerRunGivesNeverRegisteredWorkerStartupTimeoutAndRestartsGraceAfterReplacement(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active: []string{"worker-a"},
		windows: []ArrivalWindow{
			{Admitted: 100, Complete: true},
			{Admitted: 100, Complete: true},
			{Admitted: 100, Complete: true},
			{Admitted: 100, Complete: true},
		},
	}
	clock := &scriptedControllerClock{now: started, stopAfter: 4}
	manager := &fleetManagerStub{workers: []fleet.WorkerInfo{
		{WorkerID: "worker-a", State: fleet.WorkerStateRunning},
		{WorkerID: "worker-b", State: fleet.WorkerStateRunning},
	}}
	controller := mustController(t, manager, source, clock, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       2,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   10 * time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(25 * time.Second)}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(manager.replaceCalls, []string{"worker-b"}) {
		t.Fatalf("Replace() calls = %v, want one replacement after startup timeout", manager.replaceCalls)
	}
	if !reflect.DeepEqual(manager.calls, []int{2, 2, 2}) {
		t.Fatalf("Scale() calls = %v, want grace at 5s and 10s plus fresh grace after replacement", manager.calls)
	}
}

func TestControllerRunGivesRecreatedStableIDFreshStartupGrace(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active: []string{"worker-a", "worker-b"},
		windows: []ArrivalWindow{
			{Admitted: 100, Complete: true},
			{Admitted: 100, Complete: true},
		},
	}
	manager := &fleetManagerStub{workers: []fleet.WorkerInfo{
		{WorkerID: "worker-a", State: fleet.WorkerStateRunning},
		{WorkerID: "worker-b", State: fleet.WorkerStateRunning},
	}}
	controller := mustController(t, manager, source, &scriptedControllerClock{now: started, stopAfter: 2}, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       2,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   10 * time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})
	if err := controller.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	source.setActive("worker-a")
	manager.workers = manager.workers[:1]
	manager.onScale = func(desired int) {
		if desired == 2 && len(manager.workers) == 1 {
			manager.workers = append(manager.workers, fleet.WorkerInfo{WorkerID: "worker-b", State: fleet.WorkerStateRunning})
		}
	}

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(15 * time.Second)}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(manager.replaceCalls) != 0 {
		t.Fatalf("Replace() calls = %v, want recreated worker startup grace", manager.replaceCalls)
	}
}

func TestControllerRunMarksIncompleteWindowAndSkipsPolicy(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	boundary := started.Add(5 * time.Second)
	source := &controllerSourceStub{
		active:  []string{"worker-a"},
		windows: []ArrivalWindow{{Admitted: 400, Complete: false}},
	}
	manager := &fleetManagerStub{}
	clock := &scriptedControllerClock{now: started, stopAfter: 1}
	var logs bytes.Buffer
	controller, err := newController(manager, source, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       1,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	}, controllerDependencies{
		clock:  clock,
		logger: log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatalf("newController() error = %v", err)
	}
	controller.state.DeficientIntervals = 1

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: boundary}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(manager.calls, []int{1}) {
		t.Fatalf("Scale() calls = %v, want reconciliation only", manager.calls)
	}
	status := controller.Status()
	if status.WindowComplete || status.DeficientIntervals != 0 || status.ScaleActions != 0 {
		t.Fatalf("Status() = %+v, want incomplete window with reset deficit streak", status)
	}
	if !strings.Contains(logs.String(), "complete=false") {
		t.Fatalf("controller log = %q, want incomplete-window status", logs.String())
	}
}

func TestControllerRunRetriesNextBoundaryWithoutAttributingIncompleteWindow(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active: []string{"worker-a"},
		windows: []ArrivalWindow{
			{Admitted: 400, Complete: false},
			{Admitted: 400, Complete: true},
		},
	}
	manager := &fleetManagerStub{}
	controller := mustController(t, manager, source, &scriptedControllerClock{now: started, stopAfter: 2}, ControllerOptions{
		Autoscale:        true,
		MinWorkers:       1,
		MaxWorkers:       4,
		TargetRate:       100,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(4),
	})

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(10 * time.Second)}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(manager.calls, []int{1, 1}) {
		t.Fatalf("Scale() calls = %v, want reconciliation without capacity scale", manager.calls)
	}
	status := controller.Status()
	if !status.WindowComplete || status.DeficientIntervals != 1 || status.ScaleActions != 0 {
		t.Fatalf("Status() = %+v, want one complete deficient interval", status)
	}
	wantRequests := []arrivalWindowRequest{
		{from: started, to: started.Add(5 * time.Second)},
		{from: started.Add(5 * time.Second), to: started.Add(10 * time.Second)},
	}
	if !reflect.DeepEqual(source.windowRequests, wantRequests) {
		t.Fatalf("ArrivalWindow() requests = %v, want adjacent exact windows %v", source.windowRequests, wantRequests)
	}
}

func TestControllerSupportsFixedManagedFleetWithoutRate(t *testing.T) {
	t.Parallel()

	started := time.Unix(100, 0)
	source := &controllerSourceStub{
		active:  []string{"worker-a", "worker-b"},
		windows: []ArrivalWindow{{Complete: true}},
	}
	manager := &fleetManagerStub{}
	controller := mustController(t, manager, source, &scriptedControllerClock{now: started}, ControllerOptions{
		Autoscale:        false,
		MinWorkers:       2,
		MaxWorkers:       2,
		TargetRate:       0,
		StartupTimeout:   time.Second,
		EvaluationPeriod: 5 * time.Second,
		Policy:           testPolicyConfigWithMaximum(2),
	})

	if err := controller.Run(context.Background(), RunWindow{StartsAt: started, Deadline: started.Add(5 * time.Second)}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(manager.calls, []int{2}) {
		t.Fatalf("Scale() calls = %v, want fixed-capacity reconciliation [2]", manager.calls)
	}
}

func mustController(
	t *testing.T,
	manager fleet.FleetManager,
	source Source,
	clock controllerClock,
	options ControllerOptions,
) *Controller {
	t.Helper()
	controller, err := newController(manager, source, options, controllerDependencies{
		clock:  clock,
		logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("newController() error = %v", err)
	}
	return controller
}

func testPolicyConfigWithMaximum(maximum int) PolicyConfig {
	config := testPolicyConfig()
	config.MaxWorkers = maximum
	return config
}

type arrivalWindowRequest struct {
	from time.Time
	to   time.Time
}

type controllerSourceStub struct {
	mu             sync.Mutex
	active         []string
	windows        []ArrivalWindow
	windowRequests []arrivalWindowRequest
	violations     []string
}

func (s *controllerSourceStub) ActiveWorkerIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.active...)
}
func (s *controllerSourceStub) ArrivalWindow(from, to time.Time) ArrivalWindow {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windowRequests = append(s.windowRequests, arrivalWindowRequest{from: from, to: to})
	if len(s.windows) == 0 {
		return ArrivalWindow{Complete: true}
	}
	window := s.windows[0]
	s.windows = s.windows[1:]
	return window
}
func (s *controllerSourceStub) RecordInfrastructureViolation(violation string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.violations = append(s.violations, violation)
}
func (s *controllerSourceStub) setActive(workerIDs ...string) {
	s.mu.Lock()
	s.active = append([]string(nil), workerIDs...)
	s.mu.Unlock()
}

type fleetManagerStub struct {
	calls         []int
	replaceCalls  []string
	workers       []fleet.WorkerInfo
	errors        []error
	scaleErrorFor map[int][]error
	onScale       func(int)
}

func (m *fleetManagerStub) Replace(_ context.Context, workerID string) error {
	m.replaceCalls = append(m.replaceCalls, workerID)
	return nil
}

func (m *fleetManagerStub) Scale(_ context.Context, desired int) error {
	m.calls = append(m.calls, desired)
	if len(m.errors) > 0 {
		err := m.errors[0]
		m.errors = m.errors[1:]
		return err
	}
	if errorsForDesired := m.scaleErrorFor[desired]; len(errorsForDesired) > 0 {
		err := errorsForDesired[0]
		m.scaleErrorFor[desired] = errorsForDesired[1:]
		if err != nil {
			return err
		}
	}
	if m.onScale != nil {
		m.onScale(desired)
	}
	return nil
}

func (m *fleetManagerStub) List(context.Context) ([]fleet.WorkerInfo, error) {
	return append([]fleet.WorkerInfo(nil), m.workers...), nil
}
func (m *fleetManagerStub) Teardown(context.Context) error { return nil }

type scriptedControllerClock struct {
	now       time.Time
	waits     int
	stopAfter int
	onWait    func(time.Time, int) bool
}

func (c *scriptedControllerClock) Now() time.Time { return c.now }

func (c *scriptedControllerClock) WaitUntil(_ context.Context, deadline time.Time) bool {
	if !deadline.After(c.now) {
		return true
	}
	if c.stopAfter > 0 && c.waits >= c.stopAfter {
		return false
	}
	c.now = deadline
	c.waits++
	if c.onWait != nil {
		return c.onWait(deadline, c.waits)
	}
	return true
}

func countValue(values []int, want int) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}
