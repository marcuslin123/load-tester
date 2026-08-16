package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRunControllerSendsPeriodicAndFinalDeltas(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbound := make(chan *loadtestv1.WorkerMessage, 16)
	controller, err := newRunController(ctx, "worker-a", outbound, runControllerOptions{
		ReportingInterval: 20 * time.Millisecond,
		MetricsBufferSize: 1024,
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()
	startsAt := time.Now().Add(15 * time.Millisecond)
	deadline := startsAt.Add(55 * time.Millisecond)
	if err := controller.Apply(controllerAssignment(target.URL, "run-13", 1, 1, startsAt, deadline)); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	var deltas []*loadtestv1.MetricsDelta
	for len(deltas) < 3 {
		select {
		case message := <-outbound:
			if delta := message.GetMetrics(); delta != nil {
				deltas = append(deltas, delta)
			}
		case <-time.After(time.Second):
			t.Fatalf("received %d deltas, want 3", len(deltas))
		}
	}
	for index, delta := range deltas {
		if delta.GetSequence() != uint64(index+1) {
			t.Errorf("delta %d sequence = %d, want %d", index, delta.GetSequence(), index+1)
		}
		if delta.GetAssignmentRevision() != 1 {
			t.Errorf("delta %d revision = %d, want 1", index, delta.GetAssignmentRevision())
		}
	}
	if !deltas[2].GetIntervalEnd().AsTime().Equal(deadline) {
		t.Errorf("final interval end = %v, want deadline %v", deltas[2].GetIntervalEnd().AsTime(), deadline)
	}
	var requests uint64
	for _, delta := range deltas {
		requests += delta.GetCounters().GetRequests()
	}
	if requests == 0 {
		t.Fatal("total requests = 0, want generated traffic")
	}
}

func TestRunControllerContinuesNegotiatedMetricSequence(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	outbound := make(chan *loadtestv1.WorkerMessage, 4)
	controller, err := newRunController(context.Background(), "worker-a", outbound, runControllerOptions{
		ReportingInterval: 10 * time.Millisecond,
		MetricsBufferSize: 64,
		InitialSequence:   42,
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()
	startsAt := time.Now().Add(5 * time.Millisecond)
	deadline := startsAt.Add(15 * time.Millisecond)
	if err := controller.Apply(controllerAssignment(target.URL, "run-14", 1, 1, startsAt, deadline)); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	delta := receiveControllerDelta(t, outbound)
	if delta.GetSequence() != 42 {
		t.Fatalf("first resumed sequence = %d, want 42", delta.GetSequence())
	}
}

func TestRunControllerFlushesOldRevisionBeforeReplacement(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbound := make(chan *loadtestv1.WorkerMessage, 16)
	controller, err := newRunController(ctx, "worker-a", outbound, runControllerOptions{
		ReportingInterval: time.Second,
		MetricsBufferSize: 1024,
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()
	startsAt := time.Now().Add(10 * time.Millisecond)
	deadline := startsAt.Add(140 * time.Millisecond)
	first := controllerAssignment(target.URL, "run-13", 1, 1, startsAt, deadline)
	if err := controller.Apply(first); err != nil {
		t.Fatalf("Apply(revision 1) error = %v", err)
	}
	time.Sleep(45 * time.Millisecond)
	second := proto.Clone(first).(*loadtestv1.LoadAssignment)
	second.Revision = 2
	second.Load.VirtualUsers = 2
	second.ActivatesAt = timestamppb.New(time.Now().Add(20 * time.Millisecond))
	if err := controller.Apply(second); err != nil {
		t.Fatalf("Apply(revision 2) error = %v", err)
	}

	firstDelta := receiveControllerDelta(t, outbound)
	secondDelta := receiveControllerDelta(t, outbound)
	if firstDelta.GetAssignmentRevision() != 1 || firstDelta.GetSequence() != 1 {
		t.Fatalf("first delta revision/sequence = (%d, %d), want (1, 1)", firstDelta.GetAssignmentRevision(), firstDelta.GetSequence())
	}
	if secondDelta.GetAssignmentRevision() != 2 || secondDelta.GetSequence() != 2 {
		t.Fatalf("second delta revision/sequence = (%d, %d), want (2, 2)", secondDelta.GetAssignmentRevision(), secondDelta.GetSequence())
	}
	if !secondDelta.GetIntervalEnd().AsTime().Equal(deadline) {
		t.Errorf("replacement final interval end = %v, want %v", secondDelta.GetIntervalEnd().AsTime(), deadline)
	}
}

func TestRunControllerKeepsZeroLoadAssignmentIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbound := make(chan *loadtestv1.WorkerMessage, 1)
	controller, err := newRunController(ctx, "worker-a", outbound, runControllerOptions{
		ReportingInterval: 10 * time.Millisecond,
		MetricsBufferSize: 8,
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()
	startsAt := time.Now().Add(5 * time.Millisecond)
	deadline := startsAt.Add(25 * time.Millisecond)
	if err := controller.Apply(controllerAssignment("http://target.invalid", "run-13", 1, 0, startsAt, deadline)); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	time.Sleep(45 * time.Millisecond)
	select {
	case message := <-outbound:
		t.Fatalf("zero-load worker sent %T, want no metric delta", message.GetPayload())
	default:
	}
	status := controller.Status()
	if status.RunID != "run-13" || status.Revision != 1 || status.State != loadtestv1.WorkerState_WORKER_STATE_IDLE {
		t.Fatalf("status = %+v, want idle run-13 revision 1", status)
	}
}

func TestRunControllerRejectsNilAssignment(t *testing.T) {
	controller, err := newRunController(
		context.Background(),
		"worker-a",
		make(chan *loadtestv1.WorkerMessage, 1),
		runControllerOptions{},
	)
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()

	if err := controller.Apply(nil); err == nil {
		t.Fatal("Apply(nil) error = nil, want validation error")
	}
}

func TestOpenOptionsForLoadCarriesGlobalScheduleMetadata(t *testing.T) {
	t.Parallel()

	load := &loadtestv1.LoadSlice{
		Rate:               2,
		GlobalRate:         5,
		ScheduleShardIndex: 1,
		ScheduleShardCount: 3,
		MaxInFlight:        4,
		RampUp:             durationpb.New(2 * time.Second),
	}
	got := openOptionsForLoad(load, time.Unix(1_700_000_000, 0))
	if got.Rate != 2 || got.GlobalRate != 5 || got.ScheduleShardIndex != 1 || got.ScheduleShardCount != 3 {
		t.Fatalf("open options schedule = %+v, want local rate=2 global rate=5 shard=1/3", got)
	}
}

func TestRunControllersActivateStaggeredRevisionAtomically(t *testing.T) {
	clock := newFakeControllerClock(time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC).Add(-time.Second))
	startsAt := clock.Now().Add(time.Second)
	activation := startsAt.Add(time.Second)
	deadline := startsAt.Add(2 * time.Second)
	recorder := &scheduleRecorder{clock: clock}

	workerA, outboundA := newFakeTimeRunController(t, "worker-a", clock, recorder)
	defer workerA.Close()
	workerB, outboundB := newFakeTimeRunController(t, "worker-b", clock, recorder)
	defer workerB.Close()

	first := openControllerAssignment("run-atomic", 1, startsAt, startsAt, deadline, 0, 1)
	if err := workerA.Apply(first); err != nil {
		t.Fatalf("worker A Apply(revision 1) error = %v", err)
	}
	clock.Advance(startsAt)
	waitForControllerRevision(t, workerA, 1)

	clock.Advance(startsAt.Add(500 * time.Millisecond))
	secondA := openControllerAssignment("run-atomic", 2, startsAt, activation, deadline, 0, 2)
	if err := workerA.Apply(secondA); err != nil {
		t.Fatalf("worker A Apply(revision 2) error = %v", err)
	}
	clock.Advance(startsAt.Add(750 * time.Millisecond))
	secondB := openControllerAssignment("run-atomic", 2, startsAt, activation, deadline, 1, 2)
	if err := workerB.Apply(secondB); err != nil {
		t.Fatalf("worker B Apply(revision 2) error = %v", err)
	}
	if got := workerA.Status().Revision; got != 1 {
		t.Fatalf("worker A revision before activation = %d, want 1", got)
	}
	if got := workerB.Status().Revision; got != 0 {
		t.Fatalf("worker B revision before activation = %d, want 0", got)
	}

	clock.Advance(activation)
	waitForControllerRevision(t, workerA, 2)
	waitForControllerRevision(t, workerB, 2)
	clock.Advance(deadline)

	deltasA := receiveControllerDeltas(t, outboundA, 2)
	sort.Slice(deltasA, func(i, j int) bool {
		return deltasA[i].GetAssignmentRevision() < deltasA[j].GetAssignmentRevision()
	})
	assertDeltaWindow(t, deltasA[0], 1, startsAt, activation)
	assertDeltaWindow(t, deltasA[1], 2, activation, deadline)
	deltasB := receiveControllerDeltas(t, outboundB, 1)
	assertDeltaWindow(t, deltasB[0], 2, activation, deadline)
	waitForControllerIdle(t, workerA)
	waitForControllerIdle(t, workerB)

	if got, want := recorder.Sequences(), []int64{0, 1, 2, 3, 4, 5, 6, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fleet sequence arrivals = %v, want %v without duplicates or gaps", got, want)
	}
}

func TestRunControllerPreservesEveryPendingRevisionInActivationOrder(t *testing.T) {
	clock := newFakeControllerClock(time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC).Add(-time.Second))
	startsAt := clock.Now().Add(time.Second)
	secondActivation := startsAt.Add(time.Second)
	thirdActivation := startsAt.Add(2 * time.Second)
	deadline := startsAt.Add(3 * time.Second)
	recorder := &scheduleRecorder{clock: clock}
	controller, outbound := newFakeTimeRunController(t, "worker-a", clock, recorder)
	defer controller.Close()

	first := openControllerAssignment("run-queued", 1, startsAt, startsAt, deadline, 0, 1)
	if err := controller.Apply(first); err != nil {
		t.Fatalf("Apply(revision 1) error = %v", err)
	}
	clock.Advance(startsAt)
	waitForControllerRevision(t, controller, 1)

	second := openControllerAssignment("run-queued", 2, startsAt, secondActivation, deadline, 0, 1)
	if err := controller.Apply(second); err != nil {
		t.Fatalf("Apply(revision 2) error = %v", err)
	}
	third := openControllerAssignment("run-queued", 3, startsAt, thirdActivation, deadline, 0, 1)
	if err := controller.Apply(third); err != nil {
		t.Fatalf("Apply(revision 3) error = %v", err)
	}

	clock.Advance(secondActivation)
	waitForControllerRevision(t, controller, 2)
	clock.Advance(thirdActivation)
	waitForControllerRevision(t, controller, 3)
	clock.Advance(deadline)
	waitForControllerIdle(t, controller)

	deltas := receiveControllerDeltas(t, outbound, 3)
	sort.Slice(deltas, func(i, j int) bool {
		return deltas[i].GetAssignmentRevision() < deltas[j].GetAssignmentRevision()
	})
	assertDeltaWindow(t, deltas[0], 1, startsAt, secondActivation)
	assertDeltaWindow(t, deltas[1], 2, secondActivation, thirdActivation)
	assertDeltaWindow(t, deltas[2], 3, thirdActivation, deadline)
	if got, want := recorder.Sequences(), []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}; !reflect.DeepEqual(got, want) {
		t.Fatalf("scheduled sequences = %v, want %v without duplicates or gaps", got, want)
	}
}

func TestRunControllerKeepsPendingRevisionRedeliveryStrict(t *testing.T) {
	clock := newFakeControllerClock(time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC).Add(-time.Second))
	startsAt := clock.Now().Add(time.Second)
	deadline := startsAt.Add(3 * time.Second)
	controller, _ := newFakeTimeRunController(t, "worker-a", clock, &scheduleRecorder{clock: clock})
	defer controller.Close()

	first := openControllerAssignment("run-redelivery", 1, startsAt, startsAt, deadline, 0, 1)
	if err := controller.Apply(first); err != nil {
		t.Fatalf("Apply(revision 1) error = %v", err)
	}
	clock.Advance(startsAt)
	waitForControllerRevision(t, controller, 1)

	second := openControllerAssignment("run-redelivery", 2, startsAt, startsAt.Add(time.Second), deadline, 0, 1)
	if err := controller.Apply(second); err != nil {
		t.Fatalf("Apply(revision 2) error = %v", err)
	}
	if err := controller.Apply(second); err != nil {
		t.Fatalf("Apply(exact revision 2 redelivery) error = %v, want idempotent success", err)
	}
	conflict := proto.Clone(second).(*loadtestv1.LoadAssignment)
	conflict.Load.Rate++
	if err := controller.Apply(conflict); err == nil {
		t.Fatal("Apply(conflicting revision 2) error = nil, want rejection")
	}
}

func TestRunControllerRejectsInvalidAndNonIncreasingActivation(t *testing.T) {
	startsAt := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	deadline := startsAt.Add(10 * time.Second)
	tests := []struct {
		name   string
		mutate func(*loadtestv1.LoadAssignment)
	}{
		{name: "missing", mutate: func(a *loadtestv1.LoadAssignment) { a.ActivatesAt = nil }},
		{name: "before run", mutate: func(a *loadtestv1.LoadAssignment) { a.ActivatesAt = timestamppb.New(startsAt.Add(-time.Nanosecond)) }},
		{name: "initial after start", mutate: func(a *loadtestv1.LoadAssignment) { a.ActivatesAt = timestamppb.New(startsAt.Add(time.Second)) }},
		{name: "at deadline", mutate: func(a *loadtestv1.LoadAssignment) { a.ActivatesAt = timestamppb.New(deadline) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller, err := newRunController(context.Background(), "worker-a", make(chan *loadtestv1.WorkerMessage, 1), runControllerOptions{})
			if err != nil {
				t.Fatalf("newRunController() error = %v", err)
			}
			defer controller.Close()
			assignment := controllerAssignment("http://target.invalid", "run-validation", 1, 0, startsAt, deadline)
			test.mutate(assignment)
			if err := controller.Apply(assignment); err == nil {
				t.Fatal("Apply() error = nil, want invalid activation error")
			}
		})
	}

	clock := newFakeControllerClock(startsAt.Add(-time.Second))
	controller, _ := newFakeTimeRunController(t, "worker-a", clock, &scheduleRecorder{clock: clock})
	defer controller.Close()
	first := openControllerAssignment("run-validation", 1, startsAt, startsAt, deadline, 0, 1)
	if err := controller.Apply(first); err != nil {
		t.Fatalf("Apply(revision 1) error = %v", err)
	}
	second := openControllerAssignment("run-validation", 2, startsAt, startsAt, deadline, 0, 1)
	if err := controller.Apply(second); err == nil {
		t.Fatal("Apply(non-increasing activation) error = nil, want protocol error")
	}
}

func TestRunControllerFlushesRevisionAtActivationBoundary(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	now := time.Now()
	outbound := make(chan *loadtestv1.WorkerMessage, 4)
	controller, err := newRunController(context.Background(), "worker-a", outbound, runControllerOptions{
		ReportingInterval: 2 * time.Second,
		MetricsBufferSize: 64,
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()
	startsAt := now.Add(-time.Second)
	deadline := now.Add(time.Second)
	first := controllerAssignment(target.URL, "run-13", 1, 1, startsAt, deadline)
	if err := controller.Apply(first); err != nil {
		t.Fatalf("Apply(revision 1) error = %v", err)
	}
	waitForControllerRevision(t, controller, 1)
	second := proto.Clone(first).(*loadtestv1.LoadAssignment)
	second.Revision = 2
	activation := now.Add(50 * time.Millisecond)
	second.ActivatesAt = timestamppb.New(activation)
	if err := controller.Apply(second); err != nil {
		t.Fatalf("Apply(revision 2) error = %v", err)
	}

	delta := receiveControllerDelta(t, outbound)
	if got := delta.GetIntervalEnd().AsTime(); !got.Equal(activation) {
		t.Fatalf("old revision interval end = %v, want activation boundary %v", got, activation)
	}
}

func TestRunControllerLetsAdmittedRequestFinishAcrossRevisionActivation(t *testing.T) {
	requestStarted := make(chan struct{}, 1)
	releaseRequest := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRequest) }) }
	var canceledRequests atomic.Uint64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requestStarted <- struct{}{}:
		default:
		}
		select {
		case <-releaseRequest:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			canceledRequests.Add(1)
		}
	}))
	defer target.Close()

	outbound := make(chan *loadtestv1.WorkerMessage, 8)
	controller, err := newRunController(context.Background(), "worker-a", outbound, runControllerOptions{
		ReportingInterval: 100 * time.Millisecond,
		MetricsBufferSize: 64,
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()
	defer release()

	startsAt := time.Now().Add(10 * time.Millisecond)
	activation := startsAt.Add(80 * time.Millisecond)
	deadline := startsAt.Add(500 * time.Millisecond)
	first := openControllerAssignment("run-drain", 1, startsAt, startsAt, deadline, 0, 1)
	first.Target.GetHttp().Url = target.URL
	first.Load.Rate = 100
	first.Load.GlobalRate = 100
	first.Load.MaxInFlight = 1
	if err := controller.Apply(first); err != nil {
		t.Fatalf("Apply(revision 1) error = %v", err)
	}
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for admitted request")
	}

	second := proto.Clone(first).(*loadtestv1.LoadAssignment)
	second.Revision = 2
	second.ActivatesAt = timestamppb.New(activation)
	if err := controller.Apply(second); err != nil {
		t.Fatalf("Apply(revision 2) error = %v", err)
	}
	activationDeadline := activation.Add(150 * time.Millisecond)
	for time.Now().Before(activationDeadline) && controller.Status().Revision != 2 {
		time.Sleep(time.Millisecond)
	}
	if got := controller.Status().Revision; got != 2 {
		t.Fatalf("revision while old request remains blocked = %d, want 2 activated by %v", got, activationDeadline)
	}
	firstDelta := receiveControllerDelta(t, outbound)
	if got := firstDelta.GetAssignmentRevision(); got != 2 {
		t.Fatalf("first emitted revision = %d, want ready revision 2 while revision 1 remains blocked", got)
	}
	if firstDelta.GetSequence() != 1 || firstDelta.GetFinalForRevision() {
		t.Fatalf("revision 2 periodic wire state = sequence %d final=%t, want sequence 1 non-final",
			firstDelta.GetSequence(), firstDelta.GetFinalForRevision())
	}
	release()

	secondDelta := receiveControllerDelta(t, outbound)
	if got := canceledRequests.Load(); got != 0 {
		t.Fatalf("requests canceled at revision activation = %d, want 0", got)
	}
	if got := secondDelta.GetAssignmentRevision(); got != 1 {
		t.Fatalf("second emitted revision = %d, want draining revision 1 after ready revision 2", got)
	}
	if secondDelta.GetSequence() != 2 || !secondDelta.GetFinalForRevision() ||
		!secondDelta.GetIntervalEnd().AsTime().Equal(activation) {
		t.Fatalf("revision 1 wire boundary = sequence %d final=%t end=%v, want sequence 2 final at %v",
			secondDelta.GetSequence(), secondDelta.GetFinalForRevision(), secondDelta.GetIntervalEnd().AsTime(), activation)
	}
	if got := secondDelta.GetCounters().GetRequests(); got != 1 {
		t.Fatalf("completed requests in revision 1 delta = %d, want 1", got)
	}
	select {
	case err := <-controller.Errors():
		t.Fatalf("controller error after asynchronous revision drain = %v", err)
	default:
	}
}

func TestRunControllerKeepsNaturallyCompletedCountersInCurrentInterval(t *testing.T) {
	startsAt := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	boundary := startsAt.Add(time.Second)
	deadline := startsAt.Add(2 * time.Second)
	clock := newFakeControllerClock(startsAt.Add(-time.Second))
	outbound := make(chan *loadtestv1.WorkerMessage, 4)
	controller, err := newRunController(context.Background(), "worker-a", outbound, runControllerOptions{
		ReportingInterval: time.Second,
		MetricsBufferSize: 8,
		Now:               clock.Now,
		NewTimer:          clock.NewTimer,
		StartScheduler: func(_ *loadtestv1.LoadAssignment, collector *metrics.Collector) (context.CancelFunc, <-chan error, error) {
			collector.OfferAdmittedArrival()
			collector.Offer(protocol.Result{StatusCode: http.StatusOK, Latency: 10 * time.Millisecond})
			done := make(chan error, 1)
			done <- nil
			return func() {}, done, nil
		},
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()

	assignment := controllerAssignment("http://target.invalid", "run-natural", 1, 1, startsAt, deadline)
	if err := controller.Apply(assignment); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	clock.Advance(startsAt)
	waitForControllerRevision(t, controller, 1)
	time.Sleep(20 * time.Millisecond)
	clock.Advance(boundary)

	delta := receiveControllerDelta(t, outbound)
	if delta.GetSequence() != 1 {
		t.Fatalf("first interval sequence = %d, want 1", delta.GetSequence())
	}
	if got := delta.GetCounters().GetRequests(); got != 1 {
		t.Fatalf("requests in first reporting interval = %d, want naturally completed request", got)
	}
	if !delta.GetIntervalStart().AsTime().Equal(startsAt) || !delta.GetIntervalEnd().AsTime().Equal(boundary) {
		t.Fatalf("request interval = [%v, %v), want [%v, %v)",
			delta.GetIntervalStart().AsTime(), delta.GetIntervalEnd().AsTime(), startsAt, boundary)
	}
	waitForControllerTimer(t, clock, deadline)
	clock.Advance(deadline)
	finalDelta := receiveControllerDelta(t, outbound)
	if finalDelta.GetSequence() != 2 {
		t.Fatalf("final interval sequence = %d, want 2", finalDelta.GetSequence())
	}
	if got := finalDelta.GetCounters().GetRequests(); got != 0 {
		t.Fatalf("requests repeated in later final interval = %d, want 0", got)
	}
}

func controllerAssignment(
	url string,
	runID string,
	revision uint64,
	virtualUsers uint64,
	startsAt time.Time,
	deadline time.Time,
) *loadtestv1.LoadAssignment {
	return &loadtestv1.LoadAssignment{
		RunId:    runID,
		Revision: revision,
		Target: &loadtestv1.Target{Protocol: &loadtestv1.Target_Http{
			Http: &loadtestv1.HttpTarget{Url: url, Method: http.MethodGet},
		}},
		Load: &loadtestv1.LoadSlice{
			Model:        loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS,
			VirtualUsers: virtualUsers,
			RampUp:       durationpb.New(0),
		},
		StartsAt:    timestamppb.New(startsAt),
		Deadline:    timestamppb.New(deadline),
		ActivatesAt: timestamppb.New(startsAt),
	}
}

func openControllerAssignment(
	runID string,
	revision uint64,
	startsAt time.Time,
	activatesAt time.Time,
	deadline time.Time,
	shardIndex uint64,
	shardCount uint64,
) *loadtestv1.LoadAssignment {
	return &loadtestv1.LoadAssignment{
		RunId:    runID,
		Revision: revision,
		Target: &loadtestv1.Target{Protocol: &loadtestv1.Target_Http{
			Http: &loadtestv1.HttpTarget{Url: "http://target.invalid", Method: http.MethodGet},
		}},
		Load: &loadtestv1.LoadSlice{
			Model:              loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE,
			Rate:               4 / shardCount,
			GlobalRate:         4,
			ScheduleShardIndex: shardIndex,
			ScheduleShardCount: shardCount,
			MaxInFlight:        4 / shardCount,
			RampUp:             durationpb.New(0),
		},
		StartsAt:    timestamppb.New(startsAt),
		Deadline:    timestamppb.New(deadline),
		ActivatesAt: timestamppb.New(activatesAt),
	}
}

type fakeControllerTimer struct {
	at      time.Time
	ch      chan time.Time
	mu      sync.Mutex
	stopped bool
}

func (t *fakeControllerTimer) C() <-chan time.Time { return t.ch }

func (t *fakeControllerTimer) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

func (t *fakeControllerTimer) fire() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	t.mu.Unlock()
	t.ch <- t.at
}

type fakeControllerClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeControllerTimer
}

func newFakeControllerClock(now time.Time) *fakeControllerClock {
	return &fakeControllerClock{now: now}
}

func (c *fakeControllerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeControllerClock) NewTimer(at time.Time) controllerTimer {
	timer := &fakeControllerTimer{at: at, ch: make(chan time.Time, 1)}
	c.mu.Lock()
	c.timers = append(c.timers, timer)
	c.mu.Unlock()
	return timer
}

func (c *fakeControllerClock) Advance(now time.Time) {
	c.mu.Lock()
	c.now = now
	timers := append([]*fakeControllerTimer(nil), c.timers...)
	c.mu.Unlock()
	for _, timer := range timers {
		if !timer.at.After(now) {
			timer.fire()
		}
	}
}

func waitForControllerTimer(t *testing.T, clock *fakeControllerClock, at time.Time) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		clock.mu.Lock()
		found := false
		for _, timer := range clock.timers {
			if timer.at.Equal(at) {
				found = true
				break
			}
		}
		clock.mu.Unlock()
		if found {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("controller timer at %v was not scheduled", at)
}

type scheduleRecorder struct {
	clock     *fakeControllerClock
	mu        sync.Mutex
	sequences []int64
}

func (r *scheduleRecorder) Start(assignment *loadtestv1.LoadAssignment, _ *metrics.Collector) (context.CancelFunc, <-chan error, error) {
	done := make(chan error, 1)
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			started := assignment.GetStartsAt().AsTime()
			from := assignment.GetActivatesAt().AsTime()
			to := r.clock.Now()
			load := assignment.GetLoad()
			period := time.Second / time.Duration(load.GetGlobalRate())
			var sequences []int64
			for sequence := int64(0); ; sequence++ {
				arrival := started.Add(time.Duration(sequence) * period)
				if !arrival.Before(to) {
					break
				}
				if !arrival.Before(from) && uint64(sequence)%load.GetScheduleShardCount() == load.GetScheduleShardIndex() {
					sequences = append(sequences, sequence)
				}
			}
			r.mu.Lock()
			r.sequences = append(r.sequences, sequences...)
			r.mu.Unlock()
			done <- nil
		})
	}
	return cancel, done, nil
}

func (r *scheduleRecorder) Sequences() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := append([]int64(nil), r.sequences...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func newFakeTimeRunController(t *testing.T, workerID string, clock *fakeControllerClock, recorder *scheduleRecorder) (*runController, chan *loadtestv1.WorkerMessage) {
	t.Helper()
	outbound := make(chan *loadtestv1.WorkerMessage, 8)
	controller, err := newRunController(context.Background(), workerID, outbound, runControllerOptions{
		ReportingInterval: 10 * time.Second,
		MetricsBufferSize: 64,
		Now:               clock.Now,
		NewTimer:          clock.NewTimer,
		StartScheduler:    recorder.Start,
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	return controller, outbound
}

func waitForControllerRevision(t *testing.T, controller *runController, revision uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if controller.Status().Revision == revision {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("controller revision = %d, want %d", controller.Status().Revision, revision)
}

func waitForControllerIdle(t *testing.T, controller *runController) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if controller.Status().State == loadtestv1.WorkerState_WORKER_STATE_IDLE {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("controller status = %+v, want idle", controller.Status())
}

func receiveControllerDeltas(t *testing.T, outbound <-chan *loadtestv1.WorkerMessage, count int) []*loadtestv1.MetricsDelta {
	t.Helper()
	deltas := make([]*loadtestv1.MetricsDelta, count)
	for index := range deltas {
		deltas[index] = receiveControllerDelta(t, outbound)
	}
	return deltas
}

func assertDeltaWindow(t *testing.T, delta *loadtestv1.MetricsDelta, revision uint64, start, end time.Time) {
	t.Helper()
	if delta.GetAssignmentRevision() != revision || !delta.GetFinalForRevision() {
		t.Fatalf("delta revision/final = (%d, %t), want (%d, true)", delta.GetAssignmentRevision(), delta.GetFinalForRevision(), revision)
	}
	if got := delta.GetIntervalStart().AsTime(); !got.Equal(start) {
		t.Errorf("delta start = %v, want %v", got, start)
	}
	if got := delta.GetIntervalEnd().AsTime(); !got.Equal(end) {
		t.Errorf("delta end = %v, want %v", got, end)
	}
}

func receiveControllerDelta(t *testing.T, outbound <-chan *loadtestv1.WorkerMessage) *loadtestv1.MetricsDelta {
	t.Helper()
	select {
	case message := <-outbound:
		delta := message.GetMetrics()
		if delta == nil {
			t.Fatalf("outbound message = %T, want metrics delta", message.GetPayload())
		}
		return delta
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for metrics delta")
		return nil
	}
}
