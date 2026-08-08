package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
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

func TestRunControllerTimestampsRevisionAfterOldSchedulerDrains(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	var nowMu sync.Mutex
	now := time.Now()
	clock := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	advanceAfterDrain := func(cancel context.CancelFunc, done <-chan error) {
		stopScheduler(cancel, done)
		nowMu.Lock()
		now = now.Add(25 * time.Millisecond)
		nowMu.Unlock()
	}
	outbound := make(chan *loadtestv1.WorkerMessage, 4)
	controller, err := newRunController(context.Background(), "worker-a", outbound, runControllerOptions{
		ReportingInterval: time.Second,
		MetricsBufferSize: 64,
		Now:               clock,
		StopScheduler:     advanceAfterDrain,
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
	second := proto.Clone(first).(*loadtestv1.LoadAssignment)
	second.Revision = 2
	if err := controller.Apply(second); err != nil {
		t.Fatalf("Apply(revision 2) error = %v", err)
	}

	delta := receiveControllerDelta(t, outbound)
	if got, want := delta.GetIntervalEnd().AsTime(), clock(); !got.Equal(want) {
		t.Fatalf("old revision interval end = %v, want post-drain boundary %v", got, want)
	}
}

func TestRunControllerRejectsOverdueRevisionFlush(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	var nowMu sync.Mutex
	now := time.Now()
	clock := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	advanceAfterDrain := func(cancel context.CancelFunc, done <-chan error) {
		stopScheduler(cancel, done)
		nowMu.Lock()
		now = now.Add(2250 * time.Millisecond)
		nowMu.Unlock()
	}
	outbound := make(chan *loadtestv1.WorkerMessage, 8)
	controller, err := newRunController(context.Background(), "worker-a", outbound, runControllerOptions{
		ReportingInterval: time.Second,
		MetricsBufferSize: 64,
		Now:               clock,
		StopScheduler:     advanceAfterDrain,
	})
	if err != nil {
		t.Fatalf("newRunController() error = %v", err)
	}
	defer controller.Close()
	startsAt := now
	deadline := startsAt.Add(5 * time.Second)
	first := controllerAssignment(target.URL, "run-13", 1, 1, startsAt, deadline)
	if err := controller.Apply(first); err != nil {
		t.Fatalf("Apply(revision 1) error = %v", err)
	}
	second := proto.Clone(first).(*loadtestv1.LoadAssignment)
	second.Revision = 2
	second.Load.VirtualUsers = 0
	if err := controller.Apply(second); err == nil {
		t.Fatal("Apply(overdue revision) error = nil, want reporting-integrity error")
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
		StartsAt: timestamppb.New(startsAt),
		Deadline: timestamppb.New(deadline),
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
