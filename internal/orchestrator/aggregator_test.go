package orchestrator

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMetricsAggregatorMergesWorkerCountersAndHistograms(t *testing.T) {
	started := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	deadline := started.Add(time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
		"worker-b": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	first := metricsDeltaForTest(t, "worker-a", 1, 1, started, deadline, []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200, BytesRead: 10},
		{Latency: 20 * time.Millisecond, StatusCode: 500, BytesRead: 20},
	}, true)
	second := metricsDeltaForTest(t, "worker-b", 1, 1, started, deadline, []protocol.Result{
		{Latency: 100 * time.Millisecond, Err: errors.New("connection reset")},
	}, true)

	if err := aggregator.Accept("worker-a", first); err != nil {
		t.Fatalf("Accept(worker-a) error = %v", err)
	}
	if err := aggregator.Accept("worker-b", second); err != nil {
		t.Fatalf("Accept(worker-b) error = %v", err)
	}
	snapshot := aggregator.Snapshot()
	if snapshot.Requests != 3 || snapshot.Succeeded != 1 || snapshot.Failed != 2 {
		t.Fatalf("merged request counters = %+v, want requests=3 succeeded=1 failed=2", snapshot)
	}
	if snapshot.ServerErrors != 1 || snapshot.TransportErrors != 1 || snapshot.BytesRead != 30 {
		t.Errorf("merged failure/byte counters = %+v, want server=1 transport=1 bytes=30", snapshot)
	}
	if snapshot.StatusCodes[200] != 1 || snapshot.StatusCodes[500] != 1 {
		t.Errorf("merged status codes = %v, want 200=1 500=1", snapshot.StatusCodes)
	}
	if got := snapshot.Percentile(99); got < 99*time.Millisecond || got > 101*time.Millisecond {
		t.Errorf("fleet p99 = %v, want approximately 100ms", got)
	}
	workers := aggregator.WorkerSnapshots()
	if workers["worker-a"].Requests != 2 || workers["worker-b"].Requests != 1 {
		t.Fatalf("worker requests = (a=%d, b=%d), want (2, 1)", workers["worker-a"].Requests, workers["worker-b"].Requests)
	}
	if workers["worker-a"].Requests+workers["worker-b"].Requests != snapshot.Requests {
		t.Fatal("fleet requests do not equal the sum of worker requests")
	}
	if !aggregator.Complete() || len(aggregator.MissingFinal()) != 0 {
		t.Fatalf("completion = %v missing=%v, want complete", aggregator.Complete(), aggregator.MissingFinal())
	}
}

func TestMetricsAggregatorMergesAdmittedArrivals(t *testing.T) {
	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	deadline := started.Add(time.Second)
	aggregator := newMetricsAggregator("run-admitted")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-admitted", 1, 1, started, deadline),
	})
	delta := metricsDeltaForTest(t, "worker-a", 1, 1, started, deadline, []protocol.Result{
		{Latency: time.Millisecond, StatusCode: 200},
	}, true)
	delta.RunId = "run-admitted"
	delta.Counters.AdmittedArrivals = 4

	if err := aggregator.Accept("worker-a", delta); err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if got := aggregator.Snapshot().AdmittedArrivals; got != 4 {
		t.Fatalf("fleet admitted arrivals = %d, want 4", got)
	}
	if got := aggregator.WorkerSnapshots()["worker-a"].AdmittedArrivals; got != 4 {
		t.Fatalf("worker admitted arrivals = %d, want 4", got)
	}
}

func TestMetricsAggregatorPrometheusViewKeepsAcceptedVersionsConsistent(t *testing.T) {
	started := time.Now().UTC().Truncate(time.Second)
	aggregator := newMetricsAggregator("run-view")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-view", 1, 1, started, started.Add(2*time.Second)),
	})
	first := metricsDeltaForTest(
		t,
		"worker-a",
		1,
		1,
		started,
		started.Add(time.Second),
		[]protocol.Result{{Latency: time.Millisecond, StatusCode: 200}},
	)
	first.RunId = "run-view"
	if err := aggregator.Accept("worker-a", first); err != nil {
		t.Fatalf("Accept(first) error = %v", err)
	}
	second := metricsDeltaForTest(
		t,
		"worker-a",
		1,
		2,
		started.Add(time.Second),
		started.Add(2*time.Second),
		[]protocol.Result{{Latency: time.Millisecond, StatusCode: 200}},
		true,
	)
	second.RunId = "run-view"

	aggregator.mu.Lock()
	viewStarted := make(chan struct{})
	acceptStarted := make(chan struct{})
	viewResult := make(chan metrics.PrometheusView, 1)
	acceptResult := make(chan error, 1)
	go func() {
		close(viewStarted)
		viewResult <- aggregator.PrometheusView()
	}()
	go func() {
		close(acceptStarted)
		acceptResult <- aggregator.Accept("worker-a", second)
	}()
	<-viewStarted
	<-acceptStarted
	aggregator.mu.Unlock()

	view := <-viewResult
	if err := <-acceptResult; err != nil {
		t.Fatalf("Accept(second) error = %v", err)
	}
	fleetRequests := view.Fleet.Requests
	workerRequests := view.Workers["worker-a"].Requests
	if fleetRequests != workerRequests {
		t.Fatalf("concurrent PrometheusView() = (fleet=%d worker=%d), want one accepted version", fleetRequests, workerRequests)
	}
	if fleetRequests != 1 && fleetRequests != 2 {
		t.Fatalf("concurrent PrometheusView() version = %d, want 1 or 2", fleetRequests)
	}
}

func TestMetricsAggregatorIgnoresExactDuplicate(t *testing.T) {
	started := time.Now()
	deadline := started.Add(time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	delta := metricsDeltaForTest(t, "worker-a", 1, 1, started, deadline, []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	}, true)
	if err := aggregator.Accept("worker-a", delta); err != nil {
		t.Fatalf("first Accept() error = %v", err)
	}
	if err := aggregator.Accept("worker-a", proto.Clone(delta).(*loadtestv1.MetricsDelta)); err != nil {
		t.Fatalf("duplicate Accept() error = %v", err)
	}
	if got := aggregator.Snapshot().Requests; got != 1 {
		t.Fatalf("requests after duplicate = %d, want 1", got)
	}
}

func TestMetricsAggregatorArrivalWindowReturnsExactCompleteCounters(t *testing.T) {
	started := time.Now()
	boundary := started.Add(time.Second)
	deadline := started.Add(2 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
		"worker-b": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	workerA := metricsDeltaForTest(t, "worker-a", 1, 1, started, boundary, nil)
	workerA.Counters.AdmittedArrivals = 40
	workerA.Counters.UnmetDemand = 3
	workerB := metricsDeltaForTest(t, "worker-b", 1, 1, started, boundary, nil)
	workerB.Counters.AdmittedArrivals = 60
	workerB.Counters.UnmetDemand = 2
	if err := aggregator.Accept("worker-a", workerA); err != nil {
		t.Fatalf("Accept(worker-a) error = %v", err)
	}
	if err := aggregator.Accept("worker-b", workerB); err != nil {
		t.Fatalf("Accept(worker-b) error = %v", err)
	}

	got := aggregator.ArrivalWindow(started, boundary)
	if got.Admitted != 100 || got.Unmet != 5 || !got.Complete {
		t.Fatalf("ArrivalWindow() = %+v, want admitted=100 unmet=5 complete=true", got)
	}
}

func TestMetricsAggregatorArrivalWindowDoesNotShiftLatePriorDelta(t *testing.T) {
	started := time.Now()
	firstBoundary := started.Add(time.Second)
	secondBoundary := started.Add(2 * time.Second)
	deadline := started.Add(3 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})

	if got := aggregator.ArrivalWindow(started, firstBoundary); got.Complete {
		t.Fatalf("first ArrivalWindow() = %+v, want incomplete before worker delta", got)
	}
	late := metricsDeltaForTest(t, "worker-a", 1, 1, started, firstBoundary, nil)
	late.Counters.AdmittedArrivals = 40
	current := metricsDeltaForTest(t, "worker-a", 1, 2, firstBoundary, secondBoundary, nil)
	current.Counters.AdmittedArrivals = 60
	if err := aggregator.Accept("worker-a", late); err != nil {
		t.Fatalf("Accept(late) error = %v", err)
	}
	if err := aggregator.Accept("worker-a", current); err != nil {
		t.Fatalf("Accept(current) error = %v", err)
	}

	got := aggregator.ArrivalWindow(firstBoundary, secondBoundary)
	if got.Admitted != 60 || !got.Complete {
		t.Fatalf("second ArrivalWindow() = %+v, want only current 60 arrivals", got)
	}
}

func TestMetricsAggregatorAcceptsLateFinalFromOlderRevisionWithoutShiftingItsArrivals(t *testing.T) {
	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	activation := started.Add(time.Second)
	secondBoundary := started.Add(2 * time.Second)
	deadline := started.Add(3 * time.Second)
	aggregator := newMetricsAggregator("run-out-of-order-revisions")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-out-of-order-revisions", 1, 1, started, deadline),
	})
	revisionTwo := metricsAssignment("run-out-of-order-revisions", 2, 1, started, deadline)
	revisionTwo.ActivatesAt = timestamppb.New(activation)
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{"worker-a": revisionTwo})

	current := metricsDeltaForTest(t, "worker-a", 2, 1, activation, secondBoundary, []protocol.Result{
		{Latency: 20 * time.Millisecond, StatusCode: 200},
	})
	current.RunId = "run-out-of-order-revisions"
	current.Counters.AdmittedArrivals = 20
	if err := aggregator.Accept("worker-a", current); err != nil {
		t.Fatalf("Accept(revision 2 periodic) error = %v", err)
	}
	if got := aggregator.ArrivalWindow(started, activation); got.Complete {
		t.Fatalf("old ArrivalWindow() = %+v, want incomplete before revision 1 final", got)
	}

	oldFinal := metricsDeltaForTest(t, "worker-a", 1, 2, started, activation, []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	}, true)
	oldFinal.RunId = "run-out-of-order-revisions"
	oldFinal.Counters.AdmittedArrivals = 10
	if err := aggregator.Accept("worker-a", oldFinal); err != nil {
		t.Fatalf("Accept(late revision 1 final) error = %v", err)
	}
	if err := aggregator.Accept("worker-a", proto.Clone(oldFinal).(*loadtestv1.MetricsDelta)); err != nil {
		t.Fatalf("Accept(exact late-final duplicate) error = %v", err)
	}
	conflict := proto.Clone(oldFinal).(*loadtestv1.MetricsDelta)
	conflict.Counters.AdmittedArrivals++
	if err := aggregator.Accept("worker-a", conflict); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("Accept(conflicting late-final duplicate) error = %v, want sequence conflict", err)
	}

	snapshot := aggregator.Snapshot()
	if snapshot.Requests != 2 || snapshot.Succeeded != 2 || snapshot.AdmittedArrivals != 30 {
		t.Fatalf("cumulative snapshot = %+v, want requests=2 succeeded=2 admitted=30", snapshot)
	}
	window := aggregator.ArrivalWindow(activation, secondBoundary)
	if window.Admitted != 20 || window.Unmet != 0 || !window.Complete {
		t.Fatalf("new ArrivalWindow() = %+v, want only revision 2 admitted=20 complete=true", window)
	}
	if aggregator.Complete() {
		t.Fatal("Complete() = true before latest revision final delta")
	}

	latestFinal := metricsDeltaForTest(t, "worker-a", 2, 3, secondBoundary, deadline, nil, true)
	latestFinal.RunId = "run-out-of-order-revisions"
	if err := aggregator.Accept("worker-a", latestFinal); err != nil {
		t.Fatalf("Accept(revision 2 final) error = %v", err)
	}
	if !aggregator.Complete() || len(aggregator.MissingFinal()) != 0 {
		t.Fatalf("completion = %v missing=%v, want latest revision complete", aggregator.Complete(), aggregator.MissingFinal())
	}
}

func TestMetricsAggregatorArrivalWindowIsIncompleteUntilEveryActiveWorkerReportsThroughBoundary(t *testing.T) {
	started := time.Now()
	boundary := started.Add(time.Second)
	deadline := started.Add(2 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a":    metricsAssignment("run-13", 1, 1, started, deadline),
		"worker-b":    metricsAssignment("run-13", 1, 1, started, deadline),
		"worker-idle": metricsAssignment("run-13", 1, 0, started, deadline),
	})
	delta := metricsDeltaForTest(t, "worker-a", 1, 1, started, boundary, nil)
	delta.Counters.AdmittedArrivals = 10
	if err := aggregator.Accept("worker-a", delta); err != nil {
		t.Fatalf("Accept(worker-a) error = %v", err)
	}

	got := aggregator.ArrivalWindow(started, boundary)
	if got.Admitted != 10 || got.Complete {
		t.Fatalf("ArrivalWindow() = %+v, want admitted=10 complete=false", got)
	}
}

func TestMetricsAggregatorArrivalWindowPrunesConsumedRecords(t *testing.T) {
	started := time.Now()
	deadline := started.Add(64 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	for sequence := uint64(1); sequence <= 64; sequence++ {
		intervalStart := started.Add(time.Duration(sequence-1) * time.Second)
		intervalEnd := intervalStart.Add(time.Second)
		delta := metricsDeltaForTest(t, "worker-a", 1, sequence, intervalStart, intervalEnd, nil, sequence == 64)
		delta.Counters.AdmittedArrivals = 1
		if err := aggregator.Accept("worker-a", delta); err != nil {
			t.Fatalf("Accept(sequence=%d) error = %v", sequence, err)
		}
	}

	got := aggregator.ArrivalWindow(started, deadline)
	if got.Admitted != 64 || !got.Complete {
		t.Fatalf("ArrivalWindow() = %+v, want all 64 arrivals", got)
	}
	if len(aggregator.arrivalRecords) != 0 {
		t.Fatalf("arrival record count = %d, want 0 after consumed window", len(aggregator.arrivalRecords))
	}
}

func TestMetricsAggregatorRejectsRevisionPartialThatCrossesReportingBoundary(t *testing.T) {
	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	deadline := started.Add(10 * time.Second)
	activation := started.Add(5300 * time.Millisecond)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	replacement := metricsAssignment("run-13", 2, 1, started, deadline)
	replacement.ActivatesAt = timestamppb.New(activation)
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{"worker-a": replacement})

	delta := metricsDeltaForTest(
		t,
		"worker-a",
		1,
		1,
		started.Add(4800*time.Millisecond),
		activation,
		nil,
		true,
	)
	delta.Counters.AdmittedArrivals = 50
	err := aggregator.Accept("worker-a", delta)
	if err == nil || !strings.Contains(err.Error(), "reporting boundary") {
		t.Fatalf("Accept(crossing partial) error = %v, want reporting-boundary error", err)
	}
	if got := aggregator.Snapshot().AdmittedArrivals; got != 0 {
		t.Fatalf("admitted arrivals after rejected partial = %d, want 0", got)
	}
	if len(aggregator.arrivalRecords) != 0 {
		t.Fatalf("arrival record count = %d, want no partially ingested record", len(aggregator.arrivalRecords))
	}
}

func TestMetricsAggregatorArrivalWindowRequiresWorkerRemovedByFutureRevision(t *testing.T) {
	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	firstBoundary := started.Add(time.Second)
	deadline := started.Add(3 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
		"worker-b": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	future := metricsAssignment("run-13", 2, 1, started, deadline)
	future.ActivatesAt = timestamppb.New(started.Add(2 * time.Second))
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{"worker-a": future})

	workerA := metricsDeltaForTest(t, "worker-a", 1, 1, started, firstBoundary, nil)
	workerA.Counters.AdmittedArrivals = 10
	if err := aggregator.Accept("worker-a", workerA); err != nil {
		t.Fatalf("Accept(worker-a) error = %v", err)
	}

	got := aggregator.ArrivalWindow(started, firstBoundary)
	if got.Admitted != 10 || got.Complete {
		t.Fatalf("ArrivalWindow() = %+v, want admitted=10 complete=false while worker-b is missing", got)
	}
}

func TestMetricsAggregatorArrivalWindowMergesAdjacentRevisionPartialsExactly(t *testing.T) {
	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	boundary := started.Add(5 * time.Second)
	deadline := started.Add(6 * time.Second)
	activation := started.Add(4800 * time.Millisecond)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	replacement := metricsAssignment("run-13", 2, 1, started, deadline)
	replacement.ActivatesAt = timestamppb.New(activation)
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{"worker-a": replacement})

	sequence := uint64(1)
	for intervalStart := started; intervalStart.Before(started.Add(4 * time.Second)); intervalStart = intervalStart.Add(time.Second) {
		delta := metricsDeltaForTest(t, "worker-a", 1, sequence, intervalStart, intervalStart.Add(time.Second), nil)
		delta.Counters.AdmittedArrivals = 10
		if err := aggregator.Accept("worker-a", delta); err != nil {
			t.Fatalf("Accept(revision 1 sequence %d) error = %v", sequence, err)
		}
		sequence++
	}
	oldPartial := metricsDeltaForTest(t, "worker-a", 1, sequence, started.Add(4*time.Second), activation, nil, true)
	oldPartial.Counters.AdmittedArrivals = 8
	if err := aggregator.Accept("worker-a", oldPartial); err != nil {
		t.Fatalf("Accept(old revision partial) error = %v", err)
	}
	sequence++
	newPartial := metricsDeltaForTest(t, "worker-a", 2, sequence, activation, boundary, nil)
	newPartial.Counters.AdmittedArrivals = 2
	if err := aggregator.Accept("worker-a", newPartial); err != nil {
		t.Fatalf("Accept(new revision partial) error = %v", err)
	}

	got := aggregator.ArrivalWindow(started, boundary)
	if got.Admitted != 50 || got.Unmet != 0 || !got.Complete {
		t.Fatalf("ArrivalWindow() = %+v, want admitted=50 unmet=0 complete=true", got)
	}
	if len(aggregator.arrivalRecords) != 0 {
		t.Fatalf("arrival record count = %d, want exact records consumed", len(aggregator.arrivalRecords))
	}
}

func TestMetricsAggregatorRevisionActiveUntilUsesEarliestQueuedActivation(t *testing.T) {
	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	deadline := started.Add(10 * time.Second)
	aggregator := newMetricsAggregator("run-rapid-revisions")

	revisionOne := metricsAssignment("run-rapid-revisions", 1, 1, started, deadline)
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{"worker-a": revisionOne})
	revisionTwo := metricsAssignment("run-rapid-revisions", 2, 1, started, deadline)
	revisionTwo.ActivatesAt = timestamppb.New(started.Add(5 * time.Second))
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{"worker-a": revisionTwo})
	revisionThree := metricsAssignment("run-rapid-revisions", 3, 1, started, deadline)
	revisionThree.ActivatesAt = timestamppb.New(started.Add(6 * time.Second))
	aggregator.RecordAssignments(3, map[string]*loadtestv1.LoadAssignment{"worker-a": revisionThree})

	if got := aggregator.revisionActiveUntil(1, deadline); !got.Equal(started.Add(5 * time.Second)) {
		t.Fatalf("revisionActiveUntil(1) = %v, want first queued activation at +5s", got)
	}
	if got := aggregator.revisionActiveUntil(2, deadline); !got.Equal(started.Add(6 * time.Second)) {
		t.Fatalf("revisionActiveUntil(2) = %v, want next queued activation at +6s", got)
	}
}

func TestMetricsAggregatorRevisionActiveUntilIsIndependentOfWorkerMapIteration(t *testing.T) {
	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	deadline := started.Add(10 * time.Second)
	aggregator := newMetricsAggregator("run-map-order")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-map-order", 1, 1, started, deadline),
	})
	early := metricsAssignment("run-map-order", 2, 1, started, deadline)
	early.ActivatesAt = timestamppb.New(started.Add(5 * time.Second))
	late := metricsAssignment("run-map-order", 2, 1, started, deadline)
	late.ActivatesAt = timestamppb.New(started.Add(7 * time.Second))
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{
		"worker-early": early,
		"worker-late":  late,
	})

	for range 1_000 {
		if got := aggregator.revisionActiveUntil(1, deadline); !got.Equal(started.Add(5 * time.Second)) {
			t.Fatalf("revisionActiveUntil(1) = %v, want deterministic earliest activation at +5s", got)
		}
	}
}

func TestMetricsAggregatorReturnsNextSequenceForStableWorker(t *testing.T) {
	started := time.Now()
	deadline := started.Add(time.Second)
	aggregator := newMetricsAggregator("run-14")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-14", 1, 1, started, deadline),
	})
	delta := metricsDeltaForTest(t, "worker-a", 1, 1, started, deadline, []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	}, true)
	delta.RunId = "run-14"
	if err := aggregator.Accept("worker-a", delta); err != nil {
		t.Fatalf("Accept() error = %v", err)
	}

	next, err := aggregator.NextSequence("worker-a")
	if err != nil {
		t.Fatalf("NextSequence(worker-a) error = %v", err)
	}
	if next != 2 {
		t.Fatalf("NextSequence(worker-a) = %d, want 2", next)
	}
	unknown, err := aggregator.NextSequence("worker-new")
	if err != nil {
		t.Fatalf("NextSequence(worker-new) error = %v", err)
	}
	if unknown != 1 {
		t.Fatalf("NextSequence(worker-new) = %d, want 1", unknown)
	}
}

func TestMetricsAggregatorRejectsSequenceGapWithoutPartialMerge(t *testing.T) {
	started := time.Now()
	deadline := started.Add(2 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	delta := metricsDeltaForTest(t, "worker-a", 1, 2, started, started.Add(time.Second), []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	})

	if err := aggregator.Accept("worker-a", delta); err == nil {
		t.Fatal("Accept(sequence gap) error = nil, want integrity error")
	}
	if got := aggregator.Snapshot().Requests; got != 0 {
		t.Fatalf("requests after rejected delta = %d, want 0", got)
	}
	if len(aggregator.Violations()) != 1 {
		t.Fatalf("violations = %v, want one sequence violation", aggregator.Violations())
	}
}

func TestMetricsAggregatorAllowsIntervalGapAcrossZeroLoadRevision(t *testing.T) {
	started := time.Now()
	deadline := started.Add(5 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	first := metricsDeltaForTest(t, "worker-a", 1, 1, started, started.Add(time.Second), []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	})
	if err := aggregator.Accept("worker-a", first); err != nil {
		t.Fatalf("Accept(revision 1) error = %v", err)
	}
	idle := metricsAssignment("run-13", 2, 0, started, deadline)
	idle.ActivatesAt = timestamppb.New(started.Add(time.Second))
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{"worker-a": idle})
	resumed := metricsAssignment("run-13", 3, 1, started, deadline)
	resumed.ActivatesAt = timestamppb.New(started.Add(3 * time.Second))
	aggregator.RecordAssignments(3, map[string]*loadtestv1.LoadAssignment{"worker-a": resumed})
	second := metricsDeltaForTest(t, "worker-a", 3, 2, started.Add(3*time.Second), started.Add(4*time.Second), []protocol.Result{
		{Latency: 20 * time.Millisecond, StatusCode: 200},
	})

	if err := aggregator.Accept("worker-a", second); err != nil {
		t.Fatalf("Accept(revision 3 after zero load) error = %v", err)
	}
	if got := aggregator.Snapshot().Requests; got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
}

func TestMetricsAggregatorRejectsIntervalGapAcrossActiveRevisions(t *testing.T) {
	started := time.Now()
	deadline := started.Add(5 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	first := metricsDeltaForTest(t, "worker-a", 1, 1, started, started.Add(time.Second), []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	})
	if err := aggregator.Accept("worker-a", first); err != nil {
		t.Fatalf("Accept(revision 1) error = %v", err)
	}
	replacement := metricsAssignment("run-13", 2, 1, started, deadline)
	replacement.ActivatesAt = timestamppb.New(started.Add(time.Second))
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{"worker-a": replacement})
	second := metricsDeltaForTest(t, "worker-a", 2, 2, started.Add(3*time.Second), started.Add(4*time.Second), []protocol.Result{
		{Latency: 20 * time.Millisecond, StatusCode: 200},
	})

	if err := aggregator.Accept("worker-a", second); err == nil {
		t.Fatal("Accept(active revision gap) error = nil, want integrity error")
	}
}

func TestMetricsAggregatorRejectsOverlapAfterZeroLoadRevision(t *testing.T) {
	started := time.Now()
	deadline := started.Add(5 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	first := metricsDeltaForTest(t, "worker-a", 1, 1, started, started.Add(time.Second), []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	})
	if err := aggregator.Accept("worker-a", first); err != nil {
		t.Fatalf("Accept(revision 1) error = %v", err)
	}
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 2, 0, started, deadline),
	})
	aggregator.RecordAssignments(3, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 3, 1, started, deadline),
	})
	overlap := metricsDeltaForTest(t, "worker-a", 3, 2, started.Add(500*time.Millisecond), started.Add(1500*time.Millisecond), []protocol.Result{
		{Latency: 20 * time.Millisecond, StatusCode: 200},
	})

	if err := aggregator.Accept("worker-a", overlap); err == nil {
		t.Fatal("Accept(overlap after zero load) error = nil, want integrity error")
	}
}

func TestMetricsAggregatorRejectsIntervalLongerThanReportingCadence(t *testing.T) {
	started := time.Now()
	deadline := started.Add(3 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	delta := metricsDeltaForTest(t, "worker-a", 1, 1, started, started.Add(2*time.Second), []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	})

	if err := aggregator.Accept("worker-a", delta); err == nil {
		t.Fatal("Accept(two-second interval) error = nil, want cadence error")
	}
}

func TestMetricsAggregatorRejectsShortIntervalAwayFromBoundary(t *testing.T) {
	started := time.Now()
	deadline := started.Add(3 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	delta := metricsDeltaForTest(t, "worker-a", 1, 1, started, started.Add(100*time.Millisecond), []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	})

	if err := aggregator.Accept("worker-a", delta); err == nil {
		t.Fatal("Accept(off-boundary interval) error = nil, want cadence error")
	}
}

func TestMetricsAggregatorClosesReplacedRevisionAtExactBoundary(t *testing.T) {
	started := time.Now()
	deadline := started.Add(5 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	replacement := metricsAssignment("run-13", 2, 1, started, deadline)
	replacement.ActivatesAt = timestamppb.New(started.Add(time.Second))
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{"worker-a": replacement})
	closing := metricsDeltaForTest(t, "worker-a", 1, 1, started, started.Add(time.Second), []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	}, true)
	if err := aggregator.Accept("worker-a", closing); err != nil {
		t.Fatalf("Accept(closing delta) error = %v", err)
	}
	extra := metricsDeltaForTest(t, "worker-a", 1, 2, started.Add(time.Second), started.Add(2*time.Second), []protocol.Result{
		{Latency: 20 * time.Millisecond, StatusCode: 200},
	})

	if err := aggregator.Accept("worker-a", extra); err == nil {
		t.Fatal("Accept(delta after final revision marker) error = nil, want integrity error")
	}
}

func TestMetricsAggregatorRecordsOnlyActiveWorkerFailure(t *testing.T) {
	started := time.Now()
	deadline := started.Add(time.Second)
	aggregator := newMetricsAggregator("run-14")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-active": metricsAssignment("run-14", 1, 1, started, deadline),
		"worker-idle":   metricsAssignment("run-14", 1, 0, started, deadline),
	})
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{})

	if active := aggregator.RecordWorkerFailure("worker-active", "heartbeat timeout"); !active {
		t.Fatal("RecordWorkerFailure(active) = false, want true")
	}
	if active := aggregator.RecordWorkerFailure("worker-idle", "stream closed"); active {
		t.Fatal("RecordWorkerFailure(idle) = true, want false")
	}
	violations := aggregator.Violations()
	if len(violations) != 1 || !strings.Contains(violations[0], "worker-active") || !strings.Contains(violations[0], "heartbeat timeout") {
		t.Fatalf("violations = %v, want one named active-worker failure", violations)
	}
}

func TestMetricsAggregatorAddsRecoveryUnmetDemandWithoutLosingHistory(t *testing.T) {
	started := time.Now()
	deadline := started.Add(time.Second)
	aggregator := newMetricsAggregator("run-14")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-14", 1, 1, started, deadline),
	})
	delta := metricsDeltaForTest(t, "worker-a", 1, 1, started, deadline, []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
	}, true)
	delta.RunId = "run-14"
	if err := aggregator.Accept("worker-a", delta); err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if err := aggregator.AddRecoveryUnmetDemand("worker-a", 17); err != nil {
		t.Fatalf("AddRecoveryUnmetDemand() error = %v", err)
	}

	fleet := aggregator.Snapshot()
	worker := aggregator.WorkerSnapshots()["worker-a"]
	if fleet.Requests != 1 || worker.Requests != 1 {
		t.Fatalf("request history = (fleet=%d worker=%d), want 1 each", fleet.Requests, worker.Requests)
	}
	if fleet.UnmetDemand != 17 || worker.UnmetDemand != 17 {
		t.Fatalf("unmet demand = (fleet=%d worker=%d), want 17 each", fleet.UnmetDemand, worker.UnmetDemand)
	}
	next, err := aggregator.NextSequence("worker-a")
	if err != nil || next != 2 {
		t.Fatalf("NextSequence() = (%d, %v), want (2, nil)", next, err)
	}
}

func TestMetricsAggregatorRecordsRecoveryDemandInExactArrivalWindow(t *testing.T) {
	started := time.Now()
	boundary := started.Add(time.Second)
	aggregator := newMetricsAggregator("run-recovery-window")

	if err := aggregator.AddRecoveryUnmetDemandForWindow("worker-a", started, boundary, 17); err != nil {
		t.Fatalf("AddRecoveryUnmetDemandForWindow() error = %v", err)
	}
	got := aggregator.ArrivalWindow(started, boundary)
	if got.Unmet != 17 || !got.Complete {
		t.Fatalf("ArrivalWindow() = %+v, want unmet=17 complete=true", got)
	}
	if fleet := aggregator.Snapshot(); fleet.UnmetDemand != 17 {
		t.Fatalf("Snapshot().UnmetDemand = %d, want 17", fleet.UnmetDemand)
	}
	if len(aggregator.arrivalRecords) != 0 {
		t.Fatalf("arrival record count = %d, want recovery record pruned", len(aggregator.arrivalRecords))
	}
}

func TestMetricsAggregatorArrivalWindowIsRaceSafeAndBounded(t *testing.T) {
	started := time.Now()
	deadline := started.Add(32 * time.Second)
	aggregator := newMetricsAggregator("run-13")
	aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
	})
	deltas := make([]*loadtestv1.MetricsDelta, 0, 32)
	for sequence := uint64(1); sequence <= 32; sequence++ {
		intervalStart := started.Add(time.Duration(sequence-1) * time.Second)
		intervalEnd := intervalStart.Add(time.Second)
		delta := metricsDeltaForTest(t, "worker-a", 1, sequence, intervalStart, intervalEnd, nil, sequence == 32)
		delta.Counters.AdmittedArrivals = 1
		deltas = append(deltas, delta)
	}

	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		for index, delta := range deltas {
			if err := aggregator.Accept("worker-a", delta); err != nil {
				t.Errorf("Accept(sequence=%d) error = %v", index+1, err)
				return
			}
		}
	}()
	go func() {
		defer wait.Done()
		for boundary := started.Add(time.Second); !boundary.After(deadline); boundary = boundary.Add(time.Second) {
			aggregator.ArrivalWindow(boundary.Add(-time.Second), boundary)
			aggregator.Snapshot()
		}
	}()
	wait.Wait()

	aggregator.ArrivalWindow(started, deadline)
	if len(aggregator.arrivalRecords) != 0 {
		t.Fatalf("arrival record count = %d, want bounded history after final prune", len(aggregator.arrivalRecords))
	}
}

func TestMetricsAggregatorRecordsInfrastructureViolation(t *testing.T) {
	aggregator := newMetricsAggregator("run-infrastructure")
	aggregator.RecordInfrastructureViolation("scale worker fleet: quota exceeded")

	violations := aggregator.Violations()
	if len(violations) != 1 || violations[0] != "scale worker fleet: quota exceeded" {
		t.Fatalf("Violations() = %v, want one infrastructure failure", violations)
	}
}

func TestMetricsAggregatorRejectsInconsistentCountersAndHistogram(t *testing.T) {
	started := time.Now()
	deadline := started.Add(time.Second)
	tests := []struct {
		name   string
		mutate func(*loadtestv1.MetricsDelta)
	}{
		{name: "request outcomes", mutate: func(delta *loadtestv1.MetricsDelta) { delta.Counters.Succeeded++ }},
		{name: "failure categories", mutate: func(delta *loadtestv1.MetricsDelta) { delta.Counters.TransportErrors++ }},
		{name: "missing status", mutate: func(delta *loadtestv1.MetricsDelta) { clear(delta.Counters.StatusCodes) }},
		{name: "overflowing status sum", mutate: func(delta *loadtestv1.MetricsDelta) {
			delta.Counters.StatusCodes = map[int32]uint64{200: ^uint64(0), 201: 2}
		}},
		{name: "histogram count", mutate: func(delta *loadtestv1.MetricsDelta) { delta.Counters.Requests++ }},
		{name: "histogram bytes", mutate: func(delta *loadtestv1.MetricsDelta) { delta.LatencyHistogram = []byte("broken") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			aggregator := newMetricsAggregator("run-13")
			aggregator.RecordAssignments(1, map[string]*loadtestv1.LoadAssignment{
				"worker-a": metricsAssignment("run-13", 1, 1, started, deadline),
			})
			delta := metricsDeltaForTest(t, "worker-a", 1, 1, started, deadline, []protocol.Result{
				{Latency: 10 * time.Millisecond, StatusCode: 200},
			}, true)
			test.mutate(delta)
			if err := aggregator.Accept("worker-a", delta); err == nil {
				t.Fatal("Accept() error = nil, want malformed delta error")
			}
			if got := aggregator.Snapshot().Requests; got != 0 {
				t.Fatalf("requests after rejected delta = %d, want 0", got)
			}
		})
	}
}

func metricsAssignment(
	runID string,
	revision uint64,
	virtualUsers uint64,
	started time.Time,
	deadline time.Time,
) *loadtestv1.LoadAssignment {
	return &loadtestv1.LoadAssignment{
		RunId:    runID,
		Revision: revision,
		Load: &loadtestv1.LoadSlice{
			Model:        loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS,
			VirtualUsers: virtualUsers,
		},
		StartsAt: timestamppb.New(started),
		Deadline: timestamppb.New(deadline),
	}
}

func metricsDeltaForTest(
	t *testing.T,
	workerID string,
	revision uint64,
	sequence uint64,
	intervalStart time.Time,
	intervalEnd time.Time,
	results []protocol.Result,
	finalForRevision ...bool,
) *loadtestv1.MetricsDelta {
	t.Helper()
	collector, err := metrics.NewCollector(len(results) + 1)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	for _, result := range results {
		collector.Offer(result)
	}
	snapshot := collector.Close()
	histogram, err := metrics.EncodeLatencyHistogram(snapshot)
	if err != nil {
		t.Fatalf("EncodeLatencyHistogram() error = %v", err)
	}
	statusCodes := make(map[int32]uint64, len(snapshot.StatusCodes))
	for code, count := range snapshot.StatusCodes {
		statusCodes[int32(code)] = count
	}
	delta := &loadtestv1.MetricsDelta{
		WorkerId:           workerID,
		RunId:              "run-13",
		AssignmentRevision: revision,
		Sequence:           sequence,
		IntervalStart:      timestamppb.New(intervalStart),
		IntervalEnd:        timestamppb.New(intervalEnd),
		Counters: &loadtestv1.MetricCounters{
			Requests:        snapshot.Requests,
			Succeeded:       snapshot.Succeeded,
			Failed:          snapshot.Failed,
			TransportErrors: snapshot.TransportErrors,
			ClientErrors:    snapshot.ClientErrors,
			ServerErrors:    snapshot.ServerErrors,
			BytesRead:       snapshot.BytesRead,
			DroppedSamples:  snapshot.DroppedSamples,
			UnmetDemand:     snapshot.UnmetDemand,
			StatusCodes:     statusCodes,
		},
		HistogramEncoding: loadtestv1.HistogramEncoding_HISTOGRAM_ENCODING_HDR_V2_COMPRESSED,
		LatencyHistogram:  histogram,
	}
	if len(finalForRevision) > 0 {
		delta.FinalForRevision = finalForRevision[0]
	}
	return delta
}
