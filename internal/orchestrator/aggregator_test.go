package orchestrator

import (
	"errors"
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
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 2, 0, started, deadline),
	})
	aggregator.RecordAssignments(3, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 3, 1, started, deadline),
	})
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
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 2, 1, started, deadline),
	})
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
	aggregator.RecordAssignments(2, map[string]*loadtestv1.LoadAssignment{
		"worker-a": metricsAssignment("run-13", 2, 1, started, deadline),
	})
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
