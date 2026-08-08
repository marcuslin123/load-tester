package worker

import (
	"errors"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/protocol"
)

func TestNewMetricsDeltaPreservesSnapshotAndInterval(t *testing.T) {
	collector, err := metrics.NewCollector(8)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	collector.Offer(protocol.Result{Latency: 20 * time.Millisecond, StatusCode: 200, BytesRead: 10})
	collector.Offer(protocol.Result{Latency: 40 * time.Millisecond, StatusCode: 503, BytesRead: 20})
	collector.Offer(protocol.Result{Latency: 60 * time.Millisecond, Err: errors.New("connection reset")})
	collector.OfferUnmetDemand()
	snapshot := collector.Close()
	intervalStart := time.Date(2026, time.August, 4, 12, 0, 1, 0, time.UTC)
	intervalEnd := intervalStart.Add(time.Second)

	delta, err := newMetricsDelta("worker-a", "run-13", 2, 7, intervalStart, intervalEnd, snapshot, true)
	if err != nil {
		t.Fatalf("newMetricsDelta() error = %v", err)
	}
	if delta.GetWorkerId() != "worker-a" || delta.GetRunId() != "run-13" {
		t.Errorf("identity = (%q, %q), want (worker-a, run-13)", delta.GetWorkerId(), delta.GetRunId())
	}
	if delta.GetAssignmentRevision() != 2 || delta.GetSequence() != 7 {
		t.Errorf("revision/sequence = (%d, %d), want (2, 7)", delta.GetAssignmentRevision(), delta.GetSequence())
	}
	if !delta.GetFinalForRevision() {
		t.Fatal("final_for_revision = false, want true")
	}
	if !delta.GetIntervalStart().AsTime().Equal(intervalStart) || !delta.GetIntervalEnd().AsTime().Equal(intervalEnd) {
		t.Errorf("interval = (%v, %v), want (%v, %v)", delta.GetIntervalStart(), delta.GetIntervalEnd(), intervalStart, intervalEnd)
	}
	counters := delta.GetCounters()
	if counters.GetRequests() != 3 || counters.GetSucceeded() != 1 || counters.GetFailed() != 2 {
		t.Errorf("request counters = %+v, want requests=3 succeeded=1 failed=2", counters)
	}
	if counters.GetTransportErrors() != 1 || counters.GetServerErrors() != 1 || counters.GetUnmetDemand() != 1 {
		t.Errorf("failure counters = %+v, want transport=1 server=1 unmet=1", counters)
	}
	if counters.GetBytesRead() != 30 || counters.GetStatusCodes()[200] != 1 || counters.GetStatusCodes()[503] != 1 {
		t.Errorf("response counters = %+v, want bytes=30 and statuses 200/503", counters)
	}
	if delta.GetHistogramEncoding() != loadtestv1.HistogramEncoding_HISTOGRAM_ENCODING_HDR_V2_COMPRESSED {
		t.Errorf("histogram encoding = %v, want HDR V2 compressed", delta.GetHistogramEncoding())
	}
	decoded, err := metrics.DecodeLatencyHistogram(delta.GetLatencyHistogram())
	if err != nil {
		t.Fatalf("DecodeLatencyHistogram() error = %v", err)
	}
	if decoded.ObservationCount() != 3 || decoded.Percentile(99) < 59*time.Millisecond {
		t.Fatalf("decoded histogram count/p99 = (%d, %v), want 3 and approximately 60ms", decoded.ObservationCount(), decoded.Percentile(99))
	}
}

func TestNewMetricsDeltaRejectsInvalidMetadata(t *testing.T) {
	snapshot := metrics.Snapshot{}
	now := time.Now()
	tests := []struct {
		name       string
		workerID   string
		runID      string
		revision   uint64
		sequence   uint64
		start, end time.Time
	}{
		{name: "worker ID", runID: "run", revision: 1, sequence: 1, start: now, end: now.Add(time.Second)},
		{name: "run ID", workerID: "worker", revision: 1, sequence: 1, start: now, end: now.Add(time.Second)},
		{name: "revision", workerID: "worker", runID: "run", sequence: 1, start: now, end: now.Add(time.Second)},
		{name: "sequence", workerID: "worker", runID: "run", revision: 1, start: now, end: now.Add(time.Second)},
		{name: "interval", workerID: "worker", runID: "run", revision: 1, sequence: 1, start: now, end: now},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newMetricsDelta(test.workerID, test.runID, test.revision, test.sequence, test.start, test.end, snapshot, false); err == nil {
				t.Fatal("newMetricsDelta() error = nil, want metadata validation error")
			}
		})
	}
}
