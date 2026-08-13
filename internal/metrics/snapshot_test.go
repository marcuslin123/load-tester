package metrics

import (
	"reflect"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

func TestSnapshotCumulativeLatencyCounts(t *testing.T) {
	t.Parallel()

	snapshot := collectLatencies(t, 1*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond)
	bounds := []time.Duration{1 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond}
	originalBounds := append([]time.Duration(nil), bounds...)

	if got := snapshot.CumulativeLatencyCounts(bounds); !reflect.DeepEqual(got, []uint64{1, 1, 3}) {
		t.Fatalf("CumulativeLatencyCounts() = %v, want conservative HDR counts [1 1 3]", got)
	}
	if !reflect.DeepEqual(bounds, originalBounds) {
		t.Fatalf("CumulativeLatencyCounts() mutated bounds: got %v, want %v", bounds, originalBounds)
	}
	if got := (Snapshot{}).CumulativeLatencyCounts(bounds); !reflect.DeepEqual(got, []uint64{0, 0, 0}) {
		t.Fatalf("empty CumulativeLatencyCounts() = %v, want [0 0 0]", got)
	}

	if got := collectLatencies(t, 20*time.Millisecond).CumulativeLatencyCounts([]time.Duration{5 * time.Millisecond}); !reflect.DeepEqual(got, []uint64{0}) {
		t.Fatalf("bound below first observation = %v, want [0]", got)
	}
	if got := collectLatencies(t, 1*time.Millisecond, 20*time.Millisecond).CumulativeLatencyCounts([]time.Duration{10 * time.Millisecond}); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("bound between observations = %v, want [1]", got)
	}
}

func TestSnapshotCumulativeLatencyCountsExcludesHDRBarStraddlingBound(t *testing.T) {
	t.Parallel()

	snapshot := collectLatencies(t, 5001*time.Microsecond)
	if got := snapshot.CumulativeLatencyCounts([]time.Duration{5 * time.Millisecond, 10 * time.Millisecond}); !reflect.DeepEqual(got, []uint64{0, 1}) {
		t.Fatalf("CumulativeLatencyCounts() = %v, want [0 1] for observation above 5ms", got)
	}
}

func TestSnapshotLatencySumSeconds(t *testing.T) {
	t.Parallel()

	snapshot := collectLatencies(t, 1*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond)
	if got := snapshot.LatencySumSeconds(); got < 0.0309 || got > 0.0311 {
		t.Fatalf("LatencySumSeconds() = %f, want approximately 0.031", got)
	}
	if got := (Snapshot{}).LatencySumSeconds(); got != 0 {
		t.Fatalf("empty LatencySumSeconds() = %f, want 0", got)
	}
}

func TestMergeCombinesHistogramDistributionsAndCounters(t *testing.T) {
	t.Parallel()

	fast := collectRepeatedResults(t, 989, protocol.Result{
		Latency:    10 * time.Millisecond,
		StatusCode: 200,
		BytesRead:  10,
	})
	slow := collectRepeatedResults(t, 11, protocol.Result{
		Latency:    100 * time.Millisecond,
		StatusCode: 503,
		BytesRead:  20,
	})

	merged, err := Merge(fast, slow)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	assertCounter(t, "Requests", merged.Requests, 1000)
	assertCounter(t, "Succeeded", merged.Succeeded, 989)
	assertCounter(t, "Failed", merged.Failed, 11)
	assertCounter(t, "ServerErrors", merged.ServerErrors, 11)
	assertCounter(t, "BytesRead", merged.BytesRead, 10110)
	if got := merged.StatusCodes[200]; got != 989 {
		t.Errorf("StatusCodes[200] = %d, want 989", got)
	}
	if got := merged.StatusCodes[503]; got != 11 {
		t.Errorf("StatusCodes[503] = %d, want 11", got)
	}
	if got := merged.Percentile(99); got < 99*time.Millisecond || got > 101*time.Millisecond {
		t.Errorf("Percentile(99) = %v, want approximately 100ms", got)
	}
}

func TestMergeAddsUnmetDemand(t *testing.T) {
	t.Parallel()

	merged, err := Merge(
		Snapshot{UnmetDemand: 3},
		Snapshot{UnmetDemand: 4},
	)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	assertCounter(t, "UnmetDemand", merged.UnmetDemand, 7)
}

func TestMergeRejectsCounterOverflow(t *testing.T) {
	t.Parallel()

	if _, err := Merge(
		Snapshot{BytesRead: ^uint64(0)},
		Snapshot{BytesRead: 1},
	); err == nil {
		t.Fatal("Merge(counter overflow) error = nil, want overflow error")
	}
}

func collectRepeatedResults(t *testing.T, count int, result protocol.Result) Snapshot {
	t.Helper()

	collector, err := NewCollector(count)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	for range count {
		if !collector.Offer(result) {
			t.Fatal("Offer() dropped a result despite sufficient buffer capacity")
		}
	}
	return collector.Close()
}

func collectLatencies(t *testing.T, latencies ...time.Duration) Snapshot {
	t.Helper()

	collector, err := NewCollector(len(latencies))
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	for _, latency := range latencies {
		if !collector.Offer(protocol.Result{Latency: latency}) {
			t.Fatal("Offer() dropped a latency despite sufficient buffer capacity")
		}
	}
	return collector.Close()
}
