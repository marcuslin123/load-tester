package metrics

import (
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

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
