package report

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/protocol"
)

func TestEvaluatePassesAtThresholdBoundaries(t *testing.T) {
	t.Parallel()

	snapshot := collect(t,
		protocol.Result{Latency: 10 * time.Millisecond, StatusCode: 200},
		protocol.Result{Latency: 20 * time.Millisecond, StatusCode: 500},
	)
	summary := Evaluate("boundary", "constant-vus", 2*time.Second, snapshot, config.Thresholds{
		P99Latency: snapshot.Percentile(99),
		ErrorRate:  0.5,
	})

	if summary.Status != StatusPass {
		t.Fatalf("Status = %q, want %q; violations = %v", summary.Status, StatusPass, summary.Violations)
	}
	if summary.ErrorRate != 0.5 {
		t.Errorf("ErrorRate = %v, want 0.5", summary.ErrorRate)
	}
	if summary.Throughput != 1 {
		t.Errorf("Throughput = %v, want 1", summary.Throughput)
	}
}

func TestEvaluateReportsEveryFailureReason(t *testing.T) {
	t.Parallel()

	snapshot := collect(t,
		protocol.Result{Latency: 30 * time.Millisecond, StatusCode: 500},
	)
	snapshot.UnmetDemand = 2
	snapshot.DroppedSamples = 3
	summary := Evaluate("failure", "constant-rate", time.Second, snapshot, config.Thresholds{
		P99Latency: 20 * time.Millisecond,
		ErrorRate:  0.5,
	})

	if summary.Status != StatusFail {
		t.Fatalf("Status = %q, want %q", summary.Status, StatusFail)
	}
	for _, want := range []string{
		"p99 latency",
		"error rate",
		"unmet demand: 2",
		"dropped samples: 3",
	} {
		assertContains(t, strings.Join(summary.Violations, "\n"), want)
	}
}

func TestEvaluateFailsWhenNoRequestsComplete(t *testing.T) {
	t.Parallel()

	summary := Evaluate("empty", "constant-vus", time.Second, metrics.Snapshot{}, config.Thresholds{})

	if summary.Status != StatusFail {
		t.Fatalf("Status = %q, want %q", summary.Status, StatusFail)
	}
	assertContains(t, strings.Join(summary.Violations, "\n"), "no requests completed")
}

func TestEvaluateDisablesZeroThresholds(t *testing.T) {
	t.Parallel()

	snapshot := collect(t, protocol.Result{Latency: time.Second, StatusCode: 500})
	summary := Evaluate("disabled", "constant-vus", time.Second, snapshot, config.Thresholds{})

	if summary.Status != StatusPass {
		t.Fatalf("Status = %q, want %q; violations = %v", summary.Status, StatusPass, summary.Violations)
	}
}

func TestInterruptedSkipsThresholdEvaluation(t *testing.T) {
	t.Parallel()

	snapshot := collect(t, protocol.Result{Latency: 30 * time.Millisecond, StatusCode: 500})
	summary := Interrupted("stopped", "constant-vus", time.Second, snapshot)

	if summary.Status != StatusInterrupted {
		t.Fatalf("Status = %q, want %q", summary.Status, StatusInterrupted)
	}
	if len(summary.Violations) != 0 {
		t.Fatalf("Violations = %v, want none", summary.Violations)
	}
}

func TestWriteTextPrintsSummaryAndViolations(t *testing.T) {
	t.Parallel()

	snapshot := collect(t, protocol.Result{Latency: 25 * time.Millisecond, StatusCode: 503, BytesRead: 12})
	summary := Evaluate("checkout", "constant-rate", 2*time.Second, snapshot, config.Thresholds{
		P99Latency: 10 * time.Millisecond,
	})
	var output bytes.Buffer

	if err := WriteText(&output, summary); err != nil {
		t.Fatalf("WriteText() error = %v", err)
	}
	for _, want := range []string{
		"Test: checkout",
		"Model: constant-rate",
		"Result: FAIL",
		"Requests: 1",
		"Throughput: 0.50 req/s",
		"Error rate: 100.00%",
		"Latency: p50=25ms p95=25ms p99=25ms",
		"Violations:",
		"- p99 latency",
	} {
		assertContains(t, output.String(), want)
	}
}

func collect(t *testing.T, results ...protocol.Result) metrics.Snapshot {
	t.Helper()

	collector, err := metrics.NewCollector(len(results))
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	for _, result := range results {
		if !collector.Offer(result) {
			t.Fatal("Offer() unexpectedly dropped a result")
		}
	}
	return collector.Close()
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Errorf("output = %q, want substring %q", got, want)
	}
}
