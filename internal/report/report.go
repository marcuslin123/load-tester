// Package report builds and renders the final result of a local load test.
package report

import (
	"fmt"
	"io"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/metrics"
)

// Status describes whether a run passed, failed, or ended before evaluation.
type Status string

const (
	StatusPass        Status = "PASS"
	StatusFail        Status = "FAIL"
	StatusInterrupted Status = "INTERRUPTED"
)

// Summary contains the measurements and evaluation for one complete or partial run.
type Summary struct {
	Name       string
	Model      string
	Duration   time.Duration
	Metrics    metrics.Snapshot
	Throughput float64
	ErrorRate  float64
	P50        time.Duration
	P95        time.Duration
	P99        time.Duration
	Status     Status
	Violations []string
}

// Evaluate builds a final summary and records every threshold or measurement-integrity failure.
func Evaluate(name, model string, duration time.Duration, snapshot metrics.Snapshot, thresholds config.Thresholds) Summary {
	summary := build(name, model, duration, snapshot)
	summary.Status = StatusPass

	if thresholds.P99Latency > 0 && summary.P99 > thresholds.P99Latency {
		summary.Violations = append(summary.Violations, fmt.Sprintf(
			"p99 latency %s exceeded threshold %s",
			summary.P99,
			thresholds.P99Latency,
		))
	}
	if thresholds.ErrorRate > 0 && summary.ErrorRate > thresholds.ErrorRate {
		summary.Violations = append(summary.Violations, fmt.Sprintf(
			"error rate %.2f%% exceeded threshold %.2f%%",
			summary.ErrorRate*100,
			thresholds.ErrorRate*100,
		))
	}
	if snapshot.UnmetDemand > 0 {
		summary.Violations = append(summary.Violations, fmt.Sprintf("unmet demand: %d", snapshot.UnmetDemand))
	}
	if snapshot.DroppedSamples > 0 {
		summary.Violations = append(summary.Violations, fmt.Sprintf("dropped samples: %d", snapshot.DroppedSamples))
	}
	if snapshot.Requests == 0 {
		summary.Violations = append(summary.Violations, "no requests completed")
	}
	if len(summary.Violations) > 0 {
		summary.Status = StatusFail
	}
	return summary
}

// Interrupted builds a partial summary without evaluating thresholds.
func Interrupted(name, model string, duration time.Duration, snapshot metrics.Snapshot) Summary {
	summary := build(name, model, duration, snapshot)
	summary.Status = StatusInterrupted
	return summary
}

func build(name, model string, duration time.Duration, snapshot metrics.Snapshot) Summary {
	summary := Summary{
		Name:     name,
		Model:    model,
		Duration: duration,
		Metrics:  snapshot,
		P50:      snapshot.Percentile(50),
		P95:      snapshot.Percentile(95),
		P99:      snapshot.Percentile(99),
	}
	if duration > 0 {
		summary.Throughput = float64(snapshot.Requests) / duration.Seconds()
	}
	if snapshot.Requests > 0 {
		summary.ErrorRate = float64(snapshot.Failed) / float64(snapshot.Requests)
	}
	return summary
}

// WriteText renders the stable human-readable summary shared by local and distributed runs.
func WriteText(w io.Writer, summary Summary) error {
	if _, err := fmt.Fprintf(w, `Test: %s
Model: %s
Result: %s
Duration: %s
Requests: %d
Succeeded: %d
Failed: %d
Transport errors: %d
Client errors: %d
Server errors: %d
Unmet demand: %d
Dropped samples: %d
Bytes read: %d
Throughput: %.2f req/s
Error rate: %.2f%%
Latency: p50=%s p95=%s p99=%s
`,
		summary.Name,
		summary.Model,
		summary.Status,
		summary.Duration.Round(time.Millisecond),
		summary.Metrics.Requests,
		summary.Metrics.Succeeded,
		summary.Metrics.Failed,
		summary.Metrics.TransportErrors,
		summary.Metrics.ClientErrors,
		summary.Metrics.ServerErrors,
		summary.Metrics.UnmetDemand,
		summary.Metrics.DroppedSamples,
		summary.Metrics.BytesRead,
		summary.Throughput,
		summary.ErrorRate*100,
		summary.P50.Round(time.Millisecond),
		summary.P95.Round(time.Millisecond),
		summary.P99.Round(time.Millisecond),
	); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}

	if len(summary.Violations) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "Violations:"); err != nil {
		return fmt.Errorf("write violations heading: %w", err)
	}
	for _, violation := range summary.Violations {
		if _, err := fmt.Fprintf(w, "- %s\n", violation); err != nil {
			return fmt.Errorf("write violation: %w", err)
		}
	}
	return nil
}
