package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/protocol"
	"github.com/marcuslin123/load-tester/internal/report"
	"github.com/marcuslin123/load-tester/internal/runner"
	"github.com/marcuslin123/load-tester/internal/targetapp"
)

const (
	oracleLatency   = 50 * time.Millisecond
	oracleDuration  = 3 * time.Second
	oracleUsers     = 20
	oracleErrorRate = 0.20
)

// TestLocalPipelineMatchesTargetOracle guards the measurements that every
// distributed phase will later aggregate and report.
func TestLocalPipelineMatchesTargetOracle(t *testing.T) {
	server := httptest.NewServer(targetapp.NewHandler())
	t.Cleanup(server.Close)

	cfg := parseOracleConfig(t, server.URL)
	executor, err := protocol.NewHTTP(cfg.Target)
	if err != nil {
		t.Fatalf("NewHTTP() error = %v", err)
	}
	summary, err := runner.Run(context.Background(), cfg, executor, runner.Options{
		PreflightTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	t.Logf(
		"requests=%d throughput=%.2f req/s error_rate=%.2f%% p50=%v p95=%v p99=%v",
		summary.Metrics.Requests,
		summary.Throughput,
		summary.ErrorRate*100,
		summary.P50,
		summary.P95,
		summary.P99,
	)

	if summary.Status != report.StatusPass {
		t.Fatalf("Status = %q, want %q; violations = %v", summary.Status, report.StatusPass, summary.Violations)
	}
	assertDurationRange(t, "p50", summary.P50, 50*time.Millisecond, 65*time.Millisecond)
	assertDurationRange(t, "p95", summary.P95, 50*time.Millisecond, 80*time.Millisecond)
	assertDurationRange(t, "p99", summary.P99, 50*time.Millisecond, 100*time.Millisecond)
	assertFloatRange(t, "throughput", summary.Throughput, 320, 480)
	assertFloatRange(t, "error rate", summary.ErrorRate, 0.15, 0.25)

	observed := summary.Metrics
	if observed.TransportErrors != 0 {
		t.Errorf("TransportErrors = %d, want 0", observed.TransportErrors)
	}
	if observed.ClientErrors != 0 {
		t.Errorf("ClientErrors = %d, want 0", observed.ClientErrors)
	}
	if observed.ServerErrors != observed.Failed {
		t.Errorf("ServerErrors = %d, Failed = %d; want every failure classified as a server error", observed.ServerErrors, observed.Failed)
	}
	if observed.StatusCodes[http.StatusServiceUnavailable] != observed.Failed {
		t.Errorf("503 responses = %d, Failed = %d; want every failure represented by a 503", observed.StatusCodes[http.StatusServiceUnavailable], observed.Failed)
	}
	if observed.Succeeded+observed.Failed != observed.Requests {
		t.Errorf("Succeeded + Failed = %d, Requests = %d", observed.Succeeded+observed.Failed, observed.Requests)
	}
	if observed.UnmetDemand != 0 || observed.DroppedSamples != 0 {
		t.Errorf("UnmetDemand = %d, DroppedSamples = %d; want reliable complete measurements", observed.UnmetDemand, observed.DroppedSamples)
	}
}

// TestMergedSnapshotsEqualDirectAggregation proves that partitioning one real
// request population across workers does not change its reported distribution.
func TestMergedSnapshotsEqualDirectAggregation(t *testing.T) {
	server := httptest.NewServer(targetapp.NewHandler())
	t.Cleanup(server.Close)

	const samplesPerBatch = 8
	latencies := []time.Duration{10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}
	combined, err := metrics.NewCollector(samplesPerBatch * len(latencies))
	if err != nil {
		t.Fatalf("NewCollector(combined) error = %v", err)
	}

	var batches []metrics.Snapshot
	for _, latency := range latencies {
		batch, err := metrics.NewCollector(samplesPerBatch)
		if err != nil {
			t.Fatalf("NewCollector(batch) error = %v", err)
		}
		executor, err := protocol.NewHTTP(config.Target{
			Method: http.MethodGet,
			URL:    fmt.Sprintf("%s/echo?latency=%s", server.URL, latency),
		})
		if err != nil {
			t.Fatalf("NewHTTP(%s) error = %v", latency, err)
		}

		for range samplesPerBatch {
			requestCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			result := executor.Execute(requestCtx)
			cancel()
			if result.Err != nil || result.StatusCode != http.StatusOK {
				t.Fatalf("Execute(%s) = status %d, error %v; want status 200", latency, result.StatusCode, result.Err)
			}
			if !batch.Offer(result) || !combined.Offer(result) {
				t.Fatal("collector dropped a sample despite a buffer sized for the full population")
			}
		}
		batches = append(batches, batch.Close())
	}
	direct := combined.Close()
	merged, err := metrics.Merge(batches...)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	t.Logf(
		"requests=%d merged_p50=%v merged_p95=%v merged_p99=%v",
		merged.Requests,
		merged.Percentile(50),
		merged.Percentile(95),
		merged.Percentile(99),
	)

	assertCountersEqual(t, merged, direct)
	for _, percentile := range []float64{50, 95, 99} {
		if got, want := merged.Percentile(percentile), direct.Percentile(percentile); got != want {
			t.Errorf("merged p%.0f = %v, directly aggregated p%.0f = %v", percentile, got, percentile, want)
		}
	}
	if merged.Percentile(50) < 50*time.Millisecond {
		t.Errorf("merged p50 = %v, want the middle 50ms batch represented", merged.Percentile(50))
	}
	if merged.Percentile(99) < 100*time.Millisecond {
		t.Errorf("merged p99 = %v, want the slow 100ms batch represented", merged.Percentile(99))
	}
}

func parseOracleConfig(t *testing.T, serverURL string) config.Config {
	t.Helper()

	yaml := fmt.Sprintf(`
name: phase-1-oracle
target:
  protocol: http
  url: %q
load:
  model: constant-vus
  virtual_users: %d
  duration: %s
fleet:
  min_workers: 1
  max_workers: 1
`, fmt.Sprintf("%s/echo?latency=%s&error_rate=%.2f", serverURL, oracleLatency, oracleErrorRate), oracleUsers, oracleDuration)
	cfg, err := config.Parse(strings.NewReader(yaml))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	return cfg
}

func assertDurationRange(t *testing.T, name string, got, minimum, maximum time.Duration) {
	t.Helper()
	if got < minimum || got > maximum {
		t.Errorf("%s = %v, want between %v and %v", name, got, minimum, maximum)
	}
}

func assertFloatRange(t *testing.T, name string, got, minimum, maximum float64) {
	t.Helper()
	if got < minimum || got > maximum {
		t.Errorf("%s = %.4f, want between %.4f and %.4f", name, got, minimum, maximum)
	}
}

func assertCountersEqual(t *testing.T, got, want metrics.Snapshot) {
	t.Helper()

	gotCounters := []uint64{
		got.Requests,
		got.Succeeded,
		got.Failed,
		got.TransportErrors,
		got.ClientErrors,
		got.ServerErrors,
		got.BytesRead,
		got.DroppedSamples,
		got.UnmetDemand,
	}
	wantCounters := []uint64{
		want.Requests,
		want.Succeeded,
		want.Failed,
		want.TransportErrors,
		want.ClientErrors,
		want.ServerErrors,
		want.BytesRead,
		want.DroppedSamples,
		want.UnmetDemand,
	}
	if !reflect.DeepEqual(gotCounters, wantCounters) {
		t.Errorf("merged counters = %v, directly aggregated counters = %v", gotCounters, wantCounters)
	}
	if !reflect.DeepEqual(got.StatusCodes, want.StatusCodes) {
		t.Errorf("merged status codes = %v, directly aggregated status codes = %v", got.StatusCodes, want.StatusCodes)
	}
}
