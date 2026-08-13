package metrics

import (
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestPrometheusCollectorExportsFleetAndWorkerMetrics(t *testing.T) {
	t.Parallel()

	fleet := prometheusSnapshot(t, []protocol.Result{
		{Latency: time.Millisecond, StatusCode: 200, BytesRead: 100},
		{Latency: 5 * time.Millisecond, StatusCode: 404, BytesRead: 200},
		{Latency: 25 * time.Millisecond, StatusCode: 503, BytesRead: 300},
		{Latency: 100 * time.Millisecond, Err: errors.New("connection reset")},
	})
	fleet.DroppedSamples = 5
	fleet.UnmetDemand = 7
	workerA := prometheusSnapshot(t, []protocol.Result{
		{Latency: time.Millisecond, StatusCode: 200, BytesRead: 10},
		{Latency: 100 * time.Millisecond, StatusCode: 503, BytesRead: 20},
	})
	workerA.DroppedSamples = 1
	workerA.UnmetDemand = 2
	workerB := prometheusSnapshot(t, []protocol.Result{{Latency: 25 * time.Millisecond, Err: errors.New("connection reset")}})
	workerB.DroppedSamples = 3

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewPrometheusCollector(prometheusSourceStub{
		fleet:   fleet,
		workers: map[string]Snapshot{"worker-a": workerA, "worker-b": workerB},
		active:  []string{"worker-c", "worker-a"},
	}, RunInfo{RunID: "run-17", Name: "checkout", Model: "constant-rate"}))

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	byName := metricFamiliesByName(families)
	assertMetricFamilyNames(t, byName, []string{
		"loadtester_run_info",
		"loadtester_active_workers",
		"loadtester_requests_total",
		"loadtester_request_failures_total",
		"loadtester_http_responses_total",
		"loadtester_response_bytes_total",
		"loadtester_dropped_samples_total",
		"loadtester_unmet_demand_total",
		"loadtester_request_duration_seconds",
		"loadtester_worker_active",
		"loadtester_worker_requests_total",
		"loadtester_worker_request_failures_total",
		"loadtester_worker_http_responses_total",
		"loadtester_worker_response_bytes_total",
		"loadtester_worker_dropped_samples_total",
		"loadtester_worker_unmet_demand_total",
		"loadtester_worker_request_duration_seconds",
	})

	assertMetricValue(t, byName["loadtester_run_info"], map[string]string{"run_id": "run-17", "name": "checkout", "model": "constant-rate"}, 1)
	assertMetricValue(t, byName["loadtester_active_workers"], nil, 2)
	assertMetricValue(t, byName["loadtester_requests_total"], nil, 4)
	assertMetricValue(t, byName["loadtester_request_failures_total"], map[string]string{"kind": "transport"}, 1)
	assertMetricValue(t, byName["loadtester_request_failures_total"], map[string]string{"kind": "client"}, 1)
	assertMetricValue(t, byName["loadtester_request_failures_total"], map[string]string{"kind": "server"}, 1)
	assertMetricValue(t, byName["loadtester_http_responses_total"], map[string]string{"code": "200"}, 1)
	assertMetricValue(t, byName["loadtester_http_responses_total"], map[string]string{"code": "404"}, 1)
	assertMetricValue(t, byName["loadtester_http_responses_total"], map[string]string{"code": "503"}, 1)
	assertMetricValue(t, byName["loadtester_response_bytes_total"], nil, 600)
	assertMetricValue(t, byName["loadtester_dropped_samples_total"], nil, 5)
	assertMetricValue(t, byName["loadtester_unmet_demand_total"], nil, 7)
	assertHistogram(t, byName["loadtester_request_duration_seconds"], nil, 4, map[float64]uint64{
		0.001: 1,
		0.005: 1,
		0.01:  2,
		0.025: 2,
		0.1:   3,
		10:    4,
	})

	assertMetricValue(t, byName["loadtester_worker_active"], map[string]string{"worker_id": "worker-a"}, 1)
	assertMetricValue(t, byName["loadtester_worker_active"], map[string]string{"worker_id": "worker-b"}, 0)
	assertMetricValue(t, byName["loadtester_worker_active"], map[string]string{"worker_id": "worker-c"}, 1)
	assertMetricValue(t, byName["loadtester_worker_requests_total"], map[string]string{"worker_id": "worker-a"}, 2)
	assertMetricValue(t, byName["loadtester_worker_requests_total"], map[string]string{"worker_id": "worker-b"}, 1)
	assertMetricValue(t, byName["loadtester_worker_requests_total"], map[string]string{"worker_id": "worker-c"}, 0)
	assertMetricValue(t, byName["loadtester_worker_request_failures_total"], map[string]string{"worker_id": "worker-c", "kind": "transport"}, 0)
	assertMetricValue(t, byName["loadtester_worker_request_failures_total"], map[string]string{"worker_id": "worker-c", "kind": "client"}, 0)
	assertMetricValue(t, byName["loadtester_worker_request_failures_total"], map[string]string{"worker_id": "worker-c", "kind": "server"}, 0)
	assertMetricValue(t, byName["loadtester_worker_response_bytes_total"], map[string]string{"worker_id": "worker-c"}, 0)
	assertMetricValue(t, byName["loadtester_worker_dropped_samples_total"], map[string]string{"worker_id": "worker-c"}, 0)
	assertMetricValue(t, byName["loadtester_worker_unmet_demand_total"], map[string]string{"worker_id": "worker-c"}, 0)
	assertHistogram(t, byName["loadtester_worker_request_duration_seconds"], map[string]string{"worker_id": "worker-c"}, 0, map[float64]uint64{0.001: 0, 10: 0})

	for _, family := range families {
		for _, metric := range family.Metric {
			if labels := metricLabels(metric); labels["worker_id"] == "fleet" {
				t.Fatalf("%s emitted worker_id=fleet", family.GetName())
			}
		}
		if containsAny(family.GetName(), "succeeded", "rps", "error_rate", "percentile") {
			t.Fatalf("unexpected derived metric family %q", family.GetName())
		}
	}
}

func TestPrometheusCollectorExportsDisconnectedWorkerWithoutDeltas(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewPrometheusCollector(prometheusSourceStub{
		workers: map[string]Snapshot{"worker-empty": {}},
	}, RunInfo{RunID: "run-empty", Name: "empty-worker", Model: "constant-vus"}))

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	byName := metricFamiliesByName(families)
	assertMetricValue(t, byName["loadtester_active_workers"], nil, 0)
	labels := map[string]string{"worker_id": "worker-empty"}
	assertMetricValue(t, byName["loadtester_worker_active"], labels, 0)
	assertMetricValue(t, byName["loadtester_worker_requests_total"], labels, 0)
	assertHistogram(t, byName["loadtester_worker_request_duration_seconds"], labels, 0, map[float64]uint64{0.005: 0, 10: 0})
}

func TestPrometheusLatencyBucketExcludesObservationAboveFixedBound(t *testing.T) {
	t.Parallel()

	snapshot := prometheusSnapshot(t, []protocol.Result{{Latency: 5001 * time.Microsecond, StatusCode: 200}})
	buckets := latencyBucketMap(snapshot)
	if got := buckets[0.005]; got != 0 {
		t.Fatalf("5ms bucket = %d, want 0 for 5001us observation", got)
	}
	if got := buckets[0.01]; got != 1 {
		t.Fatalf("10ms bucket = %d, want 1 for 5001us observation", got)
	}
}

func TestPrometheusCollectorReadsOneSourceViewPerCollection(t *testing.T) {
	t.Parallel()

	source := &countingPrometheusSource{}
	collector := NewPrometheusCollector(source, RunInfo{RunID: "run-view", Name: "view", Model: "constant-vus"})
	metricsChannel := make(chan prometheus.Metric, 32)
	collector.Collect(metricsChannel)
	if source.calls != 1 {
		t.Fatalf("PrometheusView() calls = %d, want 1", source.calls)
	}
}

type prometheusSourceStub struct {
	fleet   Snapshot
	workers map[string]Snapshot
	active  []string
}

func (s prometheusSourceStub) PrometheusView() PrometheusView {
	return PrometheusView{
		Fleet:           s.fleet,
		Workers:         s.workers,
		ActiveWorkerIDs: s.active,
	}
}

type countingPrometheusSource struct {
	calls int
}

func (s *countingPrometheusSource) PrometheusView() PrometheusView {
	s.calls++
	return PrometheusView{}
}

func prometheusSnapshot(t *testing.T, results []protocol.Result) Snapshot {
	t.Helper()

	collector, err := NewCollector(len(results) + 1)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	for _, result := range results {
		if !collector.Offer(result) {
			t.Fatal("Offer() = false, want true")
		}
	}
	return collector.Close()
}

func metricFamiliesByName(families []*dto.MetricFamily) map[string]*dto.MetricFamily {
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}
	return byName
}

func assertMetricFamilyNames(t *testing.T, families map[string]*dto.MetricFamily, want []string) {
	t.Helper()

	got := make([]string, 0, len(families))
	for name := range families {
		got = append(got, name)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metric family names = %v, want %v", got, want)
	}
}

func assertMetricValue(t *testing.T, family *dto.MetricFamily, labels map[string]string, want float64) {
	t.Helper()

	metric := findMetric(t, family, labels)
	var got float64
	switch {
	case metric.Counter != nil:
		got = metric.Counter.GetValue()
	case metric.Gauge != nil:
		got = metric.Gauge.GetValue()
	default:
		t.Fatalf("%s has no counter or gauge", family.GetName())
	}
	if got != want {
		t.Fatalf("%s%v = %v, want %v", family.GetName(), labels, got, want)
	}
}

func assertHistogram(t *testing.T, family *dto.MetricFamily, labels map[string]string, wantCount uint64, wantBuckets map[float64]uint64) {
	t.Helper()

	histogram := findMetric(t, family, labels).GetHistogram()
	if got := histogram.GetSampleCount(); got != wantCount {
		t.Fatalf("%s%v count = %d, want %d", family.GetName(), labels, got, wantCount)
	}
	gotBuckets := make(map[float64]uint64, len(histogram.Bucket))
	for _, bucket := range histogram.Bucket {
		gotBuckets[bucket.GetUpperBound()] = bucket.GetCumulativeCount()
	}
	for upperBound, want := range wantBuckets {
		if got := gotBuckets[upperBound]; got != want {
			t.Fatalf("%s%v bucket %g = %d, want %d", family.GetName(), labels, upperBound, got, want)
		}
	}
}

func findMetric(t *testing.T, family *dto.MetricFamily, wantLabels map[string]string) *dto.Metric {
	t.Helper()

	for _, metric := range family.Metric {
		if reflect.DeepEqual(metricLabels(metric), wantLabels) {
			return metric
		}
	}
	t.Fatalf("%s has no metric with labels %v", family.GetName(), wantLabels)
	return nil
}

func metricLabels(metric *dto.Metric) map[string]string {
	if len(metric.Label) == 0 {
		return nil
	}
	labels := make(map[string]string, len(metric.Label))
	for _, label := range metric.Label {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}

func containsAny(value string, fragments ...string) bool {
	for _, fragment := range fragments {
		if len(fragment) > 0 && contains(value, fragment) {
			return true
		}
	}
	return false
}

func contains(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		if value[i:i+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
