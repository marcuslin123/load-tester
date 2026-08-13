package metrics

import (
	"slices"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var prometheusLatencyBuckets = []time.Duration{
	time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
	10 * time.Second,
}

var failureKinds = []struct {
	label string
	value func(Snapshot) uint64
}{
	{label: "transport", value: func(snapshot Snapshot) uint64 { return snapshot.TransportErrors }},
	{label: "client", value: func(snapshot Snapshot) uint64 { return snapshot.ClientErrors }},
	{label: "server", value: func(snapshot Snapshot) uint64 { return snapshot.ServerErrors }},
}

// PrometheusView is one immutable fleet, worker, and activity view for a scrape.
type PrometheusView struct {
	Fleet           Snapshot
	Workers         map[string]Snapshot
	ActiveWorkerIDs []string
}

// PrometheusSource supplies one consistent cumulative metrics view per scrape.
type PrometheusSource interface {
	PrometheusView() PrometheusView
}

// RunInfo is immutable metadata attached to the loadtester_run_info series.
type RunInfo struct {
	RunID string
	Name  string
	Model string
}

// PrometheusCollector exports aggregator snapshots without owning mutable metrics.
type PrometheusCollector struct {
	source PrometheusSource
	info   RunInfo

	runInfo              *prometheus.Desc
	activeWorkers        *prometheus.Desc
	requests             *prometheus.Desc
	requestFailures      *prometheus.Desc
	httpResponses        *prometheus.Desc
	responseBytes        *prometheus.Desc
	droppedSamples       *prometheus.Desc
	unmetDemand          *prometheus.Desc
	requestDuration      *prometheus.Desc
	workerActive         *prometheus.Desc
	workerRequests       *prometheus.Desc
	workerRequestFailure *prometheus.Desc
	workerHTTPResponses  *prometheus.Desc
	workerResponseBytes  *prometheus.Desc
	workerDroppedSamples *prometheus.Desc
	workerUnmetDemand    *prometheus.Desc
	workerDuration       *prometheus.Desc
}

// NewPrometheusCollector creates a pull collector over cumulative source snapshots.
func NewPrometheusCollector(source PrometheusSource, info RunInfo) *PrometheusCollector {
	return &PrometheusCollector{
		source: source,
		info:   info,
		runInfo: prometheus.NewDesc(
			"loadtester_run_info", "Load test run metadata.", []string{"run_id", "name", "model"}, nil,
		),
		activeWorkers: prometheus.NewDesc(
			"loadtester_active_workers", "Number of currently connected workers.", nil, nil,
		),
		requests: prometheus.NewDesc(
			"loadtester_requests_total", "Total requests.", nil, nil,
		),
		requestFailures: prometheus.NewDesc(
			"loadtester_request_failures_total", "Total failed requests by kind.", []string{"kind"}, nil,
		),
		httpResponses: prometheus.NewDesc(
			"loadtester_http_responses_total", "Total HTTP responses by status code.", []string{"code"}, nil,
		),
		responseBytes: prometheus.NewDesc(
			"loadtester_response_bytes_total", "Total response bytes.", nil, nil,
		),
		droppedSamples: prometheus.NewDesc(
			"loadtester_dropped_samples_total", "Total dropped metric samples.", nil, nil,
		),
		unmetDemand: prometheus.NewDesc(
			"loadtester_unmet_demand_total", "Total scheduled arrivals without a request.", nil, nil,
		),
		requestDuration: prometheus.NewDesc(
			"loadtester_request_duration_seconds", "Request duration distribution.", nil, nil,
		),
		workerActive: prometheus.NewDesc(
			"loadtester_worker_active", "Whether a worker is currently connected.", []string{"worker_id"}, nil,
		),
		workerRequests: prometheus.NewDesc(
			"loadtester_worker_requests_total", "Total requests by worker.", []string{"worker_id"}, nil,
		),
		workerRequestFailure: prometheus.NewDesc(
			"loadtester_worker_request_failures_total", "Total failed requests by worker and kind.", []string{"worker_id", "kind"}, nil,
		),
		workerHTTPResponses: prometheus.NewDesc(
			"loadtester_worker_http_responses_total", "Total HTTP responses by worker and status code.", []string{"worker_id", "code"}, nil,
		),
		workerResponseBytes: prometheus.NewDesc(
			"loadtester_worker_response_bytes_total", "Total response bytes by worker.", []string{"worker_id"}, nil,
		),
		workerDroppedSamples: prometheus.NewDesc(
			"loadtester_worker_dropped_samples_total", "Total dropped metric samples by worker.", []string{"worker_id"}, nil,
		),
		workerUnmetDemand: prometheus.NewDesc(
			"loadtester_worker_unmet_demand_total", "Total unmet demand by worker.", []string{"worker_id"}, nil,
		),
		workerDuration: prometheus.NewDesc(
			"loadtester_worker_request_duration_seconds", "Request duration distribution by worker.", []string{"worker_id"}, nil,
		),
	}
}

// Describe reports the collector's immutable metric descriptors.
func (c *PrometheusCollector) Describe(descriptors chan<- *prometheus.Desc) {
	for _, descriptor := range []*prometheus.Desc{
		c.runInfo,
		c.activeWorkers,
		c.requests,
		c.requestFailures,
		c.httpResponses,
		c.responseBytes,
		c.droppedSamples,
		c.unmetDemand,
		c.requestDuration,
		c.workerActive,
		c.workerRequests,
		c.workerRequestFailure,
		c.workerHTTPResponses,
		c.workerResponseBytes,
		c.workerDroppedSamples,
		c.workerUnmetDemand,
		c.workerDuration,
	} {
		descriptors <- descriptor
	}
}

// Collect translates the current immutable source snapshots into const metrics.
func (c *PrometheusCollector) Collect(metrics chan<- prometheus.Metric) {
	view := c.source.PrometheusView()
	fleet := view.Fleet
	workerSnapshots := view.Workers
	activeWorkerIDs := view.ActiveWorkerIDs

	metrics <- prometheus.MustNewConstMetric(c.runInfo, prometheus.GaugeValue, 1, c.info.RunID, c.info.Name, c.info.Model)
	metrics <- prometheus.MustNewConstMetric(c.activeWorkers, prometheus.GaugeValue, float64(len(activeWorkerIDs)))
	c.collectSnapshot(metrics, fleet, "")

	active := make(map[string]struct{}, len(activeWorkerIDs))
	workerIDs := make([]string, 0, len(activeWorkerIDs)+len(workerSnapshots))
	for _, workerID := range activeWorkerIDs {
		if _, exists := active[workerID]; exists {
			continue
		}
		active[workerID] = struct{}{}
		workerIDs = append(workerIDs, workerID)
	}
	for workerID := range workerSnapshots {
		if _, exists := active[workerID]; !exists {
			workerIDs = append(workerIDs, workerID)
		}
	}
	slices.Sort(workerIDs)

	for _, workerID := range workerIDs {
		_, isActive := active[workerID]
		activeValue := 0.0
		if isActive {
			activeValue = 1
		}
		metrics <- prometheus.MustNewConstMetric(c.workerActive, prometheus.GaugeValue, activeValue, workerID)
		c.collectSnapshot(metrics, workerSnapshots[workerID], workerID)
	}
}

func (c *PrometheusCollector) collectSnapshot(metrics chan<- prometheus.Metric, snapshot Snapshot, workerID string) {
	if workerID == "" {
		metrics <- prometheus.MustNewConstMetric(c.requests, prometheus.CounterValue, float64(snapshot.Requests))
		for _, failure := range failureKinds {
			metrics <- prometheus.MustNewConstMetric(c.requestFailures, prometheus.CounterValue, float64(failure.value(snapshot)), failure.label)
		}
		c.collectStatusCodes(metrics, c.httpResponses, snapshot.StatusCodes)
		metrics <- prometheus.MustNewConstMetric(c.responseBytes, prometheus.CounterValue, float64(snapshot.BytesRead))
		metrics <- prometheus.MustNewConstMetric(c.droppedSamples, prometheus.CounterValue, float64(snapshot.DroppedSamples))
		metrics <- prometheus.MustNewConstMetric(c.unmetDemand, prometheus.CounterValue, float64(snapshot.UnmetDemand))
		metrics <- prometheus.MustNewConstHistogram(c.requestDuration, snapshot.ObservationCount(), snapshot.LatencySumSeconds(), latencyBucketMap(snapshot))
		return
	}

	metrics <- prometheus.MustNewConstMetric(c.workerRequests, prometheus.CounterValue, float64(snapshot.Requests), workerID)
	for _, failure := range failureKinds {
		metrics <- prometheus.MustNewConstMetric(c.workerRequestFailure, prometheus.CounterValue, float64(failure.value(snapshot)), workerID, failure.label)
	}
	c.collectStatusCodes(metrics, c.workerHTTPResponses, snapshot.StatusCodes, workerID)
	metrics <- prometheus.MustNewConstMetric(c.workerResponseBytes, prometheus.CounterValue, float64(snapshot.BytesRead), workerID)
	metrics <- prometheus.MustNewConstMetric(c.workerDroppedSamples, prometheus.CounterValue, float64(snapshot.DroppedSamples), workerID)
	metrics <- prometheus.MustNewConstMetric(c.workerUnmetDemand, prometheus.CounterValue, float64(snapshot.UnmetDemand), workerID)
	metrics <- prometheus.MustNewConstHistogram(c.workerDuration, snapshot.ObservationCount(), snapshot.LatencySumSeconds(), latencyBucketMap(snapshot), workerID)
}

func (c *PrometheusCollector) collectStatusCodes(metrics chan<- prometheus.Metric, descriptor *prometheus.Desc, statusCodes map[int]uint64, labels ...string) {
	codes := make([]int, 0, len(statusCodes))
	for code := range statusCodes {
		codes = append(codes, code)
	}
	slices.Sort(codes)
	for _, code := range codes {
		metrics <- prometheus.MustNewConstMetric(descriptor, prometheus.CounterValue, float64(statusCodes[code]), append(labels, strconv.Itoa(code))...)
	}
}

func latencyBucketMap(snapshot Snapshot) map[float64]uint64 {
	counts := snapshot.CumulativeLatencyCounts(prometheusLatencyBuckets)
	buckets := make(map[float64]uint64, len(prometheusLatencyBuckets))
	for i, bound := range prometheusLatencyBuckets {
		buckets[bound.Seconds()] = counts[i]
	}
	return buckets
}
