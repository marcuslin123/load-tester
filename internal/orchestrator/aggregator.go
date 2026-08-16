package orchestrator

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"google.golang.org/protobuf/proto"
)

type assignmentMetricWindow struct {
	revision    uint64
	startsAt    time.Time
	activatesAt time.Time
	deadline    time.Time
	active      bool
}

// revisionMetricState keeps logical interval progress separate from the global
// transport sequence because request draining can reorder revision delivery.
type revisionMetricState struct {
	lastInterval    time.Time
	closed          bool
	finalAtDeadline bool
}

type workerMetricState struct {
	lastSequence uint64
	lastDelta    *loadtestv1.MetricsDelta
	revisions    map[uint64]revisionMetricState
	snapshot     metrics.Snapshot
}

type arrivalRecord struct {
	start    time.Time
	end      time.Time
	admitted uint64
	unmet    uint64
}

type arrivalWindow struct {
	Admitted uint64
	Unmet    uint64
	Complete bool
}

// metricsAggregator validates and atomically merges worker deltas for one run.
type metricsAggregator struct {
	mu sync.Mutex

	runID          string
	assignments    map[uint64]map[string]assignmentMetricWindow
	latest         map[string]assignmentMetricWindow
	lastAssigned   map[string]assignmentMetricWindow
	workers        map[string]*workerMetricState
	snapshot       metrics.Snapshot
	arrivalRecords []arrivalRecord
	violations     []string
	changed        chan struct{}
}

func newMetricsAggregator(runID string) *metricsAggregator {
	return &metricsAggregator{
		runID:        runID,
		assignments:  make(map[uint64]map[string]assignmentMetricWindow),
		latest:       make(map[string]assignmentMetricWindow),
		lastAssigned: make(map[string]assignmentMetricWindow),
		workers:      make(map[string]*workerMetricState),
		changed:      make(chan struct{}, 1),
	}
}

// RecordAssignments registers the complete revision before any of its deltas
// can arrive and replaces the set of workers expected to flush at the deadline.
func (a *metricsAggregator) RecordAssignments(
	revision uint64,
	assignments map[string]*loadtestv1.LoadAssignment,
) {
	a.mu.Lock()
	defer a.mu.Unlock()
	windows := make(map[string]assignmentMetricWindow, len(assignments))
	latest := make(map[string]assignmentMetricWindow, len(assignments))
	for workerID, assignment := range assignments {
		activatesAt := assignment.GetStartsAt().AsTime()
		if assignment.GetActivatesAt() != nil {
			activatesAt = assignment.GetActivatesAt().AsTime()
		}
		window := assignmentMetricWindow{
			revision:    revision,
			startsAt:    assignment.GetStartsAt().AsTime(),
			activatesAt: activatesAt,
			deadline:    assignment.GetDeadline().AsTime(),
			active:      isActiveSlice(assignment.GetLoad()),
		}
		windows[workerID] = window
		latest[workerID] = window
		a.lastAssigned[workerID] = window
	}
	a.assignments[revision] = windows
	a.latest = latest
	a.notify()
}

// Accept performs all validation before advancing sequence state or merging any
// counters, preserving an all-or-nothing delta boundary.
func (a *metricsAggregator) Accept(workerID string, delta *loadtestv1.MetricsDelta) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if delta == nil {
		return a.reject(workerID, errors.New("metrics delta is required"))
	}
	if strings.TrimSpace(workerID) == "" || delta.GetWorkerId() != workerID {
		return a.reject(workerID, fmt.Errorf("delta worker ID %q does not match session", delta.GetWorkerId()))
	}
	if delta.GetRunId() != a.runID {
		return a.reject(workerID, fmt.Errorf("delta run ID %q does not match %q", delta.GetRunId(), a.runID))
	}
	windows, exists := a.assignments[delta.GetAssignmentRevision()]
	if !exists {
		return a.reject(workerID, fmt.Errorf("unknown assignment revision %d", delta.GetAssignmentRevision()))
	}
	window, exists := windows[workerID]
	if !exists {
		return a.reject(workerID, fmt.Errorf("worker was not assigned revision %d", delta.GetAssignmentRevision()))
	}
	state := a.workers[workerID]
	if state == nil {
		state = &workerMetricState{}
	}
	if delta.GetSequence() == state.lastSequence && state.lastDelta != nil {
		if proto.Equal(delta, state.lastDelta) {
			return nil
		}
		return a.reject(workerID, fmt.Errorf("metrics sequence %d conflicts with the accepted delta", delta.GetSequence()))
	}
	wantSequence := state.lastSequence + 1
	if delta.GetSequence() != wantSequence {
		return a.reject(workerID, fmt.Errorf("metrics sequence %d must equal %d", delta.GetSequence(), wantSequence))
	}
	revision := delta.GetAssignmentRevision()
	revisionState := state.revisions[revision]
	if revisionState.closed {
		return a.reject(workerID, fmt.Errorf("assignment revision %d already sent its partial delta", delta.GetAssignmentRevision()))
	}

	previousEnd := revisionState.lastInterval
	if previousEnd.IsZero() {
		previousEnd = window.activatesAt
	}
	intervalStart, intervalEnd, err := validateMetricInterval(
		delta,
		window,
		previousEnd,
		a.revisionActiveUntil(revision, window.deadline),
	)
	if err != nil {
		return a.reject(workerID, err)
	}
	decoded, err := validateAndDecodeSnapshot(delta)
	if err != nil {
		return a.reject(workerID, err)
	}
	merged, err := metrics.Merge(a.snapshot, decoded)
	if err != nil {
		return a.reject(workerID, err)
	}
	workerMerged, err := metrics.Merge(state.snapshot, decoded)
	if err != nil {
		return a.reject(workerID, err)
	}

	a.snapshot = merged
	a.arrivalRecords = append(a.arrivalRecords, arrivalRecord{
		start:    intervalStart,
		end:      intervalEnd,
		admitted: decoded.AdmittedArrivals,
		unmet:    decoded.UnmetDemand,
	})
	state.snapshot = workerMerged
	state.lastSequence = delta.GetSequence()
	state.lastDelta = proto.Clone(delta).(*loadtestv1.MetricsDelta)
	if state.revisions == nil {
		state.revisions = make(map[uint64]revisionMetricState)
	}
	revisionState.lastInterval = intervalEnd
	if delta.GetFinalForRevision() {
		revisionState.closed = true
		revisionState.finalAtDeadline = intervalEnd.Equal(window.deadline)
	}
	state.revisions[revision] = revisionState
	a.workers[workerID] = state
	a.notify()
	return nil
}

func validateMetricInterval(
	delta *loadtestv1.MetricsDelta,
	window assignmentMetricWindow,
	lastInterval time.Time,
	activeUntil time.Time,
) (time.Time, time.Time, error) {
	if delta.GetIntervalStart() == nil || delta.GetIntervalEnd() == nil {
		return time.Time{}, time.Time{}, errors.New("metrics interval timestamps are required")
	}
	if err := delta.GetIntervalStart().CheckValid(); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid interval start: %w", err)
	}
	if err := delta.GetIntervalEnd().CheckValid(); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid interval end: %w", err)
	}
	start := delta.GetIntervalStart().AsTime()
	end := delta.GetIntervalEnd().AsTime()
	if !end.After(start) {
		return time.Time{}, time.Time{}, errors.New("metrics interval end must be after its start")
	}
	if end.Sub(start) > time.Second {
		return time.Time{}, time.Time{}, errors.New("metrics interval must not exceed the one-second reporting cadence")
	}
	boundaryAfterStart := window.startsAt.Add((start.Sub(window.startsAt)/time.Second + 1) * time.Second)
	if boundaryAfterStart.Before(end) {
		return time.Time{}, time.Time{}, errors.New("metrics interval must not cross a reporting boundary")
	}
	if start.Before(window.activatesAt) || end.After(activeUntil) {
		return time.Time{}, time.Time{}, errors.New("metrics interval is outside the assigned run window")
	}
	periodicBoundary := end.Sub(window.startsAt) > 0 && end.Sub(window.startsAt)%time.Second == 0
	atRevisionEnd := end.Equal(activeUntil)
	if delta.GetFinalForRevision() {
		if !atRevisionEnd {
			return time.Time{}, time.Time{}, errors.New("final metric delta requires the deadline or a replaced revision")
		}
	} else if atRevisionEnd || !periodicBoundary {
		return time.Time{}, time.Time{}, errors.New("non-final metric delta must end at a reporting boundary before the deadline")
	}
	if start.Before(lastInterval) {
		return time.Time{}, time.Time{}, fmt.Errorf("metrics interval starts at %s before previous end %s", start, lastInterval)
	}
	if !start.Equal(lastInterval) {
		return time.Time{}, time.Time{}, fmt.Errorf("metrics interval starts at %s instead of previous end %s", start, lastInterval)
	}
	return start, end, nil
}

func validateAndDecodeSnapshot(delta *loadtestv1.MetricsDelta) (metrics.Snapshot, error) {
	if delta.GetCounters() == nil {
		return metrics.Snapshot{}, errors.New("metric counters are required")
	}
	counters := delta.GetCounters()
	if counters.GetSucceeded() > counters.GetRequests() ||
		counters.GetFailed() != counters.GetRequests()-counters.GetSucceeded() {
		return metrics.Snapshot{}, errors.New("requests must equal succeeded plus failed")
	}
	if counters.GetTransportErrors() > counters.GetFailed() ||
		counters.GetClientErrors() > counters.GetFailed()-counters.GetTransportErrors() ||
		counters.GetServerErrors() != counters.GetFailed()-counters.GetTransportErrors()-counters.GetClientErrors() {
		return metrics.Snapshot{}, errors.New("failed requests must equal transport, client, and server errors")
	}
	var statusTotal uint64
	for code, count := range counters.GetStatusCodes() {
		if code < 100 || code > 599 {
			return metrics.Snapshot{}, fmt.Errorf("invalid HTTP status code %d", code)
		}
		if count > ^uint64(0)-statusTotal {
			return metrics.Snapshot{}, errors.New("status-code count overflows uint64")
		}
		statusTotal += count
	}
	minimumStatuses := counters.GetRequests() - counters.GetTransportErrors()
	if statusTotal < minimumStatuses || statusTotal > counters.GetRequests() {
		return metrics.Snapshot{}, errors.New("status-code count must cover every request without a transport error")
	}
	if delta.GetHistogramEncoding() != loadtestv1.HistogramEncoding_HISTOGRAM_ENCODING_HDR_V2_COMPRESSED {
		return metrics.Snapshot{}, fmt.Errorf("unsupported histogram encoding %v", delta.GetHistogramEncoding())
	}
	snapshot, err := metrics.DecodeLatencyHistogram(delta.GetLatencyHistogram())
	if err != nil {
		return metrics.Snapshot{}, err
	}
	if snapshot.ObservationCount() != counters.GetRequests() {
		return metrics.Snapshot{}, fmt.Errorf("histogram observations %d do not equal requests %d", snapshot.ObservationCount(), counters.GetRequests())
	}
	snapshot.Requests = counters.GetRequests()
	snapshot.Succeeded = counters.GetSucceeded()
	snapshot.Failed = counters.GetFailed()
	snapshot.TransportErrors = counters.GetTransportErrors()
	snapshot.ClientErrors = counters.GetClientErrors()
	snapshot.ServerErrors = counters.GetServerErrors()
	snapshot.BytesRead = counters.GetBytesRead()
	snapshot.DroppedSamples = counters.GetDroppedSamples()
	snapshot.UnmetDemand = counters.GetUnmetDemand()
	snapshot.AdmittedArrivals = counters.GetAdmittedArrivals()
	for code, count := range counters.GetStatusCodes() {
		snapshot.StatusCodes[int(code)] = count
	}
	return snapshot, nil
}

func (a *metricsAggregator) Snapshot() metrics.Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	cloned, err := metrics.Merge(a.snapshot)
	if err != nil {
		panic("orchestrator: clone merged metrics: " + err.Error())
	}
	return cloned
}

// ArrivalWindow consumes counters for one controller window. Accepted worker
// intervals cannot cross the run-aligned reporting boundaries that divide it.
func (a *metricsAggregator) ArrivalWindow(from, to time.Time) arrivalWindow {
	a.mu.Lock()
	defer a.mu.Unlock()
	result := arrivalWindow{Complete: a.arrivalWindowComplete(from, to)}
	kept := a.arrivalRecords[:0]
	for _, record := range a.arrivalRecords {
		if !record.start.Before(from) && !record.end.After(to) {
			result.Admitted += record.admitted
			result.Unmet += record.unmet
		}
		if record.end.After(to) {
			kept = append(kept, record)
		}
	}
	clear(a.arrivalRecords[len(kept):])
	a.arrivalRecords = kept
	return result
}

func (a *metricsAggregator) arrivalWindowComplete(from, to time.Time) bool {
	for revision, workers := range a.assignments {
		for workerID, assignment := range workers {
			if !assignment.active {
				continue
			}
			activeUntil := a.revisionActiveUntil(revision, assignment.deadline)
			if !assignment.activatesAt.Before(to) || !activeUntil.After(from) {
				continue
			}
			requiredThrough := to
			if activeUntil.Before(requiredThrough) {
				requiredThrough = activeUntil
			}
			state := a.workers[workerID]
			if state == nil || state.revisions[revision].lastInterval.Before(requiredThrough) {
				return false
			}
		}
	}
	return true
}

func (a *metricsAggregator) revisionActiveUntil(revision uint64, deadline time.Time) time.Time {
	activeUntil := deadline
	for laterRevision, workers := range a.assignments {
		if laterRevision <= revision {
			continue
		}
		for _, assignment := range workers {
			if assignment.activatesAt.Before(activeUntil) {
				activeUntil = assignment.activatesAt
			}
		}
	}
	return activeUntil
}

func (a *metricsAggregator) WorkerSnapshots() map[string]metrics.Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	workers := make(map[string]metrics.Snapshot, len(a.workers))
	for workerID, state := range a.workers {
		cloned, err := metrics.Merge(state.snapshot)
		if err != nil {
			panic("orchestrator: clone worker metrics: " + err.Error())
		}
		workers[workerID] = cloned
	}
	return workers
}

// PrometheusView clones fleet and worker snapshots under one aggregator lock.
func (a *metricsAggregator) PrometheusView() metrics.PrometheusView {
	a.mu.Lock()
	defer a.mu.Unlock()
	fleet, err := metrics.Merge(a.snapshot)
	if err != nil {
		panic("orchestrator: clone merged metrics: " + err.Error())
	}
	workers := make(map[string]metrics.Snapshot, len(a.workers))
	for workerID, state := range a.workers {
		cloned, err := metrics.Merge(state.snapshot)
		if err != nil {
			panic("orchestrator: clone worker metrics: " + err.Error())
		}
		workers[workerID] = cloned
	}
	return metrics.PrometheusView{Fleet: fleet, Workers: workers}
}

// RegisterWorker retains a worker in cumulative metrics without advancing sequence.
func (a *metricsAggregator) RegisterWorker(workerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.workers[workerID] == nil {
		a.workers[workerID] = &workerMetricState{}
	}
}

// NextSequence returns the first sequence a new connection should emit for a
// stable worker ID without discarding metrics accepted from an older session.
func (a *metricsAggregator) NextSequence(workerID string) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.workers[workerID]
	if state == nil {
		return 1, nil
	}
	if state.lastSequence == ^uint64(0) {
		return 0, fmt.Errorf("worker %s exhausted metric sequence numbers", workerID)
	}
	return state.lastSequence + 1, nil
}

// RecordWorkerFailure marks the run incomplete only when the worker's latest
// assignment carried load that could have produced an unreported interval.
func (a *metricsAggregator) RecordWorkerFailure(workerID, reason string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	latest, assigned := a.lastAssigned[workerID]
	if !assigned || !latest.active {
		return false
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "session lost"
	}
	a.violations = append(a.violations, fmt.Sprintf(
		"worker %s failed: %s; unreported metric interval lost",
		workerID,
		reason,
	))
	a.notify()
	return true
}

// AddRecoveryUnmetDemand attributes definitely missed open-model arrivals to
// the failed worker while preserving its accepted counters and sequence state.
func (a *metricsAggregator) AddRecoveryUnmetDemand(workerID string, count uint64) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("recovery worker ID is required")
	}
	if count == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	addition := metrics.Snapshot{UnmetDemand: count}
	merged, err := metrics.Merge(a.snapshot, addition)
	if err != nil {
		return err
	}
	state := a.workers[workerID]
	if state == nil {
		state = &workerMetricState{}
	}
	workerMerged, err := metrics.Merge(state.snapshot, addition)
	if err != nil {
		return err
	}
	a.snapshot = merged
	state.snapshot = workerMerged
	a.workers[workerID] = state
	a.notify()
	return nil
}

// AddRecoveryUnmetDemandForWindow records recovery demand in cumulative
// results and in the exact interval used by the autoscaler.
func (a *metricsAggregator) AddRecoveryUnmetDemandForWindow(
	workerID string,
	start time.Time,
	end time.Time,
	count uint64,
) error {
	return a.addRecoveryUnmetDemandRecords(workerID, []arrivalRecord{{
		start: start,
		end:   end,
		unmet: count,
	}})
}

func (a *metricsAggregator) addRecoveryUnmetDemandRecords(workerID string, records []arrivalRecord) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("recovery worker ID is required")
	}
	var total uint64
	valid := make([]arrivalRecord, 0, len(records))
	for _, record := range records {
		if record.start.IsZero() || !record.end.After(record.start) {
			return errors.New("valid recovery demand interval is required")
		}
		if record.unmet == 0 {
			continue
		}
		if record.unmet > ^uint64(0)-total {
			return errors.New("recovery unmet demand overflows uint64")
		}
		total += record.unmet
		valid = append(valid, record)
	}
	if total == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	addition := metrics.Snapshot{UnmetDemand: total}
	merged, err := metrics.Merge(a.snapshot, addition)
	if err != nil {
		return err
	}
	state := a.workers[workerID]
	if state == nil {
		state = &workerMetricState{}
	}
	workerMerged, err := metrics.Merge(state.snapshot, addition)
	if err != nil {
		return err
	}
	a.snapshot = merged
	state.snapshot = workerMerged
	a.workers[workerID] = state
	a.arrivalRecords = append(a.arrivalRecords, valid...)
	a.notify()
	return nil
}

func (a *metricsAggregator) Complete() bool {
	return len(a.MissingFinal()) == 0
}

func (a *metricsAggregator) MissingFinal() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	missing := make([]string, 0)
	for workerID, latest := range a.latest {
		if !latest.active {
			continue
		}
		state := a.workers[workerID]
		if state == nil || !state.revisions[latest.revision].finalAtDeadline {
			missing = append(missing, workerID)
		}
	}
	slices.Sort(missing)
	return missing
}

func (a *metricsAggregator) Violations() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.violations)
}

func (a *metricsAggregator) RecordInfrastructureViolation(violation string) {
	violation = strings.TrimSpace(violation)
	if violation == "" {
		return
	}
	a.mu.Lock()
	a.violations = append(a.violations, violation)
	a.notify()
	a.mu.Unlock()
}

func (a *metricsAggregator) Changes() <-chan struct{} {
	return a.changed
}

func (a *metricsAggregator) reject(workerID string, err error) error {
	violation := fmt.Sprintf("worker %s metrics: %v", workerID, err)
	a.violations = append(a.violations, violation)
	a.notify()
	return errors.New(violation)
}

func (a *metricsAggregator) notify() {
	select {
	case a.changed <- struct{}{}:
	default:
	}
}

func isActiveSlice(load *loadtestv1.LoadSlice) bool {
	if load == nil {
		return false
	}
	if load.GetModel() == loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS {
		return load.GetVirtualUsers() > 0
	}
	return load.GetModel() == loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE &&
		load.GetRate() > 0 && load.GetMaxInFlight() > 0
}
