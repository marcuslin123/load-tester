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
	revision uint64
	startsAt time.Time
	deadline time.Time
	active   bool
}

type workerMetricState struct {
	lastSequence   uint64
	lastDelta      *loadtestv1.MetricsDelta
	lastInterval   time.Time
	lastRevision   uint64
	closedRevision uint64
	finalRevision  uint64
	snapshot       metrics.Snapshot
}

// metricsAggregator validates and atomically merges worker deltas for one run.
type metricsAggregator struct {
	mu sync.Mutex

	runID       string
	assignments map[uint64]map[string]assignmentMetricWindow
	latest      map[string]assignmentMetricWindow
	workers     map[string]*workerMetricState
	snapshot    metrics.Snapshot
	violations  []string
	changed     chan struct{}
}

func newMetricsAggregator(runID string) *metricsAggregator {
	return &metricsAggregator{
		runID:       runID,
		assignments: make(map[uint64]map[string]assignmentMetricWindow),
		latest:      make(map[string]assignmentMetricWindow),
		workers:     make(map[string]*workerMetricState),
		changed:     make(chan struct{}, 1),
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
		window := assignmentMetricWindow{
			revision: revision,
			startsAt: assignment.GetStartsAt().AsTime(),
			deadline: assignment.GetDeadline().AsTime(),
			active:   isActiveSlice(assignment.GetLoad()),
		}
		windows[workerID] = window
		latest[workerID] = window
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
	if delta.GetAssignmentRevision() < state.lastRevision {
		return a.reject(workerID, fmt.Errorf("assignment revision %d is older than accepted revision %d", delta.GetAssignmentRevision(), state.lastRevision))
	}
	if delta.GetAssignmentRevision() == state.closedRevision {
		return a.reject(workerID, fmt.Errorf("assignment revision %d already sent its partial delta", delta.GetAssignmentRevision()))
	}

	requireContiguous := a.requiresContiguousInterval(workerID, state.lastRevision, delta.GetAssignmentRevision())
	latest := a.latest[workerID]
	allowRevisionPartial := latest.revision > delta.GetAssignmentRevision()
	intervalStart, intervalEnd, err := validateMetricInterval(
		delta,
		window,
		state.lastInterval,
		requireContiguous,
		allowRevisionPartial,
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
	state.snapshot = workerMerged
	state.lastSequence = delta.GetSequence()
	state.lastDelta = proto.Clone(delta).(*loadtestv1.MetricsDelta)
	state.lastInterval = intervalEnd
	state.lastRevision = delta.GetAssignmentRevision()
	if delta.GetFinalForRevision() {
		state.closedRevision = delta.GetAssignmentRevision()
	}
	if latest, ok := a.latest[workerID]; ok &&
		latest.revision == delta.GetAssignmentRevision() &&
		latest.active && delta.GetFinalForRevision() && intervalEnd.Equal(latest.deadline) {
		state.finalRevision = latest.revision
	}
	a.workers[workerID] = state
	_ = intervalStart
	a.notify()
	return nil
}

// requiresContiguousInterval relaxes continuity only when assignment history
// explicitly shows that this worker had no load and therefore emitted no delta.
func (a *metricsAggregator) requiresContiguousInterval(workerID string, previous, current uint64) bool {
	if previous == 0 {
		return false
	}
	for revision := previous; revision <= current; revision++ {
		windows, recorded := a.assignments[revision]
		if !recorded {
			continue
		}
		window, assigned := windows[workerID]
		if !assigned || !window.active {
			return false
		}
		if revision == current {
			break
		}
	}
	return true
}

func validateMetricInterval(
	delta *loadtestv1.MetricsDelta,
	window assignmentMetricWindow,
	lastInterval time.Time,
	requireContiguous bool,
	allowRevisionPartial bool,
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
	if start.Before(window.startsAt) || end.After(window.deadline) {
		return time.Time{}, time.Time{}, errors.New("metrics interval is outside the assigned run window")
	}
	periodicBoundary := end.Sub(window.startsAt) > 0 && end.Sub(window.startsAt)%time.Second == 0
	atDeadline := end.Equal(window.deadline)
	if delta.GetFinalForRevision() {
		if !atDeadline && !allowRevisionPartial {
			return time.Time{}, time.Time{}, errors.New("final metric delta requires the deadline or a replaced revision")
		}
	} else if atDeadline || !periodicBoundary {
		return time.Time{}, time.Time{}, errors.New("non-final metric delta must end at a reporting boundary before the deadline")
	}
	if !lastInterval.IsZero() {
		if start.Before(lastInterval) {
			return time.Time{}, time.Time{}, fmt.Errorf("metrics interval starts at %s before previous end %s", start, lastInterval)
		}
		if requireContiguous && !start.Equal(lastInterval) {
			return time.Time{}, time.Time{}, fmt.Errorf("metrics interval starts at %s instead of previous end %s", start, lastInterval)
		}
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
		if state == nil || state.finalRevision != latest.revision {
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
