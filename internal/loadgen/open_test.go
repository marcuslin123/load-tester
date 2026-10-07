package loadgen

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

func TestRunOpenRejectsDemandAtMaxInFlight(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	executor := newBlockingProtocol(3)
	sink := newOpenCountingSink()
	done := make(chan error, 1)
	go func() {
		done <- RunOpen(ctx, executor, sink, OpenOptions{
			Rate:        10_000,
			MaxInFlight: 2,
		})
	}()

	for range 2 {
		waitForSignal(t, executor.started)
	}
	waitForSignal(t, sink.unmetSignal)
	if got := executor.maximum.Load(); got != 2 {
		t.Errorf("maximum concurrent requests = %d, want 2", got)
	}

	cancel()
	if err := waitForRun(t, done); err != nil {
		t.Fatalf("RunOpen() error = %v", err)
	}
	if got := sink.unmet.Load(); got == 0 {
		t.Error("unmet demand = 0, want arrivals rejected at capacity")
	}
	if got := sink.results.Load(); got != 0 {
		t.Errorf("recorded results = %d, want cancellation-interrupted requests excluded", got)
	}
}

func TestRunOpenReportsAdmissionBeforeRequestCompletion(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := newBlockingProtocol(1)
	sink := newOpenCountingSink()
	done := make(chan error, 1)
	go func() {
		done <- RunOpen(ctx, executor, sink, OpenOptions{Rate: 10_000, MaxInFlight: 1})
	}()

	waitForSignal(t, executor.started)
	if got := sink.admitted.Load(); got != 1 {
		t.Fatalf("admitted arrivals before request completion = %d, want 1", got)
	}
	if got := sink.results.Load(); got != 0 {
		t.Fatalf("completed request results = %d, want 0 while request blocks", got)
	}

	cancel()
	if err := waitForRun(t, done); err != nil {
		t.Fatalf("RunOpen() error = %v", err)
	}
}

func TestArrivalOffsetUsesAbsoluteConstantRateSchedule(t *testing.T) {
	t.Parallel()

	options := OpenOptions{Rate: 4}
	want := []time.Duration{0, 250 * time.Millisecond, 500 * time.Millisecond, 750 * time.Millisecond, time.Second}
	for sequence, wantOffset := range want {
		if got := arrivalOffset(int64(sequence), options); got != wantOffset {
			t.Errorf("arrivalOffset(%d) = %v, want %v", sequence, got, wantOffset)
		}
	}
}

func TestArrivalOffsetLinearlyRampsRate(t *testing.T) {
	t.Parallel()

	options := OpenOptions{Rate: 4, RampUp: 2 * time.Second}
	tests := []struct {
		sequence int64
		want     time.Duration
	}{
		{sequence: 0, want: time.Second},
		{sequence: 1, want: 1414213562 * time.Nanosecond},
		{sequence: 3, want: 2 * time.Second},
		{sequence: 4, want: 2250 * time.Millisecond},
	}
	for _, test := range tests {
		got := arrivalOffset(test.sequence, options)
		if difference := got - test.want; difference < -time.Nanosecond || difference > time.Nanosecond {
			t.Errorf("arrivalOffset(%d) = %v, want approximately %v", test.sequence, got, test.want)
		}
	}
}

func TestRunOpenUsesAbsoluteDeadlines(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	started := time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	clock := &scriptedClock{now: started, arrivals: 5, cancel: cancel}
	sink := newOpenCountingSink()
	executor := protocolFunc(func(context.Context) protocol.Result {
		return protocol.Result{StatusCode: 200}
	})

	err := runOpen(ctx, executor, sink, OpenOptions{Rate: 4, MaxInFlight: 10}, clock)
	if err != nil {
		t.Fatalf("runOpen() error = %v", err)
	}

	want := []time.Duration{0, 250 * time.Millisecond, 500 * time.Millisecond, 750 * time.Millisecond, time.Second}
	deadlines := clock.recordedDeadlines()
	if len(deadlines) != len(want) {
		t.Fatalf("scheduled deadlines = %d, want %d", len(deadlines), len(want))
	}
	for index, wantOffset := range want {
		if got := deadlines[index].Sub(started); got != wantOffset {
			t.Errorf("deadline %d offset = %v, want %v", index, got, wantOffset)
		}
	}
}

func TestRunOpenExcludesArrivalAtContextDeadline(t *testing.T) {
	t.Parallel()

	started := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), started.Add(time.Second))
	defer cancel()
	clock := &scriptedClock{now: started, arrivals: 3, cancel: cancel}
	sink := newOpenCountingSink()
	executor := protocolFunc(func(context.Context) protocol.Result {
		return protocol.Result{StatusCode: 200}
	})

	if err := runOpen(ctx, executor, sink, OpenOptions{
		Rate:        2,
		MaxInFlight: 10,
		StartAt:     started,
	}, clock); err != nil {
		t.Fatalf("runOpen() error = %v", err)
	}
	if got := sink.admitted.Load(); got != 2 {
		t.Fatalf("admitted arrivals = %d, want only deadlines in [start, context deadline)", got)
	}
	want := []time.Time{started, started.Add(500 * time.Millisecond)}
	if got := clock.recordedDeadlines(); !reflect.DeepEqual(got, want) {
		t.Fatalf("scheduled deadlines = %v, want %v", got, want)
	}
}

func TestRunOpenSkipsStaleDeadlinesInsteadOfBurstingToCatchUp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	started := time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	clock := &laggingClock{
		now:       started,
		lagOnWait: 1,
		lag:       750 * time.Millisecond,
		arrivals:  3,
		cancel:    cancel,
	}
	executor := protocolFunc(func(context.Context) protocol.Result {
		return protocol.Result{StatusCode: 200}
	})

	if err := runOpen(ctx, executor, newOpenCountingSink(), OpenOptions{Rate: 4, MaxInFlight: 10}, clock); err != nil {
		t.Fatalf("runOpen() error = %v", err)
	}

	want := []time.Time{
		started,
		started.Add(750 * time.Millisecond),
		started.Add(time.Second),
	}
	if got := clock.recordedDeadlines(); !reflect.DeepEqual(got, want) {
		t.Fatalf("scheduled deadlines after lag = %v, want stale deadlines skipped as %v", got, want)
	}
}

func TestRunOpenSkipsArrivalsBeforeAbsoluteStartPosition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	clock := &scriptedClock{now: started.Add(2500 * time.Millisecond), arrivals: 3, cancel: cancel}
	executor := protocolFunc(func(context.Context) protocol.Result {
		return protocol.Result{StatusCode: 200}
	})

	err := runOpen(ctx, executor, newOpenCountingSink(), OpenOptions{
		Rate:        2,
		MaxInFlight: 2,
		StartAt:     started,
	}, clock)
	if err != nil {
		t.Fatalf("runOpen() error = %v", err)
	}

	want := []time.Time{
		started.Add(2500 * time.Millisecond),
		started.Add(3 * time.Second),
		started.Add(3500 * time.Millisecond),
	}
	deadlines := clock.recordedDeadlines()
	if len(deadlines) != len(want) {
		t.Fatalf("scheduled deadlines = %v, want %v", deadlines, want)
	}
	for index := range want {
		if !deadlines[index].Equal(want[index]) {
			t.Errorf("deadline %d = %v, want %v", index, deadlines[index], want[index])
		}
	}
}

func TestRunOpenShardsOneGlobalConstantRateSchedule(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	got := collectShardDeadlines(t, started, OpenOptions{
		Rate:               3,
		GlobalRate:         5,
		ScheduleShardIndex: 0,
		ScheduleShardCount: 2,
		MaxInFlight:        10,
	}, 3)
	got = append(got, collectShardDeadlines(t, started, OpenOptions{
		Rate:               2,
		GlobalRate:         5,
		ScheduleShardIndex: 1,
		ScheduleShardCount: 2,
		MaxInFlight:        10,
	}, 2)...)
	sort.Slice(got, func(left, right int) bool { return got[left].Before(got[right]) })

	want := []time.Time{
		started,
		started.Add(200 * time.Millisecond),
		started.Add(400 * time.Millisecond),
		started.Add(600 * time.Millisecond),
		started.Add(800 * time.Millisecond),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("shard deadline union = %v, want %v", got, want)
	}
}

func TestRunOpenShardsOneGlobalRampSchedule(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	options := []OpenOptions{
		{Rate: 2, GlobalRate: 5, ScheduleShardIndex: 0, ScheduleShardCount: 3, MaxInFlight: 10, RampUp: 2 * time.Second},
		{Rate: 2, GlobalRate: 5, ScheduleShardIndex: 1, ScheduleShardCount: 3, MaxInFlight: 10, RampUp: 2 * time.Second},
		{Rate: 1, GlobalRate: 5, ScheduleShardIndex: 2, ScheduleShardCount: 3, MaxInFlight: 10, RampUp: 2 * time.Second},
	}
	arrivals := []int{3, 3, 3}
	var got []time.Time
	for index, option := range options {
		got = append(got, collectShardDeadlines(t, started, option, arrivals[index])...)
	}
	sort.Slice(got, func(left, right int) bool { return got[left].Before(got[right]) })

	want := make([]time.Time, 9)
	for sequence := range want {
		want[sequence] = started.Add(arrivalOffset(int64(sequence), OpenOptions{Rate: 5, RampUp: 2 * time.Second}))
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ramped shard deadline union = %v, want %v", got, want)
	}
}

func TestRunOpenStartsAtFirstCurrentSequenceOwnedByShard(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	clock := &scriptedClock{now: started.Add(250 * time.Millisecond), arrivals: 2, cancel: cancel}
	err := runOpen(ctx, protocolFunc(func(context.Context) protocol.Result { return protocol.Result{} }), newOpenCountingSink(), OpenOptions{
		Rate:               2,
		GlobalRate:         5,
		ScheduleShardIndex: 1,
		ScheduleShardCount: 2,
		MaxInFlight:        1,
		StartAt:            started,
	}, clock)
	if err != nil {
		t.Fatalf("runOpen() error = %v", err)
	}

	want := []time.Time{started.Add(600 * time.Millisecond), started.Add(time.Second)}
	if got := clock.recordedDeadlines(); !reflect.DeepEqual(got, want) {
		t.Errorf("owned deadlines after late start = %v, want %v", got, want)
	}
}

func TestScheduledArrivalsBetweenUsesAbsoluteSchedule(t *testing.T) {
	started := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		from   time.Time
		to     time.Time
		rate   int
		rampUp time.Duration
		want   uint64
	}{
		{
			name: "constant rate half-open interval",
			from: started.Add(250 * time.Millisecond),
			to:   started.Add(1250 * time.Millisecond),
			rate: 2,
			want: 2,
		},
		{
			name:   "during ramp",
			from:   started,
			to:     started.Add(2 * time.Second),
			rate:   4,
			rampUp: 2 * time.Second,
			want:   3,
		},
		{
			name:   "after ramp",
			from:   started.Add(2 * time.Second),
			to:     started.Add(3 * time.Second),
			rate:   4,
			rampUp: 2 * time.Second,
			want:   4,
		},
		{
			name: "clips before run start",
			from: started.Add(-time.Second),
			to:   started.Add(500 * time.Millisecond),
			rate: 2,
			want: 1,
		},
		{
			name: "empty reversed interval",
			from: started.Add(time.Second),
			to:   started,
			rate: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ScheduledArrivalsBetween(started, test.from, test.to, test.rate, test.rampUp); got != test.want {
				t.Fatalf("ScheduledArrivalsBetween() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestScheduledShardArrivalsBetweenPartitionsGlobalCount(t *testing.T) {
	started := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	from := started.Add(250 * time.Millisecond)
	to := started.Add(1250 * time.Millisecond)

	first := ScheduledShardArrivalsBetween(started, from, to, 5, 0, 0, 2)
	second := ScheduledShardArrivalsBetween(started, from, to, 5, 0, 1, 2)
	if first != 3 || second != 2 || first+second != ScheduledArrivalsBetween(started, from, to, 5, 0) {
		t.Fatalf("shard counts = (%d, %d), want (3, 2) and global equality", first, second)
	}
}

func TestRunOpenRequestMeasuresFromIntendedArrival(t *testing.T) {
	t.Parallel()

	intended := time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	clock := &scriptedClock{now: intended.Add(75 * time.Millisecond)}
	sink := newOpenCountingSink()
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	var requests sync.WaitGroup
	requests.Add(1)

	runOpenRequest(
		context.Background(),
		protocolFunc(func(context.Context) protocol.Result {
			return protocol.Result{Latency: 5 * time.Millisecond, StatusCode: 200}
		}),
		sink,
		intended,
		slots,
		&requests,
		clock,
	)
	requests.Wait()

	result := <-sink.resultSignal
	if result.Latency != 75*time.Millisecond {
		t.Errorf("Latency = %v, want 75ms from intended arrival", result.Latency)
	}
	if got := len(slots); got != 0 {
		t.Errorf("occupied semaphore slots = %d, want released slot", got)
	}
}

func TestRunOpenValidatesOptionsAndDependencies(t *testing.T) {
	t.Parallel()

	executor := protocolFunc(func(context.Context) protocol.Result { return protocol.Result{} })
	sink := newOpenCountingSink()
	tests := []struct {
		name     string
		executor protocol.Protocol
		sink     OpenSink
		options  OpenOptions
	}{
		{name: "zero rate", executor: executor, sink: sink, options: OpenOptions{MaxInFlight: 1}},
		{name: "zero max in flight", executor: executor, sink: sink, options: OpenOptions{Rate: 1}},
		{name: "negative ramp-up", executor: executor, sink: sink, options: OpenOptions{Rate: 1, MaxInFlight: 1, RampUp: -time.Second}},
		{name: "negative global rate", executor: executor, sink: sink, options: OpenOptions{Rate: 1, GlobalRate: -1, MaxInFlight: 1}},
		{name: "negative shard index", executor: executor, sink: sink, options: OpenOptions{Rate: 1, MaxInFlight: 1, ScheduleShardIndex: -1, ScheduleShardCount: 1}},
		{name: "negative shard count", executor: executor, sink: sink, options: OpenOptions{Rate: 1, MaxInFlight: 1, ScheduleShardCount: -1}},
		{name: "shard index equals count", executor: executor, sink: sink, options: OpenOptions{Rate: 1, MaxInFlight: 1, ScheduleShardIndex: 2, ScheduleShardCount: 2}},
		{name: "shards without global rate", executor: executor, sink: sink, options: OpenOptions{Rate: 1, MaxInFlight: 1, ScheduleShardCount: 2}},
		{name: "global rate without shard count", executor: executor, sink: sink, options: OpenOptions{Rate: 1, GlobalRate: 2, MaxInFlight: 1}},
		{name: "missing executor", sink: sink, options: OpenOptions{Rate: 1, MaxInFlight: 1}},
		{name: "missing sink", executor: executor, options: OpenOptions{Rate: 1, MaxInFlight: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := RunOpen(ctx, test.executor, test.sink, test.options); err == nil {
				t.Fatal("RunOpen() error = nil, want validation error")
			}
		})
	}
}

func collectShardDeadlines(t *testing.T, started time.Time, options OpenOptions, arrivals int) []time.Time {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clock := &scriptedClock{now: started, arrivals: arrivals, cancel: cancel}
	err := runOpen(ctx, protocolFunc(func(context.Context) protocol.Result { return protocol.Result{} }), newOpenCountingSink(), options, clock)
	if err != nil {
		t.Fatalf("runOpen() error = %v", err)
	}
	return clock.recordedDeadlines()
}

func TestRunOpenDoesNotAdmitArrivalAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	clock := &cancelOnWaitClock{
		now:    time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC),
		cancel: cancel,
	}
	var calls atomic.Uint64
	executor := protocolFunc(func(context.Context) protocol.Result {
		calls.Add(1)
		return protocol.Result{StatusCode: 200}
	})

	sink := newOpenCountingSink()
	err := runOpen(ctx, executor, sink, OpenOptions{Rate: 1, MaxInFlight: 1}, clock)
	if err != nil {
		t.Fatalf("runOpen() error = %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("executed requests = %d, want no admission after cancellation", got)
	}
	if got := sink.admitted.Load(); got != 0 {
		t.Errorf("admitted arrivals = %d, want no admission after cancellation", got)
	}
}

type openCountingSink struct {
	results      atomic.Uint64
	admitted     atomic.Uint64
	unmet        atomic.Uint64
	resultSignal chan protocol.Result
	unmetSignal  chan struct{}
}

type scriptedClock struct {
	mu        sync.Mutex
	now       time.Time
	deadlines []time.Time
	arrivals  int
	cancel    context.CancelFunc
}

type cancelOnWaitClock struct {
	now    time.Time
	cancel context.CancelFunc
	waited bool
}

type laggingClock struct {
	mu        sync.Mutex
	now       time.Time
	deadlines []time.Time
	waits     int
	lagOnWait int
	lag       time.Duration
	arrivals  int
	cancel    context.CancelFunc
}

func (c *cancelOnWaitClock) Now() time.Time {
	return c.now
}

func (c *cancelOnWaitClock) WaitUntil(context.Context, time.Time) bool {
	if c.waited {
		return false
	}
	c.waited = true
	c.cancel()
	return true
}

func (c *scriptedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *scriptedClock) WaitUntil(_ context.Context, intended time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.deadlines) == c.arrivals {
		c.cancel()
		return false
	}
	c.deadlines = append(c.deadlines, intended)
	c.now = intended
	return true
}

func (c *scriptedClock) recordedDeadlines() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.deadlines...)
}

func (c *laggingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *laggingClock) WaitUntil(_ context.Context, intended time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.deadlines) == c.arrivals {
		c.cancel()
		return false
	}
	c.waits++
	c.deadlines = append(c.deadlines, intended)
	c.now = intended
	if c.waits == c.lagOnWait {
		c.now = c.now.Add(c.lag)
	}
	return true
}

func (c *laggingClock) recordedDeadlines() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.deadlines...)
}

func newOpenCountingSink() *openCountingSink {
	return &openCountingSink{
		resultSignal: make(chan protocol.Result, 16),
		unmetSignal:  make(chan struct{}, 1),
	}
}

func (s *openCountingSink) Offer(result protocol.Result) bool {
	s.results.Add(1)
	s.resultSignal <- result
	return true
}

func (s *openCountingSink) OfferAdmittedArrival() bool {
	s.admitted.Add(1)
	return true
}

func (s *openCountingSink) OfferUnmetDemand() bool {
	s.unmet.Add(1)
	select {
	case s.unmetSignal <- struct{}{}:
	default:
	}
	return true
}
