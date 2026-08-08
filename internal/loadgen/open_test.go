package loadgen

import (
	"context"
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
		{name: "missing executor", sink: sink, options: OpenOptions{Rate: 1, MaxInFlight: 1}},
		{name: "missing sink", executor: executor, options: OpenOptions{Rate: 1, MaxInFlight: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := RunOpen(context.Background(), test.executor, test.sink, test.options); err == nil {
				t.Fatal("RunOpen() error = nil, want validation error")
			}
		})
	}
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

	err := runOpen(ctx, executor, newOpenCountingSink(), OpenOptions{Rate: 1, MaxInFlight: 1}, clock)
	if err != nil {
		t.Fatalf("runOpen() error = %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("executed requests = %d, want no admission after cancellation", got)
	}
}

type openCountingSink struct {
	results      atomic.Uint64
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

func (s *openCountingSink) OfferUnmetDemand() bool {
	s.unmet.Add(1)
	select {
	case s.unmetSignal <- struct{}{}:
	default:
	}
	return true
}
