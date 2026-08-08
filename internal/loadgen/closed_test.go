package loadgen

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

func TestRunClosedLimitsConcurrentRequestsToVirtualUsers(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	executor := newBlockingProtocol(9)
	sink := &countingSink{}
	done := make(chan error, 1)

	go func() {
		done <- RunClosed(ctx, executor, sink, ClosedOptions{VirtualUsers: 8})
	}()

	for range 8 {
		waitForSignal(t, executor.started)
	}
	select {
	case <-executor.started:
		t.Fatal("more than 8 requests became concurrent")
	case <-time.After(20 * time.Millisecond):
	}

	if got := executor.maximum.Load(); got != 8 {
		t.Errorf("maximum concurrent requests = %d, want 8", got)
	}
	cancel()
	if err := waitForRun(t, done); err != nil {
		t.Fatalf("RunClosed() error = %v", err)
	}
}

func TestRunClosedStaggersUsersAcrossRampUp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	executor := newBlockingProtocol(4)
	done := make(chan error, 1)
	go func() {
		done <- RunClosed(ctx, executor, &countingSink{}, ClosedOptions{
			VirtualUsers: 4,
			RampUp:       time.Hour,
		})
	}()

	waitForSignal(t, executor.started)
	select {
	case <-executor.started:
		t.Fatal("a second virtual user started near the beginning of a long ramp-up")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	if err := waitForRun(t, done); err != nil {
		t.Fatalf("RunClosed() error = %v", err)
	}
	if got := executor.maximum.Load(); got != 1 {
		t.Errorf("started virtual users = %d, want 1 before cancellation", got)
	}
}

func TestRunClosedStartsUsersAlreadyDueFromAbsoluteStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	executor := newBlockingProtocol(3)
	done := make(chan error, 1)
	go func() {
		done <- RunClosed(ctx, executor, &countingSink{}, ClosedOptions{
			VirtualUsers: 3,
			RampUp:       time.Hour,
			StartAt:      time.Now().Add(-time.Hour),
		})
	}()

	for range 3 {
		waitForSignal(t, executor.started)
	}
	cancel()
	if err := waitForRun(t, done); err != nil {
		t.Fatalf("RunClosed() error = %v", err)
	}
}

func TestRunClosedRecordsCompletedResultWhenCancellationRacesWithReturn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	sink := &countingSink{}
	executor := protocolFunc(func(context.Context) protocol.Result {
		cancel()
		return protocol.Result{Latency: time.Millisecond, StatusCode: 200}
	})

	if err := RunClosed(ctx, executor, sink, ClosedOptions{VirtualUsers: 1}); err != nil {
		t.Fatalf("RunClosed() error = %v", err)
	}
	if got := sink.count.Load(); got != 1 {
		t.Errorf("recorded results = %d, want completed result recorded", got)
	}
}

func TestRunClosedExcludesRequestInterruptedByRunCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	sink := &countingSink{}
	executor := protocolFunc(func(context.Context) protocol.Result {
		cancel()
		return protocol.Result{Err: fmt.Errorf("execute request: %w", context.Canceled)}
	})

	if err := RunClosed(ctx, executor, sink, ClosedOptions{VirtualUsers: 1}); err != nil {
		t.Fatalf("RunClosed() error = %v", err)
	}
	if got := sink.count.Load(); got != 0 {
		t.Errorf("recorded results = %d, want cancelled request excluded", got)
	}
}

func TestRunClosedContinuesAfterRequestFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	sink := &countingSink{}
	var calls atomic.Uint64
	executor := protocolFunc(func(context.Context) protocol.Result {
		if calls.Add(1) == 1 {
			return protocol.Result{Latency: time.Millisecond, Err: errors.New("connection reset")}
		}
		cancel()
		return protocol.Result{Latency: time.Millisecond, StatusCode: 200}
	})

	if err := RunClosed(ctx, executor, sink, ClosedOptions{VirtualUsers: 1}); err != nil {
		t.Fatalf("RunClosed() error = %v", err)
	}
	if got := sink.count.Load(); got != 2 {
		t.Errorf("recorded results = %d, want failure and following success", got)
	}
}

func TestRunClosedValidatesOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options ClosedOptions
	}{
		{name: "zero virtual users", options: ClosedOptions{VirtualUsers: 0}},
		{name: "negative virtual users", options: ClosedOptions{VirtualUsers: -1}},
		{name: "negative ramp-up", options: ClosedOptions{VirtualUsers: 1, RampUp: -time.Second}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := RunClosed(context.Background(), protocolFunc(func(context.Context) protocol.Result {
				return protocol.Result{}
			}), &countingSink{}, test.options)
			if err == nil {
				t.Fatal("RunClosed() error = nil, want validation error")
			}
		})
	}
}

func TestRunClosedRequiresExecutorAndSink(t *testing.T) {
	t.Parallel()

	executor := protocolFunc(func(context.Context) protocol.Result { return protocol.Result{} })
	options := ClosedOptions{VirtualUsers: 1}
	if err := RunClosed(context.Background(), nil, &countingSink{}, options); err == nil {
		t.Error("RunClosed() with nil executor error = nil, want validation error")
	}
	if err := RunClosed(context.Background(), executor, nil, options); err == nil {
		t.Error("RunClosed() with nil sink error = nil, want validation error")
	}
}

func TestRampDelaySpansEntireRampUp(t *testing.T) {
	t.Parallel()

	want := []time.Duration{0, time.Second, 2 * time.Second, 3 * time.Second}
	for user, wantDelay := range want {
		if got := rampDelay(user, len(want), 3*time.Second); got != wantDelay {
			t.Errorf("rampDelay(%d) = %v, want %v", user, got, wantDelay)
		}
	}
}

type blockingProtocol struct {
	started chan struct{}
	current atomic.Int64
	maximum atomic.Int64
}

func newBlockingProtocol(signalBuffer int) *blockingProtocol {
	return &blockingProtocol{started: make(chan struct{}, signalBuffer)}
}

func (p *blockingProtocol) Execute(ctx context.Context) protocol.Result {
	current := p.current.Add(1)
	for maximum := p.maximum.Load(); current > maximum; maximum = p.maximum.Load() {
		if p.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	p.started <- struct{}{}
	<-ctx.Done()
	p.current.Add(-1)
	return protocol.Result{Err: ctx.Err()}
}

type countingSink struct {
	count atomic.Uint64
}

type protocolFunc func(context.Context) protocol.Result

func (f protocolFunc) Execute(ctx context.Context) protocol.Result {
	return f(ctx)
}

func (s *countingSink) Offer(protocol.Result) bool {
	s.count.Add(1)
	return true
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for request to start")
	}
}

func waitForRun(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduler to stop")
		return nil
	}
}
