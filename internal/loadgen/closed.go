package loadgen

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

// ResultSink accepts completed request measurements without controlling whether
// the virtual user may continue generating load.
type ResultSink interface {
	Offer(protocol.Result) bool
}

// ClosedOptions controls the fixed user population and how gradually it starts.
type ClosedOptions struct {
	VirtualUsers int
	RampUp       time.Duration
	StartAt      time.Time
}

// RunClosed keeps one sequential request loop active per virtual user until the
// caller cancels ctx.
func RunClosed(ctx context.Context, executor protocol.Protocol, sink ResultSink, options ClosedOptions) error {
	if options.VirtualUsers <= 0 {
		return errors.New("virtual users must be greater than zero")
	}
	if options.RampUp < 0 {
		return errors.New("ramp-up must be greater than or equal to zero")
	}
	if executor == nil {
		return errors.New("protocol executor is required")
	}
	if sink == nil {
		return errors.New("result sink is required")
	}

	var users sync.WaitGroup
	rampStarted := options.StartAt
	if rampStarted.IsZero() {
		rampStarted = time.Now()
	}
	for user := range options.VirtualUsers {
		delay := rampDelay(user, options.VirtualUsers, options.RampUp)
		if !waitForRamp(ctx, time.Until(rampStarted.Add(delay))) {
			break
		}
		users.Add(1)
		go runVirtualUser(ctx, executor, sink, &users)
	}
	users.Wait()
	return nil
}

func runVirtualUser(ctx context.Context, executor protocol.Protocol, sink ResultSink, users *sync.WaitGroup) {
	defer users.Done()
	for ctx.Err() == nil {
		result := executor.Execute(ctx)
		if interruptedByRun(ctx, result) {
			return
		}
		// A full metrics sink may drop this sample, but it must not slow the user.
		sink.Offer(result)
	}
}

// interruptedByRun separates intentional shutdown from target and network
// failures that occurred while the run was active.
func interruptedByRun(ctx context.Context, result protocol.Result) bool {
	return ctx.Err() != nil && result.Err != nil && errors.Is(result.Err, ctx.Err())
}

// rampDelay places the first user at time zero and the last at the end of ramp-up.
func rampDelay(user, virtualUsers int, rampUp time.Duration) time.Duration {
	if rampUp <= 0 || virtualUsers <= 1 {
		return 0
	}
	return time.Duration(user) * rampUp / time.Duration(virtualUsers-1)
}

func waitForRamp(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
