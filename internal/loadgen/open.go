package loadgen

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

// OpenSink records completed requests and scheduled arrivals rejected before
// they could become requests.
type OpenSink interface {
	ResultSink
	OfferUnmetDemand() bool
}

// OpenOptions controls the arrival rate and the worker's concurrency ceiling.
type OpenOptions struct {
	Rate        int
	MaxInFlight int
	RampUp      time.Duration
}

type scheduleClock interface {
	Now() time.Time
	WaitUntil(context.Context, time.Time) bool
}

type realScheduleClock struct{}

// RunOpen schedules arrivals independently of response completion until the
// caller cancels ctx.
func RunOpen(ctx context.Context, executor protocol.Protocol, sink OpenSink, options OpenOptions) error {
	return runOpen(ctx, executor, sink, options, realScheduleClock{})
}

func runOpen(
	ctx context.Context,
	executor protocol.Protocol,
	sink OpenSink,
	options OpenOptions,
	clock scheduleClock,
) error {
	if err := validateOpen(executor, sink, options); err != nil {
		return err
	}

	started := clock.Now()
	slots := make(chan struct{}, options.MaxInFlight)
	var requests sync.WaitGroup
	for sequence := int64(0); ; sequence++ {
		intended := started.Add(arrivalOffset(sequence, options))
		if !clock.WaitUntil(ctx, intended) || ctx.Err() != nil {
			break
		}

		select {
		case slots <- struct{}{}:
			requests.Add(1)
			go runOpenRequest(ctx, executor, sink, intended, slots, &requests, clock)
		default:
			sink.OfferUnmetDemand()
		}
	}
	requests.Wait()
	return nil
}

func validateOpen(executor protocol.Protocol, sink OpenSink, options OpenOptions) error {
	if options.Rate <= 0 {
		return errors.New("rate must be greater than zero")
	}
	if options.MaxInFlight <= 0 {
		return errors.New("max in flight must be greater than zero")
	}
	if options.RampUp < 0 {
		return errors.New("ramp-up must be greater than or equal to zero")
	}
	if executor == nil {
		return errors.New("protocol executor is required")
	}
	if sink == nil {
		return errors.New("open result sink is required")
	}
	return nil
}

func runOpenRequest(
	ctx context.Context,
	executor protocol.Protocol,
	sink OpenSink,
	intended time.Time,
	slots chan struct{},
	requests *sync.WaitGroup,
	clock scheduleClock,
) {
	defer requests.Done()
	defer func() { <-slots }()

	result := executor.Execute(ctx)
	if interruptedByRun(ctx, result) {
		return
	}
	result.Latency = clock.Now().Sub(intended)
	sink.Offer(result)
}

// arrivalOffset inverts the cumulative arrival curve so every deadline remains
// anchored to the original run start rather than the previous timer firing.
func arrivalOffset(sequence int64, options OpenOptions) time.Duration {
	if options.RampUp == 0 {
		return time.Duration(sequence) * time.Second / time.Duration(options.Rate)
	}

	rampSeconds := options.RampUp.Seconds()
	targetCount := float64(sequence + 1)
	rampCount := float64(options.Rate) * rampSeconds / 2
	if targetCount <= rampCount {
		seconds := math.Sqrt(2 * targetCount * rampSeconds / float64(options.Rate))
		return time.Duration(seconds * float64(time.Second))
	}
	seconds := rampSeconds + (targetCount-rampCount)/float64(options.Rate)
	return time.Duration(seconds * float64(time.Second))
}

func (realScheduleClock) Now() time.Time {
	return time.Now()
}

func (realScheduleClock) WaitUntil(ctx context.Context, intended time.Time) bool {
	delay := time.Until(intended)
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
