package loadgen

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

// OpenSink records completed requests and every scheduled arrival outcome.
type OpenSink interface {
	ResultSink
	OfferAdmittedArrival() bool
	OfferUnmetDemand() bool
}

// OpenOptions controls the arrival rate and the worker's concurrency ceiling.
type OpenOptions struct {
	Rate               int
	GlobalRate         int
	ScheduleShardIndex int
	ScheduleShardCount int
	MaxInFlight        int
	RampUp             time.Duration
	StartAt            time.Time
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

// RunOpenWithRequestContext stops admitting arrivals when scheduleCtx ends but
// lets admitted requests use requestCtx until they complete or the run ends.
func RunOpenWithRequestContext(
	scheduleCtx context.Context,
	requestCtx context.Context,
	executor protocol.Protocol,
	sink OpenSink,
	options OpenOptions,
) error {
	return runOpenWithContexts(scheduleCtx, requestCtx, executor, sink, options, realScheduleClock{})
}

func runOpen(
	ctx context.Context,
	executor protocol.Protocol,
	sink OpenSink,
	options OpenOptions,
	clock scheduleClock,
) error {
	return runOpenWithContexts(ctx, ctx, executor, sink, options, clock)
}

func runOpenWithContexts(
	scheduleCtx context.Context,
	requestCtx context.Context,
	executor protocol.Protocol,
	sink OpenSink,
	options OpenOptions,
	clock scheduleClock,
) error {
	options = normalizeOpenOptions(options)
	if err := validateOpen(executor, sink, options); err != nil {
		return err
	}
	if scheduleCtx == nil {
		return errors.New("schedule context is required")
	}
	if requestCtx == nil {
		return errors.New("request context is required")
	}

	started := options.StartAt
	if started.IsZero() {
		started = clock.Now()
	}
	schedule := options
	schedule.Rate = options.GlobalRate
	slots := make(chan struct{}, options.MaxInFlight)
	var requests sync.WaitGroup
	sequence := firstSequenceForShard(firstArrivalSequence(started, clock.Now(), schedule), options)
	for {
		intended := started.Add(arrivalOffset(sequence, schedule))
		if deadline, ok := scheduleCtx.Deadline(); ok && !intended.Before(deadline) {
			break
		}
		if !clock.WaitUntil(scheduleCtx, intended) || scheduleCtx.Err() != nil {
			break
		}

		select {
		case slots <- struct{}{}:
			sink.OfferAdmittedArrival()
			requests.Add(1)
			go runOpenRequest(requestCtx, executor, sink, intended, slots, &requests, clock)
		default:
			sink.OfferUnmetDemand()
		}
		next := sequence + int64(options.ScheduleShardCount)
		current := firstSequenceForShard(firstArrivalSequence(started, clock.Now(), schedule), options)
		if current > next {
			next = current
		}
		sequence = next
	}
	requests.Wait()
	return nil
}

func normalizeOpenOptions(options OpenOptions) OpenOptions {
	if options.GlobalRate == 0 && options.ScheduleShardCount == 0 && options.ScheduleShardIndex == 0 {
		options.GlobalRate = options.Rate
		options.ScheduleShardCount = 1
	}
	return options
}

func firstSequenceForShard(first int64, options OpenOptions) int64 {
	shardCount := int64(options.ScheduleShardCount)
	shardIndex := int64(options.ScheduleShardIndex)
	remainder := first % shardCount
	if remainder == shardIndex {
		return first
	}
	return first + (shardIndex-remainder+shardCount)%shardCount
}

// firstArrivalSequence finds the first arrival that has not already passed,
// preserving the original schedule when a replacement assignment starts late.
func firstArrivalSequence(started, now time.Time, options OpenOptions) int64 {
	elapsed := now.Sub(started)
	if elapsed <= 0 {
		return 0
	}

	high := int64(1)
	for arrivalOffset(high, options) < elapsed {
		high *= 2
	}
	low := int64(0)
	for low < high {
		middle := low + (high-low)/2
		if arrivalOffset(middle, options) < elapsed {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// ScheduledArrivalsBetween counts absolute open-model deadlines in [from, to),
// including the same ramp curve used by the live scheduler.
func ScheduledArrivalsBetween(
	started time.Time,
	from time.Time,
	to time.Time,
	rate int,
	rampUp time.Duration,
) uint64 {
	if started.IsZero() || !to.After(from) || rate <= 0 || rampUp < 0 {
		return 0
	}
	options := OpenOptions{Rate: rate, RampUp: rampUp}
	first := firstArrivalSequence(started, from, options)
	after := firstArrivalSequence(started, to, options)
	if after <= first {
		return 0
	}
	return uint64(after - first)
}

// ScheduledShardArrivalsBetween counts deadlines owned by one global schedule shard.
func ScheduledShardArrivalsBetween(
	started time.Time,
	from time.Time,
	to time.Time,
	globalRate int,
	rampUp time.Duration,
	shardIndex int,
	shardCount int,
) uint64 {
	if started.IsZero() || !to.After(from) || globalRate <= 0 || rampUp < 0 ||
		shardIndex < 0 || shardCount <= 0 || shardIndex >= shardCount {
		return 0
	}
	options := OpenOptions{
		Rate:               globalRate,
		GlobalRate:         globalRate,
		ScheduleShardIndex: shardIndex,
		ScheduleShardCount: shardCount,
	}
	first := firstSequenceForShard(firstArrivalSequence(started, from, options), options)
	after := firstArrivalSequence(started, to, options)
	if first >= after {
		return 0
	}
	return uint64((after-1-first)/int64(shardCount) + 1)
}

func validateOpen(executor protocol.Protocol, sink OpenSink, options OpenOptions) error {
	if options.Rate <= 0 {
		return errors.New("rate must be greater than zero")
	}
	if options.GlobalRate <= 0 {
		return errors.New("global rate must be greater than zero")
	}
	if options.ScheduleShardIndex < 0 {
		return errors.New("schedule shard index must be greater than or equal to zero")
	}
	if options.ScheduleShardCount <= 0 {
		return errors.New("schedule shard count must be greater than zero")
	}
	if options.ScheduleShardIndex >= options.ScheduleShardCount {
		return errors.New("schedule shard index must be less than schedule shard count")
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
