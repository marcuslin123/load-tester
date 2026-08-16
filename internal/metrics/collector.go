package metrics

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/marcuslin123/load-tester/internal/protocol"
)

type eventKind uint8

const (
	resultEvent eventKind = iota
	snapshotEvent
	closeEvent
)

type event struct {
	kind     eventKind
	result   protocol.Result
	reply    chan Snapshot
	dropped  uint64
	admitted uint64
	unmet    uint64
}

type arrivalCounters struct {
	writers  atomic.Uint64
	sealed   atomic.Bool
	admitted atomic.Uint64
	unmet    atomic.Uint64
}

// Collector aggregates request results on one goroutine so request goroutines
// never contend on histogram or counter locks.
type Collector struct {
	events    chan event
	dropped   atomic.Uint64
	arrivals  atomic.Pointer[arrivalCounters]
	closed    atomic.Bool
	controlMu sync.Mutex
	closeOnce sync.Once
	final     Snapshot
}

// NewCollector starts a collector with space for bufferSize pending results.
func NewCollector(bufferSize int) (*Collector, error) {
	if bufferSize <= 0 {
		return nil, errors.New("metrics buffer size must be greater than zero")
	}

	collector := &Collector{events: make(chan event, bufferSize)}
	collector.arrivals.Store(&arrivalCounters{})
	go collector.run()
	return collector, nil
}

// Offer queues a result without blocking the caller. It returns false when the
// collector is closed or its buffer is full.
func (c *Collector) Offer(result protocol.Result) bool {
	if c.closed.Load() {
		return false
	}
	return offerResult(c.events, &c.dropped, result)
}

// OfferAdmittedArrival records a scheduled arrival after it acquires an
// in-flight slot, independently of request completion reporting.
func (c *Collector) OfferAdmittedArrival() bool {
	return c.recordArrival(func(counters *arrivalCounters) {
		counters.admitted.Add(1)
	})
}

// OfferUnmetDemand records a scheduled arrival that could not become a request.
func (c *Collector) OfferUnmetDemand() bool {
	return c.recordArrival(func(counters *arrivalCounters) {
		counters.unmet.Add(1)
	})
}

// SnapshotAndReset returns the completed interval after all results queued
// before the snapshot command have been aggregated.
func (c *Collector) SnapshotAndReset() Snapshot {
	c.controlMu.Lock()
	defer c.controlMu.Unlock()

	if c.closed.Load() {
		return newSnapshot()
	}

	dropped := c.dropped.Swap(0)
	arrivals := c.rotateArrivalCounters()
	reply := make(chan Snapshot, 1)
	c.events <- event{
		kind:     snapshotEvent,
		reply:    reply,
		dropped:  dropped,
		admitted: arrivals.admitted.Load(),
		unmet:    arrivals.unmet.Load(),
	}
	return <-reply
}

// Close drains results accepted before shutdown and returns their final metrics.
// Call it only after all request-producing goroutines have stopped.
func (c *Collector) Close() Snapshot {
	c.closeOnce.Do(func() {
		c.controlMu.Lock()
		defer c.controlMu.Unlock()

		c.closed.Store(true)
		dropped := c.dropped.Swap(0)
		arrivals := c.rotateArrivalCounters()
		reply := make(chan Snapshot, 1)
		c.events <- event{
			kind:     closeEvent,
			reply:    reply,
			dropped:  dropped,
			admitted: arrivals.admitted.Load(),
			unmet:    arrivals.unmet.Load(),
		}
		c.final = <-reply
	})

	return cloneSnapshot(c.final)
}

// run owns all mutable histogram and counter state. Snapshot and close events
// act as FIFO barriers behind every result accepted before them.
func (c *Collector) run() {
	snapshot := newSnapshot()
	for next := range c.events {
		switch next.kind {
		case resultEvent:
			recordResult(&snapshot, next.result)
		case snapshotEvent:
			c.snapshotCounters(&snapshot, next)
			next.reply <- snapshot
			snapshot = newSnapshot()
		case closeEvent:
			c.snapshotCounters(&snapshot, next)
			next.reply <- snapshot
			return
		}
	}
}

func offerResult(events chan<- event, dropped *atomic.Uint64, result protocol.Result) bool {
	select {
	case events <- event{kind: resultEvent, result: result}:
		return true
	default:
		dropped.Add(1)
		return false
	}
}

func (c *Collector) snapshotCounters(snapshot *Snapshot, boundary event) {
	snapshot.DroppedSamples = boundary.dropped
	snapshot.AdmittedArrivals = boundary.admitted
	snapshot.UnmetDemand = boundary.unmet
}

// recordArrival uses a lock-free generation handoff so request goroutines never
// wait for the result queue or the snapshot consumer.
func (c *Collector) recordArrival(record func(*arrivalCounters)) bool {
	for {
		if c.closed.Load() {
			return false
		}
		counters := c.currentArrivalCounters()
		counters.writers.Add(1)
		if counters.sealed.Load() || c.closed.Load() {
			counters.writers.Add(^uint64(0))
			continue
		}
		record(counters)
		counters.writers.Add(^uint64(0))
		return true
	}
}

func (c *Collector) currentArrivalCounters() *arrivalCounters {
	if counters := c.arrivals.Load(); counters != nil {
		return counters
	}
	fresh := &arrivalCounters{}
	if c.arrivals.CompareAndSwap(nil, fresh) {
		return fresh
	}
	return c.arrivals.Load()
}

// rotateArrivalCounters is the linearization point for admitted and unmet
// arrivals. Existing writers finish in the sealed generation; later writers
// retry against the new generation without taking a lock.
func (c *Collector) rotateArrivalCounters() *arrivalCounters {
	current := c.currentArrivalCounters()
	current.sealed.Store(true)
	c.arrivals.Store(&arrivalCounters{})
	for current.writers.Load() != 0 {
		runtime.Gosched()
	}
	return current
}

func newSnapshot() Snapshot {
	return Snapshot{
		StatusCodes: make(map[int]uint64),
		latencies: hdrhistogram.New(
			lowestTrackableLatencyMicros,
			highestTrackableLatencyMicros,
			latencySignificantFigures,
		),
	}
}

// recordResult assigns each request to exactly one outcome category while still
// preserving any HTTP status received before a transport failure.
func recordResult(snapshot *Snapshot, result protocol.Result) {
	snapshot.Requests++
	latencyMicros := max(result.Latency.Microseconds(), lowestTrackableLatencyMicros)
	if err := snapshot.latencies.RecordValue(latencyMicros); err != nil {
		panic("metrics: latency exceeded configured histogram range: " + err.Error())
	}
	if result.BytesRead > 0 {
		snapshot.BytesRead += uint64(result.BytesRead)
	}
	if result.StatusCode > 0 {
		snapshot.StatusCodes[result.StatusCode]++
	}

	switch {
	case result.Err != nil:
		snapshot.TransportErrors++
		snapshot.Failed++
	case result.StatusCode >= 400 && result.StatusCode < 500:
		snapshot.ClientErrors++
		snapshot.Failed++
	case result.StatusCode >= 500:
		snapshot.ServerErrors++
		snapshot.Failed++
	default:
		snapshot.Succeeded++
	}
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	clone := snapshot
	if snapshot.latencies != nil {
		clone.latencies = hdrhistogram.Import(snapshot.latencies.Export())
	}
	clone.StatusCodes = make(map[int]uint64, len(snapshot.StatusCodes))
	for status, count := range snapshot.StatusCodes {
		clone.StatusCodes[status] = count
	}
	return clone
}
