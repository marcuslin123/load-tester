package metrics

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

func TestCollectorClassifiesResults(t *testing.T) {
	t.Parallel()

	collector, err := NewCollector(6)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}

	results := []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200, BytesRead: 100},
		{Latency: 20 * time.Millisecond, StatusCode: 302, BytesRead: 200},
		{Latency: 30 * time.Millisecond, StatusCode: 404, BytesRead: 300},
		{Latency: 40 * time.Millisecond, StatusCode: 503, BytesRead: 400},
		{Latency: 50 * time.Millisecond, StatusCode: 200, BytesRead: 500, Err: errors.New("truncated response")},
		{Latency: 60 * time.Millisecond, Err: errors.New("connection refused")},
	}

	for _, result := range results {
		if !collector.Offer(result) {
			t.Fatal("Offer() dropped a result despite sufficient buffer capacity")
		}
	}

	snapshot := collector.Close()

	assertCounter(t, "Requests", snapshot.Requests, 6)
	assertCounter(t, "Succeeded", snapshot.Succeeded, 2)
	assertCounter(t, "Failed", snapshot.Failed, 4)
	assertCounter(t, "TransportErrors", snapshot.TransportErrors, 2)
	assertCounter(t, "ClientErrors", snapshot.ClientErrors, 1)
	assertCounter(t, "ServerErrors", snapshot.ServerErrors, 1)
	assertCounter(t, "BytesRead", snapshot.BytesRead, 1500)
	assertCounter(t, "DroppedSamples", snapshot.DroppedSamples, 0)

	wantStatuses := map[int]uint64{200: 2, 302: 1, 404: 1, 503: 1}
	for status, want := range wantStatuses {
		if got := snapshot.StatusCodes[status]; got != want {
			t.Errorf("StatusCodes[%d] = %d, want %d", status, got, want)
		}
	}
	if _, exists := snapshot.StatusCodes[0]; exists {
		t.Error("StatusCodes contains 0 for a request that received no HTTP response")
	}
}

func TestCollectorRecordsSuccessfulAndFailedLatencies(t *testing.T) {
	t.Parallel()

	collector, err := NewCollector(3)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}

	results := []protocol.Result{
		{Latency: 10 * time.Millisecond, StatusCode: 200},
		{Latency: 100 * time.Millisecond, StatusCode: 500},
		{Latency: time.Second, Err: errors.New("request timed out")},
	}
	for _, result := range results {
		if !collector.Offer(result) {
			t.Fatal("Offer() dropped a result despite sufficient buffer capacity")
		}
	}

	snapshot := collector.Close()
	if got := snapshot.Percentile(50); got < 99*time.Millisecond || got > 101*time.Millisecond {
		t.Errorf("Percentile(50) = %v, want approximately 100ms", got)
	}
	if got := snapshot.Percentile(100); got < 999*time.Millisecond || got > 1001*time.Millisecond {
		t.Errorf("Percentile(100) = %v, want approximately 1s", got)
	}
}

func TestSnapshotAndResetReturnsIndependentDeltas(t *testing.T) {
	t.Parallel()

	collector, err := NewCollector(2)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}

	for range 2 {
		if !collector.Offer(protocol.Result{Latency: 10 * time.Millisecond, StatusCode: 200}) {
			t.Fatal("Offer() dropped a result despite sufficient buffer capacity")
		}
	}
	first := collector.SnapshotAndReset()

	if !collector.Offer(protocol.Result{Latency: 100 * time.Millisecond, StatusCode: 503}) {
		t.Fatal("Offer() dropped a result despite sufficient buffer capacity")
	}
	second := collector.SnapshotAndReset()
	final := collector.Close()

	assertCounter(t, "first.Requests", first.Requests, 2)
	assertCounter(t, "first.Succeeded", first.Succeeded, 2)
	assertCounter(t, "second.Requests", second.Requests, 1)
	assertCounter(t, "second.ServerErrors", second.ServerErrors, 1)
	assertCounter(t, "final.Requests", final.Requests, 0)

	if got := first.Percentile(100); got < 9*time.Millisecond || got > 11*time.Millisecond {
		t.Errorf("first.Percentile(100) = %v, want approximately 10ms", got)
	}
	if got := second.Percentile(100); got < 99*time.Millisecond || got > 101*time.Millisecond {
		t.Errorf("second.Percentile(100) = %v, want approximately 100ms", got)
	}
	if first.StatusCodes[503] != 0 {
		t.Error("first delta changed after the collector began its next interval")
	}
}

func TestOfferResultDropsIncomingWhenBufferIsFull(t *testing.T) {
	t.Parallel()

	events := make(chan event, 1)
	queued := protocol.Result{StatusCode: 200}
	incoming := protocol.Result{StatusCode: 503}
	events <- event{kind: resultEvent, result: queued}
	var dropped atomic.Uint64

	if offerResult(events, &dropped, incoming) {
		t.Fatal("offerResult() accepted an incoming result into a full buffer")
	}
	if got := (<-events).result.StatusCode; got != queued.StatusCode {
		t.Fatalf("queued status = %d, want original status %d", got, queued.StatusCode)
	}
	assertCounter(t, "dropped", dropped.Load(), 1)
}

func TestCloseReportsDroppedResultsAndDrainsAcceptedResults(t *testing.T) {
	t.Parallel()

	collector, err := NewCollector(1)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}

	const offered = 100_000
	var accepted uint64
	for range offered {
		if collector.Offer(protocol.Result{Latency: time.Millisecond, StatusCode: 200}) {
			accepted++
		}
	}

	snapshot := collector.Close()
	wantDropped := uint64(offered) - accepted
	if wantDropped == 0 {
		t.Fatal("test did not fill the collector buffer")
	}
	assertCounter(t, "Requests", snapshot.Requests, accepted)
	assertCounter(t, "DroppedSamples", snapshot.DroppedSamples, wantDropped)
}

func TestSnapshotAndResetResetsDroppedSampleDelta(t *testing.T) {
	t.Parallel()

	collector, err := NewCollector(1)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	collector.dropped.Add(3)

	first := collector.SnapshotAndReset()
	second := collector.Close()

	assertCounter(t, "first.DroppedSamples", first.DroppedSamples, 3)
	assertCounter(t, "second.DroppedSamples", second.DroppedSamples, 0)
}

func TestCollectorSupportsConcurrentProducers(t *testing.T) {
	t.Parallel()

	collector, err := NewCollector(64)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}

	const (
		producers   = 16
		perProducer = 1_000
	)
	var accepted atomic.Uint64
	var group sync.WaitGroup
	group.Add(producers)
	for range producers {
		go func() {
			defer group.Done()
			for range perProducer {
				if collector.Offer(protocol.Result{Latency: time.Millisecond, StatusCode: 200}) {
					accepted.Add(1)
				}
			}
		}()
	}
	group.Wait()

	snapshot := collector.Close()
	wantAccepted := accepted.Load()
	assertCounter(t, "Requests", snapshot.Requests, wantAccepted)
	assertCounter(t, "DroppedSamples", snapshot.DroppedSamples, producers*perProducer-wantAccepted)
}

func TestCloseIsIdempotentAndOfferRejectsAfterClose(t *testing.T) {
	t.Parallel()

	collector, err := NewCollector(1)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	if !collector.Offer(protocol.Result{Latency: time.Millisecond, StatusCode: 200}) {
		t.Fatal("Offer() unexpectedly rejected the first result")
	}

	first := collector.Close()
	first.StatusCodes[200] = 999
	second := collector.Close()

	assertCounter(t, "second.Requests", second.Requests, 1)
	assertCounter(t, "second.StatusCodes[200]", second.StatusCodes[200], 1)
	if collector.Offer(protocol.Result{}) {
		t.Error("Offer() accepted a result after Close()")
	}
}

func TestNewCollectorRejectsNonPositiveBufferSize(t *testing.T) {
	t.Parallel()

	for _, bufferSize := range []int{0, -1} {
		if _, err := NewCollector(bufferSize); err == nil {
			t.Errorf("NewCollector(%d) error = nil, want validation error", bufferSize)
		}
	}
}

func assertCounter(t *testing.T, name string, got, want uint64) {
	t.Helper()

	if got != want {
		t.Errorf("%s = %d, want %d", name, got, want)
	}
}
