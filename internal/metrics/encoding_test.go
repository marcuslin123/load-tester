package metrics

import (
	"bytes"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/protocol"
)

func TestLatencyHistogramRawV2RoundTrip(t *testing.T) {
	collector, err := NewCollector(8)
	if err != nil {
		t.Fatalf("NewCollector() error = %v", err)
	}
	for _, latency := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond} {
		if !collector.Offer(protocol.Result{Latency: latency, StatusCode: 200}) {
			t.Fatal("Offer() = false, want true")
		}
	}
	snapshot := collector.Close()

	encoded, err := EncodeLatencyHistogram(snapshot)
	if err != nil {
		t.Fatalf("EncodeLatencyHistogram() error = %v", err)
	}
	if bytes.HasPrefix(encoded, []byte("HIST")) {
		t.Fatalf("encoded histogram begins with base64 text %q, want raw bytes", encoded[:4])
	}
	decoded, err := DecodeLatencyHistogram(encoded)
	if err != nil {
		t.Fatalf("DecodeLatencyHistogram() error = %v", err)
	}
	if decoded.ObservationCount() != 3 {
		t.Fatalf("observation count = %d, want 3", decoded.ObservationCount())
	}
	if got := decoded.Percentile(50); got < 19*time.Millisecond || got > 21*time.Millisecond {
		t.Fatalf("p50 = %v, want approximately 20ms", got)
	}
	if got := decoded.Percentile(99); got < 29*time.Millisecond || got > 31*time.Millisecond {
		t.Fatalf("p99 = %v, want approximately 30ms", got)
	}
}

func TestLatencyHistogramRoundTripsEmptySnapshot(t *testing.T) {
	snapshot := newSnapshot()
	encoded, err := EncodeLatencyHistogram(snapshot)
	if err != nil {
		t.Fatalf("EncodeLatencyHistogram() error = %v", err)
	}
	decoded, err := DecodeLatencyHistogram(encoded)
	if err != nil {
		t.Fatalf("DecodeLatencyHistogram() error = %v", err)
	}
	if decoded.ObservationCount() != 0 {
		t.Fatalf("observation count = %d, want 0", decoded.ObservationCount())
	}
}

func TestDecodeLatencyHistogramRejectsMalformedBytes(t *testing.T) {
	if _, err := DecodeLatencyHistogram([]byte("not-an-hdr-histogram")); err == nil {
		t.Fatal("DecodeLatencyHistogram() error = nil, want malformed encoding error")
	}
}
