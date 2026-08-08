package metrics

import (
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// EncodeLatencyHistogram returns raw HDR V2 compressed bytes suitable for a
// protobuf bytes field rather than the library's base64 text-log wrapper.
func EncodeLatencyHistogram(snapshot Snapshot) ([]byte, error) {
	histogram := snapshot.latencies
	if histogram == nil {
		histogram = newSnapshot().latencies
	}
	encodedText, err := histogram.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		return nil, fmt.Errorf("encode HDR V2 histogram: %w", err)
	}
	raw := make([]byte, base64.StdEncoding.DecodedLen(len(encodedText)))
	written, err := base64.StdEncoding.Decode(raw, encodedText)
	if err != nil {
		return nil, fmt.Errorf("decode HDR base64 wrapper: %w", err)
	}
	return raw[:written], nil
}

// DecodeLatencyHistogram restores a snapshot containing the raw histogram. Its
// counters remain zero for the caller to populate from the protobuf envelope.
func DecodeLatencyHistogram(raw []byte) (Snapshot, error) {
	if len(raw) == 0 {
		return Snapshot{}, errors.New("decode latency histogram: payload is empty")
	}
	encodedText := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
	base64.StdEncoding.Encode(encodedText, raw)
	histogram, err := hdrhistogram.Decode(encodedText)
	if err != nil {
		return Snapshot{}, fmt.Errorf("decode HDR V2 histogram: %w", err)
	}
	snapshot := newSnapshot()
	snapshot.latencies = histogram
	return snapshot, nil
}
