package metrics

import (
	"fmt"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	lowestTrackableLatencyMicros  = int64(1)
	highestTrackableLatencyMicros = int64((time.Duration(1<<63 - 1)) / time.Microsecond)
	latencySignificantFigures     = 3
)

// Snapshot contains the metrics collected during one reporting interval.
type Snapshot struct {
	Requests        uint64
	Succeeded       uint64
	Failed          uint64
	TransportErrors uint64
	ClientErrors    uint64
	ServerErrors    uint64
	BytesRead       uint64
	DroppedSamples  uint64
	UnmetDemand     uint64
	StatusCodes     map[int]uint64

	latencies *hdrhistogram.Histogram
}

// ObservationCount is the number of request latencies represented by the histogram.
func (s Snapshot) ObservationCount() uint64 {
	if s.latencies == nil {
		return 0
	}
	return uint64(s.latencies.TotalCount())
}

// Percentile returns the observed latency at the requested percentile. Failed
// requests are included because their wait time is part of the target's behavior.
func (s Snapshot) Percentile(percentile float64) time.Duration {
	if s.latencies == nil || s.latencies.TotalCount() == 0 {
		return 0
	}
	return time.Duration(s.latencies.ValueAtPercentile(percentile)) * time.Microsecond
}

// Merge adds interval histograms and counters so percentiles describe the full
// request population rather than an average of worker percentiles.
func Merge(snapshots ...Snapshot) (Snapshot, error) {
	merged := newSnapshot()
	for _, snapshot := range snapshots {
		counters := []struct {
			name  string
			value *uint64
			add   uint64
		}{
			{name: "requests", value: &merged.Requests, add: snapshot.Requests},
			{name: "succeeded", value: &merged.Succeeded, add: snapshot.Succeeded},
			{name: "failed", value: &merged.Failed, add: snapshot.Failed},
			{name: "transport errors", value: &merged.TransportErrors, add: snapshot.TransportErrors},
			{name: "client errors", value: &merged.ClientErrors, add: snapshot.ClientErrors},
			{name: "server errors", value: &merged.ServerErrors, add: snapshot.ServerErrors},
			{name: "bytes read", value: &merged.BytesRead, add: snapshot.BytesRead},
			{name: "dropped samples", value: &merged.DroppedSamples, add: snapshot.DroppedSamples},
			{name: "unmet demand", value: &merged.UnmetDemand, add: snapshot.UnmetDemand},
		}
		for _, counter := range counters {
			if counter.add > ^uint64(0)-*counter.value {
				return Snapshot{}, fmt.Errorf("merge %s: uint64 overflow", counter.name)
			}
			*counter.value += counter.add
		}
		for status, count := range snapshot.StatusCodes {
			if count > ^uint64(0)-merged.StatusCodes[status] {
				return Snapshot{}, fmt.Errorf("merge HTTP status %d: uint64 overflow", status)
			}
			merged.StatusCodes[status] += count
		}

		if snapshot.latencies == nil {
			continue
		}
		if dropped := merged.latencies.Merge(snapshot.latencies); dropped != 0 {
			return Snapshot{}, fmt.Errorf("merge latency histogram: %d values were outside the configured range", dropped)
		}
	}
	return merged, nil
}
