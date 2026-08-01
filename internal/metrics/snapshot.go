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
	StatusCodes     map[int]uint64

	latencies *hdrhistogram.Histogram
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
		merged.Requests += snapshot.Requests
		merged.Succeeded += snapshot.Succeeded
		merged.Failed += snapshot.Failed
		merged.TransportErrors += snapshot.TransportErrors
		merged.ClientErrors += snapshot.ClientErrors
		merged.ServerErrors += snapshot.ServerErrors
		merged.BytesRead += snapshot.BytesRead
		merged.DroppedSamples += snapshot.DroppedSamples
		for status, count := range snapshot.StatusCodes {
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
