package worker

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newMetricsDelta converts one immutable collector snapshot into its complete
// wire representation; callers sequence only successfully constructed deltas.
func newMetricsDelta(
	workerID string,
	runID string,
	revision uint64,
	sequence uint64,
	intervalStart time.Time,
	intervalEnd time.Time,
	snapshot metrics.Snapshot,
	finalForRevision bool,
) (*loadtestv1.MetricsDelta, error) {
	if strings.TrimSpace(workerID) == "" {
		return nil, errors.New("metrics worker ID is required")
	}
	if strings.TrimSpace(runID) == "" {
		return nil, errors.New("metrics run ID is required")
	}
	if revision == 0 {
		return nil, errors.New("metrics assignment revision must be greater than zero")
	}
	if sequence == 0 {
		return nil, errors.New("metrics sequence must be greater than zero")
	}
	if !intervalEnd.After(intervalStart) {
		return nil, errors.New("metrics interval end must be after its start")
	}
	histogram, err := metrics.EncodeLatencyHistogram(snapshot)
	if err != nil {
		return nil, err
	}
	statusCodes := make(map[int32]uint64, len(snapshot.StatusCodes))
	for code, count := range snapshot.StatusCodes {
		if code < math.MinInt32 || code > math.MaxInt32 {
			return nil, fmt.Errorf("status code %d exceeds protobuf int32 range", code)
		}
		statusCodes[int32(code)] = count
	}
	return &loadtestv1.MetricsDelta{
		WorkerId:           workerID,
		RunId:              runID,
		AssignmentRevision: revision,
		Sequence:           sequence,
		IntervalStart:      timestamppb.New(intervalStart),
		IntervalEnd:        timestamppb.New(intervalEnd),
		Counters: &loadtestv1.MetricCounters{
			Requests:        snapshot.Requests,
			Succeeded:       snapshot.Succeeded,
			Failed:          snapshot.Failed,
			TransportErrors: snapshot.TransportErrors,
			ClientErrors:    snapshot.ClientErrors,
			ServerErrors:    snapshot.ServerErrors,
			BytesRead:       snapshot.BytesRead,
			DroppedSamples:  snapshot.DroppedSamples,
			UnmetDemand:     snapshot.UnmetDemand,
			StatusCodes:     statusCodes,
		},
		HistogramEncoding: loadtestv1.HistogramEncoding_HISTOGRAM_ENCODING_HDR_V2_COMPRESSED,
		LatencyHistogram:  histogram,
		FinalForRevision:  finalForRevision,
	}, nil
}
