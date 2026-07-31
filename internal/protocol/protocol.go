package protocol

import (
	"context"
	"time"
)

// Protocol executes one target request and reports its measured outcome.
type Protocol interface {
	Execute(ctx context.Context) Result
}

// Result contains the measurements and outcome of one target request.
type Result struct {
	Latency    time.Duration
	StatusCode int
	BytesRead  int64
	Err        error
}
