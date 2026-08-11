// Package fleet defines provider-neutral worker fleet operations.
package fleet

import (
	"context"
	"errors"
)

var ErrClosed = errors.New("fleet manager is closed")

// FleetManager controls worker capacity without exposing provider-specific APIs.
type FleetManager interface {
	Scale(ctx context.Context, desired int) error
	List(ctx context.Context) ([]WorkerInfo, error)
	Teardown(ctx context.Context) error
}

type WorkerState string

const (
	WorkerStateStarting WorkerState = "starting"
	WorkerStateRunning  WorkerState = "running"
	WorkerStateStopped  WorkerState = "stopped"
	WorkerStateUnknown  WorkerState = "unknown"
)

// WorkerInfo separates the stable load-tester identity from its provider resource.
type WorkerInfo struct {
	WorkerID   string
	ProviderID string
	State      WorkerState
}
