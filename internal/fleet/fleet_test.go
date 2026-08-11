package fleet

import (
	"context"
	"errors"
	"testing"
)

type stubManager struct{}

func (stubManager) Scale(context.Context, int) error           { return nil }
func (stubManager) List(context.Context) ([]WorkerInfo, error) { return nil, nil }
func (stubManager) Teardown(context.Context) error             { return nil }

func TestFleetContractExposesNormalizedWorkerIdentityAndState(t *testing.T) {
	var _ FleetManager = stubManager{}

	worker := WorkerInfo{
		WorkerID:   "run-1-worker-1",
		ProviderID: "container-abc",
		State:      WorkerStateRunning,
	}
	if worker.WorkerID != "run-1-worker-1" || worker.ProviderID != "container-abc" {
		t.Fatalf("worker identity = %+v, want stable and provider IDs", worker)
	}
	if worker.State != WorkerStateRunning {
		t.Fatalf("worker state = %q, want %q", worker.State, WorkerStateRunning)
	}
}

func TestWorkerStatesHaveStableNames(t *testing.T) {
	tests := map[WorkerState]string{
		WorkerStateStarting: "starting",
		WorkerStateRunning:  "running",
		WorkerStateStopped:  "stopped",
		WorkerStateUnknown:  "unknown",
	}
	for state, want := range tests {
		if got := string(state); got != want {
			t.Errorf("state = %q, want %q", got, want)
		}
	}
}

func TestErrClosedSupportsErrorsIs(t *testing.T) {
	wrapped := errors.Join(errors.New("scale fleet"), ErrClosed)
	if !errors.Is(wrapped, ErrClosed) {
		t.Fatalf("errors.Is(%v, ErrClosed) = false, want true", wrapped)
	}
}
