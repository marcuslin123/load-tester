package orchestrator

import (
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/config"
)

func TestSplitLoadDividesVirtualUsersEvenly(t *testing.T) {
	slices, err := splitLoad(config.Load{
		Model:        config.LoadConstantVUs,
		VirtualUsers: 100,
		RampUp:       5 * time.Second,
	}, []string{"worker-b", "worker-a"})
	if err != nil {
		t.Fatalf("split load: %v", err)
	}

	assertLoadSlice(t, slices["worker-a"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS, 50, 0, 0, 5*time.Second)
	assertLoadSlice(t, slices["worker-b"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS, 50, 0, 0, 5*time.Second)
}

func TestSplitLoadDistributesRemainderDeterministically(t *testing.T) {
	slices, err := splitLoad(config.Load{
		Model:        config.LoadConstantVUs,
		VirtualUsers: 100,
	}, []string{"worker-c", "worker-a", "worker-b"})
	if err != nil {
		t.Fatalf("split load: %v", err)
	}

	assertLoadSlice(t, slices["worker-a"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS, 34, 0, 0, 0)
	assertLoadSlice(t, slices["worker-b"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS, 33, 0, 0, 0)
	assertLoadSlice(t, slices["worker-c"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS, 33, 0, 0, 0)
}

func TestSplitLoadCapsOpenModelWorkersByMaxInFlight(t *testing.T) {
	slices, err := splitLoad(config.Load{
		Model:       config.LoadConstantRate,
		Rate:        10,
		MaxInFlight: 2,
		RampUp:      2 * time.Second,
	}, []string{"worker-c", "worker-a", "worker-b"})
	if err != nil {
		t.Fatalf("split load: %v", err)
	}

	assertLoadSlice(t, slices["worker-a"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE, 0, 5, 1, 2*time.Second)
	assertLoadSlice(t, slices["worker-b"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE, 0, 5, 1, 2*time.Second)
	assertLoadSlice(t, slices["worker-c"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE, 0, 0, 0, 2*time.Second)
}

func TestSplitLoadLeavesSurplusClosedModelWorkersIdle(t *testing.T) {
	slices, err := splitLoad(config.Load{
		Model:        config.LoadConstantVUs,
		VirtualUsers: 2,
	}, []string{"worker-a", "worker-b", "worker-c"})
	if err != nil {
		t.Fatalf("split load: %v", err)
	}

	assertLoadSlice(t, slices["worker-a"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS, 1, 0, 0, 0)
	assertLoadSlice(t, slices["worker-b"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS, 1, 0, 0, 0)
	assertLoadSlice(t, slices["worker-c"], loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS, 0, 0, 0, 0)
}

func assertLoadSlice(
	t *testing.T,
	slice *loadtestv1.LoadSlice,
	model loadtestv1.LoadModel,
	virtualUsers uint64,
	rate uint64,
	maxInFlight uint64,
	rampUp time.Duration,
) {
	t.Helper()
	if slice == nil {
		t.Fatal("load slice is nil")
	}
	if slice.GetModel() != model {
		t.Errorf("model = %v, want %v", slice.GetModel(), model)
	}
	if slice.GetVirtualUsers() != virtualUsers {
		t.Errorf("virtual users = %d, want %d", slice.GetVirtualUsers(), virtualUsers)
	}
	if slice.GetRate() != rate {
		t.Errorf("rate = %d, want %d", slice.GetRate(), rate)
	}
	if slice.GetMaxInFlight() != maxInFlight {
		t.Errorf("max in flight = %d, want %d", slice.GetMaxInFlight(), maxInFlight)
	}
	if slice.GetRampUp() == nil || slice.GetRampUp().AsDuration() != rampUp {
		t.Errorf("ramp up = %v, want %v", slice.GetRampUp(), rampUp)
	}
}
