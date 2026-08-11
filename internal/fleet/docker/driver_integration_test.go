package docker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/fleet"
	"github.com/marcuslin123/load-tester/internal/orchestrator"
	containertypes "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/grpc"
)

func TestDockerDriverLifecycle(t *testing.T) {
	if os.Getenv("LOAD_TESTER_DOCKER_INTEGRATION") != "1" {
		t.Skip("set LOAD_TESTER_DOCKER_INTEGRATION=1 to run the Docker integration test")
	}
	image := os.Getenv("LOAD_TESTER_WORKER_IMAGE")
	if image == "" {
		t.Fatal("LOAD_TESTER_WORKER_IMAGE is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	api, err := client.New(client.FromEnv, client.WithUserAgent("load-tester-integration-test"))
	if err != nil {
		t.Fatalf("create Docker client: %v", err)
	}
	t.Cleanup(func() { _ = api.Close() })

	runID := fmt.Sprintf("piece16-%d", time.Now().UnixNano())
	networkName := runID + "-network"
	if _, err := api.NetworkCreate(ctx, networkName, client.NetworkCreateOptions{Driver: "bridge"}); err != nil {
		t.Fatalf("create caller-owned network: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = api.NetworkRemove(cleanupCtx, networkName, client.NetworkRemoveOptions{})
	})

	control, address := startIntegrationOrchestrator(t)
	driver, err := New(ctx, Options{
		RunID:               runID,
		Image:               image,
		Network:             networkName,
		OrchestratorAddress: address,
		AddHostGateway:      true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = driver.Teardown(cleanupCtx)
		_ = driver.Close()
	})

	if err := driver.Scale(ctx, 3); err != nil {
		t.Fatalf("Scale(3) error = %v", err)
	}
	assertRunningContainers(t, ctx, api, runID, 3)
	waitForRegisteredWorkerIDs(t, control, []string{runID + "-worker-1", runID + "-worker-2", runID + "-worker-3"})

	if err := driver.Scale(ctx, 1); err != nil {
		t.Fatalf("Scale(1) error = %v", err)
	}
	assertRunningContainers(t, ctx, api, runID, 1)
	waitForRegisteredWorkerIDs(t, control, []string{runID + "-worker-1"})

	if err := driver.Teardown(ctx); err != nil {
		t.Fatalf("first Teardown() error = %v", err)
	}
	if err := driver.Teardown(ctx); err != nil {
		t.Fatalf("second Teardown() error = %v", err)
	}
	if err := driver.Scale(ctx, 1); !errors.Is(err, fleet.ErrClosed) {
		t.Fatalf("Scale() after teardown error = %v, want fleet.ErrClosed", err)
	}
	workers, err := driver.List(ctx)
	if err != nil {
		t.Fatalf("List() after teardown error = %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("workers after teardown = %v, want none", workers)
	}
	assertNoOwnedContainers(t, ctx, api, runID)
}

func startIntegrationOrchestrator(t *testing.T) (*orchestrator.Server, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen for orchestrator: %v", err)
	}
	control, err := orchestrator.NewServer(orchestrator.Options{HeartbeatInterval: 100 * time.Millisecond})
	if err != nil {
		listener.Close()
		t.Fatalf("NewServer() error = %v", err)
	}
	server := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(server, control)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split orchestrator address: %v", err)
	}
	return control, net.JoinHostPort("host.docker.internal", port)
}

func assertRunningContainers(t *testing.T, ctx context.Context, api *client.Client, runID string, count int) {
	t.Helper()
	filters := client.Filters{}.
		Add("label", managedLabel+"=true").
		Add("label", runIDLabel+"="+runID)
	result, err := api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		t.Fatalf("list owned containers directly: %v", err)
	}
	if len(result.Items) != count {
		t.Fatalf("owned containers = %d, want %d", len(result.Items), count)
	}
	for _, container := range result.Items {
		if container.State != containertypes.StateRunning {
			t.Fatalf("container %s state = %s, want running", container.ID, container.State)
		}
	}
}

func waitForRegisteredWorkerIDs(t *testing.T, control *orchestrator.Server, want []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		workers := control.ActiveWorkers()
		got := make([]string, 0, len(workers))
		for _, worker := range workers {
			got = append(got, worker.ID)
		}
		sort.Strings(got)
		if reflect.DeepEqual(got, want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("registered workers = %v, want %v", control.ActiveWorkers(), want)
}

func assertNoOwnedContainers(t *testing.T, ctx context.Context, api *client.Client, runID string) {
	t.Helper()
	filters := client.Filters{}.
		Add("label", managedLabel+"=true").
		Add("label", runIDLabel+"="+runID)
	result, err := api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		t.Fatalf("list owned containers directly: %v", err)
	}
	if len(result.Items) != 0 {
		t.Fatalf("Docker still has %d owned containers after teardown", len(result.Items))
	}
}
