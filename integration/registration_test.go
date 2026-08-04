package integration_test

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/orchestrator"
	workerclient "github.com/marcuslin123/load-tester/internal/worker"
	"google.golang.org/grpc"
)

func TestWorkerRegistersAndHeartbeatsOverTCP(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	var serverLogs bytes.Buffer
	control, err := orchestrator.NewServer(orchestrator.Options{
		HeartbeatInterval: 10 * time.Millisecond,
		Logger:            log.New(&serverLogs, "", 0),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	grpcServer := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(grpcServer, control)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workerclient.Run(ctx, workerclient.Config{
			Address:         listener.Addr().String(),
			WorkerID:        "integration-worker",
			Hostname:        "integration-host",
			SoftwareVersion: "test",
		}, workerclient.Options{Logger: log.New(io.Discard, "", 0)})
	}()

	worker := waitForRegisteredHeartbeat(t, control, "integration-worker", 2)
	if worker.Hostname != "integration-host" || worker.State != loadtestv1.WorkerState_WORKER_STATE_IDLE {
		t.Fatalf("worker snapshot = %+v, want registered idle worker", worker)
	}
	if !strings.Contains(serverLogs.String(), "worker registered: id=integration-worker") {
		t.Fatalf("server logs = %q, want registration log", serverLogs.String())
	}

	cancel()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("worker Run() error after cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
	waitForWorkerRemoval(t, control)
}

func waitForRegisteredHeartbeat(
	t *testing.T,
	server *orchestrator.Server,
	workerID string,
	minimumSequence uint64,
) orchestrator.WorkerSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, worker := range server.ActiveWorkers() {
			if worker.ID == workerID && worker.HeartbeatSequence >= minimumSequence {
				return worker
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("worker %q did not register and reach heartbeat sequence %d", workerID, minimumSequence)
	return orchestrator.WorkerSnapshot{}
}

func waitForWorkerRemoval(t *testing.T, server *orchestrator.Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(server.ActiveWorkers()) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("active workers = %v, want none after cancellation", server.ActiveWorkers())
}
