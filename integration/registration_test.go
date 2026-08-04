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
	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/orchestrator"
	workerclient "github.com/marcuslin123/load-tester/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func TestOrchestratorAssignsComplementarySlicesOverTCP(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control, err := orchestrator.NewServer(orchestrator.Options{
		HeartbeatInterval: time.Second,
		Logger:            log.New(io.Discard, "", 0),
		Assignment: &orchestrator.AssignmentOptions{
			Context: ctx,
			Config: config.Config{
				Name: "tcp-assignment",
				Target: config.Target{
					Protocol: config.ProtocolHTTP,
					URL:      "http://target:8080/work",
					Method:   "GET",
					Headers:  map[string]string{},
				},
				Load: config.Load{
					Model:        config.LoadConstantVUs,
					VirtualUsers: 100,
					Duration:     30 * time.Second,
				},
				Fleet: config.Fleet{MinWorkers: 2, MaxWorkers: 3},
			},
			RunID:    "tcp-run",
			LeadTime: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	grpcServer := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(grpcServer, control)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer connection.Close()
	client := loadtestv1.NewWorkerControlClient(connection)
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer streamCancel()
	first := registerTCPWorker(t, streamCtx, client, "worker-a")
	second := registerTCPWorker(t, streamCtx, client, "worker-b")

	firstAssignment := receiveTCPAssignment(t, first)
	secondAssignment := receiveTCPAssignment(t, second)
	if firstAssignment.GetLoad().GetVirtualUsers() != 50 || secondAssignment.GetLoad().GetVirtualUsers() != 50 {
		t.Fatalf(
			"virtual-user slices = (%d, %d), want (50, 50)",
			firstAssignment.GetLoad().GetVirtualUsers(),
			secondAssignment.GetLoad().GetVirtualUsers(),
		)
	}
	if firstAssignment.GetRunId() != "tcp-run" || secondAssignment.GetRunId() != "tcp-run" {
		t.Fatalf("run IDs = (%q, %q), want tcp-run", firstAssignment.GetRunId(), secondAssignment.GetRunId())
	}
	if firstAssignment.GetRevision() != 1 || secondAssignment.GetRevision() != 1 {
		t.Fatalf("revisions = (%d, %d), want 1", firstAssignment.GetRevision(), secondAssignment.GetRevision())
	}
	if !firstAssignment.GetStartsAt().AsTime().Equal(secondAssignment.GetStartsAt().AsTime()) {
		t.Fatal("workers received different start times")
	}
	if !firstAssignment.GetDeadline().AsTime().Equal(secondAssignment.GetDeadline().AsTime()) {
		t.Fatal("workers received different deadlines")
	}
}

func registerTCPWorker(
	t *testing.T,
	ctx context.Context,
	client loadtestv1.WorkerControlClient,
	workerID string,
) loadtestv1.WorkerControl_ConnectClient {
	t.Helper()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect(%s) error = %v", workerID, err)
	}
	if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Registration{
		Registration: &loadtestv1.Registration{WorkerId: workerID},
	}}); err != nil {
		t.Fatalf("Send(%s registration) error = %v", workerID, err)
	}
	message, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(%s ack) error = %v", workerID, err)
	}
	if message.GetRegistrationAck() == nil {
		t.Fatalf("first %s message = %T, want registration ack", workerID, message.GetPayload())
	}
	return stream
}

func receiveTCPAssignment(t *testing.T, stream loadtestv1.WorkerControl_ConnectClient) *loadtestv1.LoadAssignment {
	t.Helper()
	message, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(assignment) error = %v", err)
	}
	assignment := message.GetAssignment()
	if assignment == nil {
		t.Fatalf("message = %T, want assignment", message.GetPayload())
	}
	return assignment
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
