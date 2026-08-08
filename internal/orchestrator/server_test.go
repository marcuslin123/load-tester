package orchestrator

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestServerRegistersHeartbeatAndRemovesWorker(t *testing.T) {
	t.Parallel()

	server, client := startTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	sendRegistration(t, stream, "worker-1")

	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(ack) error = %v", err)
	}
	if got := ack.GetRegistrationAck().GetWorkerId(); got != "worker-1" {
		t.Fatalf("ack worker ID = %q, want worker-1", got)
	}
	if got := ack.GetRegistrationAck().GetHeartbeatInterval().AsDuration(); got != 3*time.Second {
		t.Fatalf("heartbeat interval = %v, want 3s", got)
	}
	if got := ack.GetRegistrationAck().GetNextMetricsSequence(); got != 1 {
		t.Fatalf("next metrics sequence = %d, want 1", got)
	}

	worker := waitForWorker(t, server, "worker-1")
	if worker.Hostname != "test-host" || worker.SoftwareVersion != "test-version" {
		t.Fatalf("registered worker = %+v, want registration metadata", worker)
	}
	if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Heartbeat{
		Heartbeat: &loadtestv1.Heartbeat{
			WorkerId:         "worker-1",
			Sequence:         1,
			SentAt:           timestamppb.Now(),
			State:            loadtestv1.WorkerState_WORKER_STATE_IDLE,
			InFlightRequests: 0,
		},
	}}); err != nil {
		t.Fatalf("Send(heartbeat) error = %v", err)
	}

	worker = waitForWorkerSequence(t, server, "worker-1", 1)
	if worker.State != loadtestv1.WorkerState_WORKER_STATE_IDLE {
		t.Fatalf("worker state = %v, want IDLE", worker.State)
	}
	if worker.LastHeartbeat.IsZero() {
		t.Fatal("last heartbeat was not recorded")
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}
	waitForNoWorkers(t, server)
}

func TestServerRejectsInvalidFirstMessage(t *testing.T) {
	t.Parallel()

	_, client := startTestServer(t)
	stream, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Heartbeat{
		Heartbeat: &loadtestv1.Heartbeat{WorkerId: "worker-1"},
	}}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	_, err = stream.Recv()
	assertStatusCode(t, err, codes.FailedPrecondition)
}

func TestServerRejectsEmptyWorkerID(t *testing.T) {
	t.Parallel()

	_, client := startTestServer(t)
	stream, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	sendRegistration(t, stream, "   ")

	_, err = stream.Recv()
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestServerRejectsDuplicateActiveWorkerID(t *testing.T) {
	t.Parallel()

	_, client := startTestServer(t)
	first, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(first) error = %v", err)
	}
	sendRegistration(t, first, "worker-1")
	if _, err := first.Recv(); err != nil {
		t.Fatalf("Recv(first ack) error = %v", err)
	}

	duplicate, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(duplicate) error = %v", err)
	}
	sendRegistration(t, duplicate, "worker-1")
	_, err = duplicate.Recv()
	assertStatusCode(t, err, codes.AlreadyExists)
}

func TestServerRejectsNonIncreasingHeartbeatSequence(t *testing.T) {
	t.Parallel()

	_, client := startTestServer(t)
	stream, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	sendRegistration(t, stream, "worker-1")
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv(ack) error = %v", err)
	}
	heartbeat := func(sequence uint64) {
		t.Helper()
		if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Heartbeat{
			Heartbeat: &loadtestv1.Heartbeat{
				WorkerId: "worker-1",
				Sequence: sequence,
				SentAt:   timestamppb.Now(),
				State:    loadtestv1.WorkerState_WORKER_STATE_IDLE,
			},
		}}); err != nil {
			t.Fatalf("Send(heartbeat %d) error = %v", sequence, err)
		}
	}
	heartbeat(1)
	heartbeat(1)

	_, err = stream.Recv()
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestServerExpiresSilentWorkerAfterThreeHeartbeatIntervals(t *testing.T) {
	heartbeatInterval := 20 * time.Millisecond
	server, client := startTestServerWithOptions(t, Options{
		HeartbeatInterval: heartbeatInterval,
	}, serverDependencies{now: time.Now})
	stream, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	sendRegistration(t, stream, "worker-silent")
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv(ack) error = %v", err)
	}

	received := receiveAsync(stream)
	select {
	case result := <-received:
		assertStatusCode(t, result.err, codes.Unavailable)
	case <-time.After(10 * heartbeatInterval):
		t.Fatal("silent worker stream remained open beyond liveness timeout")
	}
	waitForNoWorkers(t, server)
}

func TestServerValidHeartbeatRenewsWorkerLiveness(t *testing.T) {
	heartbeatInterval := 50 * time.Millisecond
	server, client := startTestServerWithOptions(t, Options{
		HeartbeatInterval: heartbeatInterval,
	}, serverDependencies{now: time.Now})
	stream, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	sendRegistration(t, stream, "worker-heartbeat")
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv(ack) error = %v", err)
	}

	for sequence := uint64(1); sequence <= 3; sequence++ {
		time.Sleep(2 * heartbeatInterval)
		if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Heartbeat{
			Heartbeat: &loadtestv1.Heartbeat{
				WorkerId: "worker-heartbeat",
				Sequence: sequence,
				SentAt:   timestamppb.Now(),
				State:    loadtestv1.WorkerState_WORKER_STATE_IDLE,
			},
		}}); err != nil {
			t.Fatalf("Send(heartbeat %d) error = %v", sequence, err)
		}
		waitForWorkerSequence(t, server, "worker-heartbeat", sequence)
	}

	if workers := server.ActiveWorkers(); len(workers) != 1 {
		t.Fatalf("active workers = %v, want heartbeat-renewed worker", workers)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}
}

func TestServerAcceptedMetricsRenewWorkerLiveness(t *testing.T) {
	heartbeatInterval := 60 * time.Millisecond
	cfg := assignmentTestConfig(1)
	cfg.Load.Duration = 2 * time.Second
	server, client := startTestServerWithOptions(t, Options{
		HeartbeatInterval: heartbeatInterval,
		Assignment: &AssignmentOptions{
			Config:   cfg,
			RunID:    "test-run",
			LeadTime: 10 * time.Millisecond,
		},
	}, serverDependencies{now: time.Now})
	stream := registerAssignmentWorker(t, client, "worker-metrics")
	assignment := receiveAssignment(t, stream)

	time.Sleep(100 * time.Millisecond)
	delta := metricsDeltaForTest(
		t,
		"worker-metrics",
		assignment.GetRevision(),
		1,
		assignment.GetStartsAt().AsTime(),
		assignment.GetStartsAt().AsTime().Add(time.Second),
		nil,
	)
	delta.RunId = "test-run"
	if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Metrics{Metrics: delta}}); err != nil {
		t.Fatalf("Send(metrics) error = %v", err)
	}

	time.Sleep(120 * time.Millisecond)
	if workers := server.ActiveWorkers(); len(workers) != 1 {
		t.Fatalf("active workers = %v, want metrics-renewed worker", workers)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}
}

func TestServerReportsHeartbeatTimeoutAsActiveWorkerFailure(t *testing.T) {
	heartbeatInterval := 20 * time.Millisecond
	cfg := assignmentTestConfig(1)
	cfg.Load.Duration = time.Second
	server, client := startTestServerWithOptions(t, Options{
		HeartbeatInterval: heartbeatInterval,
		Assignment: &AssignmentOptions{
			Config:   cfg,
			RunID:    "test-run",
			LeadTime: 10 * time.Millisecond,
		},
	}, serverDependencies{now: time.Now})
	stream := registerAssignmentWorker(t, client, "worker-timeout")
	receiveAssignment(t, stream)

	_, err := stream.Recv()
	assertStatusCode(t, err, codes.Unavailable)
	waitForViolation(t, server.metrics, "heartbeat timeout")
}

func TestServerRemovedSessionCannotMergeMetrics(t *testing.T) {
	server, err := newServer(Options{HeartbeatInterval: time.Second}, serverDependencies{now: time.Now})
	if err != nil {
		t.Fatalf("newServer() error = %v", err)
	}
	server.metrics = newMetricsAggregator("test-run")
	worker := &session{closed: make(chan struct{}), state: WorkerSnapshot{ID: "worker-stale"}}
	if !server.addSession("worker-stale", worker) {
		t.Fatal("addSession() = false, want true")
	}
	server.removeSession("worker-stale", worker)

	err = server.recordMetrics("worker-stale", worker, &loadtestv1.MetricsDelta{})
	if err == nil {
		t.Fatal("recordMetrics(stale session) error = nil, want rejection")
	}
	if got := server.metrics.Snapshot().Requests; got != 0 {
		t.Fatalf("merged requests = %d, want 0", got)
	}
}

func TestSessionDoesNotSendAfterStop(t *testing.T) {
	worker := &session{
		stream: &failingAssignmentStream{ctx: context.Background()},
		closed: make(chan struct{}),
	}
	worker.stop()

	err := worker.send(&loadtestv1.OrchestratorMessage{})
	if !errors.Is(err, errSessionClosed) {
		t.Fatalf("send(stopped session) error = %v, want errSessionClosed", err)
	}
}

func startTestServer(t *testing.T) (*Server, loadtestv1.WorkerControlClient) {
	t.Helper()
	return startTestServerWithOptions(t, Options{
		HeartbeatInterval: 3 * time.Second,
		Logger:            log.New(io.Discard, "", 0),
	}, serverDependencies{now: time.Now})
}

func startTestServerWithOptions(
	t *testing.T,
	options Options,
	dependencies serverDependencies,
) (*Server, loadtestv1.WorkerControlClient) {
	t.Helper()
	if options.Logger == nil {
		options.Logger = log.New(io.Discard, "", 0)
	}
	if options.Assignment != nil && options.Assignment.Context == nil {
		assignment := *options.Assignment
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		assignment.Context = ctx
		options.Assignment = &assignment
	}

	service, err := newServer(options, dependencies)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(grpcServer, service)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	connection, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return service, loadtestv1.NewWorkerControlClient(connection)
}

func sendRegistration(t *testing.T, stream loadtestv1.WorkerControl_ConnectClient, workerID string) {
	t.Helper()
	if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Registration{
		Registration: &loadtestv1.Registration{
			WorkerId:        workerID,
			Hostname:        "test-host",
			SoftwareVersion: "test-version",
		},
	}}); err != nil {
		t.Fatalf("Send(registration) error = %v", err)
	}
}

func waitForWorker(t *testing.T, server *Server, workerID string) WorkerSnapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, worker := range server.ActiveWorkers() {
			if worker.ID == workerID {
				return worker
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("worker %q did not become active", workerID)
	return WorkerSnapshot{}
}

func waitForWorkerSequence(t *testing.T, server *Server, workerID string, sequence uint64) WorkerSnapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		worker := waitForWorker(t, server, workerID)
		if worker.HeartbeatSequence == sequence {
			return worker
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("worker %q heartbeat sequence did not reach %d", workerID, sequence)
	return WorkerSnapshot{}
}

func waitForNoWorkers(t *testing.T, server *Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(server.ActiveWorkers()) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("active workers = %v, want none", server.ActiveWorkers())
}

func waitForViolation(t *testing.T, aggregator *metricsAggregator, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if containsViolation(aggregator.Violations(), want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("violations = %v, want %q", aggregator.Violations(), want)
}

func assertStatusCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status code = %v, want %v; error = %v", got, want, err)
	}
}
