package worker

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRunRegistersAndSendsOrderedHeartbeats(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	server := &testControlServer{
		handle: func(stream grpc.BidiStreamingServer[loadtestv1.WorkerMessage, loadtestv1.OrchestratorMessage], _ int) error {
			registration, err := stream.Recv()
			if err != nil {
				return err
			}
			if got := registration.GetRegistration().GetWorkerId(); got != "worker-1" {
				return status.Errorf(codes.InvalidArgument, "worker ID = %q", got)
			}
			if err := stream.Send(registrationAck("worker-1", 5*time.Millisecond)); err != nil {
				return err
			}
			for wantSequence := uint64(1); wantSequence <= 2; wantSequence++ {
				message, err := stream.Recv()
				if err != nil {
					return err
				}
				heartbeat := message.GetHeartbeat()
				if heartbeat == nil || heartbeat.GetSequence() != wantSequence {
					return status.Errorf(codes.InvalidArgument, "heartbeat = %v, want sequence %d", heartbeat, wantSequence)
				}
				if heartbeat.GetState() != loadtestv1.WorkerState_WORKER_STATE_IDLE {
					return status.Errorf(codes.InvalidArgument, "state = %v, want IDLE", heartbeat.GetState())
				}
			}
			cancel()
			return nil
		},
	}
	dependencies := startWorkerTestServer(t, server)

	err := run(ctx, validConfig(), Options{}, dependencies)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if got := server.Attempts(); got != 1 {
		t.Fatalf("connection attempts = %d, want 1", got)
	}
}

func TestRunRetriesUnavailableRegistration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	server := &testControlServer{
		handle: func(stream grpc.BidiStreamingServer[loadtestv1.WorkerMessage, loadtestv1.OrchestratorMessage], attempt int) error {
			if attempt == 1 {
				return status.Error(codes.Unavailable, "not ready")
			}
			if _, err := stream.Recv(); err != nil {
				return err
			}
			if err := stream.Send(registrationAck("worker-1", time.Hour)); err != nil {
				return err
			}
			cancel()
			return nil
		},
	}
	dependencies := startWorkerTestServer(t, server)
	var waits []time.Duration
	dependencies.wait = func(ctx context.Context, delay time.Duration) bool {
		waits = append(waits, delay)
		return ctx.Err() == nil
	}
	dependencies.jitter = func(limit time.Duration) time.Duration { return limit / 2 }

	err := run(ctx, validConfig(), Options{}, dependencies)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if got := server.Attempts(); got != 2 {
		t.Fatalf("connection attempts = %d, want 2", got)
	}
	if len(waits) != 1 || waits[0] != 250*time.Millisecond {
		t.Fatalf("retry waits = %v, want [250ms]", waits)
	}
}

func TestRunStopsOnPermanentRegistrationError(t *testing.T) {
	t.Parallel()

	server := &testControlServer{
		handle: func(stream grpc.BidiStreamingServer[loadtestv1.WorkerMessage, loadtestv1.OrchestratorMessage], _ int) error {
			if _, err := stream.Recv(); err != nil {
				return err
			}
			return status.Error(codes.PermissionDenied, "worker not allowed")
		},
	}
	dependencies := startWorkerTestServer(t, server)
	dependencies.wait = func(context.Context, time.Duration) bool {
		t.Fatal("permanent errors must not wait or retry")
		return false
	}

	err := run(context.Background(), validConfig(), Options{}, dependencies)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("run() error = %v, want PermissionDenied", err)
	}
	if got := server.Attempts(); got != 1 {
		t.Fatalf("connection attempts = %d, want 1", got)
	}
}

func TestRetryBackoffGrowsCapsAndResets(t *testing.T) {
	t.Parallel()

	backoff := newRetryBackoff(500*time.Millisecond, 10*time.Second, func(limit time.Duration) time.Duration {
		return limit / 2
	})
	var got []time.Duration
	for range 7 {
		got = append(got, backoff.Next())
	}
	want := []time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		4 * time.Second,
		5 * time.Second,
		5 * time.Second,
	}
	if len(got) != len(want) {
		t.Fatalf("delays = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("delay %d = %v, want %v", index, got[index], want[index])
		}
	}
	backoff.Reset()
	if got := backoff.Next(); got != 250*time.Millisecond {
		t.Fatalf("delay after Reset() = %v, want 250ms", got)
	}
}

func TestRunRejectsMissingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{name: "address", config: Config{WorkerID: "worker-1"}, want: "orchestrator address"},
		{name: "worker ID", config: Config{Address: "orchestrator:9090"}, want: "worker ID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := run(context.Background(), test.config, Options{}, runtimeDependencies{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("run() error = %v, want %q", err, test.want)
			}
		})
	}
}

type testControlServer struct {
	loadtestv1.UnimplementedWorkerControlServer

	mu       sync.Mutex
	attempts int
	handle   func(grpc.BidiStreamingServer[loadtestv1.WorkerMessage, loadtestv1.OrchestratorMessage], int) error
}

func (s *testControlServer) Connect(stream grpc.BidiStreamingServer[loadtestv1.WorkerMessage, loadtestv1.OrchestratorMessage]) error {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.mu.Unlock()
	return s.handle(stream, attempt)
}

func (s *testControlServer) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func startWorkerTestServer(t *testing.T, service loadtestv1.WorkerControlServer) runtimeDependencies {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	return runtimeDependencies{
		dial: func(context.Context, string) (loadtestv1.WorkerControlClient, io.Closer, error) {
			connection, err := grpc.NewClient(
				"passthrough:///bufconn",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
			)
			if err != nil {
				return nil, nil, err
			}
			return loadtestv1.NewWorkerControlClient(connection), connection, nil
		},
		wait:   waitContext,
		jitter: func(limit time.Duration) time.Duration { return limit / 2 },
		now:    time.Now,
		logger: log.New(io.Discard, "", 0),
	}
}

func registrationAck(workerID string, heartbeatInterval time.Duration) *loadtestv1.OrchestratorMessage {
	return &loadtestv1.OrchestratorMessage{Payload: &loadtestv1.OrchestratorMessage_RegistrationAck{
		RegistrationAck: &loadtestv1.RegistrationAck{
			WorkerId:          workerID,
			HeartbeatInterval: durationpb.New(heartbeatInterval),
		},
	}}
}

func validConfig() Config {
	return Config{
		Address:         "bufconn",
		WorkerID:        "worker-1",
		Hostname:        "test-host",
		SoftwareVersion: "test-version",
	}
}
