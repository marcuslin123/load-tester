package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/config"
	"google.golang.org/grpc/metadata"
)

type assignmentReceiveResult struct {
	message *loadtestv1.OrchestratorMessage
	err     error
}

type failingAssignmentStream struct {
	ctx context.Context
}

func (s *failingAssignmentStream) Send(*loadtestv1.OrchestratorMessage) error {
	return errors.New("send failed")
}

func (s *failingAssignmentStream) Recv() (*loadtestv1.WorkerMessage, error) {
	return nil, io.EOF
}

func (s *failingAssignmentStream) SetHeader(metadata.MD) error  { return nil }
func (s *failingAssignmentStream) SendHeader(metadata.MD) error { return nil }
func (s *failingAssignmentStream) SetTrailer(metadata.MD)       {}
func (s *failingAssignmentStream) Context() context.Context     { return s.ctx }
func (s *failingAssignmentStream) SendMsg(any) error            { return nil }
func (s *failingAssignmentStream) RecvMsg(any) error            { return io.EOF }

func TestCoordinatorWaitsForMinimumWorkersAndAssignsEqualSlices(t *testing.T) {
	now := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	server, client := startAssignmentServer(t, assignmentTestConfig(2), func() time.Time { return now })
	_ = server

	first := registerAssignmentWorker(t, client, "worker-a")
	firstMessage := receiveAsync(first)
	select {
	case received := <-firstMessage:
		t.Fatalf("received assignment before minimum fleet: message=%v error=%v", received.message, received.err)
	case <-time.After(25 * time.Millisecond):
	}

	second := registerAssignmentWorker(t, client, "worker-b")
	firstAssignment := receiveAssignmentResult(t, firstMessage)
	secondAssignment := receiveAssignment(t, second)

	assertAssignment(t, firstAssignment, "test-run", 1, 50, now.Add(time.Second), now.Add(31*time.Second))
	assertAssignment(t, secondAssignment, "test-run", 1, 50, now.Add(time.Second), now.Add(31*time.Second))
}

func TestCoordinatorRebalancesJoinWithoutExtendingRunWindow(t *testing.T) {
	initial := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	var unixNanos atomic.Int64
	unixNanos.Store(initial.UnixNano())
	now := func() time.Time { return time.Unix(0, unixNanos.Load()).UTC() }
	_, client := startAssignmentServer(t, assignmentTestConfig(2), now)

	first := registerAssignmentWorker(t, client, "worker-a")
	second := registerAssignmentWorker(t, client, "worker-b")
	revisionOneFirst := receiveAssignment(t, first)
	revisionOneSecond := receiveAssignment(t, second)
	assertAssignment(t, revisionOneFirst, "test-run", 1, 50, initial.Add(time.Second), initial.Add(31*time.Second))
	assertAssignment(t, revisionOneSecond, "test-run", 1, 50, initial.Add(time.Second), initial.Add(31*time.Second))

	unixNanos.Store(initial.Add(10 * time.Second).UnixNano())
	third := registerAssignmentWorker(t, client, "worker-c")
	revisionTwoFirst := receiveAssignment(t, first)
	revisionTwoSecond := receiveAssignment(t, second)
	revisionTwoThird := receiveAssignment(t, third)

	assertAssignment(t, revisionTwoFirst, "test-run", 2, 34, initial.Add(time.Second), initial.Add(31*time.Second))
	assertAssignment(t, revisionTwoSecond, "test-run", 2, 33, initial.Add(time.Second), initial.Add(31*time.Second))
	assertAssignment(t, revisionTwoThird, "test-run", 2, 33, initial.Add(time.Second), initial.Add(31*time.Second))
}

func TestCoordinatorLeavesWorkersJoiningAfterDeadlineIdle(t *testing.T) {
	initial := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	var unixNanos atomic.Int64
	unixNanos.Store(initial.UnixNano())
	now := func() time.Time { return time.Unix(0, unixNanos.Load()).UTC() }
	_, client := startAssignmentServer(t, assignmentTestConfig(2), now)

	first := registerAssignmentWorker(t, client, "worker-a")
	second := registerAssignmentWorker(t, client, "worker-b")
	receiveAssignment(t, first)
	receiveAssignment(t, second)

	unixNanos.Store(initial.Add(32 * time.Second).UnixNano())
	third := registerAssignmentWorker(t, client, "worker-c")
	received := receiveAsync(third)
	select {
	case result := <-received:
		t.Fatalf("worker joining after deadline received message=%v error=%v", result.message, result.err)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestCoordinatorRemovesSessionWhenAssignmentDeliveryFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs bytes.Buffer
	now := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	server, err := newServer(Options{
		HeartbeatInterval: time.Second,
		Logger:            log.New(&logs, "", 0),
		Assignment: &AssignmentOptions{
			Context:  ctx,
			Config:   assignmentTestConfig(1),
			RunID:    "test-run",
			LeadTime: time.Second,
		},
	}, serverDependencies{now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("newServer() error = %v", err)
	}
	worker := &session{
		stream: &failingAssignmentStream{ctx: ctx},
		state:  WorkerSnapshot{ID: "worker-a"},
	}
	if !server.addSession("worker-a", worker) {
		t.Fatal("addSession() = false, want true")
	}

	server.assignments.reconcile()

	if workers := server.ActiveWorkers(); len(workers) != 0 {
		t.Fatalf("active workers = %v, want none after failed delivery", workers)
	}
	if !bytes.Contains(logs.Bytes(), []byte("assignment delivery failed")) {
		t.Fatalf("logs = %q, want assignment delivery failure", logs.String())
	}
}

func startAssignmentServer(
	t *testing.T,
	cfg config.Config,
	now func() time.Time,
) (*Server, loadtestv1.WorkerControlClient) {
	t.Helper()
	return startTestServerWithOptions(t, Options{
		HeartbeatInterval: time.Second,
		Assignment: &AssignmentOptions{
			Config:   cfg,
			RunID:    "test-run",
			LeadTime: time.Second,
		},
	}, serverDependencies{now: now})
}

func assignmentTestConfig(minWorkers int) config.Config {
	return config.Config{
		Name: "assignment-test",
		Target: config.Target{
			Protocol: config.ProtocolHTTP,
			URL:      "http://target:8080/work",
			Method:   "POST",
			Headers:  map[string]string{"Content-Type": "application/json"},
			Body:     `{"piece":12}`,
		},
		Load: config.Load{
			Model:        config.LoadConstantVUs,
			VirtualUsers: 100,
			Duration:     30 * time.Second,
		},
		Fleet: config.Fleet{MinWorkers: minWorkers, MaxWorkers: 10},
	}
}

func registerAssignmentWorker(
	t *testing.T,
	client loadtestv1.WorkerControlClient,
	workerID string,
) loadtestv1.WorkerControl_ConnectClient {
	t.Helper()
	stream, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(%s) error = %v", workerID, err)
	}
	sendRegistration(t, stream, workerID)
	message, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(%s ack) error = %v", workerID, err)
	}
	if message.GetRegistrationAck() == nil {
		t.Fatalf("first %s message = %T, want registration ack", workerID, message.GetPayload())
	}
	return stream
}

func receiveAssignment(t *testing.T, stream loadtestv1.WorkerControl_ConnectClient) *loadtestv1.LoadAssignment {
	t.Helper()
	return receiveAssignmentResult(t, receiveAsync(stream))
}

func receiveAssignmentResult(t *testing.T, result <-chan assignmentReceiveResult) *loadtestv1.LoadAssignment {
	t.Helper()
	select {
	case received := <-result:
		if received.err != nil {
			t.Fatalf("Recv(assignment) error = %v", received.err)
		}
		assignment := received.message.GetAssignment()
		if assignment == nil {
			t.Fatalf("message = %T, want assignment", received.message.GetPayload())
		}
		return assignment
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for assignment")
		return nil
	}
}

func receiveAsync(stream loadtestv1.WorkerControl_ConnectClient) <-chan assignmentReceiveResult {
	result := make(chan assignmentReceiveResult, 1)
	go func() {
		message, err := stream.Recv()
		result <- assignmentReceiveResult{message: message, err: err}
	}()
	return result
}

func assertAssignment(
	t *testing.T,
	assignment *loadtestv1.LoadAssignment,
	runID string,
	revision uint64,
	virtualUsers uint64,
	startsAt time.Time,
	deadline time.Time,
) {
	t.Helper()
	if assignment.GetRunId() != runID {
		t.Errorf("run ID = %q, want %q", assignment.GetRunId(), runID)
	}
	if assignment.GetRevision() != revision {
		t.Errorf("revision = %d, want %d", assignment.GetRevision(), revision)
	}
	if assignment.GetLoad().GetVirtualUsers() != virtualUsers {
		t.Errorf("virtual users = %d, want %d", assignment.GetLoad().GetVirtualUsers(), virtualUsers)
	}
	if got := assignment.GetStartsAt().AsTime(); !got.Equal(startsAt) {
		t.Errorf("starts at = %v, want %v", got, startsAt)
	}
	if got := assignment.GetDeadline().AsTime(); !got.Equal(deadline) {
		t.Errorf("deadline = %v, want %v", got, deadline)
	}
	if got := assignment.GetTarget().GetHttp().GetUrl(); got != "http://target:8080/work" {
		t.Errorf("target URL = %q, want http://target:8080/work", got)
	}
}
