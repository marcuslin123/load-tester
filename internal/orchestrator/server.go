// Package orchestrator owns worker stream registration and connection state.
package orchestrator

import (
	"context"
	"errors"
	"io"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Options configures registration acknowledgments and server diagnostics.
type Options struct {
	HeartbeatInterval time.Duration
	Logger            *log.Logger
	Assignment        *AssignmentOptions
}

type serverDependencies struct {
	now func() time.Time
}

// WorkerSnapshot is a read-only view of one active worker session.
type WorkerSnapshot struct {
	ID                        string
	Hostname                  string
	SoftwareVersion           string
	ConnectedAt               time.Time
	LastHeartbeat             time.Time
	HeartbeatSequence         uint64
	State                     loadtestv1.WorkerState
	ActiveRunID               string
	AppliedAssignmentRevision uint64
	InFlightRequests          uint64
}

type session struct {
	stream grpc.BidiStreamingServer[loadtestv1.WorkerMessage, loadtestv1.OrchestratorMessage]
	sendMu sync.Mutex
	state  WorkerSnapshot
}

func (s *session) send(message *loadtestv1.OrchestratorMessage) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(message)
}

// Server implements the worker control stream and owns the active session registry.
type Server struct {
	loadtestv1.UnimplementedWorkerControlServer

	heartbeatInterval time.Duration
	logger            *log.Logger
	now               func() time.Time

	mu       sync.RWMutex
	sessions map[string]*session

	assignments *assignmentCoordinator
	metrics     *metricsAggregator
}

// NewServer creates a registration server with no active workers.
func NewServer(options Options) (*Server, error) {
	return newServer(options, serverDependencies{now: time.Now})
}

func newServer(options Options, dependencies serverDependencies) (*Server, error) {
	if options.HeartbeatInterval <= 0 {
		return nil, errors.New("heartbeat interval must be greater than zero")
	}
	if dependencies.now == nil {
		return nil, errors.New("clock is required")
	}
	logger := options.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	server := &Server{
		heartbeatInterval: options.HeartbeatInterval,
		logger:            logger,
		now:               dependencies.now,
		sessions:          make(map[string]*session),
	}
	if options.Assignment != nil {
		server.metrics = newMetricsAggregator(options.Assignment.RunID)
		coordinator, err := newAssignmentCoordinator(server, *options.Assignment, dependencies.now)
		if err != nil {
			return nil, err
		}
		server.assignments = coordinator
		go coordinator.run(options.Assignment.Context)
	}
	return server, nil
}

// Connect requires registration first, then records ordered heartbeats until disconnect.
func (s *Server) Connect(stream grpc.BidiStreamingServer[loadtestv1.WorkerMessage, loadtestv1.OrchestratorMessage]) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	registration := first.GetRegistration()
	if registration == nil {
		return status.Error(codes.FailedPrecondition, "registration must be the first worker message")
	}
	workerID := strings.TrimSpace(registration.GetWorkerId())
	if workerID == "" {
		return status.Error(codes.InvalidArgument, "worker ID is required")
	}

	worker := &session{
		stream: stream,
		state: WorkerSnapshot{
			ID:              workerID,
			Hostname:        registration.GetHostname(),
			SoftwareVersion: registration.GetSoftwareVersion(),
			ConnectedAt:     s.now(),
			State:           loadtestv1.WorkerState_WORKER_STATE_IDLE,
		},
	}
	if !s.addSession(workerID, worker) {
		return status.Errorf(codes.AlreadyExists, "worker %q is already connected", workerID)
	}
	defer s.removeSession(workerID, worker)

	if err := worker.send(&loadtestv1.OrchestratorMessage{Payload: &loadtestv1.OrchestratorMessage_RegistrationAck{
		RegistrationAck: &loadtestv1.RegistrationAck{
			WorkerId:          workerID,
			HeartbeatInterval: durationpb.New(s.heartbeatInterval),
		},
	}}); err != nil {
		return err
	}
	s.logger.Printf("worker registered: id=%s hostname=%s version=%s", workerID, worker.state.Hostname, worker.state.SoftwareVersion)
	if s.assignments != nil {
		s.assignments.membershipChanged()
	}

	for {
		message, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || stream.Context().Err() != nil {
				return nil
			}
			return err
		}
		if heartbeat := message.GetHeartbeat(); heartbeat != nil {
			if err := s.recordHeartbeat(workerID, worker, heartbeat); err != nil {
				return err
			}
			continue
		}
		if delta := message.GetMetrics(); delta != nil && s.metrics != nil {
			if err := s.metrics.Accept(workerID, delta); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
			continue
		}
		return status.Error(codes.FailedPrecondition, "only heartbeats and metric deltas are accepted after registration")
	}
}

// ActiveWorkers returns deterministic copies rather than exposing mutable sessions.
func (s *Server) ActiveWorkers() []WorkerSnapshot {
	s.mu.RLock()
	workers := make([]WorkerSnapshot, 0, len(s.sessions))
	for _, worker := range s.sessions {
		workers = append(workers, worker.state)
	}
	s.mu.RUnlock()
	slices.SortFunc(workers, func(left, right WorkerSnapshot) int {
		return strings.Compare(left.ID, right.ID)
	})
	return workers
}

// WorkerMetrics returns independent cumulative snapshots for diagnostics and reporting.
func (s *Server) WorkerMetrics() map[string]metrics.Snapshot {
	if s.metrics == nil {
		return map[string]metrics.Snapshot{}
	}
	return s.metrics.WorkerSnapshots()
}

func (s *Server) addSession(workerID string, worker *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[workerID]; exists {
		return false
	}
	s.sessions[workerID] = worker
	return true
}

func (s *Server) removeSession(workerID string, worker *session) bool {
	s.mu.Lock()
	removed := false
	if s.sessions[workerID] == worker {
		delete(s.sessions, workerID)
		removed = true
	}
	s.mu.Unlock()
	if removed {
		s.logger.Printf("worker disconnected: id=%s", workerID)
	}
	return removed
}

func (s *Server) recordHeartbeat(workerID string, worker *session, heartbeat *loadtestv1.Heartbeat) error {
	if heartbeat.GetWorkerId() != workerID {
		return status.Errorf(codes.InvalidArgument, "heartbeat worker ID %q does not match registered ID %q", heartbeat.GetWorkerId(), workerID)
	}
	if heartbeat.GetSentAt() == nil {
		return status.Error(codes.InvalidArgument, "heartbeat timestamp is required")
	}
	if err := heartbeat.GetSentAt().CheckValid(); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid heartbeat timestamp: %v", err)
	}
	if heartbeat.GetState() == loadtestv1.WorkerState_WORKER_STATE_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "worker state is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.sessions[workerID]; current != worker {
		return status.Error(codes.FailedPrecondition, "worker session is no longer active")
	}
	if heartbeat.GetSequence() <= worker.state.HeartbeatSequence {
		return status.Errorf(
			codes.InvalidArgument,
			"heartbeat sequence %d must be greater than %d",
			heartbeat.GetSequence(),
			worker.state.HeartbeatSequence,
		)
	}
	worker.state.LastHeartbeat = s.now()
	worker.state.HeartbeatSequence = heartbeat.GetSequence()
	worker.state.State = heartbeat.GetState()
	worker.state.ActiveRunID = heartbeat.GetActiveRunId()
	worker.state.AppliedAssignmentRevision = heartbeat.GetAppliedAssignmentRevision()
	worker.state.InFlightRequests = heartbeat.GetInFlightRequests()
	return nil
}
