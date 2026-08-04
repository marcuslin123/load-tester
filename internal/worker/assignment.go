package worker

import (
	"errors"
	"strings"
	"sync"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type assignmentState struct {
	mu         sync.RWMutex
	assignment *loadtestv1.LoadAssignment
}

func newAssignmentState() *assignmentState {
	return &assignmentState{}
}

// Apply installs a complete newer assignment while making exact redelivery
// idempotent and rejecting state that could move the worker backward.
func (s *assignmentState) Apply(assignment *loadtestv1.LoadAssignment) error {
	if err := validateAssignment(assignment); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid load assignment: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.assignment != nil {
		if assignment.GetRunId() != s.assignment.GetRunId() {
			return status.Errorf(codes.FailedPrecondition, "assignment run %q conflicts with active run %q", assignment.GetRunId(), s.assignment.GetRunId())
		}
		if assignment.GetRevision() < s.assignment.GetRevision() {
			return status.Errorf(codes.FailedPrecondition, "assignment revision %d is older than %d", assignment.GetRevision(), s.assignment.GetRevision())
		}
		if assignment.GetRevision() == s.assignment.GetRevision() {
			if proto.Equal(assignment, s.assignment) {
				return nil
			}
			return status.Errorf(codes.FailedPrecondition, "assignment revision %d conflicts with the applied revision", assignment.GetRevision())
		}
	}
	s.assignment = proto.Clone(assignment).(*loadtestv1.LoadAssignment)
	return nil
}

func (s *assignmentState) Status() (string, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.assignment == nil {
		return "", 0
	}
	return s.assignment.GetRunId(), s.assignment.GetRevision()
}

// validateAssignment rejects incomplete control-plane state before it can be
// acknowledged in heartbeats or used by the load generator in later pieces.
func validateAssignment(assignment *loadtestv1.LoadAssignment) error {
	if assignment == nil {
		return errors.New("assignment is required")
	}
	if strings.TrimSpace(assignment.GetRunId()) == "" {
		return errors.New("run ID is required")
	}
	if assignment.GetRevision() == 0 {
		return errors.New("revision must be greater than zero")
	}
	httpTarget := assignment.GetTarget().GetHttp()
	if httpTarget == nil {
		return errors.New("HTTP target is required")
	}
	if strings.TrimSpace(httpTarget.GetUrl()) == "" {
		return errors.New("HTTP target URL is required")
	}
	if strings.TrimSpace(httpTarget.GetMethod()) == "" {
		return errors.New("HTTP target method is required")
	}
	load := assignment.GetLoad()
	if load == nil {
		return errors.New("load slice is required")
	}
	if load.GetRampUp() == nil {
		return errors.New("ramp-up duration is required")
	}
	if err := load.GetRampUp().CheckValid(); err != nil {
		return err
	}
	if load.GetRampUp().AsDuration() < 0 {
		return errors.New("ramp-up duration must not be negative")
	}
	switch load.GetModel() {
	case loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS:
		if load.GetRate() != 0 || load.GetMaxInFlight() != 0 {
			return errors.New("constant-VU slice must not contain rate fields")
		}
	case loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE:
		if load.GetVirtualUsers() != 0 {
			return errors.New("constant-rate slice must not contain virtual users")
		}
		if (load.GetRate() == 0) != (load.GetMaxInFlight() == 0) {
			return errors.New("constant-rate slice requires both rate and max in flight")
		}
	default:
		return errors.New("load model is required")
	}
	if assignment.GetStartsAt() == nil {
		return errors.New("start time is required")
	}
	if err := assignment.GetStartsAt().CheckValid(); err != nil {
		return err
	}
	if assignment.GetDeadline() == nil {
		return errors.New("deadline is required")
	}
	if err := assignment.GetDeadline().CheckValid(); err != nil {
		return err
	}
	if !assignment.GetDeadline().AsTime().After(assignment.GetStartsAt().AsTime()) {
		return errors.New("deadline must be after start time")
	}
	return nil
}
