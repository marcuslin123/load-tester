package worker

import (
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAssignmentStateRetainsNewestAssignment(t *testing.T) {
	state := newAssignmentState()
	assignment := validAssignment("run-12", 2, 50)

	if err := state.Apply(assignment); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	runID, revision := state.Status()
	if runID != "run-12" || revision != 2 {
		t.Fatalf("Status() = (%q, %d), want (run-12, 2)", runID, revision)
	}
}

func TestAssignmentStateAcceptsExactDuplicate(t *testing.T) {
	state := newAssignmentState()
	assignment := validAssignment("run-12", 2, 50)
	if err := state.Apply(assignment); err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	if err := state.Apply(assignment); err != nil {
		t.Fatalf("duplicate Apply() error = %v", err)
	}
}

func TestAssignmentStateRejectsStaleAndConflictingRevisions(t *testing.T) {
	tests := []struct {
		name       string
		assignment *loadtestv1.LoadAssignment
	}{
		{name: "stale", assignment: validAssignment("run-12", 1, 50)},
		{name: "conflicting", assignment: validAssignment("run-12", 2, 49)},
		{name: "different run", assignment: validAssignment("other-run", 3, 50)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := newAssignmentState()
			if err := state.Apply(validAssignment("run-12", 2, 50)); err != nil {
				t.Fatalf("initial Apply() error = %v", err)
			}
			if err := state.Apply(test.assignment); err == nil {
				t.Fatal("Apply() error = nil, want protocol error")
			}
		})
	}
}

func TestAssignmentStateRejectsMalformedAssignment(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*loadtestv1.LoadAssignment)
	}{
		{name: "run ID", mutate: func(assignment *loadtestv1.LoadAssignment) { assignment.RunId = "" }},
		{name: "revision", mutate: func(assignment *loadtestv1.LoadAssignment) { assignment.Revision = 0 }},
		{name: "target", mutate: func(assignment *loadtestv1.LoadAssignment) { assignment.Target = nil }},
		{name: "load", mutate: func(assignment *loadtestv1.LoadAssignment) { assignment.Load = nil }},
		{name: "start", mutate: func(assignment *loadtestv1.LoadAssignment) { assignment.StartsAt = nil }},
		{name: "deadline", mutate: func(assignment *loadtestv1.LoadAssignment) { assignment.Deadline = nil }},
		{name: "window", mutate: func(assignment *loadtestv1.LoadAssignment) { assignment.Deadline = assignment.StartsAt }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assignment := validAssignment("run-12", 1, 50)
			test.mutate(assignment)
			if err := newAssignmentState().Apply(assignment); err == nil {
				t.Fatal("Apply() error = nil, want validation error")
			}
		})
	}
}

func validAssignment(runID string, revision, virtualUsers uint64) *loadtestv1.LoadAssignment {
	startsAt := time.Date(2026, time.August, 4, 12, 0, 1, 0, time.UTC)
	return &loadtestv1.LoadAssignment{
		RunId:    runID,
		Revision: revision,
		Target: &loadtestv1.Target{Protocol: &loadtestv1.Target_Http{
			Http: &loadtestv1.HttpTarget{
				Url:    "http://target:8080/work",
				Method: "GET",
			},
		}},
		Load: &loadtestv1.LoadSlice{
			Model:        loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS,
			VirtualUsers: virtualUsers,
			RampUp:       durationpb.New(0),
		},
		StartsAt: timestamppb.New(startsAt),
		Deadline: timestamppb.New(startsAt.Add(30 * time.Second)),
	}
}
