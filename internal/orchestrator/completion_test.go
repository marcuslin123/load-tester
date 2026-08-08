package orchestrator

import (
	"context"
	"strings"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/protocol"
	"github.com/marcuslin123/load-tester/internal/report"
)

func TestServerWaitForResultMergesFinalWorkerDelta(t *testing.T) {
	server, client := startCompletionServer(t)
	stream := registerAssignmentWorker(t, client, "worker-a")
	assignment := receiveAssignment(t, stream)
	delta := metricsDeltaForTest(
		t,
		"worker-a",
		assignment.GetRevision(),
		1,
		assignment.GetStartsAt().AsTime(),
		assignment.GetDeadline().AsTime(),
		[]protocol.Result{{Latency: 25 * time.Millisecond, StatusCode: 200}},
		true,
	)
	if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Metrics{Metrics: delta}}); err != nil {
		t.Fatalf("Send(metrics) error = %v", err)
	}

	summary, err := server.WaitForResult(context.Background(), 50*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitForResult() error = %v", err)
	}
	if summary.Status != report.StatusPass || summary.Metrics.Requests != 1 {
		t.Fatalf("summary = %+v, want passing one-request result", summary)
	}
	if summary.P99 < 24*time.Millisecond || summary.P99 > 26*time.Millisecond {
		t.Errorf("p99 = %v, want approximately 25ms", summary.P99)
	}
}

func TestServerWaitForResultFailsWhenFinalDeltaIsMissing(t *testing.T) {
	server, client := startCompletionServer(t)
	stream := registerAssignmentWorker(t, client, "worker-a")
	receiveAssignment(t, stream)

	summary, err := server.WaitForResult(context.Background(), 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitForResult() error = %v", err)
	}
	if summary.Status != report.StatusFail {
		t.Fatalf("summary status = %s, want FAIL", summary.Status)
	}
	if !containsViolation(summary.Violations, "missing final metrics: worker-a") {
		t.Fatalf("violations = %v, want missing worker-a final metrics", summary.Violations)
	}
}

func TestServerWaitForResultReportsInterruptedBeforeWorkersStart(t *testing.T) {
	server, _ := startCompletionServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	summary, err := server.WaitForResult(ctx, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitForResult() error = %v", err)
	}
	if summary.Status != report.StatusInterrupted || summary.Duration != 0 {
		t.Fatalf("summary = %+v, want zero-duration INTERRUPTED result", summary)
	}
}

func startCompletionServer(t *testing.T) (*Server, loadtestv1.WorkerControlClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := config.Config{
		Name: "completion-test",
		Target: config.Target{
			Protocol: config.ProtocolHTTP,
			URL:      "http://target:8080/work",
			Method:   "GET",
			Headers:  map[string]string{},
		},
		Load: config.Load{
			Model:        config.LoadConstantVUs,
			VirtualUsers: 1,
			Duration:     30 * time.Millisecond,
		},
		Fleet: config.Fleet{MinWorkers: 1, MaxWorkers: 1},
	}
	return startTestServerWithOptions(t, Options{
		HeartbeatInterval: time.Second,
		Assignment: &AssignmentOptions{
			Context:  ctx,
			Config:   cfg,
			RunID:    "run-13",
			LeadTime: 5 * time.Millisecond,
		},
	}, serverDependencies{now: time.Now})
}

func containsViolation(violations []string, want string) bool {
	for _, violation := range violations {
		if strings.Contains(violation, want) {
			return true
		}
	}
	return false
}
