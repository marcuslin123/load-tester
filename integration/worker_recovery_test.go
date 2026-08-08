package integration_test

import (
	"context"
	"io"
	"log"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/orchestrator"
	"github.com/marcuslin123/load-tester/internal/report"
	"github.com/marcuslin123/load-tester/internal/targetapp"
	workerclient "github.com/marcuslin123/load-tester/internal/worker"
	"google.golang.org/grpc"
)

type recoveryWorkerResult struct {
	workerID string
	err      error
}

func TestWorkerFailureRedistributesLoadAndPreservesAcceptedMetrics(t *testing.T) {
	target := httptest.NewServer(targetapp.NewHandler())
	defer target.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	orchestratorCtx, stopOrchestrator := context.WithCancel(context.Background())
	defer stopOrchestrator()
	cfg := config.Config{
		Name: "worker-recovery",
		Target: config.Target{
			Protocol: config.ProtocolHTTP,
			URL:      target.URL + "/echo?latency=5ms",
			Method:   "GET",
			Headers:  map[string]string{},
		},
		Load: config.Load{
			Model:        config.LoadConstantVUs,
			VirtualUsers: 2,
			Duration:     2400 * time.Millisecond,
		},
		Fleet: config.Fleet{MinWorkers: 2, MaxWorkers: 2},
	}
	control, err := orchestrator.NewServer(orchestrator.Options{
		HeartbeatInterval: 20 * time.Millisecond,
		Logger:            log.New(io.Discard, "", 0),
		Assignment: &orchestrator.AssignmentOptions{
			Context:  orchestratorCtx,
			Config:   cfg,
			RunID:    "worker-recovery-run",
			LeadTime: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	grpcServer := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(grpcServer, control)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	workerAContext, stopWorkerA := context.WithCancel(context.Background())
	workerBContext, stopWorkerB := context.WithCancel(context.Background())
	defer stopWorkerA()
	defer stopWorkerB()
	workerDone := make(chan recoveryWorkerResult, 2)
	startRecoveryWorker(workerAContext, listener.Addr().String(), "worker-a", workerDone)
	startRecoveryWorker(workerBContext, listener.Addr().String(), "worker-b", workerDone)

	waitForWorkerRequests(t, control, "worker-a")
	stopWorkerA()
	waitForAppliedRevision(t, control, "worker-b", 2)

	summary, err := control.WaitForResult(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("WaitForResult() error = %v", err)
	}
	stopWorkerB()
	for range 2 {
		select {
		case result := <-workerDone:
			if result.err != nil {
				t.Fatalf("%s Run() error = %v", result.workerID, result.err)
			}
		case <-time.After(time.Second):
			t.Fatal("worker did not stop after cancellation")
		}
	}

	if summary.Status != report.StatusFail {
		t.Fatalf("summary status = %s, want FAIL", summary.Status)
	}
	if !containsText(summary.Violations, "worker worker-a failed") {
		t.Fatalf("violations = %v, want named worker-a failure", summary.Violations)
	}
	if containsText(summary.Violations, "missing final metrics: worker-a") {
		t.Fatalf("violations = %v, dead worker should not remain in latest revision", summary.Violations)
	}
	workers := control.WorkerMetrics()
	if workers["worker-a"].Requests == 0 || workers["worker-b"].Requests == 0 {
		t.Fatalf("worker metrics = %+v, want accepted requests from both workers", workers)
	}
	if got := workers["worker-a"].Requests + workers["worker-b"].Requests; got != summary.Metrics.Requests {
		t.Fatalf("worker request sum = %d, fleet requests = %d", got, summary.Metrics.Requests)
	}
	if summary.Duration != cfg.Load.Duration {
		t.Fatalf("summary duration = %v, want original %v window", summary.Duration, cfg.Load.Duration)
	}
}

func startRecoveryWorker(
	ctx context.Context,
	address string,
	workerID string,
	done chan<- recoveryWorkerResult,
) {
	go func() {
		done <- recoveryWorkerResult{
			workerID: workerID,
			err: workerclient.Run(ctx, workerclient.Config{
				Address:         address,
				WorkerID:        workerID,
				Hostname:        workerID,
				SoftwareVersion: "integration-test",
			}, workerclient.Options{Logger: log.New(io.Discard, "", 0)}),
		}
	}()
}

func waitForWorkerRequests(t *testing.T, server *orchestrator.Server, workerID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if server.WorkerMetrics()[workerID].Requests > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("worker %q did not report requests", workerID)
}

func waitForAppliedRevision(t *testing.T, server *orchestrator.Server, workerID string, revision uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, worker := range server.ActiveWorkers() {
			if worker.ID == workerID && worker.AppliedAssignmentRevision >= revision {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("worker %q did not apply revision %d", workerID, revision)
}

func containsText(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}
