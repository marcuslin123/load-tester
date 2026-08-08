package integration_test

import (
	"context"
	"io"
	"log"
	"net"
	"net/http/httptest"
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

func TestDistributedWorkersMergeMetricsAgainstInjectedLatencyTarget(t *testing.T) {
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
		Name: "distributed-metrics",
		Target: config.Target{
			Protocol: config.ProtocolHTTP,
			URL:      target.URL + "/echo?latency=20ms",
			Method:   "GET",
			Headers:  map[string]string{},
		},
		Load: config.Load{
			Model:        config.LoadConstantVUs,
			VirtualUsers: 4,
			Duration:     2200 * time.Millisecond,
		},
		Fleet: config.Fleet{MinWorkers: 2, MaxWorkers: 2},
		Thresholds: config.Thresholds{
			P99Latency: 100 * time.Millisecond,
		},
	}
	control, err := orchestrator.NewServer(orchestrator.Options{
		HeartbeatInterval: 20 * time.Millisecond,
		Logger:            log.New(io.Discard, "", 0),
		Assignment: &orchestrator.AssignmentOptions{
			Context:  orchestratorCtx,
			Config:   cfg,
			RunID:    "distributed-run",
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

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	workerDone := make(chan error, 2)
	for _, workerID := range []string{"worker-a", "worker-b"} {
		workerID := workerID
		go func() {
			workerDone <- workerclient.Run(workerCtx, workerclient.Config{
				Address:         listener.Addr().String(),
				WorkerID:        workerID,
				Hostname:        workerID,
				SoftwareVersion: "integration-test",
			}, workerclient.Options{Logger: log.New(io.Discard, "", 0)})
		}()
	}

	summary, err := control.WaitForResult(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("WaitForResult() error = %v", err)
	}
	stopWorkers()
	for range 2 {
		select {
		case err := <-workerDone:
			if err != nil {
				t.Fatalf("worker Run() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("worker did not stop after cancellation")
		}
	}

	if summary.Status != report.StatusPass {
		t.Fatalf("summary status = %s violations=%v, want PASS", summary.Status, summary.Violations)
	}
	if summary.Metrics.Requests == 0 {
		t.Fatal("fleet requests = 0, want generated traffic")
	}
	if summary.P99 < 19*time.Millisecond || summary.P99 > 80*time.Millisecond {
		t.Fatalf("fleet p99 = %v, want injected 20ms latency within scheduling tolerance", summary.P99)
	}
	workers := control.WorkerMetrics()
	if len(workers) != 2 || workers["worker-a"].Requests == 0 || workers["worker-b"].Requests == 0 {
		t.Fatalf("worker metrics = %+v, want two workers with requests", workers)
	}
	workerTotal := workers["worker-a"].Requests + workers["worker-b"].Requests
	if workerTotal != summary.Metrics.Requests {
		t.Fatalf("worker request sum = %d, fleet requests = %d", workerTotal, summary.Metrics.Requests)
	}
	t.Logf(
		"fleet requests=%d p99=%s worker-a=%d worker-b=%d",
		summary.Metrics.Requests,
		summary.P99,
		workers["worker-a"].Requests,
		workers["worker-b"].Requests,
	)
}
