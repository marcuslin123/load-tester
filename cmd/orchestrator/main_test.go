package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	loadconfig "github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/report"
	"google.golang.org/grpc"
)

func TestParseOptionsRequiresConfigAndDefaultsAddress(t *testing.T) {
	options, err := parseOptions([]string{"-config", "loadtest.yaml"})
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if options.configPath != "loadtest.yaml" {
		t.Errorf("config path = %q, want loadtest.yaml", options.configPath)
	}
	if options.address != ":9090" {
		t.Errorf("address = %q, want :9090", options.address)
	}
	if options.metricsAddress != ":9091" {
		t.Errorf("metrics address = %q, want :9091", options.metricsAddress)
	}
}

func TestParseOptionsAcceptsAddressOverride(t *testing.T) {
	options, err := parseOptions([]string{
		"-config", "loadtest.yaml",
		"-addr", "127.0.0.1:19090",
		"-metrics-addr", "127.0.0.1:19091",
	})
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if options.address != "127.0.0.1:19090" {
		t.Fatalf("address = %q, want 127.0.0.1:19090", options.address)
	}
	if options.metricsAddress != "127.0.0.1:19091" {
		t.Fatalf("metrics address = %q, want 127.0.0.1:19091", options.metricsAddress)
	}
}

func TestNewHTTPHandlerServesHealthMetricsAndNotFound(t *testing.T) {
	info := metrics.RunInfo{RunID: "run-123", Name: "smoke", Model: "constant-vus"}
	for range 2 {
		handler := newHTTPHandler(prometheusSourceStub{}, info)

		t.Run("health", func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
			}
			if response.Body.String() != "ok\n" {
				t.Fatalf("body = %q, want %q", response.Body.String(), "ok\n")
			}
		})

		t.Run("metrics", func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
			}
			if body := response.Body.String(); !strings.Contains(body, `loadtester_run_info{model="constant-vus",name="smoke",run_id="run-123"} 1`) {
				t.Fatalf("body does not contain run info metric:\n%s", body)
			}
		})

		t.Run("not found", func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/unknown", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
			}
		})
	}
}

func TestHTTPHandlerRetainsInactiveWorkerWithoutDeltas(t *testing.T) {
	handler := newHTTPHandler(prometheusSourceStub{view: metrics.PrometheusView{
		Workers: map[string]metrics.Snapshot{"worker-no-delta": {}},
	}}, metrics.RunInfo{RunID: "run-empty", Name: "empty", Model: "constant-vus"})
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, metric := range []string{
		"loadtester_active_workers 0",
		`loadtester_worker_active{worker_id="worker-no-delta"} 0`,
		`loadtester_worker_requests_total{worker_id="worker-no-delta"} 0`,
		`loadtester_worker_request_duration_seconds_count{worker_id="worker-no-delta"} 0`,
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("body does not contain %q:\n%s", metric, body)
		}
	}
}

type prometheusSourceStub struct {
	view metrics.PrometheusView
}

func (s prometheusSourceStub) PrometheusView() metrics.PrometheusView { return s.view }

func TestParseOptionsRejectsMissingConfigAndPositionalArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing config", want: "-config is required"},
		{name: "positional", args: []string{"-config", "loadtest.yaml", "unexpected"}, want: "unexpected arguments"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseOptions(test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("parseOptions() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadTestConfigParsesValidatedYAML(t *testing.T) {
	path := writeOrchestratorConfig(t, `
name: distributed-test
target:
  protocol: http
  url: http://target:8080/work
load:
  model: constant-vus
  virtual_users: 100
  duration: 30s
fleet:
  min_workers: 2
  max_workers: 4
`)

	cfg, err := loadTestConfig(path)
	if err != nil {
		t.Fatalf("loadTestConfig() error = %v", err)
	}
	if cfg.Name != "distributed-test" || cfg.Load.VirtualUsers != 100 || cfg.Fleet.MinWorkers != 2 {
		t.Fatalf("config = %+v, want parsed distributed test", cfg)
	}
}

func TestLoadTestConfigRejectsInvalidYAML(t *testing.T) {
	path := writeOrchestratorConfig(t, "name: incomplete\n")
	if _, err := loadTestConfig(path); err == nil {
		t.Fatal("loadTestConfig() error = nil, want validation error")
	}
}

func TestExitCodeForSummary(t *testing.T) {
	tests := []struct {
		status report.Status
		want   exitCode
	}{
		{status: report.StatusPass, want: exitPass},
		{status: report.StatusFail, want: exitTestFailure},
		{status: report.StatusInterrupted, want: exitInterrupted},
		{status: report.Status("UNKNOWN"), want: exitSetupFailure},
	}
	for _, test := range tests {
		if got := exitCodeForSummary(report.Summary{Status: test.status}); got != test.want {
			t.Errorf("exitCodeForSummary(%s) = %d, want %d", test.status, got, test.want)
		}
	}
}

func TestRunRejectsOccupiedGRPCPort(t *testing.T) {
	occupied := listenLocal(t)
	defer occupied.Close()

	_, err := run(
		context.Background(),
		occupied.Addr().String(),
		reserveLocalAddress(t),
		loadconfig.Config{},
		"run-123",
		log.New(io.Discard, "", 0),
	)
	if err == nil || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("run() error = %v, want occupied gRPC listener error", err)
	}
}

func TestRunRejectsOccupiedMetricsPortAndClosesGRPCListener(t *testing.T) {
	grpcAddress := reserveLocalAddress(t)
	occupiedMetrics := listenLocal(t)
	defer occupiedMetrics.Close()

	_, err := run(
		context.Background(),
		grpcAddress,
		occupiedMetrics.Addr().String(),
		loadconfig.Config{},
		"run-123",
		log.New(io.Discard, "", 0),
	)
	if err == nil || !strings.Contains(err.Error(), "listen metrics on") {
		t.Fatalf("run() error = %v, want occupied metrics listener error", err)
	}

	rebound, listenErr := net.Listen("tcp", grpcAddress)
	if listenErr != nil {
		t.Fatalf("first listener was not closed after second bind failed: %v", listenErr)
	}
	if err := rebound.Close(); err != nil {
		t.Fatalf("close rebound listener: %v", err)
	}
}

func TestServeRunFailsWhenHTTPServerStopsEarly(t *testing.T) {
	grpcServer := newBlockingGRPCServer()
	httpServer := newReturningHTTPServer(errors.New("http listener failed"))
	events := make(chan lifecycleEvent, 4)

	_, err := runServeRun(t, func() {}, grpcServer, httpServer, events)
	if err == nil || !strings.Contains(err.Error(), "serve HTTP: http listener failed") {
		t.Fatalf("serveRun() error = %v, want early HTTP serve failure", err)
	}
	assertServersExited(t, grpcServer, httpServer)
}

func TestServeRunFailsWhenGRPCServerStopsEarly(t *testing.T) {
	grpcServer := newReturningGRPCServer(errors.New("grpc listener failed"))
	httpServer := newBlockingHTTPServer()
	events := make(chan lifecycleEvent, 4)

	_, err := runServeRun(t, func() {}, grpcServer, httpServer, events)
	if err == nil || !strings.Contains(err.Error(), "serve gRPC: grpc listener failed") {
		t.Fatalf("serveRun() error = %v, want early gRPC serve failure", err)
	}
	assertServersExited(t, grpcServer, httpServer)
}

func TestServeRunReturnsInterruptedResultAndStopsBothServersOnCancellation(t *testing.T) {
	events := make(chan lifecycleEvent, 6)
	events <- lifecycleEvent{kind: lifecycleCanceled}
	events <- lifecycleEvent{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusInterrupted}}}
	grpcServer := newBlockingGRPCServer()
	httpServer := newBlockingHTTPServer()

	summary, err := runServeRun(t, func() {}, grpcServer, httpServer, events)
	if err != nil {
		t.Fatalf("serveRun() error = %v", err)
	}
	if summary.Status != report.StatusInterrupted {
		t.Fatalf("status = %s, want %s", summary.Status, report.StatusInterrupted)
	}
	assertServersExited(t, grpcServer, httpServer)
}

func TestServeRunReturnsCompletedResultAndStopsBothServers(t *testing.T) {
	events := make(chan lifecycleEvent, 4)
	events <- lifecycleEvent{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusPass}}}
	grpcServer := newBlockingGRPCServer()
	httpServer := newBlockingHTTPServer()

	summary, err := runServeRun(t, func() {}, grpcServer, httpServer, events)
	if err != nil {
		t.Fatalf("serveRun() error = %v", err)
	}
	if summary.Status != report.StatusPass {
		t.Fatalf("status = %s, want %s", summary.Status, report.StatusPass)
	}
	assertServersExited(t, grpcServer, httpServer)
}

func TestCoordinateLifecycleUsesQueuedEventOrderAsLinearizationPoint(t *testing.T) {
	tests := []struct {
		name       string
		events     []lifecycleEvent
		wantStatus report.Status
		wantError  string
	}{
		{
			name: "serve sentinel before completion is failure",
			events: []lifecycleEvent{
				{kind: lifecycleGRPCStopped, err: grpc.ErrServerStopped},
				{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusPass}}},
				{kind: lifecycleHTTPStopped, err: http.ErrServerClosed},
			},
			wantError: "gRPC server stopped before the run completed",
		},
		{
			name: "serve nil before cancellation is failure",
			events: []lifecycleEvent{
				{kind: lifecycleHTTPStopped},
				{kind: lifecycleCanceled},
				{kind: lifecycleGRPCStopped, err: grpc.ErrServerStopped},
			},
			wantError: "HTTP server stopped before the run completed",
		},
		{
			name: "completion before serve sentinel succeeds",
			events: []lifecycleEvent{
				{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusPass}}},
				{kind: lifecycleGRPCStopped, err: grpc.ErrServerStopped},
				{kind: lifecycleHTTPStopped, err: http.ErrServerClosed},
			},
			wantStatus: report.StatusPass,
		},
		{
			name: "completion before unexpected serve error reports failure",
			events: []lifecycleEvent{
				{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusPass}}},
				{kind: lifecycleHTTPStopped, err: errors.New("late HTTP failure")},
				{kind: lifecycleGRPCStopped, err: grpc.ErrServerStopped},
			},
			wantError: "serve HTTP: late HTTP failure",
		},
		{
			name: "cancellation before queued serve error preserves interrupted result",
			events: []lifecycleEvent{
				{kind: lifecycleCanceled},
				{kind: lifecycleHTTPStopped, err: errors.New("late HTTP failure")},
				{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusInterrupted}}},
				{kind: lifecycleGRPCStopped, err: grpc.ErrServerStopped},
			},
			wantStatus: report.StatusInterrupted,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := make(chan lifecycleEvent, len(test.events)+2)
			for _, event := range test.events {
				events <- event
			}
			grpcServer := newBlockingGRPCServer()
			httpServer := newBlockingHTTPServer()

			summary, err := runCoordinateLifecycle(t, events, grpcServer, httpServer)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("coordinateLifecycle() error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("coordinateLifecycle() error = %v", err)
			}
			if summary.Status != test.wantStatus {
				t.Fatalf("status = %s, want %s", summary.Status, test.wantStatus)
			}
		})
	}
}

func TestServeRunTreatsUnsolicitedNilAndSentinelAsFailures(t *testing.T) {
	tests := []struct {
		name       string
		grpcError  error
		httpError  error
		grpcReturn bool
		httpReturn bool
		wantServer string
	}{
		{name: "gRPC nil", grpcReturn: true, wantServer: "gRPC server stopped"},
		{name: "gRPC sentinel", grpcError: grpc.ErrServerStopped, grpcReturn: true, wantServer: "gRPC server stopped"},
		{name: "HTTP nil", httpReturn: true, wantServer: "HTTP server stopped"},
		{name: "HTTP sentinel", httpError: http.ErrServerClosed, httpReturn: true, wantServer: "HTTP server stopped"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			grpcServer := newBlockingGRPCServer()
			httpServer := newBlockingHTTPServer()
			grpcServer.serveErr = test.grpcError
			httpServer.serveErr = test.httpError
			grpcServer.returnImmediately = test.grpcReturn
			httpServer.returnImmediately = test.httpReturn
			events := make(chan lifecycleEvent, 4)

			_, err := runServeRun(t, func() {}, grpcServer, httpServer, events)
			if err == nil || !strings.Contains(err.Error(), test.wantServer) {
				t.Fatalf("serveRun() error = %v, want %q", err, test.wantServer)
			}
			assertServersExited(t, grpcServer, httpServer)
		})
	}
}

func TestServeRunForcesHTTPCloseAfterShutdownFailure(t *testing.T) {
	shutdownErr := errors.New("shutdown failed")
	httpServer := newBlockingHTTPServer()
	httpServer.shutdownErr = shutdownErr
	grpcServer := newBlockingGRPCServer()
	events := make(chan lifecycleEvent, 4)
	events <- lifecycleEvent{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusPass}}}

	_, err := runServeRun(t, func() {}, grpcServer, httpServer, events)
	if !errors.Is(err, shutdownErr) {
		t.Fatalf("serveRun() error = %v, want shutdown failure", err)
	}
	if !httpServer.closeCalled() {
		t.Fatal("HTTP Close was not called after Shutdown failed")
	}
	assertServersExited(t, grpcServer, httpServer)
}

func TestServeRunForcesHTTPCloseAfterShutdownDeadline(t *testing.T) {
	httpServer := newBlockingHTTPServer()
	httpServer.waitForShutdownDeadline = true
	grpcServer := newBlockingGRPCServer()
	events := make(chan lifecycleEvent, 4)
	events <- lifecycleEvent{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusPass}}}

	_, err := runServeRun(t, func() {}, grpcServer, httpServer, events)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("serveRun() error = %v, want deadline exceeded", err)
	}
	if !httpServer.closeCalled() {
		t.Fatal("HTTP Close was not called after Shutdown timed out")
	}
	assertServersExited(t, grpcServer, httpServer)
}

func TestServeRunPreservesInterruptedResultAfterForcedHTTPClose(t *testing.T) {
	httpServer := newBlockingHTTPServer()
	httpServer.shutdownErr = errors.New("shutdown failed")
	grpcServer := newBlockingGRPCServer()
	events := make(chan lifecycleEvent, 4)
	events <- lifecycleEvent{kind: lifecycleCanceled}
	events <- lifecycleEvent{kind: lifecycleResult, result: runResult{summary: report.Summary{Status: report.StatusInterrupted}}}

	summary, err := runServeRun(t, func() {}, grpcServer, httpServer, events)
	if err != nil {
		t.Fatalf("serveRun() error = %v", err)
	}
	if summary.Status != report.StatusInterrupted {
		t.Fatalf("status = %s, want %s", summary.Status, report.StatusInterrupted)
	}
	if !httpServer.closeCalled() {
		t.Fatal("HTTP Close was not called after Shutdown failed")
	}
	assertServersExited(t, grpcServer, httpServer)
}

func TestStopServersForceClosesActiveHTTPHandlerAfterShutdownDeadline(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), coordinatorTestTimeout)
	defer cancelTest()
	handlerStarted := make(chan struct{})
	handlerExited := make(chan struct{})
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		defer close(handlerExited)
		close(handlerStarted)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-request.Context().Done()
	})}
	listener := listenLocal(t)
	defer listener.Close()
	defer httpServer.Close()

	serveResults := make(chan error, 1)
	go func() {
		serveResults <- httpServer.Serve(listener)
	}()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, "http://"+listener.Addr().String(), nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	type responseResult struct {
		response *http.Response
		err      error
	}
	responses := make(chan responseResult, 1)
	go func() {
		response, requestErr := transport.RoundTrip(request)
		responses <- responseResult{response: response, err: requestErr}
	}()

	awaitSignal(t, testCtx.Done(), handlerStarted, "HTTP handler did not start")
	var response *http.Response
	select {
	case result := <-responses:
		if result.err != nil {
			t.Fatalf("send request: %v", result.err)
		}
		response = result.response
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("response status = %d, want %d", response.StatusCode, http.StatusOK)
		}
	case <-testCtx.Done():
		t.Fatal("HTTP response headers were not received before test timeout")
	}
	select {
	case <-handlerExited:
		t.Fatal("HTTP handler exited before server shutdown")
	default:
	}

	stopResults := make(chan error, 1)
	go func() {
		stopResults <- stopServers(func() {}, newBlockingGRPCServer(), httpServer, 20*time.Millisecond)
	}()

	select {
	case serveErr := <-serveResults:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			t.Fatalf("HTTP Serve error = %v, want %v", serveErr, http.ErrServerClosed)
		}
	case <-testCtx.Done():
		t.Fatal("HTTP Serve did not return before test timeout")
	}

	select {
	case stopErr := <-stopResults:
		if !errors.Is(stopErr, context.DeadlineExceeded) {
			t.Fatalf("stopServers() error = %v, want shutdown deadline", stopErr)
		}
	case <-testCtx.Done():
		t.Fatal("stopServers did not return before test timeout")
	}
	awaitSignal(t, testCtx.Done(), handlerExited, "HTTP Close did not release the active handler")
}

func awaitSignal(t *testing.T, timeout, signal <-chan struct{}, timeoutMessage string) {
	t.Helper()
	select {
	case <-signal:
	case <-timeout:
		t.Fatal(timeoutMessage)
	}
}

const coordinatorTestTimeout = 500 * time.Millisecond

type serveRunOutcome struct {
	summary report.Summary
	err     error
}

func runServeRun(
	t *testing.T,
	cancel context.CancelFunc,
	grpcServer *blockingGRPCServer,
	httpServer *blockingHTTPServer,
	events chan lifecycleEvent,
) (report.Summary, error) {
	t.Helper()
	outcomes := make(chan serveRunOutcome, 1)
	go func() {
		summary, err := serveRun(cancel, grpcServer, httpServer, nil, nil, events, 20*time.Millisecond)
		outcomes <- serveRunOutcome{summary: summary, err: err}
	}()
	return awaitServeRunOutcome(t, outcomes)
}

func runCoordinateLifecycle(
	t *testing.T,
	events <-chan lifecycleEvent,
	grpcServer *blockingGRPCServer,
	httpServer *blockingHTTPServer,
) (report.Summary, error) {
	t.Helper()
	outcomes := make(chan serveRunOutcome, 1)
	go func() {
		summary, err := coordinateLifecycle(events, func() {}, grpcServer, httpServer, 20*time.Millisecond)
		outcomes <- serveRunOutcome{summary: summary, err: err}
	}()
	return awaitServeRunOutcome(t, outcomes)
}

func awaitServeRunOutcome(t *testing.T, outcomes <-chan serveRunOutcome) (report.Summary, error) {
	t.Helper()
	timer := time.NewTimer(coordinatorTestTimeout)
	defer timer.Stop()
	select {
	case outcome := <-outcomes:
		return outcome.summary, outcome.err
	case <-timer.C:
		t.Fatal("lifecycle coordinator did not return before test timeout")
		return report.Summary{}, nil
	}
}

type blockingGRPCServer struct {
	serveErr          error
	returnImmediately bool
	stopped           chan struct{}
	exited            chan struct{}
	once              sync.Once
}

func newBlockingGRPCServer() *blockingGRPCServer {
	return &blockingGRPCServer{stopped: make(chan struct{}), exited: make(chan struct{})}
}

func newReturningGRPCServer(serveErr error) *blockingGRPCServer {
	server := newBlockingGRPCServer()
	server.serveErr = serveErr
	server.returnImmediately = true
	return server
}

func (s *blockingGRPCServer) Serve(net.Listener) error {
	defer close(s.exited)
	if s.returnImmediately {
		return s.serveErr
	}
	<-s.stopped
	return grpc.ErrServerStopped
}

func (s *blockingGRPCServer) Stop() {
	s.once.Do(func() { close(s.stopped) })
}

type blockingHTTPServer struct {
	serveErr                error
	returnImmediately       bool
	shutdownErr             error
	waitForShutdownDeadline bool
	stopped                 chan struct{}
	exited                  chan struct{}
	closed                  chan struct{}
	stopOnce                sync.Once
	closeOnce               sync.Once
}

func newBlockingHTTPServer() *blockingHTTPServer {
	return &blockingHTTPServer{
		stopped: make(chan struct{}),
		exited:  make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func newReturningHTTPServer(serveErr error) *blockingHTTPServer {
	server := newBlockingHTTPServer()
	server.serveErr = serveErr
	server.returnImmediately = true
	return server
}

func (s *blockingHTTPServer) Serve(net.Listener) error {
	defer close(s.exited)
	if s.returnImmediately {
		return s.serveErr
	}
	<-s.stopped
	return http.ErrServerClosed
}

func (s *blockingHTTPServer) Shutdown(ctx context.Context) error {
	if s.waitForShutdownDeadline {
		<-ctx.Done()
		return ctx.Err()
	}
	if s.shutdownErr != nil {
		return s.shutdownErr
	}
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

func (s *blockingHTTPServer) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

func (s *blockingHTTPServer) closeCalled() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func assertServersExited(t *testing.T, grpcServer *blockingGRPCServer, httpServer *blockingHTTPServer) {
	t.Helper()
	select {
	case <-grpcServer.exited:
	case <-time.After(coordinatorTestTimeout):
		t.Error("gRPC Serve did not exit before test timeout")
	}
	select {
	case <-httpServer.exited:
	case <-time.After(coordinatorTestTimeout):
		t.Error("HTTP Serve did not exit before test timeout")
	}
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return listener
}

func reserveLocalAddress(t *testing.T) string {
	t.Helper()
	listener := listenLocal(t)
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}
	return address
}

func writeOrchestratorConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "loadtest.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(contents)), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
