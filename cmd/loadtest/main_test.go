package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunRejectsMissingConfigFlag(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	code := run(context.Background(), nil, &stdout, &stderr)

	if code != exitSetupFailure {
		t.Fatalf("exit code = %d, want %d", code, exitSetupFailure)
	}
	if !strings.Contains(stderr.String(), "-config is required") {
		t.Fatalf("stderr = %q, want missing config message", stderr.String())
	}
}

func TestRunReturnsPassForHealthyTarget(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	configPath := writeConfig(t, server.URL, "20ms", "1s", "0")
	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"-config", configPath}, &stdout, &stderr)

	if code != exitPass {
		t.Fatalf("exit code = %d, want %d; stdout = %q; stderr = %q", code, exitPass, stdout.String(), stderr.String())
	}
	for _, want := range []string{"Test: cli-test", "Result: PASS", "Requests:"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout = %q, want substring %q", stdout.String(), want)
		}
	}
}

func TestRunReturnsTestFailureForThresholdViolation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	configPath := writeConfig(t, server.URL, "30ms", "1ms", "0")
	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"-config", configPath}, &stdout, &stderr)

	if code != exitTestFailure {
		t.Fatalf("exit code = %d, want %d; stdout = %q; stderr = %q", code, exitTestFailure, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "Result: FAIL") || !strings.Contains(stdout.String(), "p99 latency") {
		t.Fatalf("stdout = %q, want p99 threshold failure", stdout.String())
	}
}

func TestRunFailsFastForUnreachableTarget(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()
	configPath := writeConfig(t, url, "1s", "0", "0")
	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{
		"-config", configPath,
		"-preflight-timeout", "100ms",
	}, &stdout, &stderr)

	if code != exitSetupFailure {
		t.Fatalf("exit code = %d, want %d; stdout = %q; stderr = %q", code, exitSetupFailure, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "preflight request failed") {
		t.Fatalf("stderr = %q, want clear preflight error", stderr.String())
	}
	if strings.Contains(stdout.String(), "Result:") {
		t.Fatalf("stdout = %q, unreachable target should not print a measured result", stdout.String())
	}
}

func TestRunReturnsInterruptedForCanceledContext(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	configPath := writeConfig(t, server.URL, "1s", "0", "0")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	var stdout, stderr bytes.Buffer

	code := run(ctx, []string{"-config", configPath}, &stdout, &stderr)

	if code != exitInterrupted {
		t.Fatalf("exit code = %d, want %d; stdout = %q; stderr = %q", code, exitInterrupted, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "Result: INTERRUPTED") {
		t.Fatalf("stdout = %q, want interrupted partial summary", stdout.String())
	}
}

func writeConfig(t *testing.T, targetURL, duration, p99, errorRate string) string {
	t.Helper()

	contents := fmt.Sprintf(`name: cli-test
target:
  protocol: http
  url: %s
load:
  model: constant-vus
  virtual_users: 1
  duration: %s
fleet:
  min_workers: 1
  max_workers: 1
thresholds:
  p99_latency: %s
  error_rate: %s
`, targetURL, duration, p99, errorRate)
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
