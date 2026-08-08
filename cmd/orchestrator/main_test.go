package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcuslin123/load-tester/internal/report"
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
}

func TestParseOptionsAcceptsAddressOverride(t *testing.T) {
	options, err := parseOptions([]string{
		"-config", "loadtest.yaml",
		"-addr", "127.0.0.1:19090",
	})
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if options.address != "127.0.0.1:19090" {
		t.Fatalf("address = %q, want 127.0.0.1:19090", options.address)
	}
}

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

func TestAwaitRunCompletionReturnsInterruptedResultWhenServerStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := make(chan runResult, 1)
	results <- runResult{summary: report.Summary{Status: report.StatusInterrupted}}
	serveErrors := make(chan error, 1)
	serveErrors <- errors.New("listener closed")

	summary, err := awaitRunCompletion(ctx, results, serveErrors, func() {})
	if err != nil {
		t.Fatalf("awaitRunCompletion() error = %v", err)
	}
	if summary.Status != report.StatusInterrupted {
		t.Fatalf("status = %s, want %s", summary.Status, report.StatusInterrupted)
	}
}

func TestAwaitRunCompletionDoesNotHideUnexpectedServerErrorAfterResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan runResult, 1)
	results <- runResult{summary: report.Summary{Status: report.StatusPass}}
	serveErrors := make(chan error, 1)
	serveErrors <- errors.New("listener failed")

	_, err := awaitRunCompletion(ctx, results, serveErrors, cancel)
	if err == nil || !strings.Contains(err.Error(), "listener failed") {
		t.Fatalf("awaitRunCompletion() error = %v, want listener failure", err)
	}
}

func writeOrchestratorConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "loadtest.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(contents)), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
