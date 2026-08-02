package runner

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/protocol"
	"github.com/marcuslin123/load-tester/internal/report"
)

func TestRunExcludesPreflightFromMetrics(t *testing.T) {
	t.Parallel()

	var calls atomic.Uint64
	executor := protocolFunc(func(context.Context) protocol.Result {
		calls.Add(1)
		return protocol.Result{Latency: time.Millisecond, StatusCode: 200}
	})

	summary, err := Run(context.Background(), closedConfig(20*time.Millisecond), executor, Options{
		PreflightTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.Status != report.StatusPass {
		t.Fatalf("Status = %q, want %q; violations = %v", summary.Status, report.StatusPass, summary.Violations)
	}
	if summary.Metrics.Requests == 0 {
		t.Fatal("Requests = 0, want at least one measured request")
	}
	if got, want := calls.Load(), summary.Metrics.Requests+1; got != want {
		t.Fatalf("executor calls = %d, want %d (measured requests plus one preflight)", got, want)
	}
}

func TestRunFailsPreflightBeforeStartingLoad(t *testing.T) {
	t.Parallel()

	var calls atomic.Uint64
	executor := protocolFunc(func(context.Context) protocol.Result {
		calls.Add(1)
		return protocol.Result{Err: errors.New("connection refused")}
	})

	_, err := Run(context.Background(), closedConfig(time.Second), executor, Options{
		PreflightTimeout: time.Second,
	})
	if err == nil {
		t.Fatal("Run() error = nil, want preflight failure")
	}
	if !strings.Contains(err.Error(), "preflight") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("Run() error = %q, want clear preflight connection error", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("executor calls = %d, want only one preflight call", got)
	}
}

func TestRunAppliesPreflightTimeout(t *testing.T) {
	t.Parallel()

	executor := protocolFunc(func(ctx context.Context) protocol.Result {
		<-ctx.Done()
		return protocol.Result{Err: ctx.Err()}
	})

	started := time.Now()
	_, err := Run(context.Background(), closedConfig(time.Second), executor, Options{
		PreflightTimeout: 10 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "preflight") {
		t.Fatalf("Run() error = %v, want preflight timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("Run() took %v, want preflight to fail quickly", elapsed)
	}
}

func TestRunTreatsPreflightCancellationAsInterruption(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	executor := protocolFunc(func(requestCtx context.Context) protocol.Result {
		cancel()
		<-requestCtx.Done()
		return protocol.Result{Err: requestCtx.Err()}
	})

	summary, err := Run(ctx, closedConfig(time.Second), executor, Options{
		PreflightTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.Status != report.StatusInterrupted {
		t.Fatalf("Status = %q, want %q", summary.Status, report.StatusInterrupted)
	}
	if summary.Metrics.Requests != 0 {
		t.Fatalf("Requests = %d, want no measured requests before the run starts", summary.Metrics.Requests)
	}
}

func TestRunTreatsParentCancellationAsInterruption(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Uint64
	executor := protocolFunc(func(requestCtx context.Context) protocol.Result {
		if calls.Add(1) == 1 {
			return protocol.Result{Latency: time.Millisecond, StatusCode: 200}
		}
		cancel()
		<-requestCtx.Done()
		return protocol.Result{Latency: time.Millisecond, Err: requestCtx.Err()}
	})

	summary, err := Run(ctx, closedConfig(time.Second), executor, Options{
		PreflightTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.Status != report.StatusInterrupted {
		t.Fatalf("Status = %q, want %q", summary.Status, report.StatusInterrupted)
	}
	if len(summary.Violations) != 0 {
		t.Fatalf("Violations = %v, want interrupted run to skip evaluation", summary.Violations)
	}
}

func TestExecuteLoadSelectsConfiguredScheduler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        config.Config
		wantClosed int
		wantOpen   int
	}{
		{name: "closed", cfg: closedConfig(time.Second), wantClosed: 1},
		{name: "open", cfg: openConfig(time.Second), wantOpen: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var closedCalls, openCalls int
			dependencies := schedulerDependencies{
				runClosed: func(context.Context, protocol.Protocol, resultSink, closedOptions) error {
					closedCalls++
					return nil
				},
				runOpen: func(context.Context, protocol.Protocol, openSink, openOptions) error {
					openCalls++
					return nil
				},
			}

			err := executeLoad(context.Background(), test.cfg, protocolFunc(func(context.Context) protocol.Result {
				return protocol.Result{}
			}), nil, dependencies)
			if err != nil {
				t.Fatalf("executeLoad() error = %v", err)
			}
			if closedCalls != test.wantClosed || openCalls != test.wantOpen {
				t.Fatalf("scheduler calls = closed %d, open %d; want closed %d, open %d", closedCalls, openCalls, test.wantClosed, test.wantOpen)
			}
		})
	}
}

type protocolFunc func(context.Context) protocol.Result

func (f protocolFunc) Execute(ctx context.Context) protocol.Result {
	return f(ctx)
}

func closedConfig(duration time.Duration) config.Config {
	return config.Config{
		Name: "closed-test",
		Load: config.Load{
			Model:        config.LoadConstantVUs,
			VirtualUsers: 1,
			Duration:     duration,
		},
	}
}

func openConfig(duration time.Duration) config.Config {
	return config.Config{
		Name: "open-test",
		Load: config.Load{
			Model:       config.LoadConstantRate,
			Rate:        10,
			MaxInFlight: 1,
			Duration:    duration,
		},
	}
}
