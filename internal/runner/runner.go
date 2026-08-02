// Package runner coordinates one local preflight and timed load-test run.
package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/loadgen"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/protocol"
	"github.com/marcuslin123/load-tester/internal/report"
)

const defaultMetricsBufferSize = 65_536

type resultSink interface {
	Offer(protocol.Result) bool
}

type openSink interface {
	resultSink
	OfferUnmetDemand() bool
}

type closedOptions = loadgen.ClosedOptions
type openOptions = loadgen.OpenOptions

type schedulerDependencies struct {
	runClosed func(context.Context, protocol.Protocol, resultSink, closedOptions) error
	runOpen   func(context.Context, protocol.Protocol, openSink, openOptions) error
}

var productionSchedulers = schedulerDependencies{
	runClosed: func(ctx context.Context, executor protocol.Protocol, sink resultSink, options closedOptions) error {
		return loadgen.RunClosed(ctx, executor, sink, options)
	},
	runOpen: func(ctx context.Context, executor protocol.Protocol, sink openSink, options openOptions) error {
		return loadgen.RunOpen(ctx, executor, sink, options)
	},
}

// Options controls local runner behavior that is not part of the load-test file.
type Options struct {
	PreflightTimeout time.Duration
}

// Run verifies target reachability, executes the configured scheduler, and builds its summary.
func Run(ctx context.Context, cfg config.Config, executor protocol.Protocol, options Options) (report.Summary, error) {
	if executor == nil {
		return report.Summary{}, errors.New("protocol executor is required")
	}
	if options.PreflightTimeout <= 0 {
		return report.Summary{}, errors.New("preflight timeout must be greater than zero")
	}
	if err := preflight(ctx, executor, options.PreflightTimeout); err != nil {
		if ctx.Err() != nil {
			return report.Interrupted(cfg.Name, cfg.Load.Model, 0, metrics.Snapshot{}), nil
		}
		return report.Summary{}, err
	}

	collector, err := metrics.NewCollector(defaultMetricsBufferSize)
	if err != nil {
		return report.Summary{}, fmt.Errorf("create metrics collector: %w", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, cfg.Load.Duration)
	started := time.Now()
	err = executeLoad(runCtx, cfg, executor, collector, productionSchedulers)
	elapsed := time.Since(started)
	cancel()
	snapshot := collector.Close()
	if err != nil {
		return report.Summary{}, fmt.Errorf("run load test: %w", err)
	}

	if ctx.Err() != nil {
		return report.Interrupted(cfg.Name, cfg.Load.Model, elapsed, snapshot), nil
	}
	return report.Evaluate(cfg.Name, cfg.Load.Model, elapsed, snapshot, cfg.Thresholds), nil
}

// preflight sends the exact configured request once without recording it in test metrics.
func preflight(ctx context.Context, executor protocol.Protocol, timeout time.Duration) error {
	preflightCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result := executor.Execute(preflightCtx)
	if result.Err != nil {
		return fmt.Errorf("preflight request failed: %w", result.Err)
	}
	return nil
}

func executeLoad(
	ctx context.Context,
	cfg config.Config,
	executor protocol.Protocol,
	sink openSink,
	schedulers schedulerDependencies,
) error {
	switch cfg.Load.Model {
	case config.LoadConstantVUs:
		return schedulers.runClosed(ctx, executor, sink, loadgen.ClosedOptions{
			VirtualUsers: cfg.Load.VirtualUsers,
			RampUp:       cfg.Load.RampUp,
		})
	case config.LoadConstantRate:
		return schedulers.runOpen(ctx, executor, sink, loadgen.OpenOptions{
			Rate:        cfg.Load.Rate,
			MaxInFlight: cfg.Load.MaxInFlight,
			RampUp:      cfg.Load.RampUp,
		})
	default:
		return fmt.Errorf("unsupported load model %q", cfg.Load.Model)
	}
}
