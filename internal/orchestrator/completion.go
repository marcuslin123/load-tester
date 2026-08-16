package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/loadgen"
	"github.com/marcuslin123/load-tester/internal/report"
)

// RunWindow is the immutable absolute timing shared by workers and controllers.
type RunWindow struct {
	StartsAt time.Time
	Deadline time.Time
}

// WaitForRunWindow lets independent lifecycle components observe the same run start.
func (s *Server) WaitForRunWindow(ctx context.Context) (RunWindow, error) {
	if s.assignments == nil {
		return RunWindow{}, errors.New("load assignments are not configured")
	}
	select {
	case <-s.assignments.started:
		return s.assignments.window, nil
	case <-ctx.Done():
		return RunWindow{}, ctx.Err()
	}
}

// WaitForResult waits for the immutable run window, its expected final deltas,
// and at most one grace period before evaluating the fleet-wide snapshot.
func (s *Server) WaitForResult(ctx context.Context, gracePeriod time.Duration) (report.Summary, error) {
	if s.assignments == nil || s.metrics == nil {
		return report.Summary{}, errors.New("load assignments are not configured")
	}
	if gracePeriod <= 0 {
		return report.Summary{}, errors.New("metrics grace period must be greater than zero")
	}

	window, err := s.WaitForRunWindow(ctx)
	if err != nil {
		return report.Interrupted(
			s.assignments.config.Name,
			s.assignments.config.Load.Model,
			0,
			s.metrics.Snapshot(),
		), nil
	}
	if !waitUntil(ctx, window.Deadline) {
		return report.Interrupted(
			s.assignments.config.Name,
			s.assignments.config.Load.Model,
			elapsedWithin(window, time.Now()),
			s.metrics.Snapshot(),
		), nil
	}

	grace := time.NewTimer(gracePeriod)
	defer grace.Stop()
	for !s.metrics.Complete() {
		select {
		case <-s.metrics.Changes():
		case <-grace.C:
			return s.finalSummary(), nil
		case <-ctx.Done():
			return report.Interrupted(
				s.assignments.config.Name,
				s.assignments.config.Load.Model,
				elapsedWithin(window, time.Now()),
				s.metrics.Snapshot(),
			), nil
		}
	}
	return s.finalSummary(), nil
}

func (s *Server) finalSummary() report.Summary {
	cfg := s.assignments.config
	snapshot := s.metrics.Snapshot()
	var scheduleIntegrity string
	if cfg.Load.Model == config.LoadConstantRate {
		expected := loadgen.ScheduledArrivalsBetween(
			s.assignments.startsAt,
			s.assignments.startsAt,
			s.assignments.deadline,
			cfg.Load.Rate,
			cfg.Load.RampUp,
		)
		processed := snapshot.AdmittedArrivals + snapshot.UnmetDemand
		overflow := processed < snapshot.AdmittedArrivals
		if overflow || processed > expected {
			scheduleIntegrity = fmt.Sprintf(
				"arrival outcomes exceed expected schedule: admitted=%d unmet=%d expected=%d",
				snapshot.AdmittedArrivals,
				snapshot.UnmetDemand,
				expected,
			)
		} else if processed < expected {
			snapshot.UnmetDemand += expected - processed
		}
	}
	summary := report.Evaluate(
		cfg.Name,
		cfg.Load.Model,
		cfg.Load.Duration,
		snapshot,
		cfg.Thresholds,
	)
	integrity := s.metrics.Violations()
	if scheduleIntegrity != "" {
		integrity = append(integrity, scheduleIntegrity)
	}
	if missing := s.metrics.MissingFinal(); len(missing) > 0 {
		integrity = append(integrity, fmt.Sprintf("missing final metrics: %s", strings.Join(missing, ", ")))
	}
	if len(integrity) > 0 {
		summary.Violations = append(summary.Violations, integrity...)
		summary.Status = report.StatusFail
	}
	return summary
}

func waitUntil(ctx context.Context, deadline time.Time) bool {
	delay := time.Until(deadline)
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func elapsedWithin(window RunWindow, now time.Time) time.Duration {
	if now.Before(window.StartsAt) {
		return 0
	}
	if now.After(window.Deadline) {
		return window.Deadline.Sub(window.StartsAt)
	}
	return now.Sub(window.StartsAt)
}
