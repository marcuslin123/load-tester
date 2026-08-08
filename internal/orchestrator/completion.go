package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/marcuslin123/load-tester/internal/report"
)

// WaitForResult waits for the immutable run window, its expected final deltas,
// and at most one grace period before evaluating the fleet-wide snapshot.
func (s *Server) WaitForResult(ctx context.Context, gracePeriod time.Duration) (report.Summary, error) {
	if s.assignments == nil || s.metrics == nil {
		return report.Summary{}, errors.New("load assignments are not configured")
	}
	if gracePeriod <= 0 {
		return report.Summary{}, errors.New("metrics grace period must be greater than zero")
	}

	var window runWindow
	select {
	case window = <-s.assignments.started:
	case <-ctx.Done():
		return report.Interrupted(
			s.assignments.config.Name,
			s.assignments.config.Load.Model,
			0,
			s.metrics.Snapshot(),
		), nil
	}
	if !waitUntil(ctx, window.deadline) {
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
	summary := report.Evaluate(
		cfg.Name,
		cfg.Load.Model,
		cfg.Load.Duration,
		s.metrics.Snapshot(),
		cfg.Thresholds,
	)
	integrity := s.metrics.Violations()
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

func elapsedWithin(window runWindow, now time.Time) time.Duration {
	if now.Before(window.startsAt) {
		return 0
	}
	if now.After(window.deadline) {
		return window.deadline.Sub(window.startsAt)
	}
	return now.Sub(window.startsAt)
}
