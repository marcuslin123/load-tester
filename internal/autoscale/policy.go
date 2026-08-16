// Package autoscale decides and coordinates worker-fleet capacity changes.
package autoscale

import (
	"errors"
	"math"
	"time"
)

// PolicyConfig defines the fixed behavior of one scale-up policy.
type PolicyConfig struct {
	Threshold           float64
	RequiredDeficits    int
	Cooldown            time.Duration
	MaximumGrowthFactor int
	MaxWorkers          int
}

// State carries hysteresis and cooldown history between evaluations.
type State struct {
	DeficientIntervals int
	LastScaleAt        time.Time
}

// AfterScale records only a successful capacity change.
func (s State) AfterScale(at time.Time) State {
	s.DeficientIntervals = 0
	s.LastScaleAt = at
	return s
}

// Observation describes one complete control interval.
type Observation struct {
	At                time.Time
	ExpectedArrivals  uint64
	AdmittedArrivals  uint64
	UnmetDemand       uint64
	DesiredWorkers    int
	RegisteredWorkers int
}

// Decision reports the next policy state and whether capacity should change.
type Decision struct {
	DesiredWorkers int
	Scale          bool
	Reason         string
	Coverage       float64
	State          State
}

// Evaluate applies one deterministic policy step without performing fleet I/O.
func Evaluate(config PolicyConfig, state State, observation Observation) (Decision, error) {
	if err := validatePolicy(config, state, observation); err != nil {
		return Decision{}, err
	}

	decision := Decision{
		DesiredWorkers: observation.DesiredWorkers,
		State:          state,
	}
	if observation.ExpectedArrivals == 0 {
		decision.Coverage = 1
		decision.State.DeficientIntervals = 0
		decision.Reason = "no scheduled arrivals"
		return decision, nil
	}

	decision.Coverage = schedulerCoverage(observation)
	if observation.RegisteredWorkers != observation.DesiredWorkers {
		decision.State.DeficientIntervals = 0
		decision.Reason = "waiting for desired workers"
		return decision, nil
	}
	if decision.Coverage >= config.Threshold {
		decision.State.DeficientIntervals = 0
		decision.Reason = "scheduler coverage meets threshold"
		return decision, nil
	}

	decision.State.DeficientIntervals++
	if decision.State.DeficientIntervals < config.RequiredDeficits {
		decision.Reason = "waiting for consecutive deficit"
		return decision, nil
	}
	if !state.LastScaleAt.IsZero() && observation.At.Sub(state.LastScaleAt) < config.Cooldown {
		decision.Reason = "cooldown active"
		return decision, nil
	}
	if observation.DesiredWorkers >= config.MaxWorkers {
		decision.Reason = "maximum workers reached"
		return decision, nil
	}

	growthCap := config.MaxWorkers
	if observation.DesiredWorkers <= config.MaxWorkers/config.MaximumGrowthFactor {
		growthCap = observation.DesiredWorkers * config.MaximumGrowthFactor
	}
	capacityCap := min(growthCap, config.MaxWorkers)
	desired := capacityCap
	if decision.Coverage > float64(observation.DesiredWorkers)/float64(capacityCap) {
		desired = int(math.Ceil(float64(observation.DesiredWorkers) / decision.Coverage))
	}
	if desired <= observation.DesiredWorkers {
		decision.Reason = "calculated capacity is unchanged"
		return decision, nil
	}

	decision.DesiredWorkers = desired
	decision.Scale = true
	decision.Reason = "scheduler coverage requires scale up"
	return decision, nil
}

func schedulerCoverage(observation Observation) float64 {
	processed := observation.AdmittedArrivals + observation.UnmetDemand
	if processed < observation.AdmittedArrivals || processed >= observation.ExpectedArrivals {
		return 1
	}
	return float64(processed) / float64(observation.ExpectedArrivals)
}

func validatePolicy(config PolicyConfig, state State, observation Observation) error {
	if math.IsNaN(config.Threshold) || math.IsInf(config.Threshold, 0) ||
		config.Threshold <= 0 || config.Threshold > 1 {
		return errors.New("autoscale threshold must be greater than zero and at most one")
	}
	if config.RequiredDeficits <= 0 {
		return errors.New("required deficits must be greater than zero")
	}
	if config.Cooldown < 0 {
		return errors.New("cooldown must not be negative")
	}
	if config.MaximumGrowthFactor < 2 {
		return errors.New("maximum growth factor must be at least two")
	}
	if config.MaxWorkers <= 0 {
		return errors.New("maximum workers must be greater than zero")
	}
	if observation.DesiredWorkers <= 0 || observation.DesiredWorkers > config.MaxWorkers {
		return errors.New("desired workers must be greater than zero and at most maximum workers")
	}
	if observation.RegisteredWorkers < 0 {
		return errors.New("registered workers must not be negative")
	}
	if !state.LastScaleAt.IsZero() && (observation.At.IsZero() || observation.At.Before(state.LastScaleAt)) {
		return errors.New("observation time must not precede the last scale time")
	}
	return nil
}
