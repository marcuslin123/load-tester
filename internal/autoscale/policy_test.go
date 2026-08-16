package autoscale

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestEvaluateTracksDeficitsAndScalesOnSecondInterval(t *testing.T) {
	t.Parallel()

	config := testPolicyConfig()
	first, err := Evaluate(config, State{}, Observation{
		At:                time.Unix(100, 0),
		ExpectedArrivals:  100,
		AdmittedArrivals:  80,
		DesiredWorkers:    2,
		RegisteredWorkers: 2,
	})
	if err != nil {
		t.Fatalf("first Evaluate() error = %v", err)
	}
	if first.Scale || first.State.DeficientIntervals != 1 || first.Coverage != 0.8 {
		t.Fatalf("first decision = %+v, want one deficit without scaling", first)
	}

	second, err := Evaluate(config, first.State, Observation{
		At:                time.Unix(105, 0),
		ExpectedArrivals:  100,
		AdmittedArrivals:  80,
		DesiredWorkers:    2,
		RegisteredWorkers: 2,
	})
	if err != nil {
		t.Fatalf("second Evaluate() error = %v", err)
	}
	if !second.Scale || second.DesiredWorkers != 3 || second.State.DeficientIntervals != 2 {
		t.Fatalf("second decision = %+v, want scale from 2 to 3", second)
	}
}

func TestEvaluateCountsUnmetDemandAsProcessedSchedule(t *testing.T) {
	t.Parallel()

	decision, err := Evaluate(testPolicyConfig(), State{}, Observation{
		At:                time.Unix(100, 0),
		ExpectedArrivals:  100,
		AdmittedArrivals:  20,
		UnmetDemand:       80,
		DesiredWorkers:    2,
		RegisteredWorkers: 2,
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if decision.Scale || decision.Coverage != 1 || decision.State.DeficientIntervals != 0 {
		t.Fatalf("decision = %+v, want complete scheduler coverage", decision)
	}
}

func TestEvaluateResetsDeficitWhenFleetIsHealthyOrReplacing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		observation Observation
	}{
		{
			name: "healthy",
			observation: Observation{
				ExpectedArrivals:  100,
				AdmittedArrivals:  95,
				DesiredWorkers:    4,
				RegisteredWorkers: 4,
			},
		},
		{
			name: "replacement pending",
			observation: Observation{
				ExpectedArrivals:  100,
				AdmittedArrivals:  50,
				DesiredWorkers:    4,
				RegisteredWorkers: 3,
			},
		},
		{
			name: "no scheduled arrivals",
			observation: Observation{
				DesiredWorkers:    4,
				RegisteredWorkers: 4,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision, err := Evaluate(testPolicyConfig(), State{DeficientIntervals: 1}, test.observation)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if decision.Scale || decision.State.DeficientIntervals != 0 {
				t.Fatalf("decision = %+v, want reset without scaling", decision)
			}
		})
	}
}

func TestEvaluateRespectsCooldownGrowthAndMaximum(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	tests := []struct {
		name        string
		config      PolicyConfig
		state       State
		observation Observation
		wantScale   bool
		wantWorkers int
	}{
		{
			name:        "cooldown",
			config:      testPolicyConfig(),
			state:       State{DeficientIntervals: 1, LastScaleAt: now.Add(-10 * time.Second)},
			observation: deficientObservation(now, 100, 10, 4, 4),
			wantWorkers: 4,
		},
		{
			name:        "exact cooldown boundary",
			config:      testPolicyConfig(),
			state:       State{DeficientIntervals: 1, LastScaleAt: now.Add(-15 * time.Second)},
			observation: deficientObservation(now, 100, 80, 2, 2),
			wantScale:   true,
			wantWorkers: 3,
		},
		{
			name:        "zero coverage doubles",
			config:      testPolicyConfig(),
			state:       State{DeficientIntervals: 1},
			observation: deficientObservation(now, 100, 0, 3, 3),
			wantScale:   true,
			wantWorkers: 6,
		},
		{
			name:        "deficit ratio capped at double",
			config:      testPolicyConfig(),
			state:       State{DeficientIntervals: 1},
			observation: deficientObservation(now, 100, 10, 4, 4),
			wantScale:   true,
			wantWorkers: 8,
		},
		{
			name: "maximum clamps growth",
			config: PolicyConfig{
				Threshold:           0.9,
				RequiredDeficits:    2,
				Cooldown:            15 * time.Second,
				MaximumGrowthFactor: 2,
				MaxWorkers:          6,
			},
			state:       State{DeficientIntervals: 1},
			observation: deficientObservation(now, 100, 10, 4, 4),
			wantScale:   true,
			wantWorkers: 6,
		},
		{
			name:        "already at maximum",
			config:      testPolicyConfig(),
			state:       State{DeficientIntervals: 1},
			observation: deficientObservation(now, 100, 10, 8, 8),
			wantWorkers: 8,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision, err := Evaluate(test.config, test.state, test.observation)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if decision.Scale != test.wantScale || decision.DesiredWorkers != test.wantWorkers {
				t.Fatalf("decision = %+v, want scale=%t workers=%d", decision, test.wantScale, test.wantWorkers)
			}
		})
	}
}

func TestEvaluateClampsExtremeRatioBeforeIntegerConversion(t *testing.T) {
	t.Parallel()

	maximum := int(^uint(0) >> 1)
	decision, err := Evaluate(PolicyConfig{
		Threshold:           0.9,
		RequiredDeficits:    2,
		Cooldown:            15 * time.Second,
		MaximumGrowthFactor: 2,
		MaxWorkers:          maximum,
	}, State{DeficientIntervals: 1}, Observation{
		At:                time.Unix(100, 0),
		ExpectedArrivals:  ^uint64(0),
		AdmittedArrivals:  1,
		DesiredWorkers:    maximum - 1,
		RegisteredWorkers: maximum - 1,
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if !decision.Scale || decision.DesiredWorkers != maximum {
		t.Fatalf("decision = %+v, want safe clamp to %d", decision, maximum)
	}
}

func TestStateAfterScaleStartsCooldownAndResetsDeficit(t *testing.T) {
	t.Parallel()

	at := time.Unix(100, 0)
	state := (State{DeficientIntervals: 4}).AfterScale(at)
	if state.DeficientIntervals != 0 || !state.LastScaleAt.Equal(at) {
		t.Fatalf("AfterScale() = %+v, want reset deficit and timestamp", state)
	}
}

func TestEvaluateDoesNotStartCooldownBeforeScaleSucceeds(t *testing.T) {
	t.Parallel()

	decision, err := Evaluate(testPolicyConfig(), State{DeficientIntervals: 1}, deficientObservation(
		time.Unix(100, 0), 100, 80, 2, 2,
	))
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if !decision.Scale || !decision.State.LastScaleAt.IsZero() {
		t.Fatalf("decision = %+v, want scale without starting cooldown", decision)
	}
}

func TestEvaluateValidatesConfigurationAndObservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      PolicyConfig
		observation Observation
		want        string
	}{
		{name: "threshold", config: PolicyConfig{Threshold: 1.1}, want: "threshold"},
		{name: "NaN threshold", config: PolicyConfig{Threshold: math.NaN()}, want: "threshold"},
		{name: "required deficits", config: PolicyConfig{Threshold: 0.9}, want: "required deficits"},
		{name: "cooldown", config: PolicyConfig{Threshold: 0.9, RequiredDeficits: 2, Cooldown: -time.Second}, want: "cooldown"},
		{name: "growth", config: PolicyConfig{Threshold: 0.9, RequiredDeficits: 2, MaximumGrowthFactor: 1}, want: "growth"},
		{name: "maximum", config: PolicyConfig{Threshold: 0.9, RequiredDeficits: 2, MaximumGrowthFactor: 2}, want: "maximum"},
		{name: "desired", config: testPolicyConfig(), observation: Observation{RegisteredWorkers: 1}, want: "desired workers"},
		{name: "registered", config: testPolicyConfig(), observation: Observation{DesiredWorkers: 1, RegisteredWorkers: -1}, want: "registered workers"},
		{
			name:   "observation before last scale",
			config: testPolicyConfig(),
			observation: Observation{
				At:                time.Unix(99, 0),
				ExpectedArrivals:  100,
				DesiredWorkers:    1,
				RegisteredWorkers: 1,
			},
			want: "observation time",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := State{}
			if test.name == "observation before last scale" {
				state.LastScaleAt = time.Unix(100, 0)
			}
			_, err := Evaluate(test.config, state, test.observation)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Evaluate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func testPolicyConfig() PolicyConfig {
	return PolicyConfig{
		Threshold:           0.9,
		RequiredDeficits:    2,
		Cooldown:            15 * time.Second,
		MaximumGrowthFactor: 2,
		MaxWorkers:          8,
	}
}

func deficientObservation(at time.Time, expected, admitted uint64, desired, registered int) Observation {
	return Observation{
		At:                at,
		ExpectedArrivals:  expected,
		AdmittedArrivals:  admitted,
		DesiredWorkers:    desired,
		RegisteredWorkers: registered,
	}
}
