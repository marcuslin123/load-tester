package config

import (
	"strings"
	"testing"
	"time"
)

const exampleConfig = `
name: checkout-api-stress
target:
  protocol: http
  url: http://target:8080/api/checkout
  method: POST
  headers:
    Content-Type: application/json
  body: '{"item_id": 42}'
load:
  model: constant-vus
  virtual_users: 50000
  duration: 5m
  ramp_up: 30s
fleet:
  min_workers: 2
  max_workers: 30
  autoscale: true
thresholds:
  p99_latency: 500ms
  error_rate: 0.01
`

func TestParseReadsExampleConfig(t *testing.T) {
	cfg, err := Parse(strings.NewReader(exampleConfig))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if cfg.Name != "checkout-api-stress" {
		t.Fatalf("Name = %q, want checkout-api-stress", cfg.Name)
	}
	if cfg.Target.Protocol != "http" {
		t.Fatalf("Target.Protocol = %q, want http", cfg.Target.Protocol)
	}
	if cfg.Target.URL != "http://target:8080/api/checkout" {
		t.Fatalf("Target.URL = %q", cfg.Target.URL)
	}
	if cfg.Target.Method != "POST" {
		t.Fatalf("Target.Method = %q, want POST", cfg.Target.Method)
	}
	if cfg.Target.Headers["Content-Type"] != "application/json" {
		t.Fatalf("Target.Headers[Content-Type] = %q", cfg.Target.Headers["Content-Type"])
	}
	if cfg.Target.Body != `{"item_id": 42}` {
		t.Fatalf("Target.Body = %q", cfg.Target.Body)
	}
	if cfg.Load.Model != "constant-vus" {
		t.Fatalf("Load.Model = %q, want constant-vus", cfg.Load.Model)
	}
	if cfg.Load.VirtualUsers != 50000 {
		t.Fatalf("Load.VirtualUsers = %d, want 50000", cfg.Load.VirtualUsers)
	}
	if cfg.Load.Duration != 5*time.Minute {
		t.Fatalf("Load.Duration = %s, want 5m", cfg.Load.Duration)
	}
	if cfg.Load.RampUp != 30*time.Second {
		t.Fatalf("Load.RampUp = %s, want 30s", cfg.Load.RampUp)
	}
	if cfg.Fleet.MinWorkers != 2 {
		t.Fatalf("Fleet.MinWorkers = %d, want 2", cfg.Fleet.MinWorkers)
	}
	if cfg.Fleet.MaxWorkers != 30 {
		t.Fatalf("Fleet.MaxWorkers = %d, want 30", cfg.Fleet.MaxWorkers)
	}
	if !cfg.Fleet.Autoscale {
		t.Fatalf("Fleet.Autoscale = false, want true")
	}
	if cfg.Thresholds.P99Latency != 500*time.Millisecond {
		t.Fatalf("Thresholds.P99Latency = %s, want 500ms", cfg.Thresholds.P99Latency)
	}
	if cfg.Thresholds.ErrorRate != 0.01 {
		t.Fatalf("Thresholds.ErrorRate = %f, want 0.01", cfg.Thresholds.ErrorRate)
	}
}

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse(strings.NewReader(`
name: local-smoke
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: constant-vus
  virtual_users: 1
  duration: 10s
fleet:
  min_workers: 1
  max_workers: 1
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if cfg.Target.Method != "GET" {
		t.Fatalf("Target.Method = %q, want GET", cfg.Target.Method)
	}
	if cfg.Target.Headers == nil {
		t.Fatalf("Target.Headers is nil, want empty map")
	}
}

func TestParseReadsConstantRateConfig(t *testing.T) {
	cfg, err := Parse(strings.NewReader(`
name: rate-smoke
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: constant-rate
  rate: 250
  duration: 10s
fleet:
  min_workers: 1
  max_workers: 2
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if cfg.Load.Model != "constant-rate" {
		t.Fatalf("Load.Model = %q, want constant-rate", cfg.Load.Model)
	}
	if cfg.Load.Rate != 250 {
		t.Fatalf("Load.Rate = %d, want 250", cfg.Load.Rate)
	}
}

func TestParseRejectsInvalidFields(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing name",
			yaml: `
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: constant-vus
  virtual_users: 1
  duration: 10s
fleet:
  min_workers: 1
  max_workers: 1
`,
			want: "name",
		},
		{
			name: "unsupported protocol",
			yaml: `
name: bad-protocol
target:
  protocol: grpc
  url: http://target:8080/echo
load:
  model: constant-vus
  virtual_users: 1
  duration: 10s
fleet:
  min_workers: 1
  max_workers: 1
`,
			want: "target.protocol",
		},
		{
			name: "bare duration",
			yaml: `
name: bare-duration
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: constant-vus
  virtual_users: 1
  duration: 30
fleet:
  min_workers: 1
  max_workers: 1
`,
			want: "load.duration",
		},
		{
			name: "bad load model",
			yaml: `
name: bad-model
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: burst
  virtual_users: 1
  duration: 10s
fleet:
  min_workers: 1
  max_workers: 1
`,
			want: "load.model",
		},
		{
			name: "invalid fleet bounds",
			yaml: `
name: bad-fleet
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: constant-vus
  virtual_users: 1
  duration: 10s
fleet:
  min_workers: 3
  max_workers: 2
`,
			want: "fleet.max_workers",
		},
		{
			name: "invalid error rate",
			yaml: `
name: bad-threshold
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: constant-vus
  virtual_users: 1
  duration: 10s
fleet:
  min_workers: 1
  max_workers: 1
thresholds:
  error_rate: 2
`,
			want: "thresholds.error_rate",
		},
		{
			name: "constant rate missing rate",
			yaml: `
name: missing-rate
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: constant-rate
  duration: 10s
fleet:
  min_workers: 1
  max_workers: 1
`,
			want: "load.rate",
		},
		{
			name: "unknown field",
			yaml: `
name: unknown-field
unexpected: true
target:
  protocol: http
  url: http://target:8080/echo
load:
  model: constant-vus
  virtual_users: 1
  duration: 10s
fleet:
  min_workers: 1
  max_workers: 1
`,
			want: "unexpected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.yaml))
			if err == nil {
				t.Fatal("Parse() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Parse() error = %q, want field %q", err.Error(), tt.want)
			}
		})
	}
}
