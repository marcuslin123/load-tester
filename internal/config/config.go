package config

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	ProtocolHTTP      = "http"
	LoadConstantVUs   = "constant-vus"
	LoadConstantRate  = "constant-rate"
	defaultHTTPMethod = http.MethodGet
)

var durationPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h)$`)

type Config struct {
	Name       string     `yaml:"name"`
	Target     Target     `yaml:"target"`
	Load       Load       `yaml:"load"`
	Fleet      Fleet      `yaml:"fleet"`
	Thresholds Thresholds `yaml:"thresholds"`
}

type Target struct {
	Protocol string            `yaml:"protocol"`
	URL      string            `yaml:"url"`
	Method   string            `yaml:"method"`
	Headers  map[string]string `yaml:"headers"`
	Body     string            `yaml:"body"`
}

type Load struct {
	Model        string        `yaml:"model"`
	VirtualUsers int           `yaml:"virtual_users"`
	Rate         int           `yaml:"rate"`
	MaxInFlight  int           `yaml:"max_in_flight"`
	Duration     time.Duration `yaml:"duration"`
	RampUp       time.Duration `yaml:"ramp_up"`
}

type Fleet struct {
	MinWorkers int  `yaml:"min_workers"`
	MaxWorkers int  `yaml:"max_workers"`
	Autoscale  bool `yaml:"autoscale"`
}

type Thresholds struct {
	P99Latency time.Duration `yaml:"p99_latency"`
	ErrorRate  float64       `yaml:"error_rate"`
}

type rawConfig struct {
	Name       string        `yaml:"name"`
	Target     Target        `yaml:"target"`
	Load       rawLoad       `yaml:"load"`
	Fleet      Fleet         `yaml:"fleet"`
	Thresholds rawThresholds `yaml:"thresholds"`
}

type rawLoad struct {
	Model        string       `yaml:"model"`
	VirtualUsers int          `yaml:"virtual_users"`
	Rate         int          `yaml:"rate"`
	MaxInFlight  int          `yaml:"max_in_flight"`
	Duration     durationText `yaml:"duration"`
	RampUp       durationText `yaml:"ramp_up"`
}

type rawThresholds struct {
	P99Latency durationText `yaml:"p99_latency"`
	ErrorRate  float64      `yaml:"error_rate"`
}

type durationText struct {
	Value string
}

func (d *durationText) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("must be a duration string")
	}
	d.Value = value.Value
	return nil
}

// Parse reads one load-test YAML file and returns the validated runtime config.
func Parse(r io.Reader) (Config, error) {
	var raw rawConfig
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	if err := decoder.Decode(&raw); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}

	cfg, err := raw.toConfig()
	if err != nil {
		return Config{}, err
	}
	applyDefaults(&cfg)
	if err := validate(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (raw rawConfig) toConfig() (Config, error) {
	loadDuration, err := parseDurationField("load.duration", raw.Load.Duration.Value)
	if err != nil {
		return Config{}, err
	}
	rampUp, err := parseOptionalDurationField("load.ramp_up", raw.Load.RampUp.Value)
	if err != nil {
		return Config{}, err
	}
	p99Latency, err := parseOptionalDurationField("thresholds.p99_latency", raw.Thresholds.P99Latency.Value)
	if err != nil {
		return Config{}, err
	}

	return Config{
		Name:   raw.Name,
		Target: raw.Target,
		Load: Load{
			Model:        raw.Load.Model,
			VirtualUsers: raw.Load.VirtualUsers,
			Rate:         raw.Load.Rate,
			MaxInFlight:  raw.Load.MaxInFlight,
			Duration:     loadDuration,
			RampUp:       rampUp,
		},
		Fleet: raw.Fleet,
		Thresholds: Thresholds{
			P99Latency: p99Latency,
			ErrorRate:  raw.Thresholds.ErrorRate,
		},
	}, nil
}

func parseDurationField(field, value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fieldError(field, "is required")
	}
	return parseOptionalDurationField(field, value)
}

func parseOptionalDurationField(field, value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "0" {
		return 0, nil
	}
	if !durationPattern.MatchString(trimmed) {
		return 0, fieldError(field, "must include a duration unit such as ms, s, or m")
	}
	parsed, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fieldError(field, err.Error())
	}
	return parsed, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Target.Method == "" {
		cfg.Target.Method = defaultHTTPMethod
	}
	cfg.Target.Method = strings.ToUpper(cfg.Target.Method)
	if cfg.Target.Headers == nil {
		cfg.Target.Headers = map[string]string{}
	}
}

func validate(cfg Config) error {
	if strings.TrimSpace(cfg.Name) == "" {
		return fieldError("name", "is required")
	}
	if cfg.Target.Protocol != ProtocolHTTP {
		return fieldError("target.protocol", "must be http")
	}
	if strings.TrimSpace(cfg.Target.URL) == "" {
		return fieldError("target.url", "is required")
	}
	if _, err := url.ParseRequestURI(cfg.Target.URL); err != nil {
		return fieldError("target.url", "must be a valid URL")
	}
	if cfg.Target.Method == "" {
		return fieldError("target.method", "is required")
	}
	if !validHTTPMethod(cfg.Target.Method) {
		return fieldError("target.method", "must be a valid HTTP method")
	}
	if cfg.Load.Model != LoadConstantVUs && cfg.Load.Model != LoadConstantRate {
		return fieldError("load.model", "must be constant-vus or constant-rate")
	}
	if cfg.Load.Model == LoadConstantVUs && cfg.Load.VirtualUsers <= 0 {
		return fieldError("load.virtual_users", "must be greater than 0")
	}
	if cfg.Load.Model == LoadConstantRate && cfg.Load.Rate <= 0 {
		return fieldError("load.rate", "must be greater than 0")
	}
	if cfg.Load.Model == LoadConstantRate && cfg.Load.MaxInFlight <= 0 {
		return fieldError("load.max_in_flight", "must be greater than 0")
	}
	if cfg.Load.Model == LoadConstantVUs && cfg.Load.MaxInFlight != 0 {
		return fieldError("load.max_in_flight", "is only valid for constant-rate")
	}
	if cfg.Load.Duration <= 0 {
		return fieldError("load.duration", "must be greater than 0")
	}
	if cfg.Load.RampUp < 0 {
		return fieldError("load.ramp_up", "must be greater than or equal to 0")
	}
	if cfg.Fleet.MinWorkers <= 0 {
		return fieldError("fleet.min_workers", "must be greater than 0")
	}
	if cfg.Fleet.MaxWorkers <= 0 {
		return fieldError("fleet.max_workers", "must be greater than 0")
	}
	if cfg.Fleet.MaxWorkers < cfg.Fleet.MinWorkers {
		return fieldError("fleet.max_workers", "must be greater than or equal to fleet.min_workers")
	}
	if cfg.Thresholds.P99Latency < 0 {
		return fieldError("thresholds.p99_latency", "must be greater than or equal to 0")
	}
	if cfg.Thresholds.ErrorRate < 0 || cfg.Thresholds.ErrorRate > 1 {
		return fieldError("thresholds.error_rate", "must be between 0 and 1")
	}
	return nil
}

func validHTTPMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return true
	default:
		return false
	}
}

func fieldError(field, message string) error {
	return fmt.Errorf("%s: %s", field, message)
}
