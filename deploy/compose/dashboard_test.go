package compose

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const dashboardPath = "grafana/dashboards/load-tester-overview.json"

type dashboard struct {
	UID        string          `json:"uid"`
	Title      string          `json:"title"`
	Editable   bool            `json:"editable"`
	Refresh    string          `json:"refresh"`
	Datasource dashboardSource `json:"datasource"`
	Panels     []panel         `json:"panels"`
}

type dashboardSource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type panel struct {
	Title       string          `json:"title"`
	Type        string          `json:"type"`
	Datasource  dashboardSource `json:"datasource"`
	Targets     []target        `json:"targets"`
	FieldConfig fieldConfig     `json:"fieldConfig"`
	Options     panelOptions    `json:"options"`
}

type target struct {
	Expr         string `json:"expr"`
	LegendFormat string `json:"legendFormat"`
}

type fieldConfig struct {
	Defaults struct {
		Unit       string      `json:"unit"`
		Thresholds thresholds  `json:"thresholds"`
		Custom     customStyle `json:"custom"`
	} `json:"defaults"`
}

type thresholds struct {
	Steps []thresholdStep `json:"steps"`
}

type thresholdStep struct {
	Value *float64 `json:"value"`
}

type customStyle struct {
	Stacking struct {
		Mode string `json:"mode"`
	} `json:"stacking"`
}

type panelOptions struct {
	Legend struct {
		ShowLegend bool `json:"showLegend"`
	} `json:"legend"`
}

func TestDashboardDefinesRequiredPanelsAndQueries(t *testing.T) {
	dashboard := readDashboard(t)

	if dashboard.UID != "load-tester-overview" {
		t.Fatalf("dashboard UID = %q, want load-tester-overview", dashboard.UID)
	}
	if dashboard.Title != "Load Tester Overview" {
		t.Fatalf("dashboard title = %q, want Load Tester Overview", dashboard.Title)
	}
	if dashboard.Editable {
		t.Fatal("provisioned dashboard must not be editable")
	}
	if dashboard.Refresh != "1s" {
		t.Fatalf("dashboard refresh = %q, want 1s", dashboard.Refresh)
	}

	requiredQueries := map[string][]string{
		"Current RPS":              {`rate(loadtester_requests_total[$__rate_interval])`},
		"Current Error Rate":       {`sum(rate(loadtester_request_failures_total[$__rate_interval]))`, `clamp_min(sum(rate(loadtester_requests_total[$__rate_interval]))`, `, 1e-9)`},
		"Active Workers":           {`loadtester_active_workers`, `loadtester_desired_workers`},
		"Dropped Samples":          {`loadtester_dropped_samples_total`},
		"Unmet Demand":             {`loadtester_unmet_demand_total`},
		"Requests per Second":      {`rate(loadtester_requests_total[$__rate_interval])`},
		"Latency Percentiles":      {`histogram_quantile(0.50`, `histogram_quantile(0.95`, `histogram_quantile(0.99`, `loadtester_request_duration_seconds_bucket`},
		"Error Rate by Kind":       {`sum(rate(loadtester_request_failures_total[$__rate_interval]))`, `sum by (kind)`, `ignoring(kind) group_left`, `clamp_min(sum(rate(loadtester_requests_total[$__rate_interval]))`, `, 1e-9)`},
		"HTTP Status Codes":        {`sum by (code)`, `rate(loadtester_http_responses_total[$__rate_interval])`},
		"Per-worker RPS":           {`sum by (worker_id)`, `rate(loadtester_worker_requests_total[$__rate_interval])`},
		"Active Workers Over Time": {`loadtester_active_workers`},
		"Scheduler Coverage":       {`loadtester_autoscaler_scheduler_coverage_ratio`},
	}

	panels := make(map[string]panel, len(dashboard.Panels))
	for _, panel := range dashboard.Panels {
		panels[panel.Title] = panel
		if panel.Datasource.UID != "prometheus" {
			t.Errorf("panel %q datasource UID = %q, want prometheus", panel.Title, panel.Datasource.UID)
		}
	}

	for title, fragments := range requiredQueries {
		panel, exists := panels[title]
		if !exists {
			t.Errorf("missing required panel %q", title)
			continue
		}
		expressions := panelExpressions(panel)
		for _, fragment := range fragments {
			if !strings.Contains(expressions, fragment) {
				t.Errorf("panel %q queries do not contain %q: %s", title, fragment, expressions)
			}
		}
	}

	assertPanelUnit(t, panels["Current Error Rate"], "percentunit")
	assertPanelUnit(t, panels["Latency Percentiles"], "s")
	assertPanelUnit(t, panels["Error Rate by Kind"], "percentunit")
	assertPanelUnit(t, panels["Scheduler Coverage"], "percentunit")
	assertStacked(t, panels["HTTP Status Codes"])
	assertStacked(t, panels["Per-worker RPS"])
	assertLatencyThreshold(t, panels["Latency Percentiles"], 0.1)
	assertThreshold(t, panels["Scheduler Coverage"], 0.90)
}

func TestPrometheusDatasourceMatchesScrapeInterval(t *testing.T) {
	contents, err := os.ReadFile("grafana/provisioning/datasources/prometheus.yml")
	if err != nil {
		t.Fatalf("read datasource provisioning: %v", err)
	}

	var config struct {
		Datasources []struct {
			UID      string `yaml:"uid"`
			JSONData struct {
				TimeInterval string `yaml:"timeInterval"`
			} `yaml:"jsonData"`
		} `yaml:"datasources"`
	}
	if err := yaml.Unmarshal(contents, &config); err != nil {
		t.Fatalf("parse datasource provisioning: %v", err)
	}
	if len(config.Datasources) != 1 {
		t.Fatalf("datasource count = %d, want 1", len(config.Datasources))
	}
	if source := config.Datasources[0]; source.UID != "prometheus" || source.JSONData.TimeInterval != "1s" {
		t.Fatalf("datasource UID/interval = %q/%q, want prometheus/1s", source.UID, source.JSONData.TimeInterval)
	}
}

func TestDashboardProvisioningLoadsCommittedDashboard(t *testing.T) {
	contents, err := os.ReadFile("grafana/provisioning/dashboards/dashboards.yml")
	if err != nil {
		t.Fatalf("read dashboard provisioning: %v", err)
	}

	var config struct {
		APIVersion int `yaml:"apiVersion"`
		Providers  []struct {
			Name            string `yaml:"name"`
			Type            string `yaml:"type"`
			DisableDeletion bool   `yaml:"disableDeletion"`
			Editable        bool   `yaml:"editable"`
			Options         struct {
				Path string `yaml:"path"`
			} `yaml:"options"`
		} `yaml:"providers"`
	}
	if err := yaml.Unmarshal(contents, &config); err != nil {
		t.Fatalf("parse dashboard provisioning: %v", err)
	}
	if config.APIVersion != 1 || len(config.Providers) != 1 {
		t.Fatalf("dashboard provisioning = apiVersion %d with %d providers, want 1 and 1", config.APIVersion, len(config.Providers))
	}

	provider := config.Providers[0]
	if provider.Name != "Load Tester" || provider.Type != "file" {
		t.Fatalf("provider = %q/%q, want Load Tester/file", provider.Name, provider.Type)
	}
	if !provider.DisableDeletion || provider.Editable {
		t.Fatal("dashboard provider must disable deletion and editing")
	}
	if provider.Options.Path != "/var/lib/grafana/dashboards" {
		t.Fatalf("dashboard provider path = %q, want /var/lib/grafana/dashboards", provider.Options.Path)
	}
}

func readDashboard(t *testing.T) dashboard {
	t.Helper()

	contents, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	var value dashboard
	if err := json.Unmarshal(contents, &value); err != nil {
		t.Fatalf("parse dashboard: %v", err)
	}
	return value
}

func panelExpressions(panel panel) string {
	expressions := make([]string, 0, len(panel.Targets))
	for _, target := range panel.Targets {
		expressions = append(expressions, target.Expr)
	}
	return strings.Join(expressions, "\n")
}

func assertPanelUnit(t *testing.T, panel panel, want string) {
	t.Helper()
	if panel.FieldConfig.Defaults.Unit != want {
		t.Errorf("panel %q unit = %q, want %q", panel.Title, panel.FieldConfig.Defaults.Unit, want)
	}
}

func assertStacked(t *testing.T, panel panel) {
	t.Helper()
	if panel.FieldConfig.Defaults.Custom.Stacking.Mode != "normal" {
		t.Errorf("panel %q stacking = %q, want normal", panel.Title, panel.FieldConfig.Defaults.Custom.Stacking.Mode)
	}
}

func assertLatencyThreshold(t *testing.T, panel panel, want float64) {
	t.Helper()
	assertThreshold(t, panel, want)
}

func assertThreshold(t *testing.T, panel panel, want float64) {
	t.Helper()
	for _, step := range panel.FieldConfig.Defaults.Thresholds.Steps {
		if step.Value != nil && *step.Value == want {
			return
		}
	}
	t.Errorf("panel %q does not define threshold %v", panel.Title, want)
}
