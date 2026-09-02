package scripts_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureGitSHA = "0123456789abcdef0123456789abcdef01234567"

func TestEKSRedactPublishesSanitizedEvidenceBundle(t *testing.T) {
	input := writeRedactionFixture(t, redactionFixtureOptions{})
	output := filepath.Join(t.TempDir(), "curated")

	result := runRedactor(t, input, output)
	if result.err != nil {
		t.Fatalf("redactor failed: %v\n%s", result.err, result.output)
	}

	metricsBytes := readFile(t, filepath.Join(output, "metrics.json"))
	report := string(readFile(t, filepath.Join(output, "report.md")))
	if !bytes.Equal(readFile(t, filepath.Join(input, "dashboard.png")), readFile(t, filepath.Join(output, "dashboard.png"))) {
		t.Fatal("approved dashboard.png changed during publication")
	}

	var metrics map[string]any
	if err := json.Unmarshal(metricsBytes, &metrics); err != nil {
		t.Fatalf("metrics.json is not JSON: %v", err)
	}
	assertRequiredEvidenceFields(t, metrics)
	assertJSONKeysSorted(t, string(metricsBytes), []string{"billing_attestation", "git_sha", "region", "run_id", "target_hpa_experiment", "teardown", "topology", "worker_experiment"})

	for _, want := range []string{
		"<aws-account-id>",
		"<aws-arn>",
		"<ip-address>",
		"<email-address>",
		"<redacted-credential>",
	} {
		if !strings.Contains(string(metricsBytes), want) {
			t.Errorf("metrics.json does not contain redaction marker %q", want)
		}
	}
	for _, want := range []string{
		fixtureGitSHA,
		"us-west-2",
		"c7i.large",
		"m7i.large",
		"142.5",
		"2.1",
		"5.2",
		"9.8",
		"123456789012",
		"dropped_samples",
		"unmet_demand",
	} {
		if !strings.Contains(string(metricsBytes), want) {
			t.Errorf("metrics.json lost curated benchmark value %q", want)
		}
	}
	for _, want := range []string{
		"Git SHA: " + fixtureGitSHA,
		"Region: us-west-2",
		"c7i.large",
		"m7i.large",
		"Peak RPS: 142.5",
		"p50=2.1ms",
		"p95=5.2ms",
		"p99=9.8ms",
		"Billing attestation: human",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report.md lost curated benchmark value %q", want)
		}
	}

	secrets := []string{
		"123456789012",
		"arn:aws:eks:us-west-2:123456789012:cluster/example",
		"198.51.100.20",
		"10.0.12.7",
		"marcus@example.com",
		"AKIAIOSFODNN7EXAMPLE",
		"aws_access_key_id",
		"aws_secret_access_key",
		"aws_session_token",
		"super-secret-value",
		"session-token-value",
		"header.payload.signature",
		"2001:db8::1",
	}
	assertNoSensitiveStrings(t, metrics, secrets, "metrics.json")
	for _, secret := range secrets {
		if strings.Contains(report, secret) {
			t.Errorf("sensitive value survived redaction in report.md: %q", secret)
		}
	}
}

func TestEKSRedactIsDeterministicForRecursiveJSONAndMixedText(t *testing.T) {
	firstInput := writeRedactionFixture(t, redactionFixtureOptions{
		MixedText: "owner=marcus@example.com arn=arn:aws:iam::123456789012:role/example ip=10.0.12.7 key=AKIAIOSFODNN7EXAMPLE",
	})
	secondInput := writeRedactionFixture(t, redactionFixtureOptions{
		MixedText: "owner=marcus@example.com arn=arn:aws:iam::123456789012:role/example ip=10.0.12.7 key=AKIAIOSFODNN7EXAMPLE",
	})
	firstOutput := filepath.Join(t.TempDir(), "curated")
	secondOutput := filepath.Join(t.TempDir(), "curated")

	if result := runRedactor(t, firstInput, firstOutput); result.err != nil {
		t.Fatalf("first redaction failed: %v\n%s", result.err, result.output)
	}
	if result := runRedactor(t, secondInput, secondOutput); result.err != nil {
		t.Fatalf("second redaction failed: %v\n%s", result.err, result.output)
	}

	firstMetrics := readFile(t, filepath.Join(firstOutput, "metrics.json"))
	secondMetrics := readFile(t, filepath.Join(secondOutput, "metrics.json"))
	if !bytes.Equal(firstMetrics, secondMetrics) {
		t.Fatalf("metrics.json is nondeterministic:\nfirst=%s\nsecond=%s", firstMetrics, secondMetrics)
	}
	firstReport := readFile(t, filepath.Join(firstOutput, "report.md"))
	secondReport := readFile(t, filepath.Join(secondOutput, "report.md"))
	if !bytes.Equal(firstReport, secondReport) {
		t.Fatalf("report.md is nondeterministic:\nfirst=%s\nsecond=%s", firstReport, secondReport)
	}
	for _, marker := range []string{"<aws-account-id>", "<aws-arn>", "<ip-address>", "<email-address>", "<redacted-credential>"} {
		if !bytes.Contains(firstMetrics, []byte(marker)) {
			t.Errorf("recursive mixed text is missing marker %q", marker)
		}
	}
}

func TestEKSRedactPreservesFullCommitSHA(t *testing.T) {
	commitSHA := "1234567890123456789012345678901234567890"
	input := writeRedactionFixture(t, redactionFixtureOptions{GitSHA: commitSHA})
	output := filepath.Join(t.TempDir(), "curated")

	if result := runRedactor(t, input, output); result.err != nil {
		t.Fatalf("redaction failed: %v\n%s", result.err, result.output)
	}
	metrics := string(readFile(t, filepath.Join(output, "metrics.json")))
	report := string(readFile(t, filepath.Join(output, "report.md")))
	if !strings.Contains(metrics, commitSHA) || !strings.Contains(report, "Git SHA: "+commitSHA) {
		t.Fatalf("full commit SHA was over-redacted")
	}
}

func TestEKSRedactRedactsBearerTokensAndIPv6Addresses(t *testing.T) {
	input := writeRedactionFixture(t, redactionFixtureOptions{
		MixedText: "Authorization: Bearer header.payload.signature source=2001:db8::1",
	})
	output := filepath.Join(t.TempDir(), "curated")

	result := runRedactor(t, input, output)
	if result.err != nil {
		t.Fatalf("redactor failed: %v\n%s", result.err, result.output)
	}
	metrics := string(readFile(t, filepath.Join(output, "metrics.json")))
	if strings.Contains(metrics, "header.payload.signature") || strings.Contains(metrics, "2001:db8::1") {
		t.Fatalf("bearer token or IPv6 address survived publication: %s", metrics)
	}
	for _, marker := range []string{"<redacted-credential>", "<ip-address>"} {
		if !strings.Contains(metrics, marker) {
			t.Errorf("metrics.json is missing %q", marker)
		}
	}
}

func TestEKSRedactRejectsInvalidEvidenceBeforePublishing(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name:   "missing required field",
			mutate: func(metrics map[string]any) { delete(metrics, "billing_attestation") },
		},
		{
			name: "dropped samples",
			mutate: func(metrics map[string]any) {
				metrics["worker_experiment"].(map[string]any)["measured"].(map[string]any)["dropped_samples"] = float64(1)
			},
		},
		{
			name: "unmet demand",
			mutate: func(metrics map[string]any) {
				metrics["worker_experiment"].(map[string]any)["measured"].(map[string]any)["unmet_demand"] = float64(1)
			},
		},
		{
			name: "target dropped samples",
			mutate: func(metrics map[string]any) {
				metrics["target_hpa_experiment"].(map[string]any)["measured"].(map[string]any)["dropped_samples"] = float64(1)
			},
		},
		{
			name: "worker request errors",
			mutate: func(metrics map[string]any) {
				metrics["worker_experiment"].(map[string]any)["measured"].(map[string]any)["error_rate"] = 0.01
			},
		},
		{
			name:   "empty topology",
			mutate: func(metrics map[string]any) { metrics["topology"] = map[string]any{} },
		},
		{
			name: "missing worker percentile",
			mutate: func(metrics map[string]any) {
				delete(metrics["worker_experiment"].(map[string]any)["measured"].(map[string]any), "p99_ms")
			},
		},
		{
			name: "zero target peak workers",
			mutate: func(metrics map[string]any) {
				metrics["target_hpa_experiment"].(map[string]any)["measured"].(map[string]any)["peak_workers"] = 0
			},
		},
		{
			name: "empty target measurements",
			mutate: func(metrics map[string]any) {
				metrics["target_hpa_experiment"].(map[string]any)["measured"] = map[string]any{}
			},
		},
		{
			name: "failed teardown verification",
			mutate: func(metrics map[string]any) {
				metrics["teardown"].(map[string]any)["verified_clean"] = false
			},
		},
		{
			name: "same day billing attestation",
			mutate: func(metrics map[string]any) {
				metrics["billing_attestation"].(map[string]any)["checked_on"] = "2026-08-28"
			},
		},
		{
			name:   "empty billing attestation",
			mutate: func(metrics map[string]any) { metrics["billing_attestation"] = map[string]any{} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := writeRedactionFixture(t, redactionFixtureOptions{Mutate: tt.mutate})
			output := filepath.Join(t.TempDir(), "curated")
			result := runRedactor(t, input, output)
			if result.err == nil {
				t.Fatalf("redactor unexpectedly succeeded:\n%s", result.output)
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("invalid evidence created output directory: %v", err)
			}
		})
	}
}

func TestEKSRedactRejectsEvidenceThatDoesNotMatchItsOwnedRun(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, input string)
	}{
		{
			name: "metrics run ID",
			mutate: func(t *testing.T, input string) {
				metrics := readJSONFixture(t, filepath.Join(input, "metrics.json"))
				metrics["run_id"] = "20260828T120000Z-deadbee"
				writeJSON(t, filepath.Join(input, "metrics.json"), metrics)
			},
		},
		{
			name: "metrics Git SHA",
			mutate: func(t *testing.T, input string) {
				metrics := readJSONFixture(t, filepath.Join(input, "metrics.json"))
				metrics["git_sha"] = strings.Repeat("a", 40)
				writeJSON(t, filepath.Join(input, "metrics.json"), metrics)
			},
		},
		{
			name: "metrics region",
			mutate: func(t *testing.T, input string) {
				metrics := readJSONFixture(t, filepath.Join(input, "metrics.json"))
				metrics["region"] = "us-east-1"
				writeJSON(t, filepath.Join(input, "metrics.json"), metrics)
			},
		},
		{
			name: "cleanup identity",
			mutate: func(t *testing.T, input string) {
				cleanup := readJSONFixture(t, filepath.Join(input, "cleanup-report.json"))
				cleanup["cluster"] = "other-cluster"
				writeJSON(t, filepath.Join(input, "cleanup-report.json"), cleanup)
			},
		},
		{
			name: "owned run ID",
			mutate: func(t *testing.T, input string) {
				owned := readJSONFixture(t, filepath.Join(input, "owned-run.json"))
				owned["run_id"] = "20260828T120000Z-deadbee"
				writeJSON(t, filepath.Join(input, "owned-run.json"), owned)
			},
		},
		{
			name: "owned Git SHA",
			mutate: func(t *testing.T, input string) {
				owned := readJSONFixture(t, filepath.Join(input, "owned-run.json"))
				owned["git_sha"] = strings.Repeat("a", 40)
				writeJSON(t, filepath.Join(input, "owned-run.json"), owned)
			},
		},
		{
			name: "owned region",
			mutate: func(t *testing.T, input string) {
				owned := readJSONFixture(t, filepath.Join(input, "owned-run.json"))
				owned["region"] = "us-east-1"
				writeJSON(t, filepath.Join(input, "owned-run.json"), owned)
			},
		},
		{
			name: "owned cleanup account",
			mutate: func(t *testing.T, input string) {
				owned := readJSONFixture(t, filepath.Join(input, "owned-run.json"))
				owned["account"] = "999999999999"
				writeJSON(t, filepath.Join(input, "owned-run.json"), owned)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := writeRedactionFixture(t, redactionFixtureOptions{})
			tt.mutate(t, input)
			result := runRedactorWithExpected(t, input, filepath.Join(t.TempDir(), "curated"), redactorExpectation{
				runID: "20260828T120000Z-0123456", gitSHA: fixtureGitSHA, region: "us-west-2",
				account: "123456789012", cluster: "load-tester-benchmark-abc123",
			})
			if result.err == nil {
				t.Fatalf("redactor published evidence with mismatched ownership: %s", result.output)
			}
		})
	}
}

func TestEKSRedactRedactsNumericAccountIDFieldsWithoutChangingMetricCounts(t *testing.T) {
	input := writeRedactionFixture(t, redactionFixtureOptions{
		Mutate: func(metrics map[string]any) {
			metrics["topology"].(map[string]any)["account_id"] = 123456789012
		},
	})
	output := filepath.Join(t.TempDir(), "curated")

	if result := runRedactor(t, input, output); result.err != nil {
		t.Fatalf("redactor failed: %v\n%s", result.err, result.output)
	}
	metrics := string(readFile(t, filepath.Join(output, "metrics.json")))
	if strings.Contains(metrics, "\"account_id\": 123456789012") {
		t.Fatalf("numeric account_id survived publication: %s", metrics)
	}
	if !strings.Contains(metrics, "\"request_count\": 123456789012") {
		t.Fatalf("legitimate 12-digit metric count was altered: %s", metrics)
	}
}

func TestEKSRedactRequiresHumanReviewedDashboardHash(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, input string)
	}{
		{
			name: "missing approval",
			mutate: func(t *testing.T, input string) {
				if err := os.Remove(filepath.Join(input, "dashboard-review.json")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "hash mismatch",
			mutate: func(t *testing.T, input string) {
				writeJSON(t, filepath.Join(input, "dashboard-review.json"), map[string]any{
					"approved":         true,
					"reviewed_by":      "human",
					"reviewed_on":      "2026-08-29",
					"dashboard_sha256": strings.Repeat("0", sha256.Size*2),
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := writeRedactionFixture(t, redactionFixtureOptions{})
			tt.mutate(t, input)
			output := filepath.Join(t.TempDir(), "curated")
			result := runRedactor(t, input, output)
			if result.err == nil {
				t.Fatalf("redactor unexpectedly published an unapproved dashboard: %s", result.output)
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("unapproved dashboard created output directory: %v", err)
			}
		})
	}
}

func TestEKSRedactRefusesToOverwriteCuratedDirectory(t *testing.T) {
	input := writeRedactionFixture(t, redactionFixtureOptions{})
	output := filepath.Join(t.TempDir(), "curated")
	if result := runRedactor(t, input, output); result.err != nil {
		t.Fatalf("initial redaction failed: %v\n%s", result.err, result.output)
	}
	metricsBefore := readFile(t, filepath.Join(output, "metrics.json"))

	result := runRedactor(t, input, output)
	if result.err == nil {
		t.Fatal("redactor unexpectedly overwrote an existing curated directory")
	}
	if !bytes.Equal(metricsBefore, readFile(t, filepath.Join(output, "metrics.json"))) {
		t.Fatal("existing curated metrics.json changed after refused overwrite")
	}
}

type redactionFixtureOptions struct {
	MixedText string
	Mutate    func(map[string]any)
	GitSHA    string
}

type redactorResult struct {
	err    error
	output string
}

func runRedactor(t *testing.T, input, output string) redactorResult {
	t.Helper()
	owned := readJSONFixture(t, filepath.Join(input, "owned-run.json"))
	return runRedactorWithExpected(t, input, output, redactorExpectation{
		runID: owned["run_id"].(string), gitSHA: owned["git_sha"].(string), region: owned["region"].(string),
		account: owned["account"].(string), cluster: owned["cluster"].(string),
	})
}

type redactorExpectation struct {
	runID, gitSHA, region, account, cluster string
}

func runRedactorWithExpected(t *testing.T, input, output string, expected redactorExpectation) redactorResult {
	t.Helper()
	command := exec.Command("python3", "eks-redact.py", "--input", input, "--output", output,
		"--expected-run-id", expected.runID,
		"--expected-git-sha", expected.gitSHA,
		"--expected-region", expected.region,
		"--expected-cleanup-account", expected.account,
		"--expected-cleanup-region", expected.region,
		"--expected-cleanup-cluster", expected.cluster)
	command.Dir = "."
	outputBytes, err := command.CombinedOutput()
	return redactorResult{err: err, output: string(outputBytes)}
}

func writeRedactionFixture(t *testing.T, options redactionFixtureOptions) string {
	t.Helper()
	input := filepath.Join(t.TempDir(), "20260828T120000Z-0123456")
	if err := os.MkdirAll(input, 0o755); err != nil {
		t.Fatal(err)
	}

	metrics := map[string]any{
		"run_id":  "20260828T120000Z-0123456",
		"git_sha": fixtureGitSHA,
		"region":  "us-west-2",
		"topology": map[string]any{
			"eks_version":        "1.34",
			"kubernetes_version": "1.34.8",
			"instance_types":     map[string]any{"loadgen": "c7i.large", "platform": "m7i.large"},
			"node_counts":        map[string]any{"loadgen": 2, "platform": 2},
			"zones":              []any{"us-west-2a", "us-west-2b"},
			"account_id":         "123456789012",
			"cluster_arn":        "arn:aws:eks:us-west-2:123456789012:cluster/example",
			"operator_ip":        "198.51.100.20",
			"private_ip":         "10.0.12.7",
			"operator_email":     "marcus@example.com",
		},
		"worker_experiment": map[string]any{
			"scenario": map[string]any{
				"target":        "/echo",
				"model":         "constant-rate",
				"rate":          150,
				"duration":      "3m",
				"ramp_up":       "30s",
				"max_in_flight": 1000,
			},
			"measured": map[string]any{
				"peak_rps":        142.5,
				"request_count":   123456789012,
				"peak_workers":    4,
				"duration":        "3m",
				"p50_ms":          2.1,
				"p95_ms":          5.2,
				"p99_ms":          9.8,
				"error_rate":      0.0,
				"dropped_samples": 0,
				"unmet_demand":    0,
			},
			"nested": []any{
				map[string]any{
					"text":                  "contact marcus@example.com at 10.0.12.7 using arn:aws:iam::123456789012:role/example",
					"aws_access_key_id":     "AKIAIOSFODNN7EXAMPLE",
					"aws_secret_access_key": "super-secret-value",
					"aws_session_token":     "session-token-value",
				},
			},
		},
		"target_hpa_experiment": map[string]any{
			"scenario": map[string]any{
				"target":   "/burn?ms=20",
				"model":    "constant-rate",
				"duration": "2m",
				"headers":  map[string]any{"Connection": "close"},
				"rates":    []any{30, 45, 67.5},
			},
			"measured": map[string]any{
				"peak_rps":        67.5,
				"peak_workers":    2,
				"target_replicas": 3,
				"duration":        "2m",
				"p50_ms":          21.0,
				"p95_ms":          35.0,
				"p99_ms":          48.0,
				"error_rate":      0.0,
				"dropped_samples": 0,
				"unmet_demand":    0,
			},
		},
		"teardown": map[string]any{
			"status":         "PASS",
			"verified_clean": true,
			"verified": map[string]any{
				"cluster_arn": "arn:aws:eks:us-west-2:123456789012:cluster/example",
				"public_ip":   "198.51.100.20",
			},
		},
		"billing_attestation": map[string]any{
			"type":       "human",
			"checked_on": "2026-08-29",
			"confirmed":  true,
			"statement":  "Billing Console reviewed by marcus@example.com",
		},
	}
	if options.GitSHA != "" {
		metrics["git_sha"] = options.GitSHA
	}
	if options.MixedText != "" {
		metrics["mixed_text"] = options.MixedText
	}
	if options.Mutate != nil {
		options.Mutate(metrics)
	}
	writeJSON(t, filepath.Join(input, "owned-run.json"), map[string]any{
		"schema_version": 1,
		"account":        "123456789012",
		"region":         "us-west-2",
		"cluster":        "load-tester-benchmark-abc123",
		"run_id":         metrics["run_id"],
		"git_sha":        metrics["git_sha"],
	})
	writeJSON(t, filepath.Join(input, "cleanup-report.json"), map[string]any{
		"status":  "PASS",
		"account": "123456789012",
		"region":  "us-west-2",
		"cluster": "load-tester-benchmark-abc123",
	})
	writeJSON(t, filepath.Join(input, "metrics.json"), metrics)
	writeJSON(t, filepath.Join(input, "report.json"), map[string]any{
		"title":   "EKS benchmark raw report",
		"notes":   "raw contact marcus@example.com from 198.51.100.20",
		"account": "123456789012",
	})
	if err := os.WriteFile(filepath.Join(input, "dashboard.png"), []byte("PNG\r\nfixture\x00bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	dashboardHash := sha256.Sum256(readFile(t, filepath.Join(input, "dashboard.png")))
	writeJSON(t, filepath.Join(input, "dashboard-review.json"), map[string]any{
		"approved":         true,
		"reviewed_by":      "human",
		"reviewed_on":      "2026-08-29",
		"dashboard_sha256": fmt.Sprintf("%x", dashboardHash),
	})
	return input
}

func assertRequiredEvidenceFields(t *testing.T, metrics map[string]any) {
	t.Helper()
	for _, field := range []string{
		"run_id",
		"git_sha",
		"region",
		"topology",
		"worker_experiment",
		"target_hpa_experiment",
		"teardown",
		"billing_attestation",
	} {
		if _, ok := metrics[field]; !ok {
			t.Errorf("metrics.json missing required field %q", field)
		}
	}
}

func assertJSONKeysSorted(t *testing.T, serialized string, keys []string) {
	t.Helper()
	previous := -1
	for _, key := range keys {
		current := strings.Index(serialized, fmt.Sprintf("\"%s\"", key))
		if current < 0 {
			t.Fatalf("serialized JSON is missing key %q", key)
		}
		if current < previous {
			t.Fatalf("serialized JSON has nondeterministic key order around %q", key)
		}
		previous = current
	}
}

func assertNoSensitiveStrings(t *testing.T, value any, secrets []string, path string) {
	t.Helper()
	switch current := value.(type) {
	case map[string]any:
		for key, child := range current {
			for _, secret := range secrets {
				if strings.Contains(key, secret) {
					t.Errorf("sensitive key survived redaction at %s: %q", path, key)
				}
			}
			assertNoSensitiveStrings(t, child, secrets, path+"."+key)
		}
	case []any:
		for index, child := range current {
			assertNoSensitiveStrings(t, child, secrets, fmt.Sprintf("%s[%d]", path, index))
		}
	case string:
		for _, secret := range secrets {
			if strings.Contains(current, secret) {
				t.Errorf("sensitive value survived redaction at %s: %q", path, secret)
			}
		}
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
