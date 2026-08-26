package loadtester_test

import (
	"bytes"
	"image/png"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
)

var markdownTarget = regexp.MustCompile(`!?\[[^]]*\]\(([^)]+)\)`)
var demoClaimPattern = regexp.MustCompile(`runs for ([a-z0-9]+) minutes at ([0-9]+) RPS with ([a-z0-9]+) workers`)

func TestReadmeContract(t *testing.T) {
	contents, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}

	for _, required := range []string{
		"docker compose up --build --detach",
		"docker compose logs --follow orchestrator",
		"docker compose down --volumes",
		"http://localhost:3000",
		"docs/images/load-tester-overview.png",
		"docs/design.md",
		"docs/roadmap.md",
		"```mermaid",
	} {
		if !bytes.Contains(contents, []byte(required)) {
			t.Errorf("README.md does not contain %q", required)
		}
	}

	for _, match := range markdownTarget.FindAllSubmatch(contents, -1) {
		target := strings.TrimSpace(string(match[1]))
		if fields := strings.Fields(target); len(fields) > 0 {
			target = strings.Trim(fields[0], "<>")
		}
		if target == "" || strings.HasPrefix(target, "#") ||
			strings.Contains(target, "://") {
			continue
		}
		target = strings.SplitN(target, "#", 2)[0]
		if _, err := os.Stat(filepath.FromSlash(target)); err != nil {
			t.Errorf("README local target %q: %v", target, err)
		}
	}

	image, err := os.Open("docs/images/load-tester-overview.png")
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	config, err := png.DecodeConfig(image)
	if err != nil {
		t.Fatalf("decode dashboard PNG: %v", err)
	}
	if config.Width < 1200 || config.Height < 650 {
		t.Fatalf("dashboard PNG is %dx%d, want at least 1200x650", config.Width, config.Height)
	}

	assertDemoClaims(t, contents)
}

// assertDemoClaims keeps the README's operational quickstart statement tied to the demo scenario.
func assertDemoClaims(t *testing.T, readme []byte) {
	t.Helper()

	demo, err := os.Open("deploy/compose/demo.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer demo.Close()

	demoConfig, err := config.Parse(demo)
	if err != nil {
		t.Fatalf("parse demo config: %v", err)
	}
	if demoConfig.Load.Duration <= 0 || demoConfig.Load.Duration%time.Minute != 0 {
		t.Fatalf("demo duration = %s, want a positive whole number of minutes", demoConfig.Load.Duration)
	}

	match := demoClaimPattern.FindSubmatch(readme)
	if match == nil {
		t.Fatalf("README is missing the demo claim matching %q", demoClaimPattern)
	}
	duration := parseClaimNumber(t, "duration", string(match[1]))
	rate := parseClaimNumber(t, "rate", string(match[2]))
	workers := parseClaimNumber(t, "workers", string(match[3]))

	wantDuration := int(demoConfig.Load.Duration / time.Minute)
	if duration != wantDuration {
		t.Errorf("README demo duration = %d minutes, want %d from deploy/compose/demo.yaml", duration, wantDuration)
	}
	if rate != demoConfig.Load.Rate {
		t.Errorf("README demo rate = %d RPS, want %d from deploy/compose/demo.yaml", rate, demoConfig.Load.Rate)
	}
	if workers != demoConfig.Fleet.MinWorkers || workers != demoConfig.Fleet.MaxWorkers {
		t.Errorf("README demo workers = %d, want %d from deploy/compose/demo.yaml", workers, demoConfig.Fleet.MinWorkers)
	}
}

func parseClaimNumber(t *testing.T, field, value string) int {
	t.Helper()
	if number, err := strconv.Atoi(value); err == nil {
		return number
	}
	words := map[string]int{
		"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
		"six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
	}
	if number, ok := words[strings.ToLower(value)]; ok {
		return number
	}
	t.Fatalf("README demo %s %q is not a supported number", field, value)
	return 0
}
