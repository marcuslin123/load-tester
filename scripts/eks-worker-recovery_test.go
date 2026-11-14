package scripts_test

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

func TestEKSWorkerRecoveryReconcilesFinalMissedArrivals(t *testing.T) {
	const log = `autoscale evaluation: expected=100 admitted=75 unmet=0 coverage=0.7500 complete=true deficits=0 desired=2 registered=1 scale=true reason="scheduler coverage requires scale up"
autoscale evaluation: expected=100 admitted=100 unmet=0 coverage=1.0000 complete=true deficits=0 desired=2 registered=2 scale=false reason="scheduler coverage meets threshold"
Test: eks-worker-autoscale
Result: FAIL
Failed: 0
Unmet demand: TOTAL
Dropped samples: 0
Violations:
- unmet demand: TOTAL
`
	for _, tc := range []struct {
		total string
		post  float64
	}{{"25", 0}, {"26", 1}} {
		cmd := exec.Command("python3", "eks-worker-recovery.py")
		cmd.Stdin = strings.NewReader(strings.ReplaceAll(log, "TOTAL", tc.total))
		output, err := cmd.CombinedOutput()
		var evidence map[string]float64
		if err != nil || json.Unmarshal(output, &evidence) != nil {
			t.Fatalf("total %s: %v: %s", tc.total, err, output)
		}
		if evidence["missed_arrivals_during_scale_up"] != 25 || evidence["unmet_demand"] != tc.post || evidence["recovered"] != 1 {
			t.Fatalf("total %s: unexpected evidence %s", tc.total, output)
		}
	}
}
