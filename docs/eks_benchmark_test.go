package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEKSBenchmarkDocumentationContract(t *testing.T) {
	runbook := normalizeDocumentation(readDocumentationFile(t, "eks-benchmark.md"))
	design := normalizeDocumentation(readDocumentationFile(t, "design.md"))
	roadmap := normalizeDocumentation(readDocumentationFile(t, "roadmap.md"))

	for _, required := range []string{
		"AWS_PROFILE=load-tester-eks",
		"AWS_REGION=us-west-2",
		"EKS_CLUSTER_NAME=load-tester-benchmark-<unique-suffix>",
		"ALLOW_AWS_BILLING=1",
		"./scripts/eks-benchmark.sh preflight",
		"./scripts/eks-benchmark.sh render",
		"./scripts/eks-benchmark.sh ecr-push",
		"./scripts/eks-benchmark.sh run",
		"./scripts/eks-benchmark.sh teardown",
		"./scripts/eks-benchmark.sh verify-cleanup",
		"./scripts/eks-benchmark.sh recover-teardown --acknowledge-recovery",
		"./scripts/eks-benchmark.sh acknowledge-billing --checked-on YYYY-MM-DD",
		"read-only AWS checks",
		"Stage 2 billable operations",
		"root MFA",
		"IAM Identity Center",
		"$10",
		"not hard caps",
		"90-minute",
		"private nodes",
		"one NAT gateway",
		"LoadBalancer",
		"dashboard-review.json",
		"SHA-256",
		"artifacts/eks/<run-id>/",
		"docs/benchmarks/eks/<run-id>/",
		"laptop or network loss",
		"do not require the billing acknowledgement",
		"workflow-identity",
		"AWS is unreachable",
		"account IDs",
		"credentials",
	} {
		if !strings.Contains(runbook, required) {
			t.Errorf("runbook is missing %q", required)
		}
	}

	for _, required := range []string{
		"calibration-first",
		"Worker-fleet experiment",
		"Target-HPA experiment",
		"c7i.large",
		"m7i.large",
		"one NAT gateway",
		"node autoscaling",
		"fail closed",
		"ownership tags",
		"raw artifacts",
		"sanitized evidence",
	} {
		if !strings.Contains(design, required) {
			t.Errorf("design is missing Piece 23 decision %q", required)
		}
	}

	for _, required := range []string{
		"Stage 1",
		"Stage 2",
		"measured EKS run",
		"next-day billing",
		"incomplete",
		"Piece 24B",
	} {
		if !strings.Contains(roadmap, required) {
			t.Errorf("roadmap is missing Piece 23 status %q", required)
		}
	}

	for _, required := range []string{
		"https://docs.aws.amazon.com/IAM/latest/UserGuide/root-user-best-practices.html",
		"https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html",
		"https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-best-practices.html",
		"https://docs.aws.amazon.com/eks/latest/eksctl/creating-and-managing-clusters.html",
	} {
		if !strings.Contains(runbook, required) {
			t.Errorf("runbook is missing AWS documentation link %q", required)
		}
	}
}

func normalizeDocumentation(contents string) string {
	return strings.Join(strings.Fields(contents), " ")
}

func readDocumentationFile(t *testing.T, name string) string {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join(".", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}
