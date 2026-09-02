package eks_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestEKSClusterTemplateUsesApprovedFixedTopology(t *testing.T) {
	cfg := renderClusterTemplate(t, map[string]string{
		"__CLUSTER_NAME__":      "load-tester-benchmark-abc123",
		"__AZ_A__":              "us-west-2a",
		"__AZ_B__":              "us-west-2b",
		"__PUBLIC_API_CIDR__":   "203.0.113.10/32",
		"__RUN_ID__":            "run-20260829-abc123",
		"__WORKFLOW_IDENTITY__": "eks-benchmark",
	})

	assertClusterString(t, cfg, "eksctl.io/v1alpha5", "apiVersion")
	assertClusterString(t, cfg, "ClusterConfig", "kind")
	assertClusterString(t, cfg, "us-west-2", "metadata", "region")
	assertClusterString(t, cfg, "run-20260829-abc123", "metadata", "tags", "load-tester.io/run-id")
	assertClusterString(t, cfg, "eks-benchmark", "metadata", "tags", "load-tester.io/workflow-identity")
	assertClusterStrings(t, cfg, []string{"us-west-2a", "us-west-2b"}, "availabilityZones")
	assertClusterString(t, cfg, "Single", "vpc", "nat", "gateway")
	assertClusterBool(t, cfg, true, "vpc", "clusterEndpoints", "publicAccess")
	assertClusterBool(t, cfg, true, "vpc", "clusterEndpoints", "privateAccess")
	assertClusterStrings(t, cfg, []string{"203.0.113.10/32"}, "vpc", "publicAccessCIDRs")

	groups, ok := clusterValue(cfg, "managedNodeGroups").([]any)
	if !ok || len(groups) != 2 {
		t.Fatalf("managedNodeGroups = %#v, want exactly two groups", groups)
	}
	assertNodeGroup(t, groups, "loadgen", "c7i.large", "load-tester.io/pool", "loadgen")
	assertNodeGroup(t, groups, "platform", "m7i.large", "load-tester.io/pool", "platform")
}

func TestEKSClusterOwnershipPlaceholdersRenderAsStrings(t *testing.T) {
	cfg := renderClusterTemplate(t, map[string]string{
		"__CLUSTER_NAME__":      "load-tester-benchmark-abc123",
		"__AZ_A__":              "us-west-2a",
		"__AZ_B__":              "us-west-2b",
		"__PUBLIC_API_CIDR__":   "203.0.113.10/32",
		"__RUN_ID__":            "123456789012",
		"__WORKFLOW_IDENTITY__": "true",
	})

	assertClusterString(t, cfg, "123456789012", "metadata", "tags", "load-tester.io/run-id")
	assertClusterString(t, cfg, "true", "metadata", "tags", "load-tester.io/workflow-identity")
}

func TestEKSClusterConfigRejectsUnsafeTopology(t *testing.T) {
	valid := renderClusterTemplate(t, map[string]string{
		"__CLUSTER_NAME__":      "load-tester-benchmark-abc123",
		"__AZ_A__":              "us-west-2a",
		"__AZ_B__":              "us-west-2b",
		"__PUBLIC_API_CIDR__":   "203.0.113.10/32",
		"__RUN_ID__":            "run-20260829-abc123",
		"__WORKFLOW_IDENTITY__": "eks-benchmark",
	})
	for name, mutate := range map[string]func(map[string]any){
		"unrestricted API": func(cfg map[string]any) { cfg["vpc"].(map[string]any)["publicAccessCIDRs"] = []any{"0.0.0.0/0"} },
		"mixed instances": func(cfg map[string]any) {
			cfg["managedNodeGroups"].([]any)[1].(map[string]any)["instanceType"] = "c7i.large"
		},
		"spot capacity": func(cfg map[string]any) {
			cfg["managedNodeGroups"].([]any)[0].(map[string]any)["spot"] = true
		},
		"elastic nodes": func(cfg map[string]any) {
			cfg["managedNodeGroups"].([]any)[0].(map[string]any)["maxSize"] = 3
		},
		"missing run ownership": func(cfg map[string]any) {
			tags, _ := cfg["metadata"].(map[string]any)["tags"].(map[string]any)
			delete(tags, "load-tester.io/run-id")
		},
		"missing workflow ownership": func(cfg map[string]any) {
			tags, _ := cfg["metadata"].(map[string]any)["tags"].(map[string]any)
			delete(tags, "load-tester.io/workflow-identity")
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := deepCopyClusterConfig(t, valid)
			mutate(cfg)
			if err := validateClusterConfig(cfg); err == nil {
				t.Fatal("validateClusterConfig() succeeded for unsafe topology")
			}
		})
	}
}

func renderClusterTemplate(t *testing.T, values map[string]string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("cluster.yaml.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(raw)
	for token, value := range values {
		rendered = strings.ReplaceAll(rendered, token, value)
	}
	if strings.Contains(rendered, "__") {
		t.Fatalf("template contains an unresolved token: %s", rendered)
	}
	var cfg map[string]any
	if err := yaml.Unmarshal([]byte(rendered), &cfg); err != nil {
		t.Fatalf("parse rendered template: %v", err)
	}
	return cfg
}

func validateClusterConfig(cfg map[string]any) error {
	if strings.Contains(fmt.Sprint(cfg), "0.0.0.0/0") {
		return fmt.Errorf("unrestricted public API access")
	}
	tags, ok := clusterValue(cfg, "metadata", "tags").(map[string]any)
	runID, runIDOK := tags["load-tester.io/run-id"].(string)
	workflowIdentity, workflowIdentityOK := tags["load-tester.io/workflow-identity"].(string)
	if !ok || !runIDOK || runID == "" || !workflowIdentityOK || workflowIdentity == "" {
		return fmt.Errorf("missing immutable ownership metadata")
	}
	groups, ok := clusterValue(cfg, "managedNodeGroups").([]any)
	if !ok || len(groups) != 2 {
		return fmt.Errorf("must contain exactly two node groups")
	}
	seen := map[string]string{}
	for _, raw := range groups {
		group, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid node group")
		}
		name, _ := group["name"].(string)
		instance, _ := group["instanceType"].(string)
		if _, hasCapacityType := group["capacityType"]; hasCapacityType || group["spot"] == true || group["privateNetworking"] != true || group["minSize"] != 2 || group["desiredCapacity"] != 2 || group["maxSize"] != 2 {
			return fmt.Errorf("node group %q is not fixed private On-Demand capacity", name)
		}
		seen[name] = instance
	}
	if seen["loadgen"] != "c7i.large" || seen["platform"] != "m7i.large" {
		return fmt.Errorf("node group types = %#v, want loadgen c7i.large and platform m7i.large", seen)
	}
	return nil
}

func deepCopyClusterConfig(t *testing.T, cfg map[string]any) map[string]any {
	t.Helper()
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var copied map[string]any
	if err := yaml.Unmarshal(raw, &copied); err != nil {
		t.Fatal(err)
	}
	return copied
}

func assertNodeGroup(t *testing.T, groups []any, name, instance, label, labelValue string) {
	t.Helper()
	for _, raw := range groups {
		group, _ := raw.(map[string]any)
		if group["name"] != name {
			continue
		}
		_, hasCapacityType := group["capacityType"]
		if group["instanceType"] != instance || hasCapacityType || group["spot"] == true || group["privateNetworking"] != true || group["minSize"] != 2 || group["desiredCapacity"] != 2 || group["maxSize"] != 2 || group["ssh"] != nil {
			t.Fatalf("node group %q = %#v, want fixed private %s On-Demand group without SSH", name, group, instance)
		}
		labels, _ := group["labels"].(map[string]any)
		if labels[label] != labelValue {
			t.Fatalf("node group %q labels = %#v", name, labels)
		}
		return
	}
	t.Fatalf("missing node group %q", name)
}

func assertClusterString(t *testing.T, cfg map[string]any, want string, path ...string) {
	t.Helper()
	if got := clusterValue(cfg, path...); got != want {
		t.Errorf("%s = %#v, want %q", strings.Join(path, "."), got, want)
	}
}
func assertClusterBool(t *testing.T, cfg map[string]any, want bool, path ...string) {
	t.Helper()
	if got := clusterValue(cfg, path...); got != want {
		t.Errorf("%s = %#v, want %t", strings.Join(path, "."), got, want)
	}
}
func assertClusterStrings(t *testing.T, cfg map[string]any, want []string, path ...string) {
	t.Helper()
	got, ok := clusterValue(cfg, path...).([]any)
	if !ok || len(got) != len(want) {
		t.Fatalf("%s = %#v, want %#v", strings.Join(path, "."), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %#v, want %q", strings.Join(path, "."), i, got[i], want[i])
		}
	}
}
func clusterValue(cfg map[string]any, path ...string) any {
	var value any = cfg
	for _, key := range path {
		typed, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = typed[key]
	}
	return value
}
