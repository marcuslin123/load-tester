#!/usr/bin/env bash
set -euo pipefail

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command is missing: %s\n' "$1" >&2
    exit 1
  fi
}

require_executable() {
  if [[ "$1" == */* ]]; then
    if [[ ! -x "$1" ]]; then
      printf 'Required executable is unavailable: %s\n' "$1" >&2
      exit 1
    fi
    return
  fi
  require_command "$1"
}

require_command curl
require_command docker
require_command kubectl
require_command python3

KIND_BIN=${KIND_BIN:-kind}
require_executable "$KIND_BIN"

if ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'Docker is installed but the daemon is unavailable' >&2
  exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
NAMESPACE=load-tester
CLUSTER_NAME=${KIND_CLUSTER_NAME:-load-tester-piece20-$$}
KUBE_CONTEXT="kind-$CLUSTER_NAME"
NODE_IMAGE=${KIND_NODE_IMAGE:-kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-180}
KEEP_KIND_CLUSTER=${KEEP_KIND_CLUSTER:-0}
ORCHESTRATOR_IMAGE=load-tester-orchestrator:kind
WORKER_IMAGE=load-tester-worker:kind
TARGET_IMAGE=load-tester-target:kind
TMP_DIR=$(mktemp -d)
PROMETHEUS_PORT_FORWARD_PID=''
GRAFANA_PORT_FORWARD_PID=''
CLUSTER_OWNED=0
CLEANUP_FAILED=0

kubectl_kind() {
  kubectl --context "$KUBE_CONTEXT" "$@"
}

cleanup_failure() {
  printf 'CLEANUP FAILED: %s\n' "$1" >&2
  CLEANUP_FAILED=1
}

diagnostics() {
  printf '%s\n' '--- Kubernetes diagnostics ---' >&2
  kubectl_kind get all --namespace "$NAMESPACE" -o wide >&2 || true
  kubectl_kind get events --namespace "$NAMESPACE" --sort-by=.lastTimestamp >&2 || true
  local orchestrator_pods
  orchestrator_pods=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector job-name=load-tester-orchestrator --no-headers 2>/dev/null || true)
  if [[ -n "$orchestrator_pods" ]]; then
    kubectl_kind logs --namespace "$NAMESPACE" job/load-tester-orchestrator --all-containers >&2 || true
  fi
}

cluster_exists() {
  "$KIND_BIN" get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"
}

cleanup() {
  local exit_status=$?
  trap - EXIT
  set +e

  if [[ -n "$PROMETHEUS_PORT_FORWARD_PID" ]]; then
    kill "$PROMETHEUS_PORT_FORWARD_PID" >/dev/null 2>&1 || true
    wait "$PROMETHEUS_PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$GRAFANA_PORT_FORWARD_PID" ]]; then
    kill "$GRAFANA_PORT_FORWARD_PID" >/dev/null 2>&1 || true
    wait "$GRAFANA_PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi

  if (( exit_status != 0 )) && (( CLUSTER_OWNED )) && cluster_exists; then
    diagnostics
  fi

  if (( CLUSTER_OWNED )); then
    if [[ "$KEEP_KIND_CLUSTER" == 1 ]]; then
      printf 'Cluster preserved for debugging: %s (context %s)\n' "$CLUSTER_NAME" "$KUBE_CONTEXT"
    else
      "$KIND_BIN" delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || cleanup_failure "could not delete kind cluster $CLUSTER_NAME"
      if "$KIND_BIN" get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"; then
        cleanup_failure "kind cluster $CLUSTER_NAME still exists"
      else
        printf 'Cleanup: PASS (workers=0 cluster=deleted)\n'
      fi
    fi
  fi
  rm -rf "$TMP_DIR" || cleanup_failure 'could not remove temporary files'

  if (( CLEANUP_FAILED )); then
    exit_status=1
  fi
  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

reserve_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_port_forward() {
  local url=$1
  local pid=$2
  local log_path=$3
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  until curl --silent --fail "$url" >/dev/null 2>&1; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      printf 'Port-forward exited before %s became ready:\n' "$url" >&2
      cat "$log_path" >&2 || true
      return 1
    fi
    if (( SECONDS >= deadline )); then
      printf 'Port-forwarded endpoint did not become ready: %s\n' "$url" >&2
      cat "$log_path" >&2 || true
      return 1
    fi
    sleep 1
  done
}

assert_can_i() {
  local want=$1
  shift
  local got
  got=$(kubectl_kind auth can-i "$@" --as system:serviceaccount:$NAMESPACE:load-tester-orchestrator 2>/dev/null || true)
  if [[ "$got" != "$want" ]]; then
    printf 'RBAC check failed: kubectl auth can-i %s = %q, want %q\n' "$*" "$got" "$want" >&2
    exit 1
  fi
}

prometheus_value() {
  local query=$1
  local response
  response=$(curl --silent --fail --get --data-urlencode "query=$query" "http://127.0.0.1:$PROMETHEUS_PORT/api/v1/query" 2>/dev/null || true)
  python3 -c 'import json,sys
try:
 data=json.loads(sys.argv[1]); result=data["data"]["result"]
 print(result[0]["value"][1] if result else "")
except Exception:
 print("")' "$response"
}

wait_for_worker_pod() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local pod_name=''
  until [[ -n "$pod_name" ]]; do
    pod_name=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector app.kubernetes.io/name=load-tester-worker -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if (( SECONDS >= deadline )); then
      printf '%s\n' 'Worker pod was not created before timeout' >&2
      return 1
    fi
    if [[ -z "$pod_name" ]]; then
      sleep 1
    fi
  done
  kubectl_kind wait --namespace "$NAMESPACE" --for=condition=Ready "pod/$pod_name" --timeout "${TIMEOUT_SECONDS}s" >/dev/null
  printf '%s\n' "$pod_name"
}

printf '%s\n' 'Building local Kubernetes images...'
docker build --quiet --target orchestrator --tag "$ORCHESTRATOR_IMAGE" "$REPO_ROOT" >/dev/null
docker build --quiet --target worker --tag "$WORKER_IMAGE" "$REPO_ROOT" >/dev/null
docker build --quiet --target target --tag "$TARGET_IMAGE" "$REPO_ROOT" >/dev/null

printf 'Creating kind cluster %s with Kubernetes 1.34.8...\n' "$CLUSTER_NAME"
if cluster_exists; then
  printf 'Refusing to reuse existing kind cluster: %s\n' "$CLUSTER_NAME" >&2
  exit 1
fi
CLUSTER_OWNED=1
"$KIND_BIN" create cluster --name "$CLUSTER_NAME" --image "$NODE_IMAGE" --wait "${TIMEOUT_SECONDS}s"

"$KIND_BIN" load docker-image --name "$CLUSTER_NAME" "$ORCHESTRATOR_IMAGE" "$WORKER_IMAGE" "$TARGET_IMAGE"

kubectl_kind apply -f "$REPO_ROOT/deploy/kubernetes/base/namespace.yaml" >/dev/null
kubectl_kind create configmap load-test-config \
  --namespace "$NAMESPACE" \
  --from-file="demo.yaml=$REPO_ROOT/deploy/docker-autoscale/demo.yaml" \
  --dry-run=client \
  -o yaml >"$TMP_DIR/load-test-config.yaml"
kubectl_kind apply -f "$TMP_DIR/load-test-config.yaml" >/dev/null
kubectl_kind apply -k "$REPO_ROOT/deploy/kubernetes/overlays/kind" >/dev/null

kubectl_kind get configmap load-test-config --namespace "$NAMESPACE" -o jsonpath='{.data.demo\.yaml}' >"$TMP_DIR/rendered-demo.yaml"
if ! cmp -s "$REPO_ROOT/deploy/docker-autoscale/demo.yaml" "$TMP_DIR/rendered-demo.yaml"; then
  printf '%s\n' 'Kubernetes ConfigMap does not contain the unchanged Piece 19 scenario' >&2
  exit 1
fi

for deployment in autoscale-target prometheus grafana; do
  kubectl_kind rollout status "deployment/$deployment" --namespace "$NAMESPACE" --timeout "${TIMEOUT_SECONDS}s"
done

assert_can_i yes get deployment/load-tester-worker --namespace "$NAMESPACE"
assert_can_i yes get deployments.apps/load-tester-worker --subresource scale --namespace "$NAMESPACE"
assert_can_i yes update deployments.apps/load-tester-worker --subresource scale --namespace "$NAMESPACE"
assert_can_i yes list pods --namespace "$NAMESPACE"
assert_can_i yes delete pods --namespace "$NAMESPACE"
assert_can_i no get secrets --namespace "$NAMESPACE"
assert_can_i no delete deployments --namespace "$NAMESPACE"
assert_can_i no create pods --namespace "$NAMESPACE"
assert_can_i no list pods --namespace default
printf '%s\n' 'RBAC: PASS (required actions allowed, unrelated actions denied)'

PROMETHEUS_PORT=$(reserve_port)
GRAFANA_PORT=$(reserve_port)
kubectl_kind port-forward --address 127.0.0.1 --namespace "$NAMESPACE" service/prometheus "$PROMETHEUS_PORT:9090" >"$TMP_DIR/prometheus-port-forward.log" 2>&1 &
PROMETHEUS_PORT_FORWARD_PID=$!
kubectl_kind port-forward --address 127.0.0.1 --namespace "$NAMESPACE" service/grafana "$GRAFANA_PORT:3000" >"$TMP_DIR/grafana-port-forward.log" 2>&1 &
GRAFANA_PORT_FORWARD_PID=$!
wait_port_forward "http://127.0.0.1:$PROMETHEUS_PORT/-/ready" "$PROMETHEUS_PORT_FORWARD_PID" "$TMP_DIR/prometheus-port-forward.log"
wait_port_forward "http://127.0.0.1:$GRAFANA_PORT/api/health" "$GRAFANA_PORT_FORWARD_PID" "$TMP_DIR/grafana-port-forward.log"

dashboard_search=$(curl --silent --fail --user admin:admin "http://127.0.0.1:$GRAFANA_PORT/api/search?query=Load%20Tester%20Overview")
if [[ "$dashboard_search" != *'"uid":"load-tester-overview"'* ]]; then
  printf 'Grafana dashboard was not provisioned: %s\n' "$dashboard_search" >&2
  exit 1
fi
printf '%s\n' 'Observability provisioning: PASS (Prometheus ready, Grafana dashboard loaded)'

kubectl_kind patch job load-tester-orchestrator --namespace "$NAMESPACE" --type merge --patch '{"spec":{"suspend":false}}' >/dev/null
worker_pod=$(wait_for_worker_pod)
kubectl_kind delete pod "$worker_pod" --namespace "$NAMESPACE" --dry-run=server --as system:serviceaccount:$NAMESPACE:load-tester-orchestrator --wait=false >/dev/null
printf 'RBAC delete authorization: PASS (server-side dry run for %s)\n' "$worker_pod"

deadline=$((SECONDS + TIMEOUT_SECONDS))
saw_prometheus_target=0
saw_scale=0
saw_recovery=0
max_desired=0
while true; do
  desired=$(kubectl_kind get deployment load-tester-worker --namespace "$NAMESPACE" -o jsonpath='{.spec.replicas}')
  if (( desired > max_desired )); then
    max_desired=$desired
  fi
  if (( desired > 1 )); then
    saw_scale=1
  fi

  up=$(prometheus_value 'up{job="orchestrator"}')
  actions=$(prometheus_value 'loadtester_autoscaler_scale_actions_total')
  coverage=$(prometheus_value 'loadtester_autoscaler_scheduler_coverage_ratio')
  if [[ "$up" == 1 ]]; then
    saw_prometheus_target=1
  fi
  if (( saw_scale )) && [[ -n "$actions" && -n "$coverage" ]] && \
    awk -v actions="$actions" -v coverage="$coverage" 'BEGIN { exit !(actions >= 1 && coverage >= 0.90) }'; then
    saw_recovery=1
  fi

  succeeded=$(kubectl_kind get job load-tester-orchestrator --namespace "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
  failed=$(kubectl_kind get job load-tester-orchestrator --namespace "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null || true)
  if [[ "$succeeded" == 1 || "$failed" == 1 ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    printf '%s\n' 'Kubernetes autoscaling proof timed out' >&2
    exit 1
  fi
  sleep 1
done

if (( ! saw_prometheus_target || ! saw_scale || ! saw_recovery )); then
  printf 'Autoscaling evidence incomplete: prometheus=%d scaled=%d recovered=%d max_desired=%d\n' \
    "$saw_prometheus_target" "$saw_scale" "$saw_recovery" "$max_desired" >&2
  exit 1
fi

orchestrator_pod=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector job-name=load-tester-orchestrator -o jsonpath='{.items[0].metadata.name}')
orchestrator_exit=$(kubectl_kind get pod "$orchestrator_pod" --namespace "$NAMESPACE" -o jsonpath='{.status.containerStatuses[?(@.name=="orchestrator")].state.terminated.exitCode}')
orchestrator_log=$(kubectl_kind logs "$orchestrator_pod" --namespace "$NAMESPACE" --container orchestrator)
case "$orchestrator_exit" in
  0)
    if [[ "$orchestrator_log" != *'Result: PASS'* ]]; then
      printf '%s\n' 'Orchestrator exited 0 without a PASS load-test result' >&2
      exit 1
    fi
    result=PASS
    ;;
  1)
    if [[ "$orchestrator_log" != *'Result: FAIL'* ]]; then
      printf '%s\n' 'Orchestrator exited 1 without a valid FAIL load-test result' >&2
      exit 1
    fi
    result=FAIL
    ;;
  *)
    printf 'Platform failure: orchestrator exit status %s\n%s\n' "$orchestrator_exit" "$orchestrator_log" >&2
    exit 1
    ;;
esac

deadline=$((SECONDS + TIMEOUT_SECONDS))
while true; do
  desired=$(kubectl_kind get deployment load-tester-worker --namespace "$NAMESPACE" -o jsonpath='{.spec.replicas}')
  remaining=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector app.kubernetes.io/name=load-tester-worker --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$desired" == 0 && "$remaining" == 0 ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    printf 'Worker cleanup timed out: desired=%s remaining=%s\n' "$desired" "$remaining" >&2
    exit 1
  fi
  sleep 1
done

printf 'Kubernetes autoscaling: PASS (max_desired=%d coverage_recovered=true load_test_result=%s exit_status=%s)\n' \
  "$max_desired" "$result" "$orchestrator_exit"
