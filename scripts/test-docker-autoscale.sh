#!/usr/bin/env bash
set -euo pipefail

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command is missing: %s\n' "$1" >&2
    exit 1
  fi
}

require_command curl
require_command docker
require_command go
require_command python3

if ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'Docker is installed but the daemon is unavailable' >&2
  exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
RATE=${RATE:-1200}
MAX_IN_FLIGHT=${MAX_IN_FLIGHT:-$((RATE * 10))}
WORKER_CPUS=${WORKER_CPUS:-0.25}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-90}
MAX_WORKERS=${MAX_WORKERS:-4}
IMAGE_PREFIX=${IMAGE_PREFIX:-load-tester-autoscale-$$}
WORKER_IMAGE_PROVIDED=${WORKER_IMAGE+x}
TARGET_IMAGE_PROVIDED=${TARGET_IMAGE+x}
WORKER_IMAGE=${WORKER_IMAGE:-$IMAGE_PREFIX-worker:test}
TARGET_IMAGE=${TARGET_IMAGE:-$IMAGE_PREFIX-target:test}
NETWORK_NAME="$IMAGE_PREFIX-network"
TARGET_CONTAINER="$IMAGE_PREFIX-target"
TMP_DIR=$(mktemp -d)
CONFIG_PATH="$TMP_DIR/demo.yaml"
ORCHESTRATOR_LOG="$TMP_DIR/orchestrator.log"
ORCHESTRATOR_PID=''
BUILT_WORKER=0
BUILT_TARGET=0
CLEANUP_FAILED=0

reserve_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

GRPC_PORT=${GRPC_PORT:-$(reserve_port)}
METRICS_PORT=${METRICS_PORT:-$(reserve_port)}
METRICS_URL="http://127.0.0.1:$METRICS_PORT/metrics"

cleanup_failure() {
  printf 'CLEANUP FAILED: %s\n' "$1" >&2
  CLEANUP_FAILED=1
}

cleanup() {
  local exit_status=$?
  trap - EXIT
  set +e

  if [[ -n "$ORCHESTRATOR_PID" ]] && kill -0 "$ORCHESTRATOR_PID" >/dev/null 2>&1; then
    kill "$ORCHESTRATOR_PID" >/dev/null 2>&1 || cleanup_failure 'could not stop orchestrator'
    wait "$ORCHESTRATOR_PID" >/dev/null 2>&1 || true
  fi

  worker_ids=$(docker ps --all --quiet --filter "ancestor=$WORKER_IMAGE" 2>/dev/null || true)
  if [[ -n "$worker_ids" ]]; then
    docker rm --force $worker_ids >/dev/null 2>&1 || cleanup_failure 'could not remove managed workers'
  fi
  if docker container inspect "$TARGET_CONTAINER" >/dev/null 2>&1; then
    docker rm --force "$TARGET_CONTAINER" >/dev/null 2>&1 || cleanup_failure 'could not remove target container'
  fi
  if docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
    docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || cleanup_failure 'could not remove test network'
  fi
  if (( BUILT_WORKER )); then
    docker image rm --force "$WORKER_IMAGE" >/dev/null 2>&1 || cleanup_failure 'could not remove worker image'
  fi
  if (( BUILT_TARGET )); then
    docker image rm --force "$TARGET_IMAGE" >/dev/null 2>&1 || cleanup_failure 'could not remove target image'
  fi
  rm -rf "$TMP_DIR" || cleanup_failure 'could not remove temporary files'

  remaining=$(docker ps --all --quiet --filter "ancestor=$WORKER_IMAGE" 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$remaining" != 0 ]]; then
    cleanup_failure "$remaining managed worker containers remain"
  fi
  if (( CLEANUP_FAILED )); then
    exit_status=1
  else
    printf 'Cleanup: PASS (workers=0 target=removed network=removed)\n'
  fi
  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cp "$REPO_ROOT/deploy/docker-autoscale/demo.yaml" "$CONFIG_PATH"
sed -i.bak \
  -e "s/^  rate: .*/  rate: $RATE/" \
  -e "s/^  max_in_flight: .*/  max_in_flight: $MAX_IN_FLIGHT/" \
  -e "s/^  max_workers: .*/  max_workers: $MAX_WORKERS/" \
  "$CONFIG_PATH"
rm -f "$CONFIG_PATH.bak"

if [[ -z "$WORKER_IMAGE_PROVIDED" ]]; then
  docker build --quiet --target worker --tag "$WORKER_IMAGE" "$REPO_ROOT" >/dev/null
  BUILT_WORKER=1
fi
if [[ -z "$TARGET_IMAGE_PROVIDED" ]]; then
  docker build --quiet --target target --tag "$TARGET_IMAGE" "$REPO_ROOT" >/dev/null
  BUILT_TARGET=1
fi

docker network create "$NETWORK_NAME" >/dev/null
docker run --detach --name "$TARGET_CONTAINER" --network "$NETWORK_NAME" --network-alias autoscale-target "$TARGET_IMAGE" >/dev/null

deadline=$((SECONDS + TIMEOUT_SECONDS))
until docker exec "$TARGET_CONTAINER" /app/healthcheck -url http://127.0.0.1:8080/health >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    printf '%s\n' 'Target did not become healthy before timeout' >&2
    exit 1
  fi
  if ! docker container inspect --format '{{.State.Running}}' "$TARGET_CONTAINER" 2>/dev/null | grep -q true; then
    printf '%s\n' 'Target container stopped before becoming healthy' >&2
    docker logs "$TARGET_CONTAINER" >&2 || true
    exit 1
  fi
  sleep 1
done

(
  cd "$REPO_ROOT"
  exec go run ./cmd/orchestrator \
    -addr "127.0.0.1:$GRPC_PORT" \
    -metrics-addr "127.0.0.1:$METRICS_PORT" \
    -config "$CONFIG_PATH" \
    -fleet-driver docker \
    -worker-image "$WORKER_IMAGE" \
    -docker-network "$NETWORK_NAME" \
    -worker-orchestrator-addr "host.docker.internal:$GRPC_PORT" \
    -docker-add-host-gateway \
    -docker-worker-cpus "$WORKER_CPUS"
) >"$ORCHESTRATOR_LOG" 2>&1 &
ORCHESTRATOR_PID=$!

metric_value() {
  local metric=$1
  curl --silent --fail "$METRICS_URL" 2>/dev/null | awk -v name="$metric" '$1 == name { print $2; exit }'
}

saw_scale=0
saw_recovery=0
max_desired=0
deadline=$((SECONDS + TIMEOUT_SECONDS))
while kill -0 "$ORCHESTRATOR_PID" >/dev/null 2>&1; do
  desired=$(metric_value loadtester_desired_workers || true)
  actions=$(metric_value loadtester_autoscaler_scale_actions_total || true)
  coverage=$(metric_value loadtester_autoscaler_scheduler_coverage_ratio || true)
  if [[ -n "$desired" ]] && awk -v value="$desired" 'BEGIN { exit !(value > 1) }'; then
    saw_scale=1
    max_desired=$desired
  fi
  if (( saw_scale )) && [[ -n "$actions" && -n "$coverage" ]] && \
    awk -v actions="$actions" -v coverage="$coverage" 'BEGIN { exit !(actions >= 1 && coverage >= 0.90) }'; then
    saw_recovery=1
  fi
  if (( SECONDS >= deadline )); then
    printf '%s\n' 'Autoscaling proof timed out' >&2
    cat "$ORCHESTRATOR_LOG" >&2
    exit 1
  fi
  sleep 1
done

set +e
wait "$ORCHESTRATOR_PID"
orchestrator_status=$?
set -e
ORCHESTRATOR_PID=''
if (( orchestrator_status != 0 && orchestrator_status != 1 )); then
  printf 'Orchestrator exited with setup/interruption status %d\n' "$orchestrator_status" >&2
  cat "$ORCHESTRATOR_LOG" >&2
  exit 1
fi
if (( ! saw_scale || ! saw_recovery )); then
  printf 'Autoscaling evidence incomplete: scaled=%d recovered=%d max_desired=%s\n' "$saw_scale" "$saw_recovery" "$max_desired" >&2
  cat "$ORCHESTRATOR_LOG" >&2
  exit 1
fi

printf 'Autoscaling: PASS (max_desired=%s coverage_recovered=true exit_status=%d)\n' \
  "$max_desired" "$orchestrator_status"
