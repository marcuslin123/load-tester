#!/usr/bin/env bash
set -euo pipefail

# A per-process default prevents concurrent smoke runs from sharing image tags.
IMAGE_PREFIX=${IMAGE_PREFIX:-load-tester-piece15-$$}
VERSION=${VERSION:-piece15-smoke}
VCS_REF=${VCS_REF:-$(git rev-parse --short=12 HEAD)}
SOURCE_URL=${SOURCE_URL:-https://github.com/marcuslin123/load-tester}

require_command() {
  local command_name=$1

  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'Required command is missing: %s\n' "$command_name" >&2
    exit 1
  fi
}

require_command docker
require_command curl
require_command go

if ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'Docker is installed but the daemon is unavailable' >&2
  exit 1
fi

DOCKER_ARCHITECTURE=$(docker info --format '{{.Architecture}}')
# Docker Desktop reports POSIX names while image inspect uses OCI names.
case "$DOCKER_ARCHITECTURE" in
  aarch64) DOCKER_ARCHITECTURE=arm64 ;;
  x86_64) DOCKER_ARCHITECTURE=amd64 ;;
esac

assert_field() {
  local target=$1
  local field=$2
  local actual=$3
  local expected=$4

  if [[ "$actual" != "$expected" ]]; then
    printf 'ASSERTION FAILED target=%s field=%s actual=%q expected=%q\n' \
      "$target" "$field" "$actual" "$expected" >&2
    return 1
  fi
}

assert_image_contract() {
  local target=$1
  local expected_entrypoint=$2
  local expected_port=$3
  local expected_title=$4
  local image="$IMAGE_PREFIX-$target:$VERSION"
  local actual_user actual_entrypoint actual_ports actual_architecture
  local actual_source actual_version actual_revision actual_title
  local expected_entrypoint_json expected_ports

  actual_user=$(docker image inspect --format '{{.Config.User}}' "$image")
  if [[ "$actual_user" != 'nonroot' && "$actual_user" != '65532' ]]; then
    printf 'ASSERTION FAILED target=%s field=%s actual=%q expected=%q\n' \
      "$target" 'Config.User' "$actual_user" 'nonroot or 65532' >&2
    return 1
  fi

  actual_source=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.source"}}' "$image")
  assert_field "$target" 'org.opencontainers.image.source' "$actual_source" "$SOURCE_URL"
  actual_version=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$image")
  assert_field "$target" 'org.opencontainers.image.version' "$actual_version" "$VERSION"
  actual_revision=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image")
  assert_field "$target" 'org.opencontainers.image.revision' "$actual_revision" "$VCS_REF"
  actual_title=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.title"}}' "$image")
  assert_field "$target" 'org.opencontainers.image.title' "$actual_title" "$expected_title"

  actual_architecture=$(docker image inspect --format '{{.Architecture}}' "$image")
  assert_field "$target" 'Architecture' "$actual_architecture" "$DOCKER_ARCHITECTURE"

  actual_entrypoint=$(docker image inspect --format '{{json .Config.Entrypoint}}' "$image")
  expected_entrypoint_json=$(printf '["%s"]' "$expected_entrypoint")
  assert_field "$target" 'Config.Entrypoint' "$actual_entrypoint" "$expected_entrypoint_json"

  actual_ports=$(docker image inspect --format '{{json .Config.ExposedPorts}}' "$image")
  if [[ -n "$expected_port" ]]; then
    expected_ports=$(printf '{"%s":{}}' "$expected_port")
  else
    expected_ports=null
  fi
  assert_field "$target" 'Config.ExposedPorts' "$actual_ports" "$expected_ports"
}

KEEP_IMAGES=${KEEP_IMAGES:-0}
TMP_DIR=''
NETWORK_NAME="${IMAGE_PREFIX}-network-$$"
TARGET_CONTAINER="${IMAGE_PREFIX}-target-$$"
ORCHESTRATOR_CONTAINER="${IMAGE_PREFIX}-orchestrator-$$"
WORKER_CONTAINER="${IMAGE_PREFIX}-worker-$$"
HOST_WORKER_CONTAINER="${IMAGE_PREFIX}-host-worker-$$"
KNOWN_CONTAINERS=("$TARGET_CONTAINER" "$ORCHESTRATOR_CONTAINER" "$WORKER_CONTAINER" "$HOST_WORKER_CONTAINER")
HOST_FIXTURE_PIDS=()
HOST_ORCHESTRATOR_PID=''
HOST_ORCHESTRATOR_EXIT_CODE=''
HOST_ORCHESTRATOR_LOG=''
CLEANED_UP=0
CLEANUP_FAILED=0

cleanup_failure() {
  printf 'CLEANUP FAILED: %s\n' "$1" >&2
  CLEANUP_FAILED=1
}

cleanup_container() {
  local container=$1

  if docker container inspect "$container" >/dev/null 2>&1; then
    if ! docker rm --force "$container" >/dev/null 2>&1; then
      cleanup_failure "unable to remove container $container"
    fi
  fi
}

cleanup_network() {
  if docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
    if ! docker network rm "$NETWORK_NAME" >/dev/null 2>&1; then
      cleanup_failure "unable to remove network $NETWORK_NAME"
    fi
  fi
}

cleanup_image() {
  local image=$1

  if docker image inspect "$image" >/dev/null 2>&1; then
    if ! docker image rm --force "$image" >/dev/null 2>&1; then
      cleanup_failure "unable to remove image $image"
    fi
  fi
}

cleanup() {
  local exit_status=$?

  if (( CLEANED_UP )); then
    return
  fi
  CLEANED_UP=1
  set +e

  if (( ${#HOST_FIXTURE_PIDS[@]} > 0 )); then
    for pid in "${HOST_FIXTURE_PIDS[@]}"; do
      if kill -0 "$pid" >/dev/null 2>&1; then
        if ! kill "$pid" >/dev/null 2>&1; then
          cleanup_failure "unable to terminate host fixture process $pid"
        fi
      fi
      wait "$pid" >/dev/null 2>&1 || true
      if kill -0 "$pid" >/dev/null 2>&1; then
        cleanup_failure "host fixture process $pid is still running"
      fi
    done
  fi

  if ! docker info >/dev/null 2>&1; then
    cleanup_failure 'Docker daemon is unavailable during cleanup'
  else
    for container in "${KNOWN_CONTAINERS[@]}"; do
      cleanup_container "$container"
    done
    cleanup_network
    if [[ "$KEEP_IMAGES" != 1 ]]; then
      for target in orchestrator worker target; do
        cleanup_image "$IMAGE_PREFIX-$target:$VERSION"
      done
    fi
  fi

  if [[ -n "$TMP_DIR" && -e "$TMP_DIR" ]]; then
    if ! rm -rf "$TMP_DIR"; then
      cleanup_failure "unable to remove temporary directory $TMP_DIR"
    fi
  fi

  if (( CLEANUP_FAILED )); then
    exit_status=1
  else
    if [[ "$KEEP_IMAGES" == 1 ]]; then
      cleanup_images=kept
    else
      cleanup_images=removed
    fi
    printf 'Cleanup: PASS (host_processes=%s containers=%s network=removed temp_dir=removed images=%s)\n' \
      "${#HOST_FIXTURE_PIDS[@]}" "${#KNOWN_CONTAINERS[@]}" "$cleanup_images"
  fi

  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

TMP_DIR=$(mktemp -d)

for target in orchestrator worker target; do
  docker build \
    --target "$target" \
    --build-arg VERSION="$VERSION" \
    --build-arg VCS_REF="$VCS_REF" \
    --build-arg SOURCE_URL="$SOURCE_URL" \
    --tag "$IMAGE_PREFIX-$target:$VERSION" \
    .
done

assert_image_contract orchestrator /app/orchestrator 9090/tcp load-tester-orchestrator
assert_image_contract worker /app/worker '' load-tester-worker
assert_image_contract target /app/target 8080/tcp load-tester-target

assert_log_contains() {
  local container=$1
  local logs=$2
  local expected=$3

  if [[ "$logs" != *"$expected"* ]]; then
    printf 'ASSERTION FAILED container=%s logs missing=%q\n%s\n' \
      "$container" "$expected" "$logs" >&2
    return 1
  fi
}

assert_log_absent() {
  local container=$1
  local logs=$2
  local unexpected=$3

  if [[ "$logs" == *"$unexpected"* ]]; then
    printf 'ASSERTION FAILED container=%s logs contain=%q\n%s\n' \
      "$container" "$unexpected" "$logs" >&2
    return 1
  fi
}

wait_for_container_exit() {
  local container=$1
  local deadline=$2
  local state

  while :; do
    state=$(docker container inspect --format '{{.State.Status}}' "$container")
    if [[ "$state" == exited || "$state" == dead ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      printf 'Timed out waiting for container to exit: %s\n' "$container" >&2
      docker logs "$container" >&2 || true
      return 1
    fi
    sleep 0.25
  done
}

wait_for_process_exit() {
  local pid=$1
  local deadline=$2

  while kill -0 "$pid" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf 'Timed out waiting for host process to exit: %s\n' "$pid" >&2
      if [[ -n "$HOST_ORCHESTRATOR_LOG" ]]; then
        cat "$HOST_ORCHESTRATOR_LOG" >&2 || true
      fi
      return 1
    fi
    sleep 0.25
  done

  if wait "$pid"; then
    HOST_ORCHESTRATOR_EXIT_CODE=0
  else
    HOST_ORCHESTRATOR_EXIT_CODE=$?
  fi
}

docker network create "$NETWORK_NAME" >/dev/null

docker run --detach \
  --name "$TARGET_CONTAINER" \
  --network "$NETWORK_NAME" \
  --network-alias target \
  --publish-all \
  "$IMAGE_PREFIX-target:$VERSION" >/dev/null

target_port_mapping=$(docker port "$TARGET_CONTAINER" 8080/tcp)
target_port_mapping=${target_port_mapping%%$'\n'*}
TARGET_PORT=${target_port_mapping##*:}
TARGET_PORT=${TARGET_PORT//$'\r'/}
if [[ ! "$TARGET_PORT" =~ ^[0-9]+$ ]]; then
  printf 'Unable to discover target port from docker port: %q\n' "$target_port_mapping" >&2
  exit 1
fi

health_deadline=$((SECONDS + 10))
target_health_body=''
while :; do
  target_health_body=$(curl --fail --silent --show-error --max-time 1 \
    "http://127.0.0.1:${TARGET_PORT}/health" 2>/dev/null || true)
  target_health_body=${target_health_body%$'\n'}
  if [[ "$target_health_body" == ok ]]; then
    printf 'Target health: PASS (host_port=%s body=%s)\n' "$TARGET_PORT" "$target_health_body"
    break
  fi
  if (( SECONDS >= health_deadline )); then
    printf 'Target health failed: host_port=%s body=%q\n' "$TARGET_PORT" "$target_health_body" >&2
    exit 1
  fi
  sleep 0.25
done

CONFIG_FILE="$TMP_DIR/test.yaml"
printf '%s\n' \
  'name: piece-15-container-smoke' \
  'target:' \
  '  protocol: http' \
  '  url: http://target:8080/echo?latency=5ms' \
  'load:' \
  '  model: constant-vus' \
  '  virtual_users: 2' \
  '  duration: 1200ms' \
  'fleet:' \
  '  min_workers: 1' \
  '  max_workers: 1' \
  'thresholds:' \
  '  p99_latency: 100ms' \
  '  error_rate: 0' \
  >"$CONFIG_FILE"

docker run --detach \
  --name "$ORCHESTRATOR_CONTAINER" \
  --network "$NETWORK_NAME" \
  --network-alias orchestrator \
  --volume "$CONFIG_FILE:/config/test.yaml:ro" \
  "$IMAGE_PREFIX-orchestrator:$VERSION" \
  -config /config/test.yaml >/dev/null

docker run --detach \
  --name "$WORKER_CONTAINER" \
  --network "$NETWORK_NAME" \
  --env ORCHESTRATOR_ADDR=orchestrator:9090 \
  --env WORKER_ID=piece-15-worker \
  "$IMAGE_PREFIX-worker:$VERSION" >/dev/null

orchestrator_deadline=$((SECONDS + 15))
wait_for_container_exit "$ORCHESTRATOR_CONTAINER" "$orchestrator_deadline"

orchestrator_exit_code=$(docker container inspect --format '{{.State.ExitCode}}' "$ORCHESTRATOR_CONTAINER")
if [[ "$orchestrator_exit_code" != 0 ]]; then
  printf 'ASSERTION FAILED container=%s field=exit_status actual=%q expected=0\n' \
    "$ORCHESTRATOR_CONTAINER" "$orchestrator_exit_code" >&2
  docker logs "$ORCHESTRATOR_CONTAINER" >&2 || true
  exit 1
fi

orchestrator_logs=$(docker logs "$ORCHESTRATOR_CONTAINER" 2>&1)
assert_log_contains "$ORCHESTRATOR_CONTAINER" "$orchestrator_logs" 'worker registered: id=piece-15-worker'
assert_log_contains "$ORCHESTRATOR_CONTAINER" "$orchestrator_logs" "version=$VERSION"
assert_log_contains "$ORCHESTRATOR_CONTAINER" "$orchestrator_logs" 'Result: PASS'
assert_log_contains "$ORCHESTRATOR_CONTAINER" "$orchestrator_logs" 'Unmet demand: 0'
assert_log_contains "$ORCHESTRATOR_CONTAINER" "$orchestrator_logs" 'Dropped samples: 0'
assert_log_absent "$ORCHESTRATOR_CONTAINER" "$orchestrator_logs" 'missing final metrics'

if [[ "$orchestrator_logs" =~ Requests:[[:space:]]*([0-9]+) ]]; then
  requests="${BASH_REMATCH[1]}"
else
  printf 'ASSERTION FAILED container=%s field=Requests actual=missing expected=greater than 0\n' \
    "$ORCHESTRATOR_CONTAINER" >&2
  exit 1
fi
if (( requests <= 0 )); then
  printf 'ASSERTION FAILED container=%s field=Requests actual=%s expected=greater than 0\n' \
    "$ORCHESTRATOR_CONTAINER" "$requests" >&2
  exit 1
fi

docker stop "$WORKER_CONTAINER" >/dev/null
printf 'Distributed run: PASS (orchestrator_exit=%s requests=%s unmet_demand=0 dropped_samples=0)\n' \
  "$orchestrator_exit_code" "$requests"

run_host_dial_test() {
  local host_port=$((30000 + ($$ % 20000)))
  local host_config="$TMP_DIR/host-test.yaml"
  local host_binary="$TMP_DIR/orchestrator"
  local host_deadline host_logs host_requests

  if ( : >/dev/tcp/127.0.0.1/"$host_port" ) 2>/dev/null; then
    printf 'Host orchestrator port is unavailable: %s\n' "$host_port" >&2
    return 1
  fi

  printf '%s\n' \
    'name: piece-15-host-dial-smoke' \
    'target:' \
    '  protocol: http' \
    "  url: http://host.docker.internal:${TARGET_PORT}/echo?latency=5ms" \
    'load:' \
    '  model: constant-vus' \
    '  virtual_users: 2' \
    '  duration: 1200ms' \
    'fleet:' \
    '  min_workers: 1' \
    '  max_workers: 1' \
    'thresholds:' \
    '  p99_latency: 100ms' \
    '  error_rate: 0' \
    >"$host_config"

  go build -trimpath -o "$host_binary" ./cmd/orchestrator
  HOST_ORCHESTRATOR_LOG="$TMP_DIR/host-orchestrator.log"
  "$host_binary" \
    -addr "0.0.0.0:${host_port}" \
    -config "$host_config" \
    >"$HOST_ORCHESTRATOR_LOG" 2>&1 &
  HOST_ORCHESTRATOR_PID=$!
  HOST_FIXTURE_PIDS+=("$HOST_ORCHESTRATOR_PID")

  host_deadline=$((SECONDS + 10))
  while ! ( : >/dev/tcp/127.0.0.1/"$host_port" ) 2>/dev/null; do
    if ! kill -0 "$HOST_ORCHESTRATOR_PID" >/dev/null 2>&1; then
      wait "$HOST_ORCHESTRATOR_PID" || true
      printf 'Host orchestrator exited before accepting connections\n' >&2
      cat "$HOST_ORCHESTRATOR_LOG" >&2
      return 1
    fi
    if (( SECONDS >= host_deadline )); then
      printf 'Timed out waiting for host orchestrator on port %s\n' "$host_port" >&2
      cat "$HOST_ORCHESTRATOR_LOG" >&2
      return 1
    fi
    sleep 0.25
  done

  docker run --detach \
    --name "$HOST_WORKER_CONTAINER" \
    --add-host host.docker.internal:host-gateway \
    --env "ORCHESTRATOR_ADDR=host.docker.internal:${host_port}" \
    --env WORKER_ID=piece-15-host-worker \
    "$IMAGE_PREFIX-worker:$VERSION" >/dev/null

  host_deadline=$((SECONDS + 15))
  wait_for_process_exit "$HOST_ORCHESTRATOR_PID" "$host_deadline"
  if [[ "$HOST_ORCHESTRATOR_EXIT_CODE" != 0 ]]; then
    printf 'ASSERTION FAILED process=host-orchestrator field=exit_status actual=%q expected=0\n' \
      "$HOST_ORCHESTRATOR_EXIT_CODE" >&2
    cat "$HOST_ORCHESTRATOR_LOG" >&2
    return 1
  fi

  host_logs=$(<"$HOST_ORCHESTRATOR_LOG")
  assert_log_contains host-orchestrator "$host_logs" 'worker registered: id=piece-15-host-worker'
  assert_log_contains host-orchestrator "$host_logs" "version=$VERSION"
  assert_log_contains host-orchestrator "$host_logs" 'Result: PASS'
  assert_log_contains host-orchestrator "$host_logs" 'Unmet demand: 0'
  assert_log_contains host-orchestrator "$host_logs" 'Dropped samples: 0'
  assert_log_absent host-orchestrator "$host_logs" 'missing final metrics'

  if [[ "$host_logs" =~ Requests:[[:space:]]*([0-9]+) ]]; then
    host_requests="${BASH_REMATCH[1]}"
  else
    printf '%s\n' 'ASSERTION FAILED process=host-orchestrator field=Requests actual=missing expected=greater than 0' >&2
    return 1
  fi
  if (( host_requests <= 0 )); then
    printf 'ASSERTION FAILED process=host-orchestrator field=Requests actual=%s expected=greater than 0\n' \
      "$host_requests" >&2
    return 1
  fi

  docker stop "$HOST_WORKER_CONTAINER" >/dev/null
  printf 'Host dial: PASS (orchestrator_exit=%s requests=%s worker_id=piece-15-host-worker)\n' \
    "$HOST_ORCHESTRATOR_EXIT_CODE" "$host_requests"
}

run_host_dial_test

build_amd64_worker() {
  local archive="$TMP_DIR/worker-linux-amd64.tar"
  local archive_bytes

  docker buildx build \
    --platform linux/amd64 \
    --target worker \
    --build-arg VERSION="$VERSION" \
    --build-arg VCS_REF="$VCS_REF" \
    --build-arg SOURCE_URL="$SOURCE_URL" \
    --output "type=oci,dest=$archive" \
    .

  if [[ ! -s "$archive" ]]; then
    printf 'ASSERTION FAILED artifact=%s expected=nonempty\n' "$archive" >&2
    return 1
  fi
  archive_bytes=$(wc -c <"$archive")
  archive_bytes=${archive_bytes//[[:space:]]/}
  printf 'Cross-build: PASS (platform=linux/amd64 archive_bytes=%s)\n' "$archive_bytes"
}

build_amd64_worker
