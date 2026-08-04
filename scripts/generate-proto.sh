#!/usr/bin/env bash

set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly MODULE="github.com/marcuslin123/load-tester"
readonly PROTO_FILE="${ROOT_DIR}/proto/loadtest/v1/loadtest.proto"
readonly BIN_DIR="${ROOT_DIR}/bin"
readonly PROTOC_GEN_GO_VERSION="v1.36.11"
readonly PROTOC_GEN_GO_GRPC_VERSION="v1.6.2"

usage() {
  echo "usage: $0 [--check]" >&2
}

require_protoc() {
  if ! command -v protoc >/dev/null 2>&1; then
    echo "protoc is required; install the protobuf compiler and retry" >&2
    exit 1
  fi
}

install_plugin() {
  local name="$1"
  local module="$2"
  local version="$3"
  local executable="${BIN_DIR}/${name}"

  if [[ -x "${executable}" ]] && "${executable}" --version 2>/dev/null | grep -q "${version#v}"; then
    return
  fi

  echo "installing ${name} ${version}" >&2
  env GOBIN="${BIN_DIR}" go install "${module}@${version}"
}

generate_into() {
  local output="$1"
  mkdir -p "${output}"

  protoc \
    --proto_path="${ROOT_DIR}/proto" \
    --go_out="${output}" \
    --go_opt="module=${MODULE}" \
    --go-grpc_out="${output}" \
    --go-grpc_opt="module=${MODULE}" \
    --plugin="protoc-gen-go=${BIN_DIR}/protoc-gen-go" \
    --plugin="protoc-gen-go-grpc=${BIN_DIR}/protoc-gen-go-grpc" \
    "${PROTO_FILE}"
}

check_generated() {
  local temporary
  temporary="$(mktemp -d)"
  trap "rm -rf '${temporary}'" EXIT
  generate_into "${temporary}"

  local stale=0
  local file
  for file in loadtest.pb.go loadtest_grpc.pb.go; do
    if ! cmp -s "${ROOT_DIR}/gen/loadtest/v1/${file}" "${temporary}/gen/loadtest/v1/${file}"; then
      echo "generated file is stale: gen/loadtest/v1/${file}" >&2
      stale=1
    fi
  done
  if [[ "${stale}" -ne 0 ]]; then
    echo "run ./scripts/generate-proto.sh and commit the result" >&2
    exit 1
  fi
}

main() {
  if [[ "$#" -gt 1 ]] || [[ "$#" -eq 1 && "$1" != "--check" ]]; then
    usage
    exit 2
  fi

  require_protoc
  mkdir -p "${BIN_DIR}"
  install_plugin "protoc-gen-go" "google.golang.org/protobuf/cmd/protoc-gen-go" "${PROTOC_GEN_GO_VERSION}"
  install_plugin "protoc-gen-go-grpc" "google.golang.org/grpc/cmd/protoc-gen-go-grpc" "${PROTOC_GEN_GO_GRPC_VERSION}"

  if [[ "$#" -eq 1 ]]; then
    check_generated
    return
  fi
  generate_into "${ROOT_DIR}"
}

main "$@"
