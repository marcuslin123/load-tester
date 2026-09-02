#!/usr/bin/env python3
"""Publish deterministic, sanitized EKS benchmark evidence."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import hashlib
import hmac
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
from typing import Any


REQUIRED_FIELDS = (
    "run_id",
    "git_sha",
    "region",
    "topology",
    "worker_experiment",
    "target_hpa_experiment",
    "teardown",
    "billing_attestation",
)

REDACTION_MARKER = "<redacted-credential>"

GIT_SHA_RE = re.compile(r"\b[0-9a-fA-F]{40}\b")
ACCOUNT_RE = re.compile(r"(?<!\d)\d{12}(?!\d)")
ARN_RE = re.compile(r"\barn:[A-Za-z0-9-]+:[^\s<>\"']+")
EMAIL_RE = re.compile(r"\b[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+\b")
IPV4_RE = re.compile(
    r"(?<![\d.])(?:25[0-5]|2[0-4]\d|1?\d?\d)"
    r"(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}(?![\d.])"
)
IPV6_RE = re.compile(
    r"(?<![0-9A-Fa-f:])(?:"
    r"(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|"
    r"(?:[0-9A-Fa-f]{1,4}:){1,7}:|"
    r"(?:[0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4}|"
    r"(?:[0-9A-Fa-f]{1,4}:){1,5}(?::[0-9A-Fa-f]{1,4}){1,2}|"
    r"(?:[0-9A-Fa-f]{1,4}:){1,4}(?::[0-9A-Fa-f]{1,4}){1,3}|"
    r"(?:[0-9A-Fa-f]{1,4}:){1,3}(?::[0-9A-Fa-f]{1,4}){1,4}|"
    r"(?:[0-9A-Fa-f]{1,4}:){1,2}(?::[0-9A-Fa-f]{1,4}){1,5}|"
    r"[0-9A-Fa-f]{1,4}:(?:(?::[0-9A-Fa-f]{1,4}){1,6})|"
    r":(?:(?::[0-9A-Fa-f]{1,4}){1,7}|:)"
    r")(?![0-9A-Fa-f:])"
)
ACCESS_KEY_RE = re.compile(
    r"\b(?:AKIA|ASIA|AIDA|AROA|AGPA|ANPA|ANVA|ASCA|ABIA|ACCA)[A-Z0-9]{16}\b"
)
CREDENTIAL_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(?:aws[-_ ]?(?:access[-_ ]?key[-_ ]?id|secret[-_ ]?access[-_ ]?key|session[-_ ]?token)|"
    r"access[-_ ]?key[-_ ]?id|secret[-_ ]?access[-_ ]?key|session[-_ ]?token|"
    r"security[-_ ]?token|authorization|api[-_ ]?(?:key|token)|access[-_ ]?token|"
    r"(?:client[-_ ]?)?secret|(?:auth|bearer|refresh)[-_ ]?token|password|"
    r"credential(?:s)?)\b"
    r"\s*[:=]\s*(?:\"[^\"]*\"|'[^']*'|[^\s,;}]+)"
)
BEARER_TOKEN_RE = re.compile(
    r"(?i)\b(?:authorization\s*[:=]\s*)?bearer\s+"
    r"(?:\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;}\]]+)"
)
CREDENTIAL_NAME_RE = re.compile(
    r"(?i)(?<!redacted-)\b(?:aws[-_ ]?(?:access[-_ ]?key[-_ ]?id|secret[-_ ]?access[-_ ]?key|session[-_ ]?token)|"
    r"access[-_ ]?key[-_ ]?id|secret[-_ ]?access[-_ ]?key|session[-_ ]?token|"
    r"security[-_ ]?token|authorization|api[-_ ]?(?:key|token)|access[-_ ]?token|"
    r"(?:client[-_ ]?)?secret|(?:auth|bearer|refresh)[-_ ]?token|password|"
    r"credential(?:s)?|private[-_ ]?key)\b"
)


class RedactionError(ValueError):
    """Raised when raw evidence cannot be safely published."""


def _redact_ipv6(match: re.Match[str]) -> str:
    try:
        address = ipaddress.ip_address(match.group(0))
    except ValueError:
        return match.group(0)
    return "<ip-address>" if address.version == 6 else match.group(0)


def _contains_ipv6(value: str) -> bool:
    for match in IPV6_RE.finditer(value):
        try:
            if ipaddress.ip_address(match.group(0)).version == 6:
                return True
        except ValueError:
            continue
    return False


def redact_text(value: str) -> str:
    """Replace sensitive values in arbitrary text without changing metrics."""

    if not isinstance(value, str):
        raise TypeError("redact_text expects a string")

    protected: dict[str, str] = {}

    def protect_commit_sha(match: re.Match[str]) -> str:
        token = f"__EKS_COMMIT_SHA_{len(protected)}__"
        protected[token] = match.group(0)
        return token

    redacted = GIT_SHA_RE.sub(protect_commit_sha, value)
    redacted = BEARER_TOKEN_RE.sub(REDACTION_MARKER, redacted)
    redacted = CREDENTIAL_ASSIGNMENT_RE.sub(REDACTION_MARKER, redacted)
    redacted = ACCESS_KEY_RE.sub(REDACTION_MARKER, redacted)
    redacted = ARN_RE.sub("<aws-arn>", redacted)
    redacted = EMAIL_RE.sub("<email-address>", redacted)
    redacted = IPV4_RE.sub("<ip-address>", redacted)
    redacted = IPV6_RE.sub(_redact_ipv6, redacted)
    redacted = ACCOUNT_RE.sub("<aws-account-id>", redacted)
    redacted = CREDENTIAL_NAME_RE.sub(REDACTION_MARKER, redacted)
    for token, original in protected.items():
        redacted = redacted.replace(token, original)
    return redacted


def _sensitive_key(key: str) -> bool:
    if key == REDACTION_MARKER or key.startswith(f"{REDACTION_MARKER}-"):
        return False
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    if normalized in {
        "accesskey",
        "accesskeyid",
        "awsaccesskeyid",
        "accountid",
        "awsaccountid",
        "secret",
        "secretkey",
        "secretaccesskey",
        "awssecretaccesskey",
        "sessiontoken",
        "awssessiontoken",
        "securitytoken",
        "authorization",
        "apikey",
        "password",
        "credential",
        "credentials",
        "privatekey",
        "accesstoken",
        "refreshtoken",
    }:
        return True
    if "secret" in normalized or "credential" in normalized or "privatekey" in normalized:
        return True
    return normalized.endswith("token") and normalized != "tokencount"


def _account_id_key(key: str) -> bool:
    return re.sub(r"[^a-z0-9]", "", key.lower()) in {"accountid", "awsaccountid"}


def _redacted_key(key: str, existing: dict[str, Any]) -> str:
    if not _sensitive_key(key):
        return redact_text(key)
    candidate = REDACTION_MARKER
    suffix = 1
    while candidate in existing:
        suffix += 1
        candidate = f"{REDACTION_MARKER}-{suffix}"
    return candidate


def redact_value(value: object) -> object:
    """Recursively redact strings and credential-bearing JSON fields."""

    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key in sorted(value, key=lambda item: str(item)):
            string_key = str(key)
            output_key = _redacted_key(string_key, redacted)
            if _account_id_key(string_key):
                redacted[redact_text(string_key)] = "<aws-account-id>"
            elif _sensitive_key(string_key):
                redacted[output_key] = REDACTION_MARKER
            else:
                redacted[output_key] = redact_value(value[key])
        return redacted
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def _required_field(value: dict[str, Any], field: str) -> object:
    if field not in value:
        raise RedactionError(f"metrics.json is missing required field: {field}")
    return value[field]


def _numeric_nonzero(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return not math.isfinite(float(value)) or value != 0
    if isinstance(value, str):
        try:
            return float(value) != 0
        except ValueError:
            return True
    return value is not None


def _mapping(value: object, path: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not value:
        raise RedactionError(f"{path} must be a non-empty object")
    return value


def _nonempty_string(value: object, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RedactionError(f"{path} must be a non-empty string")
    return value


def _positive_number(value: object, path: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RedactionError(f"{path} must be a measured positive number")
    if not math.isfinite(float(value)) or value <= 0:
        raise RedactionError(f"{path} must be a measured positive number")


def _zero_counter(value: object, path: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value != 0:
        raise RedactionError(f"{path} must be zero for curated publication")


def _validate_clean_evidence(value: object, path: str) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if normalized in {"droppedsamples", "unmetdemand"} and _numeric_nonzero(child):
                raise RedactionError(f"published evidence has nonzero {child_path}")
            if normalized in {"measuredconcurrency", "achievedconcurrency"}:
                raise RedactionError(
                    f"published evidence must not report {child_path} as measured concurrency"
                )
            if normalized == "maxinflight" and path.endswith(".measured"):
                raise RedactionError(
                    f"published evidence must not report {child_path} as measured concurrency"
                )
            _validate_clean_evidence(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_clean_evidence(child, f"{path}[{index}]")


def _validate_topology(value: object) -> None:
    topology = _mapping(value, "topology")
    _nonempty_string(topology.get("kubernetes_version"), "topology.kubernetes_version")
    _nonempty_string(topology.get("eks_version"), "topology.eks_version")
    instance_types = _mapping(topology.get("instance_types"), "topology.instance_types")
    node_counts = _mapping(topology.get("node_counts"), "topology.node_counts")
    for pool in ("loadgen", "platform"):
        _nonempty_string(instance_types.get(pool), f"topology.instance_types.{pool}")
        _positive_number(node_counts.get(pool), f"topology.node_counts.{pool}")


def _validate_experiment(value: object, path: str, require_target_replicas: bool) -> None:
    experiment = _mapping(value, path)
    scenario = _mapping(experiment.get("scenario"), f"{path}.scenario")
    measured = _mapping(experiment.get("measured"), f"{path}.measured")
    _nonempty_string(scenario.get("target"), f"{path}.scenario.target")
    if scenario.get("model") != "constant-rate":
        raise RedactionError(f"{path}.scenario.model must be constant-rate")
    _nonempty_string(scenario.get("duration"), f"{path}.scenario.duration")
    for field in ("peak_rps", "p50_ms", "p95_ms", "p99_ms", "peak_workers"):
        _positive_number(measured.get(field), f"{path}.measured.{field}")
    if require_target_replicas:
        _positive_number(measured.get("target_replicas"), f"{path}.measured.target_replicas")
    for field in ("error_rate", "dropped_samples", "unmet_demand"):
        _zero_counter(measured.get(field), f"{path}.measured.{field}")
    _validate_clean_evidence(experiment, path)


def _validate_teardown(value: object) -> None:
    teardown = _mapping(value, "teardown")
    if teardown.get("status") != "PASS":
        raise RedactionError("teardown.status must be PASS")
    if teardown.get("verified_clean") is not True:
        raise RedactionError("teardown.verified_clean must be true")


def _validate_billing_attestation(value: object, run_id: str) -> None:
    attestation = _mapping(value, "billing_attestation")
    if attestation.get("type") != "human" or attestation.get("confirmed") is not True:
        raise RedactionError("billing_attestation must be a confirmed human attestation")
    checked_on = _nonempty_string(attestation.get("checked_on"), "billing_attestation.checked_on")
    _nonempty_string(attestation.get("statement"), "billing_attestation.statement")
    try:
        run_day = date.fromisoformat(
            f"{run_id[0:4]}-{run_id[4:6]}-{run_id[6:8]}"
        )
        checked_day = date.fromisoformat(checked_on)
    except ValueError as exc:
        raise RedactionError("run_id and billing_attestation.checked_on must contain ISO dates") from exc
    if checked_day != run_day + timedelta(days=1):
        raise RedactionError("billing_attestation.checked_on must be the day after the benchmark run")


def validate_metrics(value: dict[str, object]) -> None:
    """Validate the curated evidence schema and clean worker result."""

    if not isinstance(value, dict):
        raise RedactionError("metrics.json must contain a JSON object")
    for field in REQUIRED_FIELDS:
        _required_field(value, field)
    run_id = _nonempty_string(value["run_id"], "run_id")
    if not re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-fA-F]{7,40}", run_id):
        raise RedactionError("run_id must start with a UTC date and include a commit suffix")
    if not isinstance(value["git_sha"], str) or not re.fullmatch(r"[0-9a-fA-F]{40}", value["git_sha"]):
        raise RedactionError("git_sha must be the full 40-character commit SHA")
    _nonempty_string(value["region"], "region")
    _validate_topology(value["topology"])
    _validate_experiment(value["worker_experiment"], "worker_experiment", False)
    _validate_experiment(value["target_hpa_experiment"], "target_hpa_experiment", True)
    _validate_teardown(value["teardown"])
    _validate_billing_attestation(value["billing_attestation"], run_id)


# Curated evidence is published only from one exact owned raw run. The shell
# passes the ownership state it already verified; this repeats the binding in
# the redactor so copied metrics cannot be published under another run.
def validate_bundle_identity(input_dir: Path, metrics: dict[str, object], expected: argparse.Namespace) -> None:
    expected_values = {
        "run_id": expected.expected_run_id,
        "git_sha": expected.expected_git_sha,
        "region": expected.expected_region,
    }
    if input_dir.name != expected.expected_run_id:
        raise RedactionError("input directory name does not match the expected run ID")
    owned = _load_json(input_dir / "owned-run.json")
    cleanup = _load_json(input_dir / "cleanup-report.json")
    if not isinstance(owned, dict):
        raise RedactionError("owned-run.json must contain a JSON object")
    if not isinstance(cleanup, dict):
        raise RedactionError("cleanup-report.json must contain a JSON object")
    for field, value in expected_values.items():
        if owned.get(field) != value:
            raise RedactionError(f"owned-run.json {field} does not match the expected run")
        if metrics.get(field) != value:
            raise RedactionError(f"metrics.json {field} does not match the expected run")
    cleanup_expected = {
        "account": expected.expected_cleanup_account,
        "region": expected.expected_cleanup_region,
        "cluster": expected.expected_cleanup_cluster,
    }
    if cleanup.get("status") != "PASS":
        raise RedactionError("cleanup-report.json status must be PASS")
    for field, value in cleanup_expected.items():
        if owned.get(field) != value:
            raise RedactionError(f"owned-run.json {field} does not match the expected cleanup identity")
        if cleanup.get(field) != value:
            raise RedactionError(f"cleanup-report.json {field} does not match the expected cleanup identity")


def _format_value(value: object) -> str:
    if isinstance(value, dict):
        return ", ".join(f"{key}={_format_value(value[key])}" for key in sorted(value))
    if isinstance(value, list):
        return ", ".join(_format_value(item) for item in value)
    if isinstance(value, float):
        return str(value)
    return str(value)


def _field(value: object, *keys: str, default: object = "unknown") -> object:
    current = value
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _experiment_lines(title: str, experiment: object) -> list[str]:
    scenario = _field(experiment, "scenario", default={})
    measured = _field(experiment, "measured", default={})
    target = _field(scenario, "target")
    model = _field(scenario, "model")
    duration = _field(scenario, "duration", default=_field(measured, "duration"))
    peak_rps = _field(measured, "peak_rps")
    p50 = _field(measured, "p50_ms")
    p95 = _field(measured, "p95_ms")
    p99 = _field(measured, "p99_ms")
    result_fields = (
        "peak_workers",
        "target_replicas",
        "error_rate",
        "dropped_samples",
        "unmet_demand",
    )
    measured_summary = ", ".join(
        f"{key}={_field(measured, key)}" for key in result_fields if _field(measured, key) != "unknown"
    )
    return [
        f"### {title}",
        f"- Scenario: target={target}; model={model}; duration={duration}",
        f"- Peak RPS: {peak_rps}",
        f"- Latency: p50={p50}ms p95={p95}ms p99={p99}ms",
        f"- Measured results: {measured_summary}",
    ]


def _render_report(metrics: dict[str, Any]) -> str:
    topology = metrics["topology"]
    worker = metrics["worker_experiment"]
    hpa = metrics["target_hpa_experiment"]
    billing = metrics["billing_attestation"]
    lines = [
        "# EKS Benchmark Evidence",
        "",
        f"- Run ID: {metrics['run_id']}",
        f"- Git SHA: {metrics['git_sha']}",
        f"- Region: {metrics['region']}",
        f"- Kubernetes/EKS: {_field(topology, 'kubernetes_version')} / {_field(topology, 'eks_version')}",
        f"- Instance types: {_format_value(_field(topology, 'instance_types', default={}))}",
        f"- Node counts: {_format_value(_field(topology, 'node_counts', default={}))}",
        "",
        *_experiment_lines("Worker-fleet experiment", worker),
        "",
        *_experiment_lines("Target-HPA experiment", hpa),
        "",
        f"- Teardown: {_format_value(metrics['teardown'])}",
        f"- Billing attestation: {_field(billing, 'type')} ({_field(billing, 'checked_on')})",
        "",
    ]
    return "\n".join(lines)


def _assert_no_sensitive_text(value: str, label: str) -> None:
    scan_value = GIT_SHA_RE.sub("", value)
    scan_value = re.sub(
        r"<(?:aws-account-id|aws-arn|ip-address|email-address|redacted-credential(?:-\d+)?)>",
        "",
        scan_value,
    )
    checks = (
        (ARN_RE, "AWS ARN"),
        (ACCESS_KEY_RE, "access key"),
        (EMAIL_RE, "email address"),
        (IPV4_RE, "IPv4 address"),
        (ACCOUNT_RE, "AWS account ID"),
        (CREDENTIAL_ASSIGNMENT_RE, "credential assignment"),
        (CREDENTIAL_NAME_RE, "credential field"),
    )
    for pattern, description in checks:
        if pattern.search(scan_value):
            raise RedactionError(f"{label} still contains a sensitive {description}")
    if _contains_ipv6(scan_value):
        raise RedactionError(f"{label} still contains a sensitive IPv6 address")


def _assert_no_sensitive_json(value: object, label: str = "metrics.json") -> None:
    """Scan JSON strings and keys while leaving numeric metric values intact."""

    if isinstance(value, dict):
        for key, child in value.items():
            _assert_no_sensitive_text(str(key), label)
            _assert_no_sensitive_json(child, label)
    elif isinstance(value, list):
        for child in value:
            _assert_no_sensitive_json(child, label)
    elif isinstance(value, str):
        _assert_no_sensitive_text(value, label)


def _load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise RedactionError(f"missing required input: {path.name}") from exc
    except json.JSONDecodeError as exc:
        raise RedactionError(f"invalid JSON in {path.name}: {exc}") from exc


def _validate_dashboard_review(input_dir: Path, dashboard_path: Path) -> None:
    """Require a human approval tied to the exact dashboard bytes before copying it."""

    review = _load_json(input_dir / "dashboard-review.json")
    if not isinstance(review, dict):
        raise RedactionError("dashboard-review.json must contain a JSON object")
    if review.get("approved") is not True:
        raise RedactionError("dashboard-review.json must set approved to true")
    _nonempty_string(review.get("reviewed_by"), "dashboard-review.json.reviewed_by")
    reviewed_on = _nonempty_string(review.get("reviewed_on"), "dashboard-review.json.reviewed_on")
    try:
        date.fromisoformat(reviewed_on)
    except ValueError as exc:
        raise RedactionError("dashboard-review.json.reviewed_on must be an ISO date") from exc
    expected_hash = _nonempty_string(
        review.get("dashboard_sha256"), "dashboard-review.json.dashboard_sha256"
    ).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        raise RedactionError("dashboard-review.json.dashboard_sha256 must be a SHA-256 hex digest")
    actual_hash = hashlib.sha256(dashboard_path.read_bytes()).hexdigest()
    if not hmac.compare_digest(expected_hash, actual_hash):
        raise RedactionError("dashboard-review.json SHA-256 does not match dashboard.png")


def publish_bundle(input_dir: Path, output_dir: Path, expected: argparse.Namespace) -> None:
    """Validate and atomically publish a sanitized evidence bundle."""

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    if not input_dir.is_dir():
        raise RedactionError(f"input directory does not exist: {input_dir}")
    if output_dir.exists():
        raise RedactionError(f"refusing to overwrite existing output: {output_dir}")

    metrics = _load_json(input_dir / "metrics.json")
    report_data = _load_json(input_dir / "report.json")
    if not isinstance(report_data, dict):
        raise RedactionError("report.json must contain a JSON object")
    dashboard_path = input_dir / "dashboard.png"
    if not dashboard_path.is_file():
        raise RedactionError("missing required input: dashboard.png")
    validate_metrics(metrics)
    validate_bundle_identity(input_dir, metrics, expected)
    _validate_dashboard_review(input_dir, dashboard_path)

    sanitized_metrics = redact_value(metrics)
    if not isinstance(sanitized_metrics, dict):
        raise RedactionError("redacted metrics are not a JSON object")
    report = _render_report(sanitized_metrics)
    serialized_metrics = json.dumps(
        sanitized_metrics,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    _assert_no_sensitive_json(json.loads(serialized_metrics))
    _assert_no_sensitive_text(report, "report.md")

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=str(output_dir.parent))
    )
    try:
        (temporary_dir / "metrics.json").write_text(serialized_metrics, encoding="utf-8")
        (temporary_dir / "report.md").write_text(report, encoding="utf-8")
        shutil.copyfile(dashboard_path, temporary_dir / "dashboard.png")
        if output_dir.exists():
            raise RedactionError(f"refusing to overwrite existing output: {output_dir}")
        os.rename(temporary_dir, output_dir)
    except Exception:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, dest="input_dir")
    parser.add_argument("--output", required=True, type=Path, dest="output_dir")
    parser.add_argument("--expected-run-id", required=True)
    parser.add_argument("--expected-git-sha", required=True)
    parser.add_argument("--expected-region", required=True)
    parser.add_argument("--expected-cleanup-account", required=True)
    parser.add_argument("--expected-cleanup-region", required=True)
    parser.add_argument("--expected-cleanup-cluster", required=True)
    args = parser.parse_args(argv)
    try:
        publish_bundle(args.input_dir, args.output_dir, args)
    except (OSError, RedactionError, TypeError) as exc:
        print(f"eks-redact: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
