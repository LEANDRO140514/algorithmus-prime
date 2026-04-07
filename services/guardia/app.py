"""GUARDIA microservice.

Security Gate formal para el pipeline multi-agente.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import socket
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from flask import Flask, jsonify, request
from jsonschema import ValidationError, validate

app = Flask(__name__)

AGENT_NAME = "guardia"
SCHEMA_VERSION = "v1"
DEFAULT_FAIL_SEVERITY = {"critical", "high"}
DEFAULT_MAX_MEDIUM = 10
DEFAULT_REQUIRE_NO_SECRETS = True
DEFAULT_TOKEN_TTL_MINUTES = 30
DEFAULT_MAX_AUDIT_DURATION_SECONDS = 120
DEFAULT_ALLOWED_SCAN_BASE = "/workspace"
DEFAULT_NETWORK_TIMEOUT_SECONDS = 7

SEVERITY_WEIGHT = {
    "critical": 35,
    "high": 20,
    "medium": 8,
    "low": 3,
    "unknown": 5,
}

# Regex base para detectar posibles secretos hardcoded.
SECRET_PATTERNS = {
    "aws_access_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "github_token": re.compile(r"\bghp_[A-Za-z0-9]{36}\b"),
    "slack_token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "generic_assignment": re.compile(
        r"(?i)(api[_-]?key|secret|token|password)\s*[:=]\s*['\"][^'\"]{8,}['\"]"
    ),
}

REQUEST_SCHEMA = {
    "type": "object",
    "required": [
        "schema_version",
        "pipeline_id",
        "execution_id",
        "correlation_id",
        "event",
        "scan_target",
    ],
    "properties": {
        "schema_version": {"type": "string", "const": SCHEMA_VERSION},
        "execution_id": {"type": "string", "minLength": 1},
        "correlation_id": {"type": "string", "minLength": 1},
        "pipeline_id": {"type": "string", "minLength": 1},
        "event": {
            "type": "object",
            "required": [
                "event_type",
                "repository",
                "branch",
                "author",
                "timestamp",
                "summary",
                "changed_files",
                "metadata",
            ],
            "properties": {
                "event_type": {"type": "string", "minLength": 1},
                "repository": {"type": "string", "minLength": 1},
                "branch": {"type": "string", "minLength": 1},
                "author": {"type": "string", "minLength": 1},
                "timestamp": {"type": "string", "minLength": 1},
                "summary": {"type": "string", "minLength": 1},
                "changed_files": {"type": "array", "items": {"type": "string"}},
                "metadata": {"type": "object"},
            },
            "additionalProperties": True,
        },
        "scan_target": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string", "minLength": 1},
                "diff_text": {"type": "string", "minLength": 1},
                "manifest_paths": {
                    "type": "array",
                    "items": {"type": "string", "minItems": 1},
                    "minItems": 1,
                },
            },
            "additionalProperties": True,
        },
        "policy": {
            "type": "object",
            "properties": {
                "fail_on_severity": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["critical", "high", "medium", "low", "unknown"]},
                },
                "max_medium_findings": {"type": "integer", "minimum": 0},
                "require_no_secrets": {"type": "boolean"},
            },
            "additionalProperties": True,
        },
        "network_checks": {
            "type": "object",
            "properties": {
                "target_domain": {"type": "string", "minLength": 1},
                "deploy_url": {"type": "string", "minLength": 1},
            },
            "additionalProperties": True,
        },
    },
    "additionalProperties": True,
}

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(message)s")
logger = logging.getLogger("guardia")


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def isoformat_z(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def with_execution_context(payload: dict[str, Any], headers: Any) -> dict[str, Any]:
    """Completa execution context sin romper compatibilidad."""
    normalized = dict(payload)
    execution_id = normalized.get("execution_id") or str(uuid.uuid4())
    correlation_id = normalized.get("correlation_id") or headers.get("X-Correlation-ID") or execution_id
    normalized["execution_id"] = execution_id
    normalized["correlation_id"] = correlation_id
    normalized["schema_version"] = normalized.get("schema_version") or SCHEMA_VERSION
    return normalized


def log_event(execution_id: str, correlation_id: str, event: str, **kwargs: Any) -> None:
    """Logs estructurados serializables para Cronista."""
    payload = {
        "ts": isoformat_z(utc_now()),
        "execution_id": execution_id,
        "correlation_id": correlation_id,
        "schema_version": SCHEMA_VERSION,
        "agent": AGENT_NAME,
        "event": event,
        **kwargs,
    }
    logger.info(json.dumps(payload, ensure_ascii=False))


def error_response(
    code: str,
    message: str,
    status_code: int,
    execution_id: str,
    correlation_id: str,
    details: dict[str, Any] | None = None,
) -> tuple[Any, int]:
    """Error estructurado para que Orchestrator interprete BLOCK."""
    body = {
        "agent": AGENT_NAME,
        "schema_version": SCHEMA_VERSION,
        "status": "error",
        "execution_id": execution_id,
        "correlation_id": correlation_id,
        "deployment_allowed": False,
        "security_status": "critical",
        "risk_score": 100,
        "decision_reason": f"internal_error:{code}",
        "deployment_token": None,
        "policy_applied": build_policy(None),
        "report": {
            "dependency_scan": {"findings": [], "summary": {"total": 0, "by_severity": {}}, "skipped_reason": "error"},
            "secret_scan": {"findings": [], "recommended_env_vars": [], "skipped_reason": "error"},
            "network_checks": {"results": None, "skipped_reason": "error"},
        },
        "artifact_path": None,
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
        },
        "started_at": isoformat_z(utc_now()),
        "finished_at": isoformat_z(utc_now()),
        "duration_ms": 0,
        "trace": {"request_id": hashlib.sha1(str(time.time()).encode()).hexdigest()[:12], "timings_ms": {}},
    }
    return jsonify(body), status_code


def run_command(
    cmd: list[str],
    cwd: str | None = None,
    timeout: int = 90,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Ejecuta comando con timeout local y respeto por timeout global."""
    started = time.time()
    if deadline is not None:
        remaining = deadline - time.time()
        if remaining <= 0:
            return {
                "ok": False,
                "returncode": None,
                "stdout": "",
                "stderr": "global_timeout_reached",
                "duration_ms": 0,
            }
        timeout = max(1, min(timeout, int(remaining)))

    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "duration_ms": int((time.time() - started) * 1000),
        }
    except FileNotFoundError:
        return {
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": "tool_not_installed",
            "duration_ms": int((time.time() - started) * 1000),
        }
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": "timeout",
            "duration_ms": int((time.time() - started) * 1000),
        }


def normalize_severity(raw: str | None) -> str:
    if not raw:
        return "unknown"
    value = raw.strip().lower()
    if value in {"moderate", "med"}:
        return "medium"
    if value in {"info", "negligible"}:
        return "low"
    return value


def make_finding(
    finding_type: str,
    severity: str,
    message: str,
    recommendation: str,
    *,
    location: str | None = None,
    source_tool: str | None = None,
    legacy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Estructura formal de findings con compatibilidad legacy."""
    record: dict[str, Any] = {
        "type": finding_type,
        "severity": normalize_severity(severity),
        "message": message,
        "location": location,
        "recommendation": recommendation,
    }
    if source_tool:
        record["source_tool"] = source_tool
    if legacy:
        record.update(legacy)
    return record


def parse_npm_audit(stdout: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    try:
        payload = json.loads(stdout or "{}")
    except json.JSONDecodeError:
        return findings

    vulnerabilities = payload.get("vulnerabilities", {})
    if isinstance(vulnerabilities, dict):
        for pkg, vuln in vulnerabilities.items():
            via = vuln.get("via", [])
            if not isinstance(via, list):
                via = [via]
            for item in via:
                if isinstance(item, dict):
                    fid = str(item.get("source") or item.get("url") or pkg)
                    findings.append(
                        make_finding(
                            "dependency",
                            item.get("severity") or vuln.get("severity") or "unknown",
                            item.get("title") or f"Vulnerability in {pkg}",
                            "Update dependency to a patched version.",
                            location=pkg,
                            source_tool="npm-audit",
                            legacy={
                                "id": fid,
                                "title": item.get("title") or f"Vulnerability in {pkg}",
                                "package": pkg,
                                "version": vuln.get("range"),
                                "fixed_in": (vuln.get("fixAvailable") or {}).get("name")
                                if isinstance(vuln.get("fixAvailable"), dict)
                                else None,
                            },
                        )
                    )
    return findings


def parse_pip_audit(stdout: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    try:
        payload = json.loads(stdout or "[]")
    except json.JSONDecodeError:
        return findings

    for dep in payload:
        pkg = dep.get("name")
        version = dep.get("version")
        for vuln in dep.get("vulns", []):
            findings.append(
                make_finding(
                    "dependency",
                    vuln.get("severity") or "unknown",
                    vuln.get("description") or f"Vulnerability in {pkg}",
                    "Upgrade package to fixed versions listed by pip-audit.",
                    location=pkg,
                    source_tool="pip-audit",
                    legacy={
                        "id": vuln.get("id") or f"{pkg}-unknown",
                        "title": vuln.get("description") or f"Vulnerability in {pkg}",
                        "package": pkg,
                        "version": version,
                        "fixed_in": ", ".join(vuln.get("fix_versions", []) or []),
                    },
                )
            )
    return findings


def parse_bandit(stdout: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    try:
        payload = json.loads(stdout or "{}")
    except json.JSONDecodeError:
        return findings

    for item in payload.get("results", []):
        findings.append(
            make_finding(
                "code",
                item.get("issue_severity") or "unknown",
                item.get("issue_text") or "Bandit issue",
                "Refactor insecure pattern detected by Bandit.",
                location=item.get("filename"),
                source_tool="bandit",
                legacy={
                    "id": item.get("test_id") or "bandit",
                    "title": item.get("issue_text") or "Bandit issue",
                    "package": item.get("filename"),
                    "version": None,
                    "fixed_in": None,
                },
            )
        )
    return findings


def list_scan_files(repo_path: Path, changed_files: list[str] | None) -> list[Path]:
    if changed_files:
        files = []
        for entry in changed_files:
            candidate = (repo_path / entry).resolve()
            if repo_path in candidate.parents and candidate.is_file():
                files.append(candidate)
        return files

    return [
        p
        for p in repo_path.rglob("*")
        if p.is_file()
        and p.suffix.lower()
        in {
            ".py",
            ".js",
            ".ts",
            ".env",
            ".yml",
            ".yaml",
            ".json",
            ".txt",
            ".ini",
            ".toml",
        }
    ]


def scan_secrets_in_text(text: str, location: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for secret_type, pattern in SECRET_PATTERNS.items():
        for match in pattern.finditer(text):
            findings.append(
                make_finding(
                    "secret",
                    "high",
                    f"Potential hardcoded secret detected ({secret_type}).",
                    "Move secret to environment variables (Coolify) and rotate leaked value.",
                    location=location,
                    source_tool="regex-secret-scan",
                    legacy={
                        "id": f"secret-{secret_type}",
                        "title": secret_type,
                        "package": None,
                        "version": None,
                        "fixed_in": None,
                        "match_preview": match.group(0)[:10] + "***",
                    },
                )
            )
    return findings


def infer_env_var_name(secret_type: str) -> str:
    mapping = {
        "secret-aws_access_key": "AWS_ACCESS_KEY_ID",
        "secret-github_token": "GITHUB_TOKEN",
        "secret-slack_token": "SLACK_BOT_TOKEN",
        "secret-private_key": "APP_PRIVATE_KEY",
        "secret-generic_assignment": "APP_SECRET",
    }
    return mapping.get(secret_type, "APP_SECRET")


def run_dependency_scans(
    repo_path: Path | None,
    manifest_paths: list[str] | None,
    deadline: float | None = None,
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    skipped: list[str] = []

    manifests = [Path(p) for p in (manifest_paths or [])]
    has_node = False
    has_python = False

    if repo_path:
        has_node = (repo_path / "package-lock.json").exists() or (repo_path / "package.json").exists()
        has_python = (repo_path / "requirements.txt").exists() or (repo_path / "pyproject.toml").exists()

    for manifest in manifests:
        if manifest.name in {"package.json", "package-lock.json"}:
            has_node = True
        if manifest.name in {"requirements.txt", "pyproject.toml"}:
            has_python = True

    if has_node and repo_path and (repo_path / "package-lock.json").exists():
        result = run_command(["npm", "audit", "--json"], cwd=str(repo_path), deadline=deadline)
        if result["stdout"]:
            findings.extend(parse_npm_audit(result["stdout"]))
        if not result["ok"] and not result["stdout"]:
            skipped.append(f"npm_audit:{result['stderr']}")
    else:
        skipped.append("npm_audit:missing_package_lock_or_repo_path")

    if has_python and repo_path:
        result = run_command(["pip-audit", "-f", "json"], cwd=str(repo_path), deadline=deadline)
        if result["stdout"]:
            findings.extend(parse_pip_audit(result["stdout"]))
        if not result["ok"] and not result["stdout"]:
            skipped.append(f"pip_audit:{result['stderr']}")

        bandit_res = run_command(["bandit", "-r", ".", "-f", "json", "-q"], cwd=str(repo_path), deadline=deadline)
        if bandit_res["stdout"]:
            findings.extend(parse_bandit(bandit_res["stdout"]))
        if not bandit_res["ok"] and not bandit_res["stdout"]:
            skipped.append(f"bandit:{bandit_res['stderr']}")
    else:
        skipped.append("python_scans:missing_requirements_or_repo_path")

    summary = {
        "total": len(findings),
        "by_severity": {
            sev: sum(1 for item in findings if item["severity"] == sev)
            for sev in ["critical", "high", "medium", "low", "unknown"]
        },
    }

    return {
        "findings": findings,
        "summary": summary,
        "skipped_reason": "; ".join(skipped) if skipped else None,
    }


def run_secret_scan(scan_target: dict[str, Any], changed_files: list[str] | None) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []

    diff_text = scan_target.get("diff_text")
    repo_path = scan_target.get("repo_path")

    if diff_text:
        findings.extend(scan_secrets_in_text(diff_text, "diff_text"))
    elif repo_path:
        base = Path(repo_path).resolve()
        if base.exists() and base.is_dir():
            for file_path in list_scan_files(base, changed_files):
                try:
                    text = file_path.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                findings.extend(scan_secrets_in_text(text, str(file_path.relative_to(base))))
        else:
            return {"findings": [], "recommended_env_vars": [], "skipped_reason": "repo_path_not_found"}
    else:
        return {"findings": [], "recommended_env_vars": [], "skipped_reason": "no_diff_or_repo_path"}

    recommended = sorted({infer_env_var_name(item.get("id", "")) for item in findings})
    return {
        "findings": findings,
        "recommended_env_vars": recommended,
        "skipped_reason": None,
    }


def is_public_host_allowed(host: str) -> tuple[bool, str | None]:
    allowed_hosts = [h.strip().lower() for h in os.getenv("ALLOWED_NETWORK_HOSTS", "").split(",") if h.strip()]
    host_lower = host.lower()
    if allowed_hosts and host_lower not in allowed_hosts and not any(host_lower.endswith(f".{allowed}") for allowed in allowed_hosts):
        return False, "host_not_in_whitelist"

    try:
        ip = ipaddress.ip_address(socket.gethostbyname(host))
    except OSError:
        return False, "dns_resolution_failed"

    if ip.is_loopback or ip.is_private or ip.is_link_local or ip.is_multicast or ip.is_unspecified:
        return False, "host_resolves_to_non_public_ip"

    return True, None


def validate_deploy_url(url: str) -> tuple[bool, str | None]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False, "invalid_scheme"
    if not parsed.hostname:
        return False, "missing_hostname"
    return True, None


def run_network_checks(payload: dict[str, Any], deadline: float | None = None) -> dict[str, Any]:
    net = payload.get("network_checks") or {}
    target_domain = net.get("target_domain")
    deploy_url = net.get("deploy_url")

    if not target_domain and not deploy_url:
        return {"findings": [], "results": None, "skipped_reason": "network_checks_not_provided"}

    result: dict[str, Any] = {}
    findings: list[dict[str, Any]] = []
    host = target_domain
    timeout_seconds = int(os.getenv("NETWORK_CHECK_TIMEOUT_SECONDS", str(DEFAULT_NETWORK_TIMEOUT_SECONDS)))

    if deploy_url:
        valid, reason = validate_deploy_url(deploy_url)
        if not valid:
            findings.append(
                make_finding(
                    "network",
                    "high",
                    f"Invalid deploy_url: {reason}",
                    "Provide a valid http(s) deploy_url.",
                    location="network_checks.deploy_url",
                    source_tool="network-check",
                )
            )
            return {"findings": findings, "results": None, "skipped_reason": "invalid_deploy_url"}
        parsed = urlparse(deploy_url)
        host = parsed.hostname
        result["scheme"] = parsed.scheme
        result["deploy_url"] = deploy_url

    if host:
        allowed, reason = is_public_host_allowed(host)
        if not allowed:
            findings.append(
                make_finding(
                    "network",
                    "high",
                    f"Host blocked by SSRF policy: {reason}",
                    "Use a publicly routable host present in ALLOWED_NETWORK_HOSTS.",
                    location="network_checks",
                    source_tool="network-check",
                )
            )
            return {"findings": findings, "results": {"host": host, "allowed": False}, "skipped_reason": reason}

        try:
            socket.gethostbyname(host)
            result["dns_resolves"] = True
        except OSError:
            result["dns_resolves"] = False

    if deploy_url:
        if deadline is not None and deadline <= time.time():
            return {"findings": findings, "results": result, "skipped_reason": "global_timeout_reached"}
        try:
            req = Request(deploy_url, method="HEAD")
            with urlopen(req, timeout=timeout_seconds) as response:  # nosec B310
                result["http_status"] = response.status
            result["reachable"] = True
        except Exception:
            result["reachable"] = False

    return {"findings": findings, "results": result, "skipped_reason": None}


def compute_risk_score(dependency_findings: list[dict[str, Any]], secret_findings: list[dict[str, Any]], network_findings: list[dict[str, Any]]) -> int:
    """Risk model formal 0-100.

    Fórmula:
    1) sumar pesos por severidad de findings de dependencia + red.
    2) sumar +20 por secreto (tope +40).
    3) clamp entre 0 y 100.
    """
    score = 0
    for finding in dependency_findings + network_findings:
        score += SEVERITY_WEIGHT.get(finding.get("severity", "unknown"), 5)
    score += min(40, len(secret_findings) * 20)
    return max(0, min(100, score))


def classify_security_status(risk_score: int) -> str:
    if risk_score >= 70:
        return "critical"
    if risk_score >= 35:
        return "warning"
    return "good"


def build_policy(policy_input: dict[str, Any] | None) -> dict[str, Any]:
    incoming = policy_input or {}
    return {
        "fail_on_severity": incoming.get("fail_on_severity", list(DEFAULT_FAIL_SEVERITY)),
        "max_medium_findings": int(incoming.get("max_medium_findings", DEFAULT_MAX_MEDIUM)),
        "require_no_secrets": bool(incoming.get("require_no_secrets", DEFAULT_REQUIRE_NO_SECRETS)),
    }


def evaluate_gate(
    dependency_findings: list[dict[str, Any]],
    secret_findings: list[dict[str, Any]],
    network_findings: list[dict[str, Any]],
    policy_applied: dict[str, Any],
) -> tuple[bool, str]:
    fail_on = set(policy_applied["fail_on_severity"])
    max_medium = policy_applied["max_medium_findings"]
    require_no_secrets = policy_applied["require_no_secrets"]

    for item in dependency_findings + network_findings:
        if item["severity"] in fail_on:
            return False, f"blocked_by_severity:{item['severity']}"

    medium_count = sum(1 for item in dependency_findings + network_findings if item["severity"] == "medium")
    if medium_count > max_medium:
        return False, f"blocked_by_medium_count:{medium_count}>{max_medium}"

    if require_no_secrets and len(secret_findings) > 0:
        return False, "blocked_by_secret_detection"

    return True, "policy_passed"


def build_token(
    pipeline_id: str,
    execution_id: str,
    correlation_id: str,
    ttl_minutes: int = DEFAULT_TOKEN_TTL_MINUTES,
) -> tuple[dict[str, str] | None, dt.datetime | None]:
    """Token firmado con formato futuro-compatible (objeto)."""
    secret = os.getenv("DEPLOYMENT_TOKEN_SECRET")
    if not secret:
        return None, None

    now = utc_now()
    exp = now + dt.timedelta(minutes=ttl_minutes)
    header = {"typ": "guardia-deployment-token", "alg": "HS256", "v": 1}
    payload = {
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "correlation_id": correlation_id,
        "agent": AGENT_NAME,
        "schema_version": SCHEMA_VERSION,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    header_b64 = base64.urlsafe_b64encode(header_bytes).rstrip(b"=")
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=")
    signing_input = header_b64 + b"." + payload_b64
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    token = ".".join(
        [
            header_b64.decode("utf-8"),
            payload_b64.decode("utf-8"),
            base64.urlsafe_b64encode(signature).rstrip(b"=").decode("utf-8"),
        ]
    )
    return {"token": token, "expires_at": isoformat_z(exp)}, exp


def write_artifact(pipeline_id: str, response: dict[str, Any]) -> str:
    artifact_dir = Path("artifacts/guardia")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{pipeline_id}_security.md"

    lines = [
        f"# GUARDIA Security Report - {pipeline_id}",
        "",
        f"- execution_id: `{response['execution_id']}`",
        f"- correlation_id: `{response['correlation_id']}`",
        f"- deployment_allowed: **{response['deployment_allowed']}**",
        f"- security_status: **{response['security_status']}**",
        f"- risk_score: **{response['risk_score']}/100**",
        f"- decision_reason: `{response['decision_reason']}`",
        "",
        "## Policy Applied",
        f"```json\n{json.dumps(response['policy_applied'], indent=2)}\n```",
        "",
        "## Dependency Scan",
        f"```json\n{json.dumps(response['report']['dependency_scan'], indent=2)}\n```",
        "",
        "## Secret Scan",
        f"```json\n{json.dumps(response['report']['secret_scan'], indent=2)}\n```",
        "",
        "## Network Checks",
        f"```json\n{json.dumps(response['report']['network_checks'], indent=2)}\n```",
    ]
    artifact_path.write_text("\n".join(lines), encoding="utf-8")
    return str(artifact_path)


def validate_repo_path(repo_path: str) -> tuple[bool, str | None]:
    allowed_base = Path(os.getenv("ALLOWED_SCAN_BASE", DEFAULT_ALLOWED_SCAN_BASE)).resolve()
    candidate = Path(repo_path).resolve()
    if candidate == allowed_base or allowed_base in candidate.parents:
        return True, None
    return False, f"repo_path_outside_allowed_base:{allowed_base}"


def validate_payload(payload: dict[str, Any]) -> str | None:
    try:
        validate(instance=payload, schema=REQUEST_SCHEMA)
    except ValidationError as exc:
        return f"invalid_payload:{exc.message}"

    scan_target = payload.get("scan_target", {})
    options = ["repo_path", "diff_text", "manifest_paths"]
    present = [opt for opt in options if scan_target.get(opt)]
    if len(present) != 1:
        return "scan_target must include exactly one of repo_path, diff_text, manifest_paths"

    repo_path = scan_target.get("repo_path")
    if repo_path:
        ok, reason = validate_repo_path(repo_path)
        if not ok:
            return reason

    return None


EXECUTE_ACTION_VALIDATE_SECURITY = "validate_security"


def _execute_meta_execution_id(meta: Any) -> Any:
    if isinstance(meta, dict):
        return meta.get("execution_id")
    return None


def _execute_validate_security_fields(payload: Any) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
    """Validate repository, branch, event_type; return (data, errors). errors non-empty => data is None."""
    if not isinstance(payload, dict):
        return None, [
            {"code": "VALIDATION_ERROR", "message": "payload must be a JSON object"},
        ]

    fields = {}
    for key in ("repository", "branch", "event_type"):
        val = payload.get(key)
        if not isinstance(val, str):
            return None, [
                {
                    "code": "VALIDATION_ERROR",
                    "message": f"{key} must be a non-empty string",
                },
            ]
        trimmed = val.strip()
        if not trimmed:
            return None, [
                {
                    "code": "VALIDATION_ERROR",
                    "message": f"{key} must be a non-empty string",
                },
            ]
        fields[key] = trimmed

    if fields["branch"] == "main":
        return {"allow": False, "reason": "Protected branch requires approval"}, []
    return {"allow": True, "reason": "Allowed"}, []


def _execute_response(
    *,
    status: str,
    action: str,
    data: dict[str, Any] | None,
    errors: list[dict[str, str]],
) -> tuple[Any, int]:
    body: dict[str, Any] = {
        "status": status,
        "agent": AGENT_NAME,
        "action": action,
        "data": data,
        "errors": errors,
    }
    http_status = 200 if status == "success" else 400
    return jsonify(body), http_status


@app.get("/health")
def health() -> Any:
    return jsonify({"status": "ok", "agent": AGENT_NAME}), 200


@app.post("/execute")
def execute() -> Any:
    raw = request.get_json(silent=True)
    meta = raw.get("meta") if isinstance(raw, dict) else None
    execution_id = _execute_meta_execution_id(meta)
    action_raw = raw.get("action") if isinstance(raw, dict) else None
    action_str = action_raw if isinstance(action_raw, str) else None

    print(
        {
            "event": "guardia_execute",
            "action": action_str,
            "execution_id": execution_id,
        }
    )

    def fail(action: str, errors: list[dict[str, str]]) -> tuple[Any, int]:
        err_summary = errors[0]["message"] if errors else "error"
        print({"event": "guardia_execute_error", "execution_id": execution_id, "error": err_summary})
        return _execute_response(status="error", action=action, data=None, errors=errors)

    if not isinstance(raw, dict):
        return fail(
            EXECUTE_ACTION_VALIDATE_SECURITY,
            [{"code": "VALIDATION_ERROR", "message": "Request body must be a JSON object"}],
        )

    if not isinstance(action_raw, str) or not action_raw.strip():
        return fail(
            EXECUTE_ACTION_VALIDATE_SECURITY,
            [{"code": "VALIDATION_ERROR", "message": "action must be a non-empty string"}],
        )

    action = action_raw.strip()
    if action != EXECUTE_ACTION_VALIDATE_SECURITY:
        return fail(
            action,
            [{"code": "UNSUPPORTED_ACTION", "message": f"Unsupported action: {action}"}],
        )

    payload = raw.get("payload")
    data, val_errors = _execute_validate_security_fields(payload)
    if val_errors:
        return fail(EXECUTE_ACTION_VALIDATE_SECURITY, val_errors)

    assert data is not None

    print({"event": "guardia_execute_success", "execution_id": execution_id})
    return _execute_response(
        status="success",
        action=EXECUTE_ACTION_VALIDATE_SECURITY,
        data=data,
        errors=[],
    )


@app.post("/audit")
def audit() -> Any:
    started_at = utc_now()
    raw_payload = request.get_json(silent=True) or {}
    payload = with_execution_context(raw_payload, request.headers)

    execution_id = payload["execution_id"]
    correlation_id = payload["correlation_id"]
    trace: dict[str, Any] = {
        "request_id": hashlib.sha1(str(time.time()).encode()).hexdigest()[:12],
        "timings_ms": {},
    }

    deadline = time.time() + int(
        os.getenv("MAX_AUDIT_DURATION_SECONDS", str(DEFAULT_MAX_AUDIT_DURATION_SECONDS))
    )

    log_event(execution_id, correlation_id, "audit_started", pipeline_id=payload.get("pipeline_id"))

    validation_error = validate_payload(payload)
    if validation_error:
        log_event(execution_id, correlation_id, "audit_validation_failed", error=validation_error)
        return error_response(
            "VALIDATION_ERROR",
            validation_error,
            400,
            execution_id,
            correlation_id,
            {"schema_version": SCHEMA_VERSION},
        )

    try:
        pipeline_id: str = payload["pipeline_id"]
        event = payload["event"]
        changed_files = event.get("changed_files") if isinstance(event.get("changed_files"), list) else None
        scan_target = payload["scan_target"]
        policy_applied = build_policy(payload.get("policy"))

        t0 = time.time()
        repo_path_str = scan_target.get("repo_path")
        repo_path = Path(repo_path_str).resolve() if repo_path_str else None
        dep_scan = run_dependency_scans(repo_path, scan_target.get("manifest_paths"), deadline=deadline)
        trace["timings_ms"]["dependency_scan"] = int((time.time() - t0) * 1000)

        if time.time() > deadline:
            return error_response(
                "AUDIT_TIMEOUT",
                "global scan timeout reached",
                408,
                execution_id,
                correlation_id,
                {"stage": "dependency_scan"},
            )

        t1 = time.time()
        secret_scan = run_secret_scan(scan_target, changed_files)
        trace["timings_ms"]["secret_scan"] = int((time.time() - t1) * 1000)

        if time.time() > deadline:
            return error_response(
                "AUDIT_TIMEOUT",
                "global scan timeout reached",
                408,
                execution_id,
                correlation_id,
                {"stage": "secret_scan"},
            )

        t2 = time.time()
        network_checks = run_network_checks(payload, deadline=deadline)
        trace["timings_ms"]["network_checks"] = int((time.time() - t2) * 1000)

        dependency_findings = dep_scan["findings"]
        secret_findings = secret_scan["findings"]
        network_findings = network_checks.get("findings", [])

        risk_score = compute_risk_score(dependency_findings, secret_findings, network_findings)
        security_status = classify_security_status(risk_score)
        deployment_allowed, decision_reason = evaluate_gate(
            dependency_findings,
            secret_findings,
            network_findings,
            policy_applied,
        )

        status = "success"
        deployment_token = None
        if deployment_allowed:
            deployment_token, _ = build_token(pipeline_id, execution_id, correlation_id)
            if deployment_token is None:
                status = "partial"
                decision_reason = "allowed_without_token_secret"

        report = {
            "dependency_scan": dep_scan,
            "secret_scan": secret_scan,
            "network_checks": network_checks,
        }

        finished_at = utc_now()
        response = {
            "agent": AGENT_NAME,
            "schema_version": SCHEMA_VERSION,
            "status": status,
            "pipeline_id": pipeline_id,
            "execution_id": execution_id,
            "correlation_id": correlation_id,
            "deployment_allowed": deployment_allowed,
            "security_status": security_status,
            "risk_score": risk_score,
            "decision_reason": decision_reason,
            "deployment_token": deployment_token,
            "policy_applied": policy_applied,
            "report": report,
            "artifact_path": None,
            "started_at": isoformat_z(started_at),
            "finished_at": isoformat_z(finished_at),
            "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
            "trace": trace,
        }

        response["artifact_path"] = write_artifact(pipeline_id, response)

        log_event(
            execution_id,
            correlation_id,
            "audit_finished",
            deployment_allowed=deployment_allowed,
            security_status=security_status,
            risk_score=risk_score,
            decision_reason=decision_reason,
        )
        return jsonify(response), 200
    except Exception as exc:  # noqa: BLE001
        log_event(execution_id, correlation_id, "audit_internal_error", error=str(exc))
        return error_response(
            "INTERNAL_ERROR",
            "unexpected error during audit",
            500,
            execution_id,
            correlation_id,
            {"exception": str(exc)},
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3002)
