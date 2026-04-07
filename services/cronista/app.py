"""Microservicio CRONISTA.

Capa de auditoría y trazabilidad para pipelines multi-agente con persistencia
append-only en JSONL, artifacts Markdown y dashboard ejecutivo.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fcntl
from flask import Flask, jsonify, request
from jsonschema import ValidationError, validate

app = Flask(__name__)
AGENT_NAME = "cronista"
SCHEMA_VERSION = "v1"

# Rutas centralizadas por variables de entorno para despliegues flexibles.
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "./artifacts/cronista"))
DASHBOARD_PATH = Path(os.getenv("DASHBOARD_PATH", "./DASHBOARD_ALGORITHMUS.md"))
LOGS_PATH = DATA_DIR / "logs.jsonl"
REPORTS_PATH = DATA_DIR / "reports.jsonl"
EXECUTIONS_PATH = DATA_DIR / "executions.jsonl"

# Orchestration: schedules en memoria (POST /execute).
_SCHEDULES_LOCK = threading.Lock()
_SCHEDULES: dict[str, dict[str, Any]] = {}

SEVERITY_LEVELS = {"info", "warning", "error", "critical", "audit", "warn"}

COMMON_EVENT_REQUIRED_FIELDS = [
    "event_type",
    "repository",
    "branch",
    "author",
    "timestamp",
    "summary",
    "changed_files",
    "metadata",
]

COMMON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["pipeline_id", "event"],
    "properties": {
        "pipeline_id": {"type": "string", "minLength": 1},
        "execution_id": {"type": "string", "minLength": 1},
        "correlation_id": {"type": "string", "minLength": 1},
        "audit_hash": {"type": "string", "minLength": 1},
        "simulate_docker_logs": {"type": "boolean"},
        "event": {
            "type": "object",
            "required": COMMON_EVENT_REQUIRED_FIELDS,
            "properties": {
                "event_type": {"type": "string"},
                "repository": {"type": "string"},
                "branch": {"type": "string"},
                "author": {"type": "string"},
                "timestamp": {"type": "string"},
                "summary": {"type": "string"},
                "changed_files": {"type": "array", "items": {"type": "string"}},
                "metadata": {"type": "object"},
            },
            "additionalProperties": True,
        },
    },
    "additionalProperties": True,
}

AGENT_REPORT_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["agent", "status", "duration_ms", "result"],
    "properties": {
        "agent": {"type": "string", "minLength": 1},
        "status": {"type": "string", "minLength": 1},
        "duration_ms": {"type": "number", "minimum": 0},
        "result": {},
    },
    "additionalProperties": True,
}

LOGS_SCHEMA: dict[str, Any] = {
    "allOf": [
        COMMON_SCHEMA,
        {
            "type": "object",
            "required": ["logs", "level"],
            "properties": {
                "logs": {"type": "array"},
                "level": {"type": "string"},
            },
        },
    ]
}

REPORT_SCHEMA: dict[str, Any] = {
    "allOf": [
        COMMON_SCHEMA,
        {
            "type": "object",
            "required": ["agent_reports"],
            "properties": {
                "agent_reports": {
                    "type": "array",
                    "items": AGENT_REPORT_ITEM_SCHEMA,
                    "minItems": 1,
                },
            },
        },
    ]
}

UPDATE_SCHEMA: dict[str, Any] = {
    "allOf": [
        COMMON_SCHEMA,
        {
            "type": "object",
            "required": ["executive_report"],
            "properties": {
                "executive_report": {"type": "object"},
            },
        },
    ]
}


class RequestValidationError(Exception):
    """Error controlado para problemas de validación de payload."""



def now_iso() -> str:
    """Retorna timestamp UTC en ISO-8601."""
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def locked_open(path: Path, mode: str):
    """Abre archivo con bloqueo exclusivo para prevenir corrupción concurrente."""
    with path.open(mode, encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            yield fh
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def normalize_level(level: str) -> str:
    """Normaliza 'warn' legado a 'warning'."""
    return "warning" if level == "warn" else level


def log_event(
    level: str,
    event: str,
    execution_id: str,
    correlation_id: str,
    extra: dict[str, Any] | None = None,
) -> None:
    """Log estandarizado en JSON para trazabilidad operacional."""
    payload = {
        "timestamp": now_iso(),
        "level": normalize_level(level),
        "execution_id": execution_id,
        "correlation_id": correlation_id,
        "agent": AGENT_NAME,
        "event": event,
    }
    if extra:
        payload["extra"] = extra
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def ensure_storage_paths() -> None:
    """Crea directorios de persistencia si no existen."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)


def ensure_identifiers(payload: dict[str, Any]) -> tuple[str, str]:
    """Garantiza execution_id/correlation_id para compatibilidad retro."""
    payload.setdefault("execution_id", str(uuid.uuid4()))
    payload.setdefault("correlation_id", str(uuid.uuid4()))
    payload.setdefault("schema_version", SCHEMA_VERSION)
    return payload["execution_id"], payload["correlation_id"]


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    """Escribe una línea JSON en modo append-only con lock."""
    with locked_open(path, "a") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def collect_docker_logs() -> list[dict[str, Any]]:
    """Placeholder de recolección de logs Docker (SIMULADO)."""
    ts = now_iso()
    return [
        {
            "container": "orchestrator",
            "timestamp": ts,
            "stream": "stdout",
            "message": "Pipeline triggered and waiting for downstream agents",
        },
        {
            "container": "velocista",
            "timestamp": ts,
            "stream": "stdout",
            "message": "Build stage completed in simulated mode",
        },
    ]


def validate_payload(payload: dict[str, Any], schema: dict[str, Any]) -> None:
    """Valida payload con JSON Schema y retorna error legible si falla."""
    try:
        validate(instance=payload, schema=schema)
    except ValidationError as exc:
        path = ".".join(str(p) for p in exc.path) or "payload"
        raise RequestValidationError(f"Payload inválido en '{path}': {exc.message}") from exc


def validate_level(level: str) -> None:
    """Valida niveles de severidad soportados."""
    if level not in SEVERITY_LEVELS:
        supported = ", ".join(sorted(SEVERITY_LEVELS))
        raise RequestValidationError(f"level inválido '{level}'. Soportados: {supported}")


def validate_agent_reports_structure(agent_reports: Any) -> list[dict[str, Any]]:
    """Valida estructura mínima de agent_reports y compatibilidad legacy dict->array."""
    normalized: list[dict[str, Any]] = []

    # Compatibilidad: si llega dict legacy, convertir a array de items mínimos.
    if isinstance(agent_reports, dict):
        for agent_name, report in agent_reports.items():
            if not isinstance(report, dict):
                raise RequestValidationError("agent_reports legacy debe mapear cada agente a un object")
            normalized.append(
                {
                    "agent": report.get("agent", agent_name),
                    "status": report.get("status", "unknown"),
                    "duration_ms": report.get("duration_ms", 0),
                    "result": report.get("result", report),
                    **report,
                }
            )
    elif isinstance(agent_reports, list):
        normalized = agent_reports
    else:
        raise RequestValidationError("agent_reports debe ser array (o dict legacy)")

    for idx, item in enumerate(normalized):
        try:
            validate(instance=item, schema=AGENT_REPORT_ITEM_SCHEMA)
        except ValidationError as exc:
            raise RequestValidationError(f"agent_reports[{idx}] inválido: {exc.message}") from exc

    return normalized


def build_business_summary(event: dict[str, Any], executive_report: dict[str, Any] | None = None) -> str:
    """Traduce detalles técnicos a resumen orientado a negocio."""
    findings = []
    if isinstance(executive_report, dict) and isinstance(executive_report.get("findings"), list):
        findings = [str(item) for item in executive_report["findings"]]

    summary = event.get("summary", "Sin resumen técnico provisto")
    changed = event.get("changed_files", [])
    changed_list = ", ".join(changed) if changed else "sin detalle de archivos"
    finding_line = "; ".join(findings) if findings else "sin hallazgos adicionales reportados"

    return (
        f"### Qué cambió\n"
        f"Se ejecutó el evento `{event.get('event_type', 'unknown')}` con cambios en {changed_list}. "
        f"Resumen técnico: {summary}.\n\n"
        f"### Por qué importa\n"
        "Mejora la trazabilidad del pipeline y la visibilidad sobre cambios entregados, "
        "acelerando auditoría y toma de decisiones.\n\n"
        f"### Riesgos / mitigaciones\n"
        "Riesgo: discrepancias entre agentes o información incompleta. "
        "Mitigación: persistencia append-only y validación de contrato. "
        f"Hallazgos: {finding_line}.\n\n"
        "### Próximos pasos\n"
        "Validar KPIs, revisar alertas pendientes y confirmar cierre operativo del pipeline."
    )


def build_timeline(event: dict[str, Any], executive_report: dict[str, Any], finished_at: str) -> list[dict[str, Any]]:
    """Construye timeline simple de ejecución para el chronicle."""
    started_at = event.get("timestamp", now_iso())
    return [
        {"step": "event_received", "timestamp": started_at, "detail": event.get("event_type", "unknown")},
        {"step": "executive_consolidation", "timestamp": now_iso(), "detail": "executive_report processed"},
        {"step": "execution_closed", "timestamp": finished_at, "detail": executive_report.get("status", "partial")},
    ]


def create_chronicle_markdown(
    pipeline_id: str,
    event: dict[str, Any],
    executive_report: dict[str, Any] | None,
    business_summary: str,
) -> Path:
    """Genera artifact Markdown por pipeline con timeline y decisión final."""
    chronicle_path = ARTIFACTS_DIR / f"{pipeline_id}_chronicle.md"
    report = executive_report or {}
    finished_at = now_iso()
    final_decision = report.get("final_decision", report.get("status", "partial"))
    reason = report.get("reason", "Sin razón explícita reportada")
    recommendation = report.get("recommendation", "Continuar monitoreo en siguiente ejecución")
    timeline = build_timeline(event, report, finished_at)

    body = [
        "# CRONISTA Chronicle",
        "",
        f"- Pipeline ID: `{pipeline_id}`",
        f"- Timestamp: `{finished_at}`",
        f"- Repositorio: `{event.get('repository')}`",
        f"- Branch: `{event.get('branch')}`",
        f"- Autor: `{event.get('author')}`",
        f"- Decision final: `{final_decision}`",
        f"- Reason: {reason}",
        f"- Recommendation: {recommendation}",
        "",
        "## Timeline",
        "```json",
        json.dumps(timeline, indent=2, ensure_ascii=False),
        "```",
        "",
        "## Executive Report",
        "```json",
        json.dumps(report, indent=2, ensure_ascii=False),
        "```",
        "",
        "## Business Summary",
        business_summary,
        "",
    ]
    with locked_open(chronicle_path, "w") as fh:
        fh.write("\n".join(body))
    return chronicle_path


def ensure_dashboard_exists() -> None:
    """Crea dashboard base si no existe."""
    if not DASHBOARD_PATH.exists():
        with locked_open(DASHBOARD_PATH, "w") as fh:
            fh.write(
                "# DASHBOARD ALGORITHMUS\n\n"
                "Bitácora ejecutiva append-only para trazabilidad de pipelines.\n"
            )


def build_dashboard_section(
    payload: dict[str, Any],
    status: str,
    business_summary: str,
    chronicle_path: Path,
    decision_counts: dict[str, int],
) -> str:
    """Construye sección markdown por fecha y pipeline con conteos."""
    event = payload["event"]
    executive_report = payload.get("executive_report") or {}
    artifact_links = executive_report.get("artifact_paths") if isinstance(executive_report, dict) else {}

    pipeline_id = payload["pipeline_id"]
    date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_delimiter = f"<!-- CRONISTA:PIPELINE:{pipeline_id}:START -->"
    end_delimiter = f"<!-- CRONISTA:PIPELINE:{pipeline_id}:END -->"

    section = [
        "\n",
        start_delimiter,
        f"### Fecha `{date_key}`",
        f"#### Pipeline `{pipeline_id}`",
        f"- timestamp: `{event.get('timestamp')}`",
        f"- event_type: `{event.get('event_type')}`",
        f"- repo: `{event.get('repository')}`",
        f"- branch: `{event.get('branch')}`",
        f"- author: `{event.get('author')}`",
        f"- execution_id: `{payload.get('execution_id')}`",
        f"- correlation_id: `{payload.get('correlation_id')}`",
        f"- estado_final: `{status}`",
        "- resumen pipeline:",
        f"  - final_decision: `{executive_report.get('final_decision', status)}`",
        f"  - risk_score: `{executive_report.get('risk_score', 'N/A')}`",
        "- conteo de decisiones:",
        f"  - success: `{decision_counts.get('success', 0)}`",
        f"  - partial: `{decision_counts.get('partial', 0)}`",
        f"  - fail: `{decision_counts.get('fail', 0)}`",
        "- artifacts:",
        f"  - velocista: `{artifact_links.get('velocista', 'N/A') if isinstance(artifact_links, dict) else 'N/A'}`",
        f"  - guardia: `{artifact_links.get('guardia', 'N/A') if isinstance(artifact_links, dict) else 'N/A'}`",
        f"  - orquestador: `{artifact_links.get('orquestador', 'N/A') if isinstance(artifact_links, dict) else 'N/A'}`",
        f"  - cronista: `{chronicle_path.as_posix()}`",
        "",
        "### Resumen ejecutivo",
        business_summary,
        end_delimiter,
        "",
    ]
    return "\n".join(section)


def load_decision_counts() -> dict[str, int]:
    """Calcula conteos de decisiones leyendo executions.jsonl (append-only)."""
    counts: Counter[str] = Counter()
    if not EXECUTIONS_PATH.exists():
        return {}

    with locked_open(EXECUTIONS_PATH, "r") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            decision = str(data.get("final_decision", "unknown"))
            counts[decision] += 1
    return dict(counts)


def append_dashboard_section(
    payload: dict[str, Any],
    status: str,
    business_summary: str,
    chronicle_path: Path,
) -> bool:
    """Agrega sección append-only por pipeline; evita duplicados por delimitador."""
    ensure_dashboard_exists()
    pipeline_id = payload["pipeline_id"]
    delimiter = f"<!-- CRONISTA:PIPELINE:{pipeline_id}:START -->"
    decision_counts = load_decision_counts()

    with locked_open(DASHBOARD_PATH, "a+") as fh:
        fh.seek(0)
        current_content = fh.read()
        if delimiter in current_content:
            return False
        fh.seek(0, os.SEEK_END)
        fh.write(build_dashboard_section(payload, status, business_summary, chronicle_path, decision_counts))
        return True


def extract_metrics(executive_report: dict[str, Any], agent_reports: list[dict[str, Any]]) -> dict[str, Any]:
    """Construye métricas estándar de ejecución."""
    agents_latency = {item.get("agent", "unknown"): item.get("duration_ms", 0) for item in agent_reports}
    error_count = sum(1 for item in agent_reports if str(item.get("status", "")).lower() in {"error", "fail", "failed"})
    return {
        "duration_ms": executive_report.get("duration_ms", sum(int(v) for v in agents_latency.values() if isinstance(v, (int, float)))),
        "agents_latency": agents_latency,
        "error_count": executive_report.get("error_count", error_count),
    }


def build_execution_snapshot(payload: dict[str, Any], executive_report: dict[str, Any], finished_at: str) -> dict[str, Any]:
    """Genera snapshot por ejecución para executions.jsonl."""
    event = payload["event"]
    agent_reports = validate_agent_reports_structure(
        executive_report.get("agent_reports", payload.get("agent_reports", []))
    )
    metrics = extract_metrics(executive_report, agent_reports)

    return {
        "schema_version": SCHEMA_VERSION,
        "execution_id": payload["execution_id"],
        "correlation_id": payload["correlation_id"],
        "pipeline_id": payload["pipeline_id"],
        "started_at": event.get("timestamp", now_iso()),
        "finished_at": finished_at,
        "final_decision": executive_report.get("final_decision", executive_report.get("status", "partial")),
        "risk_score": executive_report.get("risk_score", "N/A"),
        "agent_reports": agent_reports,
        "executive_summary": executive_report.get("executive_summary", event.get("summary", "")),
        "metrics": metrics,
        "audit_hash": payload.get("audit_hash"),
    }


def response_payload(
    *,
    pipeline_id: str,
    status: str,
    stored: bool,
    started_at: float,
    trace: dict[str, Any],
    chronicle_path: Path | None = None,
) -> dict[str, Any]:
    """Construye respuesta estándar sin romper contrato existente."""
    finished = time.time()
    return {
        "pipeline_id": pipeline_id,
        "agent": AGENT_NAME,
        "status": status,
        "stored": stored,
        "paths": {
            "logs_path": LOGS_PATH.as_posix(),
            "reports_path": REPORTS_PATH.as_posix(),
            "dashboard_path": DASHBOARD_PATH.as_posix(),
            "chronicle_path": chronicle_path.as_posix() if chronicle_path else None,
            "executions_path": EXECUTIONS_PATH.as_posix(),
        },
        "started_at": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
        "finished_at": datetime.fromtimestamp(finished, tz=timezone.utc).isoformat(),
        "duration_ms": int((finished - started_at) * 1000),
        "trace": trace,
    }


def handle_error(
    started: float,
    payload: dict[str, Any],
    trace: dict[str, Any],
    exc: Exception,
    execution_id: str,
    correlation_id: str,
) -> tuple[Any, int]:
    """Manejador de errores estructurado para respuestas consistentes."""
    trace.setdefault("steps", []).append({"step": "error", "ok": False, "detail": str(exc)})
    log_event("error", "request_failed", execution_id, correlation_id, {"error": str(exc)})
    return (
        jsonify(
            response_payload(
                pipeline_id=payload.get("pipeline_id", "unknown"),
                status="error",
                stored=False,
                started_at=started,
                trace=trace,
            )
        ),
        400 if isinstance(exc, RequestValidationError) else 500,
    )


def _execute_response(
    *,
    status: str,
    action: str,
    data: dict[str, Any] | None,
    errors: list[dict[str, str]],
    http_status: int = 200,
) -> tuple[Any, int]:
    """Respuesta estricta para POST /execute: data es object | null, errors siempre array."""
    safe_data: dict[str, Any] | None = data if isinstance(data, dict) else None
    safe_errors: list[dict[str, str]] = list(errors) if errors else []
    body: dict[str, Any] = {
        "status": status,
        "agent": AGENT_NAME,
        "action": action,
        "data": safe_data,
        "errors": safe_errors,
    }
    return jsonify(body), http_status


def _log_execute_request(action: str, execution_id: Any) -> None:
    print(
        json.dumps(
            {"event": "cronista_execute", "action": action, "execution_id": execution_id},
            ensure_ascii=False,
        ),
        flush=True,
    )


def _log_execute_validation_error(action: str, meta: Any, errors: list[dict[str, str]]) -> None:
    first = errors[0] if errors else {}
    err_code = first.get("code", "UNKNOWN") if isinstance(first, dict) else "UNKNOWN"
    print(
        json.dumps(
            {
                "event": "cronista_execute_validation_error",
                "action": action,
                "execution_id": meta.get("execution_id") if isinstance(meta, dict) else None,
                "error_code": err_code,
                "errors_count": len(errors),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


def _validate_schedule_required_fields(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
    """Valida name, cron, task; devuelve campos normalizados o lista de errores."""
    errors: list[dict[str, str]] = []
    out: dict[str, Any] = {}

    for key in ("name", "cron", "task"):
        raw = payload.get(key)
        if raw is None:
            errors.append({"code": "VALIDATION_ERROR", "message": f"Missing required field: {key}"})
            continue
        if not isinstance(raw, str):
            errors.append({"code": "VALIDATION_ERROR", "message": f"Field '{key}' must be a string"})
            continue
        stripped = raw.strip()
        if not stripped:
            errors.append({"code": "VALIDATION_ERROR", "message": f"Field '{key}' cannot be empty"})
            continue
        out[key] = stripped

    if errors:
        return None, errors

    repo = payload.get("repository")
    branch = payload.get("branch")
    out["repository"] = repo.strip() if isinstance(repo, str) else ("" if repo is None else str(repo))
    out["branch"] = branch.strip() if isinstance(branch, str) else ("" if branch is None else str(branch))
    return out, []


def _run_create_schedule(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
    fields, errors = _validate_schedule_required_fields(payload)
    if errors:
        return None, errors

    schedule_id = str(uuid.uuid4())  # uuid4 para IDs fuertes en producción
    schedule: dict[str, Any] = {
        "schedule_id": schedule_id,
        "name": fields["name"],
        "cron": fields["cron"],
        "task": fields["task"],
        "repository": fields["repository"],
        "branch": fields["branch"],
        "created_at": now_iso(),
    }
    with _SCHEDULES_LOCK:
        _SCHEDULES[schedule_id] = schedule
    return {"schedule": schedule}, []


def _run_list_schedules() -> tuple[dict[str, Any], list[dict[str, str]]]:
    with _SCHEDULES_LOCK:
        schedules = sorted(
            _SCHEDULES.values(),
            key=lambda x: x["created_at"],
            reverse=True,
        )
    return {"schedules": schedules}, []


@app.post("/execute")
def post_execute() -> Any:
    """Orquestación unificada (schedules en memoria); no altera /logs, /report, /update."""
    action = ""
    try:
        body = request.get_json(silent=True)
        log_action = ""
        log_execution_id: Any = None
        if isinstance(body, dict):
            ra = body.get("action")
            log_action = ra.strip() if isinstance(ra, str) else ""
            m = body.get("meta")
            if isinstance(m, dict):
                log_execution_id = m.get("execution_id")
        _log_execute_request(log_action, log_execution_id)

        if not isinstance(body, dict):
            errs_400 = [{"code": "INVALID_REQUEST", "message": "JSON body must be an object"}]
            _log_execute_validation_error(action, None, errs_400)
            return _execute_response(
                status="error",
                action=action,
                data=None,
                errors=errs_400,
                http_status=400,
            )

        raw_action = body.get("action")
        action = raw_action.strip() if isinstance(raw_action, str) else ""
        if not action:
            errs_400 = [{"code": "INVALID_REQUEST", "message": "Field 'action' is required"}]
            _log_execute_validation_error(action, body.get("meta"), errs_400)
            return _execute_response(
                status="error",
                action=action,
                data=None,
                errors=errs_400,
                http_status=400,
            )

        payload = body.get("payload")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            errs_400 = [{"code": "INVALID_REQUEST", "message": "Field 'payload' must be an object"}]
            _log_execute_validation_error(action, body.get("meta"), errs_400)
            return _execute_response(
                status="error",
                action=action,
                data=None,
                errors=errs_400,
                http_status=400,
            )

        meta = body.get("meta")
        if meta is not None and not isinstance(meta, dict):
            errs_400 = [{"code": "INVALID_REQUEST", "message": "Field 'meta' must be an object when provided"}]
            _log_execute_validation_error(action, meta, errs_400)
            return _execute_response(
                status="error",
                action=action,
                data=None,
                errors=errs_400,
                http_status=400,
            )

        if action == "create_schedule":
            data, errs = _run_create_schedule(payload)
            if errs:
                _log_execute_validation_error(action, meta, errs)
                return _execute_response(status="error", action=action, data=None, errors=errs, http_status=400)
            return _execute_response(status="success", action=action, data=data, errors=[], http_status=200)

        if action == "list_schedules":
            data, _errs = _run_list_schedules()
            return _execute_response(status="success", action=action, data=data, errors=[], http_status=200)

        errs_400 = [{"code": "UNSUPPORTED_ACTION", "message": f"Unknown action: {action}"}]
        _log_execute_validation_error(action, meta, errs_400)
        return _execute_response(
            status="error",
            action=action,
            data=None,
            errors=errs_400,
            http_status=400,
        )
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {
                    "event": "cronista_execute_error",
                    "action": action,
                    "message": str(exc),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return _execute_response(
            status="error",
            action=action,
            data=None,
            errors=[{"code": "INTERNAL_ERROR", "message": str(exc)}],
            http_status=500,
        )


@app.get("/health")
def health() -> Any:
    """Health check con validación de escritura a disco."""
    started = time.time()
    execution_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    trace = {"request_id": f"health-{int(started * 1000)}", "steps": []}

    try:
        ensure_storage_paths()
        probe_path = DATA_DIR / ".healthcheck.tmp"
        with locked_open(probe_path, "w") as fh:
            fh.write(now_iso())
        probe_path.unlink(missing_ok=True)

        trace["steps"].append({"step": "disk_write_probe", "ok": True, "path": probe_path.as_posix()})
        log_event("audit", "health_ok", execution_id, correlation_id)
        return jsonify({"agent": AGENT_NAME, "status": "ok", "timestamp": now_iso(), "trace": trace})
    except Exception as exc:  # noqa: BLE001
        trace["steps"].append({"step": "disk_write_probe", "ok": False, "error": str(exc)})
        log_event("critical", "health_failed", execution_id, correlation_id, {"error": str(exc)})
        return jsonify({"agent": AGENT_NAME, "status": "error", "timestamp": now_iso(), "trace": trace}), 500


@app.post("/logs")
def post_logs() -> Any:
    """Recibe logs de ejecución y persiste eventos crudos en logs.jsonl."""
    started = time.time()
    payload = request.get_json(silent=True) or {}
    execution_id, correlation_id = ensure_identifiers(payload)
    trace = {"request_id": f"logs-{int(started * 1000)}", "steps": []}

    try:
        ensure_storage_paths()
        validate_payload(payload, LOGS_SCHEMA)
        validate_level(payload["level"])
        trace["steps"].append({"step": "json_schema_validation", "ok": True})

        logs = payload["logs"]
        if payload.get("simulate_docker_logs") is True:
            logs = logs + collect_docker_logs()
            trace["steps"].append({"step": "collect_docker_logs", "ok": True, "count": len(logs)})

        record = {
            **payload,
            "level": normalize_level(payload["level"]),
            "schema_version": SCHEMA_VERSION,
            "ingested_at": now_iso(),
            "logs": logs,
        }
        append_jsonl(LOGS_PATH, record)
        trace["steps"].append({"step": "append_jsonl", "ok": True, "path": LOGS_PATH.as_posix()})

        log_event(payload["level"], "logs_stored", execution_id, correlation_id, {"path": LOGS_PATH.as_posix()})
        return jsonify(
            response_payload(
                pipeline_id=payload["pipeline_id"],
                status="success",
                stored=True,
                started_at=started,
                trace=trace,
            )
        )
    except Exception as exc:  # noqa: BLE001
        return handle_error(started, payload, trace, exc, execution_id, correlation_id)


@app.post("/report")
def post_report() -> Any:
    """Recibe resultados de agentes y persiste reports.jsonl."""
    started = time.time()
    payload = request.get_json(silent=True) or {}
    execution_id, correlation_id = ensure_identifiers(payload)
    trace = {"request_id": f"report-{int(started * 1000)}", "steps": []}

    try:
        ensure_storage_paths()
        # Se valida contrato común + estructura flexible y luego reglas de negocio.
        validate_payload(payload, COMMON_SCHEMA)
        agent_reports = validate_agent_reports_structure(payload.get("agent_reports"))
        trace["steps"].append({"step": "json_schema_validation", "ok": True})

        record = {
            **payload,
            "schema_version": SCHEMA_VERSION,
            "agent_reports": agent_reports,
            "ingested_at": now_iso(),
        }
        append_jsonl(REPORTS_PATH, record)
        trace["steps"].append({"step": "append_jsonl", "ok": True, "path": REPORTS_PATH.as_posix()})

        statuses = [str(x.get("status", "")).lower() for x in agent_reports]
        status = "success" if statuses and all(s in {"ok", "success", "pass"} for s in statuses) else "partial"
        log_event("audit", "report_stored", execution_id, correlation_id, {"status": status})
        return jsonify(
            response_payload(
                pipeline_id=payload["pipeline_id"],
                status=status,
                stored=True,
                started_at=started,
                trace=trace,
            )
        )
    except Exception as exc:  # noqa: BLE001
        return handle_error(started, payload, trace, exc, execution_id, correlation_id)


@app.post("/update")
def post_update() -> Any:
    """Actualiza dashboard, genera chronicle y persiste snapshot final."""
    started = time.time()
    payload = request.get_json(silent=True) or {}
    execution_id, correlation_id = ensure_identifiers(payload)
    trace = {"request_id": f"update-{int(started * 1000)}", "steps": []}

    try:
        ensure_storage_paths()
        validate_payload(payload, UPDATE_SCHEMA)
        trace["steps"].append({"step": "json_schema_validation", "ok": True})

        executive_report = payload["executive_report"]
        pipeline_status = executive_report.get("status", "partial")
        if pipeline_status not in {"success", "partial", "fail"}:
            pipeline_status = "partial"

        business_summary = build_business_summary(payload["event"], executive_report)
        chronicle_path = create_chronicle_markdown(
            payload["pipeline_id"], payload["event"], executive_report, business_summary
        )
        trace["steps"].append({"step": "create_chronicle_markdown", "ok": True, "path": chronicle_path.as_posix()})

        finished_at = now_iso()
        snapshot = build_execution_snapshot(payload, executive_report, finished_at)
        append_jsonl(EXECUTIONS_PATH, snapshot)
        trace["steps"].append({"step": "append_execution_snapshot", "ok": True, "path": EXECUTIONS_PATH.as_posix()})

        appended = append_dashboard_section(payload, pipeline_status, business_summary, chronicle_path)
        trace["steps"].append(
            {
                "step": "append_dashboard_section",
                "ok": True,
                "appended": appended,
                "path": DASHBOARD_PATH.as_posix(),
            }
        )

        status = pipeline_status if appended else "partial"
        log_event("audit", "execution_snapshot_stored", execution_id, correlation_id, {"appended": appended})
        return jsonify(
            response_payload(
                pipeline_id=payload["pipeline_id"],
                status=status,
                stored=True,
                started_at=started,
                trace=trace,
                chronicle_path=chronicle_path,
            )
        )
    except Exception as exc:  # noqa: BLE001
        return handle_error(started, payload, trace, exc, execution_id, correlation_id)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3003)
