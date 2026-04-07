# CRONISTA (Flask)

Microservicio de auditoría y trazabilidad multi-agente con persistencia append-only en JSONL.

## Ejecutar local

```bash
pip install -r requirements.txt
python app.py
```

Servicio en `http://localhost:3003`.

## Endpoints

- `POST /logs`
- `POST /report`
- `POST /update`
- `POST /execute`
- `GET /health`

## `POST /execute` (orquestación)

Endpoint unificado para acciones de orquestación. Las respuestas siguen siempre el mismo contrato JSON; `data` es un objeto en éxito o `null` en error; `errors` es siempre un array (vacío si no hay errores).

**Nota:** los horarios creados con `create_schedule` se guardan **en memoria** en el proceso; no persisten entre reinicios.

### Formato de request

```json
{
  "action": "string",
  "payload": {},
  "meta": {
    "execution_id": "string",
    "correlation_id": "string",
    "source": "orchestrator"
  }
}
```

| Campo     | Obligatorio | Descripción |
|-----------|-------------|-------------|
| `action`  | Sí          | Nombre de la acción (`create_schedule`, `list_schedules`). |
| `payload` | No          | Objeto; si se omite se interpreta como `{}`. |
| `meta`    | No          | Metadatos opcionales; si se envía, debe ser un objeto (p. ej. trazabilidad desde el orquestador). |

### Acciones soportadas

#### `create_schedule`

- **Payload:** se leen `name`, `cron`, `task`, `repository`, `branch`.
- **Obligatorios:** `name`, `cron`, `task` (strings no vacíos tras `trim`).
- **Opcionales:** `repository`, `branch` (si faltan o no son string, se normalizan a cadena vacía o `str(valor)` según el caso).
- **Efecto:** crea un objeto de horario con `schedule_id` (UUID v4), `created_at` (ISO-8601 UTC) y lo almacena en memoria.

#### `list_schedules`

- **Payload:** ignorado.
- **Efecto:** devuelve todos los horarios ordenados por `created_at` descendente.

### Formato de response

```json
{
  "status": "success | error",
  "agent": "cronista",
  "action": "<action>",
  "data": {},
  "errors": []
}
```

En error, cada elemento de `errors` tiene la forma `{ "code": "...", "message": "..." }`.

### Códigos HTTP

| Situación              | HTTP |
|------------------------|------|
| Éxito                  | `200` |
| `INVALID_REQUEST`      | `400` |
| `VALIDATION_ERROR`     | `400` |
| `UNSUPPORTED_ACTION`   | `400` |
| `INTERNAL_ERROR`       | `500` |

### Códigos de error (`errors[].code`)

| Código               | HTTP | Cuándo |
|----------------------|------|--------|
| `INVALID_REQUEST`    | 400  | Body JSON no es objeto, falta `action`, `payload` o `meta` con tipo inválido. |
| `VALIDATION_ERROR`   | 400  | `create_schedule`: falta o es inválido `name` / `cron` / `task` (tipo o vacío). |
| `UNSUPPORTED_ACTION` | 400  | `action` no es `create_schedule` ni `list_schedules`. |
| `INTERNAL_ERROR`     | 500  | Excepción no controlada en el servidor. |

### Ejemplo: `create_schedule` (request)

```json
{
  "action": "create_schedule",
  "payload": {
    "name": "nightly-sync",
    "cron": "0 2 * * *",
    "task": "sync_repositories",
    "repository": "github.com/acme/core",
    "branch": "main"
  },
  "meta": {
    "execution_id": "exec-2026-04-05-001",
    "correlation_id": "corr-abc",
    "source": "orchestrator"
  }
}
```

### Ejemplo: `create_schedule` (response, 200)

```json
{
  "status": "success",
  "agent": "cronista",
  "action": "create_schedule",
  "data": {
    "schedule": {
      "schedule_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "name": "nightly-sync",
      "cron": "0 2 * * *",
      "task": "sync_repositories",
      "repository": "github.com/acme/core",
      "branch": "main",
      "created_at": "2026-04-05T12:00:00.000000+00:00"
    }
  },
  "errors": []
}
```

### Ejemplo: `list_schedules` (request)

```json
{
  "action": "list_schedules",
  "payload": {}
}
```

### Ejemplo: `list_schedules` (response, 200)

```json
{
  "status": "success",
  "agent": "cronista",
  "action": "list_schedules",
  "data": {
    "schedules": [
      {
        "schedule_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "name": "nightly-sync",
        "cron": "0 2 * * *",
        "task": "sync_repositories",
        "repository": "github.com/acme/core",
        "branch": "main",
        "created_at": "2026-04-05T12:00:00.000000+00:00"
      }
    ]
  },
  "errors": []
}
```

### Ejemplo: error de validación (response, 400)

```json
{
  "status": "error",
  "agent": "cronista",
  "action": "create_schedule",
  "data": null,
  "errors": [
    {
      "code": "VALIDATION_ERROR",
      "message": "Missing required field: cron"
    }
  ]
}
```

### Ejemplo: acción no soportada (response, 400)

```json
{
  "status": "error",
  "agent": "cronista",
  "action": "delete_schedule",
  "data": null,
  "errors": [
    {
      "code": "UNSUPPORTED_ACTION",
      "message": "Unknown action: delete_schedule"
    }
  ]
}
```

## Storage (append-only)

- `data/logs.jsonl`: eventos crudos
- `data/reports.jsonl`: resultados por agentes
- `data/executions.jsonl`: snapshot final por ejecución
- `artifacts/cronista/<pipeline_id>_chronicle.md`: artifact markdown consolidado

## Ejemplo request JSON

```json
{
  "pipeline_id": "pipe-2026-02-22-001",
  "execution_id": "exec-001",
  "correlation_id": "corr-001",
  "schema_version": "v1",
  "audit_hash": "sha256:abcd1234",
  "event": {
    "event_type": "deploy",
    "repository": "github.com/acme/dashboard",
    "branch": "main",
    "author": "ci-bot",
    "timestamp": "2026-02-22T18:30:00Z",
    "summary": "Se actualizó el cálculo de priorización y se agregaron tests.",
    "changed_files": [
      "src/prioritizer.py",
      "tests/test_prioritizer.py"
    ],
    "metadata": {
      "commit": "abc123",
      "environment": "production"
    }
  },
  "level": "audit",
  "logs": [
    {
      "timestamp": "2026-02-22T18:31:00Z",
      "message": "Pipeline iniciado"
    }
  ],
  "agent_reports": [
    {
      "agent": "velocista",
      "status": "success",
      "duration_ms": 523,
      "result": {"build": "ok"}
    },
    {
      "agent": "guardia",
      "status": "success",
      "duration_ms": 418,
      "result": {"security": "ok"}
    }
  ],
  "executive_report": {
    "status": "success",
    "final_decision": "success",
    "reason": "Todos los agentes aprobaron",
    "recommendation": "Continuar con despliegue gradual",
    "risk_score": 0.12,
    "duration_ms": 1080,
    "findings": [
      "Latencia -12%",
      "Cobertura >90%"
    ]
  }
}
```

## Ejemplo response JSON

```json
{
  "pipeline_id": "pipe-2026-02-22-001",
  "agent": "cronista",
  "status": "success",
  "stored": true,
  "paths": {
    "logs_path": "data/logs.jsonl",
    "reports_path": "data/reports.jsonl",
    "dashboard_path": "./DASHBOARD_ALGORITHMUS.md",
    "chronicle_path": "artifacts/cronista/pipe-2026-02-22-001_chronicle.md",
    "executions_path": "data/executions.jsonl"
  },
  "started_at": "2026-02-22T18:32:00.000000+00:00",
  "finished_at": "2026-02-22T18:32:00.045000+00:00",
  "duration_ms": 45,
  "trace": {
    "request_id": "update-1740249120000",
    "steps": [
      {
        "step": "create_chronicle_markdown",
        "ok": true,
        "path": "artifacts/cronista/pipe-2026-02-22-001_chronicle.md"
      },
      {
        "step": "append_execution_snapshot",
        "ok": true,
        "path": "data/executions.jsonl"
      }
    ]
  }
}
```
