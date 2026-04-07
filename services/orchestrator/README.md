# Orchestrator (Agente Ejecutivo)

Microservicio Node.js + Express para coordinar:
- Velocista (`POST /optimize`)
- Guardia (`POST /audit`)
- Cronista (`POST /update`)

## Endpoints
- `POST /pipeline`: ejecuta pipeline, consolida resultados, persiste artifacts y actualiza dashboard.
- `GET /health`: salud del servicio.

## Identidad de ejecución
Cada corrida incluye:
- `execution_id` (uuid)
- `correlation_id` (uuid)
- `trace_id` (opcional)

Estos IDs viajan a todos los agentes, logs y artifacts.

## Decision Engine
- `BLOCK` si `guardia.deployment_allowed === false` o Guardia falla.
- `REVIEW` si `velocista.performance_status === "critical"`.
- `ALLOW` en los demás casos.

Incluye:
- `final_decision`
- `risk_score`
- `decision_factors`
- `executive_report`
- `deployment_token` + `token_expires_at` cuando aplica (`ALLOW`).

## Variables de entorno
- `PORT` (default `4000`)
- `VELOCISTA_URL` (default `http://velocista:3001`)
- `GUARDIA_URL` (default `http://guardia:3002`)
- `CRONISTA_URL` (default `http://cronista:3003`)
- `REQUEST_TIMEOUT_MS` (default `5000`)
- `VELOCISTA_TIMEOUT_MS` (default `REQUEST_TIMEOUT_MS`)
- `GUARDIA_TIMEOUT_MS` (default `REQUEST_TIMEOUT_MS`)
- `CRONISTA_TIMEOUT_MS` (default `REQUEST_TIMEOUT_MS`)
- `DASHBOARD_PATH` (default `/workspace/DASHBOARD_ALGORITHMUS.md`)
- `ARTIFACTS_BASE_DIR` (default `/app/artifacts`)

## Contrato
- `contracts/pipeline_event.schema.json` (JSON Schema draft 2020-12)

## Artifact output
- `/app/artifacts/orchestrator/{execution_id}.json`

## Coolify / Docker
```bash
docker build -t orchestrator-service .
docker run --rm -p 4000:4000 \
  -e VELOCISTA_URL=http://velocista:3001 \
  -e GUARDIA_URL=http://guardia:3002 \
  -e CRONISTA_URL=http://cronista:3003 \
  -e DASHBOARD_PATH=/workspace/DASHBOARD_ALGORITHMUS.md \
  -e ARTIFACTS_BASE_DIR=/app/artifacts \
  orchestrator-service
```