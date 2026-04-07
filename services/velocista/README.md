# VELOCISTA Agent

Microservicio Node.js para análisis y recomendaciones de performance pre-deploy en Coolify.

## Ejecutar local

```bash
npm install
npm start
```

Servicio disponible en `http://localhost:3001`.

## Endpoints

- `GET /health`
- `POST /optimize`

## Contrato mínimo /optimize

Campos requeridos:

- `pipeline_id` (string)
- `execution_id` (string)
- `correlation_id` (string)
- `event` (object)

Campos opcionales:

- `performance_requirements` (object)
- `target_url` (string)
- `mode`: `analysis_only` | `enforce_thresholds`

## Ejemplo cURL

```bash
curl -X POST http://localhost:3001/optimize \
  -H 'Content-Type: application/json' \
  -d '{
    "pipeline_id":"pipe-1234",
    "execution_id":"exec-2001",
    "correlation_id":"corr-xyz-90",
    "event":{
      "event_type":"push",
      "repository":"acme/web-app",
      "branch":"main",
      "author":"dev@acme.com",
      "timestamp":"2026-03-22T08:00:00Z",
      "summary":"Optimize homepage",
      "changed_files":["src/pages/Home.tsx","public/hero.jpg"],
      "metadata":{"commit":"abc123"}
    },
    "performance_requirements":{
      "lcp_target_ms":2500,
      "bundle_size_kb":180
    },
    "mode":"analysis_only"
  }'
```
