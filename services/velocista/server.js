const express = require("express");
const fs = require("fs/promises");
const path = require("path");
const { z } = require("zod");

const app = express();
const PORT = process.env.PORT || 3001;
const EXECUTION_TIMEOUT_MS = Number(process.env.VELOCISTA_TIMEOUT_MS || 12000);

app.use(express.json({ limit: "1mb" }));

app.use((err, req, res, next) => {
  if (err && err.type === "entity.parse.failed") {
    return res.status(400).json({
      status: "error",
      agent: "velocista",
      action: "",
      data: null,
      errors: [
        {
          code: "INVALID_JSON",
          message: err.message || "Invalid JSON body",
        },
      ],
    });
  }
  next(err);
});

/**
 * Sección: Contrato global / validación (Zod)
 * - Mantiene compatibilidad con payload actual y agrega execution_id + correlation_id.
 */
const eventSchema = z
  .object({
    event_type: z.string().optional(),
    repository: z.string().optional(),
    branch: z.string().optional(),
    author: z.string().optional(),
    timestamp: z.string().optional(),
    summary: z.string().optional(),
    changed_files: z.array(z.string()).default([]),
    metadata: z.record(z.any()).optional(),
  })
  .passthrough();

const performanceRequirementsSchema = z
  .object({
    lcp_target_ms: z.number().positive().optional(),
    cls_target: z.number().nonnegative().optional(),
    tbt_target_ms: z.number().nonnegative().optional(),
    bundle_size_kb: z.number().positive().optional(),
    image_formats_allowed: z.array(z.string()).optional(),
  })
  .passthrough()
  .optional();

const optimizeSchema = z
  .object({
    pipeline_id: z.string().min(1),
    execution_id: z.string().min(1),
    correlation_id: z.string().min(1),
    event: eventSchema,
    performance_requirements: performanceRequirementsSchema,
    target_url: z.string().url().optional(),
    mode: z
      .enum(["analysis_only", "enforce_thresholds"])
      .default("analysis_only"),
  })
  .passthrough();

const executeMetaSchema = z
  .object({
    execution_id: z.string().optional(),
    correlation_id: z.string().optional(),
    source: z.string().optional(),
  })
  .passthrough()
  .optional();

const executeRequestSchema = z.object({
  action: z.any().optional(),
  payload: z.any().optional(),
  meta: executeMetaSchema,
});

/**
 * Sección: logging estructurado para Orchestrator/Guardia/Cronista.
 */
function logStructured(level, event, context = {}) {
  const entry = {
    timestamp: new Date().toISOString(),
    level,
    agent: "velocista",
    event,
    execution_id: context.execution_id || null,
    correlation_id: context.correlation_id || null,
    pipeline_id: context.pipeline_id || null,
    details: context.details || null,
  };

  const line = JSON.stringify(entry);
  if (level === "error") {
    console.error(line);
  } else {
    console.log(line);
  }
}

function formatMs(startMs, endMs) {
  return Math.max(0, endMs - startMs);
}

function zodIssuesToExecuteErrors(issues, code = "VALIDATION_ERROR") {
  return issues.map((issue) => ({
    code,
    message: issue.path.length
      ? `${issue.path.join(".")}: ${issue.message}`
      : issue.message,
  }));
}

function buildExecuteResponse({ status, action, data = null, errors = [] }) {
  const safeErrors = Array.isArray(errors) ? errors : [];
  let safeData = null;
  if (data !== null && data !== undefined) {
    safeData =
      typeof data === "object" && !Array.isArray(data) ? data : null;
  }
  return {
    status: status === "success" ? "success" : "error",
    agent: "velocista",
    action: String(action ?? ""),
    data: safeData,
    errors: safeErrors,
  };
}

function toSafeExecutePayload(payload) {
  return payload &&
    typeof payload === "object" &&
    !Array.isArray(payload)
    ? payload
    : {};
}

function trimInput(value) {
  if (value === undefined || value === null) return "";
  return String(value).trim();
}

/**
 * analyze_event: required string fields after trim.
 * Returns { ok, data } or { ok: false, errors } with code VALIDATION_ERROR.
 */
function validateAnalyzeEventPayload(safePayload) {
  const repository = trimInput(safePayload.repository);
  const branch = trimInput(safePayload.branch);
  const event_type = trimInput(safePayload.event_type);
  const missing = [];
  if (!repository) missing.push("repository");
  if (!branch) missing.push("branch");
  if (!event_type) missing.push("event_type");
  if (missing.length) {
    return {
      ok: false,
      errors: [
        {
          code: "VALIDATION_ERROR",
          message: `Missing or empty required fields: ${missing.join(", ")}`,
        },
      ],
    };
  }
  return {
    ok: true,
    data: { ...safePayload, repository, branch, event_type },
  };
}

/**
 * Orchestration: analyze_event — riesgo por rama y score 0–100.
 */
function analyzeEventFromValidatedPayload(validated) {
  const risk = validated.branch === "main" ? "high" : "low";
  const requires_review = risk === "high";
  const score = risk === "high" ? 85 : 35;
  return { risk, requires_review, score };
}

/**
 * Sección: findings estructurados.
 */
function makeFinding({ type, severity, message, current, target }) {
  return { type, severity, message, current, target };
}

/**
 * Sección: recomendaciones/finding base por análisis estático.
 */
function buildFindings(payload) {
  const changedFiles = payload?.event?.changed_files || [];
  const requirements = payload.performance_requirements || {};

  const touchesImages = changedFiles.some((f) =>
    /\.(png|jpe?g|gif|svg|webp|avif)$/i.test(f),
  );
  const touchesFrontend = changedFiles.some((f) =>
    /\.(js|jsx|ts|tsx|css|scss|html)$/i.test(f),
  );
  const touchesBackend = changedFiles.some((f) =>
    /\.(js|ts|py|go|java|rb|php)$/i.test(f),
  );

  const findings = [
    makeFinding({
      type: "images",
      severity: touchesImages ? "high" : "medium",
      message:
        "Convertir imágenes críticas a AVIF/WebP y mantener fallback PNG/JPEG.",
      current: touchesImages
        ? "Cambios de imágenes detectados en este evento."
        : "Sin cambios explícitos de imágenes.",
      target: ">= 80% de imágenes above-the-fold en AVIF/WebP.",
    }),
    makeFinding({
      type: "js_css",
      severity: touchesFrontend ? "high" : "medium",
      message:
        "Aplicar code splitting, lazy loading y minificación en bundles de producción.",
      current: touchesFrontend
        ? "Cambios frontend detectados; riesgo de crecimiento de bundle principal."
        : "No hay evidencia de optimización de bundle en este payload.",
      target: requirements.bundle_size_kb
        ? `Bundle principal <= ${requirements.bundle_size_kb} KB.`
        : "Bundle principal controlado con budget explícito en CI.",
    }),
    makeFinding({
      type: "http_cache",
      severity: "medium",
      message:
        "Definir Cache-Control + ETag para reducir latencia y transferencias repetidas.",
      current: "No se recibió evidencia de políticas de cache en el payload.",
      target:
        "Assets versionados con cache immutable y validación condicional para dinámicos.",
    }),
    makeFinding({
      type: "redis_cache",
      severity: touchesBackend ? "medium" : "low",
      message:
        "Implementar cache Redis para endpoints costosos con TTL e invalidación por evento.",
      current: touchesBackend
        ? "Cambios backend detectados; oportunidad de cache para rutas calientes."
        : "Sin cambios backend relevantes, pero útil para escalabilidad.",
      target: "Hit ratio de cache > 60% en endpoints críticos.",
    }),
  ];

  return findings;
}

/**
 * Sección: scoring 0-100 + estado good/warning/critical.
 */
function scorePerformance({ findings, requirements, mode }) {
  let score = 92;

  findings.forEach((f) => {
    if (f.severity === "high") score -= 14;
    else if (f.severity === "medium") score -= 8;
    else score -= 3;
  });

  const unmetThresholds = [];

  if (requirements?.lcp_target_ms && requirements.lcp_target_ms < 1800) {
    score -= 5;
    unmetThresholds.push(
      `LCP objetivo muy estricto (${requirements.lcp_target_ms} ms)`,
    );
  }

  if (requirements?.bundle_size_kb && requirements.bundle_size_kb < 180) {
    score -= 5;
    unmetThresholds.push(
      `Bundle budget exigente (${requirements.bundle_size_kb} KB)`,
    );
  }

  score = Math.max(0, Math.min(100, score));

  let performanceStatus = "good";
  if (score < 80) performanceStatus = "warning";
  if (score < 60) performanceStatus = "critical";

  if (mode === "enforce_thresholds" && unmetThresholds.length > 0) {
    performanceStatus = "critical";
    score = Math.min(score, 59);
  }

  return { performanceScore: score, performanceStatus, unmetThresholds };
}

/**
 * Sección: impacto en pipeline (no bloqueante).
 */
function buildImpactOnPipeline(performanceStatus) {
  if (performanceStatus === "good") {
    return {
      confidence_delta: 0.08,
      recommended_action: "none",
    };
  }

  if (performanceStatus === "warning") {
    return {
      confidence_delta: -0.12,
      recommended_action: "review",
    };
  }

  return {
    confidence_delta: -0.3,
    recommended_action: "optimize_before_deploy",
  };
}

function buildRecommendations(findings) {
  const severityOrder = { high: 0, medium: 1, low: 2 };

  return findings
    .slice()
    .sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity])
    .map((finding) => {
      const priority =
        finding.severity === "high"
          ? "P0"
          : finding.severity === "medium"
            ? "P1"
            : "P2";
      return {
        priority,
        category: finding.type,
        title: finding.message,
        detail: `${finding.current} → ${finding.target}`,
        reason: `Severidad ${finding.severity} identificada en análisis VELOCISTA.`,
      };
    });
}

function buildActions(payload) {
  const actions = [
    {
      type: "command",
      command: "npm run build -- --minify",
      description: "Build optimizado con minificación.",
    },
    {
      type: "command",
      command: "npx vite build --reportCompressedSize",
      description: "Reporte de tamaños de bundle comprimidos (Vite).",
    },
    {
      type: "command",
      command: "npx webpack --mode production --json > stats.json",
      description: "Análisis de chunks / code splitting (Webpack).",
    },
    {
      type: "diff_plan",
      description:
        "1) migrar imágenes críticas a AVIF/WebP, 2) lazy-load de rutas no críticas, 3) headers de cache + Redis en endpoints caros.",
    },
  ];

  if (payload.mode === "enforce_thresholds") {
    actions.push({
      type: "command",
      command: "npm run perf:check",
      description:
        "Validar budgets y umbrales antes de deploy en modo enforce_thresholds.",
    });
  }

  return actions;
}

/**
 * Sección: artefacto markdown de archivado.
 */
async function writeMarkdownArtifact({
  pipelineId,
  executionId,
  correlationId,
  performanceScore,
  performanceStatus,
  impactOnPipeline,
  findings,
  startedAt,
  finishedAt,
  durationMs,
}) {
  const artifactsDir = path.join(process.cwd(), "artifacts", "velocista");
  await fs.mkdir(artifactsDir, { recursive: true });

  const artifactPath = path.join(artifactsDir, `${pipelineId}_performance.md`);

  const findingsMd = findings
    .map(
      (f, i) =>
        `${i + 1}. **${f.type}** (${f.severity})\n   - ${f.message}\n   - Current: ${f.current}\n   - Target: ${f.target}`,
    )
    .join("\n");

  const md = `# VELOCISTA Performance Report\n\n## Execution\n- Pipeline: ${pipelineId}\n- Execution ID: ${executionId}\n- Correlation ID: ${correlationId}\n- Started: ${startedAt}\n- Finished: ${finishedAt}\n- Duration: ${durationMs} ms\n\n## Summary\n- Performance Score: ${performanceScore}/100\n- Performance Status: ${performanceStatus}\n- Impact on Pipeline: confidence_delta=${impactOnPipeline.confidence_delta}, recommended_action=${impactOnPipeline.recommended_action}\n\n## Findings\n${findingsMd}\n`;

  await fs.writeFile(artifactPath, md, "utf8");
  return artifactPath;
}

function timeoutPromise(ms) {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(`Execution timeout after ${ms}ms`)), ms);
  });
}

async function executeOptimization(payload, startedAtIso, requestStartMs) {
  const trace = {
    received_at: startedAtIso,
    analysis_started_at: new Date().toISOString(),
    mode: payload.mode,
  };

  const findings = buildFindings(payload);
  const recommendations = buildRecommendations(findings);
  const actions = buildActions(payload);

  const { performanceScore, performanceStatus, unmetThresholds } =
    scorePerformance({
      findings,
      requirements: payload.performance_requirements || {},
      mode: payload.mode,
    });

  if (unmetThresholds.length) {
    findings.push(
      makeFinding({
        type: "thresholds",
        severity: payload.mode === "enforce_thresholds" ? "high" : "medium",
        message:
          "Se detectaron objetivos exigentes o potencialmente incumplibles para este cambio.",
        current: unmetThresholds.join("; "),
        target: "Alinear budgets con baseline medible por entorno.",
      }),
    );
  }

  const impactOnPipeline = buildImpactOnPipeline(performanceStatus);

  trace.analysis_finished_at = new Date().toISOString();
  const finishedAtMs = Date.now();
  const finishedAtIso = new Date(finishedAtMs).toISOString();
  const durationMs = formatMs(requestStartMs, finishedAtMs);

  const artifactPath = await writeMarkdownArtifact({
    pipelineId: payload.pipeline_id,
    executionId: payload.execution_id,
    correlationId: payload.correlation_id,
    performanceScore,
    performanceStatus,
    impactOnPipeline,
    findings,
    startedAt: startedAtIso,
    finishedAt: finishedAtIso,
    durationMs,
  });

  const report = {
    status: "success",
    metrics: {
      performance_score: performanceScore,
      performance_status: performanceStatus,
    },
    recommendations,
    actions,
    findings,
    trace: {
      ...trace,
      duration_ms: durationMs,
    },
  };

  return {
    status: "success",
    report,
    performanceScore,
    performanceStatus,
    impactOnPipeline,
    findings,
    artifactPath,
    finishedAtIso,
    durationMs,
  };
}

app.get("/health", (_req, res) => {
  res.status(200).json({ status: "ok", agent: "velocista" });
});

app.post("/execute", (req, res) => {
  const startTime = Date.now();
  const rawBody =
    req.body && typeof req.body === "object" && !Array.isArray(req.body)
      ? req.body
      : {};
  const actionNormalized = String(rawBody.action ?? "").toLowerCase();
  const meta =
    rawBody.meta && typeof rawBody.meta === "object" && !Array.isArray(rawBody.meta)
      ? rawBody.meta
      : undefined;

  console.log(
    JSON.stringify({
      event: "velocista_execute",
      action: actionNormalized,
      execution_id: meta?.execution_id ?? null,
    }),
  );

  try {
    const parsed = executeRequestSchema.safeParse(req.body);
    if (!parsed.success) {
      const duration_ms = Date.now() - startTime;
      const err = new Error("Invalid request body");
      console.error(
        JSON.stringify({
          event: "velocista_execute_error",
          action: actionNormalized,
          execution_id: meta?.execution_id ?? null,
          error: err.message,
          duration_ms,
        }),
      );
      return res.status(400).json(
        buildExecuteResponse({
          status: "error",
          action: actionNormalized,
          data: null,
          errors: zodIssuesToExecuteErrors(
            parsed.error.issues,
            "INVALID_REQUEST",
          ),
        }),
      );
    }

    const { action, payload, meta: metaParsed } = parsed.data;
    const actionNormalizedParsed = String(action ?? "").toLowerCase();
    const safePayload = toSafeExecutePayload(payload);
    const executionIdLog = metaParsed?.execution_id ?? null;

    if (!actionNormalizedParsed) {
      const duration_ms = Date.now() - startTime;
      console.error(
        JSON.stringify({
          event: "velocista_execute_error",
          action: actionNormalizedParsed,
          execution_id: executionIdLog,
          error: "INVALID_ACTION",
          duration_ms,
        }),
      );
      return res.status(400).json(
        buildExecuteResponse({
          status: "error",
          action: actionNormalizedParsed,
          data: null,
          errors: [
            {
              code: "INVALID_ACTION",
              message: "action is required and must be a non-empty string",
            },
          ],
        }),
      );
    }

    if (actionNormalizedParsed !== "analyze_event") {
      const duration_ms = Date.now() - startTime;
      console.error(
        JSON.stringify({
          event: "velocista_execute_error",
          action: actionNormalizedParsed,
          execution_id: executionIdLog,
          error: "INVALID_ACTION",
          duration_ms,
        }),
      );
      return res.status(400).json(
        buildExecuteResponse({
          status: "error",
          action: actionNormalizedParsed,
          data: null,
          errors: [
            {
              code: "INVALID_ACTION",
              message: `Unsupported or invalid action: ${actionNormalizedParsed}`,
            },
          ],
        }),
      );
    }

    const analyzed = validateAnalyzeEventPayload(safePayload);
    if (!analyzed.ok) {
      const duration_ms = Date.now() - startTime;
      console.error(
        JSON.stringify({
          event: "velocista_execute_error",
          action: actionNormalizedParsed,
          execution_id: executionIdLog,
          error: analyzed.errors[0]?.message || "VALIDATION_ERROR",
          duration_ms,
        }),
      );
      return res.status(400).json(
        buildExecuteResponse({
          status: "error",
          action: actionNormalizedParsed,
          data: null,
          errors: analyzed.errors,
        }),
      );
    }

    const data = analyzeEventFromValidatedPayload(analyzed.data);
    const duration_ms = Date.now() - startTime;
    console.log(
      JSON.stringify({
        event: "velocista_execute_success",
        action: actionNormalizedParsed,
        execution_id: executionIdLog,
        duration_ms,
      }),
    );

    return res.status(200).json(
      buildExecuteResponse({
        status: "success",
        action: actionNormalizedParsed,
        data,
        errors: [],
      }),
    );
  } catch (error) {
    const duration_ms = Date.now() - startTime;
    const err = error instanceof Error ? error : new Error(String(error));
    console.error(
      JSON.stringify({
        event: "velocista_execute_error",
        action: actionNormalized,
        execution_id: meta?.execution_id ?? null,
        error: err.message,
        duration_ms,
      }),
    );
    return res.status(500).json(
      buildExecuteResponse({
        status: "error",
        action: actionNormalized,
        data: null,
        errors: [
          {
            code: "INTERNAL_ERROR",
            message: err.message || "Unexpected error",
          },
        ],
      }),
    );
  }
});

app.post("/optimize", async (req, res) => {
  const requestStart = Date.now();
  const startedAtIso = new Date(requestStart).toISOString();

  const parsed = optimizeSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      agent: "velocista",
      status: "error",
      execution_id: req.body?.execution_id || null,
      correlation_id: req.body?.correlation_id || null,
      errors: parsed.error.issues.map((issue) => ({
        path: issue.path.join("."),
        message: issue.message,
      })),
      started_at: startedAtIso,
      finished_at: new Date().toISOString(),
      duration_ms: formatMs(requestStart, Date.now()),
    });
  }

  const payload = parsed.data;

  logStructured("info", "optimize_received", {
    execution_id: payload.execution_id,
    correlation_id: payload.correlation_id,
    pipeline_id: payload.pipeline_id,
    details: {
      mode: payload.mode,
      changed_files_count: payload.event.changed_files.length,
    },
  });

  try {
    const result = await Promise.race([
      executeOptimization(payload, startedAtIso, requestStart),
      timeoutPromise(EXECUTION_TIMEOUT_MS),
    ]);

    logStructured("info", "optimize_completed", {
      execution_id: payload.execution_id,
      correlation_id: payload.correlation_id,
      pipeline_id: payload.pipeline_id,
      details: {
        performance_score: result.performanceScore,
        performance_status: result.performanceStatus,
        impact_on_pipeline: result.impactOnPipeline,
      },
    });

    return res.status(200).json({
      agent: "velocista",
      status: result.status,
      execution_id: payload.execution_id,
      correlation_id: payload.correlation_id,
      performance_score: result.performanceScore,
      performance_status: result.performanceStatus,
      impact_on_pipeline: result.impactOnPipeline,
      findings: result.findings,
      artifact_path: result.artifactPath,
      started_at: startedAtIso,
      finished_at: result.finishedAtIso,
      duration_ms: result.durationMs,

      // Compatibilidad retroactiva
      pipeline_id: payload.pipeline_id,
      report: result.report,
    });
  } catch (error) {
    const finishedAtIso = new Date().toISOString();
    const durationMs = formatMs(requestStart, Date.now());

    logStructured("error", "optimize_failed", {
      execution_id: payload.execution_id,
      correlation_id: payload.correlation_id,
      pipeline_id: payload.pipeline_id,
      details: { message: error.message },
    });

    return res.status(500).json({
      agent: "velocista",
      status: "error",
      execution_id: payload.execution_id,
      correlation_id: payload.correlation_id,
      error: error.message,
      performance_score: 0,
      performance_status: "critical",
      impact_on_pipeline: {
        confidence_delta: -0.4,
        recommended_action: "optimize_before_deploy",
      },
      findings: [
        makeFinding({
          type: "runtime",
          severity: "high",
          message: "Error durante análisis de performance.",
          current: error.message,
          target: "Completar análisis sin excepciones.",
        }),
      ],
      artifact_path: null,
      started_at: startedAtIso,
      finished_at: finishedAtIso,
      duration_ms: durationMs,

      // Compatibilidad retroactiva
      pipeline_id: payload.pipeline_id,
      report: null,
    });
  }
});

app.listen(PORT, () => {
  console.log(`VELOCISTA agent running on port ${PORT}`);
});

/**
 * Ejemplo de request JSON
 * {
 *   "pipeline_id": "pipe-1234",
 *   "execution_id": "exec-2001",
 *   "correlation_id": "corr-xyz-90",
 *   "event": {
 *     "event_type": "push",
 *     "repository": "acme/web-app",
 *     "branch": "main",
 *     "author": "dev@acme.com",
 *     "timestamp": "2026-03-22T08:00:00Z",
 *     "summary": "Optimize home hero and cart flow",
 *     "changed_files": ["src/pages/Home.tsx", "public/hero.jpg"],
 *     "metadata": { "commit": "abc123" }
 *   },
 *   "performance_requirements": {
 *     "lcp_target_ms": 2500,
 *     "cls_target": 0.1,
 *     "tbt_target_ms": 200,
 *     "bundle_size_kb": 180,
 *     "image_formats_allowed": ["webp", "avif", "png"]
 *   },
 *   "mode": "enforce_thresholds"
 * }
 *
 * Ejemplo de response JSON
 * {
 *   "agent": "velocista",
 *   "status": "success",
 *   "execution_id": "exec-2001",
 *   "correlation_id": "corr-xyz-90",
 *   "performance_score": 62,
 *   "performance_status": "warning",
 *   "impact_on_pipeline": {
 *     "confidence_delta": -0.12,
 *     "recommended_action": "review"
 *   },
 *   "findings": [
 *     {
 *       "type": "images",
 *       "severity": "high",
 *       "message": "Convertir imágenes críticas a AVIF/WebP y mantener fallback PNG/JPEG.",
 *       "current": "Cambios de imágenes detectados en este evento.",
 *       "target": ">= 80% de imágenes above-the-fold en AVIF/WebP."
 *     }
 *   ],
 *   "artifact_path": "/app/artifacts/velocista/pipe-1234_performance.md",
 *   "started_at": "2026-03-22T08:00:00.000Z",
 *   "finished_at": "2026-03-22T08:00:00.300Z",
 *   "duration_ms": 300
 * }
 */
