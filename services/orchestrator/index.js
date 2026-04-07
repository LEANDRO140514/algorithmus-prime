const express = require("express");
const {
  buildPipelineState,
  executeAgent,
  runVelocistaExecuteStep,
  runGuardiaExecuteStep,
  validateAgentResponse,
  evaluateDecision,
  maybeGenerateDeploymentToken,
  buildExecutiveReport,
  extractDeploymentToken,
  collectArtifactPaths,
  buildOverallStatus,
  persistArtifacts,
  appendDashboard,
  createContext,
  nowISO,
  elapsedMs,
  logger,
  handleError,
} = require("./src/orchestrator");
const { validateInput } = require("./src/schema");
const { callCronista } = require("./src/cronistaClient");

const app = express();
app.use(express.json({ limit: "2mb" }));

const config = {
  port: Number(process.env.PORT || 4000),
  velocistaUrl: process.env.VELOCISTA_URL || "http://velocista:3001",
  guardiaUrl: process.env.GUARDIA_URL || "http://guardia:3002",
  cronistaUrl: process.env.CRONISTA_URL || "http://cronista:3003",
  requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 5000),
  velocistaTimeoutMs: Number(
    process.env.VELOCISTA_TIMEOUT_MS || process.env.REQUEST_TIMEOUT_MS || 5000,
  ),
  guardiaTimeoutMs: Number(
    process.env.GUARDIA_TIMEOUT_MS || process.env.REQUEST_TIMEOUT_MS || 5000,
  ),
  cronistaTimeoutMs: Number(
    process.env.CRONISTA_TIMEOUT_MS || process.env.REQUEST_TIMEOUT_MS || 5000,
  ),
  dashboardPath:
    process.env.DASHBOARD_PATH || "/workspace/DASHBOARD_ALGORITHMUS.md",
  artifactsBaseDir: process.env.ARTIFACTS_BASE_DIR || "/app/artifacts",
};

app.get("/health", (_req, res) => {
  res.status(200).json({
    status: "ok",
    service: "orchestrator",
    timestamp: nowISO(),
  });
});

app.post("/pipeline", async (req, res) => {
  const input = req.body || {};
  const validation = validateInput(input);

  if (!validation.valid) {
    return res.status(400).json({
      error: "validation_error",
      details: validation.errors,
    });
  }

  const context = createContext(input);
  const pipelineState = buildPipelineState(context);
  const errors = [];
  const started = process.hrtime.bigint();

  logger.info({
    execution_id: context.execution_id,
    correlation_id: context.correlation_id,
    pipeline_id: context.pipeline_id,
    trace_id: context.trace_id,
    agent: "orchestrator",
    event: "pipeline_started",
  });

  const agentPayload = {
    ...input,
    version: context.version,
    pipeline_id: context.pipeline_id,
    execution_id: context.execution_id,
    correlation_id: context.correlation_id,
    trace_id: context.trace_id,
  };

  const [velocistaResult, guardiaResult] = await Promise.all([
    runVelocistaExecuteStep({
      url: `${config.velocistaUrl.replace(/\/$/, "")}/execute`,
      inputEvent: input.event,
      context,
      timeoutMs: config.velocistaTimeoutMs,
      pipelineState,
    }),
    runGuardiaExecuteStep({
      url: `${config.guardiaUrl.replace(/\/$/, "")}/execute`,
      inputEvent: input.event,
      context,
      timeoutMs: config.guardiaTimeoutMs,
      pipelineState,
    }),
  ]);

  validateAgentResponse("velocista", velocistaResult, context, errors);
  validateAgentResponse("guardia", guardiaResult, context, errors);

  const degradedMode = velocistaResult.status === "error";
  if (degradedMode) {
    logger.info({
      execution_id: context.execution_id,
      correlation_id: context.correlation_id,
      pipeline_id: context.pipeline_id,
      agent: "orchestrator",
      event: "degraded_mode_enabled",
      reason: "velocista_failed",
    });
  }

  const decision = evaluateDecision(velocistaResult, guardiaResult);

  pipelineState.decision = {
    decision: decision.decision,
    deployment_allowed: decision.deployment_allowed,
    reason: decision.reason,
  };

  logger.info({
    event: "decision_made",
    execution_id: context.execution_id,
    correlation_id: context.correlation_id,
    decision: decision.decision,
    deployment_allowed: decision.deployment_allowed,
  });

  const tokenBundle = maybeGenerateDeploymentToken(decision.decision);

  const executiveReport = buildExecutiveReport({
    finalDecision: decision.decision,
    riskScore: decision.risk_score,
    decisionFactors: decision.decision_factors,
    guardiaResult,
    velocistaResult,
  });

  const deployment_allowed = decision.deployment_allowed;

  if (!deployment_allowed) {
    const reason = "deployment_not_allowed";
    pipelineState.steps.cronista = {
      agent: "cronista",
      status: "skipped",
      data: null,
      errors: [],
      completed_at: new Date().toISOString(),
      ...(reason && { reason }),
    };
    logger.info({
      event: "cronista_step_skipped",
      execution_id: context.execution_id,
      correlation_id: context.correlation_id,
    });
  } else {
    logger.info({
      event: "cronista_step_started",
      deployment_allowed,
      execution_id: context.execution_id,
      correlation_id: context.correlation_id,
    });

    try {
      const repository = input?.event?.repository || "unknown";
      const branch = input?.event?.branch || "unknown";

      const result = await callCronista({
        cronistaUrl: config.cronistaUrl,
        action: "create_schedule",
        payload: {
          name: `pipeline_${context.execution_id}_${Date.now()}`,
          cron: "0 9 * * *",
          task: "run_pipeline",
          repository,
          branch,
        },
        meta: {
          execution_id: context.execution_id,
          correlation_id: context.correlation_id,
          source: "orchestrator",
        },
        timeoutMs: config.cronistaTimeoutMs,
      });

      const safeBody = result && typeof result === "object" ? result : {};
      const cronistaStatus = safeBody.status || "error";
      const data =
        safeBody.data && typeof safeBody.data === "object"
          ? safeBody.data
          : null;
      const errors = Array.isArray(safeBody.errors) ? safeBody.errors : [];
      const has_errors = errors.length > 0;
      const stepFailed =
        cronistaStatus === "error" || has_errors;
      const normalizedStatus = stepFailed
        ? "error"
        : cronistaStatus === "success"
          ? "success"
          : cronistaStatus === "skipped"
            ? "skipped"
            : "unknown";

      pipelineState.steps.cronista = {
        agent: "cronista",
        status: normalizedStatus,
        data,
        errors,
        completed_at: new Date().toISOString(),
      };

      if (stepFailed) {
        let failureMessage = "Cronista step failed";
        if (errors.length > 0) {
          const first = errors[0];
          if (typeof first === "string") failureMessage = first;
          else if (first && typeof first === "object" && first.message) {
            failureMessage = String(first.message);
          }
        } else {
          failureMessage = `Cronista returned status ${cronistaStatus}`;
        }
        logger.error({
          event: "cronista_step_failure",
          execution_id: context.execution_id,
          correlation_id: context.correlation_id,
          error: failureMessage,
          cronista_status: cronistaStatus,
          errors_count: errors.length,
        });
      } else {
        logger.info({
          event: "cronista_step_success",
          execution_id: context.execution_id,
          correlation_id: context.correlation_id,
          cronista_status: cronistaStatus,
          has_errors: has_errors,
        });
      }
    } catch (error) {
      const message = error?.message || "Unknown error";
      const transportErrors = [
        {
          code: "CRONISTA_CALL_FAILED",
          message,
        },
      ];
      pipelineState.steps.cronista = {
        agent: "cronista",
        status: "error",
        data: null,
        errors: transportErrors,
        completed_at: new Date().toISOString(),
      };
      logger.error({
        event: "cronista_step_failure",
        execution_id: context.execution_id,
        correlation_id: context.correlation_id,
        error: message,
        cronista_status: null,
        errors_count: transportErrors.length,
      });
    }
  }

  const preReport = {
    version: context.version,
    pipeline_id: context.pipeline_id,
    execution_id: context.execution_id,
    correlation_id: context.correlation_id,
    trace_id: context.trace_id,
    started_at: context.started_at,
    finished_at: nowISO(),
    duration_ms: elapsedMs(started),
    input_event: context.event,
    pipeline_state: pipelineState,
    final_decision: decision.decision,
    risk_score: decision.risk_score,
    decision_factors: decision.decision_factors,
    executive_report: executiveReport,
    results: {
      velocista: velocistaResult,
      guardia: guardiaResult,
      cronista_update: null,
    },
    deployment_allowed,
    deployment_token:
      tokenBundle.deployment_token || extractDeploymentToken(guardiaResult),
    token_expires_at: tokenBundle.token_expires_at,
    overall_status: buildOverallStatus({
      velocista: velocistaResult,
      guardia: guardiaResult,
    }),
    executive_summary: executiveReport.summary,
    artifact_paths: collectArtifactPaths({
      velocista: velocistaResult,
      guardia: guardiaResult,
    }),
  };

  const cronistaResult = await executeAgent({
    agent: "cronista",
    url: `${config.cronistaUrl}/update`,
    payload: {
      ...agentPayload,
      agent_reports: {
        velocista: velocistaResult,
        guardia: guardiaResult,
      },
      final_decision: decision.decision,
      executive_report: executiveReport,
    },
    timeoutMs: config.cronistaTimeoutMs,
    context,
    pipelineState,
    errors,
  });

  validateAgentResponse("cronista", cronistaResult, context, errors);

  pipelineState.completed_at = nowISO();
  const velocistaStepFailed =
    pipelineState.steps.velocista.status === "error";
  const guardiaStepFailed = pipelineState.steps.guardia.status === "error";
  const cronistaScheduleFailed =
    pipelineState.steps.cronista.status === "error";
  const cronistaHttpFailed = cronistaResult.status === "error";
  pipelineState.status =
    velocistaStepFailed ||
    guardiaStepFailed ||
    cronistaScheduleFailed ||
    cronistaHttpFailed
      ? "failed"
      : "completed";

  const finalReport = {
    ...preReport,
    finished_at: pipelineState.completed_at,
    duration_ms: elapsedMs(started),
    pipeline_state: pipelineState,
    results: {
      velocista: velocistaResult,
      guardia: guardiaResult,
      cronista_update: cronistaResult,
    },
  };

  finalReport.artifact_paths = collectArtifactPaths(finalReport.results);
  finalReport.overall_status = buildOverallStatus({
    velocista: velocistaResult,
    guardia: guardiaResult,
    cronista: cronistaResult,
  });

  try {
    const artifactPath = await persistArtifacts(
      context.execution_id,
      finalReport,
      config.artifactsBaseDir,
    );
    finalReport.artifact_paths = [
      ...new Set([...finalReport.artifact_paths, artifactPath]),
    ];
  } catch (err) {
    handleError(errors, context, "artifact_writer", err, {
      stage: "persist_artifact",
    });
  }

  try {
    await appendDashboard(finalReport, config.dashboardPath);
  } catch (err) {
    handleError(errors, context, "dashboard_update", err, {
      stage: "append_dashboard",
    });
  }

  logger.info({
    execution_id: context.execution_id,
    correlation_id: context.correlation_id,
    pipeline_id: context.pipeline_id,
    trace_id: context.trace_id,
    agent: "orchestrator",
    event: "pipeline_completed",
    status: finalReport.overall_status,
    final_decision: finalReport.final_decision,
  });

  return res.status(200).json({
    execution_id: context.execution_id,
    correlation_id: context.correlation_id,
    trace_id: context.trace_id,
    pipeline_state: pipelineState,
    results: {
      velocista: velocistaResult,
      guardia: guardiaResult,
      cronista: cronistaResult,
    },
    errors,
    // Compatibilidad previa:
    ...finalReport,
  });
});

app.listen(config.port, () => {
  logger.info({
    execution_id: null,
    correlation_id: null,
    pipeline_id: null,
    trace_id: null,
    agent: "orchestrator",
    event: "service_started",
    port: config.port,
  });
});
