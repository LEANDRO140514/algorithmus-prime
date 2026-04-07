const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const axios = require("axios");
const { v4: uuidv4 } = require("uuid");
const logger = require("./logger");

function nowISO() {
  return new Date().toISOString();
}

function elapsedMs(startHrtime) {
  const diff = process.hrtime.bigint() - startHrtime;
  return Number(diff / BigInt(1e6));
}

function buildPipelineState(context) {
  return {
    execution_id: context.execution_id,
    status: "started",
    steps: {
      velocista: {
        agent: "velocista",
        status: "pending",
        data: null,
        errors: [],
        completed_at: null,
      },
      guardia: {
        agent: "guardia",
        status: "pending",
        data: null,
        errors: [],
        completed_at: null,
      },
      cronista: {
        agent: "cronista",
        status: "skipped",
        data: null,
        errors: [],
        completed_at: null,
      },
    },
    started_at: context.started_at,
    completed_at: null,
  };
}

function setStepStatus(pipelineState, agent, status) {
  const step = pipelineState.steps[agent];
  if (!step) return;
  // Cronista schedule step is owned by index.js; HTTP /update does not mutate steps.cronista.
  if (agent === "cronista") return;
  step.status = status;
}

function handleError(errors, context, agent, err, extra = {}) {
  const structured = {
    execution_id: context.execution_id,
    correlation_id: context.correlation_id,
    pipeline_id: context.pipeline_id,
    trace_id: context.trace_id || null,
    agent,
    event: "agent_error",
    code: err.code || null,
    message: err.message || "Unknown error",
    http_status: err.response?.status || null,
    ...extra,
  };

  errors.push(structured);
  logger.error(structured);
  return structured;
}

async function requestWithRetry(url, payload, timeoutMs) {
  try {
    return await axios.post(url, payload, {
      timeout: timeoutMs,
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    const hasResponse = !!err.response;
    const is4xx =
      hasResponse && err.response.status >= 400 && err.response.status < 500;
    if (hasResponse || is4xx) throw err;

    return axios.post(url, payload, {
      timeout: timeoutMs,
      headers: { "Content-Type": "application/json" },
    });
  }
}

/**
 * Velocista agent step: POST /execute (analyze_event). Mutates pipelineState.steps.velocista.
 */
async function runVelocistaExecuteStep({
  url,
  inputEvent,
  context,
  timeoutMs,
  pipelineState,
}) {
  const startTime = Date.now();
  const execution_id = context?.execution_id ?? null;
  const correlation_id = context?.correlation_id ?? null;

  logger.info({
    event: "velocista_step_started",
    execution_id,
    correlation_id,
  });

  const payload =
    inputEvent && typeof inputEvent === "object" ? inputEvent : {};

  const body = {
    action: "analyze_event",
    payload,
    meta: {
      execution_id,
      correlation_id,
      source: "orchestrator",
    },
  };

  try {
    const response = await requestWithRetry(url, body, timeoutMs);
    const rawBody =
      response && typeof response.data === "object"
        ? response.data
        : null;

    if (rawBody === null) {
      const duration_ms = Date.now() - startTime;
      const emptyErrors = [
        {
          code: "EMPTY_RESPONSE",
          message: "Velocista returned empty or invalid response",
        },
      ];
      pipelineState.steps.velocista = {
        agent: "velocista",
        status: "error",
        data: null,
        errors: emptyErrors,
        completed_at: new Date().toISOString(),
      };
      logger.error({
        event: "velocista_step_failure",
        execution_id,
        correlation_id,
        error: emptyErrors[0].message,
      });
      const httpStatus =
        response && typeof response.status === "number" ? response.status : null;
      return {
        agent: "velocista",
        status: "error",
        http_status: httpStatus,
        data: null,
        error: {
          message: emptyErrors[0].message,
          code: "EMPTY_RESPONSE",
        },
        duration_ms,
        errors: emptyErrors,
      };
    }

    const safeBody =
      rawBody && typeof rawBody === "object" ? rawBody : {};
    const status =
      safeBody.status === "success" || safeBody.status === "error"
        ? safeBody.status
        : "error";
    const data =
      safeBody.data && typeof safeBody.data === "object"
        ? safeBody.data
        : null;
    const errors = Array.isArray(safeBody.errors) ? safeBody.errors : [];

    pipelineState.steps.velocista = {
      agent: "velocista",
      status,
      data,
      errors,
      completed_at: new Date().toISOString(),
    };

    const duration_ms = Date.now() - startTime;

    if (status === "success") {
      logger.info({
        event: "velocista_step_success",
        execution_id,
        correlation_id,
      });
    } else {
      const errMsg =
        errors[0] && typeof errors[0] === "object" && errors[0].message
          ? String(errors[0].message)
          : "velocista returned error status";
      logger.error({
        event: "velocista_step_failure",
        execution_id,
        correlation_id,
        error: errMsg,
      });
    }

    const httpStatus =
      response && typeof response.status === "number" ? response.status : null;

    return {
      agent: "velocista",
      status: status === "success" ? "success" : "error",
      http_status: httpStatus,
      data,
      error:
        status === "success"
          ? null
          : {
              message:
                errors[0] && typeof errors[0] === "object" && errors[0].message
                  ? String(errors[0].message)
                  : "Velocista error",
            },
      duration_ms,
    };
  } catch (error) {
    const duration_ms = Date.now() - startTime;
    const message =
      error != null && typeof error.message === "string"
        ? error.message
        : "Unknown error";
    const isTimeout =
      error?.code === "ECONNABORTED" ||
      (typeof error?.message === "string" &&
        error.message.includes("timeout"));
    const timeoutErrors = [
      {
        code: "VELOCISTA_TIMEOUT",
        message: "Velocista request timed out",
      },
    ];
    const callFailedErrors = [
      {
        code: "VELOCISTA_CALL_FAILED",
        message,
      },
    ];
    const stepErrors = isTimeout ? timeoutErrors : callFailedErrors;

    pipelineState.steps.velocista = {
      agent: "velocista",
      status: "error",
      data: null,
      errors: stepErrors,
      completed_at: new Date().toISOString(),
    };
    logger.error({
      event: "velocista_step_failure",
      execution_id,
      correlation_id,
      error: isTimeout ? timeoutErrors[0].message : message,
    });
    return {
      agent: "velocista",
      status: "error",
      http_status: error?.response?.status ?? null,
      data: null,
      error: {
        message: isTimeout ? timeoutErrors[0].message : message,
        code: isTimeout ? "VELOCISTA_TIMEOUT" : error?.code ?? null,
      },
      duration_ms,
      errors: stepErrors,
    };
  }
}

/**
 * Guardia agent step: POST /execute (validate_security). Mutates pipelineState.steps.guardia.
 */
async function runGuardiaExecuteStep({
  url,
  inputEvent,
  context,
  timeoutMs,
  pipelineState,
}) {
  const startTime = Date.now();
  const execution_id = context?.execution_id ?? null;
  const correlation_id = context?.correlation_id ?? null;

  logger.info({
    event: "guardia_step_started",
    execution_id,
    correlation_id,
  });

  const payload =
    inputEvent && typeof inputEvent === "object" ? inputEvent : {};

  const body = {
    action: "validate_security",
    payload,
    meta: {
      execution_id,
      correlation_id,
      source: "orchestrator",
    },
  };

  try {
    const response = await requestWithRetry(url, body, timeoutMs);
    const rawBody =
      response && typeof response.data === "object"
        ? response.data
        : null;

    if (rawBody === null) {
      const duration_ms = Date.now() - startTime;
      const emptyErrors = [
        {
          code: "EMPTY_RESPONSE",
          message: "Guardia returned empty or invalid response",
        },
      ];
      pipelineState.steps.guardia = {
        agent: "guardia",
        status: "error",
        data: null,
        errors: emptyErrors,
        completed_at: new Date().toISOString(),
      };
      logger.error({
        event: "guardia_step_failure",
        execution_id,
        correlation_id,
        error: emptyErrors[0].message,
      });
      const httpStatus =
        response && typeof response.status === "number" ? response.status : null;
      return {
        agent: "guardia",
        status: "error",
        http_status: httpStatus,
        data: null,
        error: {
          message: emptyErrors[0].message,
          code: "EMPTY_RESPONSE",
        },
        duration_ms,
        errors: emptyErrors,
      };
    }

    const safeBody =
      rawBody && typeof rawBody === "object" ? rawBody : {};
    const status =
      safeBody.status === "success" || safeBody.status === "error"
        ? safeBody.status
        : "error";
    const data =
      safeBody.data && typeof safeBody.data === "object"
        ? safeBody.data
        : null;
    const errors = Array.isArray(safeBody.errors) ? safeBody.errors : [];

    pipelineState.steps.guardia = {
      agent: "guardia",
      status,
      data,
      errors,
      completed_at: new Date().toISOString(),
    };

    const duration_ms = Date.now() - startTime;

    if (status === "success") {
      logger.info({
        event: "guardia_step_success",
        execution_id,
        correlation_id,
      });
    } else {
      const errMsg =
        errors[0] && typeof errors[0] === "object" && errors[0].message
          ? String(errors[0].message)
          : "guardia returned error status";
      logger.error({
        event: "guardia_step_failure",
        execution_id,
        correlation_id,
        error: errMsg,
      });
    }

    const httpStatus =
      response && typeof response.status === "number" ? response.status : null;

    return {
      agent: "guardia",
      status: status === "success" ? "success" : "error",
      http_status: httpStatus,
      data,
      error:
        status === "success"
          ? null
          : {
              message:
                errors[0] && typeof errors[0] === "object" && errors[0].message
                  ? String(errors[0].message)
                  : "Guardia error",
            },
      duration_ms,
    };
  } catch (error) {
    const duration_ms = Date.now() - startTime;
    const message =
      error != null && typeof error.message === "string"
        ? error.message
        : "Unknown error";
    const stepErrors = [
      {
        code: "GUARDIA_CALL_FAILED",
        message,
      },
    ];

    pipelineState.steps.guardia = {
      agent: "guardia",
      status: "error",
      data: null,
      errors: stepErrors,
      completed_at: new Date().toISOString(),
    };
    logger.error({
      event: "guardia_step_failure",
      execution_id,
      correlation_id,
      error: message,
    });
    return {
      agent: "guardia",
      status: "error",
      http_status: error?.response?.status ?? null,
      data: null,
      error: {
        message,
        code: "GUARDIA_CALL_FAILED",
      },
      duration_ms,
      errors: stepErrors,
    };
  }
}

async function executeAgent({
  agent,
  url,
  payload,
  timeoutMs,
  context,
  pipelineState,
  errors,
}) {
  const started = process.hrtime.bigint();
  pipelineState.status = "in_progress";
  logger.info({
    execution_id: context.execution_id,
    correlation_id: context.correlation_id,
    pipeline_id: context.pipeline_id,
    trace_id: context.trace_id || null,
    agent,
    event: "agent_started",
  });

  try {
    const response = await requestWithRetry(url, payload, timeoutMs);
    setStepStatus(pipelineState, agent, "ok");
    logger.info({
      execution_id: context.execution_id,
      correlation_id: context.correlation_id,
      pipeline_id: context.pipeline_id,
      trace_id: context.trace_id || null,
      agent,
      event: "agent_success",
      http_status: response.status,
    });
    return {
      agent,
      status: "success",
      http_status: response.status,
      data: response.data,
      error: null,
      duration_ms: elapsedMs(started),
    };
  } catch (err) {
    const timeout = err.code === "ECONNABORTED";
    setStepStatus(pipelineState, agent, "failed");
    handleError(errors, context, agent, err, { timeout });
    return {
      agent,
      status: "error",
      http_status: err.response?.status || null,
      data: null,
      error: {
        message: err.message,
        code: err.code || null,
        timeout,
      },
      duration_ms: elapsedMs(started),
    };
  }
}

function validateAgentResponse(agent, result, context, errors) {
  if (!result || typeof result !== "object") {
    handleError(
      errors,
      context,
      agent,
      new Error("Invalid agent result envelope"),
      { event: "agent_response_validation" },
    );
    return false;
  }

  if (result.status === "error") {
    return false;
  }

  if (agent === "guardia") {
    if (result.data != null && typeof result.data !== "object") {
      handleError(
        errors,
        context,
        agent,
        new Error("Invalid agent response payload"),
        { event: "agent_response_validation" },
      );
      return false;
    }
  } else if (!result.data || typeof result.data !== "object") {
    handleError(
      errors,
      context,
      agent,
      new Error("Invalid agent response payload"),
      { event: "agent_response_validation" },
    );
    return false;
  }

  return true;
}

function evaluateDecision(velocistaResult, guardiaResult) {
  const guardiaDataRaw = guardiaResult?.data;

  const guardiaData = {
    allow:
      typeof guardiaDataRaw?.allow === "boolean"
        ? guardiaDataRaw.allow
        : typeof guardiaDataRaw?.deployment_allowed === "boolean"
          ? guardiaDataRaw.deployment_allowed
          : null,

    reason:
      typeof guardiaDataRaw?.reason === "string"
        ? guardiaDataRaw.reason
        : "",
  };

  const velocistaData =
    velocistaResult?.data && typeof velocistaResult.data === "object"
      ? velocistaResult.data
      : null;

  let decision;
  let deployment_allowed;
  let reason;

  if (guardiaData.allow === false) {
    decision = "BLOCKED";
    deployment_allowed = false;
    reason = guardiaData.reason;
  } else if (velocistaData?.requires_review === true) {
    decision = "REVIEW";
    deployment_allowed = false;
    reason = "High risk detected by Velocista";
  } else {
    decision = "APPROVED";
    deployment_allowed = true;
    reason = "Safe to deploy";
  }

  let riskScore = 20;
  if (decision === "REVIEW") riskScore = 65;
  if (decision === "BLOCKED") riskScore = 95;

  const decision_factors =
    decision === "BLOCKED"
      ? {
          security: reason,
          performance: `Velocista status: ${velocistaResult?.status ?? "unknown"}.`,
        }
      : decision === "REVIEW"
        ? {
            security: "Guardia allows deployment.",
            performance: reason,
          }
        : {
            security: "Guardia allows deployment.",
            performance: reason,
          };

  return {
    decision,
    deployment_allowed,
    reason,
    risk_score: riskScore,
    decision_factors,
  };
}

function maybeGenerateDeploymentToken(finalDecision) {
  if (finalDecision !== "APPROVED") {
    return { deployment_token: null, token_expires_at: null };
  }

  const deploymentToken = crypto.randomBytes(18).toString("hex");
  const expiry = new Date(Date.now() + 15 * 60 * 1000).toISOString();
  return { deployment_token: deploymentToken, token_expires_at: expiry };
}

function buildExecutiveReport({
  finalDecision,
  riskScore,
  decisionFactors,
  guardiaResult,
  velocistaResult,
}) {
  const status =
    finalDecision === "APPROVED"
      ? "success"
      : finalDecision === "REVIEW"
        ? "partial"
        : "fail";
  const findings = [
    `Security: ${decisionFactors.security}`,
    `Performance: ${decisionFactors.performance}`,
    `Guardia status: ${guardiaResult.status}`,
    `Velocista status: ${velocistaResult.status}`,
  ];

  const recommendation =
    finalDecision === "APPROVED"
      ? "Proceder con despliegue automático."
      : finalDecision === "REVIEW"
        ? "Revisión manual requerida antes de desplegar."
        : "Bloquear despliegue hasta resolver hallazgos de seguridad/disponibilidad.";

  return {
    status,
    summary: `Decisión final ${finalDecision} con risk_score ${riskScore}.`,
    findings,
    recommendation,
  };
}

function inferDeploymentAllowed(guardiaResult) {
  if (
    !guardiaResult ||
    guardiaResult.status !== "success" ||
    !guardiaResult.data
  )
    return false;
  const source = guardiaResult.data;
  if (typeof source.deployment_allowed === "boolean")
    return source.deployment_allowed;
  if (typeof source.allow_deploy === "boolean") return source.allow_deploy;
  if (typeof source.allowed === "boolean") return source.allowed;
  if (typeof source.approved === "boolean") return source.approved;
  return false;
}

function extractDeploymentToken(guardiaResult) {
  if (
    !guardiaResult ||
    !guardiaResult.data ||
    typeof guardiaResult.data !== "object"
  )
    return null;
  return (
    guardiaResult.data.deployment_token || guardiaResult.data.token || null
  );
}

function collectArtifactPaths(results) {
  const found = [];
  Object.values(results).forEach((item) => {
    if (
      !item ||
      typeof item !== "object" ||
      !item.data ||
      typeof item.data !== "object"
    )
      return;
    if (typeof item.data.artifact_path === "string")
      found.push(item.data.artifact_path);
    if (Array.isArray(item.data.artifact_paths)) {
      found.push(
        ...item.data.artifact_paths.filter((v) => typeof v === "string"),
      );
    }
  });
  return [...new Set(found)];
}

function buildOverallStatus(results) {
  const statuses = Object.values(results).map((r) => r.status);
  const hasSuccess = statuses.includes("success");
  const hasError = statuses.includes("error");
  if (!hasError) return "success";
  if (hasSuccess) return "partial";
  return "fail";
}

async function persistArtifacts(executionId, report, artifactsBaseDir) {
  const dir = path.resolve(artifactsBaseDir, "orchestrator");
  await fs.mkdir(dir, { recursive: true });
  const artifactPath = path.join(dir, `${executionId}.json`);
  await fs.writeFile(artifactPath, JSON.stringify(report, null, 2), "utf-8");
  return artifactPath;
}

function maskToken(token) {
  if (!token || typeof token !== "string") return null;
  if (token.length < 7) return `${token[0] || "*"}***`;
  return `${token.slice(0, 3)}***${token.slice(-3)}`;
}

async function appendDashboard(report, dashboardPath) {
  const lines = [
    `\n## Pipeline ${report.pipeline_id}`,
    `- timestamp: ${report.finished_at}`,
    `- execution_id: ${report.execution_id}`,
    `- correlation_id: ${report.correlation_id}`,
    `- event: ${report.input_event.event_type} @ ${report.input_event.repository} (${report.input_event.branch})`,
    `- estado: ${report.overall_status}`,
    `- final_decision: ${report.final_decision}`,
    `- deployment_allowed: ${report.deployment_allowed}`,
    `- token: ${report.deployment_token ? maskToken(report.deployment_token) : "N/A"}`,
    `- artifacts: ${report.artifact_paths.length ? report.artifact_paths.join(", ") : "N/A"}`,
  ].join("\n");

  await fs.mkdir(path.dirname(dashboardPath), { recursive: true });
  await fs.appendFile(dashboardPath, `${lines}\n`, "utf-8");
}

function createContext(payload) {
  return {
    version: payload.version || "1.0",
    pipeline_id: payload.pipeline_id || uuidv4(),
    execution_id: payload.execution_id || uuidv4(),
    correlation_id: payload.correlation_id || uuidv4(),
    trace_id: payload.trace_id || null,
    started_at: nowISO(),
    event: payload.event,
  };
}

module.exports = {
  buildPipelineState,
  executeAgent,
  runVelocistaExecuteStep,
  runGuardiaExecuteStep,
  handleError,
  validateAgentResponse,
  evaluateDecision,
  maybeGenerateDeploymentToken,
  buildExecutiveReport,
  inferDeploymentAllowed,
  extractDeploymentToken,
  collectArtifactPaths,
  buildOverallStatus,
  persistArtifacts,
  appendDashboard,
  createContext,
  nowISO,
  elapsedMs,
  logger,
};
