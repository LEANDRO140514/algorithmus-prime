const axios = require("axios");
const logger = require("./logger");

const INVALID_RESPONSE = "Invalid response from cronista_service";

function isPlainObject(value) {
  return (
    value !== null &&
    value !== undefined &&
    typeof value === "object" &&
    !Array.isArray(value)
  );
}

async function callCronista({
  cronistaUrl,
  action,
  payload,
  meta,
  timeoutMs = 5000,
}) {
  const execution_id = meta?.execution_id ?? null;

  logger.debug({
    event: "cronista_request",
    action,
    execution_id,
  });

  try {
    const { data } = await axios.post(
      `${String(cronistaUrl || "").replace(/\/$/, "")}/execute`,
      { action, payload, meta },
      {
        timeout: timeoutMs,
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!isPlainObject(data)) {
      throw new Error(INVALID_RESPONSE);
    }

    logger.debug({
      event: "cronista_response",
      action,
      status: data.status,
      execution_id,
    });

    return data;
  } catch (err) {
    if (err?.message === INVALID_RESPONSE) {
      throw err;
    }

    const message =
      err?.response?.data?.errors?.[0]?.message || err?.message || "Unknown error";

    logger.error({
      event: "cronista_request_failed",
      error: message,
      execution_id,
    });

    throw new Error(`Cronista error: ${message}`);
  }
}

module.exports = { callCronista };
