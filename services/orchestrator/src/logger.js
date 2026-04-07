function log(level, payload) {
  const record = {
    level,
    timestamp: new Date().toISOString(),
    ...payload
  };
  console.log(JSON.stringify(record));
}

function info(payload) {
  log('info', payload);
}

function error(payload) {
  log('error', payload);
}

function debug(payload) {
  log('debug', payload);
}

module.exports = { info, error, debug };