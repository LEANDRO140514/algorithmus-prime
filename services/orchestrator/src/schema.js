const path = require('node:path');
const Ajv = require('ajv');

const schemaPath = path.resolve(__dirname, '..', 'contracts', 'pipeline_event.schema.json');
const schema = require(schemaPath);

const ajv = new Ajv({ allErrors: true, strict: false });
const validator = ajv.compile(schema);

function validateInput(payload) {
  const valid = validator(payload || {});
  if (valid) {
    return { valid: true, errors: [] };
  }

  return {
    valid: false,
    errors: (validator.errors || []).map((err) => ({
      path: err.instancePath || '/',
      message: err.message
    }))
  };
}

module.exports = { validateInput };