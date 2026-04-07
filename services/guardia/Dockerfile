FROM python:3.11-slim

WORKDIR /app

# Dependencias mínimas para escaneo de seguridad.
RUN apt-get update && apt-get install -y --no-install-recommends \
    npm \
    && rm -rf /var/lib/apt/lists/*

COPY guardia/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
    pip-audit \
    bandit

COPY guardia/app.py /app/app.py
RUN mkdir -p /app/artifacts/guardia

# Endurecimiento básico: ejecución no-root y defaults del Security Gate.
RUN addgroup --system guardia && adduser --system --ingroup guardia guardia && chown -R guardia:guardia /app
USER guardia

ENV PYTHONUNBUFFERED=1 \
    ALLOWED_SCAN_BASE=/workspace \
    MAX_AUDIT_DURATION_SECONDS=120 \
    NETWORK_CHECK_TIMEOUT_SECONDS=7 \
    ALLOWED_NETWORK_HOSTS=

EXPOSE 3002

# Recomendación de recursos en runtime/orquestador:
# --cpus=1 --memory=512m --pids-limit=256 --read-only
CMD ["gunicorn", "--bind", "0.0.0.0:3002", "--workers", "2", "--threads", "4", "--timeout", "130", "app:app"]