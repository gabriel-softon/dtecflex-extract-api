#!/usr/bin/env bash
set -euo pipefail

# Ative o venv se existir
if [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
fi

HOST="${HOST:-0.0.0.0}"
PORT="${APP_PORT:-8000}"
WORKERS="${WORKERS:-2}"
APP_IMPORT="${APP_IMPORT:-app.main:app}"  # ajuste se seu módulo for diferente

# Execução
exec uvicorn "$APP_IMPORT" --host "$HOST" --port "$PORT" --workers "$WORKERS"
