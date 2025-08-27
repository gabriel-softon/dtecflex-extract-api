#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/softon/dtecflex-extract-api"
USER_SVC="softon"
SERVICE_NAME="dtecflex-extract-api"
POETRY_BIN="/home/softon/.local/bin/poetry"

# Ajuste este import para o seu app, se necessário:
# Exemplos comuns: "dtecflex_extract_api.main:app", "src.main:app", "app.main:app"
APP_IMPORT="dtecflex_extract_api.main:app"

# 1) Dependências do projeto
cd "$APP_DIR"
if [ ! -x "$POETRY_BIN" ]; then
  echo "Poetry não encontrado em $POETRY_BIN. Instale com: curl -sSL https://install.python-poetry.org | python3 -"
  exit 1
fi

"$POETRY_BIN" install --no-interaction --no-ansi

# 2) Criar unidade systemd
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=DTEC Flex Extract API (FastAPI via Uvicorn/Poetry)
After=network.target

[Service]
User=${USER_SVC}
Group=${USER_SVC}
WorkingDirectory=${APP_DIR}
# Incluímos ~/.local/bin no PATH para achar o poetry
Environment=PATH=/home/${USER_SVC}/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin

# Se o seu app carrega .env com python-dotenv, deixe o app cuidar disso.
# Execução do Uvicorn (ajuste APP_IMPORT se precisar)
ExecStart=${POETRY_BIN} run uvicorn ${APP_IMPORT} --host 127.0.0.1 --port 8000 --workers 2

Restart=on-failure
RestartSec=5
KillMode=mixed
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

# 3) Habilitar e subir
sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}"
sudo systemctl restart "${SERVICE_NAME}"

# 4) Status
sudo systemctl status --no-pager "${SERVICE_NAME}" || true
echo
echo "Logs ao vivo: sudo journalctl -u ${SERVICE_NAME} -f"
echo "Teste local:  curl -s http://127.0.0.1:8000/docs | head"
