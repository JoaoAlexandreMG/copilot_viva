#!/bin/bash

# Script para executar o scraping paralelo diariamente
# Agendado para rodar às 7 AM BRT (UTC-3) todos os dias

# Diretório da aplicação
APP_DIR="/home/vivaservicesai/htdocs/co-pilot/qa"

# Diretório do venv
VENV_DIR="$APP_DIR/venv"

# Log file
LOG_FILE="$APP_DIR/logs/scraping_daily.log"

# Criar diretório de logs se não existir
mkdir -p "$APP_DIR/logs"

# Função para logar com timestamp
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Iniciar log
log_message "============================================"
log_message "Iniciando scraping paralelo diário"
log_message "============================================"

# Ativar venv
source "$VENV_DIR/bin/activate"

# Verificar se o venv foi ativado corretamente
if [ $? -ne 0 ]; then
    log_message "ERRO: Falha ao ativar o venv"
    exit 1
fi

log_message "Venv ativado com sucesso"

# Navegar para o diretório da aplicação
cd "$APP_DIR" || {
    log_message "ERRO: Falha ao navegar para $APP_DIR"
    exit 1
}

log_message "Navegado para $APP_DIR"

# Executar o scraping paralelo
log_message "Executando: python3 utils/scraping_parallel.py daily 2"
python3 utils/scraping_parallel.py daily 2 >> "$LOG_FILE" 2>&1

# Verificar o status de saída
if [ $? -eq 0 ]; then
    log_message "✓ Scraping concluído com sucesso"
else
    log_message "✗ Scraping falhou com erro"
fi

log_message "============================================"
log_message "Scraping diário finalizado"
log_message "============================================"

exit 0
