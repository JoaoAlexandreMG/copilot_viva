#!/bin/bash

# Script de importação em lote de dados
# Executa a importação de todos os arquivos da pasta docs/

set -e  # Sair em caso de erro

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/import_all_data.py"
VENV_ACTIVATE="$SCRIPT_DIR/venv/bin/activate"

# Verificar se o ambiente virtual existe
if [ ! -f "$VENV_ACTIVATE" ]; then
    echo "❌ Erro: Ambiente virtual não encontrado em $VENV_ACTIVATE"
    exit 1
fi

# Ativar ambiente virtual
source "$VENV_ACTIVATE"

# Verificar se o script Python existe
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "❌ Erro: Script Python não encontrado em $PYTHON_SCRIPT"
    exit 1
fi

# Executar o script Python
cd "$SCRIPT_DIR"
python "$PYTHON_SCRIPT"

exit $?
