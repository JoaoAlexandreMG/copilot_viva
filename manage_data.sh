#!/bin/bash

# Script completo de gerenciamento de importação de dados
# Suporta: importar, limpar, backup, status

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/import_all_data.py"
VENV_ACTIVATE="$SCRIPT_DIR/venv/bin/activate"
DOCS_DIR="$SCRIPT_DIR/docs"
BACKUP_DIR="$SCRIPT_DIR/backups"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Funções auxiliares
print_header() {
    echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Verificar dependências
check_dependencies() {
    if [ ! -f "$VENV_ACTIVATE" ]; then
        print_error "Ambiente virtual não encontrado em $VENV_ACTIVATE"
        exit 1
    fi
    
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        print_error "Script Python não encontrado em $PYTHON_SCRIPT"
        exit 1
    fi
    
    if [ ! -d "$DOCS_DIR" ]; then
        print_error "Diretório docs não encontrado em $DOCS_DIR"
        exit 1
    fi
}

# Mostrar arquivos disponíveis
list_files() {
    print_header "📁 ARQUIVOS DISPONÍVEIS PARA IMPORTAÇÃO"
    
    if [ -z "$(ls -A $DOCS_DIR)" ]; then
        print_warning "Nenhum arquivo encontrado em $DOCS_DIR"
        return
    fi
    
    echo "Arquivos na pasta $DOCS_DIR:"
    ls -lh "$DOCS_DIR" | tail -n +2 | awk '{printf "  • %s (%s)\n", $9, $5}'
    
    echo ""
}

# Importar dados
import_data() {
    print_header "📥 IMPORTAÇÃO DE DADOS"
    
    check_dependencies
    
    list_files
    
    echo -e "${YELLOW}Iniciando importação...${NC}\n"
    
    # Ativar ambiente virtual e executar
    source "$VENV_ACTIVATE"
    cd "$SCRIPT_DIR"
    
    if python "$PYTHON_SCRIPT"; then
        print_success "Importação concluída com sucesso!"
        return 0
    else
        print_error "Importação falhou!"
        return 1
    fi
}

# Criar backup
backup_database() {
    print_header "💾 CRIANDO BACKUP DO BANCO DE DADOS"
    
    # Criar diretório de backup se não existir
    mkdir -p "$BACKUP_DIR"
    
    BACKUP_FILE="$BACKUP_DIR/backup_$(date +%Y%m%d_%H%M%S).sql"
    
    print_info "Usando arquivo: $BACKUP_FILE"
    
    # Tentar fazer backup (se usar PostgreSQL)
    if command -v pg_dump &> /dev/null; then
        print_info "Conectando ao PostgreSQL..."
        
        # Usar variáveis de ambiente ou valores padrão
        DB_HOST="${DB_HOST:-72.60.146.124}"
        DB_USER="${DB_USER:-postgres}"
        DB_NAME="${DB_NAME:-portal_associacao_db}"
        
        PGPASSWORD="${DB_PASSWORD:-2584}" pg_dump -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" > "$BACKUP_FILE" 2>/dev/null
        
        if [ $? -eq 0 ]; then
            print_success "Backup criado: $(basename $BACKUP_FILE)"
            du -h "$BACKUP_FILE" | awk '{print "Tamanho: " $1}'
        else
            print_error "Falha ao criar backup"
            return 1
        fi
    else
        print_warning "pg_dump não encontrado, pulando backup do banco de dados"
    fi
    
    echo ""
}

# Mostrar status
show_status() {
    print_header "📊 STATUS DO SISTEMA"
    
    print_info "Caminho da aplicação: $SCRIPT_DIR"
    print_info "Diretório de docs: $DOCS_DIR"
    print_info "Diretório de backups: $BACKUP_DIR"
    print_info "Script Python: $PYTHON_SCRIPT"
    
    echo ""
    
    # Contar arquivos
    if [ -d "$DOCS_DIR" ]; then
        FILE_COUNT=$(ls -1 "$DOCS_DIR" 2>/dev/null | wc -l)
        print_info "Arquivos de importação: $FILE_COUNT"
    fi
    
    if [ -d "$BACKUP_DIR" ]; then
        BACKUP_COUNT=$(ls -1 "$BACKUP_DIR" 2>/dev/null | wc -l)
        print_info "Backups existentes: $BACKUP_COUNT"
        
        if [ $BACKUP_COUNT -gt 0 ]; then
            echo "  Últimos backups:"
            ls -1t "$BACKUP_DIR" | head -3 | awk '{printf "    • %s\n", $1}'
        fi
    fi
    
    echo ""
}

# Mostrar ajuda
show_help() {
    cat << EOF
${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}
${BLUE}║       SCRIPT DE GERENCIAMENTO DE IMPORTAÇÃO DE DADOS           ║${NC}
${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}

${GREEN}Uso:${NC}
  ./manage_data.sh [comando]

${GREEN}Comandos:${NC}
  import      Importar todos os dados da pasta docs/
  list        Listar arquivos disponíveis para importação
  backup      Criar backup do banco de dados
  status      Mostrar status do sistema
  help        Mostrar esta mensagem de ajuda

${GREEN}Exemplos:${NC}
  ./manage_data.sh import      # Importar dados
  ./manage_data.sh list        # Listar arquivos
  ./manage_data.sh backup      # Fazer backup
  ./manage_data.sh status      # Ver status

${YELLOW}Notas:${NC}
  • O script requer ambiente virtual Python ativado
  • Os arquivos devem estar na pasta: $DOCS_DIR
  • Os backups são salvos em: $BACKUP_DIR

EOF
}

# Main
case "${1:-help}" in
    import)
        import_data
        ;;
    list)
        list_files
        ;;
    backup)
        backup_database
        ;;
    status)
        show_status
        ;;
    help)
        show_help
        ;;
    *)
        print_error "Comando desconhecido: $1"
        echo ""
        show_help
        exit 1
        ;;
esac

exit 0
