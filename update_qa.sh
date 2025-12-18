#!/bin/bash

##############################################################################
# Script para atualizar o ambiente QA com as mudanças do branch dev
# 
# Uso: ./update_qa.sh
# 
# Este script:
# 1. Verifica se há mudanças não commitadas
# 2. Faz checkout para o branch qa
# 3. Faz merge das mudanças do dev
# 4. Faz push para o repositório remoto
# 5. Reinicia os serviços (opcional)
##############################################################################

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para imprimir mensagens formatadas
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Função para imprimir título
print_title() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
}

# Verificar se estamos em um repositório git
if [ ! -d ".git" ]; then
    print_error "Este script deve ser executado a partir da raiz do repositório git!"
    exit 1
fi

print_title "Atualização do Ambiente QA"

# Obter branch atual
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

# Verificar se há mudanças não commitadas
if ! git diff-index --quiet HEAD --; then
    print_error "Existem mudanças não commitadas no branch $CURRENT_BRANCH"
    print_info "Por favor, faça commit ou stash das mudanças antes de continuar"
    exit 1
fi

print_success "Nenhuma mudança não commitada detectada"

# Verificar se o branch dev tem atualizações
print_info "Buscando atualizações do repositório remoto..."
git fetch origin

# Contar commits à frente e atrás
COMMITS_AHEAD=$(git rev-list --count origin/dev..dev 2>/dev/null || echo "0")
COMMITS_BEHIND=$(git rev-list --count dev..origin/dev 2>/dev/null || echo "0")

if [ "$COMMITS_AHEAD" -gt 0 ]; then
    print_warning "Branch dev tem $COMMITS_AHEAD commit(s) não sincronizado(s) com o remoto"
    read -p "Deseja fazer push dessas mudanças primeiro? (s/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Ss]$ ]]; then
        print_info "Fazendo push do branch dev..."
        git push origin dev
        print_success "Push realizado com sucesso"
    fi
fi

# Verificar se há mudanças entre dev e qa
print_info "Comparando branches dev e qa..."
COMMITS_DIFF=$(git rev-list --count qa..dev 2>/dev/null || echo "0")

if [ "$COMMITS_DIFF" -eq 0 ]; then
    print_warning "Os branches dev e qa estão sincronizados. Nenhuma atualização necessária."
    exit 0
fi

print_success "Encontrados $COMMITS_DIFF commit(s) para sincronizar"

# Fazer checkout para qa
print_info "Fazendo checkout para o branch qa..."
git checkout qa

print_success "Checkout para qa realizado"

# Mostrar commits que serão mergeados
print_info "Commits que serão sincronizados:"
git log --oneline qa..dev | head -10

# Fazer merge
print_info "Fazendo merge do branch dev em qa..."
git merge dev --no-edit

print_success "Merge realizado com sucesso"

# Fazer push
print_info "Fazendo push para o repositório remoto..."
git push origin qa

print_success "Push para qa realizado com sucesso"

# Retornar ao branch anterior
print_info "Retornando ao branch $CURRENT_BRANCH..."
git checkout "$CURRENT_BRANCH"

print_success "Retorno ao branch $CURRENT_BRANCH realizado"

# Oferecer opção de reiniciar serviços
echo ""
read -p "Deseja reiniciar os serviços do QA? (s/n) " -n 1 -r
echo

if [[ $REPLY =~ ^[Ss]$ ]]; then
    print_info "Reiniciando serviços..."
    # Descomentar a linha abaixo se tiver um script de gerenciamento de serviços
    # ./manage_service.sh restart
    print_warning "Script de reinicialização de serviços comentado. Descomente em manage_service.sh se necessário."
fi

print_title "Atualização do QA Concluída com Sucesso! ✓"

# Exibir resumo final
echo -e "${BLUE}Resumo da atualização:${NC}"
echo "  • Branch dev sincronizado com qa"
echo "  • Alterações enviadas para o repositório remoto"
echo "  • Ambiente QA pronto para teste"
echo ""
