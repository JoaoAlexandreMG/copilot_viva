# Quality Dashboard - Materialized View Setup Guide

## 📊 Visão Geral

O Quality Dashboard é uma página unificada que monitora a performance da empresa em 3 dimensões:

1. **Desempenho dos Técnicos** - Atividade dos técnicos nos últimos 30 dias
2. **Tempo do Compressor Ligado** - Ranking de ativos por tempo de compressor ligado
3. **Consumo de Energia** - Ranking de ativos por consumo médio de energia

## 🚀 Como Acessar

A página está disponível em: `/portal_associacao/quality-dashboard`

Você pode acessar através de:
- Link nos cards do Dashboard (Compressor e Consumo)
- URL direta: http://seu-servidor/portal_associacao/quality-dashboard

## 📋 Componentes Inclusos

### 1. Backend Route
**Arquivo**: `routes/portal/dashboard.py`
- **Função**: `render_quality_dashboard()`
- **Responsabilidades**:
  - Buscar atividade dos técnicos
  - Buscar ranking de assets por compressor on time
  - Buscar ranking de assets por consumo
  - Implementar paginação (10 items por página)

### 2. Frontend Template
**Arquivo**: `templates/portal/quality-dashboard.html`
- Design profissional e moderno
- 3 seções com cards visuais distintos
- Tabelas responsivas com badging de ranking
- Paginação funcional
- Cores codificadas por prioridade (vermelho/amarelo/verde)

### 3. Dashboard Links
**Arquivo**: `templates/portal/dashboard.html`
- Cards "Compressor" e "Consumo" agora são clicáveis
- Card "Total de Ativos" aponta para tracking sem filtros

## 💾 Materialized View (Opcional)

Se você quiser usar a Materialized View para melhor performance:

### Passo 1: Criar a MV
Execute o SQL do arquivo `quality_dashboard_mv.sql`:

```bash
psql -U seu_usuario -d seu_database -f quality_dashboard_mv.sql
```

### Passo 2: Atualizar a MV regularmente

A MV deve ser refreshada periodicamente. Adicione um cronjob:

```bash
# A cada 1 hora
0 * * * * psql -U seu_usuario -d seu_database -c "REFRESH MATERIALIZED VIEW mv_quality_dashboard_metrics;"

# A cada 4 horas (menos intensivo)
0 */4 * * * psql -U seu_usuario -d seu_database -c "REFRESH MATERIALIZED VIEW mv_quality_dashboard_metrics;"
```

### Passo 3: Atualizar backend (opcional)

Se quiser usar a MV, atualize a rota para fazer queries direto na view:

```python
# Exemplo: buscar técnicos pela MV
technicians_sql = text("""
    SELECT entity_id, entity_name, entity_contact, total_activity
    FROM mv_quality_dashboard_metrics
    WHERE metric_type = 'technician' AND client = :client
    ORDER BY total_activity DESC
    LIMIT :limit
""")
```

## 📊 Dados Exibidos

### Ranking 1: Técnicos (30 dias)
- **#**: Posição no ranking
- **Técnico**: Nome e Email
- **Leituras**: Eventos de saúde reportados
- **Ativos Fantasma**: Assets duplicados reportados
- **Total**: Soma de ambas as atividades

Cores de destaque:
- 🥇 **1º lugar**: Ouro
- 🥈 **2º lugar**: Prata
- 🥉 **3º lugar**: Bronze

### Ranking 2: Compressor On Time (30 dias)
- **#**: Posição
- **Ativo**: Serial OEM
- **Outlet**: Código e nome do outlet
- **Tempo Ligado**: Percentual (0-100%)

Cores por alerta:
- 🔴 Vermelho: > 80% (muito ligado)
- 🟠 Laranja: 50-80% (médio)
- 🟢 Verde: < 50% (normal)

### Ranking 3: Consumo (30 dias)
- **#**: Posição
- **Ativo**: Serial OEM
- **Outlet**: Código e nome do outlet
- **Consumo Médio**: em kW

Cores por alerta (similar ao compressor)

## 🔧 Customizações Possíveis

### Alterar itens por página
Em `routes/portal/dashboard.py`, função `render_quality_dashboard()`:
```python
per_page = 10  # Mude para o número desejado
```

### Alterar período de dados
Nas queries SQL, mude `INTERVAL '30 days'` para o período desejado:
- `'7 days'` - Últimas 7 dias
- `'1 month'` - Último mês
- `'3 months'` - Últimos 3 meses

### Alterar cores
No template `quality-dashboard.html`, seção `<style>`:
- `.ranking-icon.technicians`: Cor dos técnicos
- `.ranking-icon.compressor`: Cor do compressor
- `.ranking-icon.consumption`: Cor do consumo
- `.rank-badge.top1/2/3`: Cores dos rankings

## 🐛 Troubleshooting

**Problema**: Página em branco ou erro 500
- Verifique se `asset_aggregated_metrics` existe no banco
- Verifique os logs: `journalctl -u app_consultor_vendas.service`

**Problema**: Dados não aparecem
- Verifique se `health_events` e `ghost_assets` têm registros
- Verifique se os usuários têm `role = 'Technician'`

**Problema**: Performance lenta
- Implemente a Materialized View (veja acima)
- Aumente o intervalo de refresh do cronjob

## 📈 Próximas Melhorias

- [ ] Exportar dados em PDF/Excel
- [ ] Gráficos de tendência temporal
- [ ] Filtro por período (30d, 90d, etc)
- [ ] Comparação com período anterior
- [ ] Alertas automáticos para anomalias
- [ ] Dashboard real-time com WebSocket
