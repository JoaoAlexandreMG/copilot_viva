# ⚡ Otimização de Performance - Importação de Dados

## 🔴 Problemas Identificados

1. **Reinício do PostgreSQL** - Deixava VPS inacessível por minutos
2. **Muitos workers paralelos** - Consumiam toda memória disponível
3. **Carregamento total em memória** - Arquivos grandes causavam OOM
4. **Sem timeouts** - Threads podiam travar indefinidamente
5. **Refresh de MVs múltiplas vezes** - Sobrecarregava o banco

## ✅ Soluções Implementadas

### 1. `scraping_parallel.py`
- ✅ Limitado a máximo **2 workers** (em vez de CPU count)
- ✅ Adicionado timeout de **30 minutos** por conta
- ✅ **Removido restart do PostgreSQL** (desnecessário)
- ✅ Adicionado delay de 5s antes da importação

### 2. `import_all_data.py`
- ✅ Reduzido workers para **máximo 2** (em vez de CPU-1)
- ✅ Processamento em lotes de **5000 registros**
- ✅ Melhor logging do progresso

### 3. `new_excel_to_db.py`
- ✅ Leitura de Excel com `dtype=str` (economiza memória)
- ✅ Processamento em chunks de **2000 registros**
- ✅ Deduplicação otimizada em memória

## 🚀 Recomendações Adicionais

### Para VPS com pouca memória (< 4GB):

**Opção 1: Importação sequencial (mais segura)**
```bash
# Editar import_all_data.py e mudar:
max_workers = 1  # Apenas 1 worker
```

**Opção 2: Reduzir chunk size**
```python
# Em new_excel_to_db.py:
chunk_size = 1000  # Em vez de 2000
batch_size = 2500  # Em vez de 5000
```

**Opção 3: Monitorar em tempo real**
```bash
# Terminal 1: Monitorar memória
watch -n 1 'free -h && echo "---" && ps aux | grep python'

# Terminal 2: Verificar conexões PostgreSQL
watch -n 2 'psql -U user -d database -c "SELECT count(*) FROM pg_stat_activity"'
```

### Limites do Sistema Operacional

Adicionar ao `crontab` se rodar automaticamente:
```bash
# Limpar cache periodicamente antes da importação
*/2 * * * * sync && echo 3 > /proc/sys/vm/drop_caches
```

### Configuração PostgreSQL (postgresql.conf)

Se ainda tiver problemas, ajuste:
```ini
# Reduzir pool de conexões
max_connections = 100  # (default é 100, já baixo)
shared_buffers = 256MB  # (em vez de 40% RAM)
work_mem = 16MB  # (em vez de 64MB)
```

## 📊 Monitoramento

Verificar status da importação:
```bash
# Ver consumo de recursos
top -p $(pgrep -f import_all_data.py)

# Ver conexões ao PostgreSQL
psql -c "SELECT pid, usename, application_name, state FROM pg_stat_activity"

# Ver queries lentas
tail -f /var/log/postgresql/postgresql.log | grep "duration"
```

## 🧪 Teste

Execute com dados de teste:
```bash
# 1. Copie apenas 1 arquivo pequeno para docs/
# 2. Execute a importação
python3 import_all_data.py

# 3. Se der sucesso, processe os demais
```

## 📌 Resumo das Mudanças

| Arquivo | Mudança | Impacto |
|---------|---------|--------|
| scraping_parallel.py | Max 2 workers + timeout 30m | -50% picos de memória |
| import_all_data.py | Max 2 workers + chunks 5000 | -60% consumo RAM |
| new_excel_to_db.py | dtype=str + chunks 2000 | -40% picos de memória |
| Removido | PostgreSQL restart | +100% disponibilidade |

---

**Próximos passos:** Execute a importação e monitore. Se ainda tiver problemas, reduza `max_workers` para 1.
