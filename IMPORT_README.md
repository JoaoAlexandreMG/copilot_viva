# 📊 Gerenciamento de Importação de Dados

Scripts para importar, gerenciar e fazer backup de dados da aplicação Portal Associação.

## 🆕 Novidades

- ✅ **Suporte Universal** - Importa automaticamente tanto arquivos Excel (.xlsx) quanto CSV
- ✅ **Auto-detecção** - Detecta o tipo de arquivo automaticamente
- ✅ **Detecção de Nomes** - Reconhece arquivos com nomes padronizados
- ✅ **Flexível** - Escolha usar Excel ou CSV conforme necessário

## 📁 Arquivos Disponíveis

### Scripts Principais

- **`import_data.sh`** - Script shell que executa a importação automaticamente
- **`manage_data.sh`** - Script completo com múltiplos comandos (recomendado)
- **`import_all_data.py`** - Script Python que realiza as importações

### Arquivos de Importação (pasta `docs/`)

Os seguintes arquivos são importados automaticamente. Você pode usar **Excel OU CSV**:

| Dados | Arquivo Excel | Arquivo CSV | Descrição |
|-------|---------------|------------|-----------|
| Usuários | `users.xlsx` | ❌ N/A | Dados de usuários |
| Outlets | `outlets.xlsx` | ❌ N/A | Dados de pontos de venda |
| Assets | `assets.xlsx` | ❌ N/A | Dados de equipamentos |
| Smart Devices | `smartdevices.xlsx` | ❌ N/A | Dados de dispositivos |
| Health Events | `health_events.xlsx` | ❌ N/A | Eventos de saúde |
| Door Events | ❌ N/A | `door_events.csv` (UTF-16) | Eventos de porta |
| Alerts | ❌ N/A | `alerts.csv` (UTF-16) | Alertas do sistema |
| Clients | ❌ N/A | `clients.csv` (UTF-16) | Dados de clientes |
| SubClients | ❌ N/A | `subclients.csv` (UTF-16) | Dados de subclientes |

## 🚀 Como Usar

### Opção 1: Script Shell (Mais Simples)

```bash
cd /home/vivaservicesai/htdocs/app

# Importar todos os dados de uma vez
./import_data.sh
```

### Opção 2: Script Python Direto

```bash
cd /home/vivaservicesai/htdocs/app
source venv/bin/activate
python import_all_data.py
```

### Opção 3: Script Completo com Mais Opções

```bash
cd /home/vivaservicesai/htdocs/app

# Importar todos os dados
./manage_data.sh import

# Listar arquivos disponíveis
./manage_data.sh list

# Criar backup do banco de dados
./manage_data.sh backup

# Ver status do sistema
./manage_data.sh status
```

## 📋 Como Funciona

O sistema **auto-detecta** arquivos com nomes padronizados:

```
docs/
├── users.xlsx              ✅ Detectado automaticamente
├── outlets.xlsx            ✅ Detectado automaticamente
├── assets.xlsx             ✅ Detectado automaticamente
├── smartdevices.xlsx       ✅ Detectado automaticamente
├── health_events.xlsx      ✅ Detectado automaticamente
├── door_events.csv         ✅ Detectado automaticamente (UTF-16)
├── alerts.csv              ✅ Detectado automaticamente (UTF-16)
├── clients.csv             ✅ Detectado automaticamente (UTF-16)
└── subclients.csv          ✅ Detectado automaticamente (UTF-16)
```

## 📊 Saída Esperada

```
======================================================================
🚀 IMPORTAÇÃO EM LOTE DE ARQUIVOS - PORTAL ASSOCIAÇÃO
======================================================================

� Scanning directory: /home/vivaservicesai/htdocs/app/docs
======================================================================
� Importing: users.xlsx
✅ users.xlsx imported successfully
📥 Importing: outlets.xlsx
✅ outlets.xlsx imported successfully
[...]
======================================================================
✨ Import completed: 9 files imported, 0 skipped
======================================================================
```

## 🔄 Fluxo Recomendado

1. **Verificar arquivos**
   ```bash
   ls -la docs/
   ```

2. **Fazer backup (opcional)**
   ```bash
   ./manage_data.sh backup
   ```

3. **Importar dados**
   ```bash
   ./import_data.sh
   ```

4. **Verificar no banco**
   ```bash
   ./manage_data.sh status
   ```

## � Funções Python Disponíveis

```python
from db.database import get_session
from utils.excel_to_db import import_all_from_directory

# Importar todos os arquivos de um diretório
session = get_session()
import_all_from_directory(session, "docs/", verbose=True)
```

**Funções Genéricas (suportam Excel e CSV):**
- `insert_or_update_users(session, file_path)`
- `insert_or_update_outlets(session, file_path)`
- `insert_or_update_assets(session, file_path)`
- `insert_or_update_smartdevices(session, file_path)`
- `insert_or_update_movements(session, file_path)`
- `insert_or_update_health_events(session, file_path)`
- `insert_or_update_door_events(session, file_path)`
- `insert_or_update_alerts(session, file_path)`
- `insert_or_update_clients(session, file_path)`
- `insert_or_update_subclients(session, file_path)`

## ⚙️ Estrutura de Diretórios

```
app/
├── docs/                          # Arquivos para importação
│   ├── users.xlsx
│   ├── outlets.xlsx
│   ├── assets.xlsx
│   ├── smartdevices.xlsx
│   ├── health_events.xlsx
│   ├── door_events.csv
│   ├── alerts.csv
│   ├── clients.csv
│   └── subclients.csv
├── backups/                       # Backups automáticos
├── import_data.sh                 # Script principal
├── import_all_data.py             # Script Python
├── manage_data.sh                 # Script completo
└── utils/excel_to_db.py           # Funções de importação
```

## 🔐 Credenciais do Banco de Dados

Configure as variáveis de ambiente para backups:

```bash
export DB_HOST="72.60.146.124"
export DB_USER="postgres"
export DB_NAME="portal_associacao_db"
export DB_PASSWORD="2584"
```

## 📝 Notas Importantes

1. ✅ **Suporte duplo** - Use Excel ou CSV conforme necessário
2. ✅ **Auto-detecção** - Nenhuma configuração extra necessária
3. ✅ **UTF-16 para CSV** - Arquivos CSV devem estar em UTF-16
4. ✅ **Não destrutivo** - Apenas atualiza ou insere dados
5. ✅ **Tolerante** - Erros em arquivos opcionais não afetam o resto

## � Troubleshooting

| Erro | Solução |
|------|---------|
| "Diretório não encontrado" | Certifique-se de estar em `/home/vivaservicesai/htdocs/app` |
| "Ambiente virtual não encontrado" | Execute `source venv/bin/activate` primeiro |
| "Arquivo não suportado" | Use .xlsx para Excel ou .csv para CSV |
| "Permissão negada" | Execute `chmod +x *.sh` para tornar executáveis |

---

**Última atualização:** Novembro 2025
**Versão:** 2.0 (Com suporte universal Excel/CSV)