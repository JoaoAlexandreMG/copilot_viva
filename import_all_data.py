#!/usr/bin/env python3
"""
Script de importação em lote de todos os arquivos da pasta docs/
Importa automaticamente arquivos Excel (.xlsx) e CSV em qualquer formato suportado.
Otimizado para processamento paralelo (Multiprocessing).
"""

import sys
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

# Configura encoding do terminal para Windows para suportar emojis
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# Adiciona o diretório raiz ao path para imports funcionarem
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.database import get_session
from utils.new_excel_to_db import importar_dados_generico

# Diretório de importação
DOCS_DIR = Path("docs")

def process_file(file_path_str):
    """
    Função isolada para processar um único arquivo.
    Cria sua própria sessão de banco de dados.
    """
    file_path = Path(file_path_str)
    filename_lower = file_path.name.lower()
    
    # Padrões de arquivo para cada modelo
    file_patterns = {
        'HealthEvent': ['health'],
        'Movement': ['movements'],
        'User': ['users'],
        'Client': ['client'],
        'Asset': ['assets'],
        'Outlet': ['outlet'],
        'Alert': ['alerts'],
        'AlertsDefinition': ['definition'],
        'DoorEvent': ['door'],
        'GhostAsset': ['ghost_assets', 'ghost-asset', 'ghostassets', 'ghost assets'],
        'SmartDevice': ['smart_devices', 'smartdevice', 'smart-device', 'smartdevices', 'smart devices']
    }
    
    detected_model = None
    for model_name, patterns in file_patterns.items():
        if any(pattern in filename_lower for pattern in patterns):
            detected_model = model_name
            break
    
    if not detected_model:
        return {'success': False, 'file': file_path.name, 'error': 'Modelo não detectado'}

    # Cria sessão isolada para este processo
    db_session = get_session()
    try:
        print(f"🚀 [PID {os.getpid()}] Iniciando {file_path.name} ({detected_model})...")
        result = importar_dados_generico(db_session, detected_model, str(file_path))
        
        if result:
            # Deleta o arquivo após sucesso
            try:
                file_path.unlink()
                msg = "Arquivo deletado"
            except Exception as e:
                msg = f"Erro ao deletar: {e}"
            
            return {'success': True, 'file': file_path.name, 'model': detected_model, 'msg': msg}
        else:
            return {'success': False, 'file': file_path.name, 'error': 'Falha na importação genérica'}
            
    except Exception as e:
        return {'success': False, 'file': file_path.name, 'error': str(e)}
    finally:
        db_session.close()

def main():
    """Importa todos os arquivos disponíveis na pasta docs/ usando multiprocessamento"""
    
    print("\n" + "="*80)
    print("🚀 IMPORTAÇÃO EM LOTE OTIMIZADA (MULTIPROCESSING) - PORTAL ASSOCIAÇÃO")
    print("="*80 + "\n")
    
    if not DOCS_DIR.exists():
        print(f"❌ Erro: Diretório não encontrado: {DOCS_DIR}")
        return 1
    
    # Lista arquivos encontrados (usando set para evitar duplicatas em sistemas case-insensitive como Windows)
    files_set = set()
    for pattern in ['*.xlsx', '*.csv', '*.XLSX', '*.CSV']:
        files_set.update(str(f) for f in DOCS_DIR.glob(pattern))
    
    files = sorted([Path(f) for f in files_set])
    
    if not files:
        print("⚠️ Nenhum arquivo .xlsx ou .csv encontrado na pasta docs/")
        return 1

    print(f"📂 Encontrados {len(files)} arquivo(s) para importação.")
    
    start_time = time.time()
    success_count = 0
    processed_models = set()
    
    # Define número de workers (CPU count - 1 ou 4 no mínimo)
    # No Windows, multiprocessing precisa de cuidado com imports, mas como estamos usando if __name__ == "__main__", deve funcionar.
    max_workers = max(4, (os.cpu_count() or 1) - 1)
    print(f"⚙️  Utilizando {max_workers} processos paralelos.\n")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submete todas as tarefas
        future_to_file = {executor.submit(process_file, str(f)): f for f in files}
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                data = future.result()
                if data['success']:
                    print(f"✅ {data['file']}: Sucesso. ({data.get('msg', '')})")
                    success_count += 1
                    if 'model' in data:
                        processed_models.add(data['model'])
                else:
                    print(f"❌ {data['file']}: Falha. {data.get('error')}")
            except Exception as exc:
                print(f"❌ {file_path.name} gerou uma exceção: {exc}")

    elapsed_time = time.time() - start_time
    print(f"\n⏱️  Tempo total: {elapsed_time:.2f} segundos")
    print(f"📊 Resumo: {success_count}/{len(files)} arquivos importados com sucesso")

    # Atualiza MVs no final se necessário (apenas uma vez)
    tables_requiring_mv_refresh = {"Movement", "HealthEvent", "DoorEvent", "Asset", "Alert", "SmartDevice"}
    if processed_models.intersection(tables_requiring_mv_refresh):
        print(f"\n🔄 Atualizando Materialized Views finais...")
        db_session = get_session()
        try:
            from sqlalchemy import text
            # Tenta refresh concorrente
            try:
                db_session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_client_assets_report;"))
                db_session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dashboard_stats_main;"))
                db_session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_asset_current_status;"))
            except:
                db_session.rollback()
                db_session.execute(text("REFRESH MATERIALIZED VIEW mv_client_assets_report;"))
                db_session.execute(text("REFRESH MATERIALIZED VIEW mv_dashboard_stats_main;"))
                db_session.execute(text("REFRESH MATERIALIZED VIEW mv_asset_current_status;"))
            
            db_session.commit()
            print("✅ Materialized Views atualizadas.")
        except Exception as e:
            print(f"⚠️ Erro ao atualizar MVs: {e}")
        finally:
            db_session.close()

if __name__ == "__main__":
    # Necessário para Windows
    sys.exit(main())
