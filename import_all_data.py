#!/usr/bin/env python3
"""
Script de importação em lote de todos os arquivos da pasta docs/
Importa automaticamente arquivos Excel (.xlsx) e CSV em qualquer formato suportado.
"""

import sys
from pathlib import Path
from db.database import get_session
from utils.new_excel_to_db import importar_dados_generico

# Diretório de importação
DOCS_DIR = Path("docs")

def import_all_files(db_session, docs_dir, verbose=True):
    """
    Importa todos os arquivos suportados do diretório docs/
    """
    success_count = 0
    total_count = 0
    processed_models = set()
    
    # Extensões suportadas
    supported_extensions = ['.xlsx', '.csv']
    
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
        'SmartDevice': ['smart_devices', 'smartdevice', 'smart-device', 'smartdevices', 'smart devices']
    }
    
    for file_path in Path(docs_dir).iterdir():
        if not file_path.is_file() or file_path.suffix.lower() not in supported_extensions:
            continue
            
        total_count += 1
        filename_lower = file_path.name.lower()
        
        if verbose:
            print(f"\n📁 Processando arquivo: {file_path.name}")
        
        # Detecta o tipo de modelo baseado no nome do arquivo
        detected_model = None
        for model_name, patterns in file_patterns.items():
            if any(pattern in filename_lower for pattern in patterns):
                detected_model = model_name
                break
        
        try:
            if verbose:
                print(f"🔍 Tipo detectado: {detected_model}")
            
            # Chama a função de importação genérica
            result = importar_dados_generico(db_session, detected_model, str(file_path))
            if result:
                success_count += 1
                processed_models.add(detected_model)
                
                # Deleta o arquivo após importação bem-sucedida
                try:
                    file_path.unlink()
                    if verbose:
                        print(f"🗑️  Arquivo {file_path.name} deletado após importação bem-sucedida")
                except Exception as delete_error:
                    if verbose:
                        print(f"⚠️ Aviso: Não foi possível deletar {file_path.name}: {delete_error}")
        except Exception as e:
            if verbose:
                print(f"❌ Erro ao importar {file_path.name}: {str(e)}")
            # Rollback para resetar a session após erro
            try:
                db_session.rollback()
            except:
                pass
            continue
    
    # Atualiza materialized views apenas no final se algum modelo relevante foi processado
    tables_requiring_mv_refresh = {"Movement", "HealthEvent", "DoorEvent", "Asset", "Alert", "SmartDevice"}
    if processed_models.intersection(tables_requiring_mv_refresh):
        if verbose:
            print(f"\n🔄 Atualizando Materialized Views após importação de todas as tabelas...")
        try:
            from sqlalchemy import text
            db_session.execute(text("REFRESH MATERIALIZED VIEW mv_client_assets_report;"))
            db_session.execute(text("REFRESH MATERIALIZED VIEW mv_dashboard_hourly_metrics;"))
            db_session.execute(text("REFRESH MATERIALIZED VIEW mv_dashboard_stats_main;"))
            db_session.commit()
            if verbose:
                print("✅ Materialized Views atualizadas com sucesso.")
        except Exception as mv_error:
            if verbose:
                print(f"⚠️ Erro ao atualizar Materialized Views: {mv_error}")
            # Não faz rollback pois os dados já foram commitados com sucesso
    
    if verbose:
        print(f"\n📊 Resumo final: {success_count}/{total_count} arquivos importados com sucesso")
    
    return success_count, total_count

def main():
    """Importa todos os arquivos disponíveis na pasta docs/"""
    
    print("\n" + "="*80)
    print("🚀 IMPORTAÇÃO EM LOTE DE ARQUIVOS - PORTAL ASSOCIAÇÃO")
    print("="*80 + "\n")
    
    if not DOCS_DIR.exists():
        print(f"❌ Erro: Diretório não encontrado: {DOCS_DIR}")
        return 1
    
    try:
        db_session = get_session()
        
        # Lista arquivos encontrados
        files = list(DOCS_DIR.glob('*.xlsx')) + list(DOCS_DIR.glob('*.csv')) + list(DOCS_DIR.glob('*.XLSX')) + list(DOCS_DIR.glob('*.CSV'))
        print(f"📂 Encontrados {len(files)} arquivo(s) para importação:")
        for file_path in files:
            print(f"   • {file_path.name}")
        
        if not files:
            print("⚠️ Nenhum arquivo .xlsx ou .csv encontrado na pasta docs/")
            return 1
        
        print("\n🔄 Iniciando importação...\n")
        
        import_all_files(db_session, str(DOCS_DIR), verbose=True)
            
    except Exception as e:
        print(f"\n❌ Erro fatal durante a importação: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        db_session.close()

if __name__ == "__main__":
    sys.exit(main())