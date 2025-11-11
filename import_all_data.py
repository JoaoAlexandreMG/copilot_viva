#!/usr/bin/env python3
"""
Script de importação em lote de todos os arquivos da pasta docs/
Importa automaticamente arquivos Excel (.xlsx) e CSV em qualquer formato suportado.
"""

import sys
from pathlib import Path
from db.database import get_session
from utils.excel_to_db import import_all_from_directory

# Diretório de importação
DOCS_DIR = Path(__file__).parent / "docs"

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
        success = import_all_from_directory(db_session, str(DOCS_DIR), verbose=True)
        
        print("\n" + "="*80)
        if success:
            print("✨ Importação concluída com sucesso!")
            return 0
        else:
            print("⚠️  Importação concluída com alguns problemas.")
            return 1
            
    except Exception as e:
        print(f"\n❌ Erro fatal durante a importação: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        db_session.close()

if __name__ == "__main__":
    sys.exit(main())
