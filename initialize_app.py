#!/usr/bin/env python3
"""
Script para inicializar a aplicação - executa as mesmas funções
que estavam no if __name__ == "__main__"
"""

from db.database import get_session, init_db
from utils.vision_accounts import create_accounts_for_all_clients

def main():
    print("[INFO] Initializing database...")
    init_db()
    
    print("[INFO] Creating accounts for all clients...")
    db_session = get_session()
    created, total = create_accounts_for_all_clients(db_session)
    print(f"[INFO] Created {created} accounts out of {total} total.")
    
    print("[INFO] Initialization complete!")

if __name__ == "__main__":
    main()