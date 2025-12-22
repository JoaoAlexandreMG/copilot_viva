#!/usr/bin/env python3
"""
Gerenciador de importação em background.
Permite que a aplicação Flask continue rodando enquanto a importação acontece.
"""

import threading
import time
import os
import sys
import subprocess
from pathlib import Path
from enum import Enum
from datetime import datetime


# Configuração de estado global
class ImportStatus(Enum):
    IDLE = "idle"
    RUNNING = "importing"  # Changed from "running" to match frontend
    COMPLETED = "completed"
    ERROR = "error"


class ImportManager:
    """Gerencia importação em background com status"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.status = ImportStatus.IDLE
            self.progress = 0
            self.current_file = None
            self.total_files = 0
            self.processed_files = 0
            self.start_time = None
            self.end_time = None
            self.error_message = None
            self.import_thread = None
            self._initialized = True

    def is_running(self):
        """Verifica se importação está em andamento"""
        return self.status == ImportStatus.RUNNING

    def get_status(self):
        """Retorna status atual em formato dicionário"""
        elapsed = None
        if self.start_time:
            elapsed = time.time() - self.start_time

        # Converte timestamps para ISO format strings
        start_iso = None
        end_iso = None
        if self.start_time:
            from datetime import datetime as dt

            start_iso = dt.fromtimestamp(self.start_time).isoformat()
        if self.end_time:
            from datetime import datetime as dt

            end_iso = dt.fromtimestamp(self.end_time).isoformat()

        return {
            "status": self.status.value,
            "progress": self.progress,
            "current_file": self.current_file,
            "processed_files": self.processed_files,
            "total_files": self.total_files,
            "elapsed_seconds": elapsed,
            "error": self.error_message,
            "start_time": start_iso,
            "end_time": end_iso,
        }

    def start_import(self, refresh_views=False):
        """Inicia importação em thread separada"""
        print(f"[ImportManager] start_import called. is_running: {self.is_running()}")

        if self.is_running():
            print("[ImportManager] Already running, returning False")
            return False

        # Reset de status
        self.status = ImportStatus.RUNNING
        self.progress = 0
        self.current_file = None
        self.processed_files = 0
        self.error_message = None
        self.start_time = time.time()
        self.end_time = None

        print(f"[ImportManager] Status set to: {self.status}, starting thread...")

        # Inicia thread de importação
        self.import_thread = threading.Thread(
            target=self._run_import, args=(refresh_views,), daemon=False
        )
        self.import_thread.start()

        print(
            f"[ImportManager] Thread started. is_alive: {self.import_thread.is_alive()}"
        )
        return True

    def _run_import(self, refresh_views=False):
        """Executa importação (roda em thread separada)"""
        print(f"[ImportManager] _run_import started in thread")
        try:
            # Importa dinamicamente para evitar circular imports
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from import_all_data import process_file, DOCS_DIR
            from db.database import get_session, dispose_engine

            print(f"[ImportManager] Looking for files in {DOCS_DIR}")

            # Lista arquivos
            files_set = set()
            for pattern in ["*.xlsx", "*.csv", "*.XLSX", "*.CSV"]:
                files_set.update(str(f) for f in DOCS_DIR.glob(pattern))

            files = sorted([Path(f) for f in files_set])
            self.total_files = len(files)

            print(f"[ImportManager] Found {self.total_files} files to process")

            if not files:
                print("[ImportManager] No files found, completing")
                self.status = ImportStatus.COMPLETED
                self.end_time = time.time()
                return

            success_count = 0
            for idx, file_path in enumerate(files):
                if not self.is_running():  # Permite cancelamento
                    break

                self.current_file = file_path.name
                self.processed_files = idx + 1
                self.progress = int((idx + 1) / self.total_files * 100)

                try:
                    data = process_file(str(file_path))
                    if data and data.get("success"):
                        success_count += 1
                except Exception as e:
                    print(f"Erro ao processar {file_path.name}: {e}")

            # Cleanup
            dispose_engine()

            # Refresh materialized view em background (não espera terminar)
            try:
                print("[ImportManager] Iniciando refresh da MV em background...")
                subprocess.Popen(
                    [
                        "PGPASSWORD=2584",
                        "psql",
                        "-h",
                        "72.60.146.124",
                        "-U",
                        "postgres",
                        "-d",
                        "portal_associacao_db",
                        "-c",
                        "REFRESH MATERIALIZED VIEW mv_asset_current_status",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    shell=False,
                )
            except Exception as e:
                print(f"[ImportManager] Erro ao iniciar refresh: {e}")

            self.status = ImportStatus.COMPLETED
            self.progress = 100
            self.end_time = time.time()

        except Exception as e:
            self.status = ImportStatus.ERROR
            self.error_message = str(e)
            self.end_time = time.time()

    def wait_completion(self, timeout=None):
        """Aguarda conclusão da importação"""
        if self.import_thread:
            self.import_thread.join(timeout)


# Instância global
import_manager = ImportManager()
