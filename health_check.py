#!/usr/bin/env python3
"""
Health check e monitoramento de uptime do sistema.
Verifica saúde da aplicação, banco de dados e outros componentes críticos.
"""

import sys
import os
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.database import engine, get_session
from sqlalchemy import text


class HealthCheck:
    """Verifica saúde de todos os componentes críticos"""

    @staticmethod
    def check_database():
        """Testa conexão com banco de dados"""
        try:
            session = get_session()
            session.execute(text("SELECT 1"))
            session.close()
            return {"status": "healthy", "message": "PostgreSQL respondendo"}
        except Exception as e:
            return {"status": "unhealthy", "message": f"Erro DB: {str(e)}"}

    @staticmethod
    def check_connection_pool():
        """Verifica pool de conexões"""
        try:
            pool = engine.pool
            return {
                "status": "healthy",
                "pool_size": pool.size(),
                "checked_in": pool.checkedout(),
                "message": f"Pool: {pool.checkedout()}/{pool.size()} conexões ativas",
            }
        except Exception as e:
            return {"status": "unhealthy", "message": f"Erro pool: {str(e)}"}

    @staticmethod
    def check_disk_space():
        """Verifica espaço em disco"""
        try:
            import shutil

            stats = shutil.disk_usage("/")
            percent_free = (stats.free / stats.total) * 100

            if percent_free < 10:
                return {
                    "status": "warning",
                    "percent_free": percent_free,
                    "message": "Disco < 10%",
                }
            elif percent_free < 5:
                return {
                    "status": "unhealthy",
                    "percent_free": percent_free,
                    "message": "Disco crítico!",
                }

            return {
                "status": "healthy",
                "percent_free": percent_free,
                "message": f"{percent_free:.1f}% livre",
            }
        except Exception as e:
            return {"status": "unknown", "message": str(e)}

    @staticmethod
    def check_memory():
        """Verifica uso de memória"""
        try:
            import psutil

            mem = psutil.virtual_memory()

            if mem.percent > 90:
                return {
                    "status": "unhealthy",
                    "percent": mem.percent,
                    "message": "Memória crítica!",
                }
            elif mem.percent > 75:
                return {
                    "status": "warning",
                    "percent": mem.percent,
                    "message": f"Memória alta: {mem.percent}%",
                }

            return {
                "status": "healthy",
                "percent": mem.percent,
                "message": f"{mem.percent}% em uso",
            }
        except Exception as e:
            return {"status": "unknown", "message": str(e)}

    @staticmethod
    def full_check():
        """Executa verificação completa"""
        checks = {
            "timestamp": datetime.now().isoformat(),
            "database": HealthCheck.check_database(),
            "connection_pool": HealthCheck.check_connection_pool(),
            "disk_space": HealthCheck.check_disk_space(),
            "memory": HealthCheck.check_memory(),
        }

        # Status geral
        all_healthy = all(
            check.get("status") in ["healthy"]
            for check in checks.values()
            if isinstance(check, dict) and "status" in check
        )

        checks["overall"] = "healthy" if all_healthy else "warning"

        return checks


def print_health_report(checks):
    """Imprime relatório formatado"""
    print("\n" + "=" * 80)
    print("🏥 HEALTH CHECK DO SISTEMA")
    print("=" * 80)

    print(f"\n⏰ {checks['timestamp']}")
    print(f"Status Geral: {checks['overall'].upper()}")

    for component, data in checks.items():
        if component in ["timestamp", "overall"] or not isinstance(data, dict):
            continue

        status_icon = {
            "healthy": "✅",
            "warning": "⚠️",
            "unhealthy": "❌",
            "unknown": "❓",
        }.get(data.get("status"), "?")

        message = data.get("message", "N/A")
        print(f"\n{status_icon} {component.upper()}")
        print(f"   {message}")

        if "percent" in data:
            print(f"   Uso: {data['percent']}%")
        if "pool_size" in data:
            print(f"   Checked in: {data['checked_in']}/{data['pool_size']}")

    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    import json
    import argparse

    parser = argparse.ArgumentParser(description="Health check do sistema")
    parser.add_argument("--json", action="store_true", help="Saída em JSON")
    parser.add_argument(
        "--continuous", action="store_true", help="Monitorar continuamente"
    )
    parser.add_argument(
        "--interval", type=int, default=60, help="Intervalo em segundos"
    )

    args = parser.parse_args()

    if args.continuous:
        import time

        while True:
            checks = HealthCheck.full_check()
            if args.json:
                print(json.dumps(checks))
            else:
                print_health_report(checks)
            time.sleep(args.interval)
    else:
        checks = HealthCheck.full_check()
        if args.json:
            print(json.dumps(checks, indent=2))
        else:
            print_health_report(checks)
