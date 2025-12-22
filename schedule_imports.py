#!/usr/bin/env python3
"""
Agendador para executar importações automaticamente em horários específicos.
Roda como daemon em background.

Uso:
    python3 schedule_imports.py               # Inicia agendador
    python3 schedule_imports.py --list        # Lista agendamentos
    python3 schedule_imports.py --add "02:00" # Adiciona importação às 02:00
    python3 schedule_imports.py --remove "02:00"
"""

import sys
import os
import time
import json
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.import_manager import import_manager

CONFIG_FILE = Path(__file__).parent / ".import_schedule.json"


def load_schedule():
    """Carrega agendamento do arquivo"""
    if not CONFIG_FILE.exists():
        return {"schedules": [], "refresh_views": False}

    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except:
        return {"schedules": [], "refresh_views": False}


def save_schedule(config):
    """Salva agendamento no arquivo"""
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


def add_schedule(hour_str, refresh_views=False):
    """Adiciona novo agendamento"""
    try:
        parts = hour_str.split(":")
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0

        if not (0 <= hour < 24 and 0 <= minute < 60):
            print(f"❌ Horário inválido: {hour_str}")
            return False

        config = load_schedule()
        schedule_str = f"{hour:02d}:{minute:02d}"

        if schedule_str in config["schedules"]:
            print(f"⚠️ Agendamento para {schedule_str} já existe")
            return False

        config["schedules"].append(schedule_str)
        config["schedules"].sort()
        config["refresh_views"] = refresh_views
        save_schedule(config)

        print(f"✅ Agendamento adicionado: {schedule_str}")
        return True
    except Exception as e:
        print(f"❌ Erro ao adicionar agendamento: {e}")
        return False


def remove_schedule(hour_str):
    """Remove agendamento"""
    config = load_schedule()
    schedule_str = f"{hour_str}"

    if schedule_str not in config["schedules"]:
        print(f"⚠️ Agendamento não encontrado: {schedule_str}")
        return False

    config["schedules"].remove(schedule_str)
    save_schedule(config)

    print(f"✅ Agendamento removido: {schedule_str}")
    return True


def list_schedules():
    """Lista todos os agendamentos"""
    config = load_schedule()

    if not config["schedules"]:
        print("Nenhum agendamento configurado")
        return

    print("\n" + "=" * 80)
    print("📅 AGENDAMENTOS DE IMPORTAÇÃO")
    print("=" * 80)
    for schedule in config["schedules"]:
        print(f"  🕐 {schedule}")
    print(f"\nAtualizar MVs: {'Sim' if config['refresh_views'] else 'Não'}")
    print("=" * 80 + "\n")


def run_daemon(check_interval=60):
    """Executa agendador como daemon"""
    print("🔄 Agendador iniciado. Pressione Ctrl+C para parar.")
    print(f"   Verificando a cada {check_interval}s\n")

    try:
        while True:
            now = datetime.now()
            current_time = f"{now.hour:02d}:{now.minute:02d}"

            config = load_schedule()

            # Verifica se é hora de importar
            if current_time in config["schedules"]:
                if not import_manager.is_running():
                    print(
                        f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] ⏰ Hora de importar!"
                    )
                    print(
                        f"Iniciando importação com refresh_views={config['refresh_views']}..."
                    )
                    import_manager.start_import(refresh_views=config["refresh_views"])

                    # Aguarda 60 segundos para não disparar múltiplas vezes
                    time.sleep(60)

            time.sleep(check_interval)

    except KeyboardInterrupt:
        print("\n\n✋ Agendador parado")


def main():
    parser = argparse.ArgumentParser(description="Agendar importações automáticas")
    parser.add_argument("--list", action="store_true", help="Listar agendamentos")
    parser.add_argument(
        "--add", type=str, metavar="HH:MM", help="Adicionar agendamento (ex: 02:00)"
    )
    parser.add_argument(
        "--remove", type=str, metavar="HH:MM", help="Remover agendamento"
    )
    parser.add_argument(
        "--refresh-views",
        action="store_true",
        help="Ativar refresh de MVs nos agendamentos",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Rodar como daemon (padrão se nenhuma opção)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Intervalo de verificação em segundos (padrão: 60)",
    )

    args = parser.parse_args()

    # Se nenhuma opção, roda como daemon
    if not args.list and not args.add and not args.remove:
        args.daemon = True

    if args.list:
        list_schedules()
        return 0

    if args.add:
        if args.refresh_views:
            config = load_schedule()
            config["refresh_views"] = True
            save_schedule(config)
        return 0 if add_schedule(args.add) else 1

    if args.remove:
        return 0 if remove_schedule(args.remove) else 1

    if args.daemon:
        run_daemon(args.interval)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
