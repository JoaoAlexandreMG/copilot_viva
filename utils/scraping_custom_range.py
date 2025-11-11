#!/usr/bin/env python3
"""
Variante do script de scraping com suporte a datas customizadas.
Focado em baixar apenas as planilhas de Health Events e Movements
e, ao final, importar os dados para o banco via import_excel_to_db.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from typing import Optional

from scraping import (
    buscar_registros_movimento,
    buscar_registros_saude,
    deslogar,
    logar,
)
from import_excel_to_db import main as import_excel_main


DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y")


def parse_date(value: str, label: str) -> datetime:
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"{label}: formato inválido '{value}'. Use YYYY-MM-DD ou DD/MM/YYYY."
    )


def format_period(start: Optional[datetime], end: Optional[datetime]) -> str:
    if not start or not end:
        return "últimos 2 dias (padrão VisionIOT)"
    return f"{start.strftime('%d/%m/%Y')} → {end.strftime('%d/%m/%Y')}"


def run_pipeline(start: Optional[datetime], end: Optional[datetime]) -> bool:
    session = logar()
    if not session:
        print("\nERRO - Falha no login. Nao foi possivel buscar dados.")
        return False

    try:
        print(f"\nSessao estabelecida. Buscando dados no período: {format_period(start, end)}")

        dados_saude = buscar_registros_saude(session, start, end)
        dados_movimento = buscar_registros_movimento(session, start, end)
    finally:
        deslogar(session)

    print("\n" + "=" * 60)
    print("RESUMO - DOWNLOAD")
    print("=" * 60)
    print(f"Health Events:    {'OK' if dados_saude else 'ERRO'}")
    print(f"Movements:        {'OK' if dados_movimento else 'ERRO'}")

    if not all([dados_saude, dados_movimento]):
        print("\n✗ Insercao no banco cancelada devido a falhas no download")
        return False

    print("\n" + "=" * 60)
    print("EXECUTANDO import_excel_to_db.py PARA ATUALIZAR O BANCO")
    print("=" * 60)

    import_status = import_excel_main(["--tables", "health_events", "movements"])
    sucesso_import = import_status == 0

    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"Download Health Events: {'OK' if dados_saude else 'ERRO'}")
    print(f"Download Movements: {'OK' if dados_movimento else 'ERRO'}")
    print(f"Importação via import_excel_to_db: {'OK' if sucesso_import else 'ERRO'}")

    if dados_saude and dados_movimento and sucesso_import:
        print("\n✓ Processo concluido com sucesso!")
        return True

    print("\n✗ Processo concluido com erros")
    return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Exporta dados do VisionIOT permitindo informar datas customizadas."
    )
    parser.add_argument(
        "--start",
        "-s",
        help="Data inicial (YYYY-MM-DD ou DD/MM/YYYY). Se omitida, usa (data final - 2 dias).",
    )
    parser.add_argument(
        "--end",
        "-e",
        help="Data final (YYYY-MM-DD ou DD/MM/YYYY). Se omitida, usa data atual.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    start_dt = parse_date(args.start, "--start") if args.start else None
    end_dt = parse_date(args.end, "--end") if args.end else None

    if start_dt and not end_dt:
        end_dt = datetime.now()

    if end_dt and not start_dt:
        start_dt = end_dt - timedelta(days=2)

    if start_dt and end_dt and start_dt > end_dt:
        parser.error("Data inicial nao pode ser maior que a data final.")

    try:
        sucesso = run_pipeline(start_dt, end_dt)
    except ValueError as exc:
        print(f"ERRO - {exc}")
        return 1

    return 0 if sucesso else 1


if __name__ == "__main__":
    sys.exit(main())
