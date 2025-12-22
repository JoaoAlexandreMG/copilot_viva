import concurrent.futures
import sys
import os
import subprocess
from datetime import datetime

# Adicionar o diretório pai ao path para importar do utils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from utils.scraping import (
    buscar_contas_vision,
    processar_cliente_dados_diarios,
    processar_cliente_dados_estaticos,
)


def processar_conta_wrapper(args):
    """
    Wrapper para desempacotar argumentos e chamar a função de processamento apropriada
    """
    conta_data, tipo_processamento = args
    conta, client_name = conta_data

    try:
        if tipo_processamento == "static":
            return client_name, processar_cliente_dados_estaticos(
                username=conta.username,
                password=conta.password,
                client_name=client_name,
            )
        else:
            # Daily
            return client_name, processar_cliente_dados_diarios(
                username=conta.username,
                password=conta.password,
                client_name=client_name,
            )
    except Exception as e:
        print(f"ERRO CRÍTICO na thread de {client_name}: {str(e)}")
        return client_name, False


def main_parallel(tipo="daily", max_workers=2):
    """
    Executa o scraping em paralelo
    """
    print(f"INICIANDO SCRAPING PARALELO ({tipo.upper()}) - Max Workers: {max_workers}")
    print("=" * 80)

    # Buscar todas as contas da base de dados
    contas = buscar_contas_vision()

    if not contas:
        print("ERRO - Nenhuma conta VisionAccount encontrada na base de dados")
        return

    print(f"Encontradas {len(contas)} contas para processar:")
    for conta_data in contas:
        conta, client_name = conta_data
        print(f"  - {client_name} (Username: {conta.username})")

    print("\nIniciando processamento paralelo...")

    resultados = {}

    # Preparar argumentos para o map
    # Cada item é uma tupla (conta_data, tipo)
    work_items = [(conta, tipo) for conta in contas]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Mapear a função wrapper para os itens de trabalho
        future_to_client = {
            executor.submit(processar_conta_wrapper, item): item[0][1]
            for item in work_items
        }

        for future in concurrent.futures.as_completed(future_to_client):
            client_name = future_to_client[future]
            try:
                c_name, sucesso = future.result()
                resultados[c_name] = sucesso
                status = "SUCESSO" if sucesso else "ERRO"
                print(f"\n>>> CONCLUSÃO: {client_name} finalizado com {status}")
            except Exception as exc:
                print(f"\n>>> ERRO: {client_name} gerou uma exceção: {exc}")
                resultados[client_name] = False

    # Resumo final
    print(f"\n{'='*80}")
    print(f"RESUMO FINAL - TODOS OS CLIENTES ({tipo.upper()})")
    print(f"{'='*80}")

    for client_name, sucesso in resultados.items():
        status = "✓ SUCESSO" if sucesso else "✗ ERRO"
        print(f"{client_name}: {status}")

    sucessos = sum(1 for s in resultados.values() if s)
    total = len(resultados)

    print(f"\nTotal processado: {sucessos}/{total} clientes com sucesso")

    print("✓ Clientes processados com sucesso!")

    # Importação automática DESABILITADA - use o dashboard para importar
    print(f"\n{'='*60}")
    print("SCRAPING CONCLUÍDO")
    print(f"{'='*60}")
    print("Arquivos salvos em: docs/")
    print("Para importar os dados, use o botão 'Iniciar' no Dashboard Admin")

    # # Executar automaticamente o script de importação
    # print(f"\n{'='*60}")
    # print("INICIANDO IMPORTAÇÃO AUTOMÁTICA DOS DADOS")
    # print(f"{'='*60}")

    # try:
    #     # Reiniciar PostgreSQL antes da importação
    #     print("Reiniciando serviço PostgreSQL...")
    #     try:
    #         subprocess.run(
    #             ["sudo", "systemctl", "restart", "postgresql"], check=True
    #         )
    #         print("✓ PostgreSQL reiniciado com sucesso.")
    #     except subprocess.CalledProcessError as e:
    #         print(f"⚠ Aviso: Não foi possível reiniciar o PostgreSQL: {e}")
    #         print("Continuando com a importação...")

    #     # Executar o script import_all_data.py
    #     # Assumindo que está na raiz do projeto (parent_dir)
    #     script_path = os.path.join(parent_dir, "import_all_data.py")

    #     result = subprocess.run(
    #         [sys.executable, script_path],
    #         capture_output=True,
    #         text=True,
    #         cwd=parent_dir,  # Executar no diretório raiz
    #     )

    #     if result.returncode == 0:
    #         print("✓ Importação automática concluída com sucesso!")
    #         if result.stdout:
    #             print("\nSaída da importação:")
    #             print(result.stdout)
    #     else:
    #         print("✗ Erro durante a importação automática:")
    #         if result.stderr:
    #             print("Erro:", result.stderr)
    #         if result.stdout:
    #             print("Saída:", result.stdout)

    # except Exception as e:
    #     print(f"✗ Erro ao executar importação automática: {str(e)}")
    #     print("Execute manualmente: python3 import_all_data.py")


if __name__ == "__main__":
    # Default para 2 workers, mas pode ser alterado via argumento opcional
    # Uso: python scraping_parallel.py [static|daily] [num_workers]

    tipo = "daily"
    workers = 3

    if len(sys.argv) > 1:
        if sys.argv[1] == "static":
            tipo = "static"

    if len(sys.argv) > 2:
        try:
            workers = int(sys.argv[2])
        except ValueError:
            print("Número de workers inválido, usando padrão: 2")

    main_parallel(tipo, workers)
