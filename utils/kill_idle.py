from sqlalchemy import text
from db.database import engine
import logging
import sys
import os
import time

# Adiciona o diretório pai ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Forçar reconfiguração do logging
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def kill_idle_connections():
    try:
        # Usar isolation_level="AUTOCOMMIT"
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:

            logger.info("--- INICIANDO LIMPEZA DE CONEXÕES ---")

            # 1. Listar
            check_query = text(
                """
                SELECT pid, usename, state, state_change, query 
                FROM pg_stat_activity 
                WHERE state IN ('idle', 'idle in transaction')
                AND pid <> pg_backend_pid()
                AND datname = current_database();
            """
            )

            result = connection.execute(check_query)
            rows = result.fetchall()

            if not rows:
                logger.info("Nenhuma conexão ociosa encontrada.")
                return

            logger.info(f"Encontradas {len(rows)} conexões ociosas.")
            for row in rows:
                q = row[4][:50] + "..." if row[4] else "None"
                logger.info(f" -> PID: {row[0]} | State: {row[2]} | Time: {row[3]}")

            # 2. Kill
            logger.info("Executando pg_terminate_backend...")
            kill_query = text(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE state IN ('idle', 'idle in transaction')
                AND pid <> pg_backend_pid()
                AND datname = current_database();
            """
            )

            connection.execute(kill_query)

            # 3. Verificar
            time.sleep(2)

            count = connection.execute(
                text(
                    """
                SELECT count(*) 
                FROM pg_stat_activity 
                WHERE state IN ('idle', 'idle in transaction')
                AND pid <> pg_backend_pid()
                AND datname = current_database();
            """
                )
            ).scalar()

            if count == 0:
                logger.info("SUCESSO: Zero conexões ociosas restantes.")
            else:
                logger.warning(f"ATENÇÃO: Ainda restam {count} conexões ociosas.")

    except Exception as e:
        logger.error(f"Erro crítico: {e}")


if __name__ == "__main__":
    kill_idle_connections()
