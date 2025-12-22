from sqlalchemy import text
from db.database import engine
import logging
import sys
import os

# Adiciona o diretório pai ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def kill_pid(pid):
    try:
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            logger.info(f"Tentando matar PID {pid}...")
            kill_query = text(f"SELECT pg_terminate_backend({pid});")
            connection.execute(kill_query)
            logger.info(f"Comando enviado para PID {pid}.")
    except Exception as e:
        logger.error(f"Erro: {e}")


if __name__ == "__main__":
    kill_pid(11754)
