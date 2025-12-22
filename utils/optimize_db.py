from sqlalchemy import text
from db.database import engine
import logging
import sys
import os

# Adiciona o diretório pai ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def optimize_database():
    """
    Cria índices críticos para performance do dashboard.
    """
    try:
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            logger.info("Verificando e criando índices de performance...")

            # Índice para a query de Top 10 Displacement
            # WHERE start_time >= ... AND client = ...
            index_name = "idx_movements_client_starttime"
            logger.info(f"Criando índice {index_name} (se não existir)...")

            # Usamos CONCURRENTLY para não travar a tabela em produção
            create_index_sql = text(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
                ON movements (client, start_time DESC);
            """
            )

            connection.execute(create_index_sql)
            logger.info(f"Índice {index_name} verificado/criado com sucesso.")

            # Índice para Assets (usado no JOIN)
            # JOIN assets a ON r.asset_serial_number = a.oem_serial_number
            index_assets = "idx_assets_oem_serial"
            logger.info(f"Criando índice {index_assets} (se não existir)...")

            create_index_assets_sql = text(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_assets}
                ON assets (oem_serial_number);
            """
            )

            connection.execute(create_index_assets_sql)
            logger.info(f"Índice {index_assets} verificado/criado com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao otimizar banco: {e}")


if __name__ == "__main__":
    optimize_database()
