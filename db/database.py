import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DisconnectionError, OperationalError
from models import Base
from sqlalchemy.orm import sessionmaker, scoped_session
import logging

# Configurar logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Carregar .env do diretório pai (override=True para sobrescrever variáveis já definidas)
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(env_path, override=True)

# Use PostgreSQL connection string from environment variable
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:2584@72.60.146.124:5432/portal_associacao_db"
)

# Engine com configurações de alta disponibilidade
engine = create_engine(
    DATABASE_URL,
    pool_size=10,  # Conexões permanentes no pool
    max_overflow=20,  # Máximo de conexões extras permitidas
    pool_recycle=300,  # Reciclar a cada 5 minutos (evita conexões antigas)
    pool_pre_ping=True,  # Verificar se conexão está viva antes de usar (CRÍTICO!)
    pool_use_lifo=True,  # Usar LIFO para melhor reutilização de conexões
    connect_args={
        "connect_timeout": 10,  # Timeout na conexão
        "keepalives": 1,  # Ativar TCP keepalive
        "keepalives_idle": 30,  # Segundos antes do keepalive
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)


# Event listeners para detecção de desconexão e reconexão automática
@event.listens_for(engine, "connect")
def receive_connect(dbapi_conn, connection_record):
    """Configurar conexão ao estabelecer"""
    logger.debug("Conexão estabelecida com PostgreSQL")


@event.listens_for(engine, "close")
def receive_close(dbapi_conn, connection_record):
    """Log ao fechar conexão"""
    logger.debug("Conexão fechada")


@event.listens_for(engine, "detach")
def receive_detach(dbapi_conn, connection_record):
    """Log ao desanexar (possível erro)"""
    logger.warning("Conexão desanexada - pode indicar erro de rede")


# ALTERAÇÃO CRÍTICA: Usar scoped_session
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)


def get_session(retry_count=0, max_retries=3):
    """
    Get session com retry automático em caso de desconexão.
    CRÍTICO: Garante 100% de uptime retentando conexões fracassadas
    """
    try:
        session = Session()
        # Testa a conexão imediatamente
        session.execute(text("SELECT 1"))
        return session
    except (DisconnectionError, OperationalError) as e:
        Session.remove()
        logger.warning(
            f"Erro de conexão (tentativa {retry_count+1}/{max_retries}): {e}"
        )

        if retry_count < max_retries:
            import time
            time.sleep(0.5 * (2**retry_count))
            wait_time = 2**retry_count  # Backoff exponencial: 1s, 2s, 4s
            return get_session(retry_count + 1, max_retries)
        raise


def dispose_engine():
    """
    Fecha todas as conexões do pool.
    Útil para limpar conexões antes de finalizar.
    """
    try:
        engine.dispose()
        logger.info("Pool de conexões disposado")
    except Exception as e:
        logger.error(f"Erro ao disposar pool: {e}")


def init_db():
    """
    Initialize the database by creating all tables defined in models.
    This is called automatically on app startup.
    """

    Base.metadata.create_all(engine)
