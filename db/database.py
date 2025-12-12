import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from models import Base
from sqlalchemy.orm import sessionmaker

# Carregar .env do diretório pai (override=True para sobrescrever variáveis já definidas)
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path, override=True)

# Use PostgreSQL connection string from environment variable
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:2584@72.60.146.124:5432/portal_associacao_db")
engine = create_engine(
    DATABASE_URL,
    pool_size=20,              # Número de conexões permanentes no pool
    max_overflow=40,           # Máximo de conexões extras que podem ser criadas
    pool_recycle=3600,         # Reciclar conexões após 1 hora
    pool_pre_ping=True,        # Verificar se a conexão está viva antes de usar
    echo=False                 # Set to True para debug de SQL queries
)
Session = sessionmaker(bind=engine)

def get_session():
    return Session()

def init_db():
    """
    Initialize the database by creating all tables defined in models.
    This is called automatically on app startup.
    """
    
    Base.metadata.create_all(engine)