import os
from sqlalchemy import create_engine
from models import Base
from sqlalchemy.orm import sessionmaker

# Use PostgreSQL connection string from environment variable
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:2584@72.60.146.124:5432/portal_associacao_db")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def get_session():
    return Session()

def init_db():
    """
    Initialize the database by creating all tables defined in models.
    This is called automatically on app startup.
    """
    
    Base.metadata.create_all(engine)