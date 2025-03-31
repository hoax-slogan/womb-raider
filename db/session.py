from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ..config import Config


cfg = Config()
engine = create_engine(cfg.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)