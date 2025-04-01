from sqlalchemy import create_engine
from .config import Config
from .db.models import Base 


cfg = Config()
engine = create_engine(cfg.DATABASE_URL)


Base.metadata.create_all(engine)
print("Database schema created successfully.")