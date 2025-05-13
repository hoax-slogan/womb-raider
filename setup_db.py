from .config import get_database_url
from .db.models import Base
from .db.session import get_engine


engine = get_engine(get_database_url())
Base.metadata.create_all(bind=engine)
print("[db] Database schema created successfully.")
