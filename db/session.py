from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine


def get_engine(database_url: str) -> Engine:
    return create_engine(database_url)


def get_session_maker(database_url: str = None, engine=None):
    if engine is None:
        if database_url is None:
            raise ValueError("Must provide either engine or database_url")
        engine = get_engine(database_url)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)