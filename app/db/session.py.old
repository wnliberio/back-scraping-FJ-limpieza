# app/db/session.py  (SINCRÓNICO)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
import os

DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://user:pass@localhost:3306/tu_db")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# FastAPI dependency
def get_session() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
