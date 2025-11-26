# db/origen.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

def get_origen_engine():
    DATABASE_URL = os.getenv("DATABASE_ORIGEN_URL")  # nueva DB
    
    engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
    return engine

OrigenSessionLocal = sessionmaker(bind=get_origen_engine(), autoflush=False, autocommit=False, future=True)

def get_origen_db():
    db = OrigenSessionLocal()
    try:
        yield db
    finally:
        db.close()
from sqlalchemy import text

def test_origen_connection():
    """Prueba la conexión a la DB origen"""
    try:
        db = next(get_origen_db())
        result = db.execute(text("SELECT 1 AS test")).fetchone()
        db.close()
        print("✅ Conexión a DB origen exitosa")
        print(f"   Resultado del test: {result}")
        return True
    except Exception as e:
        print(f"❌ Error de conexión a DB origen: {e}")
        return False
