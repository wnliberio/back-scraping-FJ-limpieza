# app/db/session_fix.py - SOLUCIÓN PARA EL ERROR DE CONTEXT MANAGER

from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

# Usar tu configuración existente de base de datos
# Reemplaza estas líneas con tu configuración actual:

# OPCIÓN 1: Si ya tienes engine configurado, usarlo
# from app.db import engine  # Tu engine existente

# OPCIÓN 2: Si necesitas crear engine nuevo (usar tu DATABASE_URL)
import os
DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://user:pass@localhost/db")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Crear SessionLocal
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

@contextmanager
def get_session():
    """Context manager corregido para sesiones de BD"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

# ALTERNATIVA: Si prefieres sin context manager
def get_db_session():
    """Función simple que retorna sesión (manual close)"""
    return SessionLocal()

# Función para verificar conexión
def test_connection():
    """Prueba la conexión a la base de datos"""
    try:
        with get_session() as db:
            db.execute("SELECT 1")
        return {"status": "connected", "error": None}
    except Exception as e:
        return {"status": "error", "error": str(e)}