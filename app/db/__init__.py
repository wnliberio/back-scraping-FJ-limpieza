# app/db/__init__.py - SQL SERVER CON PYMSSQL (SIN connect_args)
from __future__ import annotations

import os
import urllib.parse as up
from typing import Dict, Any
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# Cargar .env
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
        print(f"✅ Archivo .env cargado desde: {env_path}")
except ImportError:
    print("⚠️ python-dotenv no instalado")
except Exception as e:
    print(f"⚠️ Error cargando .env: {e}")

# Zona horaria Ecuador
try:
    import pytz
    ECUADOR_TZ = pytz.timezone("America/Guayaquil")
    from datetime import datetime
    def get_ec_time() -> datetime:
        return datetime.now(ECUADOR_TZ)
except Exception:
    from datetime import datetime
    def get_ec_time() -> datetime:
        return datetime.now()


def _build_sqlalchemy_url() -> str:
    """Construye la URL de SQLAlchemy desde variables de entorno."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        if "mssql+pymssql://" in database_url:
            print("✅ Detectado: SQL Server con pymssql (desde DATABASE_URL)")
            return database_url
        elif "mssql+pyodbc://" in database_url:
            print("✅ Detectado: SQL Server con pyodbc (desde DATABASE_URL)")
            return database_url
        elif database_url.startswith("mysql+pymysql://"):
            print("✅ Detectado: MySQL (desde DATABASE_URL)")
            return database_url
    
    db_type = os.getenv("DB_TYPE", "").lower()
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    
    if not all([user, pwd, name]):
        raise RuntimeError(
            "⚠️ ERROR: Credenciales incompletas.\n"
            f"  - DB_USER: {'✅' if user else '❌ FALTANTE'}\n"
            f"  - DB_PASSWORD: {'✅' if pwd else '❌ FALTANTE'}\n"
            f"  - DB_NAME: {'✅' if name else '❌ FALTANTE'}\n"
        )
    
    if db_type == "sqlserver" or port == "1433":
        port = port or "1433"
        auth = f"{up.quote_plus(user)}:{up.quote_plus(pwd)}"
        url = f"mssql+pymssql://{auth}@{host}:{port}/{name}"
        print(f"✅ Construido URL SQL Server (pymssql): {host}:{port}/{name}")
        return url
    
    port = port or "3306"
    auth = f"{up.quote_plus(user)}:{up.quote_plus(pwd)}"
    url = f"mysql+pymysql://{auth}@{host}:{port}/{name}?charset=utf8mb4"
    print(f"✅ Construido URL MySQL: {host}:{port}/{name}")
    return url


# Crear Engine - SIN connect_args para pymssql
try:
    DATABASE_URL = _build_sqlalchemy_url()
    
    # pymssql NO acepta connect_args, así que NO lo pasamos
    engine = create_engine(
        DATABASE_URL,
        future=True,
        pool_pre_ping=True,
        pool_recycle=280,
        echo=False
    )
    
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base = declarative_base()
    
    print("✅ Engine de base de datos creado correctamente")
    
except Exception as e:
    print(f"❌ ERROR CRÍTICO: {e}")
    raise


def get_db():
    """Dependency para FastAPI"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection():
    """Prueba la conexión a la base de datos"""
    try:
        db = SessionLocal()
        result = db.execute(text("SELECT 1 AS test")).fetchone()
        db.close()
        print("✅ Conexión a base de datos exitosa")
        print(f"   Resultado del test: {result}")
        return True
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False

def get_db_for_data_load():
    """Dependencia específica para la carga de datos en de_clientes_rpa."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

__all__ = [
    "engine",
    "SessionLocal",
    "Base",
    "get_db",
    "test_connection",
    "get_ec_time"
]
