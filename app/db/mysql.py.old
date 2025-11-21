# app/db/mysql.py
from __future__ import annotations

import os
import re
from typing import Optional, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

def _compose_sqlalchemy_url() -> str:
    """
    Prioridades:
    1) DATABASE_URL (formato SQLAlchemy completo)
    2) URL (JDBC) + DB_USER/DB_PASSWORD  -> ej: jdbc:mysql://host:3306/db
    3) DB_HOST/DB_PORT/DB_NAME + DB_USER/DB_PASSWORD
    """
    sa_url = os.getenv("DATABASE_URL")
    if sa_url:
        return sa_url

    jdbc = os.getenv("URL")  # p.ej. jdbc:mysql://asiste.mysql.database.azure.com:3306/asistentedb
    user = os.getenv("DB_USER") or os.getenv("MYSQL_USER")
    pwd  = os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD")

    if jdbc:
        m = re.match(r"^jdbc:mysql://([^:/]+)(?::(\d+))?/([^?]+)$", jdbc.strip())
        if not m:
            raise RuntimeError("URL JDBC inválida. Esperado: jdbc:mysql://HOST:PORT/DBNAME")
        host, port, db = m.group(1), m.group(2) or "3306", m.group(3)
        if not user or not pwd:
            raise RuntimeError("Define DB_USER y DB_PASSWORD para usar URL JDBC.")
        return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    db   = os.getenv("DB_NAME")
    if host and db and user and pwd:
        return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"

    raise RuntimeError(
        "No se pudo construir la URL de base de datos. "
        "Define DATABASE_URL, o URL (JDBC) + DB_USER/DB_PASSWORD, "
        "o DB_HOST/DB_PORT/DB_NAME + DB_USER/DB_PASSWORD."
    )

# --------- SSL Helpers ----------
def _build_connect_args_for_ssl() -> Dict:
    """
    Azure MySQL Flexible/Single Server requiere TLS cuando require_secure_transport=ON.
    Usamos variables de entorno:
      DB_SSL_CA               -> ruta al certificado CA (recomendado)
      DB_SSL_VERIFY_CERT      -> '1'/'0' (default 1)
      DB_SSL_VERIFY_IDENTITY  -> '1'/'0' (default 0)
      DB_SSL_MODE             -> si no hay CA, forzar 'REQUIRED' (opcional)
    """
    connect_args: Dict = {}

    ssl_ca = os.getenv("DB_SSL_CA")  # ej: certs/DigiCertGlobalRootG2.crt.pem
    verify_cert = (os.getenv("DB_SSL_VERIFY_CERT", "1").lower() in ("1", "true", "yes"))
    verify_identity = (os.getenv("DB_SSL_VERIFY_IDENTITY", "0").lower() in ("1", "true", "yes"))

    if ssl_ca:
        # PyMySQL acepta estos kwargs directamente
        connect_args["ssl_ca"] = ssl_ca
        connect_args["ssl_verify_cert"] = verify_cert
        # verify_identity valida CN/SAN del certificado vs host
        connect_args["ssl_verify_identity"] = verify_identity
    else:
        # Fallback: intenta activar TLS aun sin CA (no recomendado, pero evita el 3159).
        # En PyMySQL, pasar 'ssl' como dict vacío obliga handshake TLS.
        # Úsalo solo si no tienes el CA a mano.
        if os.getenv("DB_SSL_MODE", "REQUIRED").upper() == "REQUIRED":
            connect_args["ssl"] = {}  # TLS sin verificación de CA

    return connect_args

# --------- Engine/Session singletons ----------
_engine = None
_SessionLocal = None

def get_engine():
    global _engine, _SessionLocal
    if _engine is None:
        sa_url = _compose_sqlalchemy_url()
        connect_args = _build_connect_args_for_ssl()

        _engine = create_engine(
            sa_url,
            pool_pre_ping=True,
            pool_recycle=280,
            future=True,
            connect_args=connect_args,
        )
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)
    return _engine

def get_session():
    """
    Uso:
        from app.db.mysql import get_session
        with get_session() as db:
            ...
    """
    engine = get_engine()
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
