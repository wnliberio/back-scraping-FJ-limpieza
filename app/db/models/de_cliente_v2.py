# app/db/models/de_cliente_v2.py
"""
Modelo SQLAlchemy para tabla de_clientes_rpa_v2
"""

from __future__ import annotations

from typing import Optional
from datetime import datetime

from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class DeClienteV2(Base):
    """
    Modelo para tabla de_clientes_rpa_v2
    
    Sincronización DB2 → SQL Server (nueva versión V2)
    """
    __tablename__ = "de_clientes_rpa_v2"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    ID_SOLICITUD: Mapped[Optional[int]] = mapped_column(Integer, unique=True, nullable=True, index=True)
    FECHA_CREACION_SOLICITUD: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ESTADO: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    AGENCIA: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    ID_PRODUCTO: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    PRODUCTO: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    CEDULA: Mapped[Optional[str]] = mapped_column(String(10), nullable=True, index=True)
    NOMBRES_CLIENTE: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    APELLIDOS_CLIENTE: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    ESTADO_CIVIL: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    CEDULA_CONYUGE: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    NOMBRES_CONYUGE: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    APELLIDOS_CONYUGE: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    CEDULA_CODEUDOR: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    NOMBRES_CODEUDOR: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    APELLIDOS_CODEUDOR: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    
    ESTADO_CONSULTA: Mapped[str] = mapped_column(String(50), nullable=False, default='Pendiente', index=True)
    
    FECHA_CREACION_REGISTRO: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now, index=True)