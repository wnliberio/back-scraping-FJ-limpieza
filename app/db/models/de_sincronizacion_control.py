# app/db/models/de_sincronizacion_control.py
"""
Modelo SQLAlchemy para tabla de_sincronizacion_control
"""

from __future__ import annotations

from typing import Optional
from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class DeSincronizacionControl(Base):
    """
    Modelo para tabla de_sincronizacion_control
    
    Auditoría completa de cada traída desde DB2
    """
    __tablename__ = "de_sincronizacion_control"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    nombre_proceso: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    numero_sincronizacion: Mapped[int] = mapped_column(Integer, nullable=False)
    
    fecha_hora_inicio: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    fecha_hora_fin: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duracion_segundos: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    registros_traidos: Mapped[int] = mapped_column(Integer, default=0)
    registros_insertados: Mapped[int] = mapped_column(Integer, default=0)
    registros_duplicados: Mapped[int] = mapped_column(Integer, default=0)
    registros_error: Mapped[int] = mapped_column(Integer, default=0)
    
    estado: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    mensaje_resultado: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    fecha_minima_db2: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    fecha_maxima_db2: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)