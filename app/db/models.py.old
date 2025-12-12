# app/db/models.py
from __future__ import annotations

from typing import Optional, Dict, Any

from sqlalchemy import Integer, String, Float, Date, Text
from sqlalchemy.dialects.mysql import JSON as MySQLJSON
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)

    tipo_alerta: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    monto_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fecha_alerta: Mapped[Optional] = mapped_column(Date, nullable=True)

    # Ruta absoluta al .docx generado (o al archivo que decidas)
    file_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Snapshot (JSON) con lo relevante del job al momento de guardar
    data_snapshot: Mapped[Optional[Dict[str, Any]]] = mapped_column(MySQLJSON, nullable=True)
