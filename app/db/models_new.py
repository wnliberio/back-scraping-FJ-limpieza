# app/db/models_new.py - MODELOS PROFESIONALES PARA LA NUEVA ESTRUCTURA
from __future__ import annotations

from typing import Optional, Dict, Any, List
from datetime import datetime, date

from sqlalchemy import Integer, String, Text, Boolean, ForeignKey, JSON, DateTime, DECIMAL, BIGINT
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.mysql import ENUM

# 
from app.db import Base

class DeCliente(Base):
    """Modelo para de_clientes_rpa (evolución de de_lista)"""
    __tablename__ = "de_clientes_rpa"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nombre: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    apellido: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    ci: Mapped[Optional[str]] = mapped_column(String(10), nullable=True, index=True)
    ruc: Mapped[Optional[str]] = mapped_column(String(13), nullable=True, index=True)
    tipo: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    monto: Mapped[Optional[float]] = mapped_column(DECIMAL(10, 2), nullable=True)
    fecha: Mapped[Optional[date]] = mapped_column(DateTime, nullable=True)
    
    estado: Mapped[str] = mapped_column(
        ENUM('Pendiente', 'Procesando', 'Procesado', 'Error', name='estado_cliente_enum'),
        nullable=False, default='Pendiente', index=True
    )
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now, index=True)

    # Relaciones
    procesos: Mapped[List["DeProceso"]] = relationship("DeProceso", back_populates="cliente")
    reportes: Mapped[List["DeReporte"]] = relationship("DeReporte", back_populates="cliente")

class DePagina(Base):
    """Modelo para de_paginas_rpa (catálogo de páginas consultables)"""
    __tablename__ = "de_paginas_rpa"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    codigo: Mapped[str] = mapped_column(String(50), nullable=False, unique=True, index=True)
    url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    descripcion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    activa: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    orden_display: Mapped[int] = mapped_column(Integer, default=0, index=True)
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)

    # Relaciones
    consultas: Mapped[List["DeConsulta"]] = relationship("DeConsulta", back_populates="pagina")

class DeProceso(Base):
    """Modelo para de_procesos_rpa (jobs/flujos completos)"""
    __tablename__ = "de_procesos_rpa"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cliente_id: Mapped[int] = mapped_column(Integer, ForeignKey("de_clientes_rpa.id"), nullable=False, index=True)
    
    # Identificadores
    job_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, unique=True, index=True)
    
    # Metadatos (snapshot del cliente)
    tipo_alerta: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    monto_usd: Mapped[Optional[float]] = mapped_column(DECIMAL(10, 2), nullable=True)
    fecha_alerta: Mapped[Optional[date]] = mapped_column(DateTime, nullable=True)
    
    # Control de flujo
    estado: Mapped[str] = mapped_column(
        ENUM('Pendiente', 'En_Proceso', 'Completado', 'Completado_Con_Errores', 'Error_Total', 
             name='estado_proceso_enum'),
        nullable=False, default='Pendiente', index=True
    )
    
    # Timestamps
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now, index=True)
    fecha_inicio: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    fecha_fin: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Configuración
    headless: Mapped[bool] = mapped_column(Boolean, default=False)
    generate_report: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Contadores (mantenidos automáticamente por triggers)
    total_paginas_solicitadas: Mapped[int] = mapped_column(Integer, default=0)
    total_paginas_exitosas: Mapped[int] = mapped_column(Integer, default=0)
    total_paginas_fallidas: Mapped[int] = mapped_column(Integer, default=0)
    
    mensaje_error_general: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relaciones
    cliente: Mapped["DeCliente"] = relationship("DeCliente", back_populates="procesos")
    consultas: Mapped[List["DeConsulta"]] = relationship("DeConsulta", back_populates="proceso", cascade="all, delete-orphan")
    reportes: Mapped[List["DeReporte"]] = relationship("DeReporte", back_populates="proceso")

class DeConsulta(Base):
    """Modelo para de_consultas_rpa (consultas individuales por página)"""
    __tablename__ = "de_consultas_rpa"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    proceso_id: Mapped[int] = mapped_column(Integer, ForeignKey("de_procesos_rpa.id"), nullable=False, index=True)
    pagina_id: Mapped[int] = mapped_column(Integer, ForeignKey("de_paginas_rpa.id"), nullable=False, index=True)
    
    # Datos enviados
    valor_enviado: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    parametros_extra: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    
    # Estado individual
    estado: Mapped[str] = mapped_column(
        ENUM('Pendiente', 'En_Proceso', 'Exitosa', 'Fallida', name='estado_consulta_enum'),
        nullable=False, default='Pendiente', index=True
    )
    
    # Resultados
    screenshot_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    screenshot_historial_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    datos_capturados: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    escenario: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    mensaje_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Control de reintentos
    intentos_realizados: Mapped[int] = mapped_column(Integer, default=0)
    max_intentos: Mapped[int] = mapped_column(Integer, default=2)
    
    # Métricas de rendimiento
    fecha_inicio: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    fecha_fin: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duracion_segundos: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Relaciones
    proceso: Mapped["DeProceso"] = relationship("DeProceso", back_populates="consultas")
    pagina: Mapped["DePagina"] = relationship("DePagina", back_populates="consultas")

class DeReporte(Base):
    """Modelo para de_reportes_rpa (reportes generados)"""
    __tablename__ = "de_reportes_rpa"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    proceso_id: Mapped[int] = mapped_column(Integer, ForeignKey("de_procesos_rpa.id"), nullable=False, index=True)
    cliente_id: Mapped[int] = mapped_column(Integer, ForeignKey("de_clientes_rpa.id"), nullable=False, index=True)
    
    # Metadatos del proceso
    tipo_alerta: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    monto_usd: Mapped[Optional[float]] = mapped_column(DECIMAL(10, 2), nullable=True)
    fecha_alerta: Mapped[Optional[date]] = mapped_column(DateTime, nullable=True)
    job_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    
    # Archivos
    nombre_archivo: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    ruta_archivo: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    url_descarga: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    tamano_bytes: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    checksum_archivo: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    
    # Metadatos del archivo
    tipo_archivo: Mapped[str] = mapped_column(
        ENUM('PDF', 'DOCX', 'HTML', name='tipo_archivo_enum'),
        nullable=False, default='DOCX', index=True
    )
    generado_exitosamente: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Datos
    data_snapshot: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    
    # Métricas
    tiempo_generacion_segundos: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    fecha_generacion: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now, index=True)

    # Relaciones
    proceso: Mapped["DeProceso"] = relationship("DeProceso", back_populates="reportes")
    cliente: Mapped["DeCliente"] = relationship("DeCliente", back_populates="reportes")