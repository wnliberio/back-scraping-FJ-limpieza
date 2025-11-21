# app/models/schemas.py - VERSIÓN SIMPLIFICADA (SOLO FUNCIÓN JUDICIAL)
from typing import List, Literal, Optional, Dict, Any
from pydantic import BaseModel, Field, constr

# -------- Modelos para el informe --------
class InformeMeta(BaseModel):
    tipo_alerta: Optional[str] = Field(None, description="Tipo de alerta (ej. Venta vehículo, Venta casa)")
    monto_usd: Optional[float] = Field(None, description="Monto en USD asociado a la alerta")
    fecha_alerta: Optional[str] = Field(None, description="Fecha ISO-8601 (YYYY-MM-DD) de la alerta")

# -------- Solo Función Judicial disponible --------
TipoItem = Literal["funcion_judicial"]

class QueryItem(BaseModel):
    """
    Modelo para items de consulta.
    Solo soporta consultas de Función Judicial.
    """
    tipo: TipoItem = Field(
        default="funcion_judicial",
        description="Tipo de consulta (solo funcion_judicial disponible)"
    )
    
    valor: constr(strip_whitespace=True, min_length=3, max_length=120) = Field(
        ...,
        description="Apellidos y nombres completos para consultar"
    )

class ConsultasBody(BaseModel):
    """
    Modelo principal para iniciar un proceso de consultas.
    """
    items: List[QueryItem] = Field(
        ..., 
        min_length=1, 
        max_length=50,
        description="Lista de consultas a ejecutar (máximo 50)"
    )
    
    # Configuración de ejecución
    headless: bool = Field(
        default=True, 
        description="Ejecutar en modo headless (sin interfaz gráfica). False útil para debugging."
    )
    
    # Metadatos del informe/proceso
    meta: Optional[InformeMeta] = Field(
        None, 
        description="Metadatos opcionales para el informe final"
    )

class JobStatusResponse(BaseModel):
    """
    Respuesta del estado de un job/proceso.
    """
    job_id: str
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None