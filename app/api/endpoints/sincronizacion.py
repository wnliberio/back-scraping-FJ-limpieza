# app/api/endpoints/sincronizacion.py
"""
Endpoints REST para controlar la sincronización DB2 → SQLServer
"""

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime, date
from typing import Dict, Any

from app.services.sincronizacion_db2_v2 import (
    sincronizar_ahora,
    obtener_logs_ultimas_sincronizaciones
)
from app.services.scheduler_sincronizacion import (
    ejecutar_sincronizacion_manual,
    obtener_estado_scheduler
)

router = APIRouter(prefix="/api/sync", tags=["Sincronización"])


@router.post("/iniciar")
def iniciar_sincronizacion_manual(
    fecha_desde: str = Query(..., description="Fecha inicio (YYYY-MM-DD)"),
    fecha_hasta: str = Query(..., description="Fecha fin (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """
    Inicia sincronización manualmente
    
    **Ejemplo:**
    ```
    POST /api/sync/iniciar?fecha_desde=2025-09-29&fecha_hasta=2025-09-30
    ```
    
    **Respuesta:**
    ```json
    {
        "exito": true,
        "numero_sincronizacion": 10,
        "registros_traidos": 100,
        "registros_insertados": 95,
        "registros_duplicados": 5,
        "registros_error": 0,
        "estado": "SUCCESS",
        "duracion_segundos": 5,
        "mensaje": "95 nuevos, 5 duplicados"
    }
    ```
    """
    
    try:
        # Validar formato de fechas
        try:
            datetime.strptime(fecha_desde, "%Y-%m-%d")
            datetime.strptime(fecha_hasta, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato de fecha inválido. Usa YYYY-MM-DD"
            )
        
        # Ejecutar sincronización
        exito, resultado = ejecutar_sincronizacion_manual(fecha_desde, fecha_hasta)
        
        return {
            "exito": exito,
            "numero_sincronizacion": resultado.get("numero_sincronizacion"),
            "registros_traidos": resultado.get("registros_traidos"),
            "registros_insertados": resultado.get("registros_insertados"),
            "registros_duplicados": resultado.get("registros_duplicados"),
            "registros_error": resultado.get("registros_error"),
            "estado": resultado.get("estado"),
            "duracion_segundos": resultado.get("duracion_segundos"),
            "mensaje": resultado.get("mensaje"),
            "fecha_minima": resultado.get("fecha_minima_db2"),
            "fecha_maxima": resultado.get("fecha_maxima_db2")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error en sincronización: {str(e)}"
        )


@router.get("/estado")
def obtener_estado_sincronizacion() -> Dict[str, Any]:
    """
    Obtiene estado actual del scheduler automático
    
    **Respuesta:**
    ```json
    {
        "scheduler_activo": true,
        "proxima_ejecucion": "2025-12-09T08:00:00",
        "jobs": [
            {
                "id": "sincronizacion_diaria",
                "nombre": "Sincronización diaria DB2→SQLServer",
                "trigger": "cron[hour='8', minute='0', timezone='America/Guayaquil']",
                "proxima_ejecucion": "2025-12-09T08:00:00"
            }
        ]
    }
    ```
    """
    try:
        estado = obtener_estado_scheduler()
        
        proxima_ejecucion = None
        if estado["jobs"]:
            proxima_ejecucion = estado["jobs"][0].get("next_run_time")
        
        return {
            "scheduler_activo": estado["running"],
            "proxima_ejecucion": proxima_ejecucion,
            "jobs": [
                {
                    "id": job["id"],
                    "nombre": job["name"],
                    "trigger": job["trigger"],
                    "proxima_ejecucion": job["next_run_time"]
                }
                for job in estado["jobs"]
            ]
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo estado: {str(e)}"
        )


@router.get("/auditoria")
def obtener_auditoria(
    cantidad: int = Query(10, ge=1, le=50, description="Número de registros")
) -> Dict[str, Any]:
    """
    Obtiene últimas sincronizaciones registradas en auditoría
    
    **Parámetros:**
    - `cantidad`: 1-50 registros (default: 10)
    
    **Respuesta:**
    ```json
    {
        "total": 10,
        "sincronizaciones": [
            {
                "numero": 9,
                "fecha_inicio": "2025-12-08T11:46:32.598",
                "fecha_fin": "2025-12-08T11:46:34.169",
                "duracion_segundos": 1,
                "registros_traidos": 1,
                "registros_insertados": 0,
                "registros_duplicados": 1,
                "registros_error": 0,
                "estado": "SUCCESS",
                "mensaje": "0 nuevos, 1 duplicados"
            }
        ]
    }
    ```
    """
    try:
        logs = obtener_logs_ultimas_sincronizaciones(cantidad)
        
        return {
            "total": len(logs),
            "sincronizaciones": [
                {
                    "numero": log.get("numero_sincronizacion"),
                    "fecha_inicio": log.get("fecha_hora_inicio"),
                    "fecha_fin": log.get("fecha_hora_fin"),
                    "duracion_segundos": log.get("duracion_segundos"),
                    "registros_traidos": log.get("registros_traidos"),
                    "registros_insertados": log.get("registros_insertados"),
                    "registros_duplicados": log.get("registros_duplicados"),
                    "registros_error": log.get("registros_error"),
                    "estado": log.get("estado"),
                    "mensaje": log.get("mensaje_resultado")
                }
                for log in logs
            ]
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo auditoría: {str(e)}"
        )