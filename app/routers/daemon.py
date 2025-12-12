# app/routers/daemon.py - VERSIÓN LIMPIA SIN DEPENDENCIAS INNECESARIAS
"""
Endpoints para controlar el daemon procesador automático.
Permite iniciar, detener y consultar el estado del daemon.
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any

from app.services.daemon_procesador import (
    iniciar_daemon,
    detener_daemon,
    obtener_estado_daemon
)

router = APIRouter(prefix="/daemon", tags=["daemon"])


@router.post("/iniciar", summary="Iniciar daemon procesador")
def endpoint_iniciar_daemon() -> Dict[str, Any]:
    """
    Inicia el daemon que procesa automáticamente clientes pendientes de de_clientes_rpa_v2.
    
    El daemon:
    - Busca 1 cliente con ESTADO_CONSULTA='Pendiente'
    - Lo procesa ejecutando consulta en Función Judicial
    - Cambia ESTADO_CONSULTA a 'Procesado' o 'Error'
    - Espera 30 minutos
    - Repite indefinidamente hasta ser detenido
    
    Returns:
        {
            "success": bool - Si se inició correctamente,
            "message": str - Mensaje descriptivo,
            "estado": str - 'running',
            "thread_id": int - ID del thread
        }
    """
    try:
        resultado = iniciar_daemon()
        return resultado
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando daemon: {str(e)}"
        )


@router.post("/detener", summary="Detener daemon procesador")
def endpoint_detener_daemon() -> Dict[str, Any]:
    """
    Detiene el daemon procesador de forma controlada.
    
    El daemon terminará el cliente que esté procesando actualmente
    y luego se detendrá sin iniciar nuevos procesamientos.
    
    Returns:
        {
            "success": bool - Si se detuvo correctamente,
            "message": str - Mensaje descriptivo,
            "estado": str - 'stopped'
        }
    """
    try:
        resultado = detener_daemon()
        return resultado
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error deteniendo daemon: {str(e)}"
        )


@router.get("/estado", summary="Obtener estado del daemon")
def endpoint_estado_daemon() -> Dict[str, Any]:
    """
    Consulta el estado actual del daemon procesador.
    
    Returns:
        {
            "running": bool - Si el daemon está ejecutándose,
            "thread_alive": bool - Si el thread del daemon está vivo,
            "cliente_actual": str - Nombre del cliente en proceso (opcional),
            "ultimo_inicio": str - Timestamp del último inicio (ISO format)
        }
    """
    try:
        estado = obtener_estado_daemon()
        return estado
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo estado: {str(e)}"
        )