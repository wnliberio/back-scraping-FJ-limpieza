# app/routers/daemon.py
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
    Inicia el daemon que procesa automáticamente clientes pendientes.
    
    El daemon:
    - Busca hasta 5 clientes en estado 'Pendiente'
    - Los procesa uno por uno ejecutando Función Judicial
    - Espera 30 minutos después de cada lote
    - Repite indefinidamente hasta ser detenido
    
    Returns:
        - success: bool - Si se inició correctamente
        - message: str - Mensaje descriptivo
        - estado: str - 'running' o 'stopped'
        - thread_id: int - ID del thread (si se inició)
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
        - success: bool - Si se detuvo correctamente
        - message: str - Mensaje descriptivo
        - estado: str - 'running' o 'stopped'
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
        - running: bool - Si el daemon está ejecutándose
        - thread_alive: bool - Si el thread del daemon está vivo
        - clientes_procesados_en_lote: int - Clientes procesados en el lote actual
        - ultimo_lote_inicio: str - Timestamp del último lote iniciado (ISO format)
    """
    try:
        estado = obtener_estado_daemon()
        return estado
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo estado: {str(e)}"
        )


@router.get("/health", summary="Health check del daemon")
def health_check_daemon() -> Dict[str, Any]:
    """
    Verifica que el sistema de daemon esté operativo.
    """
    try:
        estado = obtener_estado_daemon()
        
        return {
            "status": "healthy",
            "daemon_available": True,
            "daemon_running": estado.get("running", False),
            "message": "Sistema de daemon operativo"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "daemon_available": False,
            "error": str(e),
            "message": "Error en sistema de daemon"
        }