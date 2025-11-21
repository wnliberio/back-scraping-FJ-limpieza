# app/routers/tracking_professional.py - VERSIÓN SIN MANAGER.PY (LIMPIEZA)
from __future__ import annotations
from fastapi.responses import FileResponse
import os
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime

from app.services.tracking_professional import (
    get_paginas_activas,
    get_clientes_with_filters,
    update_cliente_estado,
    crear_proceso_completo
)

router = APIRouter(prefix="/tracking", tags=["tracking"])

# ===== MODELOS DE REQUEST =====

class IniciarProcesoRequest(BaseModel):
    cliente_id: int = Field(..., description="ID del cliente en de_clientes_rpa")
    paginas_codigos: List[str] = Field(..., min_items=1, description="Códigos de páginas a consultar")
    headless: bool = Field(False, description="Ejecutar en modo headless")
    generate_report: bool = Field(True, description="Generar reporte al finalizar")

class ActualizarEstadoClienteRequest(BaseModel):
    estado: str = Field(..., description="Nuevo estado del cliente")
    mensaje_error: Optional[str] = Field(None, description="Mensaje de error opcional")

# ===== ENDPOINTS BÁSICOS =====

@router.get("/health", summary="Health check del sistema")
def health_check() -> Dict[str, Any]:
    """Health check básico para verificar que el sistema de tracking funciona"""
    try:
        # Verificar que podemos acceder a las páginas
        paginas = get_paginas_activas()
        
        return {
            "status": "healthy",
            "message": "Sistema de tracking funcionando correctamente",
            "timestamp": datetime.now().isoformat(),
            "paginas_disponibles": len(paginas),
            "tablas_verificadas": ["de_clientes_rpa", "de_paginas_rpa", "de_procesos_rpa", "de_consultas_rpa"]
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Sistema no saludable: {str(e)}")

@router.get("/paginas", summary="Listar páginas disponibles")
def listar_paginas_disponibles() -> List[Dict[str, Any]]:
    """
    Obtiene todas las páginas disponibles para consulta.
    Se usa para mostrar los checkboxes en el frontend cuando el usuario
    selecciona qué páginas consultar para un cliente.
    """
    try:
        return get_paginas_activas()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo páginas: {str(e)}")

@router.get("/clientes", summary="Listar clientes con filtros")
def listar_clientes_con_filtros(
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    fecha_desde: Optional[str] = Query(None, description="Fecha desde (YYYY-MM-DD)"),
    fecha_hasta: Optional[str] = Query(None, description="Fecha hasta (YYYY-MM-DD)"),
    q: Optional[str] = Query(None, description="Búsqueda en nombre, apellido, CI, RUC")
) -> List[Dict[str, Any]]:
    """
    Obtiene la lista de clientes con filtros opcionales.
    Incluye el proceso activo si el cliente tiene uno en curso.
    Este endpoint reemplaza parcialmente a /api/lista.
    """
    try:
        return get_clientes_with_filters(
            estado=estado,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            q=q
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo clientes: {str(e)}")

@router.put("/clientes/{cliente_id}/estado", summary="Actualizar estado de cliente")
def actualizar_estado_cliente(
    cliente_id: int,
    request: ActualizarEstadoClienteRequest
) -> Dict[str, Any]:
    """
    Actualiza el estado de un cliente específico.
    Se usa cuando el sistema necesita cambiar el estado de un cliente
    (ej: de 'Pendiente' a 'Procesando').
    """
    try:
        success = update_cliente_estado(
            cliente_id=cliente_id,
            estado=request.estado,
            mensaje_error=request.mensaje_error
        )
        
        if not success:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        return {"success": True, "message": f"Estado actualizado a {request.estado}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error actualizando cliente: {str(e)}")

# ===== FUNCIÓN HELPER PARA CONVERTIR A QueryItem =====

def _convertir_a_query_items(cliente_data: Dict[str, Any], paginas_codigos: List[str]) -> List[Dict[str, Any]]:
    """
    Convierte los códigos de páginas en QueryItems compatibles con el executor existente.
    """
    from app.db import SessionLocal
    from app.db.models_new import DeCliente
    
    db = SessionLocal()
    try:
        # Obtener cliente
        cliente = db.query(DeCliente).filter(DeCliente.id == cliente_data['id']).first()
        if not cliente:
            raise ValueError("Cliente no encontrado")
        
        items = []
        for codigo in paginas_codigos:
            # Determinar valor según código de página
            valor = None
            apellidos = None
            nombres = None
            
            if codigo in ['ruc', 'deudas', 'mercado_valores']:
                valor = cliente.ruc
            elif codigo in ['contraloria', 'supercias_persona', 'predio_quito', 'predio_manta']:
                valor = cliente.ci
            elif codigo in ['denuncias', 'google', 'funcion_judicial']:
                valor = f"{cliente.apellido} {cliente.nombre}".strip()
            elif codigo == 'interpol':
                valor = cliente.apellido
                apellidos = cliente.apellido
                nombres = cliente.nombre
            
            if valor:
                item = {
                    "tipo": codigo,
                    "valor": valor
                }
                
                # Agregar campos opcionales para INTERPOL
                if apellidos:
                    item["apellidos"] = apellidos
                if nombres:
                    item["nombres"] = nombres
                
                items.append(item)
        
        return items
    finally:
        db.close()

# ===== FUNCIÓN DE EJECUCIÓN EN BACKGROUND (VERSIÓN SIN MANAGER.PY) =====

async def _ejecutar_proceso_en_background(
    job_id: str, 
    cliente_data: Dict[str, Any],
    paginas_codigos: List[str],
    headless: bool
):
    """
    Ejecuta un proceso DIRECTAMENTE usando el executor.
    ✅ ELIMINADA la dependencia de app.jobs.manager
    """
    try:
        print(f"🚀 Iniciando ejecución directa de proceso {job_id}")
        print(f"📋 Páginas a ejecutar: {paginas_codigos}")
        
        # 1. Construir items para el executor
        from app.models.schemas import QueryItem
        from app.services.executor import run_items
        
        # Obtener cliente de BD
        from app.db import SessionLocal
        from app.db.models_new import DeCliente
        
        db = SessionLocal()
        try:
            cliente = db.query(DeCliente).filter(DeCliente.id == cliente_data['id']).first()
            if not cliente:
                raise ValueError("Cliente no encontrado")
            
            # Construir items
            items = []
            for codigo in paginas_codigos:
                valor = None
                apellidos = None
                nombres = None
                
                if codigo in ['ruc', 'deudas', 'mercado_valores']:
                    valor = cliente.ruc
                elif codigo in ['contraloria', 'supercias_persona', 'predio_quito', 'predio_manta']:
                    valor = cliente.ci
                elif codigo in ['denuncias', 'google', 'funcion_judicial']:
                    valor = f"{cliente.apellido} {cliente.nombre}".strip()
                elif codigo == 'interpol':
                    valor = cliente.apellido
                    apellidos = cliente.apellido
                    nombres = cliente.nombre
                
                if valor:
                    item_dict = {
                        "tipo": codigo,
                        "valor": valor
                    }
                    if apellidos:
                        item_dict["apellidos"] = apellidos
                    if nombres:
                        item_dict["nombres"] = nombres
                    
                    item = QueryItem(**item_dict)
                    items.append(item)
        finally:
            db.close()
        
        # 2. Ejecutar DIRECTAMENTE (sin manager.py)
        print(f"▶️ Ejecutando {len(items)} consultas directamente...")
        resultado = run_items(items=items, headless=headless)
        print(f"✅ Ejecución completada")
        
        # 3. Sincronizar resultado con sistema de tracking
        resultado_completo = {
            "job_id": job_id,
            "status": "done",
            "data": {"results": resultado}
        }
        
        from app.services.sincronizacion_service import sincronizar_job_completado
        await sincronizar_job_completado(job_id, resultado_completo)
        print(f"✅ Resultado sincronizado con tracking")
        
    except Exception as e:
        print(f"💥 Error ejecutando proceso: {e}")
        import traceback
        traceback.print_exc()
        
        # Actualizar estado del cliente a Error
        try:
            from app.services.tracking_professional import get_proceso_by_job_id
            proceso = get_proceso_by_job_id(job_id)
            if proceso:
                from app.services.sincronizacion_service import actualizar_cliente_estado
                await actualizar_cliente_estado(
                    proceso['cliente_id'], 
                    'Error', 
                    str(e)
                )
        except Exception as inner_e:
            print(f"⚠️ Error actualizando estado de cliente: {inner_e}")

# ===== ENDPOINT PRINCIPAL - CREAR Y EJECUTAR PROCESO =====

@router.post("/procesos/crear", summary="Crear y ejecutar nuevo proceso")
def crear_nuevo_proceso(
    request: IniciarProcesoRequest, 
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """
    Crea un nuevo proceso con las páginas seleccionadas Y LO EJECUTA.
    
    Este es el endpoint principal que se llama cuando:
    1. El usuario selecciona un cliente pendiente
    2. Marca los checkboxes de las páginas que quiere consultar
    3. Hace clic en "Agregar a Cola"
    
    ✅ AHORA: Sin usar app.jobs.manager, ejecuta directamente con executor.run_items()
    """
    try:
        import uuid
        
        job_id = str(uuid.uuid4())
        
        # 1. Crear proceso en BD
        proceso_id = crear_proceso_completo(
            cliente_id=request.cliente_id,
            job_id=job_id,
            paginas_codigos=request.paginas_codigos,
            headless=request.headless,
            generate_report=request.generate_report
        )
        
        # 2. Obtener datos del cliente para pasarlos al background
        from app.db import SessionLocal
        from app.db.models_new import DeCliente
        
        db = SessionLocal()
        try:
            cliente = db.query(DeCliente).filter(DeCliente.id == request.cliente_id).first()
            if not cliente:
                raise HTTPException(status_code=404, detail="Cliente no encontrado")
            
            cliente_data = {
                'id': cliente.id,
                'nombre': cliente.nombre,
                'apellido': cliente.apellido,
                'ci': cliente.ci,
                'ruc': cliente.ruc
            }
        finally:
            db.close()
        
        # 3. Lanzar ejecución en background
        background_tasks.add_task(
            _ejecutar_proceso_en_background,
            job_id=job_id,
            cliente_data=cliente_data,
            paginas_codigos=request.paginas_codigos,
            headless=request.headless
        )
        
        return {
            "success": True,
            "proceso_id": proceso_id,
            "job_id": job_id,
            "mensaje": f"Proceso creado con {len(request.paginas_codigos)} páginas",
            "paginas_solicitadas": request.paginas_codigos
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creando proceso: {str(e)}")

# ===== ENDPOINTS DE REPORTES =====

@router.get("/reportes/{proceso_id}/download", summary="Descargar reporte de proceso")
def descargar_reporte_proceso(proceso_id: int) -> FileResponse:
    """
    Descarga el reporte DOCX de un proceso específico.
    Se usa desde el botón "Descargar Reporte" en el modal de detalles.
    """
    try:
        from app.db import SessionLocal
        from app.db.models_new import DeReporte
        
        db = SessionLocal()
        try:
            # Buscar reporte por proceso_id
            reporte = db.query(DeReporte).filter(
                DeReporte.proceso_id == proceso_id,
                DeReporte.generado_exitosamente == True
            ).first()
            
            if not reporte:
                raise HTTPException(
                    status_code=404, 
                    detail="Reporte no encontrado o no generado exitosamente"
                )
            
            # Verificar que el archivo existe físicamente
            if not reporte.ruta_archivo or not os.path.exists(reporte.ruta_archivo):
                raise HTTPException(
                    status_code=404, 
                    detail="Archivo de reporte no encontrado en el sistema"
                )
            
            print(f"📥 Descargando reporte: {reporte.nombre_archivo}")
            print(f"📁 Ruta: {reporte.ruta_archivo}")
            
            # Retornar archivo para descarga
            return FileResponse(
                path=reporte.ruta_archivo,
                filename=reporte.nombre_archivo or f"reporte_proceso_{proceso_id}.docx",
                media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                headers={
                    "Content-Disposition": f"attachment; filename=\"{reporte.nombre_archivo or f'reporte_proceso_{proceso_id}.docx'}\""
                }
            )
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error descargando reporte proceso {proceso_id}: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error interno descargando reporte: {str(e)}"
        )

@router.get("/reportes", summary="Listar todos los reportes disponibles")
def listar_reportes_tracking(
    cliente_id: Optional[int] = Query(None, description="Filtrar por cliente"),
    fecha_desde: Optional[str] = Query(None, description="Fecha desde (YYYY-MM-DD)"),
    fecha_hasta: Optional[str] = Query(None, description="Fecha hasta (YYYY-MM-DD)"),
    solo_exitosos: bool = Query(True, description="Solo reportes generados exitosamente")
) -> List[Dict[str, Any]]:
    """
    Lista todos los reportes generados por el sistema de tracking.
    Se puede usar para crear una página de administración de reportes.
    """
    try:
        from app.db import SessionLocal
        from app.db.models_new import DeReporte, DeCliente
        
        db = SessionLocal()
        try:
            query = db.query(DeReporte)
            
            # Aplicar filtros
            if cliente_id:
                query = query.filter(DeReporte.cliente_id == cliente_id)
            
            if fecha_desde and isinstance(fecha_desde, str) and fecha_desde.strip():
                try:
                    fecha_desde_dt = datetime.strptime(fecha_desde.strip(), "%Y-%m-%d")
                    query = query.filter(DeReporte.fecha_generacion >= fecha_desde_dt)
                except ValueError as e:
                    print(f"⚠️ Fecha desde inválida ignorada: {fecha_desde} - {e}")
            
            if fecha_hasta and isinstance(fecha_hasta, str) and fecha_hasta.strip():
                try:
                    fecha_hasta_dt = datetime.strptime(fecha_hasta.strip(), "%Y-%m-%d")
                    query = query.filter(DeReporte.fecha_generacion <= fecha_hasta_dt)
                except ValueError as e:
                    print(f"⚠️ Fecha hasta inválida ignorada: {fecha_hasta} - {e}")
            
            if solo_exitosos:
                query = query.filter(DeReporte.generado_exitosamente == True)
            
            reportes = query.order_by(DeReporte.fecha_generacion.desc()).all()
            
            # Enriquecer con información del cliente
            resultado = []
            for reporte in reportes:
                cliente = db.query(DeCliente).filter(DeCliente.id == reporte.cliente_id).first()
                
                archivo_existe = (reporte.ruta_archivo and os.path.exists(reporte.ruta_archivo))
                
                resultado.append({
                    'id': reporte.id,
                    'proceso_id': reporte.proceso_id,
                    'job_id': reporte.job_id,
                    'cliente': {
                        'id': cliente.id,
                        'nombre': cliente.nombre,
                        'apellido': cliente.apellido,
                        'ci': cliente.ci,
                        'ruc': cliente.ruc
                    } if cliente else None,
                    'tipo_alerta': reporte.tipo_alerta,
                    'monto_usd': float(reporte.monto_usd) if reporte.monto_usd else None,
                    'fecha_alerta': reporte.fecha_alerta.isoformat() if reporte.fecha_alerta else None,
                    'nombre_archivo': reporte.nombre_archivo,
                    'url_descarga': f"/api/tracking/reportes/{reporte.proceso_id}/download",
                    'tamano_bytes': reporte.tamano_bytes,
                    'tipo_archivo': reporte.tipo_archivo,
                    'generado_exitosamente': reporte.generado_exitosamente,
                    'fecha_generacion': reporte.fecha_generacion.isoformat() if reporte.fecha_generacion else None,
                    'archivo_existe': archivo_existe
                })
            
            return resultado
            
        finally:
            db.close()
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo reportes: {str(e)}"
        )

@router.get("/clientes/{cliente_id}/reportes", summary="Obtener reportes de un cliente")
def obtener_reportes_cliente(cliente_id: int) -> List[Dict[str, Any]]:
    """
    Obtiene todos los reportes de un cliente específico.
    Se usa en el modal de detalles para mostrar historial de reportes.
    """
    from app.db import SessionLocal
    from app.db.models_new import DeReporte, DeProceso
    
    db = SessionLocal()
    try:
        reportes = db.query(DeReporte).filter(
            DeReporte.cliente_id == cliente_id
        ).order_by(DeReporte.fecha_generacion.desc()).all()
        
        resultado = []
        for reporte in reportes:
            proceso = db.query(DeProceso).filter(DeProceso.id == reporte.proceso_id).first()
            
            resultado.append({
                'id': reporte.id,
                'proceso_id': reporte.proceso_id,
                'job_id': reporte.job_id,
                'nombre_archivo': reporte.nombre_archivo,
                'url_descarga': f"/api/tracking/reportes/{reporte.proceso_id}/download",
                'tamano_bytes': reporte.tamano_bytes,
                'tipo_archivo': reporte.tipo_archivo,
                'generado_exitosamente': reporte.generado_exitosamente,
                'fecha_generacion': reporte.fecha_generacion.isoformat() if reporte.fecha_generacion else None,
                'proceso': {
                    'estado': proceso.estado if proceso else None,
                    'fecha_creacion': proceso.fecha_creacion.isoformat() if proceso and proceso.fecha_creacion else None
                } if proceso else None
            })
        
        return resultado
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo reportes: {str(e)}"
        )
    finally:
        db.close()

# ===== ENDPOINTS STUB (PARA IMPLEMENTAR DESPUÉS) =====

@router.get("/procesos/{job_id}", summary="Obtener detalles de proceso")
def obtener_proceso_detalle(job_id: str) -> Dict[str, Any]:
    """Obtiene los detalles completos de un proceso por job_id - EN DESARROLLO"""
    raise HTTPException(status_code=501, detail="Endpoint en desarrollo")

@router.get("/clientes/{cliente_id}/detalles", summary="Obtener detalles completos de cliente")
def obtener_detalles_cliente(cliente_id: int) -> Dict[str, Any]:
    """Obtiene detalles completos de un cliente para el modal 'Detalles' - EN DESARROLLO"""
    raise HTTPException(status_code=501, detail="Endpoint en desarrollo")

@router.get("/estadisticas", summary="Obtener estadísticas del sistema")
def obtener_estadisticas() -> Dict[str, Any]:
    """Obtiene estadísticas generales del sistema para dashboards - EN DESARROLLO"""
    raise HTTPException(status_code=501, detail="Endpoint en desarrollo")