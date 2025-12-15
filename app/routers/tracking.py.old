# app/routers/tracking.py
"""
Router para sistema de tracking con sincronización automática
Aplica principios SOLID y mantiene estados actualizados
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, date
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.db import get_db
from app.db.models_new import (
    DeCliente, DeProceso, DeConsulta, DePagina, DeReporte
)
from app.services.sincronizacion_service import (
    sincronizar_job_completado,
    actualizar_cliente_estado,
    obtener_gestor_paginas,
    GestorRutasReportes
)

router = APIRouter(prefix="/tracking", tags=["tracking"])

# =============== SCHEMAS PARA REQUESTS/RESPONSES ===============

class PaginaResponse(BaseModel):
    id: int
    nombre: str
    codigo: str
    url: Optional[str] = None
    descripcion: Optional[str] = None
    activa: bool
    
    class Config:
        from_attributes = True

class ClienteTrackingResponse(BaseModel):
    id: int
    nombre: Optional[str] = None
    apellido: Optional[str] = None
    ci: Optional[str] = None
    ruc: Optional[str] = None
    tipo: Optional[str] = None
    monto: Optional[float] = None
    fecha: Optional[date] = None
    estado: str
    fecha_creacion: datetime
    
    # Información del proceso activo (si existe)
    proceso_activo: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True

class CrearProcesoRequest(BaseModel):
    cliente_id: int = Field(..., description="ID del cliente")
    paginas_codigos: List[str] = Field(..., description="Lista de códigos de páginas a consultar")
    headless: bool = Field(False, description="Ejecutar en modo headless")
    generate_report: bool = Field(True, description="Generar reporte al finalizar")

class ActualizarEstadoRequest(BaseModel):
    estado: str = Field(..., description="Nuevo estado del cliente")
    mensaje_error: Optional[str] = Field(None, description="Mensaje de error si aplica")

class SincronizarJobRequest(BaseModel):
    job_id: str = Field(..., description="ID del job completado")
    resultado: Dict[str, Any] = Field(..., description="Resultado completo del job")

# =============== ENDPOINTS DE PÁGINAS ===============

@router.get("/paginas", response_model=List[PaginaResponse])
async def obtener_paginas_disponibles(
    activas_solo: bool = Query(True, description="Solo páginas activas"),
    db: Session = Depends(get_db)
):
    """Obtiene todas las páginas disponibles para consulta"""
    query = db.query(DePagina)
    
    if activas_solo:
        query = query.filter(DePagina.activa == True)
    
    paginas = query.order_by(DePagina.orden_display, DePagina.nombre).all()
    
    return [
        PaginaResponse(
            id=pagina.id,
            nombre=pagina.nombre,
            codigo=pagina.codigo,
            url=pagina.url,
            descripcion=pagina.descripcion,
            activa=pagina.activa
        )
        for pagina in paginas
    ]

@router.get("/paginas/{codigo}")
async def obtener_pagina_por_codigo(
    codigo: str,
    db: Session = Depends(get_db)
):
    """Obtiene información específica de una página por código"""
    gestor_paginas = obtener_gestor_paginas(db)
    pagina = gestor_paginas.obtener_pagina_por_codigo(codigo)
    
    if not pagina:
        raise HTTPException(status_code=404, detail=f"Página con código '{codigo}' no encontrada")
    
    return pagina

# =============== ENDPOINTS DE CLIENTES ===============

@router.get("/clientes", response_model=List[ClienteTrackingResponse])
async def obtener_clientes_con_tracking(
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    fecha_desde: Optional[date] = Query(None, description="Fecha desde (YYYY-MM-DD)"),
    fecha_hasta: Optional[date] = Query(None, description="Fecha hasta (YYYY-MM-DD)"),
    q: Optional[str] = Query(None, description="Búsqueda por nombre, apellido, CI o RUC"),
    limit: Optional[int] = Query(None, description="Límite de resultados"),
    db: Session = Depends(get_db)
):
    """Obtiene clientes con información de tracking completa"""
    
    query = db.query(DeCliente)
    
    # Aplicar filtros
    if estado and estado != "Todos":
        query = query.filter(DeCliente.estado == estado)
    
    if fecha_desde:
        query = query.filter(DeCliente.fecha_creacion >= fecha_desde)
    
    if fecha_hasta:
        query = query.filter(DeCliente.fecha_creacion <= fecha_hasta)
    
    if q and q.strip():
        search_term = f"%{q.strip()}%"
        query = query.filter(
            DeCliente.nombre.ilike(search_term) |
            DeCliente.apellido.ilike(search_term) |
            DeCliente.ci.ilike(search_term) |
            DeCliente.ruc.ilike(search_term)
        )
    
    query = query.order_by(DeCliente.fecha_creacion.desc())
    
    if limit:
        query = query.limit(limit)
    
    clientes = query.all()
    
    # Enriquecer con información de procesos activos
    resultado = []
    for cliente in clientes:
        # Buscar proceso más reciente del cliente
        proceso_activo = db.query(DeProceso).filter(
            DeProceso.cliente_id == cliente.id
        ).order_by(DeProceso.fecha_creacion.desc()).first()
        
        proceso_info = None
        if proceso_activo:
            proceso_info = {
                'proceso_id': proceso_activo.id,
                'job_id': proceso_activo.job_id,
                'estado': proceso_activo.estado,
                'fecha_inicio': proceso_activo.fecha_inicio,
                'fecha_fin': proceso_activo.fecha_fin,
                'total_paginas_solicitadas': proceso_activo.total_paginas_solicitadas,
                'total_paginas_exitosas': proceso_activo.total_paginas_exitosas,
                'total_paginas_fallidas': proceso_activo.total_paginas_fallidas
            }
        
        resultado.append(ClienteTrackingResponse(
            id=cliente.id,
            nombre=cliente.nombre,
            apellido=cliente.apellido,
            ci=cliente.ci,
            ruc=cliente.ruc,
            tipo=cliente.tipo,
            monto=cliente.monto,
            fecha=cliente.fecha,
            estado=cliente.estado,
            fecha_creacion=cliente.fecha_creacion,
            proceso_activo=proceso_info
        ))
    
    return resultado

@router.put("/clientes/{cliente_id}/estado")
async def actualizar_estado_cliente(
    cliente_id: int,
    request: ActualizarEstadoRequest,
    db: Session = Depends(get_db)
):
    """Actualiza el estado de un cliente específico"""
    
    cliente = db.query(DeCliente).filter(DeCliente.id == cliente_id).first()
    if not cliente:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    
    cliente.estado = request.estado
    
    # Si hay mensaje de error, buscar el proceso más reciente y actualizarlo
    if request.mensaje_error:
        proceso_reciente = db.query(DeProceso).filter(
            DeProceso.cliente_id == cliente_id
        ).order_by(DeProceso.fecha_creacion.desc()).first()
        
        if proceso_reciente:
            proceso_reciente.mensaje_error_general = request.mensaje_error
    
    try:
        db.commit()
        return {"success": True, "mensaje": f"Estado actualizado a '{request.estado}'"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error actualizando estado: {str(e)}")

# =============== ENDPOINTS DE PROCESOS ===============

@router.post("/procesos/crear")
async def crear_proceso_con_paginas(
    request: CrearProcesoRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Crea un nuevo proceso de tracking con páginas específicas"""
    
    # Verificar que el cliente existe
    cliente = db.query(DeCliente).filter(DeCliente.id == request.cliente_id).first()
    if not cliente:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    
    # Verificar que las páginas existen y están activas
    gestor_paginas = obtener_gestor_paginas(db)
    paginas_validas = []
    
    for codigo in request.paginas_codigos:
        pagina_info = gestor_paginas.obtener_pagina_por_codigo(codigo)
        if not pagina_info:
            raise HTTPException(
                status_code=400, 
                detail=f"Página con código '{codigo}' no encontrada o inactiva"
            )
        paginas_validas.append(pagina_info)
    
    try:
        # Crear proceso
        nuevo_proceso = DeProceso(
            cliente_id=request.cliente_id,
            tipo_alerta=cliente.tipo,
            monto_usd=cliente.monto,
            fecha_alerta=cliente.fecha,
            estado='Pendiente',
            headless=request.headless,
            generate_report=request.generate_report,
            total_paginas_solicitadas=len(request.paginas_codigos),
            fecha_creacion=datetime.now()
        )
        
        db.add(nuevo_proceso)
        db.flush()  # Para obtener el ID
        
        # Crear consultas individuales para cada página
        for pagina_info in paginas_validas:
            pagina_db = db.query(DePagina).filter(DePagina.codigo == pagina_info['codigo']).first()
            
            nueva_consulta = DeConsulta(
                proceso_id=nuevo_proceso.id,
                pagina_id=pagina_db.id,
                valor_enviado=_obtener_valor_para_pagina(cliente, pagina_info['codigo']),
                estado='Pendiente'
            )
            db.add(nueva_consulta)
        
        # Generar job_id único
        import uuid
        job_id = str(uuid.uuid4())
        nuevo_proceso.job_id = job_id
        
        # Actualizar estado del cliente
        cliente.estado = 'Procesando'
        
        db.commit()
        
        # TODO: Aquí se debería enviar el job real al sistema de consultas
        # Por ahora simular inicio del proceso
        background_tasks.add_task(_simular_inicio_proceso, job_id)
        
        return {
            "success": True,
            "proceso_id": nuevo_proceso.id,
            "job_id": job_id,
            "mensaje": f"Proceso creado con {len(request.paginas_codigos)} páginas",
            "paginas_solicitadas": request.paginas_codigos
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error creando proceso: {str(e)}")

def _obtener_valor_para_pagina(cliente: DeCliente, codigo_pagina: str) -> Optional[str]:
    """Obtiene el valor apropiado para enviar a cada página"""
    
    # Mapeo de páginas a campos del cliente
    mapeo_valores = {
        'ruc': cliente.ruc,
        'deudas': cliente.ruc,
        'mercado_valores': cliente.ruc,
        'denuncias': f"{cliente.nombre} {cliente.apellido}".strip(),
        'interpol': cliente.apellido,
        'google': f"{cliente.nombre} {cliente.apellido}".strip(),
        'contraloria': cliente.ci,
        'supercias_persona': cliente.ci,
        'predio_quito': cliente.ci,
        'predio_manta': cliente.ci
    }
    
    return mapeo_valores.get(codigo_pagina)

async def _simular_inicio_proceso(job_id: str):
    """Simula el inicio de un proceso (reemplazar con llamada real al sistema de consultas)"""
    import asyncio
    import random
    
    # Simular tiempo de procesamiento
    await asyncio.sleep(random.randint(5, 15))
    
    # Simular resultado exitoso
    resultado_simulado = {
        'job_id': job_id,
        'status': 'done',
        'data': {
            'google': {
                'escenario': 'datos_encontrados',
                'resultado': 'Información encontrada para el cliente',
                'timestamp': datetime.now().isoformat()
            }
        }
    }
    
    # Sincronizar resultado
    await sincronizar_job_completado(job_id, resultado_simulado)

# =============== ENDPOINTS DE SINCRONIZACIÓN ===============

@router.post("/procesos/sincronizar")
async def sincronizar_job_completado_endpoint(
    request: SincronizarJobRequest,
    db: Session = Depends(get_db)
):
    """Endpoint para sincronizar cuando un job se completa"""
    
    try:
        exito = await sincronizar_job_completado(request.job_id, request.resultado)
        
        if exito:
            return {
                "success": True,
                "mensaje": f"Job {request.job_id} sincronizado exitosamente"
            }
        else:
            raise HTTPException(
                status_code=404, 
                detail=f"No se pudo sincronizar job {request.job_id}"
            )
    
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error sincronizando job: {str(e)}"
        )

@router.get("/procesos/{proceso_id}")
async def obtener_detalle_proceso(
    proceso_id: int,
    db: Session = Depends(get_db)
):
    """Obtiene detalles completos de un proceso específico"""
    
    proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
    if not proceso:
        raise HTTPException(status_code=404, detail="Proceso no encontrado")
    
    # Obtener cliente
    cliente = db.query(DeCliente).filter(DeCliente.id == proceso.cliente_id).first()
    
    # Obtener consultas del proceso
    consultas = db.query(DeConsulta).filter(
        DeConsulta.proceso_id == proceso_id
    ).all()
    
    consultas_detalle = []
    for consulta in consultas:
        pagina = db.query(DePagina).filter(DePagina.id == consulta.pagina_id).first()
        consultas_detalle.append({
            'id': consulta.id,
            'pagina_nombre': pagina.nombre if pagina else 'Página desconocida',
            'pagina_codigo': pagina.codigo if pagina else 'N/A',
            'estado': consulta.estado,
            'valor_enviado': consulta.valor_enviado,
            'fecha_inicio': consulta.fecha_inicio,
            'fecha_fin': consulta.fecha_fin,
            'duracion_segundos': consulta.duracion_segundos,
            'mensaje_error': consulta.mensaje_error,
            'datos_capturados': consulta.datos_capturados,
            'intentos_realizados': consulta.intentos_realizados
        })
    
    # Obtener reporte si existe
    reporte = db.query(DeReporte).filter(DeReporte.proceso_id == proceso_id).first()
    reporte_info = None
    if reporte:
        gestor_rutas = GestorRutasReportes()
        reporte_info = {
            'id': reporte.id,
            'nombre_archivo': reporte.nombre_archivo,
            'url_descarga': reporte.url_descarga,
            'generado_exitosamente': reporte.generado_exitosamente,
            'tamano_bytes': reporte.tamano_bytes,
            'fecha_generacion': reporte.fecha_generacion,
            'archivo_existe': gestor_rutas.verificar_reporte_existe(reporte.ruta_archivo)
        }
    
    return {
        'proceso': {
            'id': proceso.id,
            'job_id': proceso.job_id,
            'estado': proceso.estado,
            'fecha_creacion': proceso.fecha_creacion,
            'fecha_inicio': proceso.fecha_inicio,
            'fecha_fin': proceso.fecha_fin,
            'total_paginas_solicitadas': proceso.total_paginas_solicitadas,
            'total_paginas_exitosas': proceso.total_paginas_exitosas,
            'total_paginas_fallidas': proceso.total_paginas_fallidas,
            'mensaje_error_general': proceso.mensaje_error_general
        },
        'cliente': {
            'id': cliente.id,
            'nombre': cliente.nombre,
            'apellido': cliente.apellido,
            'ci': cliente.ci,
            'ruc': cliente.ruc,
            'tipo': cliente.tipo,
            'monto': cliente.monto
        } if cliente else None,
        'consultas': consultas_detalle,
        'reporte': reporte_info
    }

# =============== ENDPOINTS DE REPORTES ===============

@router.get("/reportes/{proceso_id}/download")
async def descargar_reporte_proceso(
    proceso_id: int,
    db: Session = Depends(get_db)
):
    """Descarga el reporte de un proceso específico"""
    from fastapi.responses import FileResponse
    
    reporte = db.query(DeReporte).filter(
        DeReporte.proceso_id == proceso_id,
        DeReporte.generado_exitosamente == True
    ).first()
    
    if not reporte:
        raise HTTPException(
            status_code=404, 
            detail="Reporte no encontrado o no generado exitosamente"
        )
    
    gestor_rutas = GestorRutasReportes()
    if not gestor_rutas.verificar_reporte_existe(reporte.ruta_archivo):
        raise HTTPException(
            status_code=404, 
            detail="Archivo de reporte no encontrado en el sistema"
        )
    
    return FileResponse(
        path=reporte.ruta_archivo,
        filename=reporte.nombre_archivo,
        media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    )

@router.get("/reportes")
async def listar_reportes_tracking(
    cliente_id: Optional[int] = Query(None, description="Filtrar por cliente"),
    fecha_desde: Optional[date] = Query(None, description="Fecha desde"),
    fecha_hasta: Optional[date] = Query(None, description="Fecha hasta"),
    solo_exitosos: bool = Query(True, description="Solo reportes generados exitosamente"),
    db: Session = Depends(get_db)
):
    """Lista reportes del sistema de tracking"""
    
    query = db.query(DeReporte)
    
    if cliente_id:
        query = query.filter(DeReporte.cliente_id == cliente_id)
    
    if fecha_desde:
        query = query.filter(DeReporte.fecha_generacion >= fecha_desde)
    
    if fecha_hasta:
        query = query.filter(DeReporte.fecha_generacion <= fecha_hasta)
    
    if solo_exitosos:
        query = query.filter(DeReporte.generado_exitosamente == True)
    
    reportes = query.order_by(DeReporte.fecha_generacion.desc()).all()
    
    resultado = []
    gestor_rutas = GestorRutasReportes()
    
    for reporte in reportes:
        # Obtener información del cliente
        cliente = db.query(DeCliente).filter(DeCliente.id == reporte.cliente_id).first()
        
        resultado.append({
            'id': reporte.id,
            'proceso_id': reporte.proceso_id,
            'job_id': reporte.job_id,
            'cliente': {
                'id': cliente.id,
                'nombre': cliente.nombre,
                'apellido': cliente.apellido
            } if cliente else None,
            'tipo_alerta': reporte.tipo_alerta,
            'monto_usd': reporte.monto_usd,
            'nombre_archivo': reporte.nombre_archivo,
            'url_descarga': reporte.url_descarga,
            'tamano_bytes': reporte.tamano_bytes,
            'generado_exitosamente': reporte.generado_exitosamente,
            'fecha_generacion': reporte.fecha_generacion,
            'archivo_existe': gestor_rutas.verificar_reporte_existe(reporte.ruta_archivo)
        })
    
    return resultado

# =============== ENDPOINTS DE ESTADÍSTICAS ===============

@router.get("/estadisticas")
async def obtener_estadisticas_tracking(
    fecha_desde: Optional[date] = Query(None, description="Fecha desde"),
    fecha_hasta: Optional[date] = Query(None, description="Fecha hasta"),
    db: Session = Depends(get_db)
):
    """Obtiene estadísticas del sistema de tracking"""
    
    # Filtros base
    filtro_fecha_clientes = True
    filtro_fecha_procesos = True
    
    if fecha_desde:
        filtro_fecha_clientes = DeCliente.fecha_creacion >= fecha_desde
        filtro_fecha_procesos = DeProceso.fecha_creacion >= fecha_desde
    
    if fecha_hasta:
        if fecha_desde:
            filtro_fecha_clientes = and_(
                DeCliente.fecha_creacion >= fecha_desde,
                DeCliente.fecha_creacion <= fecha_hasta
            )
            filtro_fecha_procesos = and_(
                DeProceso.fecha_creacion >= fecha_desde,
                DeProceso.fecha_creacion <= fecha_hasta
            )
        else:
            filtro_fecha_clientes = DeCliente.fecha_creacion <= fecha_hasta
            filtro_fecha_procesos = DeProceso.fecha_creacion <= fecha_hasta
    
    # Estadísticas de clientes
    total_clientes = db.query(DeCliente).filter(filtro_fecha_clientes).count()
    clientes_pendientes = db.query(DeCliente).filter(
        and_(filtro_fecha_clientes, DeCliente.estado == 'Pendiente')
    ).count()
    clientes_procesando = db.query(DeCliente).filter(
        and_(filtro_fecha_clientes, DeCliente.estado == 'Procesando')
    ).count()
    clientes_procesados = db.query(DeCliente).filter(
        and_(filtro_fecha_clientes, DeCliente.estado == 'Procesado')
    ).count()
    clientes_error = db.query(DeCliente).filter(
        and_(filtro_fecha_clientes, DeCliente.estado == 'Error')
    ).count()
    
    # Estadísticas de procesos
    total_procesos = db.query(DeProceso).filter(filtro_fecha_procesos).count()
    procesos_completados = db.query(DeProceso).filter(
        and_(filtro_fecha_procesos, DeProceso.estado == 'Completado')
    ).count()
    procesos_con_errores = db.query(DeProceso).filter(
        and_(filtro_fecha_procesos, DeProceso.estado == 'Completado_Con_Errores')
    ).count()
    procesos_fallidos = db.query(DeProceso).filter(
        and_(filtro_fecha_procesos, DeProceso.estado == 'Error_Total')
    ).count()
    
    # Estadísticas de páginas más consultadas
    from sqlalchemy import func
    paginas_populares = db.query(
        DePagina.nombre,
        DePagina.codigo,
        func.count(DeConsulta.id).label('total_consultas')
    ).join(
        DeConsulta, DePagina.id == DeConsulta.pagina_id
    ).group_by(
        DePagina.id, DePagina.nombre, DePagina.codigo
    ).order_by(
        func.count(DeConsulta.id).desc()
    ).limit(10).all()
    
    return {
        'clientes': {
            'total': total_clientes,
            'pendientes': clientes_pendientes,
            'procesando': clientes_procesando,
            'procesados': clientes_procesados,
            'errores': clientes_error
        },
        'procesos': {
            'total': total_procesos,
            'completados': procesos_completados,
            'con_errores': procesos_con_errores,
            'fallidos': procesos_fallidos
        },
        'paginas_populares': [
            {
                'nombre': p.nombre,
                'codigo': p.codigo,
                'total_consultas': p.total_consultas
            }
            for p in paginas_populares
        ]
    }