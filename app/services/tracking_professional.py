# app/services/tracking_professional.py
"""
Servicio profesional de tracking que usa la infraestructura existente.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc

from app.db import SessionLocal
from app.db.models_new import (
    DeCliente, DeProceso, DeConsulta, DePagina, DeReporte
)

def get_db_session() -> Session:
    """Obtiene una sesión de base de datos"""
    return SessionLocal()

def get_paginas_activas() -> List[Dict[str, Any]]:
    """
    Obtiene todas las páginas activas disponibles para consulta.
    Se usa para mostrar los checkboxes en el frontend.
    """
    db = get_db_session()
    try:
        paginas = db.query(DePagina).filter(
            DePagina.activa == True
        ).order_by(DePagina.orden_display, DePagina.nombre).all()
        
        return [
            {
                "id": p.id,
                "nombre": p.nombre,
                "codigo": p.codigo,
                "url": p.url,
                "descripcion": p.descripcion,
                "activa": p.activa,
                "orden_display": p.orden_display
            }
            for p in paginas
        ]
    finally:
        db.close()

def get_clientes_with_filters(
    estado: Optional[str] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    q: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Obtiene clientes con filtros opcionales.
    Incluye información del proceso activo si existe.
    """
    db = get_db_session()
    try:
        query = db.query(DeCliente)
        
        # Aplicar filtros
        if estado and estado != "Todos":
            query = query.filter(DeCliente.estado == estado)
        
        if fecha_desde:
            try:
                fecha_desde_dt = datetime.strptime(fecha_desde, "%Y-%m-%d").date()
                query = query.filter(DeCliente.fecha_creacion >= fecha_desde_dt)
            except ValueError:
                pass  # Ignorar fecha inválida
        
        if fecha_hasta:
            try:
                fecha_hasta_dt = datetime.strptime(fecha_hasta, "%Y-%m-%d").date()
                query = query.filter(DeCliente.fecha_creacion <= fecha_hasta_dt)
            except ValueError:
                pass  # Ignorar fecha inválida
        
        if q and q.strip():
            search_term = f"%{q.strip()}%"
            query = query.filter(
                or_(
                    DeCliente.nombre.ilike(search_term),
                    DeCliente.apellido.ilike(search_term),
                    DeCliente.ci.ilike(search_term),
                    DeCliente.ruc.ilike(search_term)
                )
            )
        
        query = query.order_by(desc(DeCliente.fecha_creacion))
        clientes = query.all()
        
        # Enriquecer con información de procesos activos
        resultado = []
        for cliente in clientes:
            # Buscar proceso más reciente
            proceso_activo = db.query(DeProceso).filter(
                DeProceso.cliente_id == cliente.id
            ).order_by(desc(DeProceso.fecha_creacion)).first()
            
            proceso_info = None
            if proceso_activo:
                proceso_info = {
                    "proceso_id": proceso_activo.id,
                    "job_id": proceso_activo.job_id,
                    "estado": proceso_activo.estado,
                    "fecha_inicio": proceso_activo.fecha_inicio.isoformat() if proceso_activo.fecha_inicio else None,
                    "fecha_fin": proceso_activo.fecha_fin.isoformat() if proceso_activo.fecha_fin else None,
                    "total_paginas_solicitadas": proceso_activo.total_paginas_solicitadas,
                    "total_paginas_exitosas": proceso_activo.total_paginas_exitosas,
                    "total_paginas_fallidas": proceso_activo.total_paginas_fallidas
                }
            
            resultado.append({
                "id": cliente.id,
                "nombre": cliente.nombre,
                "apellido": cliente.apellido,
                "ci": cliente.ci,
                "ruc": cliente.ruc,
                "tipo": cliente.tipo,
                "monto": float(cliente.monto) if cliente.monto else None,
                "fecha": cliente.fecha.isoformat() if cliente.fecha else None,
                "estado": cliente.estado,
                "fecha_creacion": cliente.fecha_creacion.isoformat(),
                "proceso_activo": proceso_info
            })
        
        return resultado
    finally:
        db.close()

def update_cliente_estado(cliente_id: int, estado: str, mensaje_error: Optional[str] = None) -> bool:
    """
    Actualiza el estado de un cliente específico.
    Retorna True si fue exitoso, False si no se encontró el cliente.
    """
    db = get_db_session()
    try:
        cliente = db.query(DeCliente).filter(DeCliente.id == cliente_id).first()
        if not cliente:
            return False
        
        cliente.estado = estado
        
        # Si hay mensaje de error, buscar el proceso más reciente y actualizarlo
        if mensaje_error:
            proceso_reciente = db.query(DeProceso).filter(
                DeProceso.cliente_id == cliente_id
            ).order_by(desc(DeProceso.fecha_creacion)).first()
            
            if proceso_reciente:
                proceso_reciente.mensaje_error_general = mensaje_error
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"❌ Error actualizando cliente {cliente_id}: {str(e)}")
        return False
    finally:
        db.close()

def validar_datos_cliente_para_paginas(cliente_id: int, paginas_codigos: List[str]) -> List[str]:
    """
    Valida que un cliente tenga los datos necesarios para las páginas seleccionadas.
    Retorna lista de errores (vacía si todo está bien).
    """
    db = get_db_session()
    try:
        # Obtener cliente
        cliente = db.query(DeCliente).filter(DeCliente.id == cliente_id).first()
        if not cliente:
            return ["Cliente no encontrado"]
        
        # Obtener páginas
        paginas = db.query(DePagina).filter(
            DePagina.codigo.in_(paginas_codigos),
            DePagina.activa == True
        ).all()
        
        if len(paginas) != len(paginas_codigos):
            paginas_encontradas = [p.codigo for p in paginas]
            paginas_faltantes = [c for c in paginas_codigos if c not in paginas_encontradas]
            return [f"Páginas no encontradas o inactivas: {', '.join(paginas_faltantes)}"]
        
        errores = []
        
        # Validar datos según página
        for pagina in paginas:
            codigo = pagina.codigo
            
            # Mapeo de validaciones por página
            if codigo in ['ruc', 'deudas', 'mercado_valores']:
                if not cliente.ruc or len(cliente.ruc) != 13 or not cliente.ruc.isdigit():
                    errores.append(f"{pagina.nombre} requiere RUC válido (13 dígitos)")
            
            elif codigo in ['contraloria', 'supercias_persona', 'predio_quito', 'predio_manta']:
                if not cliente.ci or len(cliente.ci) != 10 or not cliente.ci.isdigit():
                    errores.append(f"{pagina.nombre} requiere CI válida (10 dígitos)")
            
            elif codigo in ['denuncias', 'google']:
                if not cliente.nombre or not cliente.apellido:
                    errores.append(f"{pagina.nombre} requiere nombre y apellido completos")
            
            elif codigo == 'interpol':
                if not cliente.apellido:
                    errores.append(f"{pagina.nombre} requiere apellido")
        
        return errores
    finally:
        db.close()

def crear_proceso_completo(
    cliente_id: int,
    job_id: str,
    paginas_codigos: List[str],
    headless: bool = False,
    generate_report: bool = True
) -> int:
    """
    Crea un proceso completo con consultas individuales para cada página.
    Retorna el ID del proceso creado.
    """
    db = get_db_session()
    try:
        # 1. Validar que el cliente existe
        cliente = db.query(DeCliente).filter(DeCliente.id == cliente_id).first()
        if not cliente:
            raise ValueError("Cliente no encontrado")
        
        # 2. Validar datos del cliente para las páginas
        errores = validar_datos_cliente_para_paginas(cliente_id, paginas_codigos)
        if errores:
            raise ValueError(f"Datos insuficientes: {'; '.join(errores)}")
        
        # 3. Obtener páginas válidas
        paginas = db.query(DePagina).filter(
            DePagina.codigo.in_(paginas_codigos),
            DePagina.activa == True
        ).all()
        
        if len(paginas) != len(paginas_codigos):
            raise ValueError("Una o más páginas no están disponibles")
        
        # 4. Crear proceso principal
        nuevo_proceso = DeProceso(
            cliente_id=cliente_id,
            job_id=job_id,
            tipo_alerta=cliente.tipo,
            monto_usd=cliente.monto,
            fecha_alerta=cliente.fecha,
            estado='Pendiente',
            fecha_creacion=datetime.now(),
            headless=headless,
            generate_report=generate_report,
            total_paginas_solicitadas=len(paginas_codigos),
            total_paginas_exitosas=0,
            total_paginas_fallidas=0
        )
        
        db.add(nuevo_proceso)
        db.flush()  # Para obtener el ID
        
        # 5. Crear consultas individuales para cada página
        for pagina in paginas:
            # Determinar valor a enviar según la página
            valor_enviar = _obtener_valor_para_pagina(cliente, pagina.codigo)
            
            nueva_consulta = DeConsulta(
                proceso_id=nuevo_proceso.id,
                pagina_id=pagina.id,
                valor_enviado=valor_enviar,
                estado='Pendiente',
                intentos_realizados=0,
                max_intentos=2
            )
            db.add(nueva_consulta)
        
        # 6. Actualizar estado del cliente a 'Procesando'
        cliente.estado = 'Procesando'
        
        db.commit()
        
        print(f"✅ Proceso creado: ID {nuevo_proceso.id}, Job {job_id}")
        return nuevo_proceso.id
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error creando proceso: {str(e)}")
        raise
    finally:
        db.close()

def _obtener_valor_para_pagina(cliente: DeCliente, codigo_pagina: str) -> Optional[str]:
    """
    Obtiene el valor apropiado para enviar a cada página según el tipo.
    """
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
        'predio_manta': cliente.ci,
        'funcion_judicial': f"{cliente.apellido} {cliente.nombre}".strip()
    }
    
    return mapeo_valores.get(codigo_pagina)

# ===== FUNCIONES PARA EL SISTEMA DE SINCRONIZACIÓN =====

def get_proceso_by_job_id(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene información de un proceso por job_id.
    Usado por el sistema de sincronización.
    """
    db = get_db_session()
    try:
        proceso = db.query(DeProceso).filter(DeProceso.job_id == job_id).first()
        if not proceso:
            return None
        
        return {
            "id": proceso.id,
            "cliente_id": proceso.cliente_id,
            "job_id": proceso.job_id,
            "estado": proceso.estado,
            "fecha_inicio": proceso.fecha_inicio,
            "fecha_fin": proceso.fecha_fin
        }
    finally:
        db.close()

def iniciar_proceso(proceso_id: int):
    """
    Marca un proceso como iniciado.
    Usado por el executor cuando comienza a ejecutar el job.
    """
    db = get_db_session()
    try:
        proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
        if proceso:
            proceso.estado = 'En_Proceso'
            proceso.fecha_inicio = datetime.now()
            db.commit()
    finally:
        db.close()

def finalizar_proceso(proceso_id: int, exito: bool = True):
    """
    Marca un proceso como finalizado.
    Usado por el executor cuando termina la ejecución.
    """
    db = get_db_session()
    try:
        proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
        if proceso:
            proceso.fecha_fin = datetime.now()
            
            if exito:
                if proceso.total_paginas_fallidas == 0:
                    proceso.estado = 'Completado'
                else:
                    proceso.estado = 'Completado_Con_Errores'
            else:
                proceso.estado = 'Error_Total'
            
            db.commit()
    finally:
        db.close()

def actualizar_consulta_por_codigo(
    proceso_id: int, 
    codigo_pagina: str, 
    estado: str, 
    datos_capturados: Optional[Dict[str, Any]] = None,
    mensaje_error: Optional[str] = None
):
    """
    Actualiza el estado de una consulta individual por código de página.
    Usado por el executor para reportar progreso página por página.
    """
    db = get_db_session()
    try:
        # Buscar la consulta específica
        consulta = db.query(DeConsulta).join(DePagina).filter(
            DeConsulta.proceso_id == proceso_id,
            DePagina.codigo == codigo_pagina
        ).first()
        
        if consulta:
            consulta.estado = estado
            consulta.fecha_fin = datetime.now()
            
            if datos_capturados:
                consulta.datos_capturados = datos_capturados
            
            if mensaje_error:
                consulta.mensaje_error = mensaje_error
            
            # Calcular duración si hay fecha_inicio
            if consulta.fecha_inicio:
                duracion = (consulta.fecha_fin - consulta.fecha_inicio).total_seconds()
                consulta.duracion_segundos = int(duracion)
            
            db.commit()
            
            # Actualizar contadores del proceso
            _actualizar_contadores_proceso(db, proceso_id)
    finally:
        db.close()

def get_pagina_by_codigo(codigo: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene información de una página por código.
    Usado por el executor para obtener URLs dinámicas.
    """
    db = get_db_session()
    try:
        pagina = db.query(DePagina).filter(
            DePagina.codigo == codigo,
            DePagina.activa == True
        ).first()
        
        if not pagina:
            return None
        
        return {
            "id": pagina.id,
            "nombre": pagina.nombre,
            "codigo": pagina.codigo,
            "url": pagina.url,
            "descripcion": pagina.descripcion
        }
    finally:
        db.close()

def _actualizar_contadores_proceso(db: Session, proceso_id: int):
    """
    Actualiza los contadores de páginas exitosas/fallidas de un proceso.
    """
    # Contar consultas exitosas
    exitosas = db.query(DeConsulta).filter(
        DeConsulta.proceso_id == proceso_id,
        DeConsulta.estado == 'Exitosa'
    ).count()
    
    # Contar consultas fallidas
    fallidas = db.query(DeConsulta).filter(
        DeConsulta.proceso_id == proceso_id,
        DeConsulta.estado == 'Fallida'
    ).count()
    
    # Actualizar proceso
    proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
    if proceso:
        proceso.total_paginas_exitosas = exitosas
        proceso.total_paginas_fallidas = fallidas
        db.commit()