# app/services/tracking_professional.py - VERSIÓN FINAL COMPLETA
# Estados válidos en BD: 'Pendiente', 'Procesando', 'Procesado', 'Error'
"""
Servicio profesional de tracking actualizado para usar de_clientes_rpa_v2
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc

from app.db import SessionLocal
from app.db.models import DeClienteV2  # ✅ NUEVA TABLA
from app.db.models_new import (
    DeProceso, DeConsulta, DePagina, DeReporte
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
    Obtiene clientes de de_clientes_rpa_v2 con filtros opcionales.
    Retorna los campos relevantes para el frontend.
    
    Estados válidos en BD: 'Pendiente', 'Procesando', 'Procesado', 'Error'
    
    Filtros:
    - estado: filtra por ESTADO_CONSULTA
    - fecha_desde/hasta: filtra por FECHA_CREACION_SOLICITUD
    - q: búsqueda en NOMBRES_CLIENTE, APELLIDOS_CLIENTE, CEDULA
    """
    db = get_db_session()
    try:
        query = db.query(DeClienteV2)
        
        # Filtrar por ESTADO_CONSULTA
        if estado and estado != "Todos":
            # Mapear alias de estados para compatibilidad
            estado_bd = estado
            if estado == "En_Proceso":
                estado_bd = "Procesando"
            
            query = query.filter(DeClienteV2.ESTADO_CONSULTA == estado_bd)
        
        # Filtrar por rango de fechas
        if fecha_desde:
            try:
                fecha_desde_dt = datetime.strptime(fecha_desde, "%Y-%m-%d").date()
                query = query.filter(DeClienteV2.FECHA_CREACION_SOLICITUD >= fecha_desde_dt)
            except ValueError:
                pass  # Ignorar fecha inválida
        
        if fecha_hasta:
            try:
                fecha_hasta_dt = datetime.strptime(fecha_hasta, "%Y-%m-%d").date()
                query = query.filter(DeClienteV2.FECHA_CREACION_SOLICITUD <= fecha_hasta_dt)
            except ValueError:
                pass  # Ignorar fecha inválida
        
        # Búsqueda por nombre, apellido, CI
        if q and q.strip():
            search_term = f"%{q.strip()}%"
            query = query.filter(
                or_(
                    DeClienteV2.NOMBRES_CLIENTE.ilike(search_term),
                    DeClienteV2.APELLIDOS_CLIENTE.ilike(search_term),
                    DeClienteV2.CEDULA.ilike(search_term)
                )
            )
        
        # Ordenar por fecha de creación descendente
        query = query.order_by(DeClienteV2.FECHA_CREACION_REGISTRO.desc())
        
        clientes = query.all()
        
        # Convertir a diccionarios con los campos que necesita el frontend
        resultado = []
        for cliente in clientes:
            # Convertir ESTADO_CONSULTA 'Procesando' a 'En_Proceso' para el frontend
            estado_frontend = cliente.ESTADO_CONSULTA
            if estado_frontend == "Procesando":
                estado_frontend = "En_Proceso"
            
            resultado.append({
                "id": cliente.id,
                "ID_SOLICITUD": cliente.ID_SOLICITUD,
                "ESTADO": cliente.ESTADO,
                "AGENCIA": cliente.AGENCIA,
                "CEDULA": cliente.CEDULA,
                "NOMBRES_CLIENTE": cliente.NOMBRES_CLIENTE,
                "APELLIDOS_CLIENTE": cliente.APELLIDOS_CLIENTE,
                "ESTADO_CONSULTA": estado_frontend,  # Convertido para frontend
                "FECHA_CREACION_SOLICITUD": cliente.FECHA_CREACION_SOLICITUD.isoformat() if cliente.FECHA_CREACION_SOLICITUD else None,
                "FECHA_CREACION_REGISTRO": cliente.FECHA_CREACION_REGISTRO.isoformat() if cliente.FECHA_CREACION_REGISTRO else None,
                # Campos adicionales opcionales para compatibilidad
                "ID_PRODUCTO": cliente.ID_PRODUCTO,
                "PRODUCTO": cliente.PRODUCTO,
                "ESTADO_CIVIL": cliente.ESTADO_CIVIL
            })
        
        return resultado
        
    finally:
        db.close()

def update_cliente_estado(
    cliente_id: int,
    estado: str,
    mensaje_error: Optional[str] = None
) -> bool:
    """
    Actualiza el ESTADO_CONSULTA de un cliente en de_clientes_rpa_v2.
    
    Estados válidos en BD: 'Pendiente', 'Procesando', 'Procesado', 'Error'
    El frontend puede enviar 'En_Proceso' que se convierte a 'Procesando'
    
    Retorna True si se actualizó exitosamente, False si no se encontró el cliente.
    """
    db = get_db_session()
    try:
        cliente = db.query(DeClienteV2).filter(DeClienteV2.id == cliente_id).first()
        
        if not cliente:
            return False
        
        # Convertir alias de estados para BD
        estado_bd = estado
        if estado == "En_Proceso":
            estado_bd = "Procesando"
        
        cliente.ESTADO_CONSULTA = estado_bd
        db.commit()
        
        return True
        
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def validar_datos_cliente_para_paginas(
    cliente_id: int,
    paginas_codigos: List[str]
) -> List[str]:
    """
    Valida que el cliente tenga los datos necesarios para consultar las páginas especificadas.
    
    Retorna lista de errores (vacía si todo está bien).
    """
    db = get_db_session()
    try:
        # Obtener cliente
        cliente = db.query(DeClienteV2).filter(DeClienteV2.id == cliente_id).first()
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
            if codigo in ['ruc']:
                # RUC no está en V2, saltar
                pass
            
            elif codigo in ['deudas', 'mercado_valores', 'supercias_persona']:
                if not cliente.CEDULA or len(str(cliente.CEDULA)) != 10:
                    errores.append(f"{pagina.nombre} requiere CI válida (10 dígitos)")
            
            elif codigo in ['contraloria', 'predio_quito', 'predio_manta', 'interpol']:
                if not cliente.CEDULA or len(str(cliente.CEDULA)) != 10:
                    errores.append(f"{pagina.nombre} requiere CI válida (10 dígitos)")
            
            elif codigo in ['denuncias', 'google']:
                if not cliente.NOMBRES_CLIENTE or not cliente.APELLIDOS_CLIENTE:
                    errores.append(f"{pagina.nombre} requiere nombre y apellido completos")
        
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
        cliente = db.query(DeClienteV2).filter(DeClienteV2.id == cliente_id).first()
        if not cliente:
            raise ValueError("Cliente no encontrado")
        
        # 2. Validar datos del cliente para las páginas
        errores = validar_datos_cliente_para_paginas(cliente_id, paginas_codigos)
        if errores:
            raise ValueError(f"Datos insuficientes: {'; '.join(errores)}")
        
        # 3. Crear proceso
        proceso = DeProceso(
            cliente_id=cliente_id,
            job_id=job_id,
            estado='Pendiente',
            fecha_creacion=datetime.now(),
            headless=headless,
            generate_report=generate_report,
            total_paginas_solicitadas=len(paginas_codigos)
        )
        
        db.add(proceso)
        db.commit()
        
        # 4. Crear consultas para cada página
        paginas = db.query(DePagina).filter(
            DePagina.codigo.in_(paginas_codigos)
        ).all()
        
        for pagina in paginas:
            consulta = DeConsulta(
                proceso_id=proceso.id,
                pagina_id=pagina.id,
                estado='Pendiente',
                fecha_creacion=datetime.now()
            )
            db.add(consulta)
        
        db.commit()
        
        return proceso.id
        
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_estadisticas(
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None
) -> Dict[str, Any]:
    """
    Obtiene estadísticas del sistema.
    """
    db = get_db_session()
    try:
        # Parsear fechas
        fecha_desde_dt = None
        fecha_hasta_dt = None
        
        if fecha_desde:
            try:
                fecha_desde_dt = datetime.strptime(fecha_desde, "%Y-%m-%d")
            except ValueError:
                pass
        
        if fecha_hasta:
            try:
                fecha_hasta_dt = datetime.strptime(fecha_hasta, "%Y-%m-%d")
            except ValueError:
                pass
        
        # Construir filtros
        filtro_fecha_clientes = True
        if fecha_desde_dt:
            filtro_fecha_clientes = and_(filtro_fecha_clientes, DeClienteV2.FECHA_CREACION_SOLICITUD >= fecha_desde_dt.date())
        if fecha_hasta_dt:
            filtro_fecha_clientes = and_(filtro_fecha_clientes, DeClienteV2.FECHA_CREACION_SOLICITUD <= fecha_hasta_dt.date())
        
        # Estadísticas de clientes V2
        total_clientes = db.query(DeClienteV2).filter(filtro_fecha_clientes).count()
        clientes_pendientes = db.query(DeClienteV2).filter(
            and_(filtro_fecha_clientes, DeClienteV2.ESTADO_CONSULTA == 'Pendiente')
        ).count()
        clientes_procesando = db.query(DeClienteV2).filter(
            and_(filtro_fecha_clientes, DeClienteV2.ESTADO_CONSULTA == 'Procesando')
        ).count()
        clientes_procesados = db.query(DeClienteV2).filter(
            and_(filtro_fecha_clientes, DeClienteV2.ESTADO_CONSULTA == 'Procesado')
        ).count()
        clientes_error = db.query(DeClienteV2).filter(
            and_(filtro_fecha_clientes, DeClienteV2.ESTADO_CONSULTA == 'Error')
        ).count()
        
        # Estadísticas de procesos
        filtro_fecha_procesos = True
        if fecha_desde_dt:
            filtro_fecha_procesos = and_(filtro_fecha_procesos, DeProceso.fecha_creacion >= fecha_desde_dt)
        if fecha_hasta_dt:
            filtro_fecha_procesos = and_(filtro_fecha_procesos, DeProceso.fecha_creacion <= fecha_hasta_dt)
        
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
            }
        }
    finally:
        db.close()

def get_proceso_by_job_id(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene información de un proceso por su job_id.
    Se usa en routers para obtener detalles del proceso.
    """
    db = get_db_session()
    try:
        proceso = db.query(DeProceso).filter(DeProceso.job_id == job_id).first()
        
        if not proceso:
            return None
        
        return {
            'id': proceso.id,
            'cliente_id': proceso.cliente_id,
            'job_id': proceso.job_id,
            'estado': proceso.estado,
            'fecha_creacion': proceso.fecha_creacion.isoformat() if proceso.fecha_creacion else None,
            'fecha_inicio': proceso.fecha_inicio.isoformat() if proceso.fecha_inicio else None,
            'fecha_fin': proceso.fecha_fin.isoformat() if proceso.fecha_fin else None,
            'total_paginas_solicitadas': proceso.total_paginas_solicitadas,
            'total_paginas_exitosas': proceso.total_paginas_exitosas,
            'total_paginas_fallidas': proceso.total_paginas_fallidas
        }
    finally:
        db.close()