# app/services/daemon_procesador.py - VERSIÓN HTTPX DIRECTO (SIN SCRAPING)
"""
Daemon con consulta directa a API de Función Judicial via HTTPX.

✅ CASO 1: HTTPX + resultados = Reporte con datos
✅ CASO 2: HTTPX + sin procesos = Reporte sin datos

❌ Error real = Resetear a Pendiente

NOTA: Se eliminó el scraping con Selenium. Ahora usa únicamente HTTPX (API directa).
"""

import threading
import time
from typing import Optional
from datetime import datetime
import uuid
import os
import traceback

from app.db import SessionLocal
from app.db.models import DeClienteV2
from app.db.models_new import DeProceso, DeReporte

# ✅ IMPORTACIÓN HTTPX (única forma de consulta ahora)
from app.services.fj_httpx_fallback import generar_reporte_httpx

# ===== ESTADO GLOBAL =====
daemon_thread = None
daemon_running = False
daemon_lock = threading.Lock()


def log(msg: str):
    """Logging con timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[DAEMON {timestamp}] {msg}")


def _actualizar_cliente_estado(cliente_id: int, estado: str):
    """Actualiza ESTADO_CONSULTA del cliente"""
    db = SessionLocal()
    try:
        cliente = db.query(DeClienteV2).filter(DeClienteV2.id == cliente_id).first()
        if cliente:
            cliente.ESTADO_CONSULTA = estado
            cliente.FECHA_ULTIMA_CONSULTA = datetime.now()
            db.commit()
            log(f"✅ Cliente {cliente_id} → {estado}")
    except Exception as e:
        log(f"❌ Error actualizando cliente: {e}")
        db.rollback()
    finally:
        db.close()


def _crear_proceso(cliente_id: int) -> Optional[int]:
    """Crea registro en de_procesos_rpa"""
    db = SessionLocal()
    try:
        job_id = f"daemon_{uuid.uuid4().hex[:12]}"
        
        proceso = DeProceso(
            cliente_id=cliente_id,
            job_id=job_id,
            estado='Pendiente',
            fecha_creacion=datetime.now(),
            headless=True,
            generate_report=True,
            total_paginas_solicitadas=1
        )
        db.add(proceso)
        db.commit()
        
        log(f"✅ Proceso {proceso.id} creado (Job: {job_id})")
        return proceso.id
    except Exception as e:
        log(f"❌ Error creando proceso: {e}")
        db.rollback()
        return None
    finally:
        db.close()


def _obtener_job_id(proceso_id: int) -> str:
    """Obtiene job_id de un proceso"""
    db = SessionLocal()
    try:
        proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
        return proceso.job_id if proceso else f"daemon_{uuid.uuid4().hex[:12]}"
    finally:
        db.close()


def _obtener_cliente_datos(cliente_id: int) -> dict:
    """Obtiene datos del cliente para el reporte"""
    db = SessionLocal()
    try:
        cliente = db.query(DeClienteV2).filter(DeClienteV2.id == cliente_id).first()
        
        if not cliente:
            return {
                'cliente_nombre': '',
                'cliente_cedula': '',
                # Cónyuge - campos separados (compatibilidad)
                'nombre_conyuge': '',
                'cedula_conyuge': '',
                # Cónyuge - campos nuevos para APELLIDOS + NOMBRES
                'nombres_conyuge': '',
                'apellidos_conyuge': '',
                # Codeudor - campos separados (compatibilidad)
                'nombre_codeudor': '',
                'cedula_codeudor': '',
                # Codeudor - campos nuevos para APELLIDOS + NOMBRES
                'nombres_codeudor': '',
                'apellidos_codeudor': '',
                'cliente_id': cliente_id,
            }
        
        return {
            'cliente_nombre': f"{cliente.APELLIDOS_CLIENTE or ''} {cliente.NOMBRES_CLIENTE or ''}".strip(),
            'cliente_cedula': cliente.CEDULA or '',
            # Cónyuge - campos separados (compatibilidad con código existente)
            'nombre_conyuge': cliente.NOMBRES_CONYUGE or '',
            'cedula_conyuge': cliente.CEDULA_CONYUGE or '',
            # Cónyuge - campos nuevos para encabezado profesional
            'nombres_conyuge': cliente.NOMBRES_CONYUGE or '',
            'apellidos_conyuge': cliente.APELLIDOS_CONYUGE or '',
            # Codeudor - campos separados (compatibilidad con código existente)
            'nombre_codeudor': cliente.NOMBRES_CODEUDOR or '',
            'cedula_codeudor': cliente.CEDULA_CODEUDOR or '',
            # Codeudor - campos nuevos para encabezado profesional
            'nombres_codeudor': cliente.NOMBRES_CODEUDOR or '',
            'apellidos_codeudor': cliente.APELLIDOS_CODEUDOR or '',
            'cliente_id': cliente_id,
        }
    except Exception as e:
        log(f"⚠️ Error obteniendo datos cliente: {e}")
        return {
            'cliente_nombre': '',
            'cliente_cedula': '',
            'nombre_conyuge': '',
            'cedula_conyuge': '',
            'nombres_conyuge': '',
            'apellidos_conyuge': '',
            'nombre_codeudor': '',
            'cedula_codeudor': '',
            'nombres_codeudor': '',
            'apellidos_codeudor': '',
            'cliente_id': cliente_id,
        }
    finally:
        db.close()


def _guardar_reporte_en_bd(
    cliente_id: int,
    proceso_id: int,
    job_id: str,
    nombres: str,
    ruta_reporte: str,
    tipo_alerta: str
) -> bool:
    """Guarda reporte en de_reportes_rpa"""
    db = SessionLocal()
    try:
        tamano = os.path.getsize(ruta_reporte) if os.path.exists(ruta_reporte) else 0
        nombre_archivo = os.path.basename(ruta_reporte)
        
        reporte = DeReporte(
            proceso_id=proceso_id,
            cliente_id=cliente_id,
            job_id=job_id,
            nombre_archivo=nombre_archivo,
            ruta_archivo=ruta_reporte,
            tipo_archivo='DOCX',
            generado_exitosamente=True,
            tamano_bytes=tamano,
            tipo_alerta=tipo_alerta,
            fecha_generacion=datetime.now()
        )
        
        db.add(reporte)
        db.commit()
        
        log(f"✅ Reporte guardado en BD (ID: {reporte.id})")
        return True
    except Exception as e:
        log(f"❌ Error guardando reporte: {e}")
        db.rollback()
        return False
    finally:
        db.close()


def _actualizar_proceso(proceso_id: int, estado: str, exitoso: bool = True):
    """Actualiza estado del proceso"""
    db = SessionLocal()
    try:
        proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
        if proceso:
            proceso.estado = estado
            proceso.fecha_fin = datetime.now()
            if exitoso:
                proceso.total_paginas_exitosas = 1
            db.commit()
    except Exception as e:
        log(f"❌ Error actualizando proceso: {e}")
        db.rollback()
    finally:
        db.close()


def _obtener_cliente_pendiente():
    """Obtiene siguiente cliente pendiente"""
    db = SessionLocal()
    try:
        cliente = db.query(DeClienteV2).filter(
            DeClienteV2.ESTADO_CONSULTA == 'Pendiente'
        ).order_by(
            DeClienteV2.FECHA_CREACION_REGISTRO.asc()
        ).first()
        return cliente
    finally:
        db.close()


def _ejecutar_consulta_funcion_judicial(
    proceso_id: int,
    cliente_id: int,
    nombres: str,
    job_id: str
) -> bool:
    """
    FLUJO SIMPLIFICADO - SOLO HTTPX (API DIRECTA):
    
    ✅ CASO 1: HTTPX + resultados → Reporte con datos → Procesado
    ✅ CASO 2: HTTPX + sin procesos → Reporte vacío → Procesado
    ❌ Error real → Resetear a Pendiente
    """
    log(f"🌐 Consultando API Función Judicial para: {nombres}")
    
    try:
        # Obtener datos del cliente para el encabezado del reporte
        meta_cliente = _obtener_cliente_datos(cliente_id)
        meta_cliente['fecha_consulta'] = datetime.now()
        
        # ===== CONSULTA DIRECTA VIA HTTPX =====
        log(f"🔍 [HTTPX] Iniciando consulta directa a API...")
        
        # generar_reporte_httpx retorna (ruta_reporte, resultado_dict)
        ruta_reporte, resultado_httpx = generar_reporte_httpx(nombres, job_id, meta_cliente)
        
        if ruta_reporte is not None:
            # HTTPX generó un reporte (con o sin datos)
            log(f"✅ [HTTPX] Reporte generado: {ruta_reporte}")
            log(f"   - Escenario: {resultado_httpx.get('scenario')}")
            log(f"   - Procesos encontrados: {resultado_httpx.get('total_procesos', 0)}")
            
            try:
                # ✅ CASO 1: HTTPX + RESULTADOS ENCONTRADOS
                if resultado_httpx.get('scenario') == 'results_found':
                    log(f"✅ [CASO 1] HTTPX encontró procesos judiciales")
                    
                    # Guardar en BD
                    if _guardar_reporte_en_bd(
                        cliente_id, proceso_id, job_id, nombres,
                        ruta_reporte,
                        'Función Judicial (HTTPX con resultados)'
                    ):
                        _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                        return True
                    else:
                        # Error BD pero reporte existe, marcar como completado igual
                        log(f"⚠️ Reporte generado pero error guardando en BD")
                        _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                        return True
                
                # ✅ CASO 2: HTTPX + SIN PROCESOS JUDICIALES
                elif resultado_httpx.get('scenario') == 'no_results':
                    log(f"✅ [CASO 2] HTTPX: No se encontraron procesos judiciales")
                    
                    # Guardar en BD (aunque sea reporte vacío)
                    if _guardar_reporte_en_bd(
                        cliente_id, proceso_id, job_id, nombres,
                        ruta_reporte,
                        'Función Judicial (HTTPX sin procesos)'
                    ):
                        _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                        return True
                    else:
                        # Error BD pero reporte existe
                        log(f"⚠️ Reporte generado pero error guardando en BD")
                        _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                        return True
                
                else:
                    # Escenario error en HTTPX
                    log(f"⚠️ HTTPX retornó escenario inesperado: {resultado_httpx.get('scenario')}")
                    log(f"   Mensaje: {resultado_httpx.get('mensaje')}")
                    _actualizar_cliente_estado(cliente_id, 'Pendiente')
                    _actualizar_proceso(proceso_id, 'Error_HTTPX', exitoso=False)
                    return False
                    
            except Exception as e:
                log(f"❌ Error procesando resultado HTTPX: {e}")
                traceback.print_exc()
                _actualizar_cliente_estado(cliente_id, 'Pendiente')
                return False
        
        else:
            # ❌ HTTPX retornó error crítico (ruta_reporte es None)
            log(f"❌ [HTTPX] Error crítico: {resultado_httpx.get('mensaje', 'Error desconocido')}")
            _actualizar_cliente_estado(cliente_id, 'Pendiente')
            _actualizar_proceso(proceso_id, 'Error_Total', exitoso=False)
            return False
        
    except Exception as e:
        log(f"❌ Error en consulta HTTPX: {str(e)}")
        traceback.print_exc()
        
        _actualizar_cliente_estado(cliente_id, 'Pendiente')
        _actualizar_proceso(proceso_id, 'Error_Total', exitoso=False)
        
        return False


def _daemon_loop():
    """Loop principal del daemon"""
    global daemon_running
    
    log("🚀 Daemon iniciado (Modo: HTTPX Directo)")
    ciclo = 0
    
    while daemon_running:
        ciclo += 1
        
        try:
            log(f"🔄 CICLO #{ciclo}")
            
            cliente = _obtener_cliente_pendiente()
            
            if not cliente:
                log("📭 No hay clientes pendientes")
            else:
                nombres = f"{cliente.APELLIDOS_CLIENTE} {cliente.NOMBRES_CLIENTE}".strip()
                log(f"📋 Procesando: {nombres} (ID: {cliente.id})")
                
                # Cambiar a Procesando
                _actualizar_cliente_estado(cliente.id, 'Procesando')
                
                # Crear proceso
                proceso_id = _crear_proceso(cliente.id)
                if not proceso_id:
                    log(f"❌ No se pudo crear proceso")
                    _actualizar_cliente_estado(cliente.id, 'Pendiente')
                    continue
                
                # Obtener job_id
                job_id = _obtener_job_id(proceso_id)
                
                # Ejecutar consulta (HTTPX directo)
                exito = _ejecutar_consulta_funcion_judicial(
                    proceso_id, cliente.id, nombres, job_id
                )
                
                if exito:
                    _actualizar_cliente_estado(cliente.id, 'Procesado')
                    log(f"🎉 Cliente {cliente.id} procesado exitosamente")
                else:
                    log(f"⚠️ Cliente {cliente.id} no se pudo procesar")
            
            # Esperar 30 minutos
            log("⏳ Esperando 30 minutos...")
            
            for i in range(1800):
                if not daemon_running:
                    break
                time.sleep(1)
            
        except Exception as e:
            log(f"❌ Error en ciclo: {e}")
            traceback.print_exc()
            time.sleep(60)
    
    log("🛑 Daemon detenido")


def iniciar_daemon():
    """Inicia el daemon"""
    global daemon_thread, daemon_running
    
    with daemon_lock:
        if daemon_running:
            return {
                "success": False,
                "message": "Daemon ya está en ejecución",
                "estado": "running"
            }
        
        daemon_running = True
        daemon_thread = threading.Thread(target=_daemon_loop, daemon=True)
        daemon_thread.start()
        
        return {
            "success": True,
            "message": "Daemon iniciado (Modo: HTTPX Directo)",
            "estado": "running",
            "thread_id": daemon_thread.ident
        }


def detener_daemon():
    """Detiene el daemon"""
    global daemon_running
    
    with daemon_lock:
        if not daemon_running:
            return {
                "success": False,
                "message": "Daemon no está en ejecución",
                "estado": "stopped"
            }
        
        daemon_running = False
        
        return {
            "success": True,
            "message": "Daemon detenido",
            "estado": "stopped"
        }


def obtener_estado_daemon():
    """Obtiene estado del daemon"""
    global daemon_running, daemon_thread
    
    return {
        "running": daemon_running,
        "thread_alive": daemon_thread.is_alive() if daemon_thread else False,
        "modo": "HTTPX Directo",
        "timestamp": datetime.now().isoformat()
    }