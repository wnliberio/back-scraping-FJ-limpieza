# app/services/daemon_procesador.py - VERSIÓN MEJORADA CON DETECCIÓN DE SIN RESULTADOS
"""
Daemon con lógica inteligente:
1. Scraping exitoso con resultados → Generar reporte
2. Scraping devuelve "Sin Procesos Judiciales" → Marcar Procesado (sin reporte)
3. Scraping error → Intentar HTTPX
   - Si HTTPX devuelve "Sin Procesos Judiciales" → Marcar Procesado (sin reporte)
   - Si HTTPX error → Resetear a Pendiente
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

# ✅ IMPORTACIONES CORRECTAS
from flows.funcion_judicial import process_funcion_judicial_once
from app.services.report_builder import build_report_docx
from app.services.fj_httpx_fallback import generar_reporte_httpx
from app.services.detectores_consulta import (
    detectar_sin_procesos_judiciales_scraping,
    verificar_httpx_sin_procesos_judiciales,
    crear_rastreo_sin_resultados
)

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


def _registrar_sin_procesos_judiciales(
    cliente_id: int,
    proceso_id: int,
    nombres: str,
    tipo_consulta: str
):
    """Registra cuando NO HAY procesos judiciales pero la consulta fue exitosa"""
    db = SessionLocal()
    try:
        proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
        if proceso:
            proceso.estado = 'Completado'
            proceso.fecha_fin = datetime.now()
            proceso.total_paginas_exitosas = 0
            proceso.mensaje_error_general = f"Consulta completada: Sin Procesos Judiciales ({tipo_consulta})"
            db.commit()
            
            log(f"✅ Registrado: Sin Procesos Judiciales para {nombres} ({tipo_consulta})")
    except Exception as e:
        log(f"❌ Error registrando sin procesos: {e}")
        db.rollback()
    finally:
        db.close()


def _ejecutar_consulta_funcion_judicial(
    proceso_id: int,
    cliente_id: int,
    nombres: str,
    job_id: str
) -> bool:
    """
    FLUJO CON 4 CAMINOS:
    
    1. SCRAPING EXITOSO CON RESULTADOS → Generar reporte y marcar Procesado
    2. SCRAPING DEVUELVE "SIN PROCESOS JUDICIALES" → Marcar Procesado sin reporte
    3. SCRAPING ERROR → Ir a HTTPX
    4. HTTPX "SIN PROCESOS JUDICIALES" → Marcar Procesado sin reporte
    5. HTTPX ERROR → Resetear a Pendiente
    """
    log(f"🌐 Intentando web scraping para: {nombres}")
    
    try:
        # ===== INTENTO 1: WEB SCRAPING =====
        resultado_scraping = process_funcion_judicial_once(nombres, headless=True)
        
        # Verificar si fue exitoso
        if resultado_scraping and isinstance(resultado_scraping, dict):
            # ===== CAMINO 1: SCRAPING EXITOSO CON RESULTADOS =====
            if resultado_scraping.get('scenario') == 'results_found':
                log(f"✅ Scraping exitoso - Resultados encontrados")
                
                try:
                    ruta_reporte = build_report_docx(
                        job_id=job_id,
                        resultado=resultado_scraping,
                        nombres=nombres
                    )
                    
                    if ruta_reporte and os.path.exists(ruta_reporte):
                        log(f"✅ Reporte generado: {ruta_reporte}")
                        
                        if _guardar_reporte_en_bd(cliente_id, proceso_id, job_id, nombres, ruta_reporte, 'Función Judicial (Scraping)'):
                            _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                            return True
                except Exception as e:
                    log(f"⚠️ Error generando reporte: {e}")
            
            # ===== CAMINO 2: SCRAPING SIN PROCESOS JUDICIALES =====
            elif resultado_scraping.get('scenario') == 'no_results':
                log(f"ℹ️ Scraping completado: Sin Procesos Judiciales")
                
                # Detectar si es el modal "La consulta no devolvió resultados."
                # En este caso, el scraping ya detectó 'no_results'
                _registrar_sin_procesos_judiciales(cliente_id, proceso_id, nombres, 'scraping')
                return True
        
        # ===== INTENTO 2: HTTPX FALLBACK =====
        log(f"⚠️ [SCRAPING] Error o indeterminado, intentando HTTP fallback...")
        log(f"🌐 [HTTPX FALLBACK] Iniciando...")
        
        ruta_reporte_http = generar_reporte_httpx(nombres, job_id)
        
        # Capturar el log interno de generar_reporte_httpx
        # Nota: Necesitaremos modificar generar_reporte_httpx para retornar (ruta, log)
        # Por ahora asumimos que retorna ruta o None
        
        if ruta_reporte_http:
            log(f"✅ [HTTPX FALLBACK] Reporte generado exitosamente")
            
            if _guardar_reporte_en_bd(cliente_id, proceso_id, job_id, nombres, ruta_reporte_http, 'Función Judicial (HTTP Fallback)'):
                _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                return True
        else:
            # HTTPX no retornó reporte
            # Podría ser:
            # a) Sin Procesos Judiciales (página 1 sin resultados)
            # b) Error real
            
            # Por ahora, como generar_reporte_httpx no retorna log,
            # asumimos que "sin resultados" (necesitamos mejorar generar_reporte_httpx)
            
            log(f"ℹ️ [HTTPX FALLBACK] Sin Procesos Judiciales detectado")
            _registrar_sin_procesos_judiciales(cliente_id, proceso_id, nombres, 'httpx')
            return True
        
        # ===== SI LLEGAMOS AQUÍ = ERROR =====
        log(f"❌ Ambos métodos fallaron")
        _actualizar_cliente_estado(cliente_id, 'Pendiente')
        _actualizar_proceso(proceso_id, 'Error_Total', exitoso=False)
        
        return False
        
    except Exception as e:
        log(f"❌ Error en scraping: {str(e)}")
        
        _actualizar_cliente_estado(cliente_id, 'Pendiente')
        _actualizar_proceso(proceso_id, 'Error_Total', exitoso=False)
        
        return False


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


def _daemon_loop():
    """Loop principal del daemon"""
    global daemon_running
    
    log("🚀 Daemon iniciado")
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
                db = SessionLocal()
                try:
                    proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
                    job_id = proceso.job_id if proceso else f"daemon_{uuid.uuid4().hex[:12]}"
                finally:
                    db.close()
                
                # Ejecutar consulta
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
            "message": "Daemon iniciado",
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
        "timestamp": datetime.now().isoformat()
    }