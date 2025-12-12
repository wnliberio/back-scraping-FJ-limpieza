# app/services/daemon_procesador.py - VERSIÓN CON HTTPX FALLBACK
# Flujo: Scraping → Si falla → HTTP → Si falla → Resetear a Pendiente
"""
Daemon que:
1. Lee clientes de de_clientes_rpa_v2 con ESTADO_CONSULTA='Pendiente'
2. Intenta web scraping en Función Judicial
3. Si no hay screenshots de resultados → Fallback a HTTP directo
4. Si HTTP también falla → Resetear cliente a Pendiente
5. Procesa 1 cliente cada 30 minutos
"""

import threading
import time
from typing import Optional
from datetime import datetime
import uuid
import os

from app.db import SessionLocal
from app.db.models import DeClienteV2
from app.db.models_new import DeProceso, DePagina, DeReporte

# Importar web scraping y fallback
from flows.funcion_judicial import process_funcion_judicial_once
from app.services.report_builder import build_report_docx
from app.services.fj_httpx_fallback import generar_reporte_httpx

# ===== ESTADO GLOBAL DEL DAEMON =====
class DaemonState:
    def __init__(self):
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.cliente_actual: Optional[str] = None
        self.ultimo_inicio: Optional[datetime] = None
        self.lock = threading.Lock()

daemon_state = DaemonState()


def log(msg: str):
    """Helper de logging con timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[DAEMON {timestamp}] {msg}")


def _actualizar_cliente_estado(cliente_id: int, estado_nuevo: str):
    """
    Actualiza ESTADO_CONSULTA de un cliente en de_clientes_rpa_v2
    Estados válidos: 'Pendiente', 'Procesando', 'Procesado', 'Error'
    """
    db = SessionLocal()
    try:
        cliente = db.query(DeClienteV2).filter(DeClienteV2.id == cliente_id).first()
        if cliente:
            cliente.ESTADO_CONSULTA = estado_nuevo
            db.commit()
            log(f"✅ Cliente {cliente_id} → ESTADO_CONSULTA='{estado_nuevo}'")
        else:
            log(f"⚠️ Cliente {cliente_id} no encontrado")
    except Exception as e:
        log(f"❌ Error actualizando cliente {cliente_id}: {e}")
        db.rollback()
    finally:
        db.close()


def _crear_proceso(cliente_id: int, cliente: DeClienteV2) -> Optional[int]:
    """
    Crea un registro en de_procesos_rpa para este cliente.
    Retorna el ID del proceso o None si hay error.
    """
    db = SessionLocal()
    try:
        # Crear proceso
        job_id = f"daemon_{uuid.uuid4().hex[:12]}"
        
        nuevo_proceso = DeProceso(
            cliente_id=cliente_id,
            job_id=job_id,
            estado='Pendiente',
            fecha_creacion=datetime.now(),
            headless=True,
            generate_report=True,
            total_paginas_solicitadas=1,
            total_paginas_exitosas=0,
            total_paginas_fallidas=0
        )
        
        db.add(nuevo_proceso)
        db.commit()
        
        log(f"✅ Proceso {nuevo_proceso.id} creado (Job: {job_id})")
        return nuevo_proceso.id
        
    except Exception as e:
        log(f"❌ Error creando proceso: {e}")
        db.rollback()
        return None
    finally:
        db.close()


def _hay_screenshots_validos(resultado: dict) -> bool:
    """
    Verifica si el resultado del scraping tiene screenshots de RESULTADOS.
    No solo valida que exista 'screenshots', sino que haya datos reales.
    """
    if not resultado:
        return False
    
    # Verificar si hay scenario válido (no es error)
    scenario = resultado.get('scenario', '').lower()
    
    # Aceptar scenarios que indican éxito
    return scenario == 'results_found' or scenario == 'resultados_encontrados'


def _ejecutar_consulta_funcion_judicial(
    proceso_id: int,
    cliente_id: int,
    nombres: str,
    job_id: str
) -> bool:
    """
    Ejecuta consulta con 3 flujos posibles:
    1. SCRAPING: Si hay screenshots de resultados → generar DOCX
    2. FALLBACK HTTP: Si scraping falla → intentar API directa
    3. ERROR: Si ambos fallan → resetear cliente a Pendiente
    
    Retorna True si éxito, False si error
    """
    db = SessionLocal()
    try:
        # 1. Obtener proceso
        proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
        if not proceso:
            log(f"⚠️ Proceso {proceso_id} no encontrado")
            return False
        
        # 2. Marcar como iniciado (solo fecha)
        proceso.fecha_inicio = datetime.now()
        db.commit()
        
        log(f"🔄 Ejecutando web scraping para: {nombres}")
        
        # ========== CAMINO 1: WEB SCRAPING ==========
        resultado_scraping = process_funcion_judicial_once(nombres, headless=True)
        
        # Verificar si hay screenshots de RESULTADOS (no solo del formulario)
        if _hay_screenshots_validos(resultado_scraping):
            # ✅ SCRAPING EXITOSO
            log(f"✅ [SCRAPING] Resultados encontrados con screenshots")
            
            try:
                log(f"📄 Generando reporte DOCX desde scraping...")
                
                ruta_reporte = build_report_docx(
                    job_id=job_id,
                    meta={
                        'cliente_nombre': nombres,
                        'cliente_id': cliente_id,
                        'fecha_consulta': datetime.now().isoformat()
                    },
                    results={'funcion_judicial': resultado_scraping}
                )
                
                if ruta_reporte and os.path.exists(ruta_reporte):
                    log(f"✅ Reporte generado: {ruta_reporte}")
                    
                    # Guardar en de_reportes_rpa
                    tamano = os.path.getsize(ruta_reporte)
                    nombre_archivo = os.path.basename(ruta_reporte)
                    
                    nuevo_reporte = DeReporte(
                        proceso_id=proceso_id,
                        cliente_id=cliente_id,
                        job_id=job_id,
                        nombre_archivo=nombre_archivo,
                        ruta_archivo=ruta_reporte,
                        url_descarga=f"/api/tracking/reportes/{proceso_id}/download",
                        tamano_bytes=tamano,
                        tipo_archivo='DOCX',
                        tipo_alerta='Función Judicial (Scraping)',
                        generado_exitosamente=True,
                        data_snapshot={'scenario': resultado_scraping.get('scenario')},
                        fecha_generacion=datetime.now()
                    )
                    db.add(nuevo_reporte)
                    db.commit()
                    
                    # Marcar proceso como completado
                    proceso.estado = 'Completado'
                    proceso.total_paginas_exitosas = 1
                    proceso.fecha_fin = datetime.now()
                    db.commit()
                    
                    log(f"✅ [SCRAPING] Proceso completado exitosamente")
                    return True
                    
            except Exception as e:
                log(f"⚠️ Error generando reporte desde scraping: {e}")
                # Continuar a fallback
        
        # ========== CAMINO 2: FALLBACK HTTP ==========
        log(f"⚠️ [SCRAPING] Sin screenshots de resultados, intentando HTTP fallback...")
        log(f"🌐 [HTTPX FALLBACK] Iniciando...")
        
        ruta_reporte_http = generar_reporte_httpx(nombres, job_id)
        
        if ruta_reporte_http:
            # ✅ HTTP EXITOSO
            log(f"✅ [HTTPX FALLBACK] Reporte generado exitosamente")
            
            try:
                # Guardar en de_reportes_rpa
                tamano = os.path.getsize(ruta_reporte_http)
                nombre_archivo = os.path.basename(ruta_reporte_http)
                
                nuevo_reporte = DeReporte(
                    proceso_id=proceso_id,
                    cliente_id=cliente_id,
                    job_id=job_id,
                    nombre_archivo=nombre_archivo,
                    ruta_archivo=ruta_reporte_http,
                    url_descarga=f"/api/tracking/reportes/{proceso_id}/download",
                    tamano_bytes=tamano,
                    tipo_archivo='DOCX',
                    tipo_alerta='Función Judicial (HTTP Fallback)',
                    generado_exitosamente=True,
                    data_snapshot={'metodo': 'httpx_api'},
                    fecha_generacion=datetime.now()
                )
                db.add(nuevo_reporte)
                db.commit()
                
                # Marcar proceso como completado
                proceso.estado = 'Completado'
                proceso.total_paginas_exitosas = 1
                proceso.fecha_fin = datetime.now()
                db.commit()
                
                log(f"✅ [HTTPX FALLBACK] Proceso completado exitosamente")
                return True
                
            except Exception as e:
                log(f"❌ Error registrando reporte HTTP en BD: {e}")
                # Continuar a error
        
        # ========== CAMINO 3: AMBOS FALLAN ==========
        log(f"❌ [HTTPX FALLBACK] Fallback también falló")
        log(f"⚠️ [ERROR] Reseteando cliente a 'Pendiente' para reintentar...")
        
        # Resetear cliente a Pendiente
        cliente = db.query(DeClienteV2).filter(DeClienteV2.id == cliente_id).first()
        if cliente:
            cliente.ESTADO_CONSULTA = 'Pendiente'
            db.commit()
        
        # Marcar proceso como error
        proceso.estado = 'Error_Total'
        proceso.fecha_fin = datetime.now()
        db.commit()
        
        log(f"⚠️ Cliente {cliente_id} reseteado a Pendiente para reintentar después")
        return False
        
    except Exception as e:
        log(f"❌ Error ejecutando consulta: {e}")
        import traceback
        traceback.print_exc()
        
        # Marcar como error
        try:
            proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
            if proceso:
                proceso.estado = 'Error_Total'
                proceso.fecha_fin = datetime.now()
                db.commit()
        except:
            pass
        
        return False
    finally:
        db.close()


def _obtener_cliente_pendiente():
    """
    Obtiene 1 cliente con ESTADO_CONSULTA='Pendiente'
    Ordena por fecha de creación (más antiguos primero)
    """
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


def _procesar_cliente(cliente: DeClienteV2):
    """
    Procesa un cliente completo:
    1. Cambia a 'Procesando'
    2. Crea proceso en BD
    3. Ejecuta web scraping con fallback
    4. Cambia a 'Procesado' o 'Pendiente' (si ambos fallan)
    """
    cliente_id = cliente.id
    nombres = f"{cliente.NOMBRES_CLIENTE} {cliente.APELLIDOS_CLIENTE}"
    
    log(f"🔄 Procesando cliente: {nombres} (ID: {cliente_id})")
    
    try:
        # 1. Cambiar a 'Procesando'
        _actualizar_cliente_estado(cliente_id, 'Procesando')
        
        # 2. Crear proceso en BD
        proceso_id = _crear_proceso(cliente_id, cliente)
        if not proceso_id:
            raise Exception("No se pudo crear proceso en BD")
        
        # 3. Obtener job_id del proceso
        db = SessionLocal()
        try:
            proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
            job_id = proceso.job_id if proceso else f"daemon_{uuid.uuid4().hex[:12]}"
        finally:
            db.close()
        
        # 4. Ejecutar web scraping con fallback
        exito = _ejecutar_consulta_funcion_judicial(proceso_id, cliente_id, nombres, job_id)
        
        # 5. Estado final (ya se actualiza en _ejecutar_consulta_funcion_judicial)
        if exito:
            _actualizar_cliente_estado(cliente_id, 'Procesado')
            log(f"✅ Cliente {cliente_id} procesado exitosamente")
        else:
            # Si falla, el cliente ya fue reseteado a Pendiente en _ejecutar_consulta_funcion_judicial
            log(f"⚠️ Cliente {cliente_id} reseteado, se reintentará después")
        
    except Exception as e:
        log(f"❌ Error procesando cliente {cliente_id}: {e}")
        _actualizar_cliente_estado(cliente_id, 'Pendiente')


def _daemon_loop():
    """
    Loop principal del daemon.
    Procesa 1 cliente cada 30 minutos indefinidamente.
    """
    log("🚀 Daemon iniciado - Loop principal en ejecución")
    
    while daemon_state.running:
        try:
            # 1. Buscar 1 cliente pendiente
            log("🔍 Buscando cliente pendiente...")
            cliente = _obtener_cliente_pendiente()
            
            if not cliente:
                log("📭 No hay clientes pendientes. Esperando 30 minutos...")
            else:
                # 2. Procesar cliente
                with daemon_state.lock:
                    daemon_state.cliente_actual = f"{cliente.NOMBRES_CLIENTE} {cliente.APELLIDOS_CLIENTE}"
                    daemon_state.ultimo_inicio = datetime.now()
                
                _procesar_cliente(cliente)
            
            # 3. Esperar 30 minutos
            log("⏳ Esperando 30 minutos antes del próximo procesamiento...")
            
            wait_time = 30 * 60  # 30 minutos en segundos
            interval = 10  # Verificar cada 10 segundos si se detuvo
            elapsed = 0
            
            while elapsed < wait_time and daemon_state.running:
                time.sleep(interval)
                elapsed += interval
                
                # Log cada 5 minutos
                if elapsed % 300 == 0:
                    remaining = (wait_time - elapsed) // 60
                    log(f"⏱️  Faltan {remaining} minutos para el próximo procesamiento")
            
            if not daemon_state.running:
                log("⛔ Daemon detenido durante espera")
                break
                
        except Exception as e:
            log(f"❌ Error en daemon loop: {e}")
            import traceback
            traceback.print_exc()
            
            # Esperar 1 minuto antes de reintentar
            time.sleep(60)
    
    log("🛑 Daemon detenido - Loop finalizado")


# ===== FUNCIONES DE CONTROL =====

def iniciar_daemon() -> dict:
    """
    Inicia el daemon en un thread separado.
    """
    with daemon_state.lock:
        if daemon_state.running:
            return {
                "success": False,
                "message": "El daemon ya está en ejecución",
                "estado": "running"
            }
        
        daemon_state.running = True
        daemon_state.thread = threading.Thread(
            target=_daemon_loop,
            name="DaemonProcesador",
            daemon=True
        )
        daemon_state.thread.start()
        
        log("✅ Daemon iniciado correctamente")
        
        return {
            "success": True,
            "message": "Daemon iniciado correctamente",
            "estado": "running",
            "thread_id": daemon_state.thread.ident
        }


def detener_daemon() -> dict:
    """
    Detiene el daemon de forma controlada.
    """
    with daemon_state.lock:
        if not daemon_state.running:
            return {
                "success": False,
                "message": "El daemon no está en ejecución",
                "estado": "stopped"
            }
        
        daemon_state.running = False
        log("⏹️  Señal de detención enviada al daemon")
    
    # Esperar a que el thread termine (máximo 5 segundos)
    if daemon_state.thread and daemon_state.thread.is_alive():
        daemon_state.thread.join(timeout=5)
    
    log("✅ Daemon detenido correctamente")
    
    return {
        "success": True,
        "message": "Daemon detenido correctamente",
        "estado": "stopped"
    }


def obtener_estado_daemon() -> dict:
    """
    Obtiene el estado actual del daemon.
    """
    with daemon_state.lock:
        return {
            "running": daemon_state.running,
            "thread_alive": daemon_state.thread.is_alive() if daemon_state.thread else False,
            "cliente_actual": daemon_state.cliente_actual,
            "ultimo_inicio": daemon_state.ultimo_inicio.isoformat() if daemon_state.ultimo_inicio else None
        }
