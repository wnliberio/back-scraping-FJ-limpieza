# app/services/daemon_procesador.py - VERSIÓN HTTPX DIRECTO (SIN SCRAPING)
"""
Daemon con consulta directa a API de Función Judicial via HTTPX.

✅ CASO 1: HTTPX + resultados = Reporte con datos → Procesado
✅ CASO 2: HTTPX + sin procesos = Reporte sin datos → Procesado
❌ CASO 3: HTTPX + error API (500, timeout) = NO reporte → Pendiente (reintento)

NOTA: Se eliminó el scraping con Selenium. Ahora usa únicamente HTTPX (API directa).
"""

import threading
import time
from typing import Optional
from datetime import datetime
import uuid
import os
import traceback
import re

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


def _construir_nombre_busqueda(apellidos: str, nombres: str) -> str:
    """
    Genera el nombre completo del cliente de forma segura, manejando
    diferentes tipos de cliente (empresa o persona natural) y la posible
    ausencia de nombres o apellidos.
    """
    log(f"🔍 DEBUG - Valores crudos de BD:")
    log(f"   APELLIDOS_CLIENTE: '{apellidos}'")
    log(f"   NOMBRES_CLIENTE: '{nombres}'")

    apellidos = (apellidos or "").strip()
    nombres = (nombres or "").strip()

    # 🔹 Corrección exacta:
    # - '-' → espacio
    # - '.' → eliminar
    apellidos = apellidos.replace("-", " ").replace(".", "")
    nombres = nombres.replace("-", " ").replace(".", "")

    # Construir nombre
    if apellidos and nombres:
        nombre_completo = f"{apellidos} {nombres}"
    elif apellidos:
        nombre_completo = apellidos
    elif nombres:
        nombre_completo = nombres
    else:
        nombre_completo = ""

    # Normalizar espacios múltiples
    nombre_limpio = " ".join(nombre_completo.split())

    log(f"🔍 DEBUG - Nombre final para API:")
    log(f"   '{nombre_limpio}'")

    return nombre_limpio


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
        
        # ✅ USAR LA FUNCIÓN ROBUSTA PARA CONSTRUIR EL NOMBRE
        nombre_cliente = _construir_nombre_busqueda(
            cliente.APELLIDOS_CLIENTE,
            cliente.NOMBRES_CLIENTE
        )
        
        return {
            'cliente_nombre': nombre_cliente,
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
    
     CASO 1: HTTPX + resultados → Reporte con datos → Procesado
     CASO 2: HTTPX + sin procesos → Reporte sin datos → Procesado
     CASO 3: HTTPX + error API (500, timeout) → NO reporte → Pendiente
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
        
        scenario = resultado_httpx.get('scenario', 'error')
        
        #  CASO 1: HTTPX + RESULTADOS ENCONTRADOS
        if scenario == 'results_found' and ruta_reporte is not None:
            log(f"  [CASO 1] HTTPX encontró procesos judiciales")
            log(f"   - Reporte: {ruta_reporte}")
            log(f"   - Procesos: {resultado_httpx.get('total_procesos', 0)}")
            
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
                log(f" Reporte generado pero error guardando en BD")
                _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                return True
        
        #  CASO 2: HTTPX + SIN PROCESOS JUDICIALES (API respondió OK pero lista vacía)
        elif scenario == 'no_results' and ruta_reporte is not None:
            log(f" [CASO 2] HTTPX: No se encontraron procesos judiciales")
            log(f"   - Reporte: {ruta_reporte}")
            
            # Guardar en BD (reporte vacío pero válido)
            if _guardar_reporte_en_bd(
                cliente_id, proceso_id, job_id, nombres,
                ruta_reporte,
                'Función Judicial (HTTPX sin procesos)'
            ):
                _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                return True
            else:
                log(f"⚠️ Reporte generado pero error guardando en BD")
                _actualizar_proceso(proceso_id, 'Completado', exitoso=True)
                return True
        
        # ❌ CASO 3: ERROR DE API (500, timeout, etc.) - NO generar reporte
        elif scenario == 'api_error':
            log(f"❌ [CASO 3] Error de API - Cliente volverá a Pendiente")
            log(f"   - Mensaje: {resultado_httpx.get('mensaje', 'Error desconocido')}")
            log(f"   - El cliente se reintentará en el próximo ciclo")
            
            # NO generar reporte, resetear cliente a Pendiente
            _actualizar_cliente_estado(cliente_id, 'Pendiente')
            _actualizar_proceso(proceso_id, 'Error_API', exitoso=False)
            return False
        
        # ❌ OTROS ERRORES
        else:
            log(f"❌ Error inesperado - scenario: {scenario}")
            log(f"   - Mensaje: {resultado_httpx.get('mensaje', 'Error desconocido')}")
            
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
                # ✅ USAR LA FUNCIÓN ROBUSTA PARA CONSTRUIR EL NOMBRE
                nombres = _construir_nombre_busqueda(
                    cliente.APELLIDOS_CLIENTE,
                    cliente.NOMBRES_CLIENTE
                )
                
                log(f"📋 Procesando: {nombres} (ID: {cliente.id})")
                
                # Validar que tengamos un nombre válido
                if not nombres:
                    log(f"⚠️ Cliente {cliente.id} no tiene nombre válido - saltando")
                    _actualizar_cliente_estado(cliente.id, 'Error')
                    continue
                
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
                    log(f"⚠️ Cliente {cliente.id} volverá a intentarse (Pendiente)")
            
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