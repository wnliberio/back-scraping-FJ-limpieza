# app/services/daemon_procesador.py
"""
Daemon automático que procesa clientes en estado 'Pendiente'.
- Toma lotes de hasta 5 clientes
- Procesa uno por uno
- Espera 30 minutos entre lotes
- Se puede iniciar/detener con endpoints
"""

import threading
import time
from typing import Optional, List
from datetime import datetime

from app.db import SessionLocal
from app.db.models_new import DeCliente, DeProceso, DeConsulta, DePagina, DeReporte
from flows.funcion_judicial import process_funcion_judicial_once

# Importar generador de reportes
from app.services.report_builder import build_report_docx
import os
# ===== ESTADO GLOBAL DEL DAEMON =====
class DaemonState:
    def __init__(self):
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.clientes_procesados_en_lote = 0
        self.ultimo_lote_inicio: Optional[datetime] = None
        self.lock = threading.Lock()

daemon_state = DaemonState()


def log(msg: str):
    """Helper de logging con timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[DAEMON {timestamp}] {msg}")


# ===== FUNCIONES DE ACTUALIZACIÓN DE BD =====

def _actualizar_cliente_estado(cliente_id: int, estado: str):
    """Actualiza el estado de un cliente"""
    db = SessionLocal()
    try:
        cliente = db.query(DeCliente).filter(DeCliente.id == cliente_id).first()
        if cliente:
            cliente.estado = estado
            db.commit()
            log(f"✅ Cliente {cliente_id} → estado '{estado}'")
    except Exception as e:
        log(f"❌ Error actualizando cliente {cliente_id}: {e}")
        db.rollback()
    finally:
        db.close()


def _generar_reporte_para_proceso(db, proceso: DeProceso, resultado: dict, nombre_consultado: str):
    """
    Genera reporte DOCX para un proceso exitoso y lo guarda en de_reportes_rpa.
    
    Args:
        db: Sesión de base de datos activa
        proceso: Objeto DeProceso
        resultado: Diccionario con resultados del scraper
        nombre_consultado: Nombre completo consultado
    """
    try:
        log(f"📄 Generando reporte para proceso {proceso.id}...")
        
        # Preparar metadata
        meta = {
            'tipo_alerta': proceso.tipo_alerta or 'Consulta Función Judicial',
            'monto_usd': float(proceso.monto_usd) if proceso.monto_usd else None,
            'fecha_alerta': proceso.fecha_alerta.isoformat() if proceso.fecha_alerta else None,
            'nombre_consultado': nombre_consultado
        }
        
        # Preparar resultados en formato esperado por el generador
        results = {
            'funcion_judicial': resultado
        }
        
        # Generar DOCX usando el generador existente
        ruta_reporte = build_report_docx(
            job_id=proceso.job_id,
            meta=meta,
            results=results
        )
        
        if not ruta_reporte or not os.path.exists(ruta_reporte):
            log(f"⚠️ No se generó el archivo de reporte")
            return
        
        log(f"✅ Reporte DOCX generado: {ruta_reporte}")
        
        # Crear registro en de_reportes_rpa
        nombre_archivo = os.path.basename(ruta_reporte)
        tamano = os.path.getsize(ruta_reporte)
        
        nuevo_reporte = DeReporte(
            proceso_id=proceso.id,
            cliente_id=proceso.cliente_id,
            tipo_alerta=proceso.tipo_alerta,
            monto_usd=proceso.monto_usd,
            fecha_alerta=proceso.fecha_alerta,
            job_id=proceso.job_id,
            nombre_archivo=nombre_archivo,
            ruta_archivo=ruta_reporte,
            url_descarga=f"/api/tracking/reportes/{proceso.id}/download",
            tamano_bytes=tamano,
            tipo_archivo='DOCX',
            generado_exitosamente=True,
            data_snapshot={
                'resultado_scraper': resultado,
                'meta': meta
            },
            fecha_generacion=datetime.now()
        )
        
        db.add(nuevo_reporte)
        db.flush()
        
        log(f"✅ Reporte registrado en BD: ID {nuevo_reporte.id}, Archivo: {nombre_archivo}")
        
    except Exception as e:
        log(f"❌ Error generando reporte para proceso {proceso.id}: {e}")
        import traceback
        traceback.print_exc()


def _crear_proceso_para_cliente(cliente_id: int) -> Optional[int]:
    """
    Crea un proceso y consulta para Función Judicial.
    Retorna el proceso_id o None si hay error.
    """
    db = SessionLocal()
    try:
        # 1. Obtener cliente
        cliente = db.query(DeCliente).filter(DeCliente.id == cliente_id).first()
        if not cliente:
            log(f"⚠️ Cliente {cliente_id} no encontrado")
            return None
        
        # 2. Verificar que tenga nombre y apellido
        if not cliente.nombre or not cliente.apellido:
            log(f"⚠️ Cliente {cliente_id} sin nombre/apellido completo")
            _actualizar_cliente_estado(cliente_id, 'Error')
            return None
        
        # 3. Obtener página de Función Judicial
        pagina = db.query(DePagina).filter(
            DePagina.codigo == 'funcion_judicial',
            DePagina.activa == True
        ).first()
        
        if not pagina:
            log(f"❌ Página 'funcion_judicial' no encontrada en BD")
            return None
        
        # 4. Crear proceso
        import uuid
        job_id = f"daemon_{uuid.uuid4().hex[:12]}"
        
        nuevo_proceso = DeProceso(
            cliente_id=cliente_id,
            job_id=job_id,
            tipo_alerta=cliente.tipo,
            monto_usd=cliente.monto,
            fecha_alerta=cliente.fecha,
            estado='Pendiente',
            fecha_creacion=datetime.now(),
            headless=True,  # SIEMPRE HEADLESS en modo daemon
            generate_report=True,
            total_paginas_solicitadas=1,
            total_paginas_exitosas=0,
            total_paginas_fallidas=0
        )
        
        db.add(nuevo_proceso)
        db.flush()  # Para obtener el ID
        
        # 5. Crear consulta individual
        valor_busqueda = f"{cliente.apellido} {cliente.nombre}".strip()
        
        nueva_consulta = DeConsulta(
            proceso_id=nuevo_proceso.id,
            pagina_id=pagina.id,
            valor_enviado=valor_busqueda,
            estado='Pendiente',
            intentos_realizados=0,
            max_intentos=2
        )
        
        db.add(nueva_consulta)
        db.commit()
        
        log(f"✅ Proceso {nuevo_proceso.id} creado para cliente {cliente_id} (Job: {job_id})")
        return nuevo_proceso.id
        
    except Exception as e:
        log(f"❌ Error creando proceso para cliente {cliente_id}: {e}")
        db.rollback()
        return None
    finally:
        db.close()


def _ejecutar_consulta_funcion_judicial(proceso_id: int):
    """
    Ejecuta la consulta de Función Judicial y actualiza estados.
    AHORA CON: Generación de reportes y manejo de clientes sin resultados.
    """
    db = SessionLocal()
    try:
        # 1. Obtener proceso
        proceso = db.query(DeProceso).filter(DeProceso.id == proceso_id).first()
        if not proceso:
            log(f"⚠️ Proceso {proceso_id} no encontrado")
            return
        
        # 2. Marcar proceso como iniciado
        proceso.estado = 'En_Proceso'
        proceso.fecha_inicio = datetime.now()
        db.commit()
        
        # 3. Obtener consulta
        consulta = db.query(DeConsulta).filter(
            DeConsulta.proceso_id == proceso_id
        ).first()
        
        if not consulta:
            log(f"⚠️ Consulta no encontrada para proceso {proceso_id}")
            proceso.estado = 'Error_Total'
            proceso.mensaje_error_general = 'Consulta no encontrada'
            db.commit()
            return
        
        # 4. Marcar consulta como en proceso
        consulta.estado = 'En_Proceso'
        consulta.fecha_inicio = datetime.now()
        db.commit()
        
        # 5. Obtener datos del cliente y construir nombre completo
        cliente = db.query(DeCliente).filter(DeCliente.id == proceso.cliente_id).first()
        nombres = cliente.nombre if cliente else ""
        apellidos = cliente.apellido if cliente else ""
        
        # Combinar apellidos y nombres en un solo string
        apellidos_nombres = f"{apellidos} {nombres}".strip()
        
        log(f"🔄 Ejecutando Función Judicial para: {apellidos_nombres}")
        
        # 6. EJECUTAR SCRAPER REAL
        resultado = None
        try:
            resultado = process_funcion_judicial_once(
                apellidos_nombres,  # Parámetro posicional (sin nombre)
                headless=True  # Siempre headless en daemon
            )
            
            # 7. Analizar resultado y decidir acción
            if resultado:
                scenario = resultado.get('scenario', 'unknown')
                total_pages = resultado.get('total_pages', 0)
                screenshots = resultado.get('screenshots', [])
                
                log(f"📊 Resultado - Escenario: {scenario}, Páginas: {total_pages}")
                
                # DECISIÓN: ¿Hay resultados válidos?
                hay_resultados = (
                    scenario == 'results_found' and 
                    total_pages > 0 and 
                    len(screenshots) > 0
                )
                
                if hay_resultados:
                    # ✅ CASO 1: HAY RESULTADOS - Procesar normalmente
                    log(f"✅ Resultados encontrados - Procesando...")
                    
                    consulta.estado = 'Exitosa'
                    consulta.screenshot_path = screenshots[0] if screenshots else None
                    consulta.datos_capturados = resultado.get('data', {})
                    consulta.escenario = scenario
                    
                    proceso.total_paginas_exitosas = 1
                    proceso.total_paginas_fallidas = 0
                    proceso.estado = 'Completado'
                    
                    # Guardar todos los screenshots en datos_capturados
                    consulta.datos_capturados = {
                        'scenario': scenario,
                        'total_pages': total_pages,
                        'screenshots': screenshots,
                        'mensaje': resultado.get('mensaje', '')
                    }
                    
                    # 8. GENERAR REPORTE DOCX
                    _generar_reporte_para_proceso(db, proceso, resultado, apellidos_nombres)
                    
                    # Cliente → Procesado
                    _actualizar_cliente_estado(proceso.cliente_id, 'Procesado')
                    
                else:
                    # ⚠️ CASO 2: SIN RESULTADOS o INDETERMINADO - Volver a Pendiente
                    log(f"⚠️ Sin resultados válidos (escenario: {scenario}) - Cliente vuelve a Pendiente")
                    
                    consulta.estado = 'Fallida'
                    consulta.mensaje_error = f'Sin resultados válidos - Escenario: {scenario}'
                    consulta.datos_capturados = resultado.get('data', {})
                    consulta.escenario = scenario
                    
                    # Si hay al menos un screenshot, guardarlo como referencia
                    if screenshots:
                        consulta.screenshot_path = screenshots[0]
                    
                    proceso.total_paginas_exitosas = 0
                    proceso.total_paginas_fallidas = 1
                    proceso.estado = 'Completado_Con_Errores'
                    proceso.mensaje_error_general = f'Sin resultados - Escenario: {scenario}'
                    
                    # Cliente vuelve a Pendiente para reintento
                    _actualizar_cliente_estado(proceso.cliente_id, 'Pendiente')
                    
                    log(f"🔄 Cliente {proceso.cliente_id} devuelto a estado Pendiente para reintento")
                
            else:
                # SIN RESULTADO DEL SCRAPER
                log(f"⚠️ Scraper no retornó resultados")
                consulta.estado = 'Fallida'
                consulta.mensaje_error = 'Scraper no retornó resultados'
                
                proceso.total_paginas_exitosas = 0
                proceso.total_paginas_fallidas = 1
                proceso.estado = 'Completado_Con_Errores'
                
                # Cliente vuelve a Pendiente
                _actualizar_cliente_estado(proceso.cliente_id, 'Pendiente')
                
        except Exception as e:
            # ERROR EN SCRAPER
            log(f"❌ Error en scraper: {e}")
            consulta.estado = 'Fallida'
            consulta.mensaje_error = f"Error en scraper: {str(e)}"
            
            proceso.total_paginas_exitosas = 0
            proceso.total_paginas_fallidas = 1
            proceso.estado = 'Error_Total'
            proceso.mensaje_error_general = str(e)
            
            # Cliente vuelve a Pendiente en caso de error
            _actualizar_cliente_estado(proceso.cliente_id, 'Pendiente')
        
        # 9. Finalizar tiempos
        consulta.fecha_fin = datetime.now()
        if consulta.fecha_inicio:
            duracion = (consulta.fecha_fin - consulta.fecha_inicio).total_seconds()
            consulta.duracion_segundos = int(duracion)
        
        proceso.fecha_fin = datetime.now()
        
        db.commit()
        
        log(f"✅ Proceso {proceso_id} finalizado - Estado: {proceso.estado}")
        
    except Exception as e:
        log(f"❌ Error ejecutando consulta para proceso {proceso_id}: {e}")
        db.rollback()
    finally:
        db.close()


# ===== FUNCIONES PRINCIPALES DEL DAEMON =====

def _obtener_clientes_pendientes(limite: int = 5) -> List[DeCliente]:
    """
    Obtiene hasta 'limite' clientes en estado Pendiente, ordenados por fecha_creacion.
    """
    db = SessionLocal()
    try:
        clientes = db.query(DeCliente).filter(
            DeCliente.estado == 'Pendiente'
        ).order_by(
            DeCliente.fecha_creacion.asc()
        ).limit(limite).all()
        
        # Detach para usarlos fuera de la sesión
        db.expunge_all()
        return clientes
        
    finally:
        db.close()


def _procesar_lote_clientes():
    """
    Procesa un lote de hasta 5 clientes pendientes.
    """
    with daemon_state.lock:
        daemon_state.ultimo_lote_inicio = datetime.now()
        daemon_state.clientes_procesados_en_lote = 0
    
    log("🔍 Buscando clientes pendientes...")
    
    # 1. Obtener clientes
    clientes = _obtener_clientes_pendientes(limite=5)
    
    if not clientes:
        log("📭 No hay clientes pendientes. Esperando 30 minutos...")
        return
    
    log(f"📋 {len(clientes)} clientes pendientes encontrados")
    
    # 2. Procesar cada cliente
    for i, cliente in enumerate(clientes, 1):
        # Verificar si se detuvo el daemon
        if not daemon_state.running:
            log("⛔ Daemon detenido durante procesamiento")
            return
        
        log(f"\n{'='*60}")
        log(f"🔄 Procesando cliente {i}/{len(clientes)}")
        log(f"   ID: {cliente.id}")
        log(f"   Nombre: {cliente.apellido} {cliente.nombre}")
        log(f"   CI: {cliente.ci}")
        log(f"={'='*60}\n")
        
        # 2.1 Actualizar cliente a Procesando
        _actualizar_cliente_estado(cliente.id, 'Procesando')
        
        # 2.2 Crear proceso
        proceso_id = _crear_proceso_para_cliente(cliente.id)
        
        if not proceso_id:
            log(f"❌ No se pudo crear proceso para cliente {cliente.id}")
            _actualizar_cliente_estado(cliente.id, 'Error')
            continue
        
        # 2.3 Ejecutar consulta
        _ejecutar_consulta_funcion_judicial(proceso_id)
        
        # 2.4 Incrementar contador
        with daemon_state.lock:
            daemon_state.clientes_procesados_en_lote += 1
        
        log(f"✅ Cliente {cliente.id} procesado ({i}/{len(clientes)})")
    
    log(f"\n🎉 Lote completado - {len(clientes)} clientes procesados")


def _daemon_loop():
    """
    Loop principal del daemon.
    Procesa lotes y espera 30 minutos entre cada uno.
    """
    log("🚀 Daemon iniciado - Loop principal en ejecución")
    
    while daemon_state.running:
        try:
            # 1. Procesar lote
            _procesar_lote_clientes()
            
            # 2. Esperar 30 minutos (1800 segundos)
            # Dividimos en intervalos pequeños para poder detener rápido
            log("⏳ Esperando 30 minutos antes del próximo lote...")
            
            wait_time = 30 * 60  # 30 minutos en segundos
            interval = 10  # Verificar cada 10 segundos si se detuvo
            elapsed = 0
            
            while elapsed < wait_time and daemon_state.running:
                time.sleep(interval)
                elapsed += interval
                
                # Log cada 5 minutos
                if elapsed % 300 == 0:
                    remaining = (wait_time - elapsed) // 60
                    log(f"⏱️  Faltan {remaining} minutos para el próximo lote")
            
            if not daemon_state.running:
                log("⛔ Daemon detenido durante espera")
                break
                
        except Exception as e:
            log(f"❌ Error en daemon loop: {e}")
            import traceback
            traceback.print_exc()
            
            # Esperar 1 minuto antes de reintentar en caso de error
            time.sleep(60)
    
    log("🛑 Daemon detenido - Loop finalizado")


# ===== FUNCIONES DE CONTROL =====

def iniciar_daemon() -> dict:
    """
    Inicia el daemon en un thread separado.
    Retorna estado actual.
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
    Retorna estado actual.
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
            "clientes_procesados_en_lote": daemon_state.clientes_procesados_en_lote,
            "ultimo_lote_inicio": daemon_state.ultimo_lote_inicio.isoformat() if daemon_state.ultimo_lote_inicio else None
        }