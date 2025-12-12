# app/services/scheduler_sincronizacion.py
"""
Scheduler para ejecutar sincronización automática diariamente a las 7 AM (Quito)
Usa APScheduler
"""

from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.services.sincronizacion_db2_v2 import sincronizar_ahora
from core.utils.log import log

# Variable global para controlar el scheduler
_scheduler = None
_scheduler_running = False


def _tarea_sincronizacion_diaria():
    """
    Tarea que se ejecuta automáticamente
    Sincroniza el día anterior cada día a las 7 AM Quito
    """
    try:
        # Calcular rango: día anterior (00:00 a 23:59)
        hoy = datetime.now().date()
        ayer = hoy - timedelta(days=1)
        
        fecha_desde = ayer.strftime("%Y-%m-%d")
        fecha_hasta = ayer.strftime("%Y-%m-%d")
        
        log(f" [SCHEDULER] Iniciando sincronización automática del {fecha_desde}")
        
        exito, resultado = sincronizar_ahora(fecha_desde, fecha_hasta)
        
        if exito:
            log(f" [SCHEDULER] Sincronización exitosa - {resultado['registros_insertados']} registros insertados")
        else:
            log(f"❌ [SCHEDULER] Sincronización fallida - {resultado['mensaje']}")
        
        return resultado
        
    except Exception as e:
        log(f"❌ [SCHEDULER] Error en sincronización automática: {str(e)}")
        return {"estado": "ERROR", "mensaje": str(e)}


def inicializar_scheduler():
    """
    Inicializa el scheduler APScheduler
    Ejecuta sincronización a las 7:00 AM zona horaria Quito (UTC-5)
    """
    global _scheduler, _scheduler_running
    
    try:
        if _scheduler is not None and _scheduler.running:
            log("⚠️  [SCHEDULER] Ya está en ejecución")
            return True
        
        # Crear scheduler
        _scheduler = BackgroundScheduler()
        
        # Configurar trigger CRON para 7:00 AM
        # hour=7, minute=0 = 7:00 AM todos los días
        # timezone='America/Guayaquil' = zona horaria de Ecuador
        trigger = CronTrigger(
            hour=7,
            minute=0,
            timezone='America/Guayaquil'
        )
        
        # Agregar job
        _scheduler.add_job(
            func=_tarea_sincronizacion_diaria,
            trigger=trigger,
            id='sincronizacion_diaria',
            name='Sincronización diaria DB2→SQLServer',
            replace_existing=True,
            misfire_grace_time=60  # Si se pierde, ejecutar en los próximos 60s
        )
        
        # Iniciar scheduler
        _scheduler.start()
        _scheduler_running = True
        
        log("✅ [SCHEDULER] Inicializado - Sincronización cada día a las 7:00 AM (Quito)")
        return True
        
    except Exception as e:
        log(f"❌ [SCHEDULER] Error inicializando: {str(e)}")
        return False


def detener_scheduler():
    """Detiene el scheduler"""
    global _scheduler, _scheduler_running
    
    try:
        if _scheduler and _scheduler.running:
            _scheduler.shutdown()
            _scheduler_running = False
            log("✅ [SCHEDULER] Detenido")
            return True
        return False
    except Exception as e:
        log(f"❌ [SCHEDULER] Error deteniendo: {str(e)}")
        return False


def obtener_estado_scheduler():
    """Retorna estado del scheduler"""
    return {
        "running": _scheduler_running and _scheduler is not None and _scheduler.running,
        "scheduler": str(_scheduler),
        "jobs": [
            {
                "id": job.id,
                "name": job.name,
                "trigger": str(job.trigger),
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None
            }
            for job in (_scheduler.get_jobs() if _scheduler else [])
        ]
    }


def ejecutar_sincronizacion_manual(fecha_desde: str, fecha_hasta: str):
    """
    Ejecuta sincronización manualmente
    No interfiere con el scheduler automático
    """
    try:
        log(f"🔄 [MANUAL] Sincronización manual: {fecha_desde} a {fecha_hasta}")
        exito, resultado = sincronizar_ahora(fecha_desde, fecha_hasta)
        
        if exito:
            log(f"✅ [MANUAL] Éxito - {resultado['registros_insertados']} registros")
        else:
            log(f"❌ [MANUAL] Error - {resultado['mensaje']}")
        
        return exito, resultado
        
    except Exception as e:
        log(f"❌ [MANUAL] Excepción: {str(e)}")
        return False, {"estado": "ERROR", "mensaje": str(e)}