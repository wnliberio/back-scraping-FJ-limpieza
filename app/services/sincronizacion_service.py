# app/services/sincronizacion_service.py
"""
Servicio de Sincronización CON GENERADOR REAL DE REPORTES
Conecta con tu report_store.py existente
"""

from typing import Optional, Dict, Any
from datetime import datetime
import os
import json
from pathlib import Path

# Usar la importación correcta según tu estructura
# Al inicio del archivo, solo esto:
from app.db import SessionLocal
print("✅ Usando SessionLocal desde app.db")

from app.db.models_new import (
    DeCliente, DeProceso, DeConsulta, DePagina, DeReporte
)

# Importar tu generador de reportes existente
from app.services.report_store import generate_and_persist_report

class SincronizadorConReportes:
    """Sincronizador que usa el generador real de reportes"""
    
    def __init__(self):
        self.directorio_reportes = self._obtener_directorio_reportes()
        self._asegurar_directorio_existe()
    
    def _obtener_directorio_reportes(self) -> str:
        """Detecta directorio de reportes usando la misma lógica del sistema actual"""
        posibles_rutas = [
            "./sri_ruc_output/reports",
            "./reportes",
            "./reports", 
            os.path.join(os.getcwd(), "sri_ruc_output", "reports")
        ]
        
        for ruta in posibles_rutas:
            if os.path.exists(ruta):
                return os.path.abspath(ruta)
        
        # Crear usando la misma estructura que tu sistema actual
        ruta_default = os.path.join(os.getcwd(), "sri_ruc_output", "reports")
        return ruta_default
    
    def _asegurar_directorio_existe(self):
        """Crea directorio si no existe"""
        Path(self.directorio_reportes).mkdir(parents=True, exist_ok=True)
    
    async def sincronizar_job_terminado(self, job_id: str, resultado: Dict[str, Any]) -> bool:
        """
        Sincroniza cuando un job termina y genera reporte real
        """
        db = SessionLocal()
        try:
            print(f"🔄 Sincronizando job terminado: {job_id}")
            
            # 1. Buscar proceso por job_id
            proceso = db.query(DeProceso).filter(DeProceso.job_id == job_id).first()
            if not proceso:
                print(f"⚠️ No se encontró proceso para job_id: {job_id}")
                return False
            
            print(f"📋 Proceso encontrado: ID {proceso.id}, Cliente {proceso.cliente_id}")
            
            # 2. Analizar resultados y actualizar consultas individuales
            exito_total, resumen_paginas = self._analizar_y_actualizar_consultas(db, proceso, resultado)
            
            # 3. Actualizar estado del proceso
            self._actualizar_estado_proceso(db, proceso, exito_total)
            
            # 4. Actualizar estado del cliente
            cliente = db.query(DeCliente).filter(DeCliente.id == proceso.cliente_id).first()
            if cliente:
                cliente.estado = 'Procesado'
                print(f"✅ Cliente {cliente.id} marcado como Procesado")
            
            # 5. Generar reporte REAL usando tu sistema existente
            if proceso.generate_report:
                await self._generar_reporte_real(db, proceso, resultado, resumen_paginas)
            
            db.commit()
            print(f"✅ Sincronización completa para job {job_id}")
            return True
            
        except Exception as e:
            db.rollback()
            print(f"❌ Error sincronizando job {job_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            db.close()
    
    def _analizar_y_actualizar_consultas(self, db, proceso: DeProceso, resultado: Dict[str, Any]) -> tuple:
        """
        Analiza resultados y actualiza consultas individuales.
        Retorna (exito_total, resumen_paginas)
        """
        consultas = db.query(DeConsulta).filter(
            DeConsulta.proceso_id == proceso.id
        ).all()
        
        exitos = 0
        fallidas = 0
        total = len(consultas)
        resumen_paginas = {
            'exitosas': [],
            'fallidas': [],
            'total_solicitadas': total
        }
        
        # Obtener datos del resultado
        datos_resultado = resultado.get('data', {})
        if 'results' in datos_resultado:
            datos_resultado = datos_resultado['results']
        
        for consulta in consultas:
            # Obtener página correspondiente
            pagina = db.query(DePagina).filter(DePagina.id == consulta.pagina_id).first()
            if not pagina:
                continue
            
            # Buscar resultado específico para esta página
            resultado_pagina = datos_resultado.get(pagina.codigo, {})
            
            if resultado_pagina and isinstance(resultado_pagina, dict):
                # Evaluar si fue exitosa basado en el contenido
                fue_exitosa = self._evaluar_exito_consulta(resultado_pagina)
                
                # Actualizar consulta
                consulta.estado = 'Exitosa' if fue_exitosa else 'Fallida'
                consulta.fecha_fin = datetime.now()
                consulta.datos_capturados = resultado_pagina
                
                # Extraer información de screenshots
                if 'screenshot_path' in resultado_pagina:
                    consulta.screenshot_path = resultado_pagina['screenshot_path']
                if 'screenshot_historial_path' in resultado_pagina:
                    consulta.screenshot_historial_path = resultado_pagina['screenshot_historial_path']
                
                # Duración si hay fecha_inicio
                if consulta.fecha_inicio:
                    duracion = (consulta.fecha_fin - consulta.fecha_inicio).total_seconds()
                    consulta.duracion_segundos = int(duracion)
                
                if fue_exitosa:
                    exitos += 1
                    resumen_paginas['exitosas'].append({
                        'codigo': pagina.codigo,
                        'nombre': pagina.nombre,
                        'screenshot': resultado_pagina.get('screenshot_path'),
                        'escenario': resultado_pagina.get('scenario', 'Consulta exitosa')
                    })
                else:
                    fallidas += 1
                    error_msg = resultado_pagina.get('error', 'Error no especificado')
                    consulta.mensaje_error = error_msg
                    resumen_paginas['fallidas'].append({
                        'codigo': pagina.codigo,
                        'nombre': pagina.nombre,
                        'error': error_msg
                    })
            else:
                # Sin resultado para esta página
                consulta.estado = 'Fallida'
                consulta.fecha_fin = datetime.now()
                consulta.mensaje_error = 'No se encontró resultado para esta página'
                fallidas += 1
                resumen_paginas['fallidas'].append({
                    'codigo': pagina.codigo,
                    'nombre': pagina.nombre,
                    'error': 'Sin resultado'
                })
        
        # Actualizar contadores en el proceso
        proceso.total_paginas_exitosas = exitos
        proceso.total_paginas_fallidas = fallidas
        
        return exitos == total, resumen_paginas
    
    def _evaluar_exito_consulta(self, resultado_pagina: Dict[str, Any]) -> bool:
        """Evalúa si una consulta fue exitosa basado en su resultado"""
        # Si hay error explícito, no fue exitosa
        if 'error' in resultado_pagina:
            return False
        
        # Si el escenario indica error
        scenario = resultado_pagina.get('scenario', '').lower()
        if 'error' in scenario or 'fallo' in scenario or 'fail' in scenario:
            return False
        
        # Si hay screenshot, generalmente indica éxito
        if 'screenshot_path' in resultado_pagina:
            screenshot_path = resultado_pagina['screenshot_path']
            if screenshot_path and os.path.exists(screenshot_path):
                return True
        
        # Si hay datos relevantes, considerarlo exitoso
        datos_relevantes = ['data', 'resultado', 'informacion', 'contenido', 'screenshot_path']
        return any(key in resultado_pagina for key in datos_relevantes)
    
    def _actualizar_estado_proceso(self, db, proceso: DeProceso, exito_total: bool):
        """Actualiza estado del proceso"""
        proceso.fecha_fin = datetime.now()
        
        if exito_total:
            proceso.estado = 'Completado'
        elif proceso.total_paginas_exitosas > 0:
            proceso.estado = 'Completado_Con_Errores'
        else:
            proceso.estado = 'Error_Total'
    
    async def _generar_reporte_real(
        self, 
        db, 
        proceso: DeProceso, 
        resultado: Dict[str, Any],
        resumen_paginas: Dict[str, Any]
    ):
        """
        Genera reporte real usando tu sistema existente (report_store.py)
        """
        try:
            print(f"📝 Generando reporte real para proceso {proceso.id}")
            
            # Preparar metadata para el generador
            meta = {
                'tipo_alerta': proceso.tipo_alerta or 'Consulta General',
                'monto_usd': float(proceso.monto_usd) if proceso.monto_usd else None,
                'fecha_alerta': proceso.fecha_alerta.isoformat() if proceso.fecha_alerta else None
            }
            
            # Preparar resultados en el formato que espera tu generador
            resultados_formateados = resultado.get('data', {})
            if 'results' in resultados_formateados:
                resultados_formateados = resultados_formateados['results']
            
            # Llamar a tu generador existente
            reporte_info = generate_and_persist_report(
                job_id=proceso.job_id,
                results=resultados_formateados,
                meta=meta
            )
            
            print(f"📄 Reporte generado: {reporte_info}")
            
            # Crear registro en de_reportes_rpa para el nuevo sistema
            if reporte_info and 'report_path' in reporte_info:
                ruta_reporte = reporte_info['report_path']
                nombre_archivo = os.path.basename(ruta_reporte)
                
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
                    tamano_bytes=os.path.getsize(ruta_reporte) if os.path.exists(ruta_reporte) else None,
                    tipo_archivo='DOCX',
                    generado_exitosamente=os.path.exists(ruta_reporte),
                    data_snapshot={
                        'resultado_original': resultado,
                        'resumen_paginas': resumen_paginas,
                        'reporte_info': reporte_info
                    },
                    fecha_generacion=datetime.now()
                )
                
                db.add(nuevo_reporte)
                db.flush()
                
                print(f"✅ Registro de reporte creado en de_reportes_rpa: ID {nuevo_reporte.id}")
                
        except Exception as e:
            print(f"❌ Error generando reporte real: {str(e)}")
            import traceback
            traceback.print_exc()

# Instancia global con generador real
sincronizador = SincronizadorConReportes()

# Función de conveniencia para usar desde routers
async def sincronizar_job_completado(job_id: str, resultado: Dict[str, Any]) -> bool:
    """Función principal para sincronizar job completado CON REPORTE REAL"""
    return await sincronizador.sincronizar_job_terminado(job_id, resultado)

async def actualizar_cliente_estado(cliente_id: int, estado: str) -> bool:
    """Actualiza estado de cliente directamente"""
    db = SessionLocal()
    try:
        cliente = db.query(DeCliente).filter(DeCliente.id == cliente_id).first()
        if cliente:
            cliente.estado = estado
            db.commit()
            print(f"✅ Cliente {cliente_id} actualizado a estado: {estado}")
            return True
        print(f"⚠️ Cliente {cliente_id} no encontrado")
        return False
    except Exception as e:
        db.rollback()
        print(f"❌ Error actualizando cliente {cliente_id}: {str(e)}")
        return False
    finally:
        db.close()