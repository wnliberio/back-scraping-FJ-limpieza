# app/services/sincronizacion_db2_v2.py
"""
Servicio de Sincronización DB2 → SQL Server (Tabla V2) - VERSIÓN COMPLETA
Usa conexión DIRECTA a DB2 (pyodbc, sin SQLAlchemy)
"""

from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, date

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.db.models import DeClienteV2, DeSincronizacionControl
from app.db.origen_db2 import obtener_clientes_db2


class SincronizadorDB2V2:
    """Sincronizador profesional: DB2 → de_clientes_rpa_v2"""
    
    def __init__(self):
        self.nombre_proceso = "carga_clientes_db2_v2"
        self.log_mensajes = []
    
    def _log(self, msg: str, nivel: str = "INFO"):
        """Logging con timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        nivel_str = f"[{nivel}]".ljust(8)
        linea = f"{timestamp} {nivel_str} {msg}"
        self.log_mensajes.append(linea)
        print(linea)
    
    def _convertir_date_a_datetime(self, valor):
        """Convierte date a datetime si es necesario"""
        if valor and isinstance(valor, date) and not isinstance(valor, datetime):
            return datetime.combine(valor, datetime.min.time())
        return valor
    
    def obtener_ultimo_numero_sincronizacion(self, db_destino: Session) -> int:
        """Obtiene el número de sincronización más reciente"""
        try:
            ultima = db_destino.query(DeSincronizacionControl).filter(
                DeSincronizacionControl.nombre_proceso == self.nombre_proceso
            ).order_by(DeSincronizacionControl.numero_sincronizacion.desc()).first()
            
            if ultima:
                return ultima.numero_sincronizacion + 1
            return 1
        except Exception as e:
            self._log(f"Error obteniendo número sincronización: {e}", "WARN")
            return 1
    
    def _crear_cliente_v2(self, datos_dict: Dict[str, Any]) -> DeClienteV2:
        """Crea objeto DeClienteV2 desde datos de DB2"""
        
        # ✅ Convertir DATE a DATETIME si es necesario
        fecha_creacion = self._convertir_date_a_datetime(datos_dict.get("FECHA_CREACION_SOLICITUD"))
        
        return DeClienteV2(
            ID_SOLICITUD=datos_dict.get("ID_SOLICITUD"),
            FECHA_CREACION_SOLICITUD=fecha_creacion,
            ESTADO=datos_dict.get("ESTADO"),
            AGENCIA=datos_dict.get("AGENCIA"),
            ID_PRODUCTO=datos_dict.get("ID_PRODUCTO"),
            PRODUCTO=datos_dict.get("PRODUCTO"),
            CEDULA=datos_dict.get("CEDULA"),
            NOMBRES_CLIENTE=datos_dict.get("NOMBRES_CLIENTE"),
            APELLIDOS_CLIENTE=datos_dict.get("APELLIDOS_CLIENTE"),
            ESTADO_CIVIL=datos_dict.get("ESTADO_CIVIL"),
            CEDULA_CONYUGE=datos_dict.get("CEDULA_CONYUGE"),
            NOMBRES_CONYUGE=datos_dict.get("NOMBRES_CONYUGE"),
            APELLIDOS_CONYUGE=datos_dict.get("APELLIDOS_CONYUGE"),
            CEDULA_CODEUDOR=datos_dict.get("CEDULA_CODEUDOR"),
            NOMBRES_CODEUDOR=datos_dict.get("NOMBRES_CODEUDOR"),
            APELLIDOS_CODEUDOR=datos_dict.get("APELLIDOS_CODEUDOR"),
            ESTADO_CONSULTA="Pendiente",
            FECHA_CREACION_REGISTRO=datetime.now()
        )
    
    def sincronizar_clientes_db2(
        self, 
        start_date: str, 
        end_date: str,
        db_destino: Optional[Session] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """EJECUTA LA SINCRONIZACIÓN COMPLETA"""
        
        if db_destino is None:
            db_destino = SessionLocal()
        
        fecha_hora_inicio = datetime.now()
        numero_sync = self.obtener_ultimo_numero_sincronizacion(db_destino)
        
        self._log(f"🔄 INICIANDO SINCRONIZACIÓN #{numero_sync}")
        self._log(f"   Período: {start_date} a {end_date}")
        
        resultado = {
            "numero_sincronizacion": numero_sync,
            "fecha_hora_inicio": fecha_hora_inicio,
            "fecha_hora_fin": None,
            "duracion_segundos": None,
            "registros_traidos": 0,
            "registros_insertados": 0,
            "registros_duplicados": 0,
            "registros_error": 0,
            "estado": "INICIALIZANDO",
            "mensaje": "",
            "fecha_minima_db2": None,
            "fecha_maxima_db2": None,
            "errores_detallados": []
        }
        
        try:
            self._log("🔗 Conectando a DB2...")
            self._log("📊 Ejecutando query en DB2...")
            
            registros_db2 = obtener_clientes_db2(start_date, end_date)
            resultado["registros_traidos"] = len(registros_db2)
            
            if not registros_db2:
                self._log("⚠️  No hay registros en el período especificado", "WARN")
                resultado["estado"] = "SUCCESS"
                resultado["mensaje"] = "Sin registros en el período"
            else:
                self._log(f"✅ {len(registros_db2)} registros traídos desde DB2")
                
                fechas_creacion = [r.get("FECHA_CREACION_SOLICITUD") for r in registros_db2 if r.get("FECHA_CREACION_SOLICITUD")]
                if fechas_creacion:
                    resultado["fecha_minima_db2"] = min(fechas_creacion)
                    resultado["fecha_maxima_db2"] = max(fechas_creacion)
                
                self._log("💾 Insertando registros en de_clientes_rpa_v2...")
                
                for idx, reg in enumerate(registros_db2, 1):
                    try:
                        cliente_v2 = self._crear_cliente_v2(reg)
                        db_destino.add(cliente_v2)
                        db_destino.flush()
                        resultado["registros_insertados"] += 1
                        
                    except Exception as e_insert:
                        if "UNIQUE" in str(e_insert) or "duplicate" in str(e_insert).lower():
                            resultado["registros_duplicados"] += 1
                            db_destino.rollback()
                        else:
                            resultado["registros_error"] += 1
                            resultado["errores_detallados"].append(
                                f"Registro {idx} (ID_SOLICITUD={reg.get('ID_SOLICITUD')}): {str(e_insert)}"
                            )
                            self._log(f"   ❌ Error en registro {idx}: {str(e_insert)}", "WARN")
                            db_destino.rollback()
                
                self._log(f"✅ Insertados: {resultado['registros_insertados']}, Duplicados: {resultado['registros_duplicados']}, Errores: {resultado['registros_error']}")
                
                db_destino.commit()
                self._log("✅ Cambios confirmados en BD destino")
                
                if resultado["registros_error"] == 0:
                    resultado["estado"] = "SUCCESS"
                    resultado["mensaje"] = f"{resultado['registros_insertados']} nuevos, {resultado['registros_duplicados']} duplicados"
                else:
                    resultado["estado"] = "PARTIAL"
                    resultado["mensaje"] = f"Errores en {resultado['registros_error']} registros"
            
        except Exception as e:
            self._log(f"❌ ERROR CRÍTICO: {str(e)}", "ERROR")
            db_destino.rollback()
            resultado["estado"] = "ERROR"
            resultado["mensaje"] = str(e)
            return False, resultado
            
        finally:
            fecha_hora_fin = datetime.now()
            duracion = (fecha_hora_fin - fecha_hora_inicio).total_seconds()
            resultado["fecha_hora_fin"] = fecha_hora_fin
            resultado["duracion_segundos"] = int(duracion)
            
            try:
                # ✅ CONVERTIR fechas minima/maxima a DATETIME si son DATE
                fecha_minima = self._convertir_date_a_datetime(resultado["fecha_minima_db2"])
                fecha_maxima = self._convertir_date_a_datetime(resultado["fecha_maxima_db2"])
                
                auditoria = DeSincronizacionControl(
                    nombre_proceso=self.nombre_proceso,
                    numero_sincronizacion=numero_sync,
                    fecha_hora_inicio=fecha_hora_inicio,
                    fecha_hora_fin=fecha_hora_fin,
                    duracion_segundos=int(duracion),
                    registros_traidos=resultado["registros_traidos"],
                    registros_insertados=resultado["registros_insertados"],
                    registros_duplicados=resultado["registros_duplicados"],
                    registros_error=resultado["registros_error"],
                    estado=resultado["estado"],
                    mensaje_resultado=resultado["mensaje"],
                    fecha_minima_db2=fecha_minima,
                    fecha_maxima_db2=fecha_maxima,
                    fecha_creacion=fecha_hora_fin
                )
                
                db_destino.add(auditoria)
                db_destino.commit()
                
                self._log(f"📋 Auditoría registrada (ID Sync: {numero_sync})")
                
            except Exception as e:
                self._log(f"⚠️  Error registrando auditoría: {str(e)}", "WARN")
            
            if db_destino:
                db_destino.close()
            
            self._log(f"✅ SINCRONIZACIÓN #{numero_sync} FINALIZADA")
            self._log(f"   ⏱️  Duración: {duracion:.2f} segundos")
            self._log(f"   📊 Resultado: {resultado['estado']}")
        
        exito = resultado["estado"] in ["SUCCESS", "PARTIAL"]
        return exito, resultado


def sincronizar_ahora(start_date: str, end_date: str) -> Tuple[bool, Dict[str, Any]]:
    """Función simple para ejecutar sincronización desde cualquier lado"""
    sincronizador = SincronizadorDB2V2()
    return sincronizador.sincronizar_clientes_db2(start_date, end_date)


def obtener_logs_ultimas_sincronizaciones(cantidad: int = 5) -> List[Dict[str, Any]]:
    """Obtiene los últimas N sincronizaciones registradas en auditoría"""
    db = SessionLocal()
    try:
        registros = db.query(DeSincronizacionControl).filter(
            DeSincronizacionControl.nombre_proceso == "carga_clientes_db2_v2"
        ).order_by(DeSincronizacionControl.numero_sincronizacion.desc()).limit(cantidad).all()
        
        resultado = []
        for reg in registros:
            resultado.append({
                "numero_sincronizacion": reg.numero_sincronizacion,
                "fecha_hora_inicio": reg.fecha_hora_inicio.isoformat(),
                "fecha_hora_fin": reg.fecha_hora_fin.isoformat() if reg.fecha_hora_fin else None,
                "duracion_segundos": reg.duracion_segundos,
                "registros_traidos": reg.registros_traidos,
                "registros_insertados": reg.registros_insertados,
                "registros_duplicados": reg.registros_duplicados,
                "registros_error": reg.registros_error,
                "estado": reg.estado,
                "mensaje_resultado": reg.mensaje_resultado
            })
        
        return resultado
    finally:
        db.close()