# app/services/fj_httpx_fallback.py
"""
Fallback para consultas de Función Judicial usando API directa (HTTPX).
Se usa cuando el web scraping falla (no hay screenshots de resultados).

Características:
- Consulta hasta 20 páginas
- Genera DOCX con tablas formateadas
- Convierte fechas UTC → Ecuador (UTC-5)
- Guarda en sri_ruc_output\reports\
"""

import httpx
from docx import Document
from datetime import datetime, timedelta, timezone
import os
from typing import Optional, List, Dict, Any
import traceback

# ===== CONFIGURACIÓN =====
API_BASE_URL = "https://api.funcionjudicial.gob.ec/EXPEL-CONSULTA-CAUSAS-SERVICE"
PAGE_SIZE = 10
MAX_PAGES = 20
REPORTS_DIR = "sri_ruc_output/reports"

# Crear directorio si no existe
os.makedirs(REPORTS_DIR, exist_ok=True)


def log(msg: str):
    """Logging con timestamp para HTTPX fallback"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[HTTPX FALLBACK {timestamp}] {msg}")


def _convertir_fecha_utc_a_ecuador(fecha_str: str) -> str:
    """
    Convierte fecha UTC a hora de Ecuador (UTC-5).
    
    Args:
        fecha_str: Fecha en formato ISO (ej: "2025-11-17T00:00:00")
        
    Returns:
        Fecha formateada como dd/mm/yyyy
    """
    try:
        if not fecha_str:
            return "N/A"
        
        # Parsear fecha
        if "T" in fecha_str:
            dt_utc = datetime.fromisoformat(fecha_str.replace("Z", "+00:00"))
        else:
            dt_utc = datetime.fromisoformat(fecha_str)
        
        # Asegurar que tenga timezone UTC
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        
        # Convertir a Ecuador (UTC-5)
        dt_ec = dt_utc - timedelta(hours=5)
        
        # Retornar formateado
        return dt_ec.strftime("%d/%m/%Y")
    except Exception as e:
        log(f"⚠️ Error convirtiendo fecha '{fecha_str}': {e}")
        return fecha_str[:10] if len(fecha_str) >= 10 else "N/A"


def _consultar_pagina_api(nombre_buscado: str, page: int) -> Optional[List[Dict[str, Any]]]:
    """
    Consulta una página de la API de Función Judicial.
    
    Args:
        nombre_buscado: Nombre del demandado a buscar
        page: Número de página (1-based)
        
    Returns:
        Lista de procesos encontrados o None si error
    """
    try:
        url = (
            f"{API_BASE_URL}/"
            f"api/consulta-causas/informacion/buscarCausas"
            f"?page={page}&size={PAGE_SIZE}"
        )
        
        payload = {
            "numeroCausa": "",
            "actor": {
                "cedulaActor": "",
                "nombreActor": ""
            },
            "demandado": {
                "cedulaDemandado": "",
                "nombreDemandado": nombre_buscado
            },
            "provincia": "",
            "numeroFiscalia": "",
            "recaptcha": "",
            "first": page,
            "pageSize": PAGE_SIZE
        }
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        # Usar httpx con timeout
        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, json=payload, headers=headers)
        
        if response.status_code != 200:
            log(f"⚠️ API retornó status {response.status_code}")
            return None
        
        # Parsear respuesta
        data = response.json()
        
        # La API puede retornar dict o list
        if isinstance(data, dict):
            resultados = data.get("data", [])
        elif isinstance(data, list):
            resultados = data
        else:
            return None
        
        return resultados if resultados else None
        
    except httpx.TimeoutException:
        log(f"⚠️ Timeout consultando página {page}")
        return None
    except Exception as e:
        log(f"⚠️ Error consultando API página {page}: {e}")
        return None


def generar_reporte_httpx(
    nombre_cliente: str,
    job_id: str
) -> Optional[str]:
    """
    Genera reporte DOCX consultando API de Función Judicial directamente.
    Se usa como fallback cuando el web scraping falla.
    
    Args:
        nombre_cliente: Nombre completo del cliente (ej: "PAMELA ALEXANDRA CASTRO DEL POZO")
        job_id: ID único del proceso (ej: "daemon_8d8c1f044264")
        
    Returns:
        Ruta del archivo DOCX generado o None si falla
    """
    try:
        log(f"🌐 Iniciando consulta API para: {nombre_cliente}")
        
        # 1. Crear documento
        doc = Document()
        doc.add_heading(f"Procesos judiciales de: {nombre_cliente}", level=1)
        doc.add_paragraph(f"Fecha de consulta: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        doc.add_paragraph("")  # Espacio
        
        # 2. Recorrer páginas
        contador = 1
        total_resultados = 0
        alguna_pagina_con_datos = False
        pagina_actual = 1
        
        for page in range(1, MAX_PAGES + 1):
            log(f"📄 Consultando página {page}...")
            
            resultados = _consultar_pagina_api(nombre_cliente, page)
            
            if not resultados:
                log(f"📭 Página {page} sin resultados, finalizando")
                break
            
            alguna_pagina_con_datos = True
            pagina_actual = page
            total_resultados += len(resultados)
            
            # 3. Agregar encabezado de página
            doc.add_heading(f"Página {page}", level=2)
            
            # 4. Crear tabla con 5 columnas
            table = doc.add_table(rows=1, cols=5)
            table.style = "Table Grid"
            
            # Encabezados
            hdr_cells = table.rows[0].cells
            hdr_cells[0].text = "No."
            hdr_cells[1].text = "Fecha de ingreso"
            hdr_cells[2].text = "No. proceso"
            hdr_cells[3].text = "Acción / Infracción"
            hdr_cells[4].text = "Movimientos del Proceso"
            
            # 5. Llenar tabla con datos
            for caso in resultados:
                row = table.add_row().cells
                
                # Convertir fecha
                fecha_api = caso.get("fechaIngreso", "")
                fecha_formato = _convertir_fecha_utc_a_ecuador(fecha_api) if fecha_api else "N/A"
                
                # Llenar celdas
                row[0].text = str(contador)
                row[1].text = fecha_formato
                row[2].text = str(caso.get("idJuicio", "N/A"))
                row[3].text = str(caso.get("nombreDelito", "N/A"))
                row[4].text = "Movimientos del Proceso"  # Columna fija para todos
                
                contador += 1
            
            # Espacio entre páginas
            doc.add_paragraph("")
        
        # 6. Validar que se obtuvieron resultados
        if not alguna_pagina_con_datos:
            log(f"❌ No se obtuvieron resultados de la API")
            return None
        
        # 7. Resumen final
        doc.add_heading("Resumen", level=2)
        doc.add_paragraph(f"Total de procesos encontrados: {total_resultados}")
        doc.add_paragraph(f"Páginas consultadas: {pagina_actual}")
        
        # 8. Guardar documento con nombre: {nombre_cliente}_{job_id}.docx
        nombre_archivo = f"{nombre_cliente.replace(' ', '_')}_{job_id}.docx"
        ruta_completa = os.path.join(REPORTS_DIR, nombre_archivo)
        
        doc.save(ruta_completa)
        log(f"✅ Reporte DOCX generado: {ruta_completa}")
        log(f"   - Total procesos: {total_resultados}")
        log(f"   - Páginas: {pagina_actual}")
        
        return ruta_completa
        
    except Exception as e:
        log(f"❌ Error generando reporte HTTPX: {e}")
        traceback.print_exc()
        return None