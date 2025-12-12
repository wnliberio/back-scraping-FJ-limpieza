# app/services/detectores_consulta.py
"""
Detectores para distinguir entre:
1. "La consulta no devolvió resultados." (SCRAPING) → Sin Procesos Judiciales
2. "Página 1 sin resultados" (HTTPX) → Sin Procesos Judiciales
3. Errores reales → Error en consulta
"""

import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def detectar_sin_procesos_judiciales_scraping(driver, timeout: int = 5) -> bool:
    """
    Detecta el modal "La consulta no devolvió resultados."
    
    El modal aparece durante 2-3 segundos en el HTML como:
    <div class="cdk-overlay-container">
        <mat-snack-bar-container>
            <simple-snack-bar>
                "La consulta no devolvió resultados."
    
    Args:
        driver: Selenium WebDriver
        timeout: Segundos a esperar (máx 5 segundos)
    
    Returns:
        True si detectó "La consulta no devolvió resultados."
        False si no hay resultados sin procesos judiciales
    """
    print("[DETECTOR] 🔍 Buscando modal 'La consulta no devolvió resultados.'")
    
    try:
        # Estrategia 1: Buscar el texto exacto en el DOM
        xpaths_exactos = [
            "//*[contains(text(), 'La consulta no devolvió resultados.')]",
            "//div[contains(text(), 'La consulta no devolvió resultados.')]",
            "//simple-snack-bar//div[contains(text(), 'La consulta no devolvió resultados.')]",
            "//mat-snack-bar-container//div[contains(@class, 'mdc-snackbar__label')][contains(text(), 'La consulta no devolvió resultados.')]",
            "//*[@aria-live='assertive']//div[contains(text(), 'La consulta no devolvió resultados.')]"
        ]
        
        for xpath in xpaths_exactos:
            try:
                elementos = driver.find_elements(By.XPATH, xpath)
                if elementos:
                    for elem in elementos:
                        if elem.is_displayed():
                            texto = elem.text.strip()
                            if "La consulta no devolvió resultados." in texto:
                                print(f"[DETECTOR] ✅ ENCONTRADO: '{texto}'")
                                return True
            except:
                continue
        
        # Estrategia 2: Buscar en el contenedor de overlay
        try:
            overlay_container = driver.find_element(By.CSS_SELECTOR, "div.cdk-overlay-container")
            if overlay_container:
                # Esperar un momento a que se renderice
                time.sleep(0.5)
                
                # Obtener todo el texto del contenedor
                text_content = overlay_container.text
                if "La consulta no devolvió resultados." in text_content:
                    print(f"[DETECTOR] ✅ ENCONTRADO en overlay: 'La consulta no devolvió resultados.'")
                    return True
        except:
            pass
        
        # Estrategia 3: Buscar con JavaScript
        try:
            result = driver.execute_script("""
                const elements = document.querySelectorAll('*');
                for (let elem of elements) {
                    if (elem.textContent && 
                        elem.textContent.includes('La consulta no devolvió resultados.') &&
                        elem.offsetHeight > 0) {
                        return true;
                    }
                }
                return false;
            """)
            
            if result:
                print("[DETECTOR] ✅ ENCONTRADO por JavaScript: 'La consulta no devolvió resultados.'")
                return True
        except:
            pass
        
        print("[DETECTOR] ❌ Modal 'sin resultados' NO encontrado")
        return False
        
    except Exception as e:
        print(f"[DETECTOR] ⚠️ Error detectando modal: {e}")
        return False


def verificar_httpx_sin_procesos_judiciales(log_httpx: str) -> tuple[bool, str]:
    """
    Verifica si HTTPX devolvió "sin resultados" (consulta exitosa sin datos)
    vs error real.
    
    Args:
        log_httpx: String del log de la consulta HTTPX
        
    Returns:
        (es_sin_resultados: bool, tipo: str)
        - (True, "sin_procesos") → Consulta exitosa, sin procesos judiciales
        - (False, "error_api") → Error real en la API
        - (False, "indeterminado") → No se pudo determinar
    """
    print("[DETECTOR HTTPX] 🔍 Analizando resultado de HTTPX...")
    
    if not log_httpx:
        print("[DETECTOR HTTPX] ⚠️ Log vacío")
        return (False, "indeterminado")
    
    log_lower = log_httpx.lower()
    
    # Indicadores de "SIN PROCESOS JUDICIALES" (consulta exitosa)
    sin_resultados_indicators = [
        "página 1 sin resultados",  # Nuestro mensaje actual
        "sin procesos judiciales",
        "sin resultados, finalizando",
        "no hay resultados",
        "búsqueda completada sin resultados",
        "Página 1 sin resultados, finalizando",
        "0 resultados",
    ]
    
    # Indicadores de ERROR REAL
    error_indicators = [
        "no se obtuvieron resultados de la api",
        "error en la consulta",
        "conexión rechazada",
        "timeout",
        "error 500",
        "error 400",
        "exception",
        "traceback",
        "falló",
        "no disponible",
    ]
    
    # Análisis
    tiene_sin_resultados = any(indicator in log_lower for indicator in sin_resultados_indicators)
    tiene_error = any(indicator in log_lower for indicator in error_indicators)
    
    if tiene_sin_resultados and not tiene_error:
        print("[DETECTOR HTTPX] ✅ SIN PROCESOS JUDICIALES (consulta exitosa)")
        return (True, "sin_procesos")
    elif tiene_error:
        print("[DETECTOR HTTPX] ❌ ERROR REAL EN LA CONSULTA")
        return (False, "error_api")
    else:
        print("[DETECTOR HTTPX] ⚠️ No se pudo determinar")
        return (False, "indeterminado")


def crear_rastreo_sin_resultados(cliente_id: int, nombres: str, tipo_consulta: str) -> dict:
    """
    Crea un registro de rastreo cuando no hay procesos judiciales.
    
    Args:
        cliente_id: ID del cliente
        nombres: Nombre del cliente
        tipo_consulta: "scraping" o "httpx"
        
    Returns:
        Dict con información para guardar en BD
    """
    return {
        "cliente_id": cliente_id,
        "nombres": nombres,
        "tipo_consulta": tipo_consulta,
        "resultado": "Sin Procesos Judiciales",
        "estado_final": "Procesado",
        "generar_reporte": False,
        "mensaje_bd": f"Consulta completada: Sin Procesos Judiciales ({tipo_consulta})"
    }