# core/capsolver.py
"""
Integración con CapSolver para resolver reCAPTCHA de forma inteligente.
Solo se usa cuando aparece la ventana de imágenes, ahorrando créditos.
"""

import time
import requests
import urllib.parse as up
from typing import Optional, Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver

from core.utils.log import log


# ============= CONFIGURACIÓN =============
# ⚠️ IMPORTANTE: Configura tu API Key de CapSolver
CAPSOLVER_API_KEY = "CAP-E6752C17A4ABA2B00B5CA4709EB0624568BE753E83D3C4885B1AD5F121E7898D"

# Sitekey quemado de Función Judicial (puede cambiar)
DEFAULT_SITEKEY = "6LfjVAcUAAAAANT1V80aWo"


# ============= DETECCIÓN DE RECAPTCHA =============

def detectar_recaptcha_iframe(driver: WebDriver) -> Tuple[bool, Optional[str], str]:
    """
    Detecta iframes de reCAPTCHA y extrae el sitekey.
    
    Returns:
        Tuple[bool, Optional[str], str]: (hay_recaptcha, sitekey, url_actual)
    """
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            src = iframe.get_attribute("src") or ""
            if "recaptcha" in src or "google.com/recaptcha" in src:
                query = up.urlparse(src).query
                params = dict(up.parse_qsl(query))
                sitekey = params.get("k")
                return True, sitekey, driver.current_url
    except Exception as e:
        log(f"Error detectando iframe recaptcha: {e}")
    
    return False, None, driver.current_url


def detectar_ventana_imagenes_recaptcha(driver: WebDriver, timeout: int = 5) -> bool:
    """
    🎯 FUNCIÓN CLAVE: Detecta si apareció la ventana modal con el desafío de IMÁGENES.
    
    Esta ventana aparece después de hacer clic en el checkbox "I'm not a robot"
    cuando Google decide que necesita verificación adicional mediante imágenes
    (semáforos, autos, bicicletas, puentes, etc.)
    
    Returns:
        bool: True si detectó la ventana de imágenes, False si no apareció
    """
    log("Detectando ventana modal de imágenes del reCAPTCHA...")
    
    try:
        # Esperar un poco para que la ventana aparezca si va a aparecer
        time.sleep(1.5)
        
        # ===== MÉTODO 1: Buscar iframe del desafío de imágenes (bframe) =====
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            try:
                src = iframe.get_attribute("src") or ""
                # El iframe del desafío de imágenes contiene "api2/bframe" o "recaptcha/api2/bframe"
                if ("bframe" in src and "recaptcha" in src) or "api2/bframe" in src:
                    # Verificar si el iframe es visible
                    if iframe.is_displayed():
                        log(f"Iframe de desafío de imágenes detectado: {src[:80]}...")
                        return True
            except:
                continue
        
        # ===== MÉTODO 2: Buscar el contenedor de selección de imágenes directamente =====
        selectores_ventana_imagenes = [
            "div.rc-imageselect",
            "div.rc-imageselect-challenge",
            "div.rc-imageselect-target",
            "table.rc-imageselect-table-33",
            "table.rc-imageselect-table-44",
            "div.rc-imageselect-incorrect-response",
            "div.rc-imageselect-desc",
            "div.rc-imageselect-instructions"
        ]
        
        for selector in selectores_ventana_imagenes:
            try:
                elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                for elemento in elementos:
                    if elemento.is_displayed():
                        log(f"Elemento de ventana de imágenes detectado: {selector}")
                        return True
            except:
                continue
        
        # ===== MÉTODO 3: Buscar por XPath elementos específicos del desafío =====
        xpaths_ventana = [
            "//div[contains(@class, 'rc-imageselect')]",
            "//div[contains(@class, 'rc-imageselect-challenge')]",
            "//strong[contains(text(), 'Select all images') or contains(text(), 'Selecciona todas las imágenes')]"
        ]
        
        for xpath in xpaths_ventana:
            try:
                elementos = driver.find_elements(By.XPATH, xpath)
                for elemento in elementos:
                    if elemento.is_displayed():
                        log("Ventana de imágenes detectada por XPath")
                        return True
            except:
                continue
        
        # ===== MÉTODO 4: Verificar en el DOM completo con JavaScript =====
        try:
            resultado = driver.execute_script("""
                // Buscar elementos visibles que indiquen el desafío de imágenes
                const imageSelectDivs = document.querySelectorAll('div[class*="rc-imageselect"]');
                for (let div of imageSelectDivs) {
                    const style = window.getComputedStyle(div);
                    if (style.display !== 'none' && style.visibility !== 'hidden') {
                        return true;
                    }
                }
                
                // Buscar iframes del desafío
                const iframes = document.querySelectorAll('iframe');
                for (let iframe of iframes) {
                    const src = iframe.src || '';
                    if ((src.includes('bframe') && src.includes('recaptcha')) || src.includes('api2/bframe')) {
                        const style = window.getComputedStyle(iframe);
                        if (style.display !== 'none' && style.visibility !== 'hidden') {
                            return true;
                        }
                    }
                }
                
                return false;
            """)
            
            if resultado:
                log("Ventana de imágenes detectada por JavaScript")
                return True
        except Exception as e:
            log(f"Error en detección JavaScript: {e}")
        
        log("No se detectó ventana de imágenes - el checkbox se resolvió automáticamente")
        return False
        
    except Exception as e:
        log(f"Error en detección de ventana de imágenes: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============= CAPSOLVER API =============

def crear_tarea_capsolver(
    site_url: str,
    site_key: str = DEFAULT_SITEKEY,
    api_key: str = CAPSOLVER_API_KEY,
    timeout: int = 30
) -> str:
    """
    Crea una tarea en CapSolver para resolver reCAPTCHA v2.
    
    Args:
        site_url: URL del sitio donde está el captcha
        site_key: Sitekey del reCAPTCHA
        api_key: Tu API key de CapSolver
        timeout: Timeout para la request
    
    Returns:
        str: Task ID de CapSolver
    
    Raises:
        Exception: Si hay error creando la tarea
    """
    log(f"Creando tarea CapSolver para sitekey={site_key} en {site_url}...")
    
    payload = {
        "clientKey": api_key,
        "task": {
            "type": "NoCaptchaTaskProxyless",
            "websiteURL": site_url,
            "websiteKey": site_key
        }
    }
    
    response = requests.post(
        "https://api.capsolver.com/createTask",
        json=payload,
        timeout=timeout
    )
    
    result = response.json()
    
    if result.get("errorId", 0) != 0:
        raise Exception(f"Error creando tarea CapSolver: {result}")
    
    task_id = result.get("taskId")
    log(f"Tarea creada en CapSolver: {task_id}")
    
    return task_id


def obtener_resultado_capsolver(
    task_id: str,
    api_key: str = CAPSOLVER_API_KEY,
    wait_interval: int = 3,
    max_wait_s: int = 180
) -> str:
    """
    Espera y obtiene el resultado de una tarea de CapSolver.
    
    Args:
        task_id: ID de la tarea
        api_key: Tu API key de CapSolver
        wait_interval: Segundos entre cada consulta
        max_wait_s: Máximo tiempo de espera total
    
    Returns:
        str: Token de reCAPTCHA resuelto
    
    Raises:
        TimeoutError: Si no se resuelve en el tiempo máximo
        Exception: Si hay error obteniendo el resultado
    """
    log(f"Esperando resultado CapSolver para task {task_id}...")
    
    start_time = time.time()
    
    while time.time() - start_time < max_wait_s:
        response = requests.post(
            "https://api.capsolver.com/getTaskResult",
            json={
                "clientKey": api_key,
                "taskId": task_id
            },
            timeout=30
        )
        
        result = response.json()
        
        if result.get("status") == "ready":
            token = result["solution"]["gRecaptchaResponse"]
            log(f"CapSolver devolvió token (len={len(token)})")
            return token
        
        if result.get("errorId", 0) != 0:
            raise Exception(f"Error en getTaskResult: {result}")
        
        log(f"Procesando... esperando {wait_interval}s")
        time.sleep(wait_interval)
    
    raise TimeoutError("Timeout esperando solución de CapSolver")


def inyectar_token_en_pagina(driver: WebDriver, token: str):
    """
    Inyecta el token de reCAPTCHA resuelto en la página.
    
    Args:
        driver: WebDriver de Selenium
        token: Token de reCAPTCHA obtenido de CapSolver
    """
    log("Inyectando token en la página...")
    
    js_code = """
    (function(token) {
        var textarea = document.querySelector('textarea[name="g-recaptcha-response"]');
        var parent = document.querySelector('form') || document.body;
        
        if (!textarea) {
            textarea = document.createElement('textarea');
            textarea.name = 'g-recaptcha-response';
            textarea.style.display = 'none';
            parent.appendChild(textarea);
        }
        
        textarea.value = token;
        textarea.dispatchEvent(new Event('input', { bubbles: true }));
        textarea.dispatchEvent(new Event('change', { bubbles: true }));
    })(arguments[0]);
    """
    
    driver.execute_script(js_code, token)
    log("Token inyectado en textarea[name='g-recaptcha-response']")


# ============= FUNCIÓN PRINCIPAL DE RESOLUCIÓN =============

def resolver_recaptcha_si_necesario(
    driver: WebDriver,
    api_key: str = CAPSOLVER_API_KEY,
    sitekey_override: Optional[str] = None
) -> bool:
    """
    Función inteligente que:
    1. Detecta si apareció ventana de imágenes
    2. Si SÍ apareció, usa CapSolver para resolver
    3. Si NO apareció, retorna True (checkbox se resolvió solo)
    
    Args:
        driver: WebDriver de Selenium
        api_key: API key de CapSolver
        sitekey_override: Sitekey específico (si None, usa default o detecta)
    
    Returns:
        bool: True si se resolvió (o no fue necesario), False si falló
    """
    log("=" * 60)
    log("VERIFICANDO SI ES NECESARIO RESOLVER RECAPTCHA...")
    log("=" * 60)
    
    # Esperar para que aparezca la ventana si va a aparecer
    time.sleep(2.0)
    
    # Detectar si apareció ventana de imágenes
    ventana_imagenes_aparecio = detectar_ventana_imagenes_recaptcha(driver)
    
    if not ventana_imagenes_aparecio:
        log("No apareció ventana de imágenes - el checkbox se resolvió automáticamente")
        log("CapSolver NO será usado (ahorro de créditos)")
        return True
    
    # Si llegamos aquí, SÍ hay ventana de imágenes
    log("Ventana de imágenes detectada - Usando CapSolver para resolver...")
    
    try:
        # Obtener sitekey
        hay_iframe, sitekey_detected, page_url = detectar_recaptcha_iframe(driver)
        sitekey = sitekey_override or sitekey_detected or DEFAULT_SITEKEY
        
        log(f"Resolviendo reCAPTCHA: sitekey={sitekey}")
        
        # Crear tarea en CapSolver
        task_id = crear_tarea_capsolver(
            site_url=page_url,
            site_key=sitekey,
            api_key=api_key
        )
        
        # Obtener token resuelto
        token = obtener_resultado_capsolver(task_id, api_key=api_key, max_wait_s=180)
        
        # Inyectar token en la página
        inyectar_token_en_pagina(driver, token)
        
        log("reCAPTCHA resuelto exitosamente con CapSolver")
        return True
        
    except Exception as e:
        log(f"Error resolviendo reCAPTCHA con CapSolver: {e}")
        import traceback
        traceback.print_exc()
        return False