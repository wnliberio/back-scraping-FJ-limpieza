# flows/funcion_judicial.py
"""
Consulta de Función Judicial con navegación automática por TODAS las páginas.
Versión V25: Movimientos humanos completos, paginación automática, modo headless.
"""

import time
import random
from typing import Optional, Dict, List
from datetime import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from core.browser import create_driver, close_driver
from core.utils.log import log
from core.utils.screenshot import save_fullpage_png
from core.human import (
    
    random_scroll_smooth,
    human_like_scroll_and_read,
    move_mouse_in_circle,
    move_mouse_zigzag,
    move_mouse_bezier_curve,
    human_type_advanced,
    human_click_element,
    human_click_offset
)
from core.capsolver import resolver_recaptcha_si_necesario  # NUEVO
from core.config import MAX_RETRIES

# Constantes
FUNCION_JUDICIAL_URL = "https://procesosjudiciales.funcionjudicial.gob.ec/busqueda-filtros"


def _slug(text: str) -> str:
    """Genera slug para nombres de archivo"""
    import re
    return re.sub(r'[^\w\s-]', '', text).strip().replace(' ', '_').lower()


def _save_screenshot(driver, basename: str) -> str:
    """Guarda screenshot usando la función del core"""
    return save_fullpage_png(driver, basename=basename)


# ============= BÚSQUEDA DE ELEMENTOS =============

def find_name_input(driver, timeout: int = 15):
    """Encuentra el campo de nombres (#mat-input-4)"""
    log("Buscando campo de nombres...")
    
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, "mat-input-4"))
        )
        log("Campo de nombres encontrado: #mat-input-4")
        return element
    except TimeoutException:
        log("No se encontró por ID, intentando XPath...")
        try:
            element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="mat-input-4"]'))
            )
            log("Campo de nombres encontrado: XPath")
            return element
        except TimeoutException:
            log("No se pudo encontrar el campo de nombres")
            return None


def find_search_button(driver, timeout: int = 15):
    """Encuentra el botón de búsqueda"""
    log("Buscando botón de búsqueda...")
    
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "button.boton-buscar.mdc-button.mdc-button--raised.mat-mdc-raised-button.mat-accent"
            ))
        )
        log("Botón de búsqueda encontrado")
        return element
    except TimeoutException:
        log("No se encontró el botón con CSS, intentando XPath...")
        try:
            element = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[contains(@class, 'boton-buscar')]"
                ))
            )
            log("Botón de búsqueda encontrado: XPath")
            return element
        except TimeoutException:
            log("No se pudo encontrar el botón de búsqueda")
            return None


def find_next_page_button(driver, timeout: int = 10):
    """Encuentra el botón de página siguiente en el paginador"""
    log("Buscando botón de página siguiente...")
    
    # Estrategia 1: XPath específico
    xpaths = [
        "/html/body/app-root/app-expel-listado-juicios/expel-sidenav/mat-sidenav-container/mat-sidenav-content/section/footer/mat-paginator/div/div/div[2]/button[3]",
        "//mat-paginator//button[@aria-label='Página siguiente']",
        "//mat-paginator//button[contains(@class, 'mat-mdc-paginator-navigation-next')]",
        "//button[@aria-label='Next page']",
        "//button[contains(@class, 'mat-paginator-navigation-next')]"
    ]
    
    for xpath in xpaths:
        try:
            element = driver.find_element(By.XPATH, xpath)
            if element:
                log(f"Botón 'siguiente' encontrado con XPath")
                return element
        except NoSuchElementException:
            continue
    
    # Estrategia 2: CSS Selectors
    css_selectors = [
        "mat-paginator button.mat-mdc-paginator-navigation-next",
        "button[aria-label='Página siguiente']",
        ".mat-paginator-navigation-next"
    ]
    
    for selector in css_selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            if element:
                log(f"Botón 'siguiente' encontrado con CSS")
                return element
        except NoSuchElementException:
            continue
    
    log("No se pudo encontrar el botón de página siguiente")
    return None


# ============= DETECCIÓN DE ESTADO =============

def detect_no_results_modal(driver, timeout: int = 5) -> bool:
    """Detecta si apareció el modal de 'sin resultados'"""
    log("Verificando si apareció modal de 'sin resultados'...")
    
    try:
        no_results_selectors = [
            "//div[contains(text(), 'La consulta no devolvió resultados')]",
            "//div[contains(text(), 'La consulta no devolvió resultados. Cerrar')]",
            "//span[contains(text(), 'La consulta no devolvió resultadosn')]",
            "//p[contains(text(), 'La consulta no devolvió resultados')]",
            "//*[contains(@class, 'La consulta no devolvió resultados')]",
            "//div[starts-with(@id, 'mat-snack-bar-container-live')]/div/simple-snack-bar",
            "//div[starts-with(@id, 'mat-snack-bar-container-live')]//div[contains(@class, 'mat-mdc-snack-bar-label')]"
        ]
        
        for selector in no_results_selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)
                for element in elements:
                    if element.is_displayed():
                        text = element.text.strip()
                        log(f"Detectado modal 'sin resultados': {text}")
                        return True
            except:
                continue
        
        # Verificar por JavaScript
        try:
            result = driver.execute_script("""
                const text = document.body.innerText.toLowerCase();
                return text.includes('la consulta no devolvió resultados');
            """)
            if result:
                log("Detectado 'sin resultados' por contenido de texto")
                return True
        except:
            pass
        
        log("No se detectó modal 'sin resultados'")
        return False
        
    except Exception as e:
        log(f"Error detectando modal sin resultados: {e}")
        return False


def detect_results_loaded(driver, timeout: int = 10) -> bool:
    """Detecta si se cargaron resultados"""
    log("Verificando si hay resultados cargados...")
    
    try:
        wait_random(2.0, 3.0)
        
        results_selectors = [
            "table tbody tr",
            "mat-row",
            ".mat-mdc-row",
            "div.result-item",
            "div.resultado",
            ".list-item",
            "tr[role='row']"
        ]
        
        for selector in results_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                visible_elements = [e for e in elements if e.is_displayed()]
                
                if len(visible_elements) > 0:
                    log(f"Detectados {len(visible_elements)} resultados")
                    return True
            except:
                continue
        
        # Verificar contenedor de resultados específico
        try:
            results_container = driver.find_element(By.CSS_SELECTOR,
                "body > app-root > app-expel-listado-juicios > expel-sidenav > mat-sidenav-container > mat-sidenav-content > section"
            )
            if results_container.is_displayed():
                content_height = driver.execute_script(
                    "return arguments[0].scrollHeight;",
                    results_container
                )
                if content_height > 200:
                    log(f"Contenedor de resultados detectado (altura: {content_height}px)")
                    return True
        except:
            pass
        
        log("No se detectaron resultados cargados")
        return False
        
    except Exception as e:
        log(f"Error detectando resultados: {e}")
        return False


def is_next_button_enabled(driver, next_button) -> bool:
    """
    Verifica si el botón de siguiente página está habilitado.
    IMPORTANTE: Ignora 'mat-mdc-button-disabled-interactive' (solo visual).
    """
    try:
        # 1. Verificar atributo disabled REAL
        is_disabled_attr = next_button.get_attribute("disabled")
        if is_disabled_attr == "true" or is_disabled_attr == "disabled" or is_disabled_attr is not None:
            log("Botón 'siguiente' está deshabilitado (atributo disabled='true')")
            return False
        
        # 2. Verificar aria-disabled
        aria_disabled = next_button.get_attribute("aria-disabled")
        if aria_disabled == "true":
            log("Botón 'siguiente' está deshabilitado (aria-disabled='true')")
            return False
        
        # 3. Verificar clases CSS IGNORANDO la clase engañosa
        classes = next_button.get_attribute("class") or ""
        
        real_disabled_classes = [
            "mat-button-disabled",
            "mdc-button--disabled",
        ]
        
        for disabled_class in real_disabled_classes:
            if disabled_class in classes:
                log(f"Botón 'siguiente' está deshabilitado (clase CSS: {disabled_class})")
                return False
        
        if "mat-mdc-button-disabled-interactive" in classes:
            log("Detectada clase 'mat-mdc-button-disabled-interactive' (solo visual, NO funcional)")
        
        # 4. Verificar clickeabilidad con JavaScript
        is_clickable = driver.execute_script("""
            const btn = arguments[0];
            const style = window.getComputedStyle(btn);
            
            if (style.pointerEvents === 'none') return false;
            if (style.display === 'none') return false;
            if (style.visibility === 'hidden') return false;
            if (btn.disabled === true) return false;
            
            return true;
        """, next_button)
        
        if not is_clickable:
            log("Botón 'siguiente' no es clickeable (verificación JS)")
            return False
        
        # 5. Verificar info del paginador
        try:
            paginator_info = driver.execute_script("""
                const paginator = arguments[0].closest('mat-paginator');
                if (!paginator) return null;
                
                const rangeLabel = paginator.querySelector('.mat-mdc-paginator-range-label');
                if (!rangeLabel) return null;
                
                const text = rangeLabel.textContent.trim();
                const match = text.match(/(\\d+)\\s*–\\s*(\\d+)\\s*de\\s*(\\d+)/);
                if (match) {
                    const end = parseInt(match[2]);
                    const total = parseInt(match[3]);
                    return {
                        text: text,
                        hasMore: end < total
                    };
                }
                return null;
            """, next_button)
            
            if paginator_info:
                log(f"Info paginador: {paginator_info['text']}")
                if not paginator_info['hasMore']:
                    log("Ya estamos en la última página (según info del paginador)")
                    return False
                else:
                    log("Hay más páginas disponibles (según info del paginador)")
        except Exception as e:
            log(f"No se pudo obtener info del paginador: {e}")
        
        log("Botón 'siguiente' está habilitado y clickeable")
        return True
        
    except Exception as e:
        log(f"Error verificando estado del botón: {e}")
        return True  # Asumir habilitado en caso de error


# ============= CAPTURA DE SCREENSHOTS =============

def capture_results_page(driver, base_name: str, page_number: int = 1) -> str:
    """Captura screenshot de una página de resultados"""
    log(f"Capturando screenshot de página {page_number}...")
    
    try:
        # Scroll suave para asegurar visibilidad
        random_scroll_smooth(driver, 'down', 100)
        wait_random(0.5, 0.8)
        random_scroll_smooth(driver, 'up', 50)
        wait_random(0.3, 0.5)
        
        basename = f"{base_name}_page{page_number}"
        screenshot_path = _save_screenshot(driver, basename)
        
        log(f"Screenshot página {page_number} guardado: {screenshot_path}")
        return screenshot_path
    except Exception as e:
        log(f"Error capturando screenshot: {e}")
        return None


def capture_no_results_screenshot(driver, base_name: str) -> str:
    """Captura screenshot del modal de sin resultados"""
    log("Capturando screenshot del modal 'sin resultados'...")
    
    try:
        basename = f"{base_name}_SIN_RESULTADOS"
        screenshot_path = _save_screenshot(driver, basename)
        log(f"Screenshot sin resultados guardado: {screenshot_path}")
        return screenshot_path
    except Exception as e:
        log(f"Error capturando screenshot: {e}")
        return None


# ============= NAVEGACIÓN AUTOMÁTICA POR PÁGINAS =============

def capture_all_result_pages(driver, base_name: str, max_pages: int = 50) -> List[str]:
    """
    Navega por TODAS las páginas de resultados capturando screenshots.
    Lógica completa de V25.
    
    Returns:
        Lista de rutas de screenshots capturados
    """
    log("=" * 60)
    log("INICIANDO CAPTURA DE TODAS LAS PÁGINAS DE RESULTADOS")
    log("=" * 60)
    
    screenshots = []
    current_page = 1
    
    # Capturar primera página
    log(f"Capturando página {current_page}...")
    screenshot = capture_results_page(driver, base_name, current_page)
    if screenshot:
        screenshots.append(screenshot)
    
    # Scroll para revisar contenido
    log("Revisando contenido de la página...")
    random_scroll_smooth(driver, 'down', random.randint(200, 400))
    wait_random(0.8, 1.5)
    random_scroll_smooth(driver, 'up', random.randint(100, 200))
    wait_random(0.5, 1.0)
    
    # Scroll al final (donde está el paginador)
    log("Desplazando hacia el paginador...")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    wait_random(1.0, 2.0)
    
    # LOOP: Navegar por todas las páginas
    while current_page < max_pages:
        log("-" * 60)
        log(f"Intentando navegar a página {current_page + 1}...")
        
        # Buscar botón de siguiente
        next_button = find_next_page_button(driver)
        
        if not next_button:
            log("No se encontró el botón de siguiente página")
            log("Fin de la paginación (no hay botón siguiente)")
            break
        
        # Verificar si está habilitado
        if not is_next_button_enabled(driver, next_button):
            log("Fin de la paginación (botón deshabilitado - última página alcanzada)")
            break
        
        # Scroll al botón
        log("Posicionando botón 'siguiente' en vista...")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", next_button)
        wait_random(0.8, 1.5)
        
        # Movimiento humano hacia el botón
        log("Localizando visualmente el botón 'siguiente'...")
        try:
            movement_type = random.choice(['zigzag', 'circle', 'bezier'])
            
            if movement_type == 'zigzag':
                move_mouse_zigzag(driver, next_button, steps=random.randint(4, 6))
            elif movement_type == 'circle':
                move_mouse_in_circle(driver, next_button, radius=random.randint(30, 50))
            else:
                move_mouse_bezier_curve(driver, next_button, control_points=random.randint(2, 3))
        except Exception as e:
            log(f"Movimiento humano falló: {e}")
        
        wait_random(0.5, 1.0)
        
        # Click en el botón "siguiente"
        log(f"Haciendo clic en 'Página siguiente' para ir a página {current_page + 1}...")
        
        click_success = False
        
        # Intento 1: Click con movimientos humanos
        try:
            if human_click_element(driver, next_button, use_human_movement=True):
                click_success = True
        except Exception as e:
            log(f"Click humano falló: {e}")
        
        # Intento 2: Click directo
        if not click_success:
            try:
                log("Intentando click directo...")
                from selenium.webdriver.common.action_chains import ActionChains
                actions = ActionChains(driver)
                actions.move_to_element(next_button)
                actions.pause(random.uniform(0.3, 0.6))
                actions.click()
                actions.perform()
                click_success = True
            except Exception as e:
                log(f"Click directo falló: {e}")
        
        # Intento 3: JavaScript click
        if not click_success:
            try:
                log("Intentando click con JavaScript...")
                driver.execute_script("arguments[0].click();", next_button)
                click_success = True
            except Exception as e:
                log(f"JavaScript click también falló: {e}")
        
        if not click_success:
            log("No se pudo hacer clic en el botón siguiente")
            log("Terminando paginación debido a error de click")
            break
        
        log(f"Click ejecutado - Navegando a página {current_page + 1}")
        
        # Esperar carga de nueva página
        log("Esperando carga de nueva página...")
        wait_random(2.0, 4.0)
        
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            log("Nueva página cargada")
        except TimeoutException:
            log("Timeout esperando carga de página")
        
        # Incrementar contador
        current_page += 1
        
        # Scroll para revisar contenido
        log("Revisando contenido de la nueva página...")
        driver.execute_script("window.scrollTo(0, 0);")
        wait_random(0.5, 1.0)
        random_scroll_smooth(driver, 'down', random.randint(250, 450))
        wait_random(0.8, 1.5)
        random_scroll_smooth(driver, 'up', random.randint(100, 250))
        wait_random(0.5, 1.0)
        
        # Capturar screenshot de esta página
        log(f"Capturando screenshot de página {current_page}...")
        screenshot = capture_results_page(driver, base_name, current_page)
        if screenshot:
            screenshots.append(screenshot)
        else:
            log("No se pudo capturar screenshot de esta página")
        
        # Scroll al final para el siguiente ciclo
        log("Preparando para siguiente página...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        wait_random(1.0, 2.0)
        
        # Prevención de loop infinito
        if current_page >= max_pages:
            log(f"Alcanzado límite máximo de páginas ({max_pages})")
            break
    
    log("=" * 60)
    log(f"PAGINACIÓN COMPLETADA")
    log(f"Total de páginas capturadas: {len(screenshots)}")
    for i, screenshot in enumerate(screenshots, 1):
        log(f"   {i}. {screenshot}")
    log("=" * 60)
    
    return screenshots


# ============= PROCESO PRINCIPAL =============

def process_funcion_judicial_once(apellidos_nombres: str, headless: bool = True) -> Optional[Dict]:
    """
    Ejecuta UNA consulta en Función Judicial.
    Versión V25: Incluye secuencia especial de clics y paginación automática.
    """
    driver = None
    
    try:
        log("=" * 60)
        log(f"INICIANDO CONSULTA FUNCIÓN JUDICIAL V25")
        log(f"Nombre a buscar: {apellidos_nombres}")
        log(f"Modo headless: {headless}")
        log("=" * 60)
        
        # 1. Crear driver CON cookies persistentes
        driver = create_driver(headless=headless, use_cookies=True, cookies_domain="funcionjudicial")
        
        # 2. Navegar a la página
        log(f"Navegando a: {FUNCION_JUDICIAL_URL}")
        driver.get(FUNCION_JUDICIAL_URL)
        
        # Espera inicial (aumentada para evitar captcha)
        log("Esperando carga inicial de la página...")
        wait_random(5.0, 8.0)  # Aumentado de 3-5 a 5-8
        
        # Screenshot inicial
        _save_screenshot(driver, f"01_pagina_inicial_{_slug(apellidos_nombres)}")
        
        # Simular lectura humana (con delays aumentados)
        log("Simulando lectura humana de la página...")
        human_like_scroll_and_read(driver)
        
        # 3. Encontrar campo de nombres (con delay adicional)
        log("Esperando que el formulario esté listo...")
        wait_random(2.0, 4.0)  # Aumentado de 1-2 a 2-4
        
        name_input = find_name_input(driver)
        if not name_input:
            log("No se encontró el campo de nombres")
            return None
        
        # Movimiento hacia el campo
        log("Localizando campo de entrada...")
        try:
            move_mouse_in_circle(driver, name_input, radius=40)
        except Exception as e:
            log(f"Movimiento circular falló: {e}")
        
        wait_random(0.5, 1.0)
        
        # 4. Escribir apellidos y nombres
        log(f"Escribiendo en el campo: {apellidos_nombres}")
        human_type_advanced(driver, name_input, apellidos_nombres)
        wait_random(1.0, 2.0)
        
        # Screenshot después de escribir
        _save_screenshot(driver, f"02_despues_escribir_{_slug(apellidos_nombres)}")
        
        # Scroll aleatorio (revisando)
        log("Revisando datos ingresados...")
        random_scroll_smooth(driver, 'down', random.randint(50, 120))
        wait_random(0.5, 1.0)
        random_scroll_smooth(driver, 'up', random.randint(30, 80))
        wait_random(0.8, 1.5)
        
        # 5. Encontrar botón de búsqueda
        search_btn = find_search_button(driver)
        if not search_btn:
            log("No se encontró el botón de búsqueda")
            return None
        
        # 6. SECUENCIA DE CLICS ESPECIAL (3 clics)
        log("Iniciando secuencia de clics especial...")
        
        # Movimiento hacia el botón
        log("Localizando botón de búsqueda...")
        try:
            move_mouse_zigzag(driver, search_btn, steps=random.randint(5, 7))
        except Exception as e:
            log(f"Movimiento zigzag falló: {e}")
        
        wait_random(0.5, 1.0)
        
        # CLIC #1: En el botón BUSCAR
        log("CLIC #1: En botón BUSCAR")
        if not human_click_element(driver, search_btn, use_human_movement=True):
            log("Falló clic #1")
            return None
        
        wait_random(2.5, 4.0)  # Aumentado
        _save_screenshot(driver, f"03_despues_clic1_{_slug(apellidos_nombres)}")
        
        # Scroll aleatorio
        log("Scroll aleatorio post-clic #1...")
        random_scroll_smooth(driver, random.choice(['up', 'down']), random.randint(40, 100))
        wait_random(0.5, 1.0)
        
        # CLIC #2: 75px arriba del botón (ventanita)
        log("CLIC #2: Preparando posición (75px arriba del botón)")
        wait_random(1.0, 2.0)
        
        try:
            if not human_click_offset(driver, search_btn, 0, -75):
                log("Falló clic #2")
        except Exception as e:
            log(f"Clic #2 falló: {e}")
        
        wait_random(2.0, 3.0)
        _save_screenshot(driver, f"04_despues_clic2_{_slug(apellidos_nombres)}")
        
        # CLIC #3: Nuevamente en el botón BUSCAR
        log("CLIC #3: Nuevamente en botón BUSCAR")
        wait_random(1.0, 1.8)
        
        try:
            search_btn = find_search_button(driver)
            if search_btn:
                log("Localizando botón para clic #3...")
                try:
                    movement_type = random.choice(['bezier', 'circle', 'zigzag'])
                    
                    if movement_type == 'bezier':
                        move_mouse_bezier_curve(driver, search_btn, control_points=random.randint(2, 3))
                    elif movement_type == 'circle':
                        move_mouse_in_circle(driver, search_btn, radius=random.randint(35, 55))
                    else:
                        move_mouse_zigzag(driver, search_btn, steps=random.randint(4, 6))
                except Exception as e:
                    log(f"Movimiento previo al clic #3 falló: {e}")
                
                wait_random(0.5, 1.0)
                
                if human_click_element(driver, search_btn, use_human_movement=True):
                    log("Clic #3 ejecutado exitosamente")
                else:
                    log("Clic #3 falló, continuando...")
        except Exception as e:
            log(f"Error en clic #3: {e}, continuando...")
        
        wait_random(2.0, 3.5)
        _save_screenshot(driver, f"05_despues_clic3_{_slug(apellidos_nombres)}")
        
        # 7. DETECCIÓN DE RESULTADOS VS SIN RESULTADOS
        log("=" * 60)
        log("ANALIZANDO RESPUESTA DEL SISTEMA...")
        log("=" * 60)
        
        wait_random(2.0, 4.0)
        
        has_no_results_modal = detect_no_results_modal(driver)
        has_results = detect_results_loaded(driver)
        
        resultado = {
            "success": True,
            "nombre_buscado": apellidos_nombres,
            "screenshots": [],
            "total_pages": 0
        }
        
        if has_no_results_modal:
            # ESCENARIO 1: Sin resultados
            log("ESCENARIO: SIN RESULTADOS")
            log("El sistema indica que no hay procesos judiciales")
            
            screenshot_sin_resultados = capture_no_results_screenshot(
                driver,
                f"funcion_judicial_{_slug(apellidos_nombres)}"
            )
            
            resultado["scenario"] = "no_results"
            resultado["screenshot_path"] = screenshot_sin_resultados
            resultado["mensaje"] = "No se encontraron procesos judiciales"
            resultado["screenshots"].append(screenshot_sin_resultados)
            resultado["total_pages"] = 0
            
            log("Proceso completado - Sin resultados")
            
        elif has_results:
            # ESCENARIO 2: Resultados encontrados - CAPTURAR TODAS LAS PÁGINAS
            log("ESCENARIO: CON RESULTADOS")
            log("Se encontraron procesos judiciales")
            log("Iniciando captura automática de TODAS las páginas...")
            
            # CAPTURA AUTOMÁTICA DE TODAS LAS PÁGINAS (V25)
            paginated_screenshots = capture_all_result_pages(
                driver,
                f"funcion_judicial_{_slug(apellidos_nombres)}"
            )
            
            resultado["scenario"] = "results_found"
            resultado["screenshot_path"] = paginated_screenshots[0] if paginated_screenshots else None
            resultado["mensaje"] = f"Se encontraron procesos judiciales en {len(paginated_screenshots)} página(s)"
            resultado["screenshots"] = paginated_screenshots
            resultado["total_pages"] = len(paginated_screenshots)
            
            # Retrocompatibilidad: agregar screenshot_historial_path
            if len(paginated_screenshots) > 1:
                resultado["screenshot_historial_path"] = paginated_screenshots[1]
            
            log("Proceso completado - Todas las páginas capturadas")
            
        else:
            # ESCENARIO 3: Estado indeterminado
            log("ESCENARIO: INDETERMINADO")
            log("No se pudo determinar si hay resultados o no")
            
            screenshot_indeterminado = _save_screenshot(
                driver,
                f"06_estado_indeterminado_{_slug(apellidos_nombres)}"
            )
            
            resultado["scenario"] = "indeterminate"
            resultado["screenshot_path"] = screenshot_indeterminado
            resultado["mensaje"] = "No se pudo determinar el estado de los resultados"
            resultado["screenshots"].append(screenshot_indeterminado)
            resultado["total_pages"] = 0
            
            log("Proceso completado - Estado indeterminado")
        
        # Screenshot final
        screenshot_final = _save_screenshot(
            driver,
            f"99_FINAL_{_slug(apellidos_nombres)}"
        )
        resultado["screenshots"].append(screenshot_final)
        
        log("=" * 60)
        log("CONSULTA FUNCIÓN JUDICIAL V25 COMPLETADA")
        log(f"Escenario: {resultado['scenario']}")
        log(f"Mensaje: {resultado['mensaje']}")
        log(f"Total páginas: {resultado['total_pages']}")
        log(f"Screenshots capturados: {len(resultado['screenshots'])}")
        log("=" * 60)
        
        wait_random(3.0, 5.0)  # Aumentado
        
        return resultado
        
    except Exception as e:
        log(f"ERROR EN CONSULTA: {e}")
        import traceback
        traceback.print_exc()
        
        if driver:
            _save_screenshot(driver, f"error_{_slug(apellidos_nombres)}")
        
        return None
        
    finally:
        if driver:
            log("Cerrando driver y guardando cookies...")
            try:
                close_driver(driver, save_cookies_flag=True, cookies_domain="funcionjudicial")
            except Exception:
                pass


def process_funcion_judicial(apellidos_nombres: str, headless: bool = True) -> Optional[Dict]:
    """
    Ejecuta consulta en Función Judicial con reintentos.
    Patrón estándar del proyecto.
    """
    if not apellidos_nombres or len(apellidos_nombres.strip()) < 3:
        return {"error": "Función Judicial: Ingresa apellidos y nombres válidos (mínimo 3 caracteres)"}
    
    apellidos_nombres = apellidos_nombres.strip()
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log(f"--- Procesando FUNCIÓN JUDICIAL '{apellidos_nombres}' (intento {attempt}/{MAX_RETRIES}) ---")
            data = process_funcion_judicial_once(apellidos_nombres, headless=headless)
            if data:
                return data
        except Exception as e:
            log(f"Error en intento {attempt}: {e}")
        
        if attempt < MAX_RETRIES:
            wait_time = min(3 + attempt * 2, 10)
            log(f"Esperando {wait_time}s antes del siguiente intento...")
            time.sleep(wait_time)
    
    log(f"Falló el procesamiento para '{apellidos_nombres}' tras {MAX_RETRIES} intentos")
    return {"error": f"No se pudo procesar la consulta tras {MAX_RETRIES} intentos"}