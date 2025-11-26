# core/human.py
"""
Movimientos y acciones que simulan comportamiento humano realista.
Versión V25: Incluye movimientos Bézier, zigzag, círculos y scroll suave.
"""

import time
import random
import math
from typing import Optional

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.action_chains import ActionChains

from core.utils.log import log


# ============= ESPERAS Y DELAYS =============

def wait_random(min_sec: float = 0.5, max_sec: float = 2.0):
    """Espera con variación humana"""
    time.sleep(random.uniform(min_sec, max_sec))


# ============= SCROLL SUAVE Y REALISTA =============

def random_scroll_smooth(driver: WebDriver, direction: str = 'down', distance: Optional[int] = None):
    """
    Scroll aleatorio suave que simula lectura humana
    
    Args:
        driver: WebDriver de Selenium
        direction: 'down' o 'up'
        distance: Distancia en píxeles (si None, aleatorio entre 100-400)
    """
    if distance is None:
        distance = random.randint(100, 400)
    
    if direction == 'down':
        distance = abs(distance)
    else:
        distance = -abs(distance)
    
    # Scroll en pasos pequeños con pausas
    steps = random.randint(8, 15)
    step_size = distance / steps
    
    for _ in range(steps):
        driver.execute_script(f"window.scrollBy(0, {step_size});")
        time.sleep(random.uniform(0.02, 0.08))
    
    # Pequeña corrección al final (simula ajuste humano)
    correction = random.randint(-20, 20)
    driver.execute_script(f"window.scrollBy(0, {correction});")
    time.sleep(random.uniform(0.1, 0.2))


def human_like_scroll_and_read(driver: WebDriver):
    """
    Simula lectura humana de la página con scroll
    """
    log("Simulando lectura de página con scroll...")
    
    # Scroll down un poco
    random_scroll_smooth(driver, 'down', random.randint(150, 300))
    wait_random(0.5, 1.2)
    
    # Scroll up un poco (como revisando)
    random_scroll_smooth(driver, 'up', random.randint(50, 150))
    wait_random(0.3, 0.8)
    
    # Scroll down de nuevo
    random_scroll_smooth(driver, 'down', random.randint(100, 250))
    wait_random(0.4, 0.9)
    
    # Volver al top
    driver.execute_script("window.scrollTo(0, 0);")
    wait_random(0.3, 0.6)


# ============= MOVIMIENTOS DEL CURSOR =============

def move_mouse_in_circle(driver: WebDriver, element: WebElement, radius: int = 50):
    """
    Mueve el mouse en un pequeño círculo alrededor del elemento
    Simula búsqueda visual del objetivo
    """
    log("Movimiento circular del cursor (simulando búsqueda)...")
    
    actions = ActionChains(driver)
    
    # Mover al elemento primero
    actions.move_to_element(element).perform()
    
    # Crear círculo con 8 puntos
    points = 8
    for i in range(points):
        angle = (2 * math.pi * i) / points
        offset_x = int(radius * math.cos(angle))
        offset_y = int(radius * math.sin(angle))
        
        actions = ActionChains(driver)
        actions.move_to_element_with_offset(element, offset_x, offset_y)
        actions.pause(random.uniform(0.05, 0.12))
        actions.perform()
    
    # Volver al centro
    actions = ActionChains(driver)
    actions.move_to_element(element)
    actions.pause(random.uniform(0.1, 0.2))
    actions.perform()


def move_mouse_zigzag(driver: WebDriver, element: WebElement, steps: int = 5):
    """
    Mueve el mouse en zigzag hacia el elemento
    Simula movimiento natural humano (no en línea recta)
    """
    log("Movimiento en zigzag del cursor...")
    
    # Puntos intermedios con desviación lateral
    for i in range(1, steps + 1):
        progress = i / steps
        
        # Desviación lateral aleatoria (zigzag)
        lateral_offset = random.randint(-30, 30) if i < steps else 0
        vertical_offset = int(-50 * progress)  # Avanzar hacia el elemento
        
        actions = ActionChains(driver)
        actions.move_to_element_with_offset(element, lateral_offset, vertical_offset)
        actions.pause(random.uniform(0.05, 0.15))
        actions.perform()
    
    # Movimiento final al centro exacto
    actions = ActionChains(driver)
    actions.move_to_element(element)
    actions.pause(random.uniform(0.1, 0.2))
    actions.perform()


def move_mouse_bezier_curve(driver: WebDriver, element: WebElement, control_points: int = 3):
    """
    Mueve el mouse siguiendo una curva Bézier
    Simula el movimiento natural del cursor humano
    """
    log("Movimiento en curva Bézier (simulando humano)...")
    
    # Crear curva con puntos de control aleatorios
    points = []
    for i in range(control_points):
        # Generar puntos de control con desviación
        offset_x = random.randint(-80, 80)
        offset_y = random.randint(-80, 80)
        points.append((offset_x, offset_y))
    
    # Agregar punto final (centro del elemento)
    points.append((0, 0))
    
    # Interpolar puntos a lo largo de la curva
    steps = 12
    for step in range(steps):
        t = step / (steps - 1)
        
        # Interpolación simple (Bézier cuadrática)
        if len(points) >= 3:
            # Punto en la curva
            idx = int(t * (len(points) - 1))
            if idx >= len(points) - 1:
                offset_x, offset_y = points[-1]
            else:
                p1 = points[idx]
                p2 = points[idx + 1]
                local_t = (t * (len(points) - 1)) - idx
                offset_x = p1[0] + (p2[0] - p1[0]) * local_t
                offset_y = p1[1] + (p2[1] - p1[1]) * local_t
        else:
            offset_x, offset_y = 0, 0
        
        actions = ActionChains(driver)
        actions.move_to_element_with_offset(element, int(offset_x), int(offset_y))
        actions.pause(random.uniform(0.04, 0.1))
        actions.perform()


# ============= ESCRITURA HUMANA =============

def human_type(element: WebElement, text: str, base_delay: float = 0.15):
    """
    Escribe texto caracter por caracter simulando escritura humana realista.
    Compatible con la firma anterior pero con lógica mejorada de V25.
    
    Args:
        element: WebElement donde escribir
        text: Texto a escribir
        base_delay: Delay base entre caracteres (default 0.15s)
    """
    log(f"Escribiendo texto: {text[:50]}..." if len(text) > 50 else f"Escribiendo: {text}")
    
    # Limpiar campo
    element.clear()
    time.sleep(random.uniform(0.3, 0.6))
    
    # Click en el campo
    element.click()
    time.sleep(random.uniform(0.2, 0.4))
    
    # Escribir caracter por caracter
    for i, char in enumerate(text):
        element.send_keys(char)
        
        # Delay variable entre caracteres (más natural)
        delay = max(0.08, random.gauss(base_delay, base_delay * 0.4))
        time.sleep(delay)
        
        # Pausas más largas en espacios y puntuación
        if char in ' ':
            time.sleep(random.uniform(0.15, 0.35))
        elif char in ',.;:':
            time.sleep(random.uniform(0.2, 0.4))
        
        # Cada 5-8 caracteres, pausa adicional (simula pensar)
        if i > 0 and i % random.randint(5, 8) == 0:
            time.sleep(random.uniform(0.15, 0.35))


def human_type_advanced(driver: WebDriver, element: WebElement, text: str, base_delay: float = 0.15):
    """
    Versión avanzada con movimientos de mouse durante escritura.
    Simula distracciones humanas naturales.
    """
    log(f"Escribiendo con movimientos avanzados: {text[:50]}...")
    
    # Scroll al elemento
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
    wait_random(0.5, 1.0)
    
    # Limpiar campo
    element.clear()
    wait_random(0.5, 1.0)
    
    # Click con movimiento previo
    try:
        actions = ActionChains(driver)
        actions.move_to_element_with_offset(element, random.randint(-10, 10), random.randint(-5, 5))
        actions.pause(random.uniform(0.2, 0.4))
        actions.move_to_element(element)
        actions.pause(random.uniform(0.1, 0.3))
        actions.click()
        actions.perform()
    except:
        element.click()
    
    wait_random(0.3, 0.6)
    
    # Escribir caracter por caracter
    for i, char in enumerate(text):
        element.send_keys(char)
        
        delay = max(0.08, random.gauss(base_delay, base_delay * 0.4))
        time.sleep(delay)
        
        if char in ' ':
            time.sleep(random.uniform(0.15, 0.35))
        elif char in ',.;:':
            time.sleep(random.uniform(0.2, 0.4))
        
        if i > 0 and i % random.randint(5, 8) == 0:
            time.sleep(random.uniform(0.15, 0.35))
            
            # A veces mover el mouse (como distracción)
            if random.random() < 0.3:
                try:
                    actions = ActionChains(driver)
                    actions.move_by_offset(random.randint(-20, 20), random.randint(-20, 20))
                    actions.pause(0.1)
                    actions.perform()
                    # Volver al campo
                    actions = ActionChains(driver)
                    actions.move_to_element(element)
                    actions.perform()
                except:
                    pass


# ============= CLICKS HUMANOS =============

def human_click_element(driver: WebDriver, element: WebElement, use_human_movement: bool = True):
    """
    Click en elemento con movimientos humanos realistas.
    Versión V25 con múltiples tipos de movimientos.
    
    Args:
        driver: WebDriver de Selenium
        element: Elemento a clickear
        use_human_movement: Si True, usa movimientos realistas
    
    Returns:
        bool: True si exitoso, False si falló
    """
    log("Preparando click con movimientos humanos...")
    
    try:
        # Scroll al elemento suavemente
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
        wait_random(0.8, 1.5)
        
        # Pequeño scroll adicional aleatorio
        random_scroll_smooth(driver, random.choice(['up', 'down']), random.randint(20, 80))
        wait_random(0.3, 0.7)
        
        if use_human_movement:
            # Elegir tipo de movimiento aleatorio
            movement_type = random.choice(['zigzag', 'circle', 'bezier', 'direct'])
            
            if movement_type == 'zigzag':
                move_mouse_zigzag(driver, element, steps=random.randint(4, 7))
            elif movement_type == 'circle':
                move_mouse_in_circle(driver, element, radius=random.randint(30, 60))
            elif movement_type == 'bezier':
                move_mouse_bezier_curve(driver, element, control_points=random.randint(2, 4))
            else:
                # Movimiento "directo" con pequeñas desviaciones
                actions = ActionChains(driver)
                actions.move_to_element_with_offset(element, random.randint(-15, 15), random.randint(-10, 10))
                actions.pause(random.uniform(0.2, 0.4))
                actions.move_to_element(element)
                actions.pause(random.uniform(0.2, 0.5))
                actions.perform()
        else:
            # Movimiento simple
            actions = ActionChains(driver)
            actions.move_to_element(element)
            actions.pause(random.uniform(0.3, 0.7))
            actions.perform()
        
        # Pausa antes del click
        wait_random(0.4, 0.9)
        
        # Click con pequeña variación
        actions = ActionChains(driver)
        actions.click()
        actions.pause(random.uniform(0.08, 0.18))
        actions.perform()
        
        log("Click exitoso con movimientos humanos")
        return True
        
    except Exception as e:
        log(f"Movimientos humanos fallaron: {e}, intentando JavaScript...")
        
        try:
            wait_random(0.3, 0.6)
            driver.execute_script("arguments[0].click();", element)
            log("Click exitoso (JavaScript fallback)")
            return True
            
        except Exception as e2:
            log(f"JavaScript click también falló: {e2}")
            return False


def human_click_offset(driver: WebDriver, element: WebElement, offset_x: int, offset_y: int):
    """
    Click en una posición relativa al elemento.
    Usado para el clic especial en Función Judicial (75px arriba del botón).
    
    Args:
        driver: WebDriver
        element: Elemento de referencia
        offset_x: Desplazamiento horizontal
        offset_y: Desplazamiento vertical (negativo = arriba)
    """
    log(f"Click con offset: x={offset_x}, y={offset_y}")
    
    try:
        # Movimiento humano hacia la posición
        movement_type = random.choice(['bezier', 'zigzag'])
        
        if movement_type == 'bezier':
            # Curva Bézier hacia la posición
            points = []
            for i in range(3):
                rand_x = random.randint(-60, 60)
                rand_y = random.randint(offset_y - 40, offset_y + 40)
                points.append((rand_x, rand_y))
            points.append((offset_x, offset_y))
            
            steps = 10
            for step in range(steps):
                t = step / (steps - 1)
                idx = int(t * (len(points) - 1))
                if idx >= len(points) - 1:
                    off_x, off_y = points[-1]
                else:
                    p1 = points[idx]
                    p2 = points[idx + 1]
                    local_t = (t * (len(points) - 1)) - idx
                    off_x = p1[0] + (p2[0] - p1[0]) * local_t
                    off_y = p1[1] + (p2[1] - p1[1]) * local_t
                
                actions = ActionChains(driver)
                actions.move_to_element_with_offset(element, int(off_x), int(off_y))
                actions.pause(random.uniform(0.05, 0.12))
                actions.perform()
        
        else:  # zigzag
            steps = 6
            for i in range(1, steps + 1):
                progress = i / steps
                lateral = random.randint(-40, 40) if i < steps else offset_x
                vertical = int(offset_y * progress)
                
                actions = ActionChains(driver)
                actions.move_to_element_with_offset(element, lateral, vertical)
                actions.pause(random.uniform(0.08, 0.18))
                actions.perform()
        
        # Click en la posición
        wait_random(0.5, 1.0)
        actions = ActionChains(driver)
        actions.pause(random.uniform(0.15, 0.35))
        actions.click()
        actions.pause(random.uniform(0.08, 0.18))
        actions.perform()
        
        log("Click con offset exitoso")
        return True
        
    except Exception as e:
        log(f"Click con offset falló: {e}")
        return False