#core/utils/screenshot
import os, base64
from datetime import datetime
from ..config import SCREENSHOT_DIR


def save_element_full_screenshot_cdp(driver, element, basename: str) -> str:
    """
    Captura un elemento completo usando CDP, incluso si es más grande que el viewport.
    Retorna la RUTA ABSOLUTA del archivo guardado.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{basename}_{ts}.png"
    outpath = os.path.join(SCREENSHOT_DIR, filename)
    
    try:
        # Habilitar CDP
        driver.execute_cdp_cmd("Page.enable", {})
        
        # Obtener las coordenadas y dimensiones del elemento
        location = element.location
        size = element.size
        
        # Asegurar que las dimensiones sean válidas
        x = float(location['x'])
        y = float(location['y'])
        width = float(size['width'])
        height = float(size['height'])
        
        if width <= 0 or height <= 0:
            raise Exception(f"Dimensiones inválidas: width={width}, height={height}")
        
        # Capturar usando CDP con las coordenadas exactas del elemento
        result = driver.execute_cdp_cmd("Page.captureScreenshot", {
            "format": "png",
            "captureBeyondViewport": True,
            "fromSurface": True,
            "clip": {
                "x": x,
                "y": y,
                "width": width,
                "height": height,
                "scale": 1
            }
        })
        
        # Guardar la imagen
        with open(outpath, "wb") as f:
            f.write(base64.b64decode(result["data"]))
        
        return os.path.abspath(outpath)
        
    except Exception as e:
        print(f"ERROR en captura CDP del elemento: {e}")
        print(f"Ubicación: {element.location}, Tamaño: {element.size}")
        # Fallback a captura completa
        return save_fullpage_png(driver, basename + "_fallback")


def save_element_screenshot_png(driver, element, basename: str) -> str:
    """
    Captura un elemento usando el mejor método disponible.
    Primero intenta CDP para captura completa, luego fallback a método estándar.
    """
    try:
        # Intentar captura completa con CDP
        return save_element_full_screenshot_cdp(driver, element, basename)
    except Exception as e:
        print(f"CDP falló: {e}. Intentando método estándar...")
        
        # Fallback al método estándar de Selenium
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{basename}_{ts}.png"
        outpath = os.path.join(SCREENSHOT_DIR, filename)
        
        try:
            element.screenshot(outpath)
            return os.path.abspath(outpath)
        except Exception as e2:
            print(f"Método estándar también falló: {e2}")
            # Último recurso: captura completa de página
            return save_fullpage_png(driver, basename + "_complete_fallback")


def save_element_by_selector_png(driver, selector: str, basename: str, by_xpath: bool = False) -> str:
    """
    Captura un elemento encontrado por selector CSS o XPath.
    Retorna la RUTA ABSOLUTA del archivo guardado.
    """
    from selenium.webdriver.common.by import By
    
    try:
        if by_xpath:
            element = driver.find_element(By.XPATH, selector)
        else:
            element = driver.find_element(By.CSS_SELECTOR, selector)
        
        return save_element_screenshot_png(driver, element, basename)
        
    except Exception as e:
        print(f"No se pudo capturar elemento '{selector}': {e}")
        return save_fullpage_png(driver, basename + "_fallback")
    
    # core/utils/screenshot.py
import os, base64, time
from core.utils.log import log

# Carpeta de salida por defecto (ajústala si usas otra)
OUTPUT_DIR = os.getenv("SCREENSHOT_DIR", "sri_ruc_output/screenshots")

def _ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def _abs_path(basename: str) -> str:
    filename = f"{basename}.png" if not basename.lower().endswith(".png") else basename
    path = os.path.join(OUTPUT_DIR, filename)
    _ensure_dir(path)
    return os.path.abspath(path)

def save_fullpage_png(driver, basename: str) -> str:
    """
    Captura de pantalla COMPLETA, sin importar el viewport.
    - Chrome/Chromium: usa CDP Page.captureScreenshot con captureBeyondViewport.
    - Firefox: usa get_full_page_screenshot_as_file (si está disponible) o hace fallback.
    Devuelve la ruta absoluta del PNG.
    """
    path = _abs_path(basename)

    # Detectar navegador
    name = (driver.capabilities.get("browserName") or "").lower()

    try:
        if "chrome" in name or "chromium" in name or "edge" in name:
            # Asegurar que el dominio Page esté habilitado
            try:
                driver.execute_cdp_cmd("Page.enable", {})
            except Exception:
                pass

            # Obtener tamaño real del contenido
            metrics = driver.execute_cdp_cmd("Page.getLayoutMetrics", {})
            # contentSize existe desde Chrome 85+
            cs = metrics.get("contentSize") or metrics.get("cssContentSize") or {}
            content_width = int(cs.get("width", 0)) or int(
                driver.execute_script(
                    "return Math.max(document.body.scrollWidth, document.documentElement.scrollWidth);"
                )
            )
            content_height = int(cs.get("height", 0)) or int(
                driver.execute_script(
                    "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);"
                )
            )

            # Fijar métricas para que la captura abarque todo
            driver.execute_cdp_cmd(
                "Emulation.setDeviceMetricsOverride",
                {
                    "mobile": False,
                    "width": content_width,
                    "height": content_height,
                    "deviceScaleFactor": 1,
                    "scale": 1,
                },
            )

            # Capturar más allá del viewport
            result = driver.execute_cdp_cmd(
                "Page.captureScreenshot",
                {
                    "fromSurface": True,
                    "captureBeyondViewport": True,
                    # Si quisieras clip preciso:
                    # "clip": {"x": 0, "y": 0, "width": content_width, "height": content_height, "scale": 1}
                },
            )
            img_bytes = base64.b64decode(result["data"])
            with open(path, "wb") as f:
                f.write(img_bytes)
            log(f"📸 Fullpage (CDP) guardado en: {path}")
            return path

        elif "firefox" in name:
            # Firefox tiene API nativa de full page
            if hasattr(driver, "get_full_page_screenshot_as_file"):
                driver.get_full_page_screenshot_as_file(path)
                log(f"📸 Fullpage (Firefox nativo) guardado en: {path}")
                return path

            # Fallback muy básico si la API no existe
            driver.save_screenshot(path)
            log(f"📸 Viewport (fallback Firefox) guardado en: {path}")
            return path

        else:
            # Otros navegadores: mejor esfuerzo
            # Intento agrandar ventana al tamaño del documento y capturar
            total_width = driver.execute_script(
                "return Math.max(document.body.scrollWidth, document.documentElement.scrollWidth);"
            )
            total_height = driver.execute_script(
                "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);"
            )
            try:
                driver.set_window_size(total_width, total_height)
                time.sleep(0.2)
            except Exception:
                pass
            driver.save_screenshot(path)
            log(f"📸 Captura (best-effort) guardada en: {path}")
            return path

    except Exception as e:
        # Último fallback a viewport si algo falla
        log(f"⚠️ save_fullpage_png falló, usando viewport: {e}")
        driver.save_screenshot(path)
        return path


# --- AÑADIR EN: core/utils/screenshot.py ---
import os, time
from core.utils.log import log

try:
    from PIL import Image
    _HAS_PILLOW = True
except Exception:
    _HAS_PILLOW = False

OUTPUT_DIR = os.getenv("SCREENSHOT_DIR", "sri_ruc_output/screenshots")

def _ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def _abs_path(basename: str) -> str:
    filename = f"{basename}.png" if not basename.lower().endswith(".png") else basename
    path = os.path.join(OUTPUT_DIR, filename)
    _ensure_dir(path)
    return os.path.abspath(path)

def _abs_path_parts(basename: str, idx: int) -> str:
    filename = f"{basename}_part_{idx:02d}.png"
    path = os.path.join(OUTPUT_DIR, filename)
    _ensure_dir(path)
    return os.path.abspath(path)

def save_scrollable_container_png(driver, css_selector: str, basename: str, overlap_px: int = 40) -> str:
    """
    Captura FULL del contenido de un CONTENEDOR SCROLLEABLE (div) por tramos y (si hay Pillow) los cose.
    - Usa element.screenshot() (recorte exacto del elemento visible) y va haciendo scroll vertical interno.
    - Si no hay Pillow, deja las partes y devuelve la primera.

    Params:
      css_selector: selector CSS del contenedor con scroll (el que muestra la tabla/lista).
      basename: nombre base del PNG final.
      overlap_px: solape entre tramos para evitar líneas de unión visibles.

    Devuelve:
      Ruta al PNG final si se pudo coser; si no, ruta de la primera parte.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    out = _abs_path(basename)

    # 1) Encontrar elemento scrolleable
    try:
        elem = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
        )
    except Exception as e:
        log(f"❌ No se encontró contenedor scrolleable '{css_selector}': {e}")
        # Fallback: viewport
        driver.save_screenshot(out)
        return out

    # 2) Medidas del contenedor
    get_metrics_js = """
      const el = arguments[0];
      return {
        scrollHeight: el.scrollHeight,
        clientHeight: el.clientHeight,
        scrollTop: el.scrollTop
      };
    """
    m = driver.execute_script(get_metrics_js, elem)
    total = int(m["scrollHeight"])
    vh = int(m["clientHeight"])

    if total <= vh + 5:
        # Cabe en una sola vista, capturar una vez
        elem.screenshot(out)
        log(f"📸 Contenedor (una vista) guardado: {out}")
        return out

    # 3) Capturar por tramos
    parts = []
    y = 0
    idx = 1
    step = max(vh - overlap_px, 100)

    # Posicionar arriba
    driver.execute_script("arguments[0].scrollTo(0, 0);", elem)
    time.sleep(0.25)

    while y < total:
        driver.execute_script(f"arguments[0].scrollTo(0, {y});", elem)
        time.sleep(0.12)  # deja pintar
        part_path = _abs_path_parts(basename, idx)
        elem.screenshot(part_path)  # recorta SOLO el elemento, no el viewport
        parts.append(part_path)
        y += step
        idx += 1

        # Evitar bucle infinito si algo raro pasa
        if idx > 300:
            break

    # 4) Intentar coser
    if _HAS_PILLOW:
        try:
            images = [Image.open(p) for p in parts]
            widths = [im.size[0] for im in images]
            heights = [im.size[1] for im in images]
            max_w = max(widths)
            # Al coser, quitar el solape de todas menos la primera
            total_h = heights[0] + sum(h - overlap_px for h in heights[1:])

            stitched = Image.new("RGB", (max_w, total_h))
            yoff = 0
            for i, im in enumerate(images):
                if i == 0:
                    stitched.paste(im, (0, yoff))
                    yoff += im.size[1]
                else:
                    # recortar el solape superior
                    crop = im.crop((0, overlap_px, im.size[0], im.size[1]))
                    stitched.paste(crop, (0, yoff - overlap_px))
                    yoff += (im.size[1] - overlap_px)

            stitched.save(out)
            for im in images:
                im.close()
            # Limpia partes
            for p in parts:
                try:
                    os.remove(p)
                except Exception:
                    pass
            log(f"🧵 PNG cosido (contenedor) guardado: {out}")
            return out
        except Exception as e:
            log(f"⚠️ No se pudo coser (Pillow): {e}")

    # 5) Sin Pillow: devolver primera parte
    log(f"📚 Guardadas {len(parts)} partes del contenedor. Instala Pillow para coser.")
    return parts[0] if parts else out
