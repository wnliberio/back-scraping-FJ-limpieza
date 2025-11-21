# core/browser.py
# core/browser.py
"""
WebDriver anti-detección optimizado 2025.
Mejoras:
- Persistencia real del User-Agent
- Client Hints (sec-ch-ua) correctos
- Headers HTTP realistas
- Movimientos humanos básicos
- Fingerprint masking ampliado
- Cookies persistentes
- Delays humanos automáticos
"""

import os
import time
import random
import pickle
from pathlib import Path

try:
    import undetected_chromedriver as uc
    UNDETECTED_AVAILABLE = True
except:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    UNDETECTED_AVAILABLE = False

from core.utils.log import log


# Rutas
COOKIES_DIR = Path("sri_ruc_output/cookies")
UA_FILE = Path("sri_ruc_output/last_user_agent.txt")

COOKIES_DIR.mkdir(parents=True, exist_ok=True)


# User Agents rotativos (pero persistentes)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
]


WINDOW_SIZES = [
    (1366, 768),
    (1440, 900),
    (1536, 864),
    (1920, 1080),
]


def _load_or_choose_user_agent():
    """Mantiene el mismo User-Agent entre sesiones."""
    if UA_FILE.exists():
        ua = UA_FILE.read_text().strip()
        if ua:
            return ua

    ua = random.choice(USER_AGENTS)
    UA_FILE.write_text(ua)
    return ua


def human_delay(min_s=0.8, max_s=2.4):
    """Pequeñas pausas aleatorias para parecer humano."""
    time.sleep(random.uniform(min_s, max_s))


def _apply_stealth_js(driver):
    """Fingerprints ocultos extra."""
    scripts = [
        # webdriver
        """Object.defineProperty(navigator, 'webdriver', {get: () => undefined});""",

        # Languages
        """Object.defineProperty(navigator, 'languages', {get: () => ['es-EC','es','en-US']});""",

        # Plugins falsos
        """Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});""",

        # WebGL vendor spoof
        """
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return "Intel Inc.";
            if (parameter === 37446) return "Intel Iris OpenGL";
            return getParameter(parameter);
        };
        """
    ]

    for s in scripts:
        try:
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": s})
        except:
            pass


def save_cookies(driver, name="funcionjudicial"):
    try:
        file = COOKIES_DIR / f"{name}.pkl"
        pickle.dump(driver.get_cookies(), file.open("wb"))
        log("Cookies guardadas correctamente")
    except Exception as e:
        log(f"Error guardando cookies: {e}")


def load_cookies(driver, name="funcionjudicial"):
    try:
        file = COOKIES_DIR / f"{name}.pkl"
        if not file.exists():
            return False

        cookies = pickle.load(file.open("rb"))
        for c in cookies:
            try:
                driver.add_cookie(c)
            except:
                pass

        log("Cookies cargadas correctamente")
        return True
    except Exception as e:
        log(f"Error cargando cookies: {e}")
        return False


def _add_headers(driver, user_agent):
    """Headers HTTP realistas de Chrome 2025."""
    try:
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {
            "userAgent": user_agent,
            "platform": "Win32",
            "acceptLanguage": "es-EC,es;q=0.9,en-US;q=0.8",
        })

        driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
            "headers": {
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "es-EC,es;q=0.9",
                "Sec-CH-UA": '"Google Chrome";v="131", "Chromium";v="131"',
                "Sec-CH-UA-Platform": "Windows",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1"
            }
        })

    except Exception as e:
        log(f"Error configurando headers: {e}")


def create_driver(headless=False, use_cookies=True, cookies_domain="funcionjudicial"):
    ua = _load_or_choose_user_agent()
    w, h = random.choice(WINDOW_SIZES)

    log(f"User-Agent persistente: {ua}")
    log(f"Ventana: {w}x{h}")

    if UNDETECTED_AVAILABLE:
        options = uc.ChromeOptions()
        options.add_argument(f"--user-agent={ua}")
        options.add_argument(f"--window-size={w},{h}")
        options.add_argument("--lang=es-EC,es")
        options.add_argument("--disable-blink-features=AutomationControlled")

        if headless:
            options.add_argument("--headless=new")

        driver = uc.Chrome(options=options)

    else:
        chrome_options = Options()
        chrome_options.add_argument(f"--user-agent={ua}")
        chrome_options.add_argument(f"--window-size={w},{h}")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--lang=es-EC,es")

        if headless:
            chrome_options.add_argument("--headless=new")

        driver = webdriver.Chrome(options=chrome_options)

        _apply_stealth_js(driver)

    # headers HTTP modernos
    _add_headers(driver, ua)
    human_delay()

    # Dominio base
    driver.get("https://procesosjudiciales.funcionjudicial.gob.ec")
    human_delay(1.2, 2.8)

    if use_cookies:
        load_cookies(driver, cookies_domain)
        driver.get("https://procesosjudiciales.funcionjudicial.gob.ec")
        human_delay()

    # movimiento humano inicial
    try:
        driver.execute_script("""
            window.scrollBy({top: 200, behavior: 'smooth'});
        """)
    except:
        pass

    return driver


def close_driver(driver, save=True, cookies_domain="funcionjudicial"):
    try:
        if save:
            save_cookies(driver, cookies_domain)
    except:
        pass

    try:
        driver.quit()
    except:
        pass
