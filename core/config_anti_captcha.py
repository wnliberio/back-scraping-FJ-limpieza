# core/config_anti_captcha.py
"""
Configuración específica para evitar captchas.
Ajusta estos valores según la frecuencia de aparición de captchas.
"""

# Delays entre acciones (en segundos)
ANTI_CAPTCHA_DELAYS = {
    # Delay después de cargar página inicial
    "initial_load": (5.0, 8.0),
    
    # Delay después de escribir en campos
    "after_typing": (2.0, 4.0),
    
    # Delay después de cada clic
    "after_click": (2.5, 4.0),
    
    # Delay después de navegar a nueva página en paginador
    "after_page_navigation": (3.0, 5.0),
    
    # Delay antes de cerrar el navegador
    "before_closing": (3.0, 5.0),
    
    # Delay entre consultas completas (MUY IMPORTANTE)
    "between_queries": (180.0, 300.0),  # 3-5 minutos
}

# Límites de uso
MAX_QUERIES_PER_HOUR = 8  # Máximo 8 consultas por hora
MAX_QUERIES_PER_DAY = 50   # Máximo 50 consultas por día

# Configuración de User Agents
ROTATE_USER_AGENT = True  # Si True, rota user agent en cada consulta

# Cookies
USE_PERSISTENT_COOKIES = True  # Si True, guarda y carga cookies entre sesiones
COOKIES_EXPIRY_DAYS = 7        # Días antes de eliminar cookies antiguas

# Proxy (opcional)
USE_PROXY = False
PROXY_URL = None  # Ejemplo: "http://user:pass@proxy.example.com:8080"
# Para proxies de Ecuador: mejora significativamente el éxito
# Servicios recomendados:
# - BrightData: https://brightdata.com (residential proxies Ecuador)
# - Smartproxy: https://smartproxy.com
# - Oxylabs: https://oxylabs.io

# Detección de captcha
CAPTCHA_INDICATORS = [
    "captcha",
    "recaptcha",
    "hcaptcha",
    "verificación",
    "robot",
    "suspicious activity",
]

# Estrategia si se detecta captcha
ON_CAPTCHA_DETECTED = "abort"  # Opciones: "abort", "retry_later", "manual"
RETRY_DELAY_AFTER_CAPTCHA = 600  # 10 minutos

# Logging
LOG_ANTI_CAPTCHA_EVENTS = True