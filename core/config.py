# core/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# === CapSolver ===
CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "CAP-REEMPLAZA-CON-TU-KEY")
CAPSOLVER_URL = "https://api-stable.capsolver.com/createTask"
CAPSOLVER_RESULT_URL = "https://api-stable.capsolver.com/getTaskResult"

# === URLs ===
SRI_URL = os.getenv(
    "SRI_BASE_URL",
    "https://srienlinea.sri.gob.ec/sri-en-linea/SriRucWeb/ConsultaRuc/Consultas/consultaRuc"
)
SRI_DEUDAS_URL = os.getenv(
    "SRI_DEUDAS_URL",
    "https://srienlinea.sri.gob.ec/sri-en-linea/SriPagosWeb/ConsultaDeudasFirmesImpugnadas/Consultas/consultaDeudasFirmesImpugnadas"
)
FISCALIAS_DENUNCIAS_URL = os.getenv(
    "FISCALIAS_DENUNCIAS_URL",
    "https://www.gestiondefiscalias.gob.ec/siaf/informacion/web/noticiasdelito/index.php"
)

# === Salida base ===
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "sri_ruc_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Capturas finales
SCREENSHOT_DIR = os.path.join(OUTPUT_DIR, "screenshots")
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# Caché
CACHE_HOURS = int(os.getenv("CACHE_HOURS", "24"))

# Delays humanos
TYPE_BASE_DELAY = float(os.getenv("TYPE_BASE_DELAY", "0.7"))
TYPE_JITTER = float(os.getenv("TYPE_JITTER", "0.15"))
TYPE_PUNCT_PAUSE = float(os.getenv("TYPE_PUNCT_PAUSE", "0.6"))

# Timeouts
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "60"))
RESULTS_TIMEOUT = int(os.getenv("RESULTS_TIMEOUT", "60"))
RECAPTCHA_TIMEOUT = int(os.getenv("RECAPTCHA_TIMEOUT", "180"))

# Reintentos
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))

# Movimiento de mouse
MOUSE_MIN_TIME = float(os.getenv("MOUSE_MIN_TIME", "0.7"))
MOUSE_MAX_TIME = float(os.getenv("MOUSE_MAX_TIME", "1.2"))
MOUSE_STEPS = int(os.getenv("MOUSE_STEPS", "28"))
MOUSE_JITTER = int(os.getenv("MOUSE_JITTER", "2"))

# === LOGS (archivo único) ===
LOG_DIR = os.path.join(OUTPUT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "app.log")

# === Demora entre items (secuencial) ===
INTER_ITEM_DELAY_SECONDS = int(os.getenv("INTER_ITEM_DELAY_SECONDS", "3"))
