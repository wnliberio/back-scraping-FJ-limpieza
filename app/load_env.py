# app/load_env.py
"""
Carga las variables de entorno desde el archivo .env
DEBE EJECUTARSE ANTES QUE CUALQUIER OTRA IMPORTACIÓN
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Encontrar el archivo .env
# Asume que está en la raíz del proyecto (un nivel arriba de app/)
env_path = Path(__file__).parent.parent / ".env"

# Cargar variables de entorno
load_dotenv(dotenv_path=env_path, override=True)

# Verificar que se cargaron las credenciales
def verificar_credenciales():
    """Verifica que las credenciales de BD se hayan cargado correctamente"""
    required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME"]
    missing = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        print(f"⚠️ ADVERTENCIA: Variables faltantes en .env: {', '.join(missing)}")
        return False
    
    print(f"✅ Credenciales de BD cargadas correctamente:")
    #print(f"   - DB_USER: {os.getenv('DB_USER')}")
    #print(f"   - DB_HOST: {os.getenv('DB_HOST')}")
    #print(f"   - DB_NAME: {os.getenv('DB_NAME')}")
    #print(f"   - DB_PASSWORD: {'*' * len(os.getenv('DB_PASSWORD', ''))}")
    return True

# Auto-verificar al importar
if __name__ != "__main__":
    verificar_credenciales()