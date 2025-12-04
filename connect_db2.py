'''
# Código para porbar rápidamente la conexión, pero sin un control de errores profesional

import pyodbc

conn_str = (
    "DRIVER={driver que se este usando};"
    "DATABASE=namedb;"
    "HOSTNAME=ip;"
    "PORT=puerto;"
    "PROTOCOL=TCPIP;"
    "UID=user;"
    "PWD=password;"
)

try:
    conn = pyodbc.connect(conn_str)
    print("✔ Cnnected correct to DB2!")
except Exception as e:
    print("❌ Error de conexión:", e)
'''

import pyodbc # Con pyodbc se crea una conexión permanente.
from dotenv import load_dotenv
import os

# -------------------------------
# 1 Cargar variables del .env 
# -------------------------------
load_dotenv()  # carga automáticamente .env en la raíz del proyecto

driver = os.getenv("DB_DRIVER")
database = os.getenv("DB_DATABASE")
hostname = os.getenv("DB_HOSTNAME")
port = os.getenv("DB_PORT")
protocol = os.getenv("DB_PROTOCOL")
uid = os.getenv("DB_UID")
pwd = os.getenv("DB_PWD")

# Validar que todas las variables existan
missing_vars = [v for v, name in zip(
    [driver, database, hostname, port, protocol, uid, pwd],
    ["DB_DRIVER","DB_DATABASE","DB_HOSTNAME","DB_PORT","DB_PROTOCOL","DB_UID","DB_PWD"]
) if not v]

if missing_vars:
    print(f"❌ Faltan estas variables en el .env: {missing_vars}")
    exit(1)

# -------------------------------
# 2️ Construir cadena de conexión
# -------------------------------
conn_str = (
    f"DRIVER={driver};"
    f"DATABASE={database};"
    f"HOSTNAME={hostname};"
    f"PORT={port};"
    f"PROTOCOL={protocol};"
    f"UID={uid};"
    f"PWD={pwd};"
)

# -------------------------------
# 3️ Intentar conexión
# -------------------------------
try:
    conn = pyodbc.connect(conn_str)
    print("✔ Conectado correctamente a DB2!")
except pyodbc.InterfaceError as ie:
    print("❌ Error de interfaz ODBC:", ie)
except pyodbc.Error as e:
    print("❌ Error general de conexión:", e)
except Exception as ex:
    print("❌ Otro error inesperado:", ex)
