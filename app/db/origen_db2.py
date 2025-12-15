# app/db/origen_db2.py
"""
Conexión DIRECTA a DB2 usando pyodbc (SIN SQLAlchemy)
"""

import pyodbc
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import os

load_dotenv()

DB_DRIVER = os.getenv("DB_DRIVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_HOSTNAME = os.getenv("DB_HOSTNAME")
DB_PORT = os.getenv("DB_PORT")
DB_PROTOCOL = os.getenv("DB_PROTOCOL")
DB_UID = os.getenv("DB_UID")
DB_PWD = os.getenv("DB_PWD")

_missing = [v for v, name in zip(
    [DB_DRIVER, DB_DATABASE, DB_HOSTNAME, DB_PORT, DB_PROTOCOL, DB_UID, DB_PWD],
    ["DB_DRIVER", "DB_DATABASE", "DB_HOSTNAME", "DB_PORT", "DB_PROTOCOL", "DB_UID", "DB_PWD"]
) if not v]

if _missing:
    raise RuntimeError(f"❌ Faltan en .env: {', '.join(_missing)}")

CONN_STR = (
    f"DRIVER={DB_DRIVER};"
    f"DATABASE={DB_DATABASE};"
    f"HOSTNAME={DB_HOSTNAME};"
    f"PORT={DB_PORT};"
    f"PROTOCOL={DB_PROTOCOL};"
    f"UID={DB_UID};"
    f"PWD={DB_PWD};"
)

print(f"✅ Cadena de conexión DB2 construida: {DB_HOSTNAME}:{DB_PORT}/{DB_DATABASE}")


def conectar_db2() -> pyodbc.Connection:
    """Establece conexión directa a DB2"""
    try:
        conn = pyodbc.connect(CONN_STR)
        return conn
    except pyodbc.InterfaceError as e:
        raise RuntimeError(f"❌ Error de interfaz ODBC: {str(e)}")
    except pyodbc.Error as e:
        raise RuntimeError(f"❌ Error de conexión a DB2: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"❌ Error inesperado: {str(e)}")


def ejecutar_query_db2(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    fetch_all: bool = True
) -> List[Dict[str, Any]]:
    """Ejecuta query en DB2 y retorna resultados como lista de dicts"""
    conn = None
    try:
        conn = conectar_db2()
        cursor = conn.cursor()
        
        if params:
            valores = list(params.values())
            cursor.execute(query, valores)
        else:
            cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        
        if fetch_all:
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        else:
            row = cursor.fetchone()
            return [dict(zip(columns, row))] if row else []
        
    except Exception as e:
        raise RuntimeError(f"❌ Error ejecutando query: {str(e)}")
    finally:
        if conn:
            conn.close()


def test_conexion_db2() -> bool:
    """Prueba la conexión a DB2"""
    try:
        conn = conectar_db2()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS test FROM SYSIBM.SYSDUMMY1")
        result = cursor.fetchone()
        conn.close()
        
        print("✅ Conexión a DB2 exitosa")
        return True
    except Exception as e:
        print(f"❌ Error de conexión a DB2: {str(e)}")
        return False


def obtener_clientes_db2(
    fecha_desde: str,
    fecha_hasta: str
) -> List[Dict[str, Any]]:
    """
    Obtiene clientes desde DB2 en rango de fechas
    
    NOTA:
    - Sin SELECT DISTINCT por incompatibilidad con ORDER BY en DB2
    - Query validada: OK en DB2
    """
    
    # QUERY CORREGIDA - SIN SELECT DISTINCT (causa conflicto con ORDER BY)
    query = """
    SELECT 
        SC.ID_SOLICITUD,
        DATE(SC.FECHA_HORA_SOLIC) AS FECHA_CREACION_SOLICITUD,
        (SELECT S.DESC_ESTADO FROM CR_ESTADO_SEG S WHERE S.ID_ESTADO = SC.ESTADO) AS ESTADO,
        (SELECT H.DESCRIP_ORIGEN FROM AH_ORIGEN H WHERE H.ID_AGENCIA = SC.AGENCIA) AS AGENCIA,
        PR.ID_PRODUCTO,
        PR.PRODUCTO,
        CC.CEDULA,
        RTRIM(CC.NOMBRES) AS NOMBRES_CLIENTE,
        RTRIM(CC.APELLIDOS) AS APELLIDOS_CLIENTE,
        CC.ESTADO_CIVIL,
        CO.CEDULA_CONYUGE,
        CO.NOMBRES_CONYUGE,
        CO.APELLIDOS_CONYUGE,
        IT.CEDULA_CODEUDOR,
        IT.NOMBRES_CODEUDOR,
        IT.APELLIDOS_CODEUDOR
    FROM CL_CLIENTE CC
    INNER  JOIN CL_SOLICITUD_CRED SC ON SC.ID_CLIENTE = CC.ID_CLIENTE
    INNER JOIN (
        SELECT R.ID_SOLICITUD, O.ID_PRODUCTO, O.NOM_PRODUCTO AS PRODUCTO
        FROM CR_OPERACION R
        INNER JOIN CR_PRODUCTO_OC O ON R.PRODUCTO = O.ID_PRODUCTO
    ) AS PR ON PR.ID_SOLICITUD = SC.ID_SOLICITUD
    LEFT JOIN (
        SELECT T.ID_SOLICITUD, C.CEDULA AS CEDULA_CONYUGE, RTRIM(C.NOMBRES) AS NOMBRES_CONYUGE, RTRIM(C.APELLIDOS) AS APELLIDOS_CONYUGE
        FROM CL_INTEGRANTE_CRED T
        INNER JOIN CL_CLIENTE C ON T.ID_CONYUGUE = C.ID_CLIENTE
        WHERE T.TIPO = 'DEUDOR'
    ) AS CO ON CO.ID_SOLICITUD = SC.ID_SOLICITUD
    LEFT JOIN (
        SELECT T.ID_SOLICITUD, C.CEDULA AS CEDULA_CODEUDOR, RTRIM(C.NOMBRES) AS NOMBRES_CODEUDOR, RTRIM(C.APELLIDOS) AS APELLIDOS_CODEUDOR
        FROM CL_INTEGRANTE_CRED T
        INNER JOIN CL_CLIENTE C ON T.ID_CLIENTE = C.ID_CLIENTE
        WHERE T.TIPO = 'CODEUDOR'
    ) AS IT ON IT.ID_SOLICITUD = SC.ID_SOLICITUD
    WHERE DATE(SC.FECHA_HORA_SOLIC) BETWEEN ? AND ? AND SC.ESTADO = 'T'
    ORDER BY SC.FECHA_HORA_SOLIC DESC
    """
    
    return ejecutar_query_db2(query, {"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta})