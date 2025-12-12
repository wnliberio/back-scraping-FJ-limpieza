# app/services/load_client_data.py - CORREGIDO para insertar en de_clientes_rpa_v2
"""
Carga datos desde DB origen (DB2) a DB destino (SQL Server)
Inserta en la NUEVA tabla de_clientes_rpa_v2
"""

from sqlalchemy import text
from datetime import datetime

def load_client_data(origen_db, destino_db, start_date: str, end_date: str):
    """
    Carga datos desde la DB origen hacia la DB destino.
    ✅ CORREGIDO: Inserta en de_clientes_rpa_v2 (tabla nueva)
    
    Args:
        origen_db: sesión SQLAlchemy de la DB origen (lectura)
        destino_db: sesión SQLAlchemy de la DB destino (escritura)
        start_date: fecha inicial 'YYYY-MM-DD'
        end_date: fecha final 'YYYY-MM-DD'
    """
    query = text("""
    SELECT DISTINCT 
        SC.ID_SOLICITUD,
        CONVERT(DATE, SC.FECHA_HORA_SOLIC) AS FECHA_CREACION_SOLICITUD,
        (SELECT S.DESC_ESTADO FROM dbo.CR_ESTADO_SEG S WHERE S.ID_ESTADO = SC.ESTADO) AS ESTADO,
        (SELECT H.DESCRIP_ORIGEN FROM dbo.AH_ORIGEN H WHERE H.ID_AGENCIA = SC.AGENCIA) AS AGENCIA,
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
    FROM dbo.CL_CLIENTE CC
    LEFT JOIN dbo.CL_SOLICITUD_CRED SC ON SC.ID_CLIENTE = CC.ID_CLIENTE
    INNER JOIN (
        SELECT R.ID_SOLICITUD, O.ID_PRODUCTO, O.NOM_PRODUCTO AS PRODUCTO
        FROM dbo.CR_OPERACION R
        INNER JOIN dbo.CR_PRODUCTO_OC O ON R.PRODUCTO = O.ID_PRODUCTO
    ) AS PR ON PR.ID_SOLICITUD = SC.ID_SOLICITUD
    LEFT JOIN (
        SELECT T.ID_SOLICITUD, C.CEDULA AS CEDULA_CONYUGE, RTRIM(C.NOMBRES) AS NOMBRES_CONYUGE, RTRIM(C.APELLIDOS) AS APELLIDOS_CONYUGE
        FROM dbo.CL_INTEGRANTE_CRED T
        INNER JOIN dbo.CL_CLIENTE C ON T.ID_CONYUGUE = C.ID_CLIENTE
        WHERE T.TIPO = 'DEUDOR'
    ) AS CO ON CO.ID_SOLICITUD = SC.ID_SOLICITUD
    LEFT JOIN (
        SELECT T.ID_SOLICITUD, C.CEDULA AS CEDULA_CODEUDOR, RTRIM(C.NOMBRES) AS NOMBRES_CODEUDOR, RTRIM(C.APELLIDOS) AS APELLIDOS_CODEUDOR
        FROM dbo.CL_INTEGRANTE_CRED T
        INNER JOIN dbo.CL_CLIENTE C ON T.ID_CLIENTE = C.ID_CLIENTE
        WHERE T.TIPO = 'CODEUDOR'
    ) AS IT ON IT.ID_SOLICITUD = SC.ID_SOLICITUD
    WHERE CONVERT(DATE, SC.FECHA_HORA_SOLIC) BETWEEN :start_date AND :end_date;
    """)

    result = origen_db.execute(query, {"start_date": start_date, "end_date": end_date}).fetchall()

    contador = 0
    for row in result:
        try:
            # ✅ CORREGIDO: Insertar en de_clientes_rpa_v2 (tabla NUEVA)
            destino_db.execute(
                text("""
                    INSERT INTO de_clientes_rpa_v2 (
                        id,
                        ID_SOLICITUD,
                        ESTADO,
                        AGENCIA,
                        ID_PRODUCTO,
                        PRODUCTO,
                        CEDULA,
                        NOMBRES_CLIENTE,
                        APELLIDOS_CLIENTE,
                        ESTADO_CIVIL,
                        CEDULA_CONYUGE,
                        NOMBRES_CONYUGE,
                        APELLIDOS_CONYUGE,
                        CEDULA_CODEUDOR,
                        NOMBRES_CODEUDOR,
                        APELLIDOS_CODEUDOR,
                        ESTADO_CONSULTA,
                        FECHA_CREACION_SOLICITUD,
                        FECHA_CREACION_REGISTRO,
                        FECHA_ULTIMA_CONSULTA
                    ) VALUES (
                        :id,
                        :id_solicitud,
                        :estado,
                        :agencia,
                        :id_producto,
                        :producto,
                        :cedula,
                        :nombres_cliente,
                        :apellidos_cliente,
                        :estado_civil,
                        :cedula_conyuge,
                        :nombres_conyuge,
                        :apellidos_conyuge,
                        :cedula_codeudor,
                        :nombres_codeudor,
                        :apellidos_codeudor,
                        :estado_consulta,
                        :fecha_creacion_solicitud,
                        :fecha_creacion_registro,
                        :fecha_ultima_consulta
                    )
                """),
                {
                    "id": row["ID_SOLICITUD"],
                    "id_solicitud": row["ID_SOLICITUD"],
                    "estado": row["ESTADO"],
                    "agencia": row["AGENCIA"],
                    "id_producto": row["ID_PRODUCTO"],
                    "producto": row["PRODUCTO"],
                    "cedula": row["CEDULA"],
                    "nombres_cliente": row["NOMBRES_CLIENTE"],
                    "apellidos_cliente": row["APELLIDOS_CLIENTE"],
                    "estado_civil": row["ESTADO_CIVIL"],
                    "cedula_conyuge": row["CEDULA_CONYUGE"],
                    "nombres_conyuge": row["NOMBRES_CONYUGE"],
                    "apellidos_conyuge": row["APELLIDOS_CONYUGE"],
                    "cedula_codeudor": row["CEDULA_CODEUDOR"],
                    "nombres_codeudor": row["NOMBRES_CODEUDOR"],
                    "apellidos_codeudor": row["APELLIDOS_CODEUDOR"],
                    "estado_consulta": "Pendiente",
                    "fecha_creacion_solicitud": row["FECHA_CREACION_SOLICITUD"],
                    "fecha_creacion_registro": datetime.now(),
                    "fecha_ultima_consulta": None
                }
            )
            contador += 1
            
        except Exception as e:
            print(f"⚠️ Error insertando registro {row['ID_SOLICITUD']}: {e}")
            continue

    destino_db.commit()
    print(f"✅ {contador} registros cargados en de_clientes_rpa_v2")
EOF
cat /mnt/user-data/outputs/load_client_data_CORREGIDO.py
Salida

# app/services/load_client_data.py - CORREGIDO para insertar en de_clientes_rpa_v2
"""
Carga datos desde DB origen (DB2) a DB destino (SQL Server)
Inserta en la NUEVA tabla de_clientes_rpa_v2
"""

from sqlalchemy import text
from datetime import datetime

def load_client_data(origen_db, destino_db, start_date: str, end_date: str):
    """
    Carga datos desde la DB origen hacia la DB destino.
    ✅ CORREGIDO: Inserta en de_clientes_rpa_v2 (tabla nueva)
    
    Args:
        origen_db: sesión SQLAlchemy de la DB origen (lectura)
        destino_db: sesión SQLAlchemy de la DB destino (escritura)
        start_date: fecha inicial 'YYYY-MM-DD'
        end_date: fecha final 'YYYY-MM-DD'
    """
    query = text("""
    SELECT DISTINCT 
        SC.ID_SOLICITUD,
        CONVERT(DATE, SC.FECHA_HORA_SOLIC) AS FECHA_CREACION_SOLICITUD,
        (SELECT S.DESC_ESTADO FROM dbo.CR_ESTADO_SEG S WHERE S.ID_ESTADO = SC.ESTADO) AS ESTADO,
        (SELECT H.DESCRIP_ORIGEN FROM dbo.AH_ORIGEN H WHERE H.ID_AGENCIA = SC.AGENCIA) AS AGENCIA,
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
    FROM dbo.CL_CLIENTE CC
    LEFT JOIN dbo.CL_SOLICITUD_CRED SC ON SC.ID_CLIENTE = CC.ID_CLIENTE
    INNER JOIN (
        SELECT R.ID_SOLICITUD, O.ID_PRODUCTO, O.NOM_PRODUCTO AS PRODUCTO
        FROM dbo.CR_OPERACION R
        INNER JOIN dbo.CR_PRODUCTO_OC O ON R.PRODUCTO = O.ID_PRODUCTO
    ) AS PR ON PR.ID_SOLICITUD = SC.ID_SOLICITUD
    LEFT JOIN (
        SELECT T.ID_SOLICITUD, C.CEDULA AS CEDULA_CONYUGE, RTRIM(C.NOMBRES) AS NOMBRES_CONYUGE, RTRIM(C.APELLIDOS) AS APELLIDOS_CONYUGE
        FROM dbo.CL_INTEGRANTE_CRED T
        INNER JOIN dbo.CL_CLIENTE C ON T.ID_CONYUGUE = C.ID_CLIENTE
        WHERE T.TIPO = 'DEUDOR'
    ) AS CO ON CO.ID_SOLICITUD = SC.ID_SOLICITUD
    LEFT JOIN (
        SELECT T.ID_SOLICITUD, C.CEDULA AS CEDULA_CODEUDOR, RTRIM(C.NOMBRES) AS NOMBRES_CODEUDOR, RTRIM(C.APELLIDOS) AS APELLIDOS_CODEUDOR
        FROM dbo.CL_INTEGRANTE_CRED T
        INNER JOIN dbo.CL_CLIENTE C ON T.ID_CLIENTE = C.ID_CLIENTE
        WHERE T.TIPO = 'CODEUDOR'
    ) AS IT ON IT.ID_SOLICITUD = SC.ID_SOLICITUD
    WHERE CONVERT(DATE, SC.FECHA_HORA_SOLIC) BETWEEN :start_date AND :end_date;
    """)

    result = origen_db.execute(query, {"start_date": start_date, "end_date": end_date}).fetchall()

    contador = 0
    for row in result:
        try:
            # ✅ CORREGIDO: Insertar en de_clientes_rpa_v2 (tabla NUEVA)
            destino_db.execute(
                text("""
                    INSERT INTO de_clientes_rpa_v2 (
                        id,
                        ID_SOLICITUD,
                        ESTADO,
                        AGENCIA,
                        ID_PRODUCTO,
                        PRODUCTO,
                        CEDULA,
                        NOMBRES_CLIENTE,
                        APELLIDOS_CLIENTE,
                        ESTADO_CIVIL,
                        CEDULA_CONYUGE,
                        NOMBRES_CONYUGE,
                        APELLIDOS_CONYUGE,
                        CEDULA_CODEUDOR,
                        NOMBRES_CODEUDOR,
                        APELLIDOS_CODEUDOR,
                        ESTADO_CONSULTA,
                        FECHA_CREACION_SOLICITUD,
                        FECHA_CREACION_REGISTRO,
                        FECHA_ULTIMA_CONSULTA
                    ) VALUES (
                        :id,
                        :id_solicitud,
                        :estado,
                        :agencia,
                        :id_producto,
                        :producto,
                        :cedula,
                        :nombres_cliente,
                        :apellidos_cliente,
                        :estado_civil,
                        :cedula_conyuge,
                        :nombres_conyuge,
                        :apellidos_conyuge,
                        :cedula_codeudor,
                        :nombres_codeudor,
                        :apellidos_codeudor,
                        :estado_consulta,
                        :fecha_creacion_solicitud,
                        :fecha_creacion_registro,
                        :fecha_ultima_consulta
                    )
                """),
                {
                    "id": row["ID_SOLICITUD"],
                    "id_solicitud": row["ID_SOLICITUD"],
                    "estado": row["ESTADO"],
                    "agencia": row["AGENCIA"],
                    "id_producto": row["ID_PRODUCTO"],
                    "producto": row["PRODUCTO"],
                    "cedula": row["CEDULA"],
                    "nombres_cliente": row["NOMBRES_CLIENTE"],
                    "apellidos_cliente": row["APELLIDOS_CLIENTE"],
                    "estado_civil": row["ESTADO_CIVIL"],
                    "cedula_conyuge": row["CEDULA_CONYUGE"],
                    "nombres_conyuge": row["NOMBRES_CONYUGE"],
                    "apellidos_conyuge": row["APELLIDOS_CONYUGE"],
                    "cedula_codeudor": row["CEDULA_CODEUDOR"],
                    "nombres_codeudor": row["NOMBRES_CODEUDOR"],
                    "apellidos_codeudor": row["APELLIDOS_CODEUDOR"],
                    "estado_consulta": "Pendiente",
                    "fecha_creacion_solicitud": row["FECHA_CREACION_SOLICITUD"],
                    "fecha_creacion_registro": datetime.now(),
                    "fecha_ultima_consulta": None
                }
            )
            contador += 1
            
        except Exception as e:
            print(f"⚠️ Error insertando registro {row['ID_SOLICITUD']}: {e}")
            continue

    destino_db.commit()
    print(f"✅ {contador} registros cargados en de_clientes_rpa_v2")