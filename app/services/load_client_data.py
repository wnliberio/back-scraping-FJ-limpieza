# app/services/load_client_data.py

from sqlalchemy import text

def load_client_data(origen_db, destino_db, start_date: str, end_date: str):
    """
    Carga datos desde la DB origen hacia la DB destino.
    
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

    for row in result:
        destino_db.execute(
            """
            INSERT INTO de_clientes_rpa (
                id, nombre, apellido, ci, ruc, tipo, monto, fecha, estado, fecha_creacion
            ) VALUES (
                :id, :nombre, :apellido, :ci, :ruc, :tipo, :monto, :fecha, :estado, :fecha_creacion
            )
            """,
            {
                "id": row["ID_SOLICITUD"],
                "nombre": row["NOMBRES_CLIENTE"],
                "apellido": row["APELLIDOS_CLIENTE"],
                "ci": row["CEDULA"],
                "ruc": None,
                "tipo": "cliente",
                "monto": None,
                "fecha": row["FECHA_CREACION_SOLICITUD"],
                "estado": "Pendiente",
                "fecha_creacion": row["FECHA_CREACION_SOLICITUD"]
            }
        )

    destino_db.commit()
