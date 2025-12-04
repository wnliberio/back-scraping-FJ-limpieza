# db/origen.py
# db/origen.py
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from connect_db2 import conn_str  # usamos la cadena ODBC de connect_db2.py
import urllib

# -------------------------------
# 1️⃣ Crear engine usando SQLAlchemy + pyodbc
# -------------------------------
def get_origen_engine():
    """
    Crea un engine de SQLAlchemy usando pyodbc.
    """
    # Escapamos la cadena ODBC para SQLAlchemy
    odbc_conn_str = urllib.parse.quote_plus(conn_str)
    
    # Usamos el dialecto pyodbc con SQLAlchemy
    DATABASE_URL = f"mssql+pyodbc:///?odbc_connect={odbc_conn_str}"  # funciona con ODBC
    
    engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
    print("✅ Engine de base de datos creado correctamente")
    return engine

# -------------------------------
# 2️⃣ Session local
# -------------------------------
OrigenSessionLocal = sessionmaker(
    bind=get_origen_engine(),
    autoflush=False,
    autocommit=False,
    future=True
)

# -------------------------------
# 3️⃣ Función para obtener sesión
# -------------------------------
def get_origen_db():
    """
    Generador para usar la sesión de SQLAlchemy en contextos.
    Ejemplo:
        with next(get_origen_db()) as db:
            db.execute(...)
    """
    db = OrigenSessionLocal()
    try:
        yield db
    finally:
        db.close()

# -------------------------------
# 4️⃣ Función de prueba de conexión
# -------------------------------
def test_origen_connection():
    """
    Prueba la conexión a la DB origen ejecutando un SELECT 1
    """
    try:
        with next(get_origen_db()) as db:
            result = db.execute(text("SELECT 1 AS test")).fetchone()
            print("✅ Conexión a DB origen exitosa")
            print(f"   Resultado del test: {result}")
            return True
    except Exception as e:
        print(f"❌ Error de conexión a DB origen: {e}")
        return False
