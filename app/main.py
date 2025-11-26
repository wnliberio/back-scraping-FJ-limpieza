# app/main.py - VERSIÓN CON DAEMON AUTOMÁTICO

# ⚠️ CRÍTICO: Cargar .env ANTES de cualquier otra importación
from app.load_env import verificar_credenciales

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.services.load_client_data import load_client_data
from app.db import get_db
from app.db import get_db_for_data_load
from sqlalchemy.orm import Session

app = FastAPI(
    title="Sistema de Consultas Función Judicial",
    description="Sistema automatizado con procesamiento en background",
    version="3.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
        "*"  # Para desarrollo
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== IMPORTAR ROUTERS =====

# Router de Tracking (principal)
try:
    from app.routers.tracking_professional import router as tracking_router
    app.include_router(tracking_router, prefix="/api")
    print("✅ Router tracking professional cargado")
except ImportError as e:
    print(f"❌ Error cargando router tracking: {e}")

# Router del Daemon (NUEVO)
try:
    from app.routers.daemon import router as daemon_router
    app.include_router(daemon_router, prefix="/api")
    print("✅ Router daemon cargado")
except ImportError as e:
    print(f"❌ Error cargando router daemon: {e}")

# Router de Reports (si existe)
try:
    from app.routers.reports import router as reports_router
    app.include_router(reports_router, prefix="/api")
    print("✅ Router reports cargado")
except ImportError as e:
    print(f"⚠️ Router reports no disponible: {e}")

# ===== EVENTOS DE STARTUP =====

@app.on_event("startup")
async def startup_event():
    print("🚀 Iniciando Sistema de Consultas v3.0")

    # --- Verificar DB destino ---
    try:
        from app.db import engine
        print("✅ Conexión a DB destino verificada")
    except Exception as e:
        print(f"❌ Error de conexión a DB destino: {e}")

    # --- Verificar DB origen ---
    try:
        from app.db.origen import test_origen_connection
        if test_origen_connection():
            print("✅ Conexión a DB origen verificada")
    except Exception as e:
        print(f"❌ Error de conexión a DB origen: {e}")


    # --- Cargar datos si ambas DB OK ---
    if origen_db:
        try:
            from app.db import get_db
            destino_db = next(get_db())
            load_client_data(
                origen_db=origen_db,
                destino_db=destino_db,
                start_date='2025-09-29',
                end_date='2025-09-30'
            )
            destino_db.close()
            print("✅ Datos cargados exitosamente en de_clientes_rpa")
        except Exception as e:
            print(f"⚠️ Error cargando datos: {e}")
        finally:
            origen_db.close()

    print("🎯 Sistema listo para recibir requests")

@app.on_event("shutdown")
async def shutdown_event():
    """Limpieza al cerrar el sistema"""
    print("🛑 Cerrando sistema...")
    
    # Detener daemon si está corriendo
    try:
        from app.services.daemon_procesador import detener_daemon, obtener_estado_daemon
        estado = obtener_estado_daemon()
        if estado.get('running'):
            print("⏹️  Deteniendo daemon...")
            detener_daemon()
            print("✅ Daemon detenido")
    except Exception as e:
        print(f"⚠️ Error deteniendo daemon: {e}")
    
    print("👋 Sistema cerrado")

# ===== ENDPOINTS RAÍZ =====

@app.get("/")
def root():
    """Endpoint raíz con información del sistema"""
    return {
        "ok": True,
        "service": "Sistema de Consultas Función Judicial",
        "version": "3.0.0",
        "features": {
            "tracking_granular": True,
            "procesamiento_automatico": True,
            "daemon_controlable": True,
            "solo_funcion_judicial": True
        },
        "endpoints": {
            "daemon": [
                "/api/daemon/iniciar",
                "/api/daemon/detener",
                "/api/daemon/estado"
            ],
            "tracking": [
                "/api/tracking/health",
                "/api/tracking/paginas",
                "/api/tracking/clientes"
            ]
        },
        "docs": "/docs",
        "status": "active"
    }

@app.get("/health")
def health_check():
    """Health check completo del sistema"""
    health_status = {
        "status": "healthy",
        "timestamp": "2025-01-20T00:00:00Z",
        "version": "3.0.0",
        "components": {}
    }
    
    # Verificar BD
    try:
        from app.db import SessionLocal
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        health_status["components"]["database"] = "ok"
    except Exception as e:
        health_status["components"]["database"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Verificar tracking
    try:
        from app.services.tracking_professional import get_paginas_activas
        paginas = get_paginas_activas()
        health_status["components"]["tracking"] = f"ok ({len(paginas)} páginas)"
    except Exception as e:
        health_status["components"]["tracking"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Verificar daemon
    try:
        from app.services.daemon_procesador import obtener_estado_daemon
        estado = obtener_estado_daemon()
        health_status["components"]["daemon"] = "running" if estado["running"] else "stopped"
    except Exception as e:
        health_status["components"]["daemon"] = f"error: {str(e)}"
    
    return health_status
    