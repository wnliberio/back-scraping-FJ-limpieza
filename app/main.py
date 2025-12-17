# app/main.py - VERSIÓN FINAL (con health check corregido)

# ⚠️ CRÍTICO: Cargar .env ANTES de cualquier otra importación


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
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

# Router del Daemon
try:
    from app.routers.daemon import router as daemon_router
    app.include_router(daemon_router, prefix="/api")
    print("✅ Router daemon cargado")
except ImportError as e:
    print(f"❌ Error cargando router daemon: {e}")

# Router de Sincronización (NUEVO)
try:
    from app.api.endpoints.sincronizacion import router as sync_router
    app.include_router(sync_router)
    print("✅ Router sincronización cargado")
except ImportError as e:
    print(f"⚠️ Router sincronización no disponible: {e}")

# Router de Reports
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

    # --- Verificar DB origen (DB2) ---
    try:
        from app.db.origen_db2 import test_conexion_db2
        if test_conexion_db2():
            print("✅ Conexión a DB2 verificada")
    except ImportError:
        print("⚠️ DB2 origen no disponible")
    except Exception as e:
        print(f"❌ Error de conexión a DB2: {e}")

    # --- NUEVO: Inicializar Scheduler de Sincronización ---
    try:
        from app.services.scheduler_sincronizacion import inicializar_scheduler
        if inicializar_scheduler():
            print("✅ Scheduler de sincronización inicializado")
        else:
            print("⚠️ No se pudo inicializar scheduler de sincronización")
    except ImportError:
        print("⚠️ Scheduler de sincronización no disponible")
    except Exception as e:
        print(f"⚠️ Error inicializando scheduler: {e}")

    print("🎯 Sistema listo para recibir requests")

@app.on_event("shutdown")
async def shutdown_event():
    """Limpieza al cerrar el sistema"""
    print("🛑 Cerrando sistema...")
    
    # Detener scheduler de sincronización
    try:
        from app.services.scheduler_sincronizacion import detener_scheduler
        detener_scheduler()
    except Exception as e:
        print(f"⚠️ Error deteniendo scheduler: {e}")
    
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
            "sincronizacion_automatica": True,
            "solo_funcion_judicial": True
        },
        "endpoints": {
            "daemon": [
                "/api/daemon/iniciar",
                "/api/daemon/detener",
                "/api/daemon/estado"
            ],
            "sincronizacion": [
                "/api/sync/iniciar",
                "/api/sync/estado",
                "/api/sync/auditoria"
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
    
    # Verificar BD destino
    try:
        from app.db import SessionLocal
        db = SessionLocal()
        db.execute(text("SELECT 1"))  # ✅ CORREGIDO: usar text()
        db.close()
        health_status["components"]["database"] = "ok"
    except Exception as e:
        health_status["components"]["database"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Verificar DB origen (DB2)
    try:
        from app.db.origen_db2 import test_conexion_db2
        if test_conexion_db2():
            health_status["components"]["db2"] = "ok"
        else:
            health_status["components"]["db2"] = "error"
    except Exception as e:
        health_status["components"]["db2"] = f"error: {str(e)}"
    
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
    
    # Verificar scheduler de sincronización
    try:
        from app.services.scheduler_sincronizacion import obtener_estado_scheduler
        estado_sync = obtener_estado_scheduler()
        health_status["components"]["scheduler"] = "running" if estado_sync["running"] else "stopped"
    except Exception as e:
        health_status["components"]["scheduler"] = f"error: {str(e)}"
    
    return health_status
