# TEST_03_ENDPOINTS_SINCRONIZACION.py
"""
TEST 3: Probar endpoints de sincronización
Simula requests HTTP sin necesidad de servidor corriendo
"""

import requests
import json
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"

def test_root():
    """Test endpoint raíz"""
    print("\n" + "="*70)
    print("TEST 1: GET / (Información del sistema)")
    print("="*70)
    try:
        response = requests.get(f"{BASE_URL}/")
        print(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_health():
    """Test health check"""
    print("\n" + "="*70)
    print("TEST 2: GET /health (Health check)")
    print("="*70)
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status: {response.status_code}")
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        # Verificar componentes
        if "components" in data:
            print("\n📊 Estado de componentes:")
            for comp, estado in data["components"].items():
                print(f"   {comp}: {estado}")
        
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_sync_iniciar():
    """Test iniciar sincronización manual"""
    print("\n" + "="*70)
    print("TEST 3: POST /api/sync/iniciar (Sincronización manual)")
    print("="*70)
    try:
        # Usar rango de fechas que sabemos tiene datos
        fecha_desde = "2025-11-30"
        fecha_hasta = "2025-12-02"
        
        params = {
            "fecha_desde": fecha_desde,
            "fecha_hasta": fecha_hasta
        }
        
        print(f"Parámetros: {params}")
        response = requests.post(f"{BASE_URL}/api/sync/iniciar", params=params)
        print(f"Status: {response.status_code}")
        
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        if response.status_code == 200:
            print("\n✅ RESULTADO:")
            print(f"   Éxito: {data.get('exito')}")
            print(f"   Registros traídos: {data.get('registros_traidos')}")
            print(f"   Registros insertados: {data.get('registros_insertados')}")
            print(f"   Duplicados: {data.get('registros_duplicados')}")
            print(f"   Errores: {data.get('registros_error')}")
            print(f"   Estado: {data.get('estado')}")
            print(f"   Duración: {data.get('duracion_segundos')}s")
        
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_sync_estado():
    """Test obtener estado del scheduler"""
    print("\n" + "="*70)
    print("TEST 4: GET /api/sync/estado (Estado del scheduler)")
    print("="*70)
    try:
        response = requests.get(f"{BASE_URL}/api/sync/estado")
        print(f"Status: {response.status_code}")
        
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        if response.status_code == 200:
            print("\n✅ SCHEDULER:")
            print(f"   Activo: {data.get('scheduler_activo')}")
            print(f"   Próxima ejecución: {data.get('proxima_ejecucion')}")
            
            if data.get('jobs'):
                print(f"   Jobs: {len(data.get('jobs'))}")
                for job in data.get('jobs'):
                    print(f"      - {job.get('nombre')}")
                    print(f"        Próximo: {job.get('proxima_ejecucion')}")
        
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_sync_auditoria():
    """Test obtener auditoría"""
    print("\n" + "="*70)
    print("TEST 5: GET /api/sync/auditoria (Histórico de sincronizaciones)")
    print("="*70)
    try:
        params = {"cantidad": 5}
        response = requests.get(f"{BASE_URL}/api/sync/auditoria", params=params)
        print(f"Status: {response.status_code}")
        
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        if response.status_code == 200:
            print(f"\n✅ AUDITORÍA ({data.get('total')} registros):")
            for sync in data.get('sincronizaciones', []):
                print(f"\n   Sync #{sync.get('numero')}:")
                print(f"      Estado: {sync.get('estado')}")
                print(f"      Traídos: {sync.get('registros_traidos')}")
                print(f"      Insertados: {sync.get('registros_insertados')}")
                print(f"      Duplicados: {sync.get('registros_duplicados')}")
                print(f"      Errores: {sync.get('registros_error')}")
                print(f"      Duración: {sync.get('duracion_segundos')}s")
                print(f"      Mensaje: {sync.get('mensaje')}")
        
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_sync_error_fechas():
    """Test validación de fechas"""
    print("\n" + "="*70)
    print("TEST 6: POST /api/sync/iniciar (Validación de fechas)")
    print("="*70)
    try:
        params = {
            "fecha_desde": "invalid-date",
            "fecha_hasta": "invalid-date"
        }
        
        print(f"Parámetros inválidos: {params}")
        response = requests.post(f"{BASE_URL}/api/sync/iniciar", params=params)
        print(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
        
        # Esperamos 400 (Bad Request)
        if response.status_code == 400:
            print("\n✅ Validación funciona correctamente")
            return True
        else:
            print("\n❌ Se esperaba status 400")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 INICIANDO TESTS DE ENDPOINTS")
    print("="*70)
    print("\n⚠️  IMPORTANTE: Asegúrate que el servidor está corriendo:")
    print("   uvicorn app.main:app --reload")
    print("\n" + "="*70)
    
    resultados = []
    
    # Test 1: Root
    resultados.append(("GET /", test_root()))
    
    # Test 2: Health
    resultados.append(("GET /health", test_health()))
    
    # Test 3: Sync iniciar
    resultados.append(("POST /api/sync/iniciar", test_sync_iniciar()))
    
    # Test 4: Sync estado
    resultados.append(("GET /api/sync/estado", test_sync_estado()))
    
    # Test 5: Sync auditoría
    resultados.append(("GET /api/sync/auditoria", test_sync_auditoria()))
    
    # Test 6: Validación de errores
    resultados.append(("Validación fechas", test_sync_error_fechas()))
    
    # Resumen
    print("\n" + "="*70)
    print("📊 RESUMEN DE TESTS")
    print("="*70)
    
    pasados = sum(1 for _, resultado in resultados if resultado)
    totales = len(resultados)
    
    for nombre, resultado in resultados:
        estado = "✅ PASS" if resultado else "❌ FAIL"
        print(f"{estado} - {nombre}")
    
    print(f"\n{pasados}/{totales} tests pasaron")
    
    if pasados == totales:
        print("\n🎉 ¡TODOS LOS TESTS PASARON!")
    else:
        print(f"\n⚠️  {totales - pasados} test(s) fallaron")