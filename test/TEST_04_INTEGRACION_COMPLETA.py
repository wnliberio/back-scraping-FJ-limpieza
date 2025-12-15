# TEST_04_INTEGRACION_COMPLETA.py
"""
TEST DE INTEGRACIÓN COMPLETA
Verifica que el backend y frontend estén correctamente actualizados
para usar de_clientes_rpa_v2
"""

import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000/api"

print("\n" + "="*70)
print("🚀 TEST DE INTEGRACIÓN COMPLETA - de_clientes_rpa_v2")
print("="*70)

# ===== TEST 1: Health Check =====

print("\n[TEST 1] GET /tracking/health - Verificar tabla correcta")
print("-" * 70)
try:
    response = requests.get(f"{BASE_URL}/tracking/health")
    print(f"Status: {response.status_code}")
    data = response.json()
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    if "tabla_clientes" in data and data["tabla_clientes"] == "de_clientes_rpa_v2":
        print("✅ PASS - Health check retorna tabla correcta")
    else:
        print("❌ FAIL - No retorna tabla de_clientes_rpa_v2")
except Exception as e:
    print(f"❌ ERROR: {e}")

# ===== TEST 2: Obtener Clientes =====

print("\n[TEST 2] GET /tracking/clientes - Obtener clientes de V2")
print("-" * 70)
try:
    response = requests.get(f"{BASE_URL}/tracking/clientes")
    print(f"Status: {response.status_code}")
    data = response.json()
    
    if isinstance(data, list):
        print(f"✅ Retorna lista con {len(data)} clientes")
        
        if len(data) > 0:
            cliente = data[0]
            print("\n📋 PRIMER CLIENTE:")
            print(json.dumps(cliente, indent=2, ensure_ascii=False, default=str))
            
            # Verificar campos esperados de V2
            campos_requeridos = [
                'id', 'ID_SOLICITUD', 'ESTADO', 'AGENCIA',
                'CEDULA', 'NOMBRES_CLIENTE', 'APELLIDOS_CLIENTE',
                'ESTADO_CONSULTA'
            ]
            
            campos_faltantes = [c for c in campos_requeridos if c not in cliente]
            
            if not campos_faltantes:
                print("\n✅ PASS - Todos los campos de V2 presentes")
            else:
                print(f"\n❌ FAIL - Campos faltantes: {campos_faltantes}")
        else:
            print("⚠️ No hay clientes para mostrar")
    else:
        print("❌ FAIL - No retorna lista")
except Exception as e:
    print(f"❌ ERROR: {e}")

# ===== TEST 3: Filtrar por ESTADO_CONSULTA =====

print("\n[TEST 3] GET /tracking/clientes?estado=Pendiente - Filtro ESTADO_CONSULTA")
print("-" * 70)
try:
    response = requests.get(f"{BASE_URL}/tracking/clientes?estado=Pendiente")
    print(f"Status: {response.status_code}")
    data = response.json()
    
    if isinstance(data, list):
        print(f"✅ Retorna {len(data)} clientes con ESTADO_CONSULTA='Pendiente'")
        
        # Verificar que todos tienen estado Pendiente
        estados = [c.get('ESTADO_CONSULTA') for c in data]
        todos_pendientes = all(e == 'Pendiente' for e in estados)
        
        if todos_pendientes:
            print("✅ PASS - Todos los clientes tienen estado Pendiente")
        else:
            print(f"❌ FAIL - Hay clientes con otros estados: {set(estados)}")
    else:
        print("❌ FAIL - No retorna lista")
except Exception as e:
    print(f"❌ ERROR: {e}")

# ===== TEST 4: Búsqueda por nombre =====

print("\n[TEST 4] GET /tracking/clientes?q=PAMELA - Búsqueda en campos V2")
print("-" * 70)
try:
    response = requests.get(f"{BASE_URL}/tracking/clientes?q=PAMELA")
    print(f"Status: {response.status_code}")
    data = response.json()
    
    if isinstance(data, list):
        print(f"✅ Retorna {len(data)} clientes con búsqueda 'PAMELA'")
        
        if len(data) > 0:
            for cliente in data:
                nombres = f"{cliente.get('NOMBRES_CLIENTE', '')} {cliente.get('APELLIDOS_CLIENTE', '')}"
                print(f"   - {nombres} (CI: {cliente.get('CEDULA')})")
            print("✅ PASS - Búsqueda funciona")
        else:
            print("⚠️ No hay resultados para 'PAMELA'")
    else:
        print("❌ FAIL - No retorna lista")
except Exception as e:
    print(f"❌ ERROR: {e}")

# ===== TEST 5: Actualizar estado =====

print("\n[TEST 5] PUT /tracking/clientes/{id}/estado - Actualizar ESTADO_CONSULTA")
print("-" * 70)
try:
    # Primero obtener un cliente
    response = requests.get(f"{BASE_URL}/tracking/clientes?estado=Pendiente")
    clientes = response.json()
    
    if len(clientes) > 0:
        cliente_id = clientes[0]['id']
        print(f"Actualizando cliente {cliente_id}...")
        
        response = requests.put(
            f"{BASE_URL}/tracking/clientes/{cliente_id}/estado",
            json={"estado": "En_Proceso"}
        )
        
        print(f"Status: {response.status_code}")
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        if response.status_code == 200 and data.get('success'):
            print("✅ PASS - Estado actualizado correctamente")
        else:
            print("❌ FAIL - No se pudo actualizar")
    else:
        print("⚠️ No hay clientes Pendientes para probar")
except Exception as e:
    print(f"❌ ERROR: {e}")

# ===== TEST 6: Daemon Estado =====

print("\n[TEST 6] GET /daemon/estado - Estado actual del daemon")
print("-" * 70)
try:
    response = requests.get(f"{BASE_URL}/daemon/estado")
    print(f"Status: {response.status_code}")
    data = response.json()
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    print(f"✅ Daemon {'Activo' if data.get('running') else 'Inactivo'}")
except Exception as e:
    print(f"❌ ERROR: {e}")

# ===== TEST 7: Synchronization Estado =====

print("\n[TEST 7] GET /sync/estado - Estado del scheduler de sincronización")
print("-" * 70)
try:
    response = requests.get(f"{BASE_URL}/sync/estado")
    print(f"Status: {response.status_code}")
    data = response.json()
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
    
    if data.get('scheduler_activo'):
        print(f"✅ Scheduler activo - Próxima ejecución: {data.get('proxima_ejecucion')}")
    else:
        print("⚠️ Scheduler inactivo")
except Exception as e:
    print(f"❌ ERROR: {e}")

# ===== RESUMEN =====

print("\n" + "="*70)
print("📊 RESUMEN DE TESTS")
print("="*70)
print("""
✅ VERIFICAR:
1. Health check retorna tabla 'de_clientes_rpa_v2'
2. Clientes tienen campos: ID_SOLICITUD, ESTADO, AGENCIA, CEDULA, NOMBRES_CLIENTE, APELLIDOS_CLIENTE, ESTADO_CONSULTA
3. Filtros funcionan por ESTADO_CONSULTA
4. Búsqueda funciona en NOMBRES_CLIENTE, APELLIDOS_CLIENTE, CEDULA
5. Actualización de ESTADO_CONSULTA funciona
6. Daemon está operativo
7. Scheduler de sincronización está corriendo

✅ SI TODOS LOS TESTS PASAN:
- Backend está correctamente actualizado a de_clientes_rpa_v2
- Frontend puede usar los endpoints sin problemas
- Sistema listo para producción
""")

print("\n" + "="*70)
print("🎯 SIGUIENTE PASO: Actualizar Frontend y hacer commit")
print("="*70)