# TEST_01_SINCRONIZACION.py
"""
TEST 1: Verificar que sincronización funciona
- Conecta a DB2
- Trae registros
- Inserta en de_clientes_rpa_v2
- Registra auditoría

EJECUCIÓN:
cd /ruta/proyecto
python TEST_01_SINCRONIZACION.py
"""

from datetime import datetime, timedelta
from app.services.sincronizacion_db2_v2 import sincronizar_ahora, obtener_logs_ultimas_sincronizaciones

def test_1_sincronizar():
    """TEST 1: Ejecutar sincronización"""
    print("\n" + "="*70)
    print("TEST 1: SINCRONIZACIÓN DB2 → de_clientes_rpa_v2")
    print("="*70)
    
    # Hacer sincronización de hoy
    hoy = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n📅 Período: {hoy} a {hoy}")
    print("🔄 Iniciando sincronización...\n")
    
    try:
        exito, resultado = sincronizar_ahora(hoy, hoy)
        
        print("\n" + "="*70)
        print("RESULTADO DE SINCRONIZACIÓN")
        print("="*70)
        print(f"✅ Éxito: {exito}")
        print(f"📊 Estado: {resultado['estado']}")
        print(f"📋 Sincronización #: {resultado['numero_sincronizacion']}")
        print(f"⏱️  Duración: {resultado['duracion_segundos']} segundos")
        print(f"\n📈 CONTADORES:")
        print(f"   Traídos de DB2: {resultado['registros_traidos']}")
        print(f"   Insertados: {resultado['registros_insertados']}")
        print(f"   Duplicados: {resultado['registros_duplicados']}")
        print(f"   Errores: {resultado['registros_error']}")
        print(f"\n📅 RANGO DE FECHAS:")
        print(f"   Más antigua: {resultado['fecha_minima_db2']}")
        print(f"   Más nueva: {resultado['fecha_maxima_db2']}")
        print(f"\n💬 Mensaje: {resultado['mensaje']}")
        
        if resultado['errores_detallados']:
            print(f"\n❌ ERRORES DETALLADOS:")
            for err in resultado['errores_detallados']:
                print(f"   - {err}")
        
        return exito
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_2_leer_auditoria():
    """TEST 2: Leer logs de sincronizaciones"""
    print("\n" + "="*70)
    print("TEST 2: LEER AUDITORÍA (Últimas 5 sincronizaciones)")
    print("="*70)
    
    try:
        logs = obtener_logs_ultimas_sincronizaciones(5)
        
        if not logs:
            print("⚠️  No hay sincronizaciones registradas aún")
            return True
        
        print(f"\n📋 Encontradas {len(logs)} sincronizaciones:\n")
        
        for log in logs:
            print(f"   Sync #{log['numero_sincronizacion']}:")
            print(f"     Inicio: {log['fecha_hora_inicio']}")
            print(f"     Fin: {log['fecha_hora_fin']}")
            print(f"     Duración: {log['duracion_segundos']}s")
            print(f"     Traídos: {log['registros_traidos']} | Insertados: {log['registros_insertados']} | Duplicados: {log['registros_duplicados']}")
            print(f"     Estado: {log['estado']}")
            print(f"     Mensaje: {log['mensaje_resultado']}")
            print()
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n🚀 INICIANDO TESTS DE SINCRONIZACIÓN\n")
    
    # TEST 1: Sincronizar
    test1_ok = test_1_sincronizar()
    
    # TEST 2: Leer auditoría
    test2_ok = test_2_leer_auditoria()
    
    # RESUMEN
    print("\n" + "="*70)
    print("RESUMEN FINAL")
    print("="*70)
    print(f"TEST 1 (Sincronización): {'✅ PASS' if test1_ok else '❌ FAIL'}")
    print(f"TEST 2 (Auditoría): {'✅ PASS' if test2_ok else '❌ FAIL'}")
    print()
    
    if test1_ok and test2_ok:
        print("✅ TODOS LOS TESTS PASARON")
    else:
        print("❌ ALGUNOS TESTS FALLARON")
