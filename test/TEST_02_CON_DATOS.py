# TEST_02_CON_DATOS.py
"""
TEST 2: Sincronización con período que TIENE DATOS
Prueba con fechas donde sabes que hay registros en DB2
"""

from app.services.sincronizacion_db2_v2 import sincronizar_ahora

def test_con_datos():
    """Probar sincronización con fechas que tienen datos"""
    
    print("\n" + "="*70)
    print("TEST 2: SINCRONIZACIÓN CON PERÍODO QUE TIENE DATOS")
    print("="*70)
    
    # ⚠️ CAMBIA ESTAS FECHAS por un período que SEPAS que tiene datos
    fecha_desde = "2025-11-30"
    fecha_hasta = "2025-12-02"
    
    print(f"\n📅 Período: {fecha_desde} a {fecha_hasta}")
    print("🔄 Iniciando sincronización...\n")
    
    try:
        exito, resultado = sincronizar_ahora(fecha_desde, fecha_hasta)
        
        print("\n" + "="*70)
        print("RESULTADO")
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
        
        if resultado['registros_traidos'] > 0:
            print(f"\n📅 RANGO DE FECHAS:")
            print(f"   Más antigua: {resultado['fecha_minima_db2']}")
            print(f"   Más nueva: {resultado['fecha_maxima_db2']}")
        
        print(f"\n💬 Mensaje: {resultado['mensaje']}")
        
        if resultado['errores_detallados']:
            print(f"\n❌ ERRORES:")
            for err in resultado['errores_detallados']:
                print(f"   - {err}")
        
        return exito
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("🚀 INICIANDO TEST CON DATOS\n")
    exito = test_con_datos()
    
    if exito:
        print("\n✅ TEST EXITOSO - Los datos se están sincronizando correctamente")
    else:
        print("\n❌ TEST FALLIDO - Revisa el error arriba")
