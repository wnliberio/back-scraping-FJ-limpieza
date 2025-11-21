# app/services/executor.py - VERSIÓN SIMPLIFICADA (SOLO FUNCIÓN JUDICIAL)
from typing import Dict, Any, List
from time import sleep
from core.utils.log import log

# ✅ ÚNICO scraper activo
from flows.funcion_judicial import process_funcion_judicial

from app.models.schemas import QueryItem
from core.config import INTER_ITEM_DELAY_SECONDS


def run_items(items: List[QueryItem], headless: bool = False) -> Dict[str, Any]:
    """
    Ejecuta consultas SOLO de Función Judicial.
    Sistema simplificado - mono-scraper.
    
    Args:
        items: Lista de QueryItem a procesar
        headless: Si True, ejecuta sin interfaz gráfica
        
    Returns:
        Dict con resultados por tipo de consulta
    """
    results: Dict[str, Any] = {}
    
    for index, it in enumerate(items, start=1):
        tipo = it.tipo.lower()
        valor = it.valor.strip()
        log(f"⚙️ Ejecutando item {index}/{len(items)}: {tipo} → {valor}")

        if tipo == "funcion_judicial":
            # Validar que tenga apellidos y nombres
            if len(valor.strip()) < 3:
                results["funcion_judicial"] = {
                    "error": "Debe proporcionar apellidos y nombres (mínimo 3 caracteres)"
                }
            else:
                # Ejecutar consulta de Función Judicial
                res = process_funcion_judicial(valor, headless=headless)
                results["funcion_judicial"] = res
        else:
            # Tipo no soportado
            log(f"⚠️ Tipo no soportado: {tipo}")
            results[tipo] = {
                "error": f"Tipo '{tipo}' no soportado. Solo se acepta 'funcion_judicial'"
            }

        # Pausa entre items (si hay más de uno)
        if index < len(items):
            log(f"⏳ Pausa de {INTER_ITEM_DELAY_SECONDS}s...")
            sleep(INTER_ITEM_DELAY_SECONDS)

    return results