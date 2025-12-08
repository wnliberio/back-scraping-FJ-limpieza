# app/db/models/__init__.py
"""
Módulo de modelos (cada tabla en su archivo)

ESTRUCTURA:
  app/db/models/
    ├── __init__.py (este archivo)
    ├── de_cliente_v2.py (tabla: de_clientes_rpa_v2)
    ├── de_sincronizacion_control.py (tabla: de_sincronizacion_control)
    └── ... (futuros modelos)

IMPORTAR DESDE CUALQUIER LUGAR:
  from app.db.models import DeClienteV2, DeSincronizacionControl
"""

from app.db.models.de_cliente_v2 import DeClienteV2
from app.db.models.de_sincronizacion_control import DeSincronizacionControl

__all__ = [
    "DeClienteV2",
    "DeSincronizacionControl",
]
