# app/routers/lista.py
from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Any, Dict
from sqlalchemy import text
from app.db import engine, SessionLocal

router = APIRouter(prefix="/lista", tags=["lista"])

def _row_to_dict(row) -> Dict[str, Any]:
    return {
        "id": row.id_lista,
        "nombre": row.nombre,
        "apellido": row.apellido,
        "ci": row.ci,
        "ruc": row.ruc,
        "tipo": row.tipo,
        "monto": float(row.monto) if row.monto is not None else None,
        "fecha": row.fecha.isoformat() if row.fecha else None,
        "estado": row.estado,
        "fecha_creacion": row.fecha_creacion.isoformat() if row.fecha_creacion else None,
        "fecha_inicio_flujo": row.fecha_inicio_flujo.isoformat() if row.fecha_inicio_flujo else None,
        "fecha_fin_flujo": row.fecha_fin_flujo.isoformat() if row.fecha_fin_flujo else None,
        "mensaje_error": row.mensaje_error,
    }

@router.get("")
def list_lista(
    estado: Optional[str] = Query(None, description="Pendiente|Procesando|Procesado|Error"),
    fecha_desde: Optional[str] = Query(None, description="YYYY-MM-DD"),
    fecha_hasta: Optional[str] = Query(None, description="YYYY-MM-DD"),
    q: Optional[str] = Query(None, description="Busca en nombre, apellido, ci, ruc"),
) -> List[Dict[str, Any]]:
    """
    Lista registros de `de_lista` con filtros básicos.
    """
    with engine.connect() as conn:
        where = ["1=1"]
        params: Dict[str, Any] = {}
        if estado:
            where.append("estado = :estado")
            params["estado"] = estado
        if fecha_desde:
            where.append("fecha >= :fdesde")
            params["fdesde"] = fecha_desde
        if fecha_hasta:
            where.append("fecha <= :fhasta")
            params["fhasta"] = fecha_hasta
        if q:
            where.append("(nombre LIKE :q OR apellido LIKE :q OR ci LIKE :q OR ruc LIKE :q)")
            params["q"] = f"%{q}%"

        sql = text(f"""
            SELECT id_lista, nombre, apellido, ci, ruc, tipo, monto, fecha,
                   estado, fecha_creacion, fecha_inicio_flujo, fecha_fin_flujo, mensaje_error
            FROM de_lista
            WHERE {" AND ".join(where)}
            ORDER BY fecha_creacion DESC, id_lista DESC
        """)
        rows = conn.execute(sql, params).mappings().all()
        return [_row_to_dict(r) for r in rows]

@router.put("/{id_lista}/estado")
def update_estado(
    id_lista: int,
    payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Actualiza estado y marcas de tiempos.
    Body ejemplo:
    { "estado":"Procesando", "mensaje_error":null }
    """
    estado = (payload.get("estado") or "").strip()
    if estado not in {"Pendiente", "Procesando", "Procesado", "Error"}:
        raise HTTPException(status_code=400, detail="Estado inválido.")

    set_campos = ["estado = :estado"]
    params: Dict[str, Any] = {"estado": estado, "id": id_lista}

    if estado == "Procesando":
        set_campos.append("fecha_inicio_flujo = CURRENT_TIMESTAMP")
        set_campos.append("mensaje_error = NULL")
    elif estado == "Procesado":
        set_campos.append("fecha_fin_flujo = CURRENT_TIMESTAMP")
        set_campos.append("mensaje_error = NULL")
    elif estado == "Error":
        set_campos.append("fecha_fin_flujo = CURRENT_TIMESTAMP")
        msg = payload.get("mensaje_error")
        params["mensaje_error"] = (msg or "")[:1024]
        set_campos.append("mensaje_error = :mensaje_error")

    sql = text(f"UPDATE de_lista SET {', '.join(set_campos)} WHERE id_lista = :id")
    with engine.begin() as conn:
        res = conn.execute(sql, params)
        if res.rowcount == 0:
            raise HTTPException(status_code=404, detail="Registro no encontrado")
    return {"ok": True}
