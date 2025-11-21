# app/routers/reports.py - ACTUALIZADO PARA SQL SERVER
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import text
from app.db import engine  # ✅ Usa el engine principal (ahora SQL Server)
import os

router = APIRouter(prefix="/reports", tags=["reports"])

@router.get("/by-job/{job_id}")
def get_report_by_job(job_id: str):
    """
    ⚠️ LEGACY: Busca en tabla 'reports' vieja
    TODO: Migrar a de_reportes_rpa
    """
    try:
        # SQL compatible con SQL Server
        sql = text("""
            SELECT TOP 1 id, job_id, file_path, created_at
            FROM reports
            WHERE job_id = :jid
            ORDER BY id DESC
        """)
        
        with engine.connect() as conn:
            row = conn.execute(sql, {"jid": job_id}).mappings().first()
            
            if not row:
                raise HTTPException(status_code=404, detail="No hay reporte para ese job_id")
            
            return {
                "id": row["id"],
                "job_id": row["job_id"],
                "file_path": row["file_path"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            }
    except Exception as e:
        # Si la tabla no existe en SQL Server, retornar mensaje claro
        if "Invalid object name 'reports'" in str(e):
            raise HTTPException(
                status_code=404, 
                detail="Tabla 'reports' no existe en SQL Server. Usa /api/tracking/reportes"
            )
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/{report_id}/download")
def download_report(report_id: int):
    """
    ⚠️ LEGACY: Descarga desde tabla 'reports' vieja
    TODO: Migrar a /api/tracking/reportes/{proceso_id}/download
    """
    try:
        sql = text("SELECT file_path FROM reports WHERE id = :id")
        
        with engine.connect() as conn:
            row = conn.execute(sql, {"id": report_id}).mappings().first()
            
            if not row:
                raise HTTPException(status_code=404, detail="Reporte no encontrado")
            
            file_path = row["file_path"]
            
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail="Archivo no encontrado en disco")
            
            filename = os.path.basename(file_path)
            
            return FileResponse(
                path=file_path,
                filename=filename,
                media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            )
    except Exception as e:
        if "Invalid object name 'reports'" in str(e):
            raise HTTPException(
                status_code=404, 
                detail="Tabla 'reports' no existe en SQL Server"
            )
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")