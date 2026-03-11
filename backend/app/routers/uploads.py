from fastapi import APIRouter, UploadFile, File, Form
from app.database import get_connection
from app.services.csv_service import handle_csv_upload

router = APIRouter()

@router.post("/uploads/csv")
async def upload_csv(
    matrix_type: str = Form(...),
    file: UploadFile = File(...)
):
    return await handle_csv_upload(matrix_type, file)

@router.get("/uploads")
def get_uploads():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM upload_runs ORDER BY uploaded_at DESC")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows

@router.get("/uploads/{upload_run_id}/reporter-list-rows")
def get_reporter_list_rows(upload_run_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM reporter_list_rows
        WHERE upload_run_id = ?
        ORDER BY row_index
    """, (upload_run_id,))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows