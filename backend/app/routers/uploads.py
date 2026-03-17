from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from app.database import get_connection
from app.services.csv_service import handle_csv_upload

router = APIRouter()

MATRIX_TYPE_TO_TABLE = {
    "reporter_list": "reporter_list_rows",
    "source_matrix": "source_matrix_rows",
    "size_class": "size_class_rows",
    "brand_mapping": "brand_mapping_rows",
    "group_country": "group_country_rows",
    "machine_line_mapping": "machine_line_mapping_rows",
    "oth_data": "oth_data_rows",
}
REQUIRED_UPLOAD_TYPES = [
    "source_matrix",
    "reporter_list",
    "size_class",
    "brand_mapping",
    "group_country",
    "machine_line_mapping",
    "oth_data",
]

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

@router.get("/uploads/completeness")
def get_upload_completeness():
    conn = get_connection()
    cursor = conn.cursor()
    items = []

    for matrix_type in REQUIRED_UPLOAD_TYPES:
        cursor.execute("""
            SELECT id, uploaded_at, status, row_count, original_file_name
            FROM upload_runs
            WHERE matrix_type = ?
            ORDER BY uploaded_at DESC, id DESC
            LIMIT 1
        """, (matrix_type,))
        latest_upload = cursor.fetchone()

        cursor.execute("""
            SELECT id, uploaded_at, status, row_count, original_file_name
            FROM upload_runs
            WHERE matrix_type = ? AND status = 'success'
            ORDER BY uploaded_at DESC, id DESC
            LIMIT 1
        """, (matrix_type,))
        latest_success_upload = cursor.fetchone()

        items.append({
            "matrix_type": matrix_type,
            "uploaded": latest_success_upload is not None,
            "latest_upload": dict(latest_upload) if latest_upload else None,
            "latest_success_upload": dict(latest_success_upload) if latest_success_upload else None,
        })

    conn.close()

    missing_types = [item["matrix_type"] for item in items if not item["uploaded"]]
    return {
        "all_uploaded": len(missing_types) == 0,
        "missing_types": missing_types,
        "items": items,
    }

@router.get("/uploads/latest/{matrix_type}")
def get_latest_upload_by_matrix_type(matrix_type: str):
    table_name = MATRIX_TYPE_TO_TABLE.get(matrix_type)
    if table_name is None:
        raise HTTPException(status_code=400, detail="Unsupported matrix type")

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM upload_runs
        WHERE matrix_type = ?
        ORDER BY uploaded_at DESC, id DESC
        LIMIT 1
    """, (matrix_type,))
    latest_upload = cursor.fetchone()

    if latest_upload is None:
        conn.close()
        raise HTTPException(status_code=404, detail="No uploads found for this matrix type")

    latest_upload_dict = dict(latest_upload)
    cursor.execute(f"""
        SELECT *
        FROM {table_name}
        WHERE upload_run_id = ?
        ORDER BY row_index ASC, id ASC
    """, (latest_upload_dict["id"],))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {
        "upload_run": latest_upload_dict,
        "rows": rows
    }

@router.get("/uploads/{upload_run_id}")
def get_upload(upload_run_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM upload_runs
        WHERE id = ?
    """, (upload_run_id,))
    row = cursor.fetchone()
    conn.close()

    if row is None:
        raise HTTPException(status_code=404, detail="Upload run not found")

    return dict(row)

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
