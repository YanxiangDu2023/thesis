import os
import uuid
import pandas as pd
from datetime import datetime
from fastapi import UploadFile, HTTPException
from app.database import get_connection

BASE_UPLOAD_DIR = "uploads"

async def handle_csv_upload(matrix_type: str, file: UploadFile):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

    target_dir = os.path.join(BASE_UPLOAD_DIR, matrix_type)
    os.makedirs(target_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_name = f"{timestamp}_{uuid.uuid4().hex}_{file.filename}"
    stored_path = os.path.join(target_dir, unique_name)

    content = await file.read()
    with open(stored_path, "wb") as f:
        f.write(content)

    conn = get_connection()
    cursor = conn.cursor()
    upload_run_id = None

    try:
        cursor.execute("""
            INSERT INTO upload_runs (
                matrix_type,
                original_file_name,
                stored_file_name,
                stored_path,
                status
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            matrix_type,
            file.filename,
            unique_name,
            stored_path,
            "processing"
        ))
        upload_run_id = cursor.lastrowid
        conn.commit()

        df = pd.read_csv(stored_path)

        print(df.columns.tolist())

        row_count = len(df)

        if matrix_type == "reporter_list":
            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO reporter_list_rows (
                        upload_run_id,
                        row_index,
                        calendar,
                        source,
                        source_code,
                        machine_line,
                        machine_code,
                        brand_name,
                        brand_code
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    str(row.get("Calendar", "")),
                    str(row.get("Source", "")),
                    str(row.get("Source Code", "")),
                    str(row.get("Machine Line", "")),
                    str(row.get("Machine Code", "")),
                    str(row.get("Brand Name", "")),
                    str(row.get("Brand Code", ""))
                ))
        

        elif matrix_type == "source_matrix":
        # print("CSV columns:", df.columns.tolist())

            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO source_matrix_rows (
                        upload_run_id,
                        row_index,
                        country_grouping,
                        country_name,
                        machine_line_code,
                        machine_line_name,
                        primary_source,
                        secondary_source,
                        crp_source,
                        change_indicator
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    str(row.get("Country Grouping", "")),
                    str(row.get("Unnamed: 1", "")),
                    str(row.get("Machine Line", "")),
                    str(row.get("Unnamed: 3", "")),
                    str(row.get("Primary Source", "")),
                    str(row.get("Secondary source NOT IN USE", "")),
                    str(row.get("CRP source", "")),
                    str(row.get("Change Indicator", ""))
            ))
                
        

        elif matrix_type == "size_class":
    # print("CSV columns:", df.columns.tolist())

            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO size_class_rows (
                        upload_run_id,
                        row_index,
                        calendar,
                        source,
                        source_code,
                        machine_line,
                        machine_code,
                        brand_name,
                        brand_code,
                        size_class
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    str(row.get("Calendar", "")),
                    str(row.get("Source", "")),
                    str(row.get("Source Code", "")),
                    str(row.get("Machine Line", "")),
                    str(row.get("Machine Code", "")),
                    str(row.get("Brand Name", "")),
                    str(row.get("Brand Code", "")),
                    str(row.get("Size Class", ""))
                ))


        elif matrix_type == "brand_mapping":
            print("CSV columns:", df.columns.tolist())

            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO brand_mapping_rows (
                        upload_run_id,
                        row_index,
                        brand_name,
                        brand_code,
                        deletion_indicator
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    str(row.get("Brand Name", "")),
                    str(row.get("Brand Code", "")),
                    str(row.get("Deletion Indicator", ""))
                ))



        else:
            raise HTTPException(status_code=400, detail="Unsupported matrix type.")

        cursor.execute("""
            UPDATE upload_runs
            SET row_count = ?, status = ?, message = ?
            WHERE id = ?
        """, (
            row_count,
            "success",
            "Upload completed successfully",
            upload_run_id
        ))
        conn.commit()

        return {
            "message": "Upload successful",
            "upload_run_id": upload_run_id,
            "row_count": row_count
        }

    except Exception as e:
        if upload_run_id is not None:
            cursor.execute("""
                UPDATE upload_runs
                SET status = ?, message = ?
                WHERE id = ?
            """, (
                "failed",
                str(e),
                upload_run_id
            ))
            conn.commit()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()