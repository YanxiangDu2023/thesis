import os
import uuid
import pandas as pd
from datetime import datetime
from fastapi import UploadFile, HTTPException
from app.database import get_connection

BASE_UPLOAD_DIR = "uploads"
OTH_EXPECTED_HEADERS = {
    "year",
    "source",
    "brand",
    "machine line",
    "country",
    "size class",
    "quantity",
}
SIZE_CLASS_EXPECTED_HEADERS = {
    "calendar",
    "source",
    "source code",
    "machine line",
    "machine code",
    "brand name",
    "brand code",
    "size class",
}
SIZE_CLASS_COLUMN_NAMES = [
    "calendar",
    "source",
    "source_code",
    "machine_line",
    "machine_code",
    "brand_name",
    "brand_code",
    "size_class",
]
MACHINE_LINE_MAPPING_EXPECTED_HEADERS = {
    "machine line name",
    "machine line code",
}
MACHINE_LINE_MAPPING_COLUMN_NAMES = [
    "machine_line_name",
    "machine_line_code",
]


def _clean_cell(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _normalize_header(value) -> str:
    return " ".join(str(value).replace("\n", " ").strip().lower().split())


def _looks_like_oth_header(first_row) -> bool:
    normalized_headers = {_normalize_header(value) for value in first_row if str(value).strip()}
    header_matches = normalized_headers.intersection(OTH_EXPECTED_HEADERS)
    return len(header_matches) >= 4


def _load_oth_dataframe(stored_path: str) -> pd.DataFrame:
    oth_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if oth_df.empty:
        return oth_df

    if _looks_like_oth_header(oth_df.iloc[0].tolist()):
        oth_df = oth_df.iloc[1:].reset_index(drop=True)

    if oth_df.shape[1] < 9:
        for missing_col in range(oth_df.shape[1], 9):
            oth_df[missing_col] = ""

    return oth_df


def _looks_like_size_class_header(first_row) -> bool:
    normalized_headers = {_normalize_header(value) for value in first_row if str(value).strip()}
    header_matches = normalized_headers.intersection(SIZE_CLASS_EXPECTED_HEADERS)
    return len(header_matches) >= 5


def _load_size_class_dataframe(stored_path: str) -> pd.DataFrame:
    size_class_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if size_class_df.empty:
        return pd.DataFrame(columns=SIZE_CLASS_COLUMN_NAMES)

    if _looks_like_size_class_header(size_class_df.iloc[0].tolist()):
        size_class_df = size_class_df.iloc[1:].reset_index(drop=True)

    if size_class_df.shape[1] < len(SIZE_CLASS_COLUMN_NAMES):
        for missing_col in range(size_class_df.shape[1], len(SIZE_CLASS_COLUMN_NAMES)):
            size_class_df[missing_col] = ""

    size_class_df = size_class_df.iloc[:, :len(SIZE_CLASS_COLUMN_NAMES)].copy()
    size_class_df.columns = SIZE_CLASS_COLUMN_NAMES

    return size_class_df


def _looks_like_machine_line_mapping_header(first_row) -> bool:
    normalized_headers = {_normalize_header(value) for value in first_row if str(value).strip()}
    header_matches = normalized_headers.intersection(MACHINE_LINE_MAPPING_EXPECTED_HEADERS)
    return len(header_matches) >= 1


def _load_machine_line_mapping_dataframe(stored_path: str) -> pd.DataFrame:
    machine_line_mapping_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if machine_line_mapping_df.empty:
        return pd.DataFrame(columns=MACHINE_LINE_MAPPING_COLUMN_NAMES)

    if _looks_like_machine_line_mapping_header(machine_line_mapping_df.iloc[0].tolist()):
        machine_line_mapping_df = machine_line_mapping_df.iloc[1:].reset_index(drop=True)

    if machine_line_mapping_df.shape[1] < len(MACHINE_LINE_MAPPING_COLUMN_NAMES):
        for missing_col in range(machine_line_mapping_df.shape[1], len(MACHINE_LINE_MAPPING_COLUMN_NAMES)):
            machine_line_mapping_df[missing_col] = ""

    machine_line_mapping_df = machine_line_mapping_df.iloc[:, :len(MACHINE_LINE_MAPPING_COLUMN_NAMES)].copy()
    machine_line_mapping_df.columns = MACHINE_LINE_MAPPING_COLUMN_NAMES

    return machine_line_mapping_df

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

        df = pd.read_csv(stored_path, dtype=str, keep_default_na=False)

        row_count = 0

        if matrix_type == "reporter_list":
            row_count = len(df)
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
                    _clean_cell(row.get("Calendar", "")),
                    _clean_cell(row.get("Source", "")),
                    _clean_cell(row.get("Source Code", "")),
                    _clean_cell(row.get("Machine Line", "")),
                    _clean_cell(row.get("Machine Code", "")),
                    _clean_cell(row.get("Brand Name", "")),
                    _clean_cell(row.get("Brand Code", ""))
                ))

        elif matrix_type == "source_matrix":
            row_count = len(df)
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
                    _clean_cell(row.get("Country Grouping", "")),
                    _clean_cell(row.get("Unnamed: 1", "")),
                    _clean_cell(row.get("Machine Line", "")),
                    _clean_cell(row.get("Unnamed: 3", "")),
                    _clean_cell(row.get("Primary Source", "")),
                    _clean_cell(row.get("Secondary source NOT IN USE", "")),
                    _clean_cell(row.get("CRP source", "")),
                    _clean_cell(row.get("Change Indicator", ""))
                ))

        elif matrix_type == "size_class":
            size_class_df = _load_size_class_dataframe(stored_path)
            row_count = len(size_class_df)

            for idx, row in size_class_df.iterrows():
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
                    _clean_cell(row.get("calendar", "")),
                    _clean_cell(row.get("source", "")),
                    _clean_cell(row.get("source_code", "")),
                    _clean_cell(row.get("machine_line", "")),
                    _clean_cell(row.get("machine_code", "")),
                    _clean_cell(row.get("brand_name", "")),
                    _clean_cell(row.get("brand_code", "")),
                    _clean_cell(row.get("size_class", ""))
                ))

        elif matrix_type == "brand_mapping":
            row_count = len(df)
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
                    _clean_cell(row.get("Brand Name", "")),
                    _clean_cell(row.get("Brand Code", "")),
                    _clean_cell(row.get("Deletion Indicator", ""))
                ))

        elif matrix_type == "group_country":
            row_count = len(df)
            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO group_country_rows (
                        upload_run_id,
                        row_index,
                        country_code,
                        country_name,
                        country_grouping,
                        region,
                        market_area,
                        change_indicator
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.get("Country", row.get("Country Code", ""))),
                    _clean_cell(row.get("Country Name", row.get("Unnamed: 1", ""))),
                    _clean_cell(row.get("Country Grouping", "")),
                    _clean_cell(row.get("Region", "")),
                    _clean_cell(row.get("Market Area", row.get("Market area", ""))),
                    _clean_cell(row.get("Change Indicator", ""))
                ))

        elif matrix_type == "machine_line_mapping":
            machine_line_mapping_df = _load_machine_line_mapping_dataframe(stored_path)
            row_count = len(machine_line_mapping_df)

            for idx, row in machine_line_mapping_df.iterrows():
                cursor.execute("""
                    INSERT INTO machine_line_mapping_rows (
                        upload_run_id,
                        row_index,
                        machine_line_name,
                        machine_line_code
                    ) VALUES (?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.get("machine_line_name", "")),
                    _clean_cell(row.get("machine_line_code", ""))
                ))

        elif matrix_type == "oth_data":
            oth_df = _load_oth_dataframe(stored_path)
            row_count = len(oth_df)

            for idx, row in oth_df.iterrows():
                cursor.execute("""
                    INSERT INTO oth_data_rows (
                        upload_run_id,
                        row_index,
                        year,
                        source,
                        brand_name,
                        machine_line,
                        empty_col_1,
                        country,
                        empty_col_2,
                        size_class,
                        quantity
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.iloc[0]) if len(row) > 0 else "",
                    _clean_cell(row.iloc[1]) if len(row) > 1 else "",
                    _clean_cell(row.iloc[2]) if len(row) > 2 else "",
                    _clean_cell(row.iloc[3]) if len(row) > 3 else "",
                    _clean_cell(row.iloc[4]) if len(row) > 4 else "",
                    _clean_cell(row.iloc[5]) if len(row) > 5 else "",
                    _clean_cell(row.iloc[6]) if len(row) > 6 else "",
                    _clean_cell(row.iloc[7]) if len(row) > 7 else "",
                    _clean_cell(row.iloc[8]) if len(row) > 8 else ""
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
            "row_count": row_count,
            "matrix_type": matrix_type,
            "original_file_name": file.filename,
            "status": "success"
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
