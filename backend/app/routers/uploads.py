import csv
import json
import os
import re
import uuid
import unicodedata
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, UploadFile, File, Form, HTTPException, Query
from pydantic import BaseModel
from app.database import get_connection, get_table_columns
from app.services.csv_service import handle_csv_upload
from app.settings import get_upload_root_dir

router = APIRouter()

MATRIX_TYPE_TO_TABLE = {
    "reporter_list": "reporter_list_rows",
    "source_matrix": "source_matrix_rows",
    "size_class": "size_class_rows",
    "brand_mapping": "brand_mapping_rows",
    "group_country": "group_country_rows",
    "machine_line_mapping": "machine_line_mapping_rows",
    "oth_data": "oth_data_rows",
    "volvo_sale_data": "volvo_sale_data_rows",
    "tma_data": "tma_data_rows",
    "split_manual_cex": "split_manual_rows",
    "split_manual_gec": "split_manual_rows",
    "split_manual_gew": "split_manual_rows",
    "split_manual_wlo_gt10": "split_manual_rows",
    "split_manual_wlo_lt10": "split_manual_rows",
    "split_manual_wlo_lt12": "split_manual_rows",
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

P00_RUN_KEYS = {
    "crp_d1_combined": "p00_crp_d1_combined",
    "oth_deletion_flag": "p00_oth_deletion_flag",
    "three_check": "p00_three_check",
    "total_market_eligible_oth": "p00_total_market_eligible_oth",
    "total_market_calculated": "p00_total_market_calculated",
}
INTEGER_LIKE_COLUMNS = {
    "calendar",
    "year",
    "machine",
    "machine_code",
    "machine_line_code",
}
INTEGER_LIKE_PATTERN = re.compile(r"^[+-]?\d+(?:\.0+)?$")


class SaveEditedUploadRequest(BaseModel):
    matrix_type: str
    rows: list[dict[str, Any]]
    source_upload_run_id: int | None = None
    planning_year: int | None = None


class SyncBrandMappingFromOthRequest(BaseModel):
    planning_year: int | None = None


class ExcavatorsSplitCaseSnapshotRequest(BaseModel):
    case_type: str
    summary_rows: list[dict[str, Any]]
    detail_rows: list[dict[str, Any]]
    summary: dict[str, Any]
    source_row_count: int
    oth_row_count: int
    p10_row_count: int
    planning_year: int | None = None
    message: str = "Excavators split case snapshot saved"


class TotalMarketEligibleOthSnapshotRequest(BaseModel):
    rows: list[dict[str, Any]]
    message: str = "Total Market Calculation snapshot saved from editor"
    planning_year: int | None = None
    source_row_count: int | None = None
    split_machine_lines: list[str] | None = None
    split_input_rows: int | None = None
    split_output_rows: int | None = None
    source_report_run_id: int | None = None
    source_report_created_at: str | None = None
    three_check_report_run_id: int | None = None
    three_check_report_created_at: str | None = None


class PlanningYearRequest(BaseModel):
    year: int


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).strip()
    return "" if text.lower() == "nan" else text


def _to_case_insensitive_key(value: Any) -> str:
    return _to_text(value).upper()


def _volvo_sale_brand_name_from_code(brand_code: Any) -> str:
    brand_code_key = _to_case_insensitive_key(brand_code)
    if brand_code_key == "ABG":
        return "Rokbak"
    if brand_code_key == "SDLG":
        return "SDLG"
    if brand_code_key in {"VCE", "VOL", "VOLVO"}:
        return "VOLVO CE"
    return _to_text(brand_code)


def _normalize_saved_value(column: str, value: Any) -> str:
    if column in INTEGER_LIKE_COLUMNS:
        text = _to_text(value)
        normalized = text.replace(",", "")
        if normalized and INTEGER_LIKE_PATTERN.fullmatch(normalized):
            try:
                return str(int(float(normalized)))
            except ValueError:
                return text
        return text

    text = _to_text(value)
    if column in {"size_class", "size_class_mapping"}:
        return text.upper()
    return text


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: _normalize_saved_value(key, value) if key != "id" and key != "upload_run_id" and key != "row_index" else value
        for key, value in row.items()
    }


def _to_number(value: Any) -> float:
    text = _to_text(value).replace(",", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _to_size_class_key(value: Any) -> str:
    return _to_text(value).replace(" ", "").upper()


def _round_to_4(value: float) -> float:
    rounded = round(value, 4)
    return 0.0 if abs(rounded) < 0.00005 else rounded


def _format_percent_2(value: float) -> str:
    return f"{value:.2f}%"


def _get_table_insert_columns(cursor, table_name: str) -> list[str]:
    columns = get_table_columns(cursor, table_name)
    excluded = {"id", "upload_run_id", "row_index"}
    return [column for column in columns if column not in excluded]


def _normalize_planning_year(value: int | None) -> int | None:
    if value is None:
        return None
    if value < 1900 or value > 2999:
        raise HTTPException(status_code=400, detail="planning_year must be between 1900 and 2999")
    return value


def _get_latest_success_upload_id(cursor, matrix_type: str, planning_year: int | None = None):
    if planning_year is None:
        cursor.execute("""
            SELECT id
            FROM upload_runs
            WHERE matrix_type = ? AND status = 'success'
            ORDER BY uploaded_at DESC, id DESC
            LIMIT 1
        """, (matrix_type,))
    else:
        cursor.execute("""
            SELECT id
            FROM upload_runs
            WHERE matrix_type = ? AND planning_year = ? AND status = 'success'
            ORDER BY uploaded_at DESC, id DESC
            LIMIT 1
        """, (matrix_type, planning_year))
    row = cursor.fetchone()
    return row["id"] if row else None


def _record_report_run(report_key: str) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO report_run_history (report_key)
            VALUES (?)
        """, (report_key,))
        conn.commit()
    finally:
        conn.close()


def _get_latest_report_run_time(cursor, report_key: str) -> str | None:
    cursor.execute("""
        SELECT triggered_at
        FROM report_run_history
        WHERE report_key = ?
        ORDER BY triggered_at DESC, id DESC
        LIMIT 1
    """, (report_key,))
    row = cursor.fetchone()
    return row["triggered_at"] if row else None


@router.post("/uploads/save-edited")
def save_edited_upload(payload: SaveEditedUploadRequest):
    matrix_type = payload.matrix_type
    table_name = MATRIX_TYPE_TO_TABLE.get(matrix_type)
    if table_name is None:
        raise HTTPException(status_code=400, detail="Unsupported matrix type")

    if len(payload.rows) == 0:
        raise HTTPException(status_code=400, detail="No rows to save")

    conn = get_connection()
    cursor = conn.cursor()
    upload_run_id = None
    planning_year = _normalize_planning_year(payload.planning_year)

    try:
        insert_columns = _get_table_insert_columns(cursor, table_name)
        if len(insert_columns) == 0:
            raise HTTPException(
                status_code=500,
                detail=f"No writable columns found for {matrix_type}",
            )

        target_dir = os.path.join(get_upload_root_dir(), matrix_type)
        os.makedirs(target_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        source_suffix = f"_from_{payload.source_upload_run_id}" if payload.source_upload_run_id else ""
        original_file_name = f"{matrix_type}_edited{source_suffix}.csv"
        stored_file_name = f"{timestamp}_{uuid.uuid4().hex}_{original_file_name}"
        stored_path = os.path.join(target_dir, stored_file_name)

        with open(stored_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(insert_columns)
            for row in payload.rows:
                writer.writerow([
                    _normalize_saved_value(column, row.get(column, ""))
                    for column in insert_columns
                ])

        cursor.execute("""
            INSERT INTO upload_runs (
                matrix_type,
                planning_year,
                original_file_name,
                stored_file_name,
                stored_path,
                status
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            matrix_type,
            planning_year,
            original_file_name,
            stored_file_name,
            stored_path,
            "processing",
        ))
        upload_run_id = cursor.lastrowid

        column_sql = ", ".join(insert_columns)
        value_placeholders = ", ".join(["?"] * (len(insert_columns) + 2))
        insert_sql = f"""
            INSERT INTO {table_name} (
                upload_run_id,
                row_index,
                {column_sql}
            ) VALUES ({value_placeholders})
        """

        for idx, row in enumerate(payload.rows, start=1):
            values = [_normalize_saved_value(column, row.get(column, "")) for column in insert_columns]
            cursor.execute(insert_sql, (upload_run_id, idx, *values))

        row_count = len(payload.rows)
        cursor.execute("""
            UPDATE upload_runs
            SET row_count = ?, status = ?, message = ?
            WHERE id = ?
        """, (
            row_count,
            "success",
            "Saved from in-page editor",
            upload_run_id,
        ))

        conn.commit()

        return {
            "message": "Save successful",
            "upload_run_id": upload_run_id,
            "row_count": row_count,
            "matrix_type": matrix_type,
            "original_file_name": original_file_name,
            "status": "success",
        }
    except HTTPException as e:
        if upload_run_id is not None:
            cursor.execute("""
                UPDATE upload_runs
                SET status = ?, message = ?
                WHERE id = ?
            """, ("failed", str(e.detail), upload_run_id))
            conn.commit()
        raise e
    except Exception as e:
        if upload_run_id is not None:
            cursor.execute("""
                UPDATE upload_runs
                SET status = ?, message = ?
                WHERE id = ?
            """, ("failed", str(e), upload_run_id))
            conn.commit()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/uploads/brand-mapping/sync-from-oth")
def sync_brand_mapping_from_oth(payload: SyncBrandMappingFromOthRequest):
    planning_year = _normalize_planning_year(payload.planning_year)
    conn = get_connection()
    cursor = conn.cursor()
    upload_run_id = None

    try:
        latest_oth_upload_run_id = _get_latest_success_upload_id(cursor, "oth_data", planning_year)
        if latest_oth_upload_run_id is None:
            raise HTTPException(status_code=404, detail="No successful OTH Data upload found for this planning year.")

        latest_brand_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "brand_mapping", planning_year)

        cursor.execute("""
            SELECT DISTINCT TRIM(brand_name) AS brand_name
            FROM oth_data_rows
            WHERE upload_run_id = ?
              AND brand_name IS NOT NULL
              AND TRIM(brand_name) != ''
            ORDER BY UPPER(TRIM(brand_name)) ASC
        """, (latest_oth_upload_run_id,))
        oth_brand_names = [row["brand_name"] for row in cursor.fetchall()]

        existing_rows: list[dict[str, Any]] = []
        existing_brand_keys: set[str] = set()
        if latest_brand_mapping_upload_run_id is not None:
            cursor.execute("""
                SELECT
                    brand_name,
                    brand_code,
                    deletion_indicator
                FROM brand_mapping_rows
                WHERE upload_run_id = ?
                ORDER BY row_index ASC, id ASC
            """, (latest_brand_mapping_upload_run_id,))
            existing_rows = [dict(row) for row in cursor.fetchall()]
            existing_brand_keys = {
                str(row.get("brand_name") or "").strip().upper()
                for row in existing_rows
                if str(row.get("brand_name") or "").strip()
            }

        new_brand_names = []
        seen_new_brand_keys: set[str] = set()
        for brand_name in oth_brand_names:
            brand_key = brand_name.strip().upper()
            if brand_key in existing_brand_keys or brand_key in seen_new_brand_keys:
                continue
            seen_new_brand_keys.add(brand_key)
            new_brand_names.append(brand_name)

        if len(new_brand_names) == 0:
            return {
                "message": "Brand Mapping already covers all OTH brand names.",
                "upload_run_id": latest_brand_mapping_upload_run_id,
                "row_count": len(existing_rows),
                "added_count": 0,
                "added_brand_names": [],
                "matrix_type": "brand_mapping",
                "status": "success",
            }

        merged_rows = [
            {
                "brand_name": row.get("brand_name") or "",
                "brand_code": row.get("brand_code") or "",
                "deletion_indicator": row.get("deletion_indicator") or "",
            }
            for row in existing_rows
        ]
        merged_rows.extend(
            {
                "brand_name": brand_name,
                "brand_code": "",
                "deletion_indicator": "",
            }
            for brand_name in new_brand_names
        )

        target_dir = os.path.join(get_upload_root_dir(), "brand_mapping")
        os.makedirs(target_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        source_suffix = f"_from_{latest_brand_mapping_upload_run_id}" if latest_brand_mapping_upload_run_id else ""
        original_file_name = f"brand_mapping_synced_from_oth{source_suffix}.csv"
        stored_file_name = f"{timestamp}_{uuid.uuid4().hex}_{original_file_name}"
        stored_path = os.path.join(target_dir, stored_file_name)
        insert_columns = ["brand_name", "brand_code", "deletion_indicator"]

        with open(stored_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(insert_columns)
            for row in merged_rows:
                writer.writerow([row[column] for column in insert_columns])

        cursor.execute("""
            INSERT INTO upload_runs (
                matrix_type,
                planning_year,
                original_file_name,
                stored_file_name,
                stored_path,
                status
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            "brand_mapping",
            planning_year,
            original_file_name,
            stored_file_name,
            stored_path,
            "processing",
        ))
        upload_run_id = cursor.lastrowid

        cursor.executemany("""
            INSERT INTO brand_mapping_rows (
                upload_run_id,
                row_index,
                brand_name,
                brand_code,
                deletion_indicator
            ) VALUES (?, ?, ?, ?, ?)
        """, [
            (
                upload_run_id,
                idx,
                row["brand_name"],
                row["brand_code"],
                row["deletion_indicator"],
            )
            for idx, row in enumerate(merged_rows, start=1)
        ])

        row_count = len(merged_rows)
        cursor.execute("""
            UPDATE upload_runs
            SET row_count = ?, status = ?, message = ?
            WHERE id = ?
        """, (
            row_count,
            "success",
            f"Synced from OTH upload {latest_oth_upload_run_id}; added {len(new_brand_names)} brand(s)",
            upload_run_id,
        ))
        conn.commit()

        return {
            "message": "Brand Mapping synced from OTH.",
            "upload_run_id": upload_run_id,
            "row_count": row_count,
            "added_count": len(new_brand_names),
            "added_brand_names": new_brand_names,
            "matrix_type": "brand_mapping",
            "original_file_name": original_file_name,
            "status": "success",
        }
    except HTTPException as e:
        if upload_run_id is not None:
            cursor.execute("""
                UPDATE upload_runs
                SET status = ?, message = ?
                WHERE id = ?
            """, ("failed", str(e.detail), upload_run_id))
            conn.commit()
        raise e
    except Exception as e:
        if upload_run_id is not None:
            cursor.execute("""
                UPDATE upload_runs
                SET status = ?, message = ?
                WHERE id = ?
            """, ("failed", str(e), upload_run_id))
            conn.commit()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.post("/uploads/csv")
async def upload_csv(
    matrix_type: str = Form(...),
    file: UploadFile = File(...),
    planning_year: int | None = Form(default=None),
):
    return await handle_csv_upload(matrix_type, file, _normalize_planning_year(planning_year))


@router.get("/planning-years")
def get_planning_years():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        years = set()
        cursor.execute("SELECT year FROM planning_years ORDER BY year DESC")
        years.update(int(row["year"]) for row in cursor.fetchall() if row["year"] is not None)
        cursor.execute("""
            SELECT DISTINCT planning_year
            FROM upload_runs
            WHERE planning_year IS NOT NULL
        """)
        years.update(int(row["planning_year"]) for row in cursor.fetchall() if row["planning_year"] is not None)
        return {"years": sorted(years, reverse=True)}
    finally:
        conn.close()


@router.post("/planning-years")
def create_planning_year(request: PlanningYearRequest):
    planning_year = _normalize_planning_year(request.year)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO planning_years (year)
            VALUES (?)
            ON CONFLICT(year) DO NOTHING
        """, (planning_year,))
        conn.commit()
        return {"year": planning_year}
    finally:
        conn.close()

@router.get("/uploads")
def get_uploads():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM upload_runs ORDER BY uploaded_at DESC")
    rows = [_serialize_row(dict(row)) for row in cursor.fetchall()]
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


@router.post("/reports/control-report-clean-data/run")
def run_control_report_clean_data():
    conn = get_connection()
    cursor = conn.cursor()

    oth_upload_run_id = _get_latest_success_upload_id(cursor, "oth_data")
    group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country")
    machine_line_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "machine_line_mapping")
    brand_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "brand_mapping")

    missing_types = []
    if oth_upload_run_id is None:
        missing_types.append("oth_data")
    if group_country_upload_run_id is None:
        missing_types.append("group_country")
    if machine_line_mapping_upload_run_id is None:
        missing_types.append("machine_line_mapping")
    if brand_mapping_upload_run_id is None:
        missing_types.append("brand_mapping")

    if missing_types:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail=f"Missing latest successful upload for: {', '.join(missing_types)}"
        )

    cursor.execute("""
        INSERT INTO control_report_clean_runs (
            oth_upload_run_id,
            group_country_upload_run_id,
            machine_line_mapping_upload_run_id,
            brand_mapping_upload_run_id,
            status,
            message
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        oth_upload_run_id,
        group_country_upload_run_id,
        machine_line_mapping_upload_run_id,
        brand_mapping_upload_run_id,
        "running",
        "Control report run started"
    ))
    control_run_id = cursor.lastrowid
    conn.commit()

    try:
        cursor.execute("""
            SELECT
                o.year AS year,
                o.source AS source,
                o.country AS country_code,
                COALESCE(g.country_name, o.country) AS country,
                g.country_grouping AS country_grouping,
                g.region AS region,
                g.market_area AS market_area,
                COALESCE(m.machine_line_name, o.machine_line) AS machine_line_name,
                m.machine_line_code AS machine_line_code,
                COALESCE(b.brand_name, o.brand_name) AS brand_name,
                b.brand_code AS brand_code,
                o.size_class AS size_class_flag,
                o.quantity AS fid,
                NULL AS ms_percent
            FROM oth_data_rows o
            LEFT JOIN group_country_rows g
                ON UPPER(TRIM(o.country)) = UPPER(TRIM(g.country_code))
               AND UPPER(TRIM(o.year)) = UPPER(TRIM(g.year))
               AND g.upload_run_id = ?
            LEFT JOIN machine_line_mapping_rows m
                ON (
                    UPPER(TRIM(o.machine_line)) = UPPER(TRIM(m.machine_line_name))
                    OR UPPER(TRIM(o.machine_line)) = UPPER(TRIM(m.machine_line_code))
                )
               AND m.upload_run_id = ?
            LEFT JOIN brand_mapping_rows b
                ON UPPER(TRIM(o.brand_name)) = UPPER(TRIM(b.brand_name))
               AND b.upload_run_id = ?
            WHERE o.upload_run_id = ?
            ORDER BY o.row_index ASC
        """, (
            group_country_upload_run_id,
            machine_line_mapping_upload_run_id,
            brand_mapping_upload_run_id,
            oth_upload_run_id
        ))
        rows = cursor.fetchall()
        insert_rows = [
            (
                control_run_id,
                index,
                row["year"],
                row["source"],
                row["country_code"],
                row["country"],
                row["country_grouping"],
                row["region"],
                row["market_area"],
                row["machine_line_name"],
                row["machine_line_code"],
                row["brand_name"],
                row["brand_code"],
                row["size_class_flag"],
                row["fid"],
                row["ms_percent"],
            )
            for index, row in enumerate(rows, start=1)
        ]

        cursor.executemany("""
            INSERT INTO control_report_clean_rows (
                control_run_id,
                row_index,
                year,
                source,
                country_code,
                country,
                country_grouping,
                region,
                market_area,
                machine_line_name,
                machine_line_code,
                brand_name,
                brand_code,
                size_class_flag,
                fid,
                ms_percent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, insert_rows)

        cursor.execute("""
            UPDATE control_report_clean_runs
            SET row_count = ?, status = ?, message = ?
            WHERE id = ?
        """, (
            len(rows),
            "success",
            "Control Report - Clean Data generated successfully",
            control_run_id
        ))
        conn.commit()

        return {
            "message": "Control Report - Clean Data generated successfully",
            "control_run_id": control_run_id,
            "row_count": len(rows)
        }
    except Exception as e:
        cursor.execute("""
            UPDATE control_report_clean_runs
            SET status = ?, message = ?
            WHERE id = ?
        """, (
            "failed",
            str(e),
            control_run_id
        ))
        conn.commit()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/reports/control-report-clean-data/latest")
def get_latest_control_report_clean_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM control_report_clean_runs
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """)
    latest_run = cursor.fetchone()
    if latest_run is None:
        conn.close()
        raise HTTPException(status_code=404, detail="No control report runs found")

    latest_run_dict = dict(latest_run)
    cursor.execute("""
        SELECT *
        FROM control_report_clean_rows
        WHERE control_run_id = ?
        ORDER BY row_index ASC, id ASC
    """, (latest_run_dict["id"],))
    rows = [_serialize_row(dict(row)) for row in cursor.fetchall()]
    conn.close()

    return {
        "run": latest_run_dict,
        "rows": rows
    }


@router.post("/reports/crp-tma-clean-data/run")
def run_crp_tma_report_clean_data():
    conn = get_connection()
    cursor = conn.cursor()

    tma_upload_run_id = _get_latest_success_upload_id(cursor, "tma_data")
    if tma_upload_run_id is None:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Missing latest successful upload for: tma_data"
        )

    cursor.execute("SELECT planning_year FROM upload_runs WHERE id = ?", (tma_upload_run_id,))
    tma_upload = cursor.fetchone()
    planning_year = tma_upload["planning_year"] if tma_upload else None
    source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix", planning_year)
    if source_matrix_upload_run_id is None and planning_year is not None:
        source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix")
    if source_matrix_upload_run_id is None:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Missing latest successful upload for: source_matrix"
        )
    group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country", planning_year)
    if group_country_upload_run_id is None and planning_year is not None:
        group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country")
    if group_country_upload_run_id is None:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Missing latest successful upload for: group_country"
        )

    cursor.execute("""
        INSERT INTO crp_tma_report_runs (
            tma_upload_run_id,
            source_matrix_upload_run_id,
            status,
            message
        ) VALUES (?, ?, ?, ?)
    """, (
        tma_upload_run_id,
        source_matrix_upload_run_id,
        "running",
        "Control TMA report run started"
    ))
    report_run_id = cursor.lastrowid
    conn.commit()

    try:
        cursor.execute("""
            WITH group_country_by_name AS (
                SELECT
                    REPLACE(UPPER(TRIM(country_name)), ' ', '') AS country_name_key,
                    UPPER(TRIM(year)) AS year_key,
                    MIN(TRIM(country_code)) AS country_code
                FROM group_country_rows
                WHERE upload_run_id = ?
                GROUP BY
                    REPLACE(UPPER(TRIM(country_name)), ' ', ''),
                    UPPER(TRIM(year))
            ),
            source_keys AS (
                SELECT
                    UPPER(TRIM(COALESCE(NULLIF(TRIM(sm.country_code), ''), gc.country_code))) AS country_code_key,
                    TRIM(sm.country_name) AS country_name,
                    UPPER(TRIM(sm.machine_line_name)) AS machine_line_key,
                    COALESCE(NULLIF(TRIM(sm.artificial_machine_line), ''), TRIM(sm.machine_line_name)) AS artificial_machine_line
                FROM source_matrix_rows sm
                LEFT JOIN group_country_by_name gc
                  ON gc.country_name_key = REPLACE(UPPER(TRIM(sm.country_name)), ' ', '')
                WHERE sm.upload_run_id = ?
                  AND TRIM(COALESCE(sm.crp_source, '')) <> ''
                  AND TRIM(COALESCE(NULLIF(TRIM(sm.country_code), ''), gc.country_code, '')) <> ''
                GROUP BY
                    UPPER(TRIM(COALESCE(NULLIF(TRIM(sm.country_code), ''), gc.country_code))),
                    TRIM(sm.country_name),
                    UPPER(TRIM(sm.machine_line_name)),
                    COALESCE(NULLIF(TRIM(sm.artificial_machine_line), ''), TRIM(sm.machine_line_name))
            )
            SELECT
                t.year AS year,
                t.geographical_region AS geographical_region,
                t.geographical_market_area AS geographical_market_area,
                t.end_country_code AS end_country_code,
                sk.country_name AS country,
                GROUP_CONCAT(DISTINCT t.machine_line) AS machine_line,
                GROUP_CONCAT(DISTINCT t.machine_line_code) AS machine_line_code,
                sk.artificial_machine_line AS artificial_machine_line,
                t.size_class_mapping AS size_class_mapping,
                SUM(
                    CAST(
                        REPLACE(NULLIF(TRIM(t.total_market_fid_sales), ''), ',', '')
                        AS REAL
                    )
                ) AS fid_sum,
                'TMA' AS source
            FROM tma_data_rows t
            JOIN source_keys sk
              ON sk.country_code_key = UPPER(TRIM(t.end_country_code))
             AND sk.machine_line_key = UPPER(TRIM(t.machine_line))
            WHERE t.upload_run_id = ?
            GROUP BY
                t.year,
                t.geographical_region,
                t.geographical_market_area,
                t.end_country_code,
                sk.country_name,
                sk.artificial_machine_line,
                t.size_class_mapping
            ORDER BY
                t.year,
                t.geographical_region,
                t.geographical_market_area,
                t.end_country_code,
                sk.country_name,
                sk.artificial_machine_line,
                t.size_class_mapping
        """, (group_country_upload_run_id, source_matrix_upload_run_id, tma_upload_run_id))
        rows = cursor.fetchall()

        for index, row in enumerate(rows, start=1):
            cursor.execute("""
                INSERT INTO crp_tma_report_rows (
                    report_run_id,
                    row_index,
                    year,
                    geographical_region,
                    geographical_market_area,
                    end_country_code,
                    country,
                    machine_line,
                    machine_line_code,
                    artificial_machine_line,
                    size_class_mapping,
                    fid_sum,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                report_run_id,
                index,
                row["year"],
                row["geographical_region"],
                row["geographical_market_area"],
                row["end_country_code"],
                row["country"],
                row["machine_line"],
                row["machine_line_code"],
                row["artificial_machine_line"],
                row["size_class_mapping"],
                row["fid_sum"],
                row["source"]
            ))

        cursor.execute("""
            UPDATE crp_tma_report_runs
            SET row_count = ?, status = ?, message = ?
            WHERE id = ?
        """, (
            len(rows),
            "success",
            "Control TMA report generated successfully",
            report_run_id
        ))
        conn.commit()

        return {
            "message": "Control TMA report generated successfully",
            "report_run_id": report_run_id,
            "row_count": len(rows)
        }
    except Exception as e:
        cursor.execute("""
            UPDATE crp_tma_report_runs
            SET status = ?, message = ?
            WHERE id = ?
        """, (
            "failed",
            str(e),
            report_run_id
        ))
        conn.commit()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/reports/crp-tma-clean-data/latest")
def get_latest_crp_tma_report_clean_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM crp_tma_report_runs
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """)
    latest_run = cursor.fetchone()
    if latest_run is None:
        conn.close()
        raise HTTPException(status_code=404, detail="No CRP TMA report runs found")

    latest_run_dict = dict(latest_run)
    cursor.execute("""
        SELECT *
        FROM crp_tma_report_rows
        WHERE report_run_id = ?
        ORDER BY row_index ASC, id ASC
    """, (latest_run_dict["id"],))
    rows = [_serialize_row(dict(row)) for row in cursor.fetchall()]
    conn.close()

    return {
        "run": latest_run_dict,
        "rows": rows
    }


@router.post("/reports/crp-sal-clean-data/run")
def run_crp_sal_report_clean_data():
    conn = get_connection()
    cursor = conn.cursor()

    volvo_upload_run_id = _get_latest_success_upload_id(cursor, "volvo_sale_data")
    if volvo_upload_run_id is None:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Missing latest successful upload for: volvo_sale_data"
        )

    cursor.execute("SELECT planning_year FROM upload_runs WHERE id = ?", (volvo_upload_run_id,))
    volvo_upload = cursor.fetchone()
    planning_year = volvo_upload["planning_year"] if volvo_upload else None
    source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix", planning_year)
    if source_matrix_upload_run_id is None and planning_year is not None:
        source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix")
    if source_matrix_upload_run_id is None:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Missing latest successful upload for: source_matrix"
        )

    cursor.execute("""
        INSERT INTO crp_sal_report_runs (
            volvo_upload_run_id,
            source_matrix_upload_run_id,
            status,
            message
        ) VALUES (?, ?, ?, ?)
    """, (
        volvo_upload_run_id,
        source_matrix_upload_run_id,
        "running",
        "Control Volvo SAL report run started"
    ))
    report_run_id = cursor.lastrowid
    conn.commit()

    try:
        cursor.execute("""
            WITH source_keys AS (
                SELECT
                    UPPER(TRIM(country_name)) AS country_key,
                    UPPER(TRIM(machine_line_name)) AS machine_line_key,
                    COALESCE(NULLIF(TRIM(artificial_machine_line), ''), TRIM(machine_line_name)) AS artificial_machine_line
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(crp_source, '')) <> ''
                GROUP BY
                    UPPER(TRIM(country_name)),
                    UPPER(TRIM(machine_line_name)),
                    COALESCE(NULLIF(TRIM(artificial_machine_line), ''), TRIM(machine_line_name))
            )
            SELECT
                v.calendar,
                v.region,
                v.market,
                v.country,
                GROUP_CONCAT(DISTINCT v.machine) AS machine,
                GROUP_CONCAT(DISTINCT v.machine_line) AS machine_line,
                sk.artificial_machine_line AS artificial_machine_line,
                CASE
                    WHEN UPPER(TRIM(COALESCE(v.size_class, ''))) = 'MINI' THEN '<6T'
                    WHEN UPPER(TRIM(COALESCE(v.size_class, ''))) = 'MIDI' THEN '6<10T'
                    ELSE TRIM(v.size_class)
                END AS size_class,
                GROUP_CONCAT(DISTINCT v.brand_owner_code) AS brand_owner_code,
                GROUP_CONCAT(DISTINCT v.brand_owner) AS brand_owner,
                GROUP_CONCAT(DISTINCT v.brand) AS brand,
                GROUP_CONCAT(DISTINCT v.brand_nationality) AS brand_nationality,
                COALESCE(NULLIF(TRIM(v.source), ''), 'SAL') AS source,
                SUM(CAST(REPLACE(NULLIF(TRIM(v.fid), ''), ',', '') AS REAL)) AS fid
            FROM volvo_sale_data_rows v
            JOIN source_keys sk
              ON sk.country_key = UPPER(TRIM(v.country))
             AND sk.machine_line_key = UPPER(TRIM(v.machine_line))
            WHERE v.upload_run_id = ?
            GROUP BY
                v.calendar,
                v.region,
                v.market,
                v.country,
                sk.artificial_machine_line,
                CASE
                    WHEN UPPER(TRIM(COALESCE(v.size_class, ''))) = 'MINI' THEN '<6T'
                    WHEN UPPER(TRIM(COALESCE(v.size_class, ''))) = 'MIDI' THEN '6<10T'
                    ELSE TRIM(v.size_class)
                END,
                COALESCE(NULLIF(TRIM(v.source), ''), 'SAL')
            ORDER BY
                v.calendar,
                v.region,
                v.market,
                v.country,
                sk.artificial_machine_line,
                v.size_class
        """, (source_matrix_upload_run_id, volvo_upload_run_id))
        rows = cursor.fetchall()

        for index, row in enumerate(rows, start=1):
            cursor.execute("""
                INSERT INTO crp_sal_report_rows (
                    report_run_id,
                    row_index,
                    calendar,
                    region,
                    market,
                    country,
                    machine,
                    machine_line,
                    artificial_machine_line,
                    size_class,
                    brand_owner_code,
                    brand_owner,
                    brand,
                    brand_nationality,
                    source,
                    fid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                report_run_id,
                index,
                row["calendar"],
                row["region"],
                row["market"],
                row["country"],
                row["machine"],
                row["machine_line"],
                row["artificial_machine_line"],
                row["size_class"],
                row["brand_owner_code"],
                row["brand_owner"],
                row["brand"],
                row["brand_nationality"],
                row["source"],
                row["fid"],
            ))

        cursor.execute("""
            UPDATE crp_sal_report_runs
            SET row_count = ?, status = ?, message = ?
            WHERE id = ?
        """, (
            len(rows),
            "success",
            "Control Volvo SAL report generated successfully",
            report_run_id
        ))
        conn.commit()

        return {
            "message": "Control Volvo SAL report generated successfully",
            "report_run_id": report_run_id,
            "row_count": len(rows)
        }
    except Exception as e:
        cursor.execute("""
            UPDATE crp_sal_report_runs
            SET status = ?, message = ?
            WHERE id = ?
        """, (
            "failed",
            str(e),
            report_run_id
        ))
        conn.commit()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/reports/crp-sal-clean-data/latest")
def get_latest_crp_sal_report_clean_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM crp_sal_report_runs
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """)
    latest_run = cursor.fetchone()
    if latest_run is None:
        conn.close()
        raise HTTPException(status_code=404, detail="No Control Volvo SAL report runs found")

    latest_run_dict = dict(latest_run)
    cursor.execute("""
        SELECT *
        FROM crp_sal_report_rows
        WHERE report_run_id = ?
        ORDER BY row_index ASC, id ASC
    """, (latest_run_dict["id"],))
    rows = [_serialize_row(dict(row)) for row in cursor.fetchall()]
    conn.close()

    return {
        "run": latest_run_dict,
        "rows": rows
    }


def _get_crp_d1_combined_report_data(include_all_sal: bool, planning_year: int | None = None):
    conn = get_connection()
    cursor = conn.cursor()

    latest_tma_upload_run_id = _get_latest_success_upload_id(cursor, "tma_data", planning_year)
    latest_volvo_upload_run_id = _get_latest_success_upload_id(cursor, "volvo_sale_data", planning_year)
    latest_group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country", planning_year)
    latest_source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix", planning_year)
    latest_machine_line_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "machine_line_mapping", planning_year)
    latest_reporter_list_upload_run_id = _get_latest_success_upload_id(cursor, "reporter_list", planning_year)

    missing_types = []
    if latest_tma_upload_run_id is None:
        missing_types.append("tma_data")
    if latest_volvo_upload_run_id is None:
        missing_types.append("volvo_sale_data")
    if latest_group_country_upload_run_id is None:
        missing_types.append("group_country")
    if latest_source_matrix_upload_run_id is None:
        missing_types.append("source_matrix")
    if latest_machine_line_mapping_upload_run_id is None:
        missing_types.append("machine_line_mapping")
    if latest_reporter_list_upload_run_id is None:
        missing_types.append("reporter_list")

    if missing_types:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail=f"Missing latest successful upload for: {', '.join(missing_types)}"
        )

    try:
        filtered_rows_cte = """
            filtered_rows AS (
                SELECT * FROM final_rows
            ),
        """
        if not include_all_sal:
            filtered_rows_cte = """
            filtered_rows AS (
                SELECT *
                FROM final_rows
                WHERE (
                        UPPER(TRIM(source)) <> 'SAL'
                     OR TRIM(COALESCE(reporter_flag, '')) <> ''
                )
                  AND TRIM(COALESCE(machine_line_code, '')) <> '390'
                  AND UPPER(TRIM(COALESCE(artificial_machine_line, ''))) <> 'COMPACTION MACHINES'
            ),
        """

        cursor.execute(f"""
            WITH gc_by_code AS (
                SELECT
                    UPPER(TRIM(country_code)) AS country_code_key,
                    UPPER(TRIM(year)) AS year_key,
                    MIN(country_code) AS country_code,
                    MIN(group_code) AS group_code,
                    MIN(country_grouping) AS country_grouping,
                    MIN(country_name) AS country_name,
                    MIN(region) AS region
                FROM group_country_rows
                WHERE upload_run_id = ?
                GROUP BY UPPER(TRIM(country_code)), UPPER(TRIM(year))
            ),
            gc_by_name AS (
                SELECT
                    REPLACE(UPPER(TRIM(country_name)), ' ', '') AS country_name_key,
                    UPPER(TRIM(year)) AS year_key,
                    MIN(country_code) AS country_code,
                    MIN(group_code) AS group_code,
                    MIN(country_grouping) AS country_grouping,
                    MIN(country_name) AS country_name,
                    MIN(region) AS region
                FROM group_country_rows
                WHERE upload_run_id = ?
                GROUP BY UPPER(TRIM(country_name)), UPPER(TRIM(year))
            ),
            tma_agg AS (
                SELECT
                    TRIM(t.year) AS year,
                    TRIM(t.end_country_code) AS end_country_code,
                    TRIM(t.end_country) AS country_raw,
                    TRIM(t.geographical_region) AS region_raw,
                    TRIM(t.machine_line_code) AS machine_line_code,
                    TRIM(t.machine_line) AS machine_line_name,
                    TRIM(t.size_class_mapping) AS size_class,
                    SUM(
                        CAST(REPLACE(NULLIF(TRIM(t.total_market_fid_sales), ''), ',', '') AS REAL)
                    ) AS fid,
                    'TMA' AS source,
                    '#' AS brand_code,
                    'TOTAL MARKET' AS brand_name
                FROM tma_data_rows t
                WHERE t.upload_run_id = ?
                GROUP BY
                    TRIM(t.year),
                    TRIM(t.end_country_code),
                    TRIM(t.end_country),
                    TRIM(t.geographical_region),
                    TRIM(t.machine_line_code),
                    TRIM(t.machine_line),
                    TRIM(t.size_class_mapping)
            ),
            volvo_agg AS (
                SELECT
                    TRIM(v.calendar) AS year,
                    TRIM(v.country) AS end_country_code,
                    TRIM(v.country) AS country_raw,
                    TRIM(v.region) AS region_raw,
                    TRIM(v.machine) AS machine_line_code,
                    TRIM(v.machine_line) AS machine_line_name,
                    TRIM(v.size_class) AS size_class,
                    SUM(
                        CAST(REPLACE(NULLIF(TRIM(v.fid), ''), ',', '') AS REAL)
                    ) AS fid,
                    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL') AS source,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'ROKBAK' THEN 'ABG'
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'SDLG' THEN 'SDLG'
                        ELSE 'VCE'
                    END AS brand_code,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'ROKBAK' THEN 'Rokbak'
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'SDLG' THEN 'SDLG'
                        ELSE 'VOLVO CE'
                    END AS brand_name
                FROM volvo_sale_data_rows v
                WHERE v.upload_run_id = ?
                GROUP BY
                    TRIM(v.calendar),
                    TRIM(v.country),
                    TRIM(v.region),
                    TRIM(v.machine),
                    TRIM(v.machine_line),
                    TRIM(v.size_class),
                    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL'),
                    CASE
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'ROKBAK' THEN 'ABG'
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'SDLG' THEN 'SDLG'
                        ELSE 'VCE'
                    END,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'ROKBAK' THEN 'Rokbak'
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'SDLG' THEN 'SDLG'
                        ELSE 'VOLVO CE'
                    END
            ),
            all_agg AS (
                SELECT * FROM tma_agg
                UNION ALL
                SELECT * FROM volvo_agg
            ),
            source_matrix_country_artificial_lines AS (
                SELECT
                    REPLACE(UPPER(TRIM(country_name)), ' ', '') AS country_name_key,
                    UPPER(TRIM(artificial_machine_line)) AS artificial_machine_line_key,
                    MAX(
                        CASE
                            WHEN TRIM(COALESCE(crp_source, '')) <> '' THEN TRIM(crp_source)
                            ELSE NULL
                        END
                    ) AS crp_source
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(country_name, '')) <> ''
                  AND TRIM(COALESCE(artificial_machine_line, '')) <> ''
                  AND TRIM(COALESCE(crp_source, '')) <> ''
                GROUP BY
                    REPLACE(UPPER(TRIM(country_name)), ' ', ''),
                    UPPER(TRIM(artificial_machine_line))
            ),
            reporter_list_artificial_brand AS (
                SELECT
                    UPPER(TRIM(source_code)) AS source_code_key,
                    UPPER(TRIM(artificial_machine_line)) AS artificial_machine_line_key,
                    UPPER(TRIM(brand_code)) AS brand_code_key
                FROM reporter_list_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(source_code, '')) <> ''
                  AND TRIM(COALESCE(artificial_machine_line, '')) <> ''
                  AND TRIM(COALESCE(brand_code, '')) <> ''
                GROUP BY
                    UPPER(TRIM(source_code)),
                    UPPER(TRIM(artificial_machine_line)),
                    UPPER(TRIM(brand_code))
            ),
            final_rows_base AS (
                SELECT
                    ROW_NUMBER() OVER (
                        ORDER BY
                            a.year,
                            COALESCE(g_code.group_code, g_name.group_code, ''),
                            COALESCE(g_code.country_grouping, g_name.country_grouping, ''),
                            COALESCE(g_code.country_name, g_name.country_name, a.country_raw),
                            COALESCE(g_code.region, g_name.region, a.region_raw),
                            a.machine_line_code,
                            a.machine_line_name,
                            a.size_class,
                            a.source,
                            a.fid
                    ) AS base_row_id,
                    a.year AS year,
                    COALESCE(g_code.group_code, g_name.group_code, '') AS country_group_code,
                    COALESCE(g_code.country_code, g_name.country_code, a.end_country_code, '') AS country_code,
                    COALESCE(g_code.country_grouping, g_name.country_grouping, '') AS country_grouping,
                    COALESCE(g_code.country_name, g_name.country_name, a.country_raw) AS country,
                    COALESCE(g_code.region, g_name.region, a.region_raw) AS region,
                    a.machine_line_code AS machine_line_code,
                    a.machine_line_name AS machine_line_name,
                    a.size_class AS size_class,
                    a.brand_code AS brand_code,
                    a.brand_name AS brand_name,
                    '#' AS pri_sec,
                    a.source AS source,
                    a.fid AS fid
                FROM all_agg a
                LEFT JOIN gc_by_code g_code
                  ON UPPER(TRIM(a.end_country_code)) = g_code.country_code_key
                 AND UPPER(TRIM(a.year)) = g_code.year_key
                LEFT JOIN gc_by_name g_name
                  ON REPLACE(UPPER(TRIM(a.country_raw)), ' ', '') = g_name.country_name_key
                 AND UPPER(TRIM(a.year)) = g_name.year_key
            ),
            machine_line_mapping_matches AS (
                SELECT
                    frb.base_row_id AS base_row_id,
                    TRIM(mlm.artificial_machine_line) AS artificial_machine_line,
                    ROW_NUMBER() OVER (
                        PARTITION BY frb.base_row_id
                        ORDER BY
                            CASE
                                WHEN UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, ''))) THEN 0
                                WHEN UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, ''))) THEN 1
                                WHEN UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, ''))) THEN 2
                                WHEN UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, ''))) THEN 3
                                ELSE 4
                            END,
                            mlm.row_index ASC,
                            mlm.id ASC
                    ) AS match_rank
                FROM final_rows_base frb
                JOIN machine_line_mapping_rows mlm
                  ON mlm.upload_run_id = ?
                 AND UPPER(TRIM(COALESCE(mlm.size_class, ''))) = UPPER(TRIM(COALESCE(frb.size_class, '')))
                 AND (
                        UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, '')))
                     OR UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, '')))
                     OR UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, '')))
                     OR UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, '')))
                 )
                 AND (
                        TRIM(COALESCE(mlm.position, '')) = ''
                     OR INSTR(
                            ',' || REPLACE(UPPER(TRIM(COALESCE(mlm.position, ''))), ' ', '') || ',',
                            ',' || REPLACE(UPPER(TRIM(COALESCE(frb.source, ''))), ' ', '') || ','
                        ) > 0
                 )
            ),
            final_rows AS (
                SELECT
                    frb.base_row_id AS base_row_id,
                    frb.year AS year,
                    frb.country_group_code AS country_group_code,
                    frb.country_code AS country_code,
                    frb.country_grouping AS country_grouping,
                    frb.country AS country,
                    frb.region AS region,
                    frb.machine_line_code AS machine_line_code,
                    frb.machine_line_name AS machine_line_name,
                    frb.size_class AS size_class,
                    COALESCE(mlmm.artificial_machine_line, '') AS artificial_machine_line,
                    frb.brand_code AS brand_code,
                    frb.brand_name AS brand_name,
                    CASE
                        WHEN UPPER(TRIM(frb.source)) = 'TMA' THEN '#'
                        WHEN UPPER(TRIM(frb.source)) = 'SAL' THEN 'Y'
                        ELSE ''
                    END AS reporter_flag,
                    frb.pri_sec AS pri_sec,
                    frb.source AS source,
                    CASE
                        WHEN UPPER(TRIM(frb.source)) = 'SAL'
                             AND TRIM(CAST(frb.machine_line_code AS TEXT)) = '390' THEN 'Y'
                        WHEN UPPER(TRIM(frb.source)) = 'SAL'
                             AND TRIM(COALESCE(mlmm.artificial_machine_line, '')) <> ''
                             AND sm_artificial.country_name_key IS NULL THEN 'Y'
                        ELSE ''
                    END AS deletion_flag,
                    frb.fid AS fid
                FROM final_rows_base frb
                LEFT JOIN machine_line_mapping_matches mlmm
                  ON frb.base_row_id = mlmm.base_row_id
                 AND mlmm.match_rank = 1
                LEFT JOIN source_matrix_country_artificial_lines sm_artificial
                  ON REPLACE(UPPER(TRIM(COALESCE(frb.country, ''))), ' ', '') = sm_artificial.country_name_key
                 AND UPPER(TRIM(COALESCE(mlmm.artificial_machine_line, ''))) = sm_artificial.artificial_machine_line_key
                LEFT JOIN reporter_list_artificial_brand rl_artificial
                  ON UPPER(TRIM(COALESCE(sm_artificial.crp_source, ''))) = rl_artificial.source_code_key
                 AND UPPER(TRIM(COALESCE(mlmm.artificial_machine_line, ''))) = rl_artificial.artificial_machine_line_key
                 AND UPPER(TRIM(COALESCE(frb.brand_code, ''))) = rl_artificial.brand_code_key
            ),
            {filtered_rows_cte}
            display_rows AS (
                SELECT
                    year,
                    country_group_code,
                    country_code,
                    country_grouping,
                    country,
                    region,
                    machine_line_code,
                    machine_line_name,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(source, ''))) = 'SAL'
                         AND UPPER(TRIM(COALESCE(size_class, ''))) = 'MINI'
                        THEN '<6T'
                        WHEN UPPER(TRIM(COALESCE(source, ''))) = 'SAL'
                         AND UPPER(TRIM(COALESCE(size_class, ''))) = 'MIDI'
                        THEN '6<10T'
                        ELSE size_class
                    END AS size_class,
                    artificial_machine_line,
                    brand_code,
                    brand_name,
                    reporter_flag,
                    pri_sec,
                    source,
                    deletion_flag,
                    SUM(COALESCE(CAST(NULLIF(REPLACE(TRIM(CAST(fid AS TEXT)), ',', ''), '') AS DOUBLE PRECISION), 0)) AS fid
                FROM filtered_rows
                GROUP BY
                    year,
                    country_group_code,
                    country_code,
                    country_grouping,
                    country,
                    region,
                    machine_line_code,
                    machine_line_name,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(source, ''))) = 'SAL'
                         AND UPPER(TRIM(COALESCE(size_class, ''))) = 'MINI'
                        THEN '<6T'
                        WHEN UPPER(TRIM(COALESCE(source, ''))) = 'SAL'
                         AND UPPER(TRIM(COALESCE(size_class, ''))) = 'MIDI'
                        THEN '6<10T'
                        ELSE size_class
                    END,
                    artificial_machine_line,
                    brand_code,
                    brand_name,
                    reporter_flag,
                    pri_sec,
                    source,
                    deletion_flag
            ),
            row_stats AS (
                SELECT
                    year,
                    country,
                    UPPER(TRIM(COALESCE(machine_line_code, ''))) AS machine_line_code_key,
                    UPPER(TRIM(COALESCE(machine_line_name, ''))) AS machine_line_name_key,
                    UPPER(TRIM(COALESCE(size_class, ''))) AS size_class_key,
                    SUM(
                        CASE
                            WHEN UPPER(TRIM(source)) = 'TMA' THEN COALESCE(CAST(NULLIF(REPLACE(TRIM(CAST(fid AS TEXT)), ',', ''), '') AS DOUBLE PRECISION), 0)
                            ELSE 0
                        END
                    ) AS tm,
                    SUM(
                        CASE
                            WHEN UPPER(TRIM(source)) = 'SAL'
                                 AND UPPER(TRIM(COALESCE(reporter_flag, ''))) = 'Y'
                                 AND UPPER(TRIM(COALESCE(deletion_flag, ''))) <> 'Y'
                            THEN COALESCE(CAST(NULLIF(REPLACE(TRIM(CAST(fid AS TEXT)), ',', ''), '') AS DOUBLE PRECISION), 0)
                            ELSE 0
                        END
                    ) AS vce_fid,
                    MAX(CASE WHEN UPPER(TRIM(source)) = 'TMA' THEN 1 ELSE 0 END) AS has_tm,
                    MAX(
                        CASE
                            WHEN UPPER(TRIM(source)) = 'SAL'
                                 AND UPPER(TRIM(COALESCE(reporter_flag, ''))) = 'Y'
                                 AND UPPER(TRIM(COALESCE(deletion_flag, ''))) <> 'Y'
                            THEN 1
                            ELSE 0
                        END
                    ) AS has_vce
                FROM display_rows
                WHERE UPPER(TRIM(COALESCE(machine_line_code, ''))) <> 'MOTOR GRADERS'
                  AND UPPER(TRIM(COALESCE(machine_line_name, ''))) <> 'MOTOR GRADERS'
                GROUP BY
                    year,
                    country,
                    UPPER(TRIM(COALESCE(machine_line_code, ''))),
                    UPPER(TRIM(COALESCE(machine_line_name, ''))),
                    UPPER(TRIM(COALESCE(size_class, '')))
            )
            SELECT
                fr.year AS year,
                fr.country_group_code AS country_group_code,
                fr.country_code AS country_code,
                fr.country_grouping AS country_grouping,
                fr.country AS country,
                fr.region AS region,
                fr.machine_line_code AS machine_line_code,
                fr.machine_line_name AS machine_line_name,
                fr.size_class AS size_class,
                fr.artificial_machine_line AS artificial_machine_line,
                fr.brand_code AS brand_code,
                fr.brand_name AS brand_name,
                fr.reporter_flag AS reporter_flag,
                fr.pri_sec AS pri_sec,
                fr.source AS source,
                fr.deletion_flag AS deletion_flag,
                fr.fid AS fid,
                CASE
                    WHEN UPPER(TRIM(COALESCE(fr.source, ''))) = 'SAL' THEN NULL
                    ELSE COALESCE(rs.tm, 0)
                END AS tm,
                CASE
                    WHEN UPPER(TRIM(COALESCE(fr.source, ''))) = 'SAL' THEN NULL
                    ELSE COALESCE(rs.vce_fid, 0)
                END AS vce_fid,
                CASE
                    WHEN UPPER(TRIM(COALESCE(fr.source, ''))) = 'TMA' THEN
                        CASE
                            WHEN COALESCE(rs.tm, 0) - COALESCE(rs.vce_fid, 0) > 0 THEN COALESCE(rs.tm, 0) - COALESCE(rs.vce_fid, 0)
                            ELSE 0
                        END
                    ELSE NULL
                END AS tm_non_vce
            FROM display_rows fr
            LEFT JOIN row_stats rs
              ON fr.year = rs.year
             AND fr.country = rs.country
             AND UPPER(TRIM(COALESCE(fr.machine_line_code, ''))) = rs.machine_line_code_key
             AND UPPER(TRIM(COALESCE(fr.machine_line_name, ''))) = rs.machine_line_name_key
             AND UPPER(TRIM(COALESCE(fr.size_class, ''))) = rs.size_class_key
            ORDER BY
                country_grouping,
                country_group_code,
                country,
                machine_line_code,
                machine_line_name,
                size_class
        """, (
            latest_group_country_upload_run_id,
            latest_group_country_upload_run_id,
            latest_tma_upload_run_id,
            latest_volvo_upload_run_id,
            latest_source_matrix_upload_run_id,
            latest_reporter_list_upload_run_id,
            latest_machine_line_mapping_upload_run_id,
        ))

        rows = [dict(row) for row in cursor.fetchall()]
        return {
            "row_count": len(rows),
            "rows": rows,
            "tma_upload_run_id": latest_tma_upload_run_id,
            "volvo_upload_run_id": latest_volvo_upload_run_id,
            "group_country_upload_run_id": latest_group_country_upload_run_id,
            "source_matrix_upload_run_id": latest_source_matrix_upload_run_id,
            "machine_line_mapping_upload_run_id": latest_machine_line_mapping_upload_run_id,
            "reporter_list_upload_run_id": latest_reporter_list_upload_run_id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/reports/crp-d1-combined")
def get_crp_d1_combined_report(track_run: bool = False, planning_year: int | None = Query(default=None)):
    normalized_year = _normalize_planning_year(planning_year)
    result = _get_crp_d1_combined_report_data(include_all_sal=True, planning_year=normalized_year)
    if track_run:
        _record_report_run(P00_RUN_KEYS["crp_d1_combined"])
        _save_p00_report_snapshot(
            P00_RUN_KEYS["crp_d1_combined"],
            result["rows"],
            "CRP D1 Combined report generated successfully",
            meta={
                "planning_year": normalized_year,
                "tma_upload_run_id": result["tma_upload_run_id"],
                "volvo_upload_run_id": result["volvo_upload_run_id"],
                "group_country_upload_run_id": result["group_country_upload_run_id"],
                "source_matrix_upload_run_id": result["source_matrix_upload_run_id"],
                "machine_line_mapping_upload_run_id": result["machine_line_mapping_upload_run_id"],
                "reporter_list_upload_run_id": result["reporter_list_upload_run_id"],
            },
            planning_year=normalized_year,
        )
    return result


@router.get("/reports/a10-adjustment")
def get_a10_adjustment_report(planning_year: int | None = Query(default=None)):
    normalized_year = _normalize_planning_year(planning_year)
    conn = get_connection()
    cursor = conn.cursor()

    latest_tma_upload_run_id = _get_latest_success_upload_id(cursor, "tma_data", normalized_year)
    latest_volvo_upload_run_id = _get_latest_success_upload_id(cursor, "volvo_sale_data", normalized_year)
    latest_group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country", normalized_year)
    latest_source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix", normalized_year)
    latest_machine_line_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "machine_line_mapping", normalized_year)
    latest_reporter_list_upload_run_id = _get_latest_success_upload_id(cursor, "reporter_list", normalized_year)

    missing_types = []
    if latest_tma_upload_run_id is None:
        missing_types.append("tma_data")
    if latest_volvo_upload_run_id is None:
        missing_types.append("volvo_sale_data")
    if latest_group_country_upload_run_id is None:
        missing_types.append("group_country")
    if latest_source_matrix_upload_run_id is None:
        missing_types.append("source_matrix")
    if latest_machine_line_mapping_upload_run_id is None:
        missing_types.append("machine_line_mapping")
    if latest_reporter_list_upload_run_id is None:
        missing_types.append("reporter_list")

    if missing_types:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail=f"Missing latest successful upload for: {', '.join(missing_types)}"
        )

    try:
        cursor.execute("""
            WITH gc_by_code AS (
                SELECT
                    UPPER(TRIM(country_code)) AS country_code_key,
                    UPPER(TRIM(year)) AS year_key,
                    MIN(country_code) AS country_code,
                    MIN(group_code) AS group_code,
                    MIN(country_grouping) AS country_grouping,
                    MIN(country_name) AS country_name,
                    MIN(region) AS region
                FROM group_country_rows
                WHERE upload_run_id = ?
                GROUP BY UPPER(TRIM(country_code)), UPPER(TRIM(year))
            ),
            gc_by_name AS (
                SELECT
                    REPLACE(UPPER(TRIM(country_name)), ' ', '') AS country_name_key,
                    UPPER(TRIM(year)) AS year_key,
                    MIN(country_code) AS country_code,
                    MIN(group_code) AS group_code,
                    MIN(country_grouping) AS country_grouping,
                    MIN(country_name) AS country_name,
                    MIN(region) AS region
                FROM group_country_rows
                WHERE upload_run_id = ?
                GROUP BY UPPER(TRIM(country_name)), UPPER(TRIM(year))
            ),
            tma_agg AS (
                SELECT
                    TRIM(t.year) AS year,
                    TRIM(t.end_country_code) AS end_country_code,
                    TRIM(t.end_country) AS country_raw,
                    TRIM(t.geographical_region) AS region_raw,
                    TRIM(t.machine_line_code) AS machine_line_code,
                    TRIM(t.machine_line) AS machine_line_name,
                    TRIM(t.size_class_mapping) AS size_class,
                    SUM(
                        CAST(REPLACE(NULLIF(TRIM(t.total_market_fid_sales), ''), ',', '') AS REAL)
                    ) AS fid,
                    'TMA' AS source,
                    '#' AS brand_code,
                    'TOTAL MARKET' AS brand_name
                FROM tma_data_rows t
                WHERE t.upload_run_id = ?
                GROUP BY
                    TRIM(t.year),
                    TRIM(t.end_country_code),
                    TRIM(t.end_country),
                    TRIM(t.geographical_region),
                    TRIM(t.machine_line_code),
                    TRIM(t.machine_line),
                    TRIM(t.size_class_mapping)
            ),
            volvo_agg AS (
                SELECT
                    TRIM(v.calendar) AS year,
                    TRIM(v.country) AS end_country_code,
                    TRIM(v.country) AS country_raw,
                    TRIM(v.region) AS region_raw,
                    TRIM(v.machine) AS machine_line_code,
                    TRIM(v.machine_line) AS machine_line_name,
                    TRIM(v.size_class) AS size_class,
                    SUM(
                        CAST(REPLACE(NULLIF(TRIM(v.fid), ''), ',', '') AS REAL)
                    ) AS fid,
                    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL') AS source,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'ROKBAK' THEN 'ABG'
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'SDLG' THEN 'SDLG'
                        ELSE 'VCE'
                    END AS brand_code,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'ROKBAK' THEN 'Rokbak'
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'SDLG' THEN 'SDLG'
                        ELSE 'VOLVO CE'
                    END AS brand_name
                FROM volvo_sale_data_rows v
                WHERE v.upload_run_id = ?
                GROUP BY
                    TRIM(v.calendar),
                    TRIM(v.country),
                    TRIM(v.region),
                    TRIM(v.machine),
                    TRIM(v.machine_line),
                    TRIM(v.size_class),
                    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL'),
                    CASE
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'ROKBAK' THEN 'ABG'
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'SDLG' THEN 'SDLG'
                        ELSE 'VCE'
                    END,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'ROKBAK' THEN 'Rokbak'
                        WHEN UPPER(TRIM(COALESCE(v.brand, ''))) = 'SDLG' THEN 'SDLG'
                        ELSE 'VOLVO CE'
                    END
            ),
            all_agg AS (
                SELECT * FROM tma_agg
                UNION ALL
                SELECT * FROM volvo_agg
            ),
            source_matrix_country_artificial_lines AS (
                SELECT
                    REPLACE(UPPER(TRIM(country_name)), ' ', '') AS country_name_key,
                    UPPER(TRIM(artificial_machine_line)) AS artificial_machine_line_key,
                    MAX(
                        CASE
                            WHEN TRIM(COALESCE(crp_source, '')) <> '' THEN TRIM(crp_source)
                            ELSE NULL
                        END
                    ) AS crp_source
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(country_name, '')) <> ''
                  AND TRIM(COALESCE(artificial_machine_line, '')) <> ''
                  AND TRIM(COALESCE(crp_source, '')) <> ''
                GROUP BY
                    REPLACE(UPPER(TRIM(country_name)), ' ', ''),
                    UPPER(TRIM(artificial_machine_line))
            ),
            reporter_list_artificial_brand AS (
                SELECT
                    UPPER(TRIM(source_code)) AS source_code_key,
                    UPPER(TRIM(artificial_machine_line)) AS artificial_machine_line_key,
                    UPPER(TRIM(brand_code)) AS brand_code_key
                FROM reporter_list_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(source_code, '')) <> ''
                  AND TRIM(COALESCE(artificial_machine_line, '')) <> ''
                  AND TRIM(COALESCE(brand_code, '')) <> ''
                GROUP BY
                    UPPER(TRIM(source_code)),
                    UPPER(TRIM(artificial_machine_line)),
                    UPPER(TRIM(brand_code))
            ),
            final_rows_base AS (
                SELECT
                    ROW_NUMBER() OVER (
                        ORDER BY
                            a.year,
                            COALESCE(g_code.group_code, g_name.group_code, ''),
                            COALESCE(g_code.country_grouping, g_name.country_grouping, ''),
                            COALESCE(g_code.country_name, g_name.country_name, a.country_raw),
                            COALESCE(g_code.region, g_name.region, a.region_raw),
                            a.machine_line_code,
                            a.machine_line_name,
                            a.size_class,
                            a.source,
                            a.fid
                    ) AS base_row_id,
                    a.year AS year,
                    COALESCE(g_code.group_code, g_name.group_code, '') AS country_group_code,
                    COALESCE(g_code.country_code, g_name.country_code, a.end_country_code, '') AS country_code,
                    COALESCE(g_code.country_grouping, g_name.country_grouping, '') AS country_grouping,
                    COALESCE(g_code.country_name, g_name.country_name, a.country_raw) AS country,
                    COALESCE(g_code.region, g_name.region, a.region_raw) AS region,
                    a.machine_line_code AS machine_line_code,
                    a.machine_line_name AS machine_line_name,
                    a.size_class AS size_class,
                    a.brand_code AS brand_code,
                    a.brand_name AS brand_name,
                    '#' AS pri_sec,
                    a.source AS source,
                    a.fid AS raw_fid
                FROM all_agg a
                LEFT JOIN gc_by_code g_code
                  ON UPPER(TRIM(a.end_country_code)) = g_code.country_code_key
                 AND UPPER(TRIM(a.year)) = g_code.year_key
                LEFT JOIN gc_by_name g_name
                  ON REPLACE(UPPER(TRIM(a.country_raw)), ' ', '') = g_name.country_name_key
                 AND UPPER(TRIM(a.year)) = g_name.year_key
            ),
            machine_line_mapping_matches AS (
                SELECT
                    frb.base_row_id AS base_row_id,
                    TRIM(mlm.artificial_machine_line) AS artificial_machine_line,
                    ROW_NUMBER() OVER (
                        PARTITION BY frb.base_row_id
                        ORDER BY
                            CASE
                                WHEN UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, ''))) THEN 0
                                WHEN UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, ''))) THEN 1
                                WHEN UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, ''))) THEN 2
                                WHEN UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, ''))) THEN 3
                                ELSE 4
                            END,
                            mlm.row_index ASC,
                            mlm.id ASC
                    ) AS match_rank
                FROM final_rows_base frb
                JOIN machine_line_mapping_rows mlm
                  ON mlm.upload_run_id = ?
                 AND UPPER(TRIM(COALESCE(mlm.size_class, ''))) = UPPER(TRIM(COALESCE(frb.size_class, '')))
                 AND (
                        UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, '')))
                     OR UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, '')))
                     OR UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, '')))
                     OR UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, '')))
                 )
                 AND (
                        TRIM(COALESCE(mlm.position, '')) = ''
                     OR INSTR(
                            ',' || REPLACE(UPPER(TRIM(COALESCE(mlm.position, ''))), ' ', '') || ',',
                            ',' || REPLACE(UPPER(TRIM(COALESCE(frb.source, ''))), ' ', '') || ','
                        ) > 0
                 )
            ),
            final_rows AS (
                SELECT
                    frb.year AS year,
                    frb.country_group_code AS country_group_code,
                    frb.country_code AS country_code,
                    frb.country_grouping AS country_grouping,
                    frb.country AS country,
                    frb.region AS region,
                    frb.machine_line_code AS machine_line_code,
                    frb.machine_line_name AS machine_line_name,
                    COALESCE(mlmm.artificial_machine_line, '') AS artificial_machine_line,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(frb.source, ''))) = 'SAL'
                         AND UPPER(TRIM(COALESCE(frb.size_class, ''))) = 'MINI'
                        THEN '<6T'
                        WHEN UPPER(TRIM(COALESCE(frb.source, ''))) = 'SAL'
                         AND UPPER(TRIM(COALESCE(frb.size_class, ''))) = 'MIDI'
                        THEN '6<10T'
                        ELSE frb.size_class
                    END AS size_class,
                    frb.brand_code AS brand_code,
                    frb.brand_name AS brand_name,
                    CASE
                        WHEN UPPER(TRIM(frb.source)) = 'TMA' THEN '#'
                        WHEN UPPER(TRIM(frb.source)) = 'SAL' THEN 'Y'
                        ELSE ''
                    END AS reporter_flag,
                    CASE
                        WHEN UPPER(TRIM(frb.source)) = 'SAL' THEN 'Y'
                        ELSE '#'
                    END AS vce_flag,
                    frb.pri_sec AS pri_sec,
                    frb.source AS source,
                    CASE
                        WHEN UPPER(TRIM(frb.source)) = 'SAL'
                             AND TRIM(CAST(frb.machine_line_code AS TEXT)) = '390' THEN 'Y'
                        WHEN UPPER(TRIM(frb.source)) = 'SAL'
                             AND TRIM(COALESCE(mlmm.artificial_machine_line, '')) <> ''
                             AND sm_artificial.country_name_key IS NULL THEN 'Y'
                        ELSE ''
                    END AS deletion_flag,
                    frb.raw_fid AS raw_fid
                FROM final_rows_base frb
                LEFT JOIN machine_line_mapping_matches mlmm
                  ON frb.base_row_id = mlmm.base_row_id
                 AND mlmm.match_rank = 1
                LEFT JOIN source_matrix_country_artificial_lines sm_artificial
                  ON REPLACE(UPPER(TRIM(COALESCE(frb.country, ''))), ' ', '') = sm_artificial.country_name_key
                 AND UPPER(TRIM(COALESCE(mlmm.artificial_machine_line, ''))) = sm_artificial.artificial_machine_line_key
                LEFT JOIN reporter_list_artificial_brand rl_artificial
                  ON UPPER(TRIM(COALESCE(sm_artificial.crp_source, ''))) = rl_artificial.source_code_key
                 AND UPPER(TRIM(COALESCE(mlmm.artificial_machine_line, ''))) = rl_artificial.artificial_machine_line_key
                 AND UPPER(TRIM(COALESCE(frb.brand_code, ''))) = rl_artificial.brand_code_key
            ),
            filtered_rows AS (
                SELECT *
                FROM final_rows
                WHERE UPPER(TRIM(source)) <> 'SAL'
                   OR TRIM(COALESCE(reporter_flag, '')) <> ''
            ),
            display_rows AS (
                SELECT
                    year,
                    country_group_code,
                    country_code,
                    country_grouping,
                    country,
                    region,
                    machine_line_code,
                    machine_line_name,
                    artificial_machine_line,
                    size_class,
                    brand_code,
                    brand_name,
                    reporter_flag,
                    vce_flag,
                    pri_sec,
                    source,
                    deletion_flag,
                    SUM(COALESCE(raw_fid, 0)) AS raw_fid
                FROM filtered_rows
                GROUP BY
                    year,
                    country_group_code,
                    country_code,
                    country_grouping,
                    country,
                    region,
                    machine_line_code,
                    machine_line_name,
                    artificial_machine_line,
                    size_class,
                    brand_code,
                    brand_name,
                    reporter_flag,
                    vce_flag,
                    pri_sec,
                    source,
                    deletion_flag
            ),
            row_stats AS (
                SELECT
                    year,
                    country_group_code,
                    country_grouping,
                    country,
                    region,
                    machine_line_code,
                    machine_line_name,
                    MAX(artificial_machine_line) AS artificial_machine_line,
                    size_class,
                    SUM(
                        CASE
                            WHEN UPPER(TRIM(source)) = 'TMA' THEN COALESCE(raw_fid, 0)
                            ELSE 0
                        END
                    ) AS tm_fid,
                    SUM(
                        CASE
                            WHEN UPPER(TRIM(source)) = 'SAL'
                                 AND UPPER(TRIM(COALESCE(reporter_flag, ''))) = 'Y'
                                 AND UPPER(TRIM(COALESCE(deletion_flag, ''))) <> 'Y'
                            THEN COALESCE(raw_fid, 0)
                            ELSE 0
                        END
                    ) AS vce_fid
                FROM display_rows
                GROUP BY
                    year,
                    country_group_code,
                    country_grouping,
                    country,
                    region,
                    machine_line_code,
                    machine_line_name,
                    size_class
            ),
            detail_rows AS (
                SELECT
                    fr.year AS year,
                    fr.country_group_code AS country_group_code,
                    fr.country_grouping AS country_grouping,
                    fr.country AS country,
                    fr.region AS region,
                    fr.machine_line_code AS machine_line_code,
                    fr.machine_line_name AS machine_line_name,
                    fr.artificial_machine_line AS artificial_machine_line,
                    fr.size_class AS size_class,
                    fr.brand_code AS brand_code,
                    fr.brand_name AS brand_name,
                    fr.reporter_flag AS reporter_flag,
                    fr.vce_flag AS vce_flag,
                    fr.source AS source,
                    fr.pri_sec AS pri_sec,
                    'A10' AS calculation_step,
                    CASE
                        WHEN UPPER(TRIM(fr.source)) = 'SAL' THEN COALESCE(fr.raw_fid, 0)
                        ELSE 0
                    END AS fid,
                    CASE
                        WHEN UPPER(TRIM(fr.source)) = 'TMA' THEN COALESCE(fr.raw_fid, 0)
                        ELSE 0
                    END AS tm_fid,
                    CASE
                        WHEN UPPER(TRIM(fr.source)) = 'TMA' THEN
                            CASE
                                WHEN COALESCE(rs.tm_fid, 0) - COALESCE(rs.vce_fid, 0) > 0 THEN COALESCE(rs.tm_fid, 0) - COALESCE(rs.vce_fid, 0)
                                ELSE 0
                            END
                        ELSE 0
                    END AS tm_non_vce,
                    CASE
                        WHEN UPPER(TRIM(fr.source)) = 'SAL' THEN 1
                        WHEN UPPER(TRIM(fr.source)) = 'TMA' THEN 2
                        ELSE 9
                    END AS sort_order
                FROM display_rows fr
                LEFT JOIN row_stats rs
                  ON fr.year = rs.year
                 AND fr.country_group_code = rs.country_group_code
                 AND fr.country_grouping = rs.country_grouping
                 AND fr.country = rs.country
                 AND fr.region = rs.region
                 AND fr.machine_line_code = rs.machine_line_code
                 AND fr.machine_line_name = rs.machine_line_name
                 AND fr.size_class = rs.size_class
            ),
            result_rows AS (
                SELECT
                    rs.year AS year,
                    rs.country_group_code AS country_group_code,
                    rs.country_grouping AS country_grouping,
                    rs.country AS country,
                    rs.region AS region,
                    rs.machine_line_code AS machine_line_code,
                    rs.machine_line_name AS machine_line_name,
                    rs.artificial_machine_line AS artificial_machine_line,
                    rs.size_class AS size_class,
                    'Result' AS brand_code,
                    'Result' AS brand_name,
                    '' AS reporter_flag,
                    '' AS vce_flag,
                    '' AS source,
                    '' AS pri_sec,
                    'A10' AS calculation_step,
                    COALESCE(rs.vce_fid, 0) AS fid,
                    COALESCE(rs.tm_fid, 0) AS tm_fid,
                    CASE
                        WHEN COALESCE(rs.tm_fid, 0) - COALESCE(rs.vce_fid, 0) > 0 THEN COALESCE(rs.tm_fid, 0) - COALESCE(rs.vce_fid, 0)
                        ELSE 0
                    END AS tm_non_vce,
                    3 AS sort_order
                FROM row_stats rs
            )
            SELECT
                year,
                country_group_code,
                country_grouping,
                country,
                region,
                machine_line_code,
                machine_line_name,
                artificial_machine_line,
                size_class,
                brand_code,
                brand_name,
                reporter_flag,
                vce_flag,
                source,
                pri_sec,
                calculation_step,
                fid,
                tm_fid,
                tm_non_vce
            FROM (
                SELECT * FROM detail_rows
                UNION ALL
                SELECT * FROM result_rows
            )
            ORDER BY
                country_grouping,
                country_group_code,
                country,
                machine_line_code,
                machine_line_name,
                size_class,
                sort_order
        """, (
            latest_group_country_upload_run_id,
            latest_group_country_upload_run_id,
            latest_tma_upload_run_id,
            latest_volvo_upload_run_id,
            latest_source_matrix_upload_run_id,
            latest_reporter_list_upload_run_id,
            latest_machine_line_mapping_upload_run_id,
        ))

        rows = [dict(row) for row in cursor.fetchall()]
        return {
            "row_count": len(rows),
            "rows": rows,
            "tma_upload_run_id": latest_tma_upload_run_id,
            "volvo_upload_run_id": latest_volvo_upload_run_id,
            "group_country_upload_run_id": latest_group_country_upload_run_id,
            "source_matrix_upload_run_id": latest_source_matrix_upload_run_id,
            "machine_line_mapping_upload_run_id": latest_machine_line_mapping_upload_run_id,
            "reporter_list_upload_run_id": latest_reporter_list_upload_run_id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


def _save_p00_report_snapshot(
    report_key: str,
    rows: list[dict[str, Any]],
    message: str,
    meta: dict[str, Any] | None = None,
    planning_year: int | None = None,
) -> int:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO p00_report_runs (report_key, planning_year, row_count, status, message, meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            report_key,
            planning_year,
            len(rows),
            "running",
            message,
            json.dumps(meta or {}, ensure_ascii=False, default=str),
        ))
        run_id = cursor.lastrowid

        cursor.executemany("""
            INSERT INTO p00_report_rows (report_run_id, row_index, row_json)
            VALUES (?, ?, ?)
        """, [
            (run_id, index, json.dumps(row, ensure_ascii=False, default=str))
            for index, row in enumerate(rows, start=1)
        ])

        cursor.execute("""
            UPDATE p00_report_runs
            SET row_count = ?, status = ?, message = ?
            WHERE id = ?
        """, (
            len(rows),
            "success",
            message,
            run_id,
        ))
        conn.commit()
        return run_id
    except Exception as e:
        if "run_id" in locals():
            cursor.execute("""
                UPDATE p00_report_runs
                SET status = ?, message = ?
                WHERE id = ?
            """, ("failed", str(e), run_id))
            conn.commit()
        raise
    finally:
        conn.close()


def _create_p00_report_run(
    report_key: str,
    message: str,
    meta: dict[str, Any] | None = None,
    planning_year: int | None = None,
) -> int:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO p00_report_runs (report_key, planning_year, row_count, status, message, meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            report_key,
            planning_year,
            0,
            "running",
            message,
            json.dumps(meta or {}, ensure_ascii=False, default=str),
        ))
        run_id = cursor.lastrowid
        conn.commit()
        return run_id
    finally:
        conn.close()


def _get_p00_report_run(run_id: int) -> dict[str, Any]:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT *
            FROM p00_report_runs
            WHERE id = ?
        """, (run_id,))
        run = cursor.fetchone()
        if run is None:
            raise HTTPException(status_code=404, detail="P00 report run not found")
        return dict(run)
    finally:
        conn.close()


def _update_p00_report_run(
    run_id: int,
    *,
    row_count: int | None = None,
    status: str | None = None,
    message: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT *
            FROM p00_report_runs
            WHERE id = ?
        """, (run_id,))
        run = cursor.fetchone()
        if run is None:
            raise HTTPException(status_code=404, detail="P00 report run not found")
        run_dict = dict(run)

        next_row_count = row_count if row_count is not None else run_dict["row_count"]
        next_status = status if status is not None else run_dict["status"]
        next_message = message if message is not None else run_dict["message"]
        next_meta = json.dumps(
            meta if meta is not None else json.loads(run_dict.get("meta_json") or "{}"),
            ensure_ascii=False,
            default=str,
        )

        cursor.execute("""
            UPDATE p00_report_runs
            SET row_count = ?, status = ?, message = ?, meta_json = ?
            WHERE id = ?
        """, (next_row_count, next_status, next_message, next_meta, run_id))
        conn.commit()
    finally:
        conn.close()


def _save_p00_report_snapshot_to_existing_run(
    run_id: int,
    rows: list[dict[str, Any]],
    message: str,
    meta: dict[str, Any] | None = None,
) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT *
            FROM p00_report_runs
            WHERE id = ?
        """, (run_id,))
        run = cursor.fetchone()
        if run is None:
            raise HTTPException(status_code=404, detail="P00 report run not found")
        run_dict = dict(run)
        next_meta = json.dumps(
            meta if meta is not None else json.loads(run_dict.get("meta_json") or "{}"),
            ensure_ascii=False,
            default=str,
        )

        cursor.execute("""
            DELETE FROM p00_report_rows
            WHERE report_run_id = ?
        """, (run_id,))
        cursor.executemany("""
            INSERT INTO p00_report_rows (report_run_id, row_index, row_json)
            VALUES (?, ?, ?)
        """, [
            (run_id, index, json.dumps(row, ensure_ascii=False, default=str))
            for index, row in enumerate(rows, start=1)
        ])

        cursor.execute("""
            UPDATE p00_report_runs
            SET row_count = ?, status = ?, message = ?, meta_json = ?
            WHERE id = ?
        """, (
            len(rows),
            "success",
            message,
            next_meta,
            run_id,
        ))
        conn.commit()
    except Exception as e:
        cursor.execute("""
            UPDATE p00_report_runs
            SET status = ?, message = ?
            WHERE id = ?
        """, ("failed", str(e), run_id))
        conn.commit()
        raise
    finally:
        conn.close()


def _get_latest_p00_report_snapshot(
    report_key: str,
    planning_year: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if planning_year is None:
            cursor.execute("""
                SELECT *
                FROM p00_report_runs
                WHERE report_key = ? AND status = 'success'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (report_key,))
        else:
            cursor.execute("""
                SELECT *
                FROM p00_report_runs
                WHERE report_key = ? AND planning_year = ? AND status = 'success'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (report_key, planning_year))
        latest_run = cursor.fetchone()
        if latest_run is None:
            raise HTTPException(status_code=404, detail="No report runs found")

        latest_run_dict = dict(latest_run)
        cursor.execute("""
            SELECT row_json
            FROM p00_report_rows
            WHERE report_run_id = ?
            ORDER BY row_index ASC, id ASC
        """, (latest_run_dict["id"],))
        rows = [json.loads(row["row_json"]) for row in cursor.fetchall()]
        return latest_run_dict, rows
    finally:
        conn.close()


def _create_excavators_split_case_run(
    case_type: str,
    message: str,
    planning_year: int | None = None,
) -> int:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO excavators_split_case_runs (case_type, planning_year, row_count, status, message, meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            case_type,
            planning_year,
            0,
            "running",
            message,
            json.dumps({}, ensure_ascii=False, default=str),
        ))
        run_id = cursor.lastrowid
        conn.commit()
        return run_id
    finally:
        conn.close()


def _update_excavators_split_case_run(
    run_id: int,
    *,
    row_count: int | None = None,
    status: str | None = None,
    message: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT *
            FROM excavators_split_case_runs
            WHERE id = ?
        """, (run_id,))
        run = cursor.fetchone()
        if run is None:
            raise HTTPException(status_code=404, detail="Excavators split run not found")
        run_dict = dict(run)

        next_row_count = row_count if row_count is not None else run_dict["row_count"]
        next_status = status if status is not None else run_dict["status"]
        next_message = message if message is not None else run_dict["message"]
        next_meta = json.dumps(
            meta if meta is not None else json.loads(run_dict.get("meta_json") or "{}"),
            ensure_ascii=False,
            default=str,
        )

        cursor.execute("""
            UPDATE excavators_split_case_runs
            SET row_count = ?, status = ?, message = ?, meta_json = ?
            WHERE id = ?
        """, (next_row_count, next_status, next_message, next_meta, run_id))
        conn.commit()
    finally:
        conn.close()


def _save_excavators_split_case_snapshot(
    run_id: int,
    result: dict[str, Any],
    message: str,
) -> None:
    summary_rows = list(result.get("summary_rows") or [])
    detail_rows = list(result.get("detail_rows") or [])
    meta = {
        "summary": result.get("summary") or {},
        "source_row_count": result.get("source_row_count"),
        "oth_row_count": result.get("oth_row_count"),
        "p10_row_count": result.get("p10_row_count"),
        "summary_row_count": len(summary_rows),
        "detail_row_count": len(detail_rows),
        "case_type": result.get("case_type"),
        "dependency_signature": result.get("dependency_signature") or {},
    }

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DELETE FROM excavators_split_case_rows
            WHERE report_run_id = ?
        """, (run_id,))
        cursor.executemany("""
            INSERT INTO excavators_split_case_rows (report_run_id, section, row_index, row_json)
            VALUES (?, ?, ?, ?)
        """, [
            (run_id, "summary", index, json.dumps(row, ensure_ascii=False, default=str))
            for index, row in enumerate(summary_rows, start=1)
        ] + [
            (run_id, "detail", index, json.dumps(row, ensure_ascii=False, default=str))
            for index, row in enumerate(detail_rows, start=1)
        ])
        cursor.execute("""
            UPDATE excavators_split_case_runs
            SET row_count = ?, status = ?, message = ?, meta_json = ?
            WHERE id = ?
        """, (
            len(summary_rows) + len(detail_rows),
            "success",
            message,
            json.dumps(meta, ensure_ascii=False, default=str),
            run_id,
        ))
        conn.commit()
    except Exception as e:
        cursor.execute("""
            UPDATE excavators_split_case_runs
            SET status = ?, message = ?
            WHERE id = ?
        """, ("failed", str(e), run_id))
        conn.commit()
        raise
    finally:
        conn.close()


def _get_latest_excavators_split_case_snapshot(
    case_type: str,
    planning_year: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if planning_year is None:
            cursor.execute("""
                SELECT *
                FROM excavators_split_case_runs
                WHERE case_type = ? AND status = 'success'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (case_type,))
        else:
            cursor.execute("""
                SELECT *
                FROM excavators_split_case_runs
                WHERE case_type = ? AND planning_year = ? AND status = 'success'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (case_type, planning_year))
        latest_run = cursor.fetchone()
        if latest_run is None:
            raise HTTPException(status_code=404, detail=f"No saved excavators split run found for case type: {case_type}")

        latest_run_dict = dict(latest_run)
        cursor.execute("""
            SELECT section, row_json
            FROM excavators_split_case_rows
            WHERE report_run_id = ?
            ORDER BY section ASC, row_index ASC, id ASC
        """, (latest_run_dict["id"],))
        summary_rows: list[dict[str, Any]] = []
        detail_rows: list[dict[str, Any]] = []
        for row in cursor.fetchall():
            if row["section"] == "summary":
                summary_rows.append(json.loads(row["row_json"]))
            else:
                detail_rows.append(json.loads(row["row_json"]))

        return latest_run_dict, summary_rows, detail_rows
    finally:
        conn.close()


def _build_excavators_split_dependency_signature(
    case_type: str,
    oth_report: dict[str, Any],
    three_check_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "logic_version": 2,
        "case_type": case_type,
        "oth_report_run_id": oth_report.get("run_id"),
        "oth_report_created_at": oth_report.get("created_at"),
        "oth_upload_run_id": oth_report.get("oth_upload_run_id"),
        "three_check_run_id": three_check_report.get("run_id") if three_check_report else None,
        "three_check_created_at": three_check_report.get("created_at") if three_check_report else None,
        "three_check_oth_upload_run_id": three_check_report.get("oth_upload_run_id") if three_check_report else None,
        "three_check_tma_upload_run_id": three_check_report.get("tma_upload_run_id") if three_check_report else None,
        "three_check_volvo_upload_run_id": three_check_report.get("volvo_upload_run_id") if three_check_report else None,
        "three_check_source_matrix_upload_run_id": three_check_report.get("source_matrix_upload_run_id") if three_check_report else None,
        "three_check_machine_line_mapping_upload_run_id": three_check_report.get("machine_line_mapping_upload_run_id") if three_check_report else None,
    }


def _try_reuse_excavators_split_case_snapshot(
    case_type: str,
    dependency_signature: dict[str, Any],
    planning_year: int | None = None,
) -> dict[str, Any] | None:
    try:
        latest_run, summary_rows, detail_rows = _get_latest_excavators_split_case_snapshot(
            case_type,
            planning_year=planning_year,
        )
    except HTTPException as exc:
        if exc.status_code == 404:
            return None
        raise

    meta = json.loads(latest_run.get("meta_json") or "{}")
    if (meta.get("dependency_signature") or {}) != dependency_signature:
        return None

    return {
        "case_type": case_type,
        "summary_rows": summary_rows,
        "detail_rows": detail_rows,
        "summary": meta.get("summary") or {},
        "source_row_count": meta.get("source_row_count") or len(detail_rows),
        "oth_row_count": meta.get("oth_row_count") or 0,
        "p10_row_count": meta.get("p10_row_count") or 0,
        "dependency_signature": dependency_signature,
    }


def _get_excavators_split_case_run(run_id: int) -> dict[str, Any]:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT *
            FROM excavators_split_case_runs
            WHERE id = ?
        """, (run_id,))
        run = cursor.fetchone()
        if run is None:
            raise HTTPException(status_code=404, detail="Excavators split run not found")
        return dict(run)
    finally:
        conn.close()


def _get_excavators_split_detail_config(case_type: str) -> dict[str, Any] | None:
    if case_type == "CEX":
        return {
            "input_size_label": "<10T",
            "first_target_label": "<6T",
            "second_target_label": "6<10T",
            "third_target_label": None,
        }
    if case_type == "GEC":
        return {
            "input_size_label": ">6T",
            "first_target_label": ">10T",
            "second_target_label": "6<10T",
            "third_target_label": None,
        }
    if case_type == "GEW":
        return {
            "input_size_label": ">6T",
            "first_target_label": "6<11T",
            "second_target_label": ">11T",
            "third_target_label": None,
        }
    if case_type == "WLO_GT10":
        return {
            "input_size_label": ">10",
            "first_target_label": "10<12",
            "second_target_label": ">12",
            "third_target_label": None,
        }
    if case_type == "WLO_LT10":
        return {
            "input_size_label": "<10",
            "first_target_label": "7<10",
            "second_target_label": "<7",
            "third_target_label": None,
        }
    if case_type == "WLO_LT12":
        return {
            "input_size_label": "<12",
            "first_target_label": "10<12",
            "second_target_label": "7<10",
            "third_target_label": "<7",
        }
    return None


def _matches_excavators_split_oth_case(row: dict[str, Any], case_type: str) -> bool:
    if _to_case_insensitive_key(row.get("reporter_flag")) != "Y":
        return False

    artificial_machine_line_key = _to_case_insensitive_key(row.get("artificial_machine_line"))
    size_class_key = _to_size_class_key(row.get("size_class_flag"))

    if case_type == "ALL":
        return (
            (artificial_machine_line_key == "CEX" and size_class_key == "<10T")
            or (artificial_machine_line_key == "GEC" and size_class_key == ">6T")
            or (artificial_machine_line_key == "GEW" and size_class_key == ">6T")
        )
    if case_type == "CEX":
        return artificial_machine_line_key == "CEX" and size_class_key == "<10T"
    if case_type == "GEC":
        return artificial_machine_line_key == "GEC" and size_class_key == ">6T"
    if case_type == "GEW":
        return artificial_machine_line_key == "GEW" and size_class_key == ">6T"
    if case_type == "WLO_GT10":
        return artificial_machine_line_key == "WLO" and size_class_key == ">10"
    if case_type == "WLO_LT10":
        return artificial_machine_line_key == "WLO" and size_class_key == "<10"
    if case_type == "WLO_LT12":
        return artificial_machine_line_key == "WLO" and size_class_key == "<12"
    return False


def _matches_excavators_split_tma_case(row: dict[str, Any], case_type: str) -> bool:
    if _to_case_insensitive_key(row.get("source")) != "TMA":
        return False

    artificial_machine_line_key = _to_case_insensitive_key(row.get("artificial_machine_line"))
    size_class_key = _to_size_class_key(row.get("size_class"))

    if case_type == "ALL":
        return (
            (artificial_machine_line_key == "CEX" and size_class_key in {"<6T", "6<10T", "<10T"})
            or (artificial_machine_line_key == "GEC" and size_class_key == ">6T")
            or (artificial_machine_line_key == "GEW" and size_class_key == ">6T")
        )
    if case_type == "CEX":
        return artificial_machine_line_key == "CEX" and size_class_key in {"<6T", "6<10T", "<10T"}
    if case_type == "GEC":
        return artificial_machine_line_key == "GEC" and size_class_key in {"6<10T", ">10T"}
    if case_type == "GEW":
        return artificial_machine_line_key == "GEW" and size_class_key in {"6<11T", ">11T"}
    if case_type == "WLO_GT10":
        return artificial_machine_line_key == "WLO" and size_class_key in {"10<12", ">12"}
    if case_type == "WLO_LT10":
        return artificial_machine_line_key == "WLO" and size_class_key in {"7<10", "<7"}
    if case_type == "WLO_LT12":
        return artificial_machine_line_key == "WLO" and size_class_key in {"10<12", "7<10", "<7"}
    return False


def _excavators_split_reference_machine_key(row: dict[str, Any], case_type: str) -> str:
    artificial_machine_line_key = _to_case_insensitive_key(row.get("artificial_machine_line"))
    if case_type in {"GEC", "GEW", "WLO_GT10", "WLO_LT10", "WLO_LT12"}:
        return artificial_machine_line_key
    return f"{artificial_machine_line_key}|{_to_case_insensitive_key(row.get('machine_line_name'))}"


def _build_excavators_split_case_rows_from_oth(
    oth_rows: list[dict[str, Any]],
    case_type: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in oth_rows:
      if not _matches_excavators_split_oth_case(row, case_type):
            continue

      key = "|".join([
          _to_case_insensitive_key(row.get("year")),
          _to_case_insensitive_key(row.get("machine_line_name")),
          _to_case_insensitive_key(row.get("artificial_machine_line")),
          _to_case_insensitive_key(row.get("source")),
          _to_size_class_key(row.get("size_class_flag")),
      ])

      if key not in grouped:
          grouped[key] = {
              "year": _to_text(row.get("year")),
              "machine_line_name": _to_text(row.get("machine_line_name")),
              "machine_line_code": _to_text(row.get("artificial_machine_line")),
              "source": _to_text(row.get("source")),
              "size_class_flag": _to_text(row.get("size_class_flag")),
              "matched_rows": 0,
              "gross_fid": 0.0,
              "volvo_deduction": 0.0,
              "net_fid": 0.0,
          }

      current = grouped[key]
      fid = _to_number(row.get("fid"))
      current["matched_rows"] += 1
      current["gross_fid"] += fid
      if _to_case_insensitive_key(row.get("brand_name")) == "VOLVO":
          current["volvo_deduction"] += fid
      else:
          current["net_fid"] += fid

    return sorted(
        grouped.values(),
        key=lambda item: (
            item["year"],
            item["machine_line_name"],
            item["source"],
            item["size_class_flag"],
        ),
    )


def _build_excavators_split_detail_rows_from_three_check(
    three_check_rows: list[dict[str, Any]],
    case_type: str,
) -> list[dict[str, Any]]:
    detail_config = _get_excavators_split_detail_config(case_type)
    if detail_config is None or case_type == "ALL":
        return []

    detail_rows: list[dict[str, Any]] = []
    country_tm_by_group: dict[str, dict[str, Any]] = {}
    region_tm_by_group: dict[str, dict[str, Any]] = {}
    grouping_tm_by_group: dict[str, dict[str, Any]] = {}
    used_reference_groups: dict[str, dict[str, Any]] = {}

    def ensure_group(target_map: dict[str, dict[str, Any]], key: str, row: dict[str, Any]) -> dict[str, Any]:
        if key not in target_map:
            target_map[key] = {
                "year": _to_text(row.get("year")),
                "country_grouping": _to_text(row.get("country_grouping")),
                "country": _to_text(row.get("country")),
                "region": _to_text(row.get("region")),
                "machine_line": _to_text(row.get("machine_line_name")),
                "artificial_machine_line": _to_text(row.get("artificial_machine_line")),
                "first_target_tm_non_vce": 0.0,
                "second_target_tm_non_vce": 0.0,
                "third_target_tm_non_vce": 0.0,
            }
        return target_map[key]

    def get_reference_machine_key(row: dict[str, Any]) -> str:
        return _excavators_split_reference_machine_key(row, case_type)

    for row in three_check_rows:
        if not _matches_excavators_split_tma_case(row, case_type):
            continue

        reference_machine_key = get_reference_machine_key(row)
        country_key = "|".join([
            _to_case_insensitive_key(row.get("year")),
            _to_case_insensitive_key(row.get("country")),
            reference_machine_key,
        ])
        region_key = "|".join([
            _to_case_insensitive_key(row.get("year")),
            _to_case_insensitive_key(row.get("region")),
            reference_machine_key,
        ])
        grouping_key = "|".join([
            _to_case_insensitive_key(row.get("year")),
            _to_case_insensitive_key(row.get("country_grouping")),
            reference_machine_key,
        ])

        country_group = ensure_group(country_tm_by_group, country_key, row)
        region_group = ensure_group(region_tm_by_group, region_key, row)
        grouping_group = ensure_group(grouping_tm_by_group, grouping_key, row)

        size_class_key = _to_size_class_key(row.get("size_class"))
        tm_non_vce = _to_number(row.get("tm_non_vce"))
        matches_first_target = size_class_key in {
            _to_size_class_key(detail_config["first_target_label"])
        }
        matches_second_target = size_class_key in {
            _to_size_class_key(detail_config["second_target_label"])
        }
        third_target_label = detail_config.get("third_target_label")
        matches_third_target = third_target_label is not None and size_class_key == _to_size_class_key(third_target_label)

        if matches_first_target:
            country_group["first_target_tm_non_vce"] += tm_non_vce
            region_group["first_target_tm_non_vce"] += tm_non_vce
            grouping_group["first_target_tm_non_vce"] += tm_non_vce
        elif matches_second_target:
            country_group["second_target_tm_non_vce"] += tm_non_vce
            region_group["second_target_tm_non_vce"] += tm_non_vce
            grouping_group["second_target_tm_non_vce"] += tm_non_vce
        elif matches_third_target:
            country_group["third_target_tm_non_vce"] += tm_non_vce
            region_group["third_target_tm_non_vce"] += tm_non_vce
            grouping_group["third_target_tm_non_vce"] += tm_non_vce

    for row in three_check_rows:
        if _to_case_insensitive_key(row.get("source")) == "TMA":
            continue
        if not _matches_excavators_split_oth_case({
            "reporter_flag": row.get("reporter_flag"),
            "artificial_machine_line": row.get("artificial_machine_line"),
            "size_class_flag": row.get("size_class"),
        }, case_type):
            continue

        reference_machine_key = get_reference_machine_key(row)
        country_key = "|".join([
            _to_case_insensitive_key(row.get("year")),
            _to_case_insensitive_key(row.get("country")),
            reference_machine_key,
        ])
        region_key = "|".join([
            _to_case_insensitive_key(row.get("year")),
            _to_case_insensitive_key(row.get("region")),
            reference_machine_key,
        ])
        grouping_key = "|".join([
            _to_case_insensitive_key(row.get("year")),
            _to_case_insensitive_key(row.get("country_grouping")),
            reference_machine_key,
        ])

        fallback_candidates = [
            ("Country", country_key, country_tm_by_group.get(country_key)),
            ("Region", region_key, region_tm_by_group.get(region_key)),
            ("Country Grouping", grouping_key, grouping_tm_by_group.get(grouping_key)),
        ]
        matching_reference = next(
            (
                candidate
                for candidate in fallback_candidates
                if candidate[2]
                and (
                    candidate[2]["first_target_tm_non_vce"]
                    + candidate[2]["second_target_tm_non_vce"]
                    + candidate[2]["third_target_tm_non_vce"]
                    > 0
                )
            ),
            None,
        ) or next((candidate for candidate in fallback_candidates if candidate[2]), None)

        tm_group = matching_reference[2] if matching_reference else None
        reference_level = matching_reference[0] if matching_reference else ""

        before_split_fid = _to_number(row.get("fid"))
        first_target_tm = tm_group["first_target_tm_non_vce"] if tm_group else 0.0
        second_target_tm = tm_group["second_target_tm_non_vce"] if tm_group else 0.0
        third_target_tm = tm_group["third_target_tm_non_vce"] if tm_group else 0.0
        tm_total = first_target_tm + second_target_tm + third_target_tm
        after_first_target = 0.0
        after_second_target = 0.0
        after_third_target = 0.0
        split_ratio = ""

        if first_target_tm > 0 and second_target_tm <= 0 and third_target_tm <= 0:
            after_first_target = _round_to_4(before_split_fid)
            after_second_target = 0.0
            after_third_target = 0.0
            split_ratio = (
                f"{_format_percent_2(100.0)} / {_format_percent_2(0.0)} / {_format_percent_2(0.0)}"
                if third_target_label
                else f"{_format_percent_2(100.0)} / {_format_percent_2(0.0)}"
            )
        elif first_target_tm <= 0 and second_target_tm > 0 and third_target_tm <= 0:
            after_first_target = 0.0
            after_second_target = _round_to_4(before_split_fid)
            after_third_target = 0.0
            split_ratio = (
                f"{_format_percent_2(0.0)} / {_format_percent_2(100.0)} / {_format_percent_2(0.0)}"
                if third_target_label
                else f"{_format_percent_2(0.0)} / {_format_percent_2(100.0)}"
            )
        elif tm_total > 0:
            after_first_target = _round_to_4((before_split_fid * first_target_tm) / tm_total)
            after_second_target = _round_to_4((before_split_fid * second_target_tm) / tm_total)
            after_third_target = _round_to_4((before_split_fid * third_target_tm) / tm_total)
            if third_target_label:
                split_ratio = (
                    f"{_format_percent_2((first_target_tm / tm_total) * 100)} / "
                    f"{_format_percent_2((second_target_tm / tm_total) * 100)} / "
                    f"{_format_percent_2((third_target_tm / tm_total) * 100)}"
                )
            else:
                split_ratio = (
                    f"{_format_percent_2((first_target_tm / tm_total) * 100)} / "
                    f"{_format_percent_2((second_target_tm / tm_total) * 100)}"
                )

        difference = _round_to_4(before_split_fid - after_first_target - after_second_target - after_third_target)

        detail_rows.append({
            "row_type": "OTH",
            "year": _to_text(row.get("year")),
            "country_grouping": _to_text(row.get("country_grouping")),
            "country": _to_text(row.get("country")),
            "region": _to_text(row.get("region")),
            "machine_line": _to_text(row.get("machine_line_name")),
            "artificial_machine_line": _to_text(row.get("artificial_machine_line")),
            "brand_code": _to_text(row.get("brand_code")),
            "reporter_flag": _to_text(row.get("reporter_flag")),
            "source": _to_text(row.get("source")),
            "pri_sec": _to_text(row.get("pri_sec")),
            "size_class": _to_text(row.get("size_class")),
            "before_split_fid_lt_10t": _round_to_4(before_split_fid),
            "copy_fid_lt_10t": 0.0,
            "after_split_fid_lt_6t": after_first_target,
            "after_split_fid_6_10t": after_second_target,
            "after_split_fid_target_three": _round_to_4(after_third_target) if third_target_label else "",
            "tm_non_vce_lt_6t": "",
            "tm_non_vce_6_10t": "",
            "tm_non_vce_target_three": "",
            "resplit": "",
            "after_resplit_fid_lt_6t": "",
            "after_resplit_fid_6_10t": "",
            "after_resplit_fid_target_three": "",
            "before_after_difference": difference,
            "reference_level": reference_level,
            "split_ratio": split_ratio,
        })

        if tm_group and reference_level:
            used_reference_groups[f"{reference_level}|{country_key}|{region_key}|{grouping_key}"] = {
                **tm_group,
                "reference_level": reference_level,
            }

    for tm_group in used_reference_groups.values():
        detail_rows.append({
            "row_type": "TMA",
            "year": tm_group["year"],
            "country_grouping": tm_group["country_grouping"],
            "country": tm_group["country"],
            "region": tm_group["region"],
            "machine_line": tm_group["machine_line"],
            "artificial_machine_line": tm_group["artificial_machine_line"],
            "brand_code": "#",
            "reporter_flag": "#",
            "source": "TMA",
            "pri_sec": "#",
            "size_class": detail_config["input_size_label"],
            "before_split_fid_lt_10t": "",
            "copy_fid_lt_10t": "",
            "after_split_fid_lt_6t": "",
            "after_split_fid_6_10t": "",
            "after_split_fid_target_three": "",
            "tm_non_vce_lt_6t": _round_to_4(tm_group["first_target_tm_non_vce"]),
            "tm_non_vce_6_10t": _round_to_4(tm_group["second_target_tm_non_vce"]),
            "tm_non_vce_target_three": (
                _round_to_4(tm_group["third_target_tm_non_vce"]) if detail_config.get("third_target_label") else ""
            ),
            "resplit": "",
            "after_resplit_fid_lt_6t": "",
            "after_resplit_fid_6_10t": "",
            "after_resplit_fid_target_three": "",
            "before_after_difference": "",
            "reference_level": tm_group["reference_level"],
            "split_ratio": detail_config["third_target_label"]
                and (
                    f"{_round_to_4((tm_group['first_target_tm_non_vce'] / (tm_group['first_target_tm_non_vce'] + tm_group['second_target_tm_non_vce'] + tm_group['third_target_tm_non_vce'])) * 100)}% / "
                    f"{_round_to_4((tm_group['second_target_tm_non_vce'] / (tm_group['first_target_tm_non_vce'] + tm_group['second_target_tm_non_vce'] + tm_group['third_target_tm_non_vce'])) * 100)}% / "
                    f"{_round_to_4((tm_group['third_target_tm_non_vce'] / (tm_group['first_target_tm_non_vce'] + tm_group['second_target_tm_non_vce'] + tm_group['third_target_tm_non_vce'])) * 100)}%"
                )
                or (
                    f"{_round_to_4((tm_group['first_target_tm_non_vce'] / (tm_group['first_target_tm_non_vce'] + tm_group['second_target_tm_non_vce'] + tm_group['third_target_tm_non_vce'])) * 100)}% / "
                    f"{_round_to_4((tm_group['second_target_tm_non_vce'] / (tm_group['first_target_tm_non_vce'] + tm_group['second_target_tm_non_vce'] + tm_group['third_target_tm_non_vce'])) * 100)}%"
                ),
        })

    detail_rows.sort(
        key=lambda item: (
            item["year"],
            item["country_grouping"],
            item["country"],
            item["row_type"],
            item["source"],
            item["brand_code"],
        )
    )
    return detail_rows


def _build_excavators_split_case_report(case_type: str, planning_year: int | None = None):
    normalized_case_type = case_type.strip().upper()
    if normalized_case_type == "CEX":
        return _build_cex_split_case_report(planning_year)

    try:
        oth = get_latest_oth_deletion_flag_report(planning_year=planning_year)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        oth = get_oth_deletion_flag_report(track_run=False, planning_year=planning_year)

    summary_rows = _build_excavators_split_case_rows_from_oth(oth["rows"], normalized_case_type)
    detail_rows = []
    three_check = None
    dependency_signature = _build_excavators_split_dependency_signature(normalized_case_type, oth)

    # Fast path: if there is no matching OTH input for this case, return immediately.
    if normalized_case_type != "ALL" and len(summary_rows) == 0:
        return {
            "case_type": normalized_case_type,
            "summary_rows": [],
            "detail_rows": [],
            "summary": {
                "grouped_rows": 0,
                "matched_rows": 0,
                "gross_fid_total": 0.0,
                "volvo_deduction_total": 0.0,
                "net_fid_total": 0.0,
            },
            "source_row_count": 0,
            "oth_row_count": oth["row_count"],
            "p10_row_count": 0,
            "dependency_signature": dependency_signature,
        }

    if normalized_case_type != "ALL":
        try:
            three_check = get_latest_p00_three_check_report(planning_year=planning_year)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            three_check = get_p00_three_check_report(track_run=False, planning_year=planning_year)
        dependency_signature = _build_excavators_split_dependency_signature(normalized_case_type, oth, three_check)

        reusable_result = _try_reuse_excavators_split_case_snapshot(
            normalized_case_type,
            dependency_signature,
            planning_year=planning_year,
        )
        if reusable_result is not None:
            return reusable_result
        detail_rows = _build_excavators_split_detail_rows_from_three_check(three_check["rows"], normalized_case_type)
    else:
        reusable_result = _try_reuse_excavators_split_case_snapshot(
            normalized_case_type,
            dependency_signature,
            planning_year=planning_year,
        )
        if reusable_result is not None:
            return reusable_result

    grouped_rows = len(summary_rows)
    matched_rows = sum(int(item["matched_rows"]) for item in summary_rows)
    gross_fid_total = sum(float(item["gross_fid"]) for item in summary_rows)
    volvo_deduction_total = sum(float(item["volvo_deduction"]) for item in summary_rows)
    net_fid_total = sum(float(item["net_fid"]) for item in summary_rows)

    return {
        "case_type": normalized_case_type,
        "summary_rows": summary_rows,
        "detail_rows": detail_rows,
        "summary": {
            "grouped_rows": grouped_rows,
            "matched_rows": matched_rows,
            "gross_fid_total": gross_fid_total,
            "volvo_deduction_total": volvo_deduction_total,
            "net_fid_total": net_fid_total,
        },
        "source_row_count": len(detail_rows),
        "oth_row_count": oth["row_count"],
        "p10_row_count": three_check["row_count"] if normalized_case_type != "ALL" and three_check is not None else 0,
        "dependency_signature": dependency_signature,
    }


def _run_excavators_split_case_background(run_id: int) -> None:
    try:
        run = _get_excavators_split_case_run(run_id)
        result = _build_excavators_split_case_report(
            run.get("case_type", ""),
            run.get("planning_year"),
        )
        if result.get("case_type") != run.get("case_type"):
            raise RuntimeError("Excavators split run type mismatch")
        _save_excavators_split_case_snapshot(
            run_id,
            result,
            f"Excavators Split {run.get('case_type')} report generated successfully",
        )
    except Exception as e:
        try:
            _update_excavators_split_case_run(run_id, status="failed", message=str(e))
        except Exception:
            pass



@router.get("/reports/crp-d1-combined/latest")
def get_latest_crp_d1_combined_report(planning_year: int | None = Query(default=None)):
    normalized_year = _normalize_planning_year(planning_year)
    run, rows = _get_latest_p00_report_snapshot(
        P00_RUN_KEYS["crp_d1_combined"],
        planning_year=normalized_year,
    )
    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "row_count": run.get("row_count") or len(rows),
        "rows": rows,
        "run_id": run.get("id"),
        "status": run.get("status"),
        "created_at": run.get("created_at"),
        **meta,
    }


@router.get("/reports/oth-deletion-flag")
def get_oth_deletion_flag_report(
    track_run: bool = False,
    planning_year: int | None = Query(default=None),
):
    normalized_year = _normalize_planning_year(planning_year)
    conn = get_connection()
    cursor = conn.cursor()

    latest_oth_upload_run_id = _get_latest_success_upload_id(cursor, "oth_data", normalized_year)
    latest_group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country", normalized_year)
    latest_machine_line_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "machine_line_mapping", normalized_year)
    latest_brand_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "brand_mapping", normalized_year)
    latest_source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix", normalized_year)
    latest_reporter_list_upload_run_id = _get_latest_success_upload_id(cursor, "reporter_list", normalized_year)

    missing_types = []
    if latest_oth_upload_run_id is None:
        missing_types.append("oth_data")
    if latest_group_country_upload_run_id is None:
        missing_types.append("group_country")
    if latest_machine_line_mapping_upload_run_id is None:
        missing_types.append("machine_line_mapping")
    if latest_brand_mapping_upload_run_id is None:
        missing_types.append("brand_mapping")
    if latest_source_matrix_upload_run_id is None:
        missing_types.append("source_matrix")
    if latest_reporter_list_upload_run_id is None:
        missing_types.append("reporter_list")

    if missing_types:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail=f"Missing latest successful upload for: {', '.join(missing_types)}"
        )

    try:
        cursor.execute("""
            WITH source_matrix_base AS (
                SELECT
                    REPLACE(UPPER(TRIM(country_name)), ' ', '') AS country_name_key,
                    UPPER(TRIM(artificial_machine_line)) AS artificial_machine_line_key,
                    UPPER(TRIM(primary_source)) AS primary_source_key,
                    UPPER(TRIM(secondary_source)) AS secondary_source_key
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(country_name, '')) <> ''
                  AND TRIM(COALESCE(artificial_machine_line, '')) <> ''
                  AND TRIM(COALESCE(crp_source, '')) <> ''
            ),
            source_matrix_keys AS (
                SELECT
                    country_name_key,
                    artificial_machine_line_key
                FROM source_matrix_base
                GROUP BY
                    country_name_key,
                    artificial_machine_line_key
            ),
            source_matrix_source_flags AS (
                SELECT
                    country_name_key,
                    artificial_machine_line_key,
                    primary_source_key AS source_key,
                    'P' AS pri_sec
                FROM source_matrix_base
                WHERE TRIM(COALESCE(primary_source_key, '')) <> ''
                UNION ALL
                SELECT
                    country_name_key,
                    artificial_machine_line_key,
                    secondary_source_key AS source_key,
                    'S' AS pri_sec
                FROM source_matrix_base
                WHERE TRIM(COALESCE(secondary_source_key, '')) <> ''
            ),
            source_matrix_source_flags_dedup AS (
                SELECT
                    country_name_key,
                    artificial_machine_line_key,
                    source_key,
                    CASE
                        WHEN SUM(CASE WHEN pri_sec = 'P' THEN 1 ELSE 0 END) > 0 THEN 'P'
                        WHEN SUM(CASE WHEN pri_sec = 'S' THEN 1 ELSE 0 END) > 0 THEN 'S'
                        ELSE ''
                    END AS pri_sec
                FROM source_matrix_source_flags
                GROUP BY
                    country_name_key,
                    artificial_machine_line_key,
                    source_key
            ),
            brand_mapping_dedup AS (
                SELECT
                    UPPER(TRIM(COALESCE(brand_name, ''))) AS brand_name_key,
                    TRIM(brand_name) AS brand_name,
                    TRIM(brand_code) AS brand_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(COALESCE(brand_name, '')))
                        ORDER BY row_index ASC, id ASC
                    ) AS match_rank
                FROM brand_mapping_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(brand_name, '')) <> ''
            ),
            oth_base AS (
                SELECT
                    o.row_index AS oth_row_index,
                    o.year AS year,
                    o.source AS source,
                    o.country AS country_code,
                    COALESCE(g.country_name, o.country) AS country,
                    g.country_grouping AS country_grouping,
                    g.region AS region,
                    g.market_area AS market_area,
                    o.machine_line AS raw_machine_line_name,
                    o.size_class AS size_class_flag,
                    COALESCE(b.brand_name, o.brand_name) AS brand_name,
                    b.brand_code AS brand_code,
                    o.quantity AS fid
                FROM oth_data_rows o
                LEFT JOIN group_country_rows g
                    ON UPPER(TRIM(o.country)) = UPPER(TRIM(g.country_code))
                   AND UPPER(TRIM(o.year)) = UPPER(TRIM(g.year))
                   AND g.upload_run_id = ?
                LEFT JOIN brand_mapping_dedup b
                    ON UPPER(TRIM(o.brand_name)) = b.brand_name_key
                   AND b.match_rank = 1
                WHERE o.upload_run_id = ?
            ),
            machine_line_code_matches AS (
                SELECT
                    ob.oth_row_index AS oth_row_index,
                    TRIM(mlm.machine_line_name) AS machine_line_name,
                    TRIM(mlm.machine_line_code) AS machine_line_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY ob.oth_row_index
                        ORDER BY
                            CASE
                                WHEN UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, ''))) THEN 0
                                WHEN UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, ''))) THEN 1
                                ELSE 2
                            END,
                            mlm.row_index ASC,
                            mlm.id ASC
                    ) AS match_rank
                FROM oth_base ob
                JOIN machine_line_mapping_rows mlm
                  ON mlm.upload_run_id = ?
                 AND (
                        UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, '')))
                     OR UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, '')))
                 )
            ),
            artificial_machine_line_matches AS (
                SELECT
                    ob.oth_row_index AS oth_row_index,
                    TRIM(COALESCE(mlm.artificial_machine_line, '')) AS artificial_machine_line,
                    ROW_NUMBER() OVER (
                        PARTITION BY ob.oth_row_index
                        ORDER BY
                            CASE
                                WHEN UPPER(TRIM(COALESCE(ob.size_class_flag, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(ob.size_class_flag, ''))) = UPPER(TRIM(COALESCE(mlm.size_class, '')))
                                 AND (
                                        TRIM(COALESCE(mlm.position, '')) = ''
                                     OR INSTR(
                                            ',' || REPLACE(UPPER(TRIM(COALESCE(mlm.position, ''))), ' ', '') || ',',
                                            ',OTH,'
                                        ) > 0
                                 ) THEN 0
                                WHEN UPPER(TRIM(COALESCE(ob.size_class_flag, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(ob.size_class_flag, ''))) = UPPER(TRIM(COALESCE(mlm.size_class, ''))) THEN 1
                                WHEN (
                                        TRIM(COALESCE(mlm.position, '')) = ''
                                     OR INSTR(
                                            ',' || REPLACE(UPPER(TRIM(COALESCE(mlm.position, ''))), ' ', '') || ',',
                                            ',OTH,'
                                        ) > 0
                                 ) THEN 2
                                ELSE 3
                            END,
                            CASE
                                WHEN UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, ''))) THEN 0
                                WHEN UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) <> ''
                                 AND UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, ''))) THEN 1
                                ELSE 2
                            END,
                            mlm.row_index ASC,
                            mlm.id ASC
                    ) AS match_rank
                FROM oth_base ob
                JOIN machine_line_mapping_rows mlm
                  ON mlm.upload_run_id = ?
                 AND (
                        UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, '')))
                     OR UPPER(TRIM(COALESCE(ob.raw_machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, '')))
                 )
            ),
            final_rows AS (
                SELECT
                    ob.year AS year,
                    ob.source AS source,
                    ob.country_code AS country_code,
                    ob.country AS country,
                    ob.country_grouping AS country_grouping,
                    ob.region AS region,
                    ob.market_area AS market_area,
                    COALESCE(mlcm.machine_line_name, ob.raw_machine_line_name) AS machine_line_name,
                    COALESCE(mlcm.machine_line_code, '') AS machine_line_code,
                    COALESCE(amlm.artificial_machine_line, '') AS artificial_machine_line,
                    ob.brand_name AS brand_name,
                    ob.brand_code AS brand_code,
                    ob.size_class_flag AS size_class_flag,
                    ob.fid AS fid
                FROM oth_base ob
                LEFT JOIN machine_line_code_matches mlcm
                  ON ob.oth_row_index = mlcm.oth_row_index
                 AND mlcm.match_rank = 1
                LEFT JOIN artificial_machine_line_matches amlm
                  ON ob.oth_row_index = amlm.oth_row_index
                 AND amlm.match_rank = 1
            )
            SELECT
                year,
                source,
                country_code,
                country,
                country_grouping,
                region,
                market_area,
                machine_line_name,
                machine_line_code,
                artificial_machine_line,
                brand_name,
                brand_code,
                size_class_flag,
                SUM(COALESCE(CAST(NULLIF(REPLACE(TRIM(CAST(fid AS TEXT)), ',', ''), '') AS DOUBLE PRECISION), 0)) AS fid,
                NULL AS ms_percent,
                deletion_flag,
                pri_sec,
                reporter_flag
            FROM (
                SELECT
                    fr.year AS year,
                    fr.source AS source,
                    fr.country_code AS country_code,
                    fr.country AS country,
                    fr.country_grouping AS country_grouping,
                    fr.region AS region,
                    fr.market_area AS market_area,
                    fr.machine_line_name AS machine_line_name,
                    fr.machine_line_code AS machine_line_code,
                    fr.artificial_machine_line AS artificial_machine_line,
                    fr.brand_name AS brand_name,
                    fr.brand_code AS brand_code,
                    fr.size_class_flag AS size_class_flag,
                    fr.fid AS fid,
                    CASE
                        WHEN TRIM(COALESCE(fr.machine_line_code, '')) = '390' THEN 'Y'
                        WHEN TRIM(COALESCE(fr.country, '')) <> ''
                         AND TRIM(COALESCE(fr.artificial_machine_line, '')) <> ''
                         AND smk.country_name_key IS NULL THEN 'Y'
                        ELSE ''
                    END AS deletion_flag,
                    COALESCE(smsf.pri_sec, '') AS pri_sec,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM source_matrix_rows sm
                            JOIN reporter_list_rows rl
                              ON UPPER(TRIM(COALESCE(rl.source_code, ''))) = UPPER(TRIM(COALESCE(sm.crp_source, '')))
                             AND UPPER(TRIM(COALESCE(rl.artificial_machine_line, ''))) = UPPER(TRIM(COALESCE(fr.artificial_machine_line, '')))
                             AND UPPER(TRIM(COALESCE(rl.brand_code, ''))) = UPPER(TRIM(COALESCE(fr.brand_code, '')))
                             AND rl.upload_run_id = ?
                            WHERE sm.upload_run_id = ?
                              AND REPLACE(UPPER(TRIM(COALESCE(sm.country_name, ''))), ' ', '') = REPLACE(UPPER(TRIM(COALESCE(fr.country, ''))), ' ', '')
                              AND UPPER(TRIM(COALESCE(sm.artificial_machine_line, ''))) = UPPER(TRIM(COALESCE(fr.artificial_machine_line, '')))
                              AND TRIM(COALESCE(sm.crp_source, '')) <> ''
                        ) THEN 'Y'
                        ELSE ''
                    END AS reporter_flag
                FROM final_rows fr
                LEFT JOIN source_matrix_keys smk
                    ON REPLACE(UPPER(TRIM(COALESCE(fr.country, ''))), ' ', '') = smk.country_name_key
                   AND UPPER(TRIM(COALESCE(fr.artificial_machine_line, ''))) = smk.artificial_machine_line_key
                LEFT JOIN source_matrix_source_flags_dedup smsf
                    ON REPLACE(UPPER(TRIM(COALESCE(fr.country, ''))), ' ', '') = smsf.country_name_key
                   AND UPPER(TRIM(COALESCE(fr.artificial_machine_line, ''))) = smsf.artificial_machine_line_key
                   AND UPPER(TRIM(COALESCE(fr.source, ''))) = smsf.source_key
            )
            GROUP BY
                year,
                source,
                country_code,
                country,
                country_grouping,
                region,
                market_area,
                machine_line_name,
                machine_line_code,
                artificial_machine_line,
                brand_name,
                brand_code,
                size_class_flag,
                deletion_flag,
                pri_sec,
                reporter_flag
            ORDER BY year ASC, country_code ASC, machine_line_name ASC, brand_name ASC, size_class_flag ASC
        """, (
            latest_source_matrix_upload_run_id,
            latest_brand_mapping_upload_run_id,
            latest_group_country_upload_run_id,
            latest_oth_upload_run_id,
            latest_machine_line_mapping_upload_run_id,
            latest_machine_line_mapping_upload_run_id,
            latest_reporter_list_upload_run_id,
            latest_source_matrix_upload_run_id,
        ))

        rows = [dict(row) for row in cursor.fetchall()]
        result = {
            "row_count": len(rows),
            "rows": rows,
            "oth_upload_run_id": latest_oth_upload_run_id,
            "group_country_upload_run_id": latest_group_country_upload_run_id,
            "machine_line_mapping_upload_run_id": latest_machine_line_mapping_upload_run_id,
            "brand_mapping_upload_run_id": latest_brand_mapping_upload_run_id,
            "source_matrix_upload_run_id": latest_source_matrix_upload_run_id,
            "reporter_list_upload_run_id": latest_reporter_list_upload_run_id,
        }
        if track_run:
            _record_report_run(P00_RUN_KEYS["oth_deletion_flag"])
            _save_p00_report_snapshot(
                P00_RUN_KEYS["oth_deletion_flag"],
                result["rows"],
                "OTH Deletion Flag report generated successfully",
                meta={
                    "planning_year": normalized_year,
                    "oth_upload_run_id": result["oth_upload_run_id"],
                    "group_country_upload_run_id": result["group_country_upload_run_id"],
                    "machine_line_mapping_upload_run_id": result["machine_line_mapping_upload_run_id"],
                    "brand_mapping_upload_run_id": result["brand_mapping_upload_run_id"],
                    "source_matrix_upload_run_id": result["source_matrix_upload_run_id"],
                    "reporter_list_upload_run_id": result["reporter_list_upload_run_id"],
                },
                planning_year=normalized_year,
            )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/reports/oth-deletion-flag/latest")
def get_latest_oth_deletion_flag_report(planning_year: int | None = Query(default=None)):
    normalized_year = _normalize_planning_year(planning_year)
    run, rows = _get_latest_p00_report_snapshot(
        P00_RUN_KEYS["oth_deletion_flag"],
        planning_year=normalized_year,
    )
    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "row_count": run.get("row_count") or len(rows),
        "rows": rows,
        "run_id": run.get("id"),
        "status": run.get("status"),
        "created_at": run.get("created_at"),
        **meta,
    }


def _build_total_market_split_input_key_from_oth_row(row: dict[str, Any]) -> str:
    return "|".join([
        _to_case_insensitive_key(row.get("year")),
        _to_case_insensitive_key(row.get("country_grouping")),
        _to_case_insensitive_key(row.get("country")),
        _to_case_insensitive_key(row.get("region")),
        _to_case_insensitive_key(row.get("machine_line_name")),
        _to_case_insensitive_key(row.get("artificial_machine_line")),
        _to_case_insensitive_key(row.get("brand_code")),
        _to_case_insensitive_key(row.get("source")),
        _to_case_insensitive_key(row.get("pri_sec")),
        _to_size_class_key(row.get("size_class_flag")),
        _to_case_insensitive_key(row.get("reporter_flag")),
    ])


def _build_total_market_split_input_key_from_detail_row(row: dict[str, Any]) -> str:
    return "|".join([
        _to_case_insensitive_key(row.get("year")),
        _to_case_insensitive_key(row.get("country_grouping")),
        _to_case_insensitive_key(row.get("country")),
        _to_case_insensitive_key(row.get("region")),
        _to_case_insensitive_key(row.get("machine_line")),
        _to_case_insensitive_key(row.get("artificial_machine_line")),
        _to_case_insensitive_key(row.get("brand_code")),
        _to_case_insensitive_key(row.get("source")),
        _to_case_insensitive_key(row.get("pri_sec")),
        _to_size_class_key(row.get("size_class")),
        _to_case_insensitive_key(row.get("reporter_flag")),
    ])

def _build_total_market_split_relaxed_key_from_oth_row(row: dict[str, Any]) -> str:
    return "|".join([
        _to_case_insensitive_key(row.get("year")),
        _to_case_insensitive_key(row.get("country")),
        _to_case_insensitive_key(row.get("artificial_machine_line")),
        _to_case_insensitive_key(row.get("brand_code")),
        _to_case_insensitive_key(row.get("source")),
        _to_case_insensitive_key(row.get("pri_sec")),
        _to_size_class_key(row.get("size_class_flag")),
        _to_case_insensitive_key(row.get("reporter_flag")),
    ])


def _build_total_market_split_relaxed_key_from_detail_row(row: dict[str, Any]) -> str:
    return "|".join([
        _to_case_insensitive_key(row.get("year")),
        _to_case_insensitive_key(row.get("country")),
        _to_case_insensitive_key(row.get("artificial_machine_line")),
        _to_case_insensitive_key(row.get("brand_code")),
        _to_case_insensitive_key(row.get("source")),
        _to_case_insensitive_key(row.get("pri_sec")),
        _to_size_class_key(row.get("size_class")),
        _to_case_insensitive_key(row.get("reporter_flag")),
    ])


def _is_volvo_oth_row(row: dict[str, Any]) -> bool:
    brand_code_key = _to_case_insensitive_key(row.get("brand_code"))
    brand_name_key = _to_case_insensitive_key(row.get("brand_name"))
    return (
        brand_code_key in {"VCE", "VOL", "VOLVO"}
        or "VOLVO" in brand_name_key
    )


def _load_total_market_split_detail_rows(
    case_type: str,
    three_check_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    try:
        _, _, detail_rows = _get_latest_excavators_split_case_snapshot(case_type)
        if detail_rows:
            return detail_rows
    except HTTPException as exc:
        if exc.status_code == 404:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Missing saved split/resplit snapshot for case '{case_type}'. "
                    "Please finish re-split and save the case first."
                ),
            )
        raise
    # Strict mode: Total Market must be built from saved split/resplit snapshots only.
    # No fallback to raw split recomputation.
    return detail_rows


def _pick_total_market_split_value(
    detail_row: dict[str, Any],
    resplit_field: str,
) -> float:
    resplit_text = _to_text(detail_row.get(resplit_field))
    if resplit_text == "":
        raise HTTPException(
            status_code=400,
            detail=(
                "Total Market is configured to require re-split values, but one or more rows are missing "
                f"'{resplit_field}'. Please complete and save re-split results first."
            ),
        )
    return _to_number(detail_row.get(resplit_field))


def _expand_total_market_rows_with_split(
    oth_rows: list[dict[str, Any]],
    three_check_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    split_cases = ["CEX", "GEC", "GEW", "WLO_GT10", "WLO_LT10", "WLO_LT12"]
    split_plans_by_key: dict[str, list[dict[str, Any]]] = {}
    split_plans_by_relaxed_key: dict[str, list[dict[str, Any]]] = {}

    for case_type in split_cases:
        detail_config = _get_excavators_split_detail_config(case_type)
        if detail_config is None:
            continue

        detail_rows = _load_total_market_split_detail_rows(case_type, three_check_rows)
        for detail_row in detail_rows:
            if _to_case_insensitive_key(detail_row.get("row_type")) != "OTH":
                continue

            key = _build_total_market_split_input_key_from_detail_row(detail_row)
            split_targets: list[tuple[str, float]] = [
                (
                    detail_config["first_target_label"],
                    _pick_total_market_split_value(
                        detail_row,
                        "after_resplit_fid_lt_6t",
                    ),
                ),
                (
                    detail_config["second_target_label"],
                    _pick_total_market_split_value(
                        detail_row,
                        "after_resplit_fid_6_10t",
                    ),
                ),
            ]
            third_target_label = detail_config.get("third_target_label")
            if third_target_label:
                split_targets.append(
                    (
                        third_target_label,
                        _pick_total_market_split_value(
                            detail_row,
                            "after_resplit_fid_target_three",
                        ),
                    )
                )

            outputs: list[dict[str, Any]] = [
                {"size_class_flag": size_label, "fid": _round_to_4(fid)}
                for size_label, fid in split_targets
                if _round_to_4(fid) > 0
            ]
            if len(outputs) == 0:
                continue

            if key not in split_plans_by_key:
                split_plans_by_key[key] = []
            plan_entry = {
                "outputs": outputs,
                "before_split_fid": _round_to_4(_to_number(detail_row.get("before_split_fid_lt_10t"))),
            }
            split_plans_by_key[key].append(plan_entry)

            relaxed_key = _build_total_market_split_relaxed_key_from_detail_row(detail_row)
            if relaxed_key not in split_plans_by_relaxed_key:
                split_plans_by_relaxed_key[relaxed_key] = []
            split_plans_by_relaxed_key[relaxed_key].append(plan_entry)

    expanded_rows: list[dict[str, Any]] = []
    split_input_rows = 0
    split_output_rows = 0

    for row in oth_rows:
        key = _build_total_market_split_input_key_from_oth_row(row)
        plan_queue = split_plans_by_key.get(key)
        queue_source = "strict"

        if not plan_queue:
            relaxed_key = _build_total_market_split_relaxed_key_from_oth_row(row)
            plan_queue = split_plans_by_relaxed_key.get(relaxed_key)
            queue_source = "relaxed"

        if not plan_queue:
            expanded_rows.append(row)
            continue

        plan = plan_queue.pop(0)
        if len(plan_queue) == 0:
            if queue_source == "strict":
                split_plans_by_key.pop(key, None)
            else:
                relaxed_key = _build_total_market_split_relaxed_key_from_oth_row(row)
                split_plans_by_relaxed_key.pop(relaxed_key, None)

        outputs = list(plan.get("outputs", []))
        if len(outputs) == 0:
            expanded_rows.append(row)
            continue

        # Keep total FID invariant when split values were rounded.
        original_fid = _round_to_4(_to_number(row.get("fid")))
        split_total = _round_to_4(sum(_to_number(item.get("fid")) for item in outputs))
        delta = _round_to_4(original_fid - split_total)
        if abs(delta) >= 0.0001:
            outputs[-1]["fid"] = _round_to_4(_to_number(outputs[-1].get("fid")) + delta)

        split_input_rows += 1
        split_output_rows += len(outputs)
        for item in outputs:
            expanded_rows.append({
                **row,
                "size_class_flag": item["size_class_flag"],
                "fid": item["fid"],
            })

    return expanded_rows, {
        "split_input_rows": split_input_rows,
        "split_output_rows": split_output_rows,
    }


def _map_crp_combined_row_to_total_market_row(row: dict[str, Any]) -> dict[str, Any]:
    source_text = _to_text(row.get("source"))
    source_key = _to_case_insensitive_key(source_text)
    brand_code = _to_text(row.get("brand_code"))
    if source_key == "SAL" and not brand_code:
        brand_code = "VCE"
    if source_key == "SAL":
        brand_name = _to_text(row.get("brand_name")) or _volvo_sale_brand_name_from_code(brand_code)
    elif source_key == "TMA":
        brand_name = "TOTAL MARKET"
    else:
        brand_name = ""

    return {
        "year": _to_text(row.get("year")),
        "source": source_text,
        "country_code": _to_text(row.get("country_code")) or _to_text(row.get("country_group_code")),
        "country": _to_text(row.get("country")),
        "country_grouping": _to_text(row.get("country_grouping")),
        "region": _to_text(row.get("region")),
        "market_area": "",
        "machine_line_name": _to_text(row.get("machine_line_name")),
        "machine_line_code": _to_text(row.get("machine_line_code")),
        "artificial_machine_line": _to_text(row.get("artificial_machine_line")),
        "brand_name": brand_name,
        "brand_code": brand_code,
        "size_class_flag": _to_text(row.get("size_class")),
        "fid": _round_to_4(_to_number(row.get("fid"))),
        "ms_percent": "",
        "deletion_flag": _to_text(row.get("deletion_flag")),
        "pri_sec": _to_text(row.get("pri_sec")),
        "reporter_flag": _to_text(row.get("reporter_flag")),
        "source_flag": source_key,
    }


def _compute_total_market_calculation_eligible_oth_result(
    planning_year: int | None = None,
) -> dict[str, Any]:
    split_machine_lines = {"CEX", "GEC", "GEW", "WLO"}

    try:
        source_report = get_latest_oth_deletion_flag_report(planning_year=planning_year)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        source_report = get_oth_deletion_flag_report(track_run=False, planning_year=planning_year)

    try:
        three_check_report = get_latest_p00_three_check_report(planning_year=planning_year)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        three_check_report = get_p00_three_check_report(track_run=False, planning_year=planning_year)

    try:
        combined_report = get_latest_crp_d1_combined_report(planning_year=planning_year)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        combined_report = _get_crp_d1_combined_report_data(
            include_all_sal=True,
            planning_year=planning_year,
        )

    expanded_oth_rows, split_meta = _expand_total_market_rows_with_split(
        source_report["rows"],
        three_check_report["rows"],
    )

    oth_rows: list[dict[str, Any]] = []
    for row in expanded_oth_rows:
        if _is_volvo_oth_row(row):
            continue
        if _to_case_insensitive_key(row.get("reporter_flag")) != "Y":
            continue
        if _to_case_insensitive_key(row.get("deletion_flag")) == "Y":
            continue
        oth_rows.append({
            **row,
            "source_flag": _to_case_insensitive_key(row.get("source")),
        })

    combined_rows: list[dict[str, Any]] = []
    for row in combined_report["rows"]:
        source_key = _to_case_insensitive_key(row.get("source"))
        reporter_key = _to_case_insensitive_key(row.get("reporter_flag"))
        deletion_key = _to_case_insensitive_key(row.get("deletion_flag"))
        if deletion_key == "Y":
            continue
        if source_key == "TMA":
            combined_rows.append(_map_crp_combined_row_to_total_market_row(row))
            continue
        if source_key == "SAL" and reporter_key == "Y":
            combined_rows.append(_map_crp_combined_row_to_total_market_row(row))

    final_rows = [*combined_rows, *oth_rows]

    def _source_sort_rank(source_value: Any) -> int:
        source_key = _to_case_insensitive_key(source_value)
        if source_key == "SAL":
            return 0
        if source_key == "TMA":
            return 1
        if source_key == "OTH":
            return 2
        return 9

    # Keep SAL/TMA adjacent for the same business key, then show OTH rows.
    final_rows.sort(
        key=lambda row: (
            _to_text(row.get("year")),
            _to_text(row.get("country_grouping")),
            _to_text(row.get("country")),
            _to_text(row.get("region")),
            _to_text(row.get("machine_line_code")),
            _to_text(row.get("machine_line_name")),
            _to_text(row.get("artificial_machine_line")),
            _to_text(row.get("size_class_flag")),
            _source_sort_rank(row.get("source")),
            _to_text(row.get("brand_code")),
            _to_text(row.get("brand_name")),
        )
    )

    return {
        "row_count": len(final_rows),
        "rows": final_rows,
        "source_row_count": len(combined_report["rows"]) + len(expanded_oth_rows),
        "split_machine_lines": sorted(split_machine_lines),
        "source_report_run_id": source_report.get("run_id"),
        "source_report_created_at": source_report.get("created_at"),
        "three_check_report_run_id": three_check_report.get("run_id"),
        "three_check_report_created_at": three_check_report.get("created_at"),
        **split_meta,
    }


def _run_total_market_calculation_eligible_oth_background(run_id: int) -> None:
    try:
        run = _get_p00_report_run(run_id)
        planning_year = _normalize_planning_year(run.get("planning_year"))
        result = _compute_total_market_calculation_eligible_oth_result(planning_year=planning_year)
        _record_report_run(P00_RUN_KEYS["total_market_eligible_oth"])
        _save_p00_report_snapshot_to_existing_run(
            run_id,
            result["rows"],
            "Total Market Calculation rows generated successfully",
            meta={
                "source_row_count": result["source_row_count"],
                "split_machine_lines": result["split_machine_lines"],
                "source_report_run_id": result["source_report_run_id"],
                "source_report_created_at": result["source_report_created_at"],
                "three_check_report_run_id": result["three_check_report_run_id"],
                "three_check_report_created_at": result["three_check_report_created_at"],
                "split_input_rows": result["split_input_rows"],
                "split_output_rows": result["split_output_rows"],
            },
        )
    except Exception as e:
        try:
            _update_p00_report_run(run_id, status="failed", message=str(e))
        except Exception:
            pass


@router.get("/reports/total-market-calculation/eligible-oth")
def get_total_market_calculation_eligible_oth_rows(
    planning_year: int | None = Query(default=None),
):
    normalized_year = _normalize_planning_year(planning_year)
    result = _compute_total_market_calculation_eligible_oth_result(planning_year=normalized_year)

    _record_report_run(P00_RUN_KEYS["total_market_eligible_oth"])
    snapshot_run_id = _save_p00_report_snapshot(
        P00_RUN_KEYS["total_market_eligible_oth"],
        result["rows"],
        "Total Market Calculation rows generated successfully",
        meta={
            "source_row_count": result["source_row_count"],
            "split_machine_lines": result["split_machine_lines"],
            "source_report_run_id": result["source_report_run_id"],
            "source_report_created_at": result["source_report_created_at"],
            "three_check_report_run_id": result["three_check_report_run_id"],
            "three_check_report_created_at": result["three_check_report_created_at"],
            "split_input_rows": result["split_input_rows"],
            "split_output_rows": result["split_output_rows"],
        },
        planning_year=normalized_year,
    )
    return {
        **result,
        "run_id": snapshot_run_id,
        "planning_year": normalized_year,
    }


@router.post("/reports/total-market-calculation/eligible-oth/run")
def run_total_market_calculation_eligible_oth_report(
    background_tasks: BackgroundTasks,
    planning_year: int | None = Query(default=None),
):
    normalized_year = _normalize_planning_year(planning_year)
    run_id = _create_p00_report_run(
        P00_RUN_KEYS["total_market_eligible_oth"],
        "Running Total Market Calculation report",
        planning_year=normalized_year,
    )
    background_tasks.add_task(_run_total_market_calculation_eligible_oth_background, run_id)
    run = _get_p00_report_run(run_id)
    return {
        "run_id": run_id,
        "status": run.get("status"),
        "message": run.get("message"),
        "created_at": run.get("created_at"),
        "planning_year": run.get("planning_year"),
    }


@router.get("/reports/total-market-calculation/eligible-oth/runs/{run_id}")
def get_total_market_calculation_eligible_oth_run(run_id: int):
    run = _get_p00_report_run(run_id)
    if run.get("report_key") != P00_RUN_KEYS["total_market_eligible_oth"]:
        raise HTTPException(status_code=404, detail="Total Market Calculation run not found")
    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "run_id": run.get("id"),
        "status": run.get("status"),
        "message": run.get("message"),
        "row_count": run.get("row_count"),
        "created_at": run.get("created_at"),
        "planning_year": run.get("planning_year"),
        **meta,
    }


@router.get("/reports/total-market-calculation/eligible-oth/latest")
def get_latest_total_market_calculation_eligible_oth_rows(
    planning_year: int | None = Query(default=None),
):
    normalized_year = _normalize_planning_year(planning_year)
    run, rows = _get_latest_p00_report_snapshot(
        P00_RUN_KEYS["total_market_eligible_oth"],
        planning_year=normalized_year,
    )
    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "row_count": run.get("row_count") or len(rows),
        "rows": rows,
        "run_id": run.get("id"),
        "status": run.get("status"),
        "created_at": run.get("created_at"),
        "planning_year": run.get("planning_year"),
        **meta,
    }


@router.post("/reports/total-market-calculation/eligible-oth/snapshots")
def save_total_market_calculation_eligible_oth_snapshot(
    request: TotalMarketEligibleOthSnapshotRequest
):
    if len(request.rows) == 0:
        raise HTTPException(status_code=400, detail="No rows to save")

    normalized_year = _normalize_planning_year(request.planning_year)

    latest_meta: dict[str, Any] = {}
    previous_row_count: int | None = None
    try:
        latest_run, _ = _get_latest_p00_report_snapshot(
            P00_RUN_KEYS["total_market_eligible_oth"],
            planning_year=normalized_year,
        )
        latest_meta = json.loads(latest_run.get("meta_json") or "{}")
        previous_row_count = latest_run.get("row_count")
    except HTTPException as exc:
        if exc.status_code != 404:
            raise

    request_meta: dict[str, Any] = {}
    if request.source_row_count is not None:
        request_meta["source_row_count"] = request.source_row_count
    if request.split_machine_lines is not None:
        request_meta["split_machine_lines"] = request.split_machine_lines
    if request.split_input_rows is not None:
        request_meta["split_input_rows"] = request.split_input_rows
    if request.split_output_rows is not None:
        request_meta["split_output_rows"] = request.split_output_rows
    if request.source_report_run_id is not None:
        request_meta["source_report_run_id"] = request.source_report_run_id
    if request.source_report_created_at is not None:
        request_meta["source_report_created_at"] = request.source_report_created_at
    if request.three_check_report_run_id is not None:
        request_meta["three_check_report_run_id"] = request.three_check_report_run_id
    if request.three_check_report_created_at is not None:
        request_meta["three_check_report_created_at"] = request.three_check_report_created_at

    final_meta = {
        **latest_meta,
        **request_meta,
    }

    _record_report_run(P00_RUN_KEYS["total_market_eligible_oth"])
    run_id = _save_p00_report_snapshot(
        P00_RUN_KEYS["total_market_eligible_oth"],
        request.rows,
        request.message,
        meta=final_meta,
        planning_year=normalized_year,
    )
    return {
        "run_id": run_id,
        "row_count": len(request.rows),
        "status": "success",
        "message": request.message,
        "previous_row_count": previous_row_count,
        "planning_year": normalized_year,
    }


@router.get("/reports/total-market-calculation/calculated/latest")
def get_latest_total_market_calculation_calculated_rows(
    planning_year: int | None = Query(default=None),
):
    normalized_year = _normalize_planning_year(planning_year)
    run, rows = _get_latest_p00_report_snapshot(
        P00_RUN_KEYS["total_market_calculated"],
        planning_year=normalized_year,
    )
    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "row_count": run.get("row_count") or len(rows),
        "rows": rows,
        "run_id": run.get("id"),
        "status": run.get("status"),
        "created_at": run.get("created_at"),
        "planning_year": run.get("planning_year"),
        **meta,
    }


@router.post("/reports/total-market-calculation/calculated/snapshots")
def save_total_market_calculation_calculated_snapshot(
    request: TotalMarketEligibleOthSnapshotRequest
):
    if len(request.rows) == 0:
        raise HTTPException(status_code=400, detail="No rows to save")

    normalized_year = _normalize_planning_year(request.planning_year)

    latest_meta: dict[str, Any] = {}
    previous_row_count: int | None = None
    try:
        latest_run, _ = _get_latest_p00_report_snapshot(
            P00_RUN_KEYS["total_market_calculated"],
            planning_year=normalized_year,
        )
        latest_meta = json.loads(latest_run.get("meta_json") or "{}")
        previous_row_count = latest_run.get("row_count")
    except HTTPException as exc:
        if exc.status_code != 404:
            raise

    request_meta: dict[str, Any] = {}
    if request.source_row_count is not None:
        request_meta["source_row_count"] = request.source_row_count
    if request.split_machine_lines is not None:
        request_meta["split_machine_lines"] = request.split_machine_lines
    if request.split_input_rows is not None:
        request_meta["split_input_rows"] = request.split_input_rows
    if request.split_output_rows is not None:
        request_meta["split_output_rows"] = request.split_output_rows
    if request.source_report_run_id is not None:
        request_meta["source_report_run_id"] = request.source_report_run_id
    if request.source_report_created_at is not None:
        request_meta["source_report_created_at"] = request.source_report_created_at
    if request.three_check_report_run_id is not None:
        request_meta["three_check_report_run_id"] = request.three_check_report_run_id
    if request.three_check_report_created_at is not None:
        request_meta["three_check_report_created_at"] = request.three_check_report_created_at

    final_meta = {
        **latest_meta,
        **request_meta,
    }

    _record_report_run(P00_RUN_KEYS["total_market_calculated"])
    run_id = _save_p00_report_snapshot(
        P00_RUN_KEYS["total_market_calculated"],
        request.rows,
        request.message,
        meta=final_meta,
        planning_year=normalized_year,
    )
    return {
        "run_id": run_id,
        "row_count": len(request.rows),
        "status": "success",
        "message": request.message,
        "previous_row_count": previous_row_count,
        "planning_year": normalized_year,
    }


@router.get("/reports/total-market-calculation/double-brand-check")
def get_total_market_calculation_double_brand_check_rows(
    planning_year: int | None = Query(default=None),
):
    normalized_year = _normalize_planning_year(planning_year)
    try:
        run, latest_rows = _get_latest_p00_report_snapshot(
            P00_RUN_KEYS["total_market_eligible_oth"],
            planning_year=normalized_year,
        )
        source_report = {
            "rows": latest_rows,
            "row_count": run.get("row_count") or len(latest_rows),
            "run_id": run.get("id"),
            "created_at": run.get("created_at"),
            "planning_year": run.get("planning_year"),
        }
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        computed = _compute_total_market_calculation_eligible_oth_result(planning_year=normalized_year)
        source_report = {
            "rows": computed["rows"],
            "row_count": computed["row_count"],
            "run_id": None,
            "created_at": None,
            "planning_year": normalized_year,
        }

    # Check duplicates on split-applied OTH working rows only.
    oth_working_rows = [
        row for row in source_report["rows"]
        if _to_case_insensitive_key(row.get("source_flag")) not in {"TMA", "SAL"}
    ]

    grouped_rows: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}

    for row in oth_working_rows:
        year_key = _to_case_insensitive_key(row.get("year"))
        country_key = _to_case_insensitive_key(row.get("country"))
        artificial_machine_line_key = _to_case_insensitive_key(row.get("artificial_machine_line"))
        size_class_key = _to_case_insensitive_key(row.get("size_class_flag"))
        brand_key = _to_case_insensitive_key(row.get("brand_code")) or _to_case_insensitive_key(row.get("brand_name"))

        # Grouping key requested by business flow:
        # country + artificial machine line + size class + brand (kept year-safe).
        if not (
            year_key
            and country_key
            and artificial_machine_line_key
            and size_class_key
            and brand_key
        ):
            continue

        group_key = (
            year_key,
            country_key,
            artificial_machine_line_key,
            size_class_key,
            brand_key,
        )
        if group_key not in grouped_rows:
            grouped_rows[group_key] = {
                "rows": [],
                "source_labels_by_key": {},
            }

        source_key = _to_case_insensitive_key(row.get("source_flag")) or _to_case_insensitive_key(row.get("source"))
        source_label = _to_text(row.get("source_flag")) or _to_text(row.get("source"))
        if source_key:
            grouped_rows[group_key]["source_labels_by_key"][source_key] = source_label or source_key
        grouped_rows[group_key]["rows"].append(row)

    duplicate_rows: list[dict[str, Any]] = []
    duplicate_group_count = 0

    for grouped in grouped_rows.values():
        source_labels_by_key: dict[str, str] = grouped["source_labels_by_key"]
        if len(source_labels_by_key) < 2:
            continue

        duplicate_group_count += 1
        distinct_sources = sorted(source_labels_by_key.values(), key=lambda value: value.upper())
        distinct_sources_text = ", ".join(distinct_sources)
        distinct_source_count = len(distinct_sources)

        for row in grouped["rows"]:
            duplicate_rows.append({
                **row,
                "distinct_source_count": distinct_source_count,
                "distinct_sources": distinct_sources_text,
            })

    duplicate_rows.sort(
        key=lambda row: (
            _to_text(row.get("year")),
            _to_text(row.get("country")),
            _to_text(row.get("artificial_machine_line")),
            _to_text(row.get("size_class_flag")),
            _to_text(row.get("brand_code")),
            _to_text(row.get("source")),
        )
    )

    return {
        "row_count": len(duplicate_rows),
        "rows": duplicate_rows,
        "duplicate_group_count": duplicate_group_count,
        "source_row_count": len(oth_working_rows),
        "source_report_run_id": source_report.get("run_id"),
        "source_report_created_at": source_report.get("created_at"),
        "planning_year": source_report.get("planning_year"),
    }


@router.get("/reports/p00-three-check")
def get_p00_three_check_report(
    track_run: bool = False,
    planning_year: int | None = Query(default=None),
):
    normalized_year = _normalize_planning_year(planning_year)
    combined = _get_crp_d1_combined_report_data(include_all_sal=True, planning_year=normalized_year)
    oth = get_oth_deletion_flag_report(track_run=False, planning_year=normalized_year)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        latest_machine_line_mapping_upload_run_id = _get_latest_success_upload_id(
            cursor,
            "machine_line_mapping",
            normalized_year,
        )
        if latest_machine_line_mapping_upload_run_id is None:
            raise HTTPException(status_code=400, detail="Missing latest successful upload for: machine_line_mapping")

        cursor.execute("""
            SELECT
                machine_line_name,
                machine_line_code,
                size_class,
                artificial_machine_line,
                position,
                row_index,
                id
            FROM machine_line_mapping_rows
            WHERE upload_run_id = ?
            ORDER BY row_index ASC, id ASC
        """, (latest_machine_line_mapping_upload_run_id,))
        machine_line_mapping_rows = [dict(row) for row in cursor.fetchall()]

        def to_number(value: Any) -> float:
            text = _to_text(value).replace(",", "")
            if not text:
                return 0.0
            try:
                return float(text)
            except ValueError:
                return 0.0

        def resolve_oth_size_class(machine_line_name: Any, machine_line_code: Any, size_class: Any) -> str:
            machine_line_name_key = _to_text(machine_line_name).upper()
            machine_line_code_key = _to_text(machine_line_code).upper()
            size_class_key = _to_text(size_class).upper()

            for mapping_row in machine_line_mapping_rows:
                position = _to_text(mapping_row.get("position", "")).upper().replace(" ", "")
                if position and "OTH" not in position.split(","):
                    continue

                mapping_size_class_key = _to_text(mapping_row.get("size_class", "")).upper()
                if mapping_size_class_key != size_class_key:
                    continue

                mapping_name_key = _to_text(mapping_row.get("machine_line_name", "")).upper()
                mapping_code_key = _to_text(mapping_row.get("machine_line_code", "")).upper()
                if not (
                    machine_line_name_key == mapping_name_key
                    or machine_line_name_key == mapping_code_key
                    or machine_line_code_key == mapping_name_key
                    or machine_line_code_key == mapping_code_key
                ):
                    continue

                artificial_machine_line = _to_text(mapping_row.get("artificial_machine_line", ""))
                if artificial_machine_line.upper() in {"MINI", "MIDI"}:
                    return artificial_machine_line
                break

            return _to_text(size_class)

        stats_by_group: dict[tuple[str, str, str, str], dict[str, float]] = {}
        result_rows: list[dict[str, Any]] = []

        for row in combined["rows"]:
            group_key = (
                _to_case_insensitive_key(row.get("year")),
                _to_case_insensitive_key(row.get("country")),
                _to_case_insensitive_key(row.get("machine_line_name")),
                _to_case_insensitive_key(row.get("size_class")),
            )
            if group_key not in stats_by_group:
                stats_by_group[group_key] = {
                    "tm": to_number(row.get("tm")),
                    "vce_fid": to_number(row.get("vce_fid")),
                    "tm_non_vce": to_number(row.get("tm_non_vce")),
                }

            source = _to_text(row.get("source")).upper()
            if source not in {"TMA", "SAL"}:
                continue

            result_rows.append({
                "year": _to_text(row.get("year")),
                "country": _to_text(row.get("country")),
                "country_grouping": _to_text(row.get("country_grouping")),
                "region": _to_text(row.get("region")),
                "machine_line_name": _to_text(row.get("machine_line_name")),
                "machine_line_code": _to_text(row.get("machine_line_code")),
                "artificial_machine_line": _to_text(row.get("artificial_machine_line")),
                "brand_name": (
                    "TMA"
                    if source == "TMA"
                    else _to_text(row.get("brand_name")) or _volvo_sale_brand_name_from_code(row.get("brand_code"))
                ),
                "brand_code": _to_text(row.get("brand_code")),
                "size_class": _to_text(row.get("size_class")),
                "source": source,
                "fid": to_number(row.get("fid")),
                "tm": to_number(row.get("tm")) if source == "TMA" else "",
                "vce_fid": to_number(row.get("vce_fid")),
                "tm_non_vce": to_number(row.get("tm_non_vce")),
                "reporter_flag": _to_text(row.get("reporter_flag")),
                "deletion_flag": _to_text(row.get("deletion_flag")),
                "pri_sec": _to_text(row.get("pri_sec")),
            })

        for row in oth["rows"]:
            comparison_size_class = resolve_oth_size_class(
                row.get("machine_line_name"),
                row.get("machine_line_code"),
                row.get("size_class_flag"),
            )
            group_key = (
                _to_case_insensitive_key(row.get("year")),
                _to_case_insensitive_key(row.get("country")),
                _to_case_insensitive_key(row.get("machine_line_name")),
                _to_case_insensitive_key(comparison_size_class),
            )
            stats = stats_by_group.get(group_key, {"tm": 0.0, "vce_fid": 0.0, "tm_non_vce": 0.0})

            result_rows.append({
                "year": _to_text(row.get("year")),
                "country": _to_text(row.get("country")),
                "country_grouping": _to_text(row.get("country_grouping")),
                "region": _to_text(row.get("region")),
                "machine_line_name": _to_text(row.get("machine_line_name")),
                "machine_line_code": _to_text(row.get("machine_line_code")),
                "artificial_machine_line": _to_text(row.get("artificial_machine_line")),
                "brand_name": _to_text(row.get("brand_name")),
                "brand_code": _to_text(row.get("brand_code")),
                "size_class": comparison_size_class,
                "source": _to_text(row.get("source")),
                "fid": to_number(row.get("fid")),
                "tm": "",
                "vce_fid": "",
                "tm_non_vce": "",
                "reporter_flag": _to_text(row.get("reporter_flag")),
                "deletion_flag": _to_text(row.get("deletion_flag")),
                "pri_sec": _to_text(row.get("pri_sec")),
            })

        result_rows.sort(key=lambda row: (
            row["year"],
            row["country"],
            row["machine_line_name"],
            row["size_class"],
            row["brand_name"],
            row["source"],
        ))

        result = {
            "row_count": len(result_rows),
            "rows": result_rows,
            "tma_upload_run_id": combined.get("tma_upload_run_id"),
            "volvo_upload_run_id": combined.get("volvo_upload_run_id"),
            "group_country_upload_run_id": combined.get("group_country_upload_run_id"),
            "source_matrix_upload_run_id": combined.get("source_matrix_upload_run_id"),
            "machine_line_mapping_upload_run_id": latest_machine_line_mapping_upload_run_id,
            "oth_upload_run_id": oth.get("oth_upload_run_id"),
        }
        if track_run:
            _record_report_run(P00_RUN_KEYS["three_check"])
            _save_p00_report_snapshot(
                P00_RUN_KEYS["three_check"],
                result["rows"],
                "P00 3 Check report generated successfully",
                meta={
                    "planning_year": normalized_year,
                    "tma_upload_run_id": result["tma_upload_run_id"],
                    "volvo_upload_run_id": result["volvo_upload_run_id"],
                    "group_country_upload_run_id": result["group_country_upload_run_id"],
                    "source_matrix_upload_run_id": result["source_matrix_upload_run_id"],
                    "machine_line_mapping_upload_run_id": result["machine_line_mapping_upload_run_id"],
                    "oth_upload_run_id": result["oth_upload_run_id"],
                },
                planning_year=normalized_year,
            )
        return result
    finally:
        conn.close()


@router.get("/reports/p00-three-check/latest")
def get_latest_p00_three_check_report(planning_year: int | None = Query(default=None)):
    normalized_year = _normalize_planning_year(planning_year)
    run, rows = _get_latest_p00_report_snapshot(
        P00_RUN_KEYS["three_check"],
        planning_year=normalized_year,
    )
    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "row_count": run.get("row_count") or len(rows),
        "rows": rows,
        "run_id": run.get("id"),
        "status": run.get("status"),
        "created_at": run.get("created_at"),
        **meta,
    }


@router.get("/reports/p00-run-times")
def get_p00_run_times():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        return {
            "crp_d1_combined_run_at": _get_latest_report_run_time(cursor, P00_RUN_KEYS["crp_d1_combined"]),
            "oth_deletion_flag_run_at": _get_latest_report_run_time(cursor, P00_RUN_KEYS["oth_deletion_flag"]),
            "p00_three_check_run_at": _get_latest_report_run_time(cursor, P00_RUN_KEYS["three_check"]),
        }
    finally:
        conn.close()


@router.get("/reports/p10-vce-non-vce")
def get_p10_vce_non_vce_report(planning_year: int | None = Query(default=None)):
    normalized_year = _normalize_planning_year(planning_year)
    combined = _get_crp_d1_combined_report_data(include_all_sal=False, planning_year=normalized_year)
    combined_rows = combined["rows"]

    grouped = {}
    for row in combined_rows:
        source = str(row.get("source", "")).strip().upper()
        raw_size_class = _to_text(row.get("size_class", ""))
        artificial_machine_line = _to_text(row.get("artificial_machine_line", ""))
        if source == "TMA" and artificial_machine_line.upper() in {"MINI", "MIDI"}:
            p10_size_class = artificial_machine_line
        else:
            p10_size_class = raw_size_class
        key = (
            _to_case_insensitive_key(row.get("year", "")),
            _to_case_insensitive_key(row.get("country_group_code", "")),
            _to_case_insensitive_key(row.get("country_grouping", "")),
            _to_case_insensitive_key(row.get("country", "")),
            _to_case_insensitive_key(row.get("region", "")),
            _to_case_insensitive_key(row.get("machine_line_code", "")),
            _to_case_insensitive_key(row.get("machine_line_name", "")),
            _to_case_insensitive_key(artificial_machine_line),
            _to_case_insensitive_key(p10_size_class),
        )
        if key not in grouped:
            grouped[key] = {
                "year": row.get("year", ""),
                "country_group_code": row.get("country_group_code", ""),
                "country_grouping": row.get("country_grouping", ""),
                "country": row.get("country", ""),
                "region": row.get("region", ""),
                "machine_line_code": row.get("machine_line_code", ""),
                "machine_line_name": row.get("machine_line_name", ""),
                "artificial_machine_line": artificial_machine_line,
                "size_class": p10_size_class,
                "total_market": 0.0,
                "vce": 0.0,
                "non_vce": 0.0,
                "vce_share_pct": "",
                "exclude_from_report": False,
            }

        target = grouped[key]
        fid = float(row.get("fid") or 0)
        reporter_flag = str(row.get("reporter_flag", "")).strip().upper()
        deletion_flag = str(row.get("deletion_flag", "")).strip().upper()
        machine_line_code = str(row.get("machine_line_code", "")).strip().upper()
        machine_line_name = str(row.get("machine_line_name", "")).strip().upper()

        if source != "TMA" and deletion_flag == "Y":
            target["exclude_from_report"] = True

        if machine_line_code == "MOTOR GRADERS" or machine_line_name == "MOTOR GRADERS":
            target["exclude_from_report"] = True

        if source == "TMA":
            target["total_market"] += fid

        if reporter_flag == "Y" and deletion_flag != "Y":
            target["vce"] += fid

    result_rows = []
    total_market_sum = 0.0
    vce_sum = 0.0
    non_vce_sum = 0.0

    for row in grouped.values():
        if row["exclude_from_report"]:
            continue

        non_vce = row["total_market"] - row["vce"]
        row["non_vce"] = non_vce if non_vce > 0 else 0.0
        row["vce_share_pct"] = (
            f"{(row['vce'] / row['total_market']) * 100:.1f}%"
            if row["total_market"] > 0
            else ""
        )
        row.pop("exclude_from_report", None)
        total_market_sum += row["total_market"]
        vce_sum += row["vce"]
        non_vce_sum += row["non_vce"]
        result_rows.append(row)

    result_rows.sort(
        key=lambda item: (
            item["country_grouping"],
            item["country_group_code"],
            item["country"],
            item["machine_line_code"],
            item["machine_line_name"],
            item["artificial_machine_line"],
            item["size_class"],
        )
    )

    return {
        "row_count": len(result_rows),
        "rows": result_rows,
        "summary": {
            "total_market_sum": total_market_sum,
            "vce_sum": vce_sum,
            "non_vce_sum": non_vce_sum,
        },
        "source_row_count": combined["row_count"],
        "tma_upload_run_id": combined["tma_upload_run_id"],
        "volvo_upload_run_id": combined["volvo_upload_run_id"],
        "group_country_upload_run_id": combined["group_country_upload_run_id"],
        "source_matrix_upload_run_id": combined["source_matrix_upload_run_id"],
    }


def _build_cex_split_case_report(planning_year: int | None = None):
    try:
        oth = get_latest_oth_deletion_flag_report(planning_year=planning_year)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        oth = get_oth_deletion_flag_report(track_run=False, planning_year=planning_year)

    source_size_key = "<10T"
    target_lt_6t_key = "<6T"
    target_6_10t_key = "6<10T"

    cex_candidate_rows = [
        row for row in oth["rows"]
        if _to_case_insensitive_key(row.get("reporter_flag")) == "Y"
        and _to_case_insensitive_key(row.get("artificial_machine_line")) == "CEX"
        and _to_size_class_key(row.get("size_class_flag")) == source_size_key
    ]

    dependency_signature: dict[str, Any] = {
        "logic_version": 2,
        "case_type": "CEX",
        "oth_report_run_id": oth.get("run_id"),
        "oth_report_created_at": oth.get("created_at"),
        "oth_upload_run_id": oth.get("oth_upload_run_id"),
    }

    if len(cex_candidate_rows) == 0:
        return {
            "case_type": "CEX",
            "summary_rows": [],
            "detail_rows": [],
            "summary": {
                "grouped_rows": 0,
                "matched_rows": 0,
                "gross_fid_total": 0.0,
                "volvo_deduction_total": 0.0,
                "net_fid_total": 0.0,
            },
            "source_row_count": 0,
            "oth_row_count": oth["row_count"],
            "p10_row_count": 0,
            "dependency_signature": dependency_signature,
        }

    try:
        latest_combined = get_latest_crp_d1_combined_report(planning_year=planning_year)
        dependency_signature["crp_d1_combined_run_id"] = latest_combined.get("run_id")
        dependency_signature["crp_d1_combined_created_at"] = latest_combined.get("created_at")
        dependency_signature["crp_d1_combined_row_count"] = latest_combined.get("row_count")
    except HTTPException as exc:
        if exc.status_code != 404:
            raise

    reusable_result = _try_reuse_excavators_split_case_snapshot(
        "CEX",
        dependency_signature,
        planning_year=planning_year,
    )
    if reusable_result is not None:
        return reusable_result

    p10 = get_p10_vce_non_vce_report(planning_year=planning_year)
    dependency_signature["p10_row_count"] = p10.get("row_count")
    dependency_signature["p10_total_market_sum"] = p10.get("total_market_sum")
    dependency_signature["p10_vce_sum"] = p10.get("vce_sum")
    dependency_signature["p10_non_vce_sum"] = p10.get("non_vce_sum")

    def add_reference_value(target: dict[tuple[str, str, str], float], key_parts: tuple[str, str], size_key: str, value: float):
        target[(key_parts[0], key_parts[1], size_key)] = target.get((key_parts[0], key_parts[1], size_key), 0.0) + value

    country_reference: dict[tuple[str, str, str], float] = {}
    region_reference: dict[tuple[str, str, str], float] = {}
    country_grouping_reference: dict[tuple[str, str, str], float] = {}

    for row in p10["rows"]:
        if _to_case_insensitive_key(row.get("artificial_machine_line")) != "CEX":
            continue

        size_key = _to_size_class_key(row.get("size_class"))
        if size_key not in {target_lt_6t_key, target_6_10t_key}:
            continue

        non_vce = _to_number(row.get("non_vce"))
        year_key = _to_case_insensitive_key(row.get("year"))
        country_key = _to_case_insensitive_key(row.get("country"))
        region_key = _to_case_insensitive_key(row.get("region"))
        country_grouping_key = _to_case_insensitive_key(row.get("country_grouping"))

        if year_key and country_key:
            add_reference_value(country_reference, (year_key, country_key), size_key, non_vce)
        if year_key and region_key:
            add_reference_value(region_reference, (year_key, region_key), size_key, non_vce)
        if year_key and country_grouping_key:
            add_reference_value(country_grouping_reference, (year_key, country_grouping_key), size_key, non_vce)

    def get_reference_distribution(row: dict[str, Any]) -> tuple[str, float, float]:
        year_key = _to_case_insensitive_key(row.get("year"))
        country_key = _to_case_insensitive_key(row.get("country"))
        region_key = _to_case_insensitive_key(row.get("region"))
        country_grouping_key = _to_case_insensitive_key(row.get("country_grouping"))

        lookup_chain = [
            ("Country", country_reference, (year_key, country_key)),
            ("Region", region_reference, (year_key, region_key)),
            ("Country Grouping", country_grouping_reference, (year_key, country_grouping_key)),
        ]

        for level, source_map, key_parts in lookup_chain:
            if not key_parts[0] or not key_parts[1]:
                continue

            tm_non_vce_lt_6t = source_map.get((key_parts[0], key_parts[1], target_lt_6t_key), 0.0)
            tm_non_vce_6_10t = source_map.get((key_parts[0], key_parts[1], target_6_10t_key), 0.0)
            if tm_non_vce_lt_6t + tm_non_vce_6_10t > 0:
                return level, tm_non_vce_lt_6t, tm_non_vce_6_10t

        return "", 0.0, 0.0

    def build_split_ratio_text(tm_non_vce_lt_6t: float, tm_non_vce_6_10t: float) -> str:
        total_reference = tm_non_vce_lt_6t + tm_non_vce_6_10t
        if total_reference <= 0:
            return ""
        lt_6t_ratio = (tm_non_vce_lt_6t / total_reference) * 100
        ratio_6_10t = (tm_non_vce_6_10t / total_reference) * 100
        return f"<6T {lt_6t_ratio:.2f}% | 6<10T {ratio_6_10t:.2f}%"

    summary_by_group: dict[str, dict[str, Any]] = {}
    detail_rows: list[dict[str, Any]] = []
    tma_rows_by_group: dict[str, dict[str, Any]] = {}

    for row in cex_candidate_rows:
        summary_key = "|".join([
            _to_case_insensitive_key(row.get("year")),
            _to_case_insensitive_key(row.get("machine_line_name")),
            _to_case_insensitive_key(row.get("machine_line_code")),
            _to_case_insensitive_key(row.get("source")),
            _to_size_class_key(row.get("size_class_flag")),
        ])
        if summary_key not in summary_by_group:
            summary_by_group[summary_key] = {
                "year": _to_text(row.get("year")),
                "machine_line_name": _to_text(row.get("machine_line_name")),
                "machine_line_code": _to_text(row.get("machine_line_code")),
                "source": _to_text(row.get("source")),
                "size_class_flag": _to_text(row.get("size_class_flag")),
                "matched_rows": 0,
                "gross_fid": 0.0,
                "volvo_deduction": 0.0,
                "net_fid": 0.0,
            }

        summary_row = summary_by_group[summary_key]
        fid = _to_number(row.get("fid"))
        summary_row["matched_rows"] += 1
        summary_row["gross_fid"] += fid

        if _to_case_insensitive_key(row.get("brand_name")) == "VOLVO":
            summary_row["volvo_deduction"] += fid
        else:
            summary_row["net_fid"] += fid

        if _to_case_insensitive_key(row.get("brand_name")) == "VOLVO":
            continue

        reference_level, tm_non_vce_lt_6t, tm_non_vce_6_10t = get_reference_distribution(row)
        total_reference = tm_non_vce_lt_6t + tm_non_vce_6_10t
        split_ratio = build_split_ratio_text(tm_non_vce_lt_6t, tm_non_vce_6_10t)
        after_split_fid_lt_6t = (
            _round_to_4((fid * tm_non_vce_lt_6t) / total_reference)
            if total_reference > 0
            else 0.0
        )
        after_split_fid_6_10t = (
            _round_to_4((fid * tm_non_vce_6_10t) / total_reference)
            if total_reference > 0
            else 0.0
        )
        before_after_difference = _round_to_4(
            fid - after_split_fid_lt_6t - after_split_fid_6_10t
        )

        detail_rows.append({
            "row_type": "OTH",
            "year": _to_text(row.get("year")),
            "country_grouping": _to_text(row.get("country_grouping")),
            "country": _to_text(row.get("country")),
            "region": _to_text(row.get("region")),
            "machine_line": _to_text(row.get("machine_line_name")),
            "artificial_machine_line": _to_text(row.get("artificial_machine_line")),
            "brand_code": _to_text(row.get("brand_code")),
            "reporter_flag": _to_text(row.get("reporter_flag")),
            "source": _to_text(row.get("source")),
            "pri_sec": _to_text(row.get("pri_sec")),
            "size_class": _to_text(row.get("size_class_flag")),
            "before_split_fid_lt_10t": _round_to_4(fid),
            "copy_fid_lt_10t": 0.0,
            "after_split_fid_lt_6t": after_split_fid_lt_6t,
            "after_split_fid_6_10t": after_split_fid_6_10t,
            "tm_non_vce_lt_6t": "",
            "tm_non_vce_6_10t": "",
            "before_after_difference": before_after_difference,
            "reference_level": reference_level,
            "split_ratio": split_ratio,
        })

        detail_group_key = "|".join([
            _to_case_insensitive_key(row.get("year")),
            _to_case_insensitive_key(row.get("country_grouping")),
            _to_case_insensitive_key(row.get("country")),
            _to_case_insensitive_key(row.get("region")),
            _to_case_insensitive_key(row.get("machine_line_name")),
            _to_case_insensitive_key(row.get("artificial_machine_line")),
        ])
        if detail_group_key not in tma_rows_by_group:
            tma_rows_by_group[detail_group_key] = {
                "row_type": "TMA",
                "year": _to_text(row.get("year")),
                "country_grouping": _to_text(row.get("country_grouping")),
                "country": _to_text(row.get("country")),
                "region": _to_text(row.get("region")),
                "machine_line": _to_text(row.get("machine_line_name")),
                "artificial_machine_line": _to_text(row.get("artificial_machine_line")),
                "brand_code": "#",
                "reporter_flag": "#",
                "source": "TMA",
                "pri_sec": "#",
                "size_class": _to_text(row.get("size_class_flag")),
                "before_split_fid_lt_10t": "",
                "copy_fid_lt_10t": "",
                "after_split_fid_lt_6t": "",
                "after_split_fid_6_10t": "",
                "tm_non_vce_lt_6t": _round_to_4(tm_non_vce_lt_6t),
                "tm_non_vce_6_10t": _round_to_4(tm_non_vce_6_10t),
                "before_after_difference": "",
                "reference_level": reference_level,
                "split_ratio": build_split_ratio_text(tm_non_vce_lt_6t, tm_non_vce_6_10t),
            }

    detail_rows.extend(tma_rows_by_group.values())

    summary_rows = sorted(
        summary_by_group.values(),
        key=lambda item: (
            item["year"],
            item["machine_line_name"],
            item["source"],
            item["size_class_flag"],
        ),
    )
    detail_rows.sort(
        key=lambda item: (
            item["year"],
            item["country_grouping"],
            item["country"],
            item["region"],
            item["row_type"],
            item["source"],
            item["brand_code"],
        )
    )

    grouped_rows = len(summary_rows)
    matched_rows = sum(int(item["matched_rows"]) for item in summary_rows)
    gross_fid_total = sum(float(item["gross_fid"]) for item in summary_rows)
    volvo_deduction_total = sum(float(item["volvo_deduction"]) for item in summary_rows)
    net_fid_total = sum(float(item["net_fid"]) for item in summary_rows)

    return {
        "case_type": "CEX",
        "summary_rows": summary_rows,
        "detail_rows": detail_rows,
        "summary": {
            "grouped_rows": grouped_rows,
            "matched_rows": matched_rows,
            "gross_fid_total": gross_fid_total,
            "volvo_deduction_total": volvo_deduction_total,
            "net_fid_total": net_fid_total,
        },
        "source_row_count": len(detail_rows),
        "oth_row_count": oth["row_count"],
        "p10_row_count": p10["row_count"],
        "dependency_signature": dependency_signature,
    }


@router.post("/reports/excavators-split/snapshots")
def save_excavators_split_case_snapshot(request: ExcavatorsSplitCaseSnapshotRequest):
    case_type = request.case_type.strip().upper()
    if case_type not in {"ALL", "CEX", "GEC", "GEW", "WLO_GT10", "WLO_LT10", "WLO_LT12"}:
        raise HTTPException(status_code=400, detail="Unsupported excavators split case type")
    normalized_year = _normalize_planning_year(request.planning_year)

    run_id = _create_excavators_split_case_run(case_type, request.message, planning_year=normalized_year)
    result = {
        "case_type": case_type,
        "summary_rows": request.summary_rows,
        "detail_rows": request.detail_rows,
        "summary": request.summary,
        "source_row_count": request.source_row_count,
        "oth_row_count": request.oth_row_count,
        "p10_row_count": request.p10_row_count,
    }
    _save_excavators_split_case_snapshot(
        run_id,
        result,
        request.message,
    )
    return {
        "run_id": run_id,
        "case_type": case_type,
        "planning_year": normalized_year,
        "status": "success",
        "message": request.message,
        "row_count": len(request.summary_rows) + len(request.detail_rows),
    }


@router.get("/reports/excavators-split/{case_type}/latest")
def get_latest_excavators_split_case_report(
    case_type: str,
    planning_year: int | None = Query(default=None),
):
    normalized_case_type = case_type.strip().upper()
    if normalized_case_type not in {"ALL", "CEX", "GEC", "GEW", "WLO_GT10", "WLO_LT10", "WLO_LT12"}:
        raise HTTPException(status_code=400, detail="Unsupported excavators split case type")
    normalized_year = _normalize_planning_year(planning_year)

    run, summary_rows, detail_rows = _get_latest_excavators_split_case_snapshot(
        normalized_case_type,
        planning_year=normalized_year,
    )
    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "case_type": normalized_case_type,
        "summary_rows": summary_rows,
        "detail_rows": detail_rows,
        "summary": meta.get("summary") or {},
        "source_row_count": meta.get("source_row_count"),
        "oth_row_count": meta.get("oth_row_count"),
        "p10_row_count": meta.get("p10_row_count"),
        "run_id": run["id"],
        "planning_year": run.get("planning_year"),
        "status": run["status"],
        "created_at": run["created_at"],
        "message": run["message"],
        "row_count": run["row_count"],
    }


@router.post("/reports/excavators-split/{case_type}/run")
def run_excavators_split_case_report(
    case_type: str,
    background_tasks: BackgroundTasks,
    planning_year: int | None = Query(default=None),
):
    normalized_case_type = case_type.strip().upper()
    if normalized_case_type not in {"ALL", "CEX", "GEC", "GEW", "WLO_GT10", "WLO_LT10", "WLO_LT12"}:
        raise HTTPException(status_code=400, detail="Unsupported excavators split case type")
    normalized_year = _normalize_planning_year(planning_year)

    run_id = _create_excavators_split_case_run(
        normalized_case_type,
        f"Excavators Split {normalized_case_type} run started",
        planning_year=normalized_year,
    )
    background_tasks.add_task(_run_excavators_split_case_background, run_id)
    return {
        "run_id": run_id,
        "case_type": normalized_case_type,
        "planning_year": normalized_year,
        "status": "running",
        "message": f"Excavators Split {normalized_case_type} run started",
    }


@router.get("/reports/excavators-split/{case_type}/runs/{run_id}")
def get_excavators_split_case_run(case_type: str, run_id: int):
    normalized_case_type = case_type.strip().upper()
    if normalized_case_type not in {"ALL", "CEX", "GEC", "GEW", "WLO_GT10", "WLO_LT10", "WLO_LT12"}:
        raise HTTPException(status_code=400, detail="Unsupported excavators split case type")

    run = _get_excavators_split_case_run(run_id)
    if run.get("case_type") != normalized_case_type:
        raise HTTPException(status_code=404, detail="Excavators split run not found")

    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "run_id": run["id"],
        "case_type": run["case_type"],
        "planning_year": run.get("planning_year"),
        "status": run["status"],
        "message": run["message"],
        "created_at": run["created_at"],
        "row_count": run["row_count"],
        **meta,
    }


@router.post("/reports/excavators-split-cex/run")
def run_excavators_split_cex_report(
    background_tasks: BackgroundTasks,
    planning_year: int | None = Query(default=None),
):
    normalized_year = _normalize_planning_year(planning_year)
    run_id = _create_excavators_split_case_run(
        "CEX",
        "Excavators Split CEX run started",
        planning_year=normalized_year,
    )
    background_tasks.add_task(_run_excavators_split_case_background, run_id)
    return {
        "run_id": run_id,
        "case_type": "CEX",
        "planning_year": normalized_year,
        "status": "running",
        "message": "Excavators Split CEX run started",
    }


@router.get("/reports/excavators-split-cex/runs/{run_id}")
def get_excavators_split_cex_run(run_id: int):
    run = _get_excavators_split_case_run(run_id)
    meta = json.loads(run.get("meta_json") or "{}")
    return {
        "run_id": run["id"],
        "case_type": run["case_type"],
        "planning_year": run.get("planning_year"),
        "status": run["status"],
        "message": run["message"],
        "created_at": run["created_at"],
        "row_count": run["row_count"],
        **meta,
    }


@router.get("/reports/excavators-split-cex")
@router.get("/reports/excavators-split-cex/latest")
def get_latest_excavators_split_cex_report(planning_year: int | None = Query(default=None)):
    return get_latest_excavators_split_case_report("CEX", planning_year=planning_year)

@router.get("/uploads/latest/{matrix_type}")
def get_latest_upload_by_matrix_type(
    matrix_type: str,
    planning_year: int | None = Query(default=None),
):
    table_name = MATRIX_TYPE_TO_TABLE.get(matrix_type)
    if table_name is None:
        raise HTTPException(status_code=400, detail="Unsupported matrix type")
    normalized_year = _normalize_planning_year(planning_year)

    conn = get_connection()
    cursor = conn.cursor()
    if normalized_year is None:
        cursor.execute("""
            SELECT *
            FROM upload_runs
            WHERE matrix_type = ?
            ORDER BY uploaded_at DESC, id DESC
            LIMIT 1
        """, (matrix_type,))
    else:
        cursor.execute("""
            SELECT *
            FROM upload_runs
            WHERE matrix_type = ? AND planning_year = ?
            ORDER BY uploaded_at DESC, id DESC
            LIMIT 1
        """, (matrix_type, normalized_year))
    latest_upload = cursor.fetchone()

    if latest_upload is None:
        conn.close()
        raise HTTPException(status_code=404, detail="No uploads found for this matrix type")

    latest_upload_dict = dict(latest_upload)
    if matrix_type == "oth_data":
        cursor.execute("""
            SELECT
                id,
                upload_run_id,
                row_index,
                year,
                source,
                brand_name,
                machine_line,
                country,
                size_class,
                quantity
            FROM oth_data_rows
            WHERE upload_run_id = ?
            ORDER BY row_index ASC, id ASC
        """, (latest_upload_dict["id"],))
    elif matrix_type == "group_country":
        cursor.execute("""
            SELECT
                id,
                upload_run_id,
                row_index,
                year,
                group_code,
                country_code,
                country_name,
                country_grouping,
                region,
                market_area,
                market_area_code
            FROM group_country_rows
            WHERE upload_run_id = ?
            ORDER BY row_index ASC, id ASC
        """, (latest_upload_dict["id"],))
    else:
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
