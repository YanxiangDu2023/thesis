import csv
import os
import uuid
import unicodedata
from datetime import datetime
from typing import Any

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
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
    "volvo_sale_data": "volvo_sale_data_rows",
    "tma_data": "tma_data_rows",
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


class SaveEditedUploadRequest(BaseModel):
    matrix_type: str
    rows: list[dict[str, Any]]
    source_upload_run_id: int | None = None


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).strip()
    return "" if text.lower() == "nan" else text


def _get_table_insert_columns(cursor, table_name: str) -> list[str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row["name"] for row in cursor.fetchall()]
    excluded = {"id", "upload_run_id", "row_index"}
    return [column for column in columns if column not in excluded]


def _get_latest_success_upload_id(cursor, matrix_type: str):
    cursor.execute("""
        SELECT id
        FROM upload_runs
        WHERE matrix_type = ? AND status = 'success'
        ORDER BY uploaded_at DESC, id DESC
        LIMIT 1
    """, (matrix_type,))
    row = cursor.fetchone()
    return row["id"] if row else None


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

    try:
        insert_columns = _get_table_insert_columns(cursor, table_name)
        if len(insert_columns) == 0:
            raise HTTPException(
                status_code=500,
                detail=f"No writable columns found for {matrix_type}",
            )

        target_dir = os.path.join("uploads", matrix_type)
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
                writer.writerow([_to_text(row.get(column, "")) for column in insert_columns])

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
            values = [_to_text(row.get(column, "")) for column in insert_columns]
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

        for index, row in enumerate(rows, start=1):
            cursor.execute("""
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
            """, (
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
                row["ms_percent"]
            ))

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
    rows = [dict(row) for row in cursor.fetchall()]
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

    cursor.execute("""
        INSERT INTO crp_tma_report_runs (
            tma_upload_run_id,
            status,
            message
        ) VALUES (?, ?, ?)
    """, (
        tma_upload_run_id,
        "running",
        "CRP TMA report run started"
    ))
    report_run_id = cursor.lastrowid
    conn.commit()

    try:
        cursor.execute("""
            SELECT
                t.year AS year,
                t.geographical_region AS geographical_region,
                t.geographical_market_area AS geographical_market_area,
                t.end_country_code AS end_country_code,
                t.end_country AS country,
                t.machine_line AS machine_line,
                t.machine_line_code AS machine_line_code,
                t.size_class_mapping AS size_class_mapping,
                SUM(
                    CAST(
                        REPLACE(NULLIF(TRIM(t.total_market_fid_sales), ''), ',', '')
                        AS REAL
                    )
                ) AS fid_sum,
                'TMA' AS source
            FROM tma_data_rows t
            WHERE t.upload_run_id = ?
            GROUP BY
                t.year,
                t.geographical_region,
                t.geographical_market_area,
                t.end_country_code,
                t.end_country,
                t.machine_line,
                t.machine_line_code,
                t.size_class_mapping
            ORDER BY
                t.year,
                t.geographical_region,
                t.geographical_market_area,
                t.end_country_code,
                t.end_country,
                t.machine_line,
                t.machine_line_code,
                t.size_class_mapping
        """, (tma_upload_run_id,))
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
                    size_class_mapping,
                    fid_sum,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            "CRP TMA report generated successfully",
            report_run_id
        ))
        conn.commit()

        return {
            "message": "CRP TMA report generated successfully",
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
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {
        "run": latest_run_dict,
        "rows": rows
    }


def _get_crp_d1_combined_report_data(include_all_sal: bool):
    conn = get_connection()
    cursor = conn.cursor()

    latest_tma_upload_run_id = _get_latest_success_upload_id(cursor, "tma_data")
    latest_volvo_upload_run_id = _get_latest_success_upload_id(cursor, "volvo_sale_data")
    latest_group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country")
    latest_source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix")
    latest_machine_line_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "machine_line_mapping")

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

    if missing_types:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail=f"Missing latest successful upload for: {', '.join(missing_types)}"
        )

    try:
        sal_visibility_clause = ""
        if not include_all_sal:
            sal_visibility_clause = """
                WHERE UPPER(TRIM(a.source)) <> 'SAL'
                   OR TRIM(COALESCE(sm_country_line.crp_source, '')) <> ''
            """

        cursor.execute(f"""
            WITH gc_by_code AS (
                SELECT
                    UPPER(TRIM(country_code)) AS country_code_key,
                    UPPER(TRIM(year)) AS year_key,
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
                    UPPER(TRIM(country_name)) AS country_name_key,
                    UPPER(TRIM(year)) AS year_key,
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
                    'TMA' AS source
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
                    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL') AS source
                FROM volvo_sale_data_rows v
                WHERE v.upload_run_id = ?
                GROUP BY
                    TRIM(v.calendar),
                    TRIM(v.country),
                    TRIM(v.region),
                    TRIM(v.machine),
                    TRIM(v.machine_line),
                    TRIM(v.size_class),
                    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL')
            ),
            all_agg AS (
                SELECT * FROM tma_agg
                UNION ALL
                SELECT * FROM volvo_agg
            ),
            source_matrix_country_line AS (
                SELECT
                    UPPER(TRIM(country_name)) AS country_name_key,
                    UPPER(TRIM(machine_line_name)) AS machine_line_name_key,
                    MAX(
                        CASE
                            WHEN TRIM(COALESCE(crp_source, '')) <> '' THEN TRIM(crp_source)
                            ELSE NULL
                        END
                    ) AS crp_source
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(country_name, '')) <> ''
                  AND TRIM(COALESCE(machine_line_name, '')) <> ''
                GROUP BY
                    UPPER(TRIM(country_name)),
                    UPPER(TRIM(machine_line_name))
            ),
            source_matrix_machine_lines AS (
                SELECT
                    UPPER(TRIM(machine_line_name)) AS machine_line_name_key
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(machine_line_name, '')) <> ''
                GROUP BY UPPER(TRIM(machine_line_name))
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
                    COALESCE(g_code.country_grouping, g_name.country_grouping, '') AS country_grouping,
                    COALESCE(g_code.country_name, g_name.country_name, a.country_raw) AS country,
                    COALESCE(g_code.region, g_name.region, a.region_raw) AS region,
                    a.machine_line_code AS machine_line_code,
                    a.machine_line_name AS machine_line_name,
                    a.size_class AS size_class,
                    CASE
                        WHEN UPPER(TRIM(a.source)) = 'SAL' THEN 'VCE'
                        ELSE '#'
                    END AS brand_code,
                    CASE
                        WHEN UPPER(TRIM(a.source)) = 'TMA' THEN '#'
                        WHEN UPPER(TRIM(a.source)) = 'SAL'
                             AND TRIM(COALESCE(sm_country_line.crp_source, '')) <> '' THEN 'Y'
                        ELSE ''
                    END AS reporter_flag,
                    '#' AS pri_sec,
                    a.source AS source,
                    CASE
                        WHEN UPPER(TRIM(a.source)) = 'SAL'
                             AND TRIM(CAST(a.machine_line_code AS TEXT)) = '390' THEN 'Y'
                        WHEN UPPER(TRIM(a.source)) = 'SAL'
                             AND TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), '')) <> ''
                             AND sm.machine_line_name_key IS NULL THEN 'Y'
                        ELSE ''
                    END AS deletion_flag,
                    a.fid AS fid
                FROM all_agg a
                LEFT JOIN gc_by_code g_code
                  ON UPPER(TRIM(a.end_country_code)) = g_code.country_code_key
                 AND UPPER(TRIM(a.year)) = g_code.year_key
                LEFT JOIN gc_by_name g_name
                  ON UPPER(TRIM(a.country_raw)) = g_name.country_name_key
                 AND UPPER(TRIM(a.year)) = g_name.year_key
                LEFT JOIN source_matrix_country_line sm_country_line
                  ON UPPER(TRIM(COALESCE(g_code.country_name, g_name.country_name, a.country_raw))) = sm_country_line.country_name_key
                 AND UPPER(TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), ''))) = sm_country_line.machine_line_name_key
                LEFT JOIN source_matrix_machine_lines sm
                  ON UPPER(TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), ''))) = sm.machine_line_name_key
                {sal_visibility_clause}
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
                    frb.country_grouping AS country_grouping,
                    frb.country AS country,
                    frb.region AS region,
                    frb.machine_line_code AS machine_line_code,
                    frb.machine_line_name AS machine_line_name,
                    frb.size_class AS size_class,
                    COALESCE(mlmm.artificial_machine_line, '') AS artificial_machine_line,
                    frb.brand_code AS brand_code,
                    frb.reporter_flag AS reporter_flag,
                    frb.pri_sec AS pri_sec,
                    frb.source AS source,
                    frb.deletion_flag AS deletion_flag,
                    frb.fid AS fid
                FROM final_rows_base frb
                LEFT JOIN machine_line_mapping_matches mlmm
                  ON frb.base_row_id = mlmm.base_row_id
                 AND mlmm.match_rank = 1
            ),
            display_rows AS (
                SELECT
                    year,
                    country_group_code,
                    country_grouping,
                    country,
                    region,
                    machine_line_code,
                    machine_line_name,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(source, ''))) = 'TMA'
                         AND UPPER(TRIM(COALESCE(artificial_machine_line, ''))) IN ('MINI', 'MIDI')
                        THEN artificial_machine_line
                        ELSE size_class
                    END AS size_class,
                    artificial_machine_line,
                    brand_code,
                    reporter_flag,
                    pri_sec,
                    source,
                    deletion_flag,
                    SUM(COALESCE(fid, 0)) AS fid
                FROM final_rows
                GROUP BY
                    year,
                    country_group_code,
                    country_grouping,
                    country,
                    region,
                    machine_line_code,
                    machine_line_name,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(source, ''))) = 'TMA'
                         AND UPPER(TRIM(COALESCE(artificial_machine_line, ''))) IN ('MINI', 'MIDI')
                        THEN artificial_machine_line
                        ELSE size_class
                    END,
                    artificial_machine_line,
                    brand_code,
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
                            WHEN UPPER(TRIM(source)) = 'TMA' THEN COALESCE(fid, 0)
                            ELSE 0
                        END
                    ) AS tm,
                    SUM(
                        CASE
                            WHEN UPPER(TRIM(source)) = 'SAL'
                                 AND UPPER(TRIM(COALESCE(reporter_flag, ''))) = 'Y'
                                 AND UPPER(TRIM(COALESCE(deletion_flag, ''))) <> 'Y'
                            THEN COALESCE(fid, 0)
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
                fr.country_grouping AS country_grouping,
                fr.country AS country,
                fr.region AS region,
                fr.machine_line_code AS machine_line_code,
                fr.machine_line_name AS machine_line_name,
                fr.size_class AS size_class,
                fr.artificial_machine_line AS artificial_machine_line,
                fr.brand_code AS brand_code,
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
            latest_source_matrix_upload_run_id,
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
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/reports/crp-d1-combined")
def get_crp_d1_combined_report():
    return _get_crp_d1_combined_report_data(include_all_sal=True)


@router.get("/reports/a10-adjustment")
def get_a10_adjustment_report():
    conn = get_connection()
    cursor = conn.cursor()

    latest_tma_upload_run_id = _get_latest_success_upload_id(cursor, "tma_data")
    latest_volvo_upload_run_id = _get_latest_success_upload_id(cursor, "volvo_sale_data")
    latest_group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country")
    latest_source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix")
    latest_machine_line_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "machine_line_mapping")

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
                    UPPER(TRIM(country_name)) AS country_name_key,
                    UPPER(TRIM(year)) AS year_key,
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
                    'TMA' AS source
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
                    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL') AS source
                FROM volvo_sale_data_rows v
                WHERE v.upload_run_id = ?
                GROUP BY
                    TRIM(v.calendar),
                    TRIM(v.country),
                    TRIM(v.region),
                    TRIM(v.machine),
                    TRIM(v.machine_line),
                    TRIM(v.size_class),
                    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL')
            ),
            all_agg AS (
                SELECT * FROM tma_agg
                UNION ALL
                SELECT * FROM volvo_agg
            ),
            source_matrix_country_line AS (
                SELECT
                    UPPER(TRIM(country_name)) AS country_name_key,
                    UPPER(TRIM(machine_line_name)) AS machine_line_name_key,
                    MAX(
                        CASE
                            WHEN TRIM(COALESCE(crp_source, '')) <> '' THEN TRIM(crp_source)
                            ELSE NULL
                        END
                    ) AS crp_source
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(country_name, '')) <> ''
                  AND TRIM(COALESCE(machine_line_name, '')) <> ''
                GROUP BY
                    UPPER(TRIM(country_name)),
                    UPPER(TRIM(machine_line_name))
            ),
            source_matrix_machine_lines AS (
                SELECT
                    UPPER(TRIM(machine_line_name)) AS machine_line_name_key
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(machine_line_name, '')) <> ''
                GROUP BY UPPER(TRIM(machine_line_name))
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
                    COALESCE(g_code.country_grouping, g_name.country_grouping, '') AS country_grouping,
                    COALESCE(g_code.country_name, g_name.country_name, a.country_raw) AS country,
                    COALESCE(g_code.region, g_name.region, a.region_raw) AS region,
                    a.machine_line_code AS machine_line_code,
                    a.machine_line_name AS machine_line_name,
                    a.size_class AS size_class,
                    CASE
                        WHEN UPPER(TRIM(a.source)) = 'SAL' THEN 'VCE'
                        ELSE '#'
                    END AS brand_code,
                    CASE
                        WHEN UPPER(TRIM(a.source)) = 'TMA' THEN '#'
                        WHEN UPPER(TRIM(a.source)) = 'SAL'
                             AND TRIM(COALESCE(sm_country_line.crp_source, '')) <> '' THEN 'Y'
                        ELSE ''
                    END AS reporter_flag,
                    CASE
                        WHEN UPPER(TRIM(a.source)) = 'SAL'
                             AND TRIM(COALESCE(sm_country_line.crp_source, '')) <> '' THEN 'Y'
                        ELSE '#'
                    END AS vce_flag,
                    '#' AS pri_sec,
                    a.source AS source,
                    CASE
                        WHEN UPPER(TRIM(a.source)) = 'SAL'
                             AND TRIM(CAST(a.machine_line_code AS TEXT)) = '390' THEN 'Y'
                        WHEN UPPER(TRIM(a.source)) = 'SAL'
                             AND TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), '')) <> ''
                             AND sm.machine_line_name_key IS NULL THEN 'Y'
                        ELSE ''
                    END AS deletion_flag,
                    a.fid AS raw_fid
                FROM all_agg a
                LEFT JOIN gc_by_code g_code
                  ON UPPER(TRIM(a.end_country_code)) = g_code.country_code_key
                 AND UPPER(TRIM(a.year)) = g_code.year_key
                LEFT JOIN gc_by_name g_name
                  ON UPPER(TRIM(a.country_raw)) = g_name.country_name_key
                 AND UPPER(TRIM(a.year)) = g_name.year_key
                LEFT JOIN source_matrix_country_line sm_country_line
                  ON UPPER(TRIM(COALESCE(g_code.country_name, g_name.country_name, a.country_raw))) = sm_country_line.country_name_key
                 AND UPPER(TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), ''))) = sm_country_line.machine_line_name_key
                LEFT JOIN source_matrix_machine_lines sm
                  ON UPPER(TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), ''))) = sm.machine_line_name_key
                WHERE UPPER(TRIM(a.source)) <> 'SAL'
                   OR TRIM(COALESCE(sm_country_line.crp_source, '')) <> ''
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
                    frb.country_grouping AS country_grouping,
                    frb.country AS country,
                    frb.region AS region,
                    frb.machine_line_code AS machine_line_code,
                    frb.machine_line_name AS machine_line_name,
                    COALESCE(mlmm.artificial_machine_line, '') AS artificial_machine_line,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(frb.source, ''))) = 'TMA'
                         AND UPPER(TRIM(COALESCE(mlmm.artificial_machine_line, ''))) IN ('MINI', 'MIDI')
                        THEN mlmm.artificial_machine_line
                        ELSE frb.size_class
                    END AS size_class,
                    frb.brand_code AS brand_code,
                    frb.reporter_flag AS reporter_flag,
                    frb.vce_flag AS vce_flag,
                    frb.pri_sec AS pri_sec,
                    frb.source AS source,
                    frb.deletion_flag AS deletion_flag,
                    frb.raw_fid AS raw_fid
                FROM final_rows_base frb
                LEFT JOIN machine_line_mapping_matches mlmm
                  ON frb.base_row_id = mlmm.base_row_id
                 AND mlmm.match_rank = 1
            ),
            display_rows AS (
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
                    reporter_flag,
                    vce_flag,
                    pri_sec,
                    source,
                    deletion_flag,
                    SUM(COALESCE(raw_fid, 0)) AS raw_fid
                FROM final_rows
                GROUP BY
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
            latest_source_matrix_upload_run_id,
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
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/reports/oth-deletion-flag")
def get_oth_deletion_flag_report():
    conn = get_connection()
    cursor = conn.cursor()

    latest_oth_upload_run_id = _get_latest_success_upload_id(cursor, "oth_data")
    latest_group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country")
    latest_machine_line_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "machine_line_mapping")
    latest_brand_mapping_upload_run_id = _get_latest_success_upload_id(cursor, "brand_mapping")
    latest_source_matrix_upload_run_id = _get_latest_success_upload_id(cursor, "source_matrix")
    latest_reporter_list_upload_run_id = _get_latest_success_upload_id(cursor, "reporter_list")

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
                    UPPER(TRIM(country_name)) AS country_name_key,
                    UPPER(TRIM(machine_line_name)) AS machine_line_name_key,
                    UPPER(TRIM(primary_source)) AS primary_source_key,
                    UPPER(TRIM(secondary_source)) AS secondary_source_key
                FROM source_matrix_rows
                WHERE upload_run_id = ?
                  AND TRIM(COALESCE(country_name, '')) <> ''
                  AND TRIM(COALESCE(machine_line_name, '')) <> ''
            ),
            source_matrix_keys AS (
                SELECT
                    country_name_key,
                    machine_line_name_key
                FROM source_matrix_base
                GROUP BY
                    country_name_key,
                    machine_line_name_key
            ),
            source_matrix_source_flags AS (
                SELECT
                    country_name_key,
                    machine_line_name_key,
                    primary_source_key AS source_key,
                    'P' AS pri_sec
                FROM source_matrix_base
                WHERE TRIM(COALESCE(primary_source_key, '')) <> ''
                UNION ALL
                SELECT
                    country_name_key,
                    machine_line_name_key,
                    secondary_source_key AS source_key,
                    'S' AS pri_sec
                FROM source_matrix_base
                WHERE TRIM(COALESCE(secondary_source_key, '')) <> ''
            ),
            source_matrix_source_flags_dedup AS (
                SELECT
                    country_name_key,
                    machine_line_name_key,
                    source_key,
                    CASE
                        WHEN SUM(CASE WHEN pri_sec = 'P' THEN 1 ELSE 0 END) > 0 THEN 'P'
                        WHEN SUM(CASE WHEN pri_sec = 'S' THEN 1 ELSE 0 END) > 0 THEN 'S'
                        ELSE ''
                    END AS pri_sec
                FROM source_matrix_source_flags
                GROUP BY
                    country_name_key,
                    machine_line_name_key,
                    source_key
            )
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
                NULL AS ms_percent,
                CASE
                    WHEN TRIM(COALESCE(m.machine_line_code, '')) = '390' THEN 'Y'
                    WHEN TRIM(COALESCE(g.country_name, o.country, '')) <> ''
                     AND TRIM(COALESCE(m.machine_line_name, o.machine_line, '')) <> ''
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
                         AND UPPER(TRIM(COALESCE(rl.machine_line, ''))) = UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line, '')))
                         AND UPPER(TRIM(COALESCE(rl.brand_code, ''))) = UPPER(TRIM(COALESCE(b.brand_code, '')))
                         AND rl.upload_run_id = ?
                        WHERE sm.upload_run_id = ?
                          AND UPPER(TRIM(COALESCE(sm.country_name, ''))) = UPPER(TRIM(COALESCE(g.country_name, o.country, '')))
                          AND UPPER(TRIM(COALESCE(sm.machine_line_name, ''))) = UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line, '')))
                          AND TRIM(COALESCE(sm.crp_source, '')) <> ''
                    ) THEN 'Y'
                    ELSE ''
                END AS reporter_flag
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
            LEFT JOIN source_matrix_keys smk
                ON UPPER(TRIM(COALESCE(g.country_name, o.country))) = smk.country_name_key
               AND UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line))) = smk.machine_line_name_key
            LEFT JOIN source_matrix_source_flags_dedup smsf
                ON UPPER(TRIM(COALESCE(g.country_name, o.country))) = smsf.country_name_key
               AND UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line))) = smsf.machine_line_name_key
               AND UPPER(TRIM(COALESCE(o.source, ''))) = smsf.source_key
            WHERE o.upload_run_id = ?
            ORDER BY o.row_index ASC
        """, (
            latest_source_matrix_upload_run_id,
            latest_reporter_list_upload_run_id,
            latest_source_matrix_upload_run_id,
            latest_group_country_upload_run_id,
            latest_machine_line_mapping_upload_run_id,
            latest_brand_mapping_upload_run_id,
            latest_oth_upload_run_id,
        ))

        rows = [dict(row) for row in cursor.fetchall()]
        return {
            "row_count": len(rows),
            "rows": rows,
            "oth_upload_run_id": latest_oth_upload_run_id,
            "group_country_upload_run_id": latest_group_country_upload_run_id,
            "machine_line_mapping_upload_run_id": latest_machine_line_mapping_upload_run_id,
            "brand_mapping_upload_run_id": latest_brand_mapping_upload_run_id,
            "source_matrix_upload_run_id": latest_source_matrix_upload_run_id,
            "reporter_list_upload_run_id": latest_reporter_list_upload_run_id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/reports/p10-vce-non-vce")
def get_p10_vce_non_vce_report():
    combined = _get_crp_d1_combined_report_data(include_all_sal=False)
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
            row.get("year", ""),
            row.get("country_group_code", ""),
            row.get("country_grouping", ""),
            row.get("country", ""),
            row.get("region", ""),
            row.get("machine_line_code", ""),
            row.get("machine_line_name", ""),
            artificial_machine_line,
            p10_size_class,
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
