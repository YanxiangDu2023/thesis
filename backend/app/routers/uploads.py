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


@router.get("/reports/crp-d1-combined")
def get_crp_d1_combined_report():
    conn = get_connection()
    cursor = conn.cursor()

    latest_tma_upload_run_id = _get_latest_success_upload_id(cursor, "tma_data")
    latest_volvo_upload_run_id = _get_latest_success_upload_id(cursor, "volvo_sale_data")
    latest_group_country_upload_run_id = _get_latest_success_upload_id(cursor, "group_country")

    missing_types = []
    if latest_tma_upload_run_id is None:
        missing_types.append("tma_data")
    if latest_volvo_upload_run_id is None:
        missing_types.append("volvo_sale_data")
    if latest_group_country_upload_run_id is None:
        missing_types.append("group_country")

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
            )
            SELECT
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
                    ELSE 'Y'
                END AS reporter_flag,
                '#' AS pri_sec,
                a.source AS source,
                CASE
                    WHEN TRIM(CAST(a.machine_line_code AS TEXT)) = '390' THEN 'Y'
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
        ))

        rows = [dict(row) for row in cursor.fetchall()]
        return {
            "row_count": len(rows),
            "rows": rows,
            "tma_upload_run_id": latest_tma_upload_run_id,
            "volvo_upload_run_id": latest_volvo_upload_run_id,
            "group_country_upload_run_id": latest_group_country_upload_run_id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

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
