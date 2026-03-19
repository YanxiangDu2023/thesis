from app.database import get_connection

def _ensure_column(cursor, table_name: str, column_name: str, column_type: str):
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    if column_name not in existing_columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS upload_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        matrix_type TEXT NOT NULL,
        original_file_name TEXT NOT NULL,
        stored_file_name TEXT NOT NULL,
        stored_path TEXT NOT NULL,
        uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        row_count INTEGER,
        status TEXT,
        message TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reporter_list_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        calendar TEXT,
        source TEXT,
        source_code TEXT,
        machine_line TEXT,
        machine_code TEXT,
        brand_name TEXT,
        brand_code TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
    )
    """)

     
     
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS source_matrix_rows (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      upload_run_id INTEGER NOT NULL,
      row_index INTEGER,
      country_grouping TEXT,
      country_name TEXT,
      machine_line_code TEXT,
      machine_line_name TEXT,
      primary_source TEXT,
      secondary_source TEXT,
      crp_source TEXT,
      change_indicator TEXT,
      FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS size_class_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        calendar TEXT,
        source TEXT,
        source_code TEXT,
        machine_line TEXT,
        machine_code TEXT,
        brand_name TEXT,
        brand_code TEXT,
        size_class TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS brand_mapping_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        brand_name TEXT,
        brand_code TEXT,
        deletion_indicator TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS group_country_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        year TEXT,
        group_code TEXT,
        country_code TEXT,
        country_name TEXT,
        country_grouping TEXT,
        region TEXT,
        market_area TEXT,
        market_area_code TEXT,
        change_indicator TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")

    # Backward-compatible migration for existing local DBs.
    _ensure_column(cursor, "group_country_rows", "year", "TEXT")
    _ensure_column(cursor, "group_country_rows", "group_code", "TEXT")
    _ensure_column(cursor, "group_country_rows", "market_area_code", "TEXT")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS machine_line_mapping_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        machine_line_name TEXT,
        machine_line_code TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS oth_data_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        year TEXT,
        source TEXT,
        brand_name TEXT,
        machine_line TEXT,
        empty_col_1 TEXT,
        country TEXT,
        empty_col_2 TEXT,
        size_class TEXT,
        quantity TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS volvo_sale_data_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        calendar TEXT,
        region TEXT,
        market TEXT,
        country TEXT,
        machine TEXT,
        machine_line TEXT,
        size_class TEXT,
        brand_owner_code TEXT,
        brand_owner TEXT,
        brand TEXT,
        brand_nationality TEXT,
        source TEXT,
        fid TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")
    _ensure_column(cursor, "volvo_sale_data_rows", "brand_owner_code", "TEXT")
    _ensure_column(cursor, "volvo_sale_data_rows", "brand_owner", "TEXT")
    _ensure_column(cursor, "volvo_sale_data_rows", "brand_nationality", "TEXT")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tma_data_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        year TEXT,
        geographical_region TEXT,
        geographical_market_area TEXT,
        end_country TEXT,
        end_country_code TEXT,
        machine_family TEXT,
        machine_line TEXT,
        machine_line_code TEXT,
        size_class TEXT,
        size_class_mapping TEXT,
        total_market_fid_sales TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")
    _ensure_column(cursor, "tma_data_rows", "machine_line_code", "TEXT")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS control_report_clean_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        oth_upload_run_id INTEGER NOT NULL,
        group_country_upload_run_id INTEGER NOT NULL,
        machine_line_mapping_upload_run_id INTEGER NOT NULL,
        brand_mapping_upload_run_id INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        row_count INTEGER,
        status TEXT,
        message TEXT,
        FOREIGN KEY (oth_upload_run_id) REFERENCES upload_runs(id),
        FOREIGN KEY (group_country_upload_run_id) REFERENCES upload_runs(id),
        FOREIGN KEY (machine_line_mapping_upload_run_id) REFERENCES upload_runs(id),
        FOREIGN KEY (brand_mapping_upload_run_id) REFERENCES upload_runs(id)
)
""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS control_report_clean_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        control_run_id INTEGER NOT NULL,
        row_index INTEGER,
        year TEXT,
        source TEXT,
        country_code TEXT,
        country TEXT,
        country_grouping TEXT,
        region TEXT,
        market_area TEXT,
        machine_line_name TEXT,
        machine_line_code TEXT,
        brand_name TEXT,
        brand_code TEXT,
        size_class_flag TEXT,
        fid TEXT,
        ms_percent TEXT,
        FOREIGN KEY (control_run_id) REFERENCES control_report_clean_runs(id)
)
""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS crp_tma_report_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tma_upload_run_id INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        row_count INTEGER,
        status TEXT,
        message TEXT,
        FOREIGN KEY (tma_upload_run_id) REFERENCES upload_runs(id)
)
""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS crp_tma_report_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_run_id INTEGER NOT NULL,
        row_index INTEGER,
        year TEXT,
        geographical_region TEXT,
        geographical_market_area TEXT,
        end_country_code TEXT,
        country TEXT,
        machine_line TEXT,
        machine_line_code TEXT,
        size_class_mapping TEXT,
        fid_sum REAL,
        source TEXT,
        FOREIGN KEY (report_run_id) REFERENCES crp_tma_report_runs(id)
)
""")
    _ensure_column(cursor, "crp_tma_report_rows", "year", "TEXT")
    _ensure_column(cursor, "crp_tma_report_rows", "geographical_region", "TEXT")
    _ensure_column(cursor, "crp_tma_report_rows", "geographical_market_area", "TEXT")
    _ensure_column(cursor, "crp_tma_report_rows", "end_country_code", "TEXT")
    _ensure_column(cursor, "crp_tma_report_rows", "machine_line_code", "TEXT")
    _ensure_column(cursor, "crp_tma_report_rows", "source", "TEXT")

    conn.commit()
    conn.close()
