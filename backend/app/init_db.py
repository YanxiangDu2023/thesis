from app.database import get_connection, get_table_columns

def _ensure_column(cursor, table_name: str, column_name: str, column_type: str):
    existing_columns = set(get_table_columns(cursor, table_name))
    if column_name not in existing_columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

def _ensure_oth_data_schema(cursor):
    columns = get_table_columns(cursor, "oth_data_rows")
    if not columns:
        return

    has_legacy_placeholders = "empty_col_1" in columns or "empty_col_2" in columns
    if not has_legacy_placeholders:
        return

    cursor.execute("DROP TABLE IF EXISTS oth_data_rows__legacy")
    cursor.execute("ALTER TABLE oth_data_rows RENAME TO oth_data_rows__legacy")

    cursor.execute("""
    CREATE TABLE oth_data_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        year TEXT,
        source TEXT,
        brand_name TEXT,
        machine_line TEXT,
        country TEXT,
        size_class TEXT,
        quantity TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
    )
    """)

    cursor.execute("""
        INSERT INTO oth_data_rows (
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
        )
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
        FROM oth_data_rows__legacy
    """)

    cursor.execute("DROP TABLE oth_data_rows__legacy")


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
        artificial_machine_line TEXT,
        brand_name TEXT,
        brand_code TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
    )
    """)
    _ensure_column(cursor, "reporter_list_rows", "artificial_machine_line", "TEXT")

     
     
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS source_matrix_rows (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      upload_run_id INTEGER NOT NULL,
      row_index INTEGER,
      country_grouping TEXT,
      country_name TEXT,
      machine_line_code TEXT,
      machine_line_name TEXT,
      artificial_machine_line TEXT,
      primary_source TEXT,
      secondary_source TEXT,
      crp_source TEXT,
      change_indicator TEXT,
      FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")
    _ensure_column(cursor, "source_matrix_rows", "artificial_machine_line", "TEXT")
    
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
        size_class TEXT,
        artificial_machine_line TEXT,
        position TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")
    _ensure_column(cursor, "machine_line_mapping_rows", "size_class", "TEXT")
    _ensure_column(cursor, "machine_line_mapping_rows", "artificial_machine_line", "TEXT")
    _ensure_column(cursor, "machine_line_mapping_rows", "position", "TEXT")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS oth_data_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        year TEXT,
        source TEXT,
        brand_name TEXT,
        machine_line TEXT,
        country TEXT,
        size_class TEXT,
        quantity TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")
    _ensure_oth_data_schema(cursor)

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
    CREATE TABLE IF NOT EXISTS split_manual_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_run_id INTEGER NOT NULL,
        row_index INTEGER,
        case_type TEXT,
        row_type TEXT,
        year TEXT,
        country_grouping TEXT,
        country TEXT,
        region TEXT,
        machine_line TEXT,
        artificial_machine_line TEXT,
        brand_code TEXT,
        reporter_flag TEXT,
        source TEXT,
        pri_sec TEXT,
        size_class TEXT,
        before_split_fid_lt_10t TEXT,
        copy_fid_lt_10t TEXT,
        after_split_fid_lt_6t TEXT,
        after_split_fid_6_10t TEXT,
        after_split_fid_target_three TEXT,
        tm_non_vce_lt_6t TEXT,
        tm_non_vce_6_10t TEXT,
        tm_non_vce_target_three TEXT,
        resplit TEXT,
        after_resplit_fid_lt_6t TEXT,
        after_resplit_fid_6_10t TEXT,
        after_resplit_fid_target_three TEXT,
        before_after_difference TEXT,
        reference_level TEXT,
        split_ratio TEXT,
        FOREIGN KEY (upload_run_id) REFERENCES upload_runs(id)
)
""")

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
    CREATE INDEX IF NOT EXISTS idx_oth_data_upload_run_row_index
    ON oth_data_rows(upload_run_id, row_index)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_group_country_upload_run_country_year
    ON group_country_rows(upload_run_id, country_code, year)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_machine_line_mapping_upload_run_name_code
    ON machine_line_mapping_rows(upload_run_id, machine_line_name, machine_line_code)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_brand_mapping_upload_run_brand_name
    ON brand_mapping_rows(upload_run_id, brand_name)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_control_report_clean_rows_control_run_row_index
    ON control_report_clean_rows(control_run_id, row_index)
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS report_run_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_key TEXT NOT NULL,
        triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_report_run_history_key_time
    ON report_run_history(report_key, triggered_at DESC, id DESC)
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS p00_report_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_key TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        row_count INTEGER,
        status TEXT,
        message TEXT,
        meta_json TEXT
    )
    """)
    _ensure_column(cursor, "p00_report_runs", "meta_json", "TEXT")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS p00_report_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_run_id INTEGER NOT NULL,
        row_index INTEGER,
        row_json TEXT NOT NULL,
        FOREIGN KEY (report_run_id) REFERENCES p00_report_runs(id)
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_p00_report_runs_key_time
    ON p00_report_runs(report_key, created_at DESC, id DESC)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_p00_report_rows_run_row
    ON p00_report_rows(report_run_id, row_index, id)
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS excavators_split_case_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_type TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        row_count INTEGER,
        status TEXT,
        message TEXT,
        meta_json TEXT
    )
    """)
    _ensure_column(cursor, "excavators_split_case_runs", "meta_json", "TEXT")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS excavators_split_case_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_run_id INTEGER NOT NULL,
        section TEXT NOT NULL,
        row_index INTEGER,
        row_json TEXT NOT NULL,
        FOREIGN KEY (report_run_id) REFERENCES excavators_split_case_runs(id)
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_excavators_split_case_runs_type_time
    ON excavators_split_case_runs(case_type, status, created_at DESC, id DESC)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_excavators_split_case_rows_run_section_row
    ON excavators_split_case_rows(report_run_id, section, row_index, id)
    """)

    conn.commit()
    conn.close()
