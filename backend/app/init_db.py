from app.database import get_connection

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

    conn.commit()
    conn.close()
