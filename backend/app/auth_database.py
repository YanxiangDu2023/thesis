import os
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
_configured_path = os.getenv("AUTH_DB_PATH", "auth.db").strip() or "auth.db"
AUTH_DB_PATH = Path(_configured_path)
if not AUTH_DB_PATH.is_absolute():
    AUTH_DB_PATH = BASE_DIR / AUTH_DB_PATH


def get_auth_connection():
    conn = sqlite3.connect(str(AUTH_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn
