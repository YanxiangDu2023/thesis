import sqlite3
from pathlib import Path

AUTH_DB_PATH = Path(__file__).resolve().parents[1] / "auth.db"


def get_auth_connection():
    conn = sqlite3.connect(str(AUTH_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn
