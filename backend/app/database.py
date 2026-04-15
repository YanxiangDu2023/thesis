import re
import sqlite3
from pathlib import Path
from urllib.parse import unquote, urlparse

from app.settings import get_database_url

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SQLITE_PATH = BASE_DIR / "tmc.db"
_AUTOINCREMENT_PATTERN = re.compile(
    r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
    flags=re.IGNORECASE,
)


def _database_backend_from_url(database_url: str) -> str:
    normalized = database_url.strip().lower()
    if normalized.startswith("postgresql://") or normalized.startswith("postgres://"):
        return "postgres"
    return "sqlite"


def get_database_backend() -> str:
    return _database_backend_from_url(get_database_url())


def _resolve_sqlite_path(database_url: str) -> Path:
    if database_url.strip() == "":
        return DEFAULT_SQLITE_PATH

    if database_url.lower().startswith("sqlite://"):
        parsed = urlparse(database_url)
        raw_path = unquote(parsed.path or "")

        # Windows absolute path in sqlite URL may look like "/C:/path/to/file.db".
        if raw_path.startswith("/") and len(raw_path) > 2 and raw_path[2] == ":":
            raw_path = raw_path[1:]
        # Relative sqlite URL is usually written as sqlite:///./file.db.
        elif raw_path.startswith("/./"):
            raw_path = raw_path[1:]

        if raw_path == "":
            return DEFAULT_SQLITE_PATH

        candidate = Path(raw_path)
        return candidate if candidate.is_absolute() else (BASE_DIR / candidate)

    candidate = Path(database_url)
    return candidate if candidate.is_absolute() else (BASE_DIR / candidate)


def _adapt_sql_for_postgres(query: str) -> str:
    adapted = _AUTOINCREMENT_PATTERN.sub("BIGSERIAL PRIMARY KEY", query)
    adapted = re.sub(r"\bDATETIME\b", "TIMESTAMP", adapted, flags=re.IGNORECASE)
    adapted = adapted.replace("?", "%s")
    return adapted


class PostgresCursor:
    def __init__(self, cursor):
        self._cursor = cursor
        self._last_insert_pending = False
        self._lastrowid_cache = None

    def execute(self, query: str, params=None):
        sql = _adapt_sql_for_postgres(query)
        values = () if params is None else params
        stripped = sql.lstrip().upper()
        self._cursor.execute(sql, values)
        self._last_insert_pending = stripped.startswith("INSERT INTO")
        if not self._last_insert_pending:
            self._lastrowid_cache = None
        return self

    def executemany(self, query: str, seq_of_params):
        sql = _adapt_sql_for_postgres(query)
        self._cursor.executemany(sql, seq_of_params)
        self._last_insert_pending = False
        self._lastrowid_cache = None
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def lastrowid(self):
        if not self._last_insert_pending:
            return self._lastrowid_cache

        self._cursor.execute("SELECT LASTVAL() AS id")
        row = self._cursor.fetchone()
        self._lastrowid_cache = row["id"] if row else None
        self._last_insert_pending = False
        return self._lastrowid_cache

    def __getattr__(self, attr):
        return getattr(self._cursor, attr)


class PostgresConnection:
    def __init__(self, connection):
        self._connection = connection

    def cursor(self):
        return PostgresCursor(self._connection.cursor())

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self._connection.close()

    def __getattr__(self, attr):
        return getattr(self._connection, attr)


def _connect_postgres(database_url: str):
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL support requires 'psycopg'. Install backend dependencies before starting the app."
        ) from exc

    connection = psycopg.connect(database_url, row_factory=dict_row)
    return PostgresConnection(connection)


def get_connection():
    database_url = get_database_url()
    if _database_backend_from_url(database_url) == "postgres":
        return _connect_postgres(database_url)

    sqlite_path = _resolve_sqlite_path(database_url)
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    return conn


def get_table_columns(cursor, table_name: str) -> list[str]:
    if get_database_backend() == "postgres":
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row["column_name"] for row in cursor.fetchall()]

    cursor.execute(f"PRAGMA table_info({table_name})")
    columns: list[str] = []
    for row in cursor.fetchall():
        if isinstance(row, sqlite3.Row):
            columns.append(row["name"])
        elif isinstance(row, dict):
            columns.append(row.get("name", ""))
        else:
            columns.append(row[1])
    return [column for column in columns if column]


def table_exists(cursor, table_name: str) -> bool:
    if get_database_backend() == "postgres":
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = ?
            ) AS table_exists
            """,
            (table_name,),
        )
        row = cursor.fetchone()
        return bool(row["table_exists"]) if row else False

    cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    )
    return cursor.fetchone() is not None
