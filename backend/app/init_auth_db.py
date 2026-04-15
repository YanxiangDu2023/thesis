from app.auth_database import get_auth_connection
from app.database import get_connection, table_exists


def _table_exists(cursor, table_name: str) -> bool:
    return table_exists(cursor, table_name)


def _migrate_legacy_auth_data() -> None:
    source_conn = get_connection()
    source_cursor = source_conn.cursor()

    has_users = _table_exists(source_cursor, "users")
    has_sessions = _table_exists(source_cursor, "auth_sessions")
    if not has_users and not has_sessions:
        source_conn.close()
        return

    auth_conn = get_auth_connection()
    auth_cursor = auth_conn.cursor()
    auth_cursor.execute("SELECT COUNT(*) AS count FROM users")
    user_count = auth_cursor.fetchone()["count"]
    auth_cursor.execute("SELECT COUNT(*) AS count FROM auth_sessions")
    session_count = auth_cursor.fetchone()["count"]

    if user_count == 0 and has_users:
        source_cursor.execute(
            """
            SELECT id, full_name, email, password_hash, created_at
            FROM users
            ORDER BY id ASC
            """
        )
        for row in source_cursor.fetchall():
            auth_cursor.execute(
                """
                INSERT INTO users (id, full_name, email, password_hash, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["full_name"],
                    row["email"],
                    row["password_hash"],
                    row["created_at"],
                ),
            )

    if session_count == 0 and has_sessions:
        source_cursor.execute(
            """
            SELECT id, user_id, token_hash, created_at
            FROM auth_sessions
            ORDER BY id ASC
            """
        )
        for row in source_cursor.fetchall():
            auth_cursor.execute(
                """
                INSERT INTO auth_sessions (id, user_id, token_hash, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["user_id"],
                    row["token_hash"],
                    row["created_at"],
                ),
            )

    auth_conn.commit()
    auth_conn.close()
    source_conn.close()


def init_auth_db():
    conn = get_auth_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        full_name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS auth_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token_hash TEXT NOT NULL UNIQUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)

    conn.commit()
    conn.close()

    # Preserve any accounts/sessions created before auth was split from tmc.db.
    _migrate_legacy_auth_data()
