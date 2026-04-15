import argparse
import os
import sqlite3
from pathlib import Path

import psycopg
from psycopg import sql


DEFAULT_SQLITE_PATH = Path(__file__).resolve().parents[1] / "tmc.db"
BATCH_SIZE = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy all application tables from SQLite to PostgreSQL."
    )
    parser.add_argument(
        "--sqlite-path",
        default=str(DEFAULT_SQLITE_PATH),
        help="Path to the SQLite source database file.",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="Target PostgreSQL DATABASE_URL.",
    )
    parser.add_argument(
        "--skip-empty",
        action="store_true",
        help="Skip truncate/insert for tables that have 0 source rows.",
    )
    return parser.parse_args()


def load_sqlite_tables(source_conn: sqlite3.Connection) -> list[str]:
    cursor = source_conn.cursor()
    cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    )
    return [row[0] for row in cursor.fetchall()]


def load_sqlite_columns(source_conn: sqlite3.Connection, table_name: str) -> list[str]:
    cursor = source_conn.cursor()
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return [row[1] for row in cursor.fetchall()]


def postgres_table_exists(target_conn, table_name: str) -> bool:
    with target_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
            """,
            (table_name,),
        )
        row = cursor.fetchone()
        return bool(row[0]) if row else False


def stream_sqlite_rows(
    source_conn: sqlite3.Connection,
    table_name: str,
    columns: list[str],
):
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    cursor = source_conn.cursor()
    cursor.execute(f'SELECT {quoted_columns} FROM "{table_name}"')
    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        yield rows


def migrate_table(source_conn: sqlite3.Connection, target_conn, table_name: str, skip_empty: bool) -> None:
    columns = load_sqlite_columns(source_conn, table_name)
    if not columns:
        print(f"[skip] {table_name}: no columns")
        return

    source_count_cursor = source_conn.cursor()
    source_count_cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
    source_row_count = int(source_count_cursor.fetchone()[0])

    if source_row_count == 0 and skip_empty:
        print(f"[skip] {table_name}: empty")
        return

    if not postgres_table_exists(target_conn, table_name):
        print(f"[skip] {table_name}: target table missing")
        return

    print(f"[table] {table_name}: {source_row_count} rows")

    with target_conn.cursor() as cursor:
        truncate_stmt = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
            sql.Identifier(table_name)
        )
        cursor.execute(truncate_stmt)

        insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )

        copied_count = 0
        for chunk in stream_sqlite_rows(source_conn, table_name, columns):
            cursor.executemany(insert_stmt, chunk)
            copied_count += len(chunk)

    target_conn.commit()
    print(f"[done] {table_name}: copied {copied_count} rows")


def main() -> None:
    args = parse_args()
    postgres_url = args.postgres_url
    if not postgres_url:
        raise SystemExit("Missing --postgres-url (or DATABASE_URL env var).")
    if not (postgres_url.startswith("postgres://") or postgres_url.startswith("postgresql://")):
        raise SystemExit("Target URL must be PostgreSQL.")

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite database not found: {sqlite_path}")

    source_conn = sqlite3.connect(str(sqlite_path))
    target_conn = psycopg.connect(postgres_url)

    try:
        for table_name in load_sqlite_tables(source_conn):
            migrate_table(source_conn, target_conn, table_name, args.skip_empty)
    finally:
        source_conn.close()
        target_conn.close()

    print("Migration completed.")


if __name__ == "__main__":
    main()
