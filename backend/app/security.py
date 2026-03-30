import hashlib
import hmac
import secrets
from typing import Any

from fastapi import Header, HTTPException, status

from app.auth_database import get_auth_connection
from app.settings import is_auth_bypassed

PBKDF2_ITERATIONS = 120_000
DEV_USER = {
    "id": 0,
    "full_name": "Developer Mode",
    "email": "dev@local",
    "created_at": "",
}


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    derived_key = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        PBKDF2_ITERATIONS,
    )
    return f"{PBKDF2_ITERATIONS}${salt}${derived_key.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        iterations_text, salt, expected_hash = password_hash.split("$", 2)
        iterations = int(iterations_text)
    except ValueError:
        return False

    derived_key = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        iterations,
    )
    return hmac.compare_digest(derived_key.hex(), expected_hash)


def create_session(cursor, user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    cursor.execute(
        """
        INSERT INTO auth_sessions (user_id, token_hash)
        VALUES (?, ?)
        """,
        (user_id, token_hash),
    )
    return token


def delete_session(token: str) -> None:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    conn = get_auth_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM auth_sessions WHERE token_hash = ?", (token_hash,))
    conn.commit()
    conn.close()


def _read_bearer_token(authorization: str | None) -> str:
    if authorization is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization token",
        )

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header",
        )
    return token.strip()


def _load_user_by_token(token: str) -> dict[str, Any]:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    conn = get_auth_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT u.id, u.full_name, u.email, u.created_at
        FROM auth_sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token_hash = ?
        """,
        (token_hash,),
    )
    row = cursor.fetchone()
    conn.close()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired or invalid",
        )

    return dict(row)


def get_current_user(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    if is_auth_bypassed():
        if authorization:
            try:
                token = _read_bearer_token(authorization)
                return _load_user_by_token(token)
            except HTTPException:
                pass
        return DEV_USER

    token = _read_bearer_token(authorization)
    return _load_user_by_token(token)


def require_auth(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    if is_auth_bypassed():
        if authorization:
            try:
                token = _read_bearer_token(authorization)
                return _load_user_by_token(token)
            except HTTPException:
                pass
        return DEV_USER

    return get_current_user(authorization)
