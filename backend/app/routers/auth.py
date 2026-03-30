from typing import Any
import re

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field

from app.auth_database import get_auth_connection
from app.security import (
    create_session,
    delete_session,
    get_current_user,
    hash_password,
    verify_password,
)

router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    full_name: str = Field(min_length=2, max_length=120)
    email: str = Field(min_length=5, max_length=255)
    password: str = Field(min_length=10, max_length=128)


class LoginRequest(BaseModel):
    email: str = Field(min_length=5, max_length=255)
    password: str = Field(min_length=10, max_length=128)


def _normalize_email(email: str) -> str:
    normalized = email.strip().lower()
    if "@" not in normalized or "." not in normalized.rsplit("@", 1)[-1]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Please enter a valid email address",
        )
    return normalized


def _validate_password(password: str) -> None:
    if len(password) < 10:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Password must be at least 10 characters long",
        )

    if not any(char.isupper() for char in password):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Password must include at least one uppercase letter",
        )

    if re.search(r"[^A-Za-z0-9]", password) is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Password must include at least one special character",
        )


def _serialize_auth_response(user: dict[str, Any], token: str) -> dict[str, Any]:
    return {
        "token": token,
        "user": {
            "id": user["id"],
            "full_name": user["full_name"],
            "email": user["email"],
            "created_at": user["created_at"],
        },
    }


@router.post("/register")
def register(payload: RegisterRequest):
    email = _normalize_email(payload.email)
    full_name = payload.full_name.strip()
    if len(full_name) < 2:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Please enter your full name",
        )
    _validate_password(payload.password)

    conn = get_auth_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE LOWER(email) = ?", (email,))
    existing_user = cursor.fetchone()
    if existing_user is not None:
        conn.close()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This email is already registered",
        )

    password_hash = hash_password(payload.password)
    cursor.execute(
        """
        INSERT INTO users (full_name, email, password_hash)
        VALUES (?, ?, ?)
        """,
        (full_name, email, password_hash),
    )
    user_id = cursor.lastrowid
    token = create_session(cursor, user_id)
    conn.commit()

    cursor.execute(
        """
        SELECT id, full_name, email, created_at
        FROM users
        WHERE id = ?
        """,
        (user_id,),
    )
    user = dict(cursor.fetchone())
    conn.close()
    return _serialize_auth_response(user, token)


@router.post("/login")
def login(payload: LoginRequest):
    email = _normalize_email(payload.email)

    conn = get_auth_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, full_name, email, password_hash, created_at
        FROM users
        WHERE LOWER(email) = ?
        """,
        (email,),
    )
    row = cursor.fetchone()
    if row is None or not verify_password(payload.password, row["password_hash"]):
        conn.close()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    user = dict(row)
    token = create_session(cursor, user["id"])
    conn.commit()
    conn.close()

    user.pop("password_hash", None)
    return _serialize_auth_response(user, token)


@router.get("/me")
def me(current_user: dict[str, Any] = Depends(get_current_user)):
    return {"user": current_user}


@router.post("/logout")
def logout(authorization: str | None = Header(default=None)):
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

    delete_session(token.strip())
    return {"message": "Logged out successfully"}
