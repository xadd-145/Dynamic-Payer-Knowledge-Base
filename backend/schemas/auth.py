# backend/routers/auth.py
import sqlite3
from datetime import datetime, timedelta, timezone

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, status
from jose import jwt

from backend.dependencies import get_db, SECRET_KEY, ALGORITHM
from backend.schemas.user import LoginRequest, TokenResponse

router = APIRouter()


def _verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def _create_token(username: str, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=8)
    payload = {
        "sub":  username,
        "role": role,
        "exp":  expire,
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


@router.post("/login", response_model=TokenResponse)
def login(body: LoginRequest, conn: sqlite3.Connection = Depends(get_db)):
    user = conn.execute(
        "SELECT username, password_hash, role, is_active FROM users WHERE username = ?",
        (body.username,)
    ).fetchone()

    if not user or not user["is_active"]:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid credentials")

    if not _verify_password(body.password, user["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid credentials")

    token = _create_token(user["username"], user["role"])
    return {"access_token": token, "token_type": "bearer", "role": user["role"]}