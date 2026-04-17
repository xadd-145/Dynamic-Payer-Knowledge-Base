# backend/dependencies.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import sqlite3
from typing import Generator

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt

from db_init import get_connection

# ---------------------------------------------------------------------------
# JWT CONFIG
# ---------------------------------------------------------------------------
SECRET_KEY = "dpkb-secret-key-change-in-production"
ALGORITHM  = "HS256"

security = HTTPBearer()


# ---------------------------------------------------------------------------
# DB DEPENDENCY
# ---------------------------------------------------------------------------
def get_db() -> Generator:
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# AUTH DEPENDENCY
# ---------------------------------------------------------------------------
def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str     = payload.get("role")
        if username is None or role is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
            )
        return {"username": username, "role": role}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


# ---------------------------------------------------------------------------
# ROLE GUARD
# ---------------------------------------------------------------------------
def require_role(*allowed_roles: str):
    """
    Usage: Depends(require_role("admin", "sme"))
    """
    def _check(current_user: dict = Depends(get_current_user)) -> dict:
        if current_user["role"] not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{current_user['role']}' is not authorized for this action.",
            )
        return current_user
    return _check