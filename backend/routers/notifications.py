# backend/routers/notifications.py
import sqlite3
from fastapi import APIRouter, Depends

from backend.dependencies import get_db, get_current_user
from db_init import utc_now

router = APIRouter()


@router.get("/unread")
def get_unread(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    rows = conn.execute(
        "SELECT notification_id, message, rule_code, triggered_by, created_at "
        "FROM notifications WHERE is_read = 0 ORDER BY created_at DESC"
    ).fetchall()
    return {"count": len(rows), "notifications": [dict(r) for r in rows]}


@router.post("/read")
def mark_all_read(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    conn.execute(
        "UPDATE notifications SET is_read = 1 WHERE is_read = 0"
    )
    conn.commit()
    return {"status": "ok"}