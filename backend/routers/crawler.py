# backend/routers/crawler.py
import sqlite3
from fastapi import APIRouter, Depends
from backend.dependencies import get_db, require_role
from db_init import utc_now

router = APIRouter()


@router.post("/run")
def trigger_crawl(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    now = utc_now()
    run_id = conn.execute(
        """INSERT INTO crawler_runs
        (started_at, completed_at, documents_found, documents_added,
         documents_skipped, documents_failed, run_status, notes)
        VALUES (?, ?, 1, 0, 1, 0, 'completed', 'Mock run — real crawler not yet wired')""",
        (now, now)
    ).lastrowid
    conn.commit()
    return {"status": "completed", "run_id": run_id}


@router.get("/status")
def crawler_status(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    row = conn.execute(
        "SELECT * FROM crawler_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return {"status": "never_run"}
    return dict(row)