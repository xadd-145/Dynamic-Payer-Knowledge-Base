# backend/routers/fragments.py
import sqlite3
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from backend.dependencies import get_db, get_current_user, require_role
from modules.module_c.rule_publisher import publish_fragment, reject_fragment

router = APIRouter()


class ApproveRequest(BaseModel):
    policy_fragment_id: int
    rule_code: str
    rule_name: str
    topic_code: str
    effective_start_date: str
    effective_end_date: Optional[str] = None
    notes: Optional[str] = None


class RejectRequest(BaseModel):
    policy_fragment_id: int
    notes: Optional[str] = None


class EscalateRequest(BaseModel):
    policy_fragment_id: int
    notes: Optional[str] = None


@router.get("/tier2")
def get_tier2(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    rows = conn.execute(
        """SELECT pf.*, sd.document_title, sd.source_url
        FROM policy_fragments pf
        JOIN source_documents sd ON sd.source_document_id = pf.source_document_id
        WHERE pf.confidence_tier = 'TIER_2' AND pf.review_status = 'pending'
        ORDER BY pf.created_at DESC"""
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/tier3")
def get_tier3(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin", "sme")),
):
    rows = conn.execute(
        """SELECT pf.*, sd.document_title, sd.source_url
        FROM policy_fragments pf
        JOIN source_documents sd ON sd.source_document_id = pf.source_document_id
        WHERE pf.confidence_tier = 'TIER_3' AND pf.review_status = 'pending'
        ORDER BY pf.created_at DESC"""
    ).fetchall()
    return [dict(r) for r in rows]


@router.post("/approve")
def approve(
    body: ApproveRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin", "sme")),
):
    result = publish_fragment(
        conn,
        policy_fragment_id=body.policy_fragment_id,
        reviewed_by=current_user["username"],
        rule_code=body.rule_code,
        rule_name=body.rule_name,
        topic_code=body.topic_code,
        effective_start_date=body.effective_start_date,
        effective_end_date=body.effective_end_date,
        notes=body.notes,
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["detail"])
    return result


@router.post("/escalate")
def escalate(
    body: EscalateRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    now = __import__('db_init').utc_now()
    conn.execute(
        """UPDATE policy_fragments SET confidence_tier = 'TIER_3',
        review_notes = ?, reviewed_by = ?, reviewed_at = ?
        WHERE policy_fragment_id = ?""",
        (body.notes, current_user["username"], now, body.policy_fragment_id)
    )
    conn.commit()
    return {"status": "escalated", "policy_fragment_id": body.policy_fragment_id}


@router.post("/reject")
def reject(
    body: RejectRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin", "sme")),
):
    return reject_fragment(conn, body.policy_fragment_id,
                           current_user["username"], body.notes)