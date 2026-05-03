# backend/routers/fragments.py
"""
Fragment review endpoints.

Governance model:
  All pipeline fragments → Admin pending queue (all tiers)
  Admin: Approve & Publish | Reject | Escalate to SME
  SME: sees ONLY Admin-manually-escalated fragments
  SME: Recommend Approval | Recommend Rejection (never publishes directly)
  Admin: sees SME responses, makes final publish/reject call
"""

import sqlite3
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from backend.dependencies import get_db, require_role
from modules.module_c.rule_publisher import publish_fragment, reject_fragment

router = APIRouter()


# ---------------------------------------------------------------------------
# REQUEST MODELS
# ---------------------------------------------------------------------------

class ApproveRequest(BaseModel):
    policy_fragment_id:   int
    rule_code:            str
    rule_name:            str
    topic_code:           str
    effective_start_date: str
    effective_end_date:   Optional[str] = None
    notes:                Optional[str] = None


class RejectRequest(BaseModel):
    policy_fragment_id: int
    notes:              Optional[str] = None


class EscalateRequest(BaseModel):
    policy_fragment_id: int
    notes:              Optional[str] = None


class SMEReviewRequest(BaseModel):
    policy_fragment_id: int
    notes:              Optional[str] = None


# ---------------------------------------------------------------------------
# ADMIN QUEUE — all pending fragments, all tiers, TIER_1 first
# ---------------------------------------------------------------------------

@router.get("/tier2")
def get_admin_queue(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    """
    Admin review queue.
    Returns ALL pending fragments regardless of tier.
    TIER_1 shown first (highest confidence = highest priority).
    Route kept as /tier2 to avoid breaking existing frontend calls.
    """
    rows = conn.execute("""
        SELECT pf.*, sd.document_title, sd.source_url
        FROM policy_fragments pf
        JOIN source_documents sd
          ON sd.source_document_id = pf.source_document_id
        WHERE pf.review_status = 'pending'
        ORDER BY
            CASE pf.confidence_tier
                WHEN 'TIER_1' THEN 1
                WHEN 'TIER_2' THEN 2
                WHEN 'TIER_3' THEN 3
                ELSE 4
            END,
            pf.confidence_score DESC,
            pf.created_at DESC
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# SME QUEUE — only Admin-manually-escalated fragments
# ---------------------------------------------------------------------------

@router.get("/tier3")
def get_sme_queue(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin", "sme")),
):
    """
    SME review queue.
    Shows ONLY fragments where Admin clicked Escalate to SME.
    review_status = 'escalated' is set ONLY by the /escalate endpoint.
    Pipeline never sets escalated. Fragment_writer always writes pending.
    """
    rows = conn.execute("""
        SELECT pf.*, sd.document_title, sd.source_url
        FROM policy_fragments pf
        JOIN source_documents sd
          ON sd.source_document_id = pf.source_document_id
        WHERE pf.review_status = 'escalated'
        ORDER BY
            CASE pf.confidence_tier
                WHEN 'TIER_2' THEN 1
                WHEN 'TIER_3' THEN 2
                ELSE 3
            END,
            pf.confidence_score DESC,
            pf.created_at DESC
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# ADMIN SME RESPONSES — fragments SME already reviewed
# ---------------------------------------------------------------------------

@router.get("/sme-responses")
def get_sme_responses(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    """
    Admin view of SME-reviewed fragments.
    Admin remains final authority — must approve or reject after SME input.
    """
    rows = conn.execute("""
        SELECT pf.*, sd.document_title, sd.source_url
        FROM policy_fragments pf
        JOIN source_documents sd
          ON sd.source_document_id = pf.source_document_id
        WHERE pf.review_status IN ('sme_approved', 'sme_rejected')
        ORDER BY pf.reviewed_at DESC, pf.created_at DESC
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# ADMIN APPROVE & PUBLISH
# ---------------------------------------------------------------------------

@router.post("/approve")
def approve(
    body: ApproveRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    """
    Admin approves and publishes a fragment.
    Admin only — SME cannot publish directly.
    """
    result = publish_fragment(
        conn,
        policy_fragment_id   = body.policy_fragment_id,
        reviewed_by          = current_user["username"],
        rule_code            = body.rule_code,
        rule_name            = body.rule_name,
        topic_code           = body.topic_code,
        effective_start_date = body.effective_start_date,
        effective_end_date   = body.effective_end_date,
        notes                = body.notes,
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["detail"])
    return result


# ---------------------------------------------------------------------------
# ADMIN ESCALATE TO SME
# ---------------------------------------------------------------------------

@router.post("/escalate")
def escalate(
    body: EscalateRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    """
    Admin manually escalates a fragment to SME.
    This is the ONLY way a fragment gets review_status = 'escalated'.
    Pipeline never sets escalated. Fragment_writer always writes pending.
    TIER_1 fragments are downgraded to TIER_2 on escalation
    because TIER_1 is supposed to be strong — Admin escalating it
    signals uncertainty, so it becomes a TIER_2 SME candidate.
    """
    now = __import__('db_init').utc_now()

    conn.execute("""
        UPDATE policy_fragments
        SET confidence_tier = CASE
                WHEN confidence_tier = 'TIER_1' THEN 'TIER_2'
                ELSE confidence_tier
            END,
            review_status = 'escalated',
            review_notes  = ?,
            reviewed_by   = ?,
            reviewed_at   = ?
        WHERE policy_fragment_id = ?
    """, (body.notes, current_user["username"], now, body.policy_fragment_id))

    if conn.total_changes == 0:
        raise HTTPException(status_code=404, detail="Fragment not found.")

    conn.commit()
    return {"status": "escalated", "policy_fragment_id": body.policy_fragment_id}


# ---------------------------------------------------------------------------
# ADMIN REJECT (from pending queue or after SME response)
# ---------------------------------------------------------------------------

@router.post("/reject")
def reject(
    body: RejectRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    """
    Admin rejects a fragment.
    Stays in DB as rejected for audit trail.
    Admin only.
    """
    return reject_fragment(
        conn,
        body.policy_fragment_id,
        current_user["username"],
        body.notes,
    )


# ---------------------------------------------------------------------------
# SME RECOMMEND APPROVAL
# ---------------------------------------------------------------------------

@router.post("/sme-approve")
def sme_approve(
    body: SMEReviewRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("sme")),
):
    """
    SME recommends approval.
    Does NOT publish. Returns fragment to Admin SME Responses tab.
    Admin must approve & publish after seeing SME recommendation.
    """
    now = __import__('db_init').utc_now()

    conn.execute("""
        UPDATE policy_fragments
        SET review_status = 'sme_approved',
            review_notes  = ?,
            reviewed_by   = ?,
            reviewed_at   = ?
        WHERE policy_fragment_id = ?
          AND review_status = 'escalated'
    """, (body.notes, current_user["username"], now, body.policy_fragment_id))

    if conn.total_changes == 0:
        raise HTTPException(
            status_code=400,
            detail="Fragment not found or not currently escalated."
        )

    conn.commit()
    return {"status": "sme_approved", "policy_fragment_id": body.policy_fragment_id}


# ---------------------------------------------------------------------------
# SME RECOMMEND REJECTION
# ---------------------------------------------------------------------------

@router.post("/sme-reject")
def sme_reject(
    body: SMEReviewRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("sme")),
):
    """
    SME recommends rejection.
    Does NOT finalize rejection. Returns fragment to Admin SME Responses tab.
    Admin must confirm rejection.
    """
    now = __import__('db_init').utc_now()

    conn.execute("""
        UPDATE policy_fragments
        SET review_status = 'sme_rejected',
            review_notes  = ?,
            reviewed_by   = ?,
            reviewed_at   = ?
        WHERE policy_fragment_id = ?
          AND review_status = 'escalated'
    """, (body.notes, current_user["username"], now, body.policy_fragment_id))

    if conn.total_changes == 0:
        raise HTTPException(
            status_code=400,
            detail="Fragment not found or not currently escalated."
        )

    conn.commit()
    return {"status": "sme_rejected", "policy_fragment_id": body.policy_fragment_id}