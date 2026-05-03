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

from backend.utils.email_helper import (
    notify_sme_escalation,
    notify_admin_sme_response,
)

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
    notify_sme_escalation(
        fragment_id  = body.policy_fragment_id,
        escalated_by = current_user["username"],
        notes        = body.notes,
    )
    return {"status": "escalated", "policy_fragment_id": body.policy_fragment_id}


# ---------------------------------------------------------------------------
# ADMIN REJECT
# ---------------------------------------------------------------------------

@router.post("/reject")
def reject(
    body: RejectRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
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
    notify_admin_sme_response(
        fragment_id    = body.policy_fragment_id,
        recommendation = "approved",
        sme_user       = current_user["username"],
        notes          = body.notes,
    )
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
    notify_admin_sme_response(
        fragment_id    = body.policy_fragment_id,
        recommendation = "rejected",
        sme_user       = current_user["username"],
        notes          = body.notes,
    )
    return {"status": "sme_rejected", "policy_fragment_id": body.policy_fragment_id}


# ---------------------------------------------------------------------------
# ADMIN ACTIVITY LOG — NEW
# Returns all fragments that have been actioned (anything not pending)
# ---------------------------------------------------------------------------

@router.get("/activity-log")
def get_activity_log(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    rows = conn.execute("""
        SELECT
            pf.policy_fragment_id,
            pf.review_status   AS action,
            pf.reviewed_by,
            sd.document_title,
            pf.review_notes,
            pf.reviewed_at,
            pf.confidence_tier,
            pf.confidence_score,
            pf.page_number_start
        FROM policy_fragments pf
        JOIN source_documents sd
          ON sd.source_document_id = pf.source_document_id
        WHERE pf.review_status != 'pending'
        ORDER BY pf.reviewed_at DESC
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# SME ACTIVITY LOG — NEW
# Returns only this SME user's own recommendations
# ---------------------------------------------------------------------------

@router.get("/sme-activity-log")
def get_sme_activity_log(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("sme")),
):
    rows = conn.execute("""
        SELECT
            pf.policy_fragment_id,
            pf.review_status   AS action,
            pf.reviewed_by,
            sd.document_title,
            pf.review_notes,
            pf.reviewed_at,
            pf.confidence_tier,
            pf.confidence_score
        FROM policy_fragments pf
        JOIN source_documents sd
          ON sd.source_document_id = pf.source_document_id
        WHERE pf.review_status IN ('sme_approved', 'sme_rejected')
          AND pf.reviewed_by = ?
        ORDER BY pf.reviewed_at DESC
    """, (current_user["username"],)).fetchall()
    return [dict(r) for r in rows]


@router.post("/suggest")
def suggest_metadata(
    body: dict,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(require_role("admin")),
):
    import os, json
    from openai import OpenAI

    policy_fragment_id = body.get("policy_fragment_id")
    fragment = conn.execute(
        """SELECT pf.*, sd.document_title
           FROM policy_fragments pf
           JOIN source_documents sd ON sd.source_document_id = pf.source_document_id
           WHERE pf.policy_fragment_id = ?""",
        (policy_fragment_id,)
    ).fetchone()

    if not fragment:
        raise HTTPException(status_code=404, detail="Fragment not found")

    fragment = dict(fragment)

    topics = conn.execute(
        "SELECT topic_code FROM rule_topics WHERE is_active = 1 ORDER BY topic_code"
    ).fetchall()
    valid_topics = [t[0] for t in topics]

    prompt = f"""You are a New York Medicaid facility billing expert.
Analyze this policy fragment and suggest metadata for creating a billing rule.

Fragment text:
{fragment['fragment_text_raw']}

Source document: {fragment['document_title']}
Extracted effective date: {fragment['extracted_effective_start_date'] or 'Not detected'}

Valid topic codes: {', '.join(valid_topics)}

Respond ONLY with a JSON object, no markdown, no explanation:
{{
  "rule_code": "TOPIC-NNN format e.g. ER-001",
  "rule_name": "Short descriptive name max 10 words",
  "topic_code": "One of the valid topic codes above",
  "effective_start_date": "YYYY-MM-DD or empty string if unknown",
  "effective_end_date": "YYYY-MM-DD or empty string if not applicable",
  "notes": "One sentence explaining the rule or empty string"
}}"""

    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return {
                "status": "partial",
                "suggestion": {
                    "rule_code": "", "rule_name": "", "topic_code": "",
                    "effective_start_date": fragment["extracted_effective_start_date"] or "",
                    "effective_end_date": "", "notes": "",
                }
            }

        gpt = OpenAI(api_key=api_key)
        response = gpt.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=300,
        )

        raw = response.choices[0].message.content.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        suggestion = json.loads(raw)

        if suggestion.get("topic_code") not in valid_topics:
            suggestion["topic_code"] = ""

        if not suggestion.get("effective_start_date"):
            suggestion["effective_start_date"] = fragment["extracted_effective_start_date"] or ""

        return {"status": "ok", "suggestion": suggestion}

    except Exception:
        return {
            "status": "partial",
            "suggestion": {
                "rule_code": "", "rule_name": "", "topic_code": "",
                "effective_start_date": fragment["extracted_effective_start_date"] or "",
                "effective_end_date": "", "notes": "",
            }
        }