# backend/routers/rules.py
import sqlite3
from fastapi import APIRouter, Depends, HTTPException

from backend.dependencies import get_db, get_current_user
from backend.schemas.rule import ResolveRequest
from modules.module_e.resolver import resolve_all, resolve

router = APIRouter()


@router.get("/topics")
def get_topics(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    rows = conn.execute(
        "SELECT rule_topic_id, topic_code, topic_name FROM rule_topics WHERE is_active = 1 ORDER BY topic_name"
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/anchors")
def get_anchors(
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    rows = conn.execute(
        "SELECT ub04_anchor_type_id, anchor_type_code, anchor_type_name FROM ub04_anchor_types ORDER BY anchor_type_name"
    ).fetchall()
    return [dict(r) for r in rows]


@router.post("/resolve")
def resolve_rules(
    body: ResolveRequest,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    if body.anchor_type_id and body.anchor_value:
        result = resolve(
            conn,
            body.rule_topic_id,
            body.query_date,
            body.query_date_type,
            body.anchor_type_id,
            body.anchor_value,
        )
    else:
        result = resolve_all(
            conn,
            body.rule_topic_id,
            body.query_date,
            body.query_date_type,
        )

    # Enrich rules with source evidence
    rules_list = []
    if isinstance(result, list):
        rules_list = result
    elif isinstance(result, dict) and "results" in result:
        rules_list = result.get("results") or []
    elif isinstance(result, dict):
        rules_list = [result]

    for rule in rules_list:
        if not isinstance(rule, dict):
            continue
        rv_id = rule.get("rule_version_id")
        if not rv_id:
            continue
        evidence = conn.execute("""
            SELECT
                sd.document_title,
                sd.source_url,
                sd.file_path_local,
                rel.page_number_start,
                rel.citation_text
            FROM rule_evidence_links rel
            JOIN source_documents sd
              ON sd.source_document_id = rel.source_document_id
            WHERE rel.rule_version_id = ?
            LIMIT 1
        """, (rv_id,)).fetchone()
        if evidence:
            rule["source_document_title"] = evidence[0]
            rule["source_url"] = evidence[1]
            rule["source_file_path"] = evidence[2]
            rule["source_page"] = evidence[3]
            rule["citation_text"] = evidence[4]

    return result


@router.get("/history/{rule_code}")
def get_rule_history(
    rule_code: str,
    conn: sqlite3.Connection = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    rows = conn.execute(
        """
        SELECT rv.rule_version_id, rv.version_number, rv.normalized_rule_text,
               rv.effective_start_date, rv.effective_end_date, rv.lifecycle_status,
               rv.is_published, rv.superseded_by_version_id, rv.created_at,
               ar.rule_code, ar.rule_name
        FROM rule_versions rv
        JOIN atomic_rules ar ON ar.atomic_rule_id = rv.atomic_rule_id
        WHERE ar.rule_code = ?
        ORDER BY rv.version_number DESC
        """,
        (rule_code,)
    ).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_code}' not found")
    return [dict(r) for r in rows]