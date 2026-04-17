# modules/module_e/resolver.py
from __future__ import annotations

import json
import sqlite3
import traceback
from datetime import datetime, timezone
from typing import Any


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _get_topic_code(conn: sqlite3.Connection, rule_topic_id: int) -> str:
    row = conn.execute(
        "SELECT topic_code FROM rule_topics WHERE rule_topic_id = ?",
        (rule_topic_id,),
    ).fetchone()
    return row["topic_code"] if row else str(rule_topic_id)


def _validate_inputs(conn, rule_topic_id, query_date, query_date_type,
                     anchor_type_id, anchor_value):
    if not isinstance(rule_topic_id, int):
        raise ValueError("rule_topic_id must be an integer")
    if not isinstance(query_date, str) or len(query_date) != 10:
        raise ValueError("query_date must be a YYYY-MM-DD string")
    try:
        datetime.strptime(query_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("query_date must be a valid YYYY-MM-DD date") from exc
    if query_date_type not in ("date_of_service", "date_of_discharge"):
        raise ValueError("query_date_type must be 'date_of_service' or 'date_of_discharge'")
    if not conn.execute(
        "SELECT 1 FROM rule_topics WHERE rule_topic_id = ?", (rule_topic_id,)
    ).fetchone():
        raise ValueError(f"rule_topic_id {rule_topic_id} does not exist")
    if anchor_type_id is None and anchor_value is not None:
        raise ValueError("anchor_value cannot be provided without anchor_type_id")
    if anchor_type_id is not None:
        if not conn.execute(
            "SELECT 1 FROM ub04_anchor_types WHERE ub04_anchor_type_id = ?",
            (anchor_type_id,)
        ).fetchone():
            raise ValueError(f"anchor_type_id {anchor_type_id} does not exist")


def _candidate_query(anchor_type_id, anchor_value):
    """
    Locked resolver rules:
    - WHERE is_published = 1 AND lifecycle_status = 'published'
    - effective_start_date <= query_date
    - effective_end_date IS NULL OR effective_end_date >= query_date
    - ORDER BY: specificity DESC, exception_flag DESC, version_number DESC
    """
    params = []

    sql = """
        SELECT
            rv.rule_version_id,
            rv.atomic_rule_id,
            ar.rule_code,
            ar.rule_name                                          AS rule_title,
            ar.rule_topic_id,
            ('v' || CAST(rv.version_number AS TEXT))             AS version_label,
            rv.version_number,
            rv.normalized_rule_text,
            rv.normalized_rule_text                              AS display_rule_text,
            rv.effective_start_date,
            rv.effective_end_date,
            ar.specificity                                        AS specificity_score,
            rv.exception_flag,
            NULL                                                  AS resolver_priority,
            NULL                                                  AS change_type,
            rv.superseded_by_version_id                          AS supersedes_rule_version_id,
            CASE WHEN rv.superseded_by_version_id IS NOT NULL
                 THEN 1 ELSE 0 END                               AS is_superseded
        FROM rule_versions rv
        JOIN atomic_rules ar ON rv.atomic_rule_id = ar.atomic_rule_id
    """

    if anchor_type_id is not None and anchor_value is not None:
        sql += """
        JOIN rule_version_anchors rva ON rv.rule_version_id = rva.rule_version_id
        JOIN ub04_anchor_codes uac ON rva.ub04_anchor_code_id = uac.ub04_anchor_code_id
        """

    sql += """
        WHERE rv.is_published = 1
          AND rv.lifecycle_status = 'published'
          AND ar.rule_topic_id = ?
          AND rv.effective_start_date <= ?
          AND (rv.effective_end_date IS NULL OR rv.effective_end_date >= ?)
    """
    params.extend(["__RULE_TOPIC_ID__", "__QUERY_DATE__", "__QUERY_DATE__"])

    if anchor_type_id is not None and anchor_value is not None:
        sql += """
          AND uac.ub04_anchor_type_id = ?
          AND uac.anchor_code = ?
        """
        params.extend([anchor_type_id, anchor_value])

    sql += """
        ORDER BY
            ar.specificity DESC,
            rv.exception_flag DESC,
            rv.version_number DESC,
            rv.rule_version_id ASC
    """
    return sql, params


def _serialize_candidate_ids(rows):
    return json.dumps([row["rule_version_id"] for row in rows])


def _make_decision_trace(status, rule_topic_id, query_date, query_date_type,
                         anchor_type_id, anchor_value, candidates, winner,
                         note=None, error_detail=None):
    payload = {
        "resolution_status": status,
        "rule_topic_id": rule_topic_id,
        "query_date": query_date,
        "query_date_type": query_date_type,
        "anchor_type_id": anchor_type_id,
        "anchor_value": anchor_value,
        "candidate_rule_version_ids": [r["rule_version_id"] for r in candidates],
        "tie_break_order": ["specificity DESC", "exception_flag DESC", "version_number DESC"],
        "winner_rule_version_id": winner["rule_version_id"] if winner else None,
        "winner_specificity_score": winner["specificity_score"] if winner else None,
        "winner_exception_flag": winner["exception_flag"] if winner else None,
        "winner_version_number": winner["version_number"] if winner else None,
        "note": note,
        "error_detail": error_detail,
    }
    return json.dumps(payload, ensure_ascii=False)


def log_resolution(conn, rule_topic_id, query_date, query_date_type,
                   candidates, winner, status, decision_trace,
                   anchor_type_id=None, anchor_value=None):
    topic_code = _get_topic_code(conn, rule_topic_id)

    anchor_type_code = None
    if anchor_type_id is not None:
        row = conn.execute(
            "SELECT anchor_type_code FROM ub04_anchor_types WHERE ub04_anchor_type_id = ?",
            (anchor_type_id,)
        ).fetchone()
        if row:
            anchor_type_code = row["anchor_type_code"]

    conn.execute(
        """
        INSERT INTO resolution_logs (
            query_topic_code,
            query_date,
            query_date_type,
            query_anchor_type_code,
            query_anchor_value,
            candidate_rule_version_ids,
            winning_rule_version_id,
            resolution_status,
            decision_trace,
            queried_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            topic_code,
            query_date,
            query_date_type,
            anchor_type_code,
            anchor_value,
            _serialize_candidate_ids(candidates),
            winner["rule_version_id"] if winner else None,
            status,
            decision_trace,
            utc_now_str(),
        ),
    )
    conn.commit()


def resolve(conn, rule_topic_id, query_date, query_date_type,
            anchor_type_id=None, anchor_value=None):
    """
    Deterministic resolver. Returns one of: resolved, no_match, ambiguous, error.
    Never raises to caller.
    """
    try:
        if anchor_type_id is not None and (
            anchor_value is None or not str(anchor_value).strip()
        ):
            raise ValueError("anchor_value is required when anchor_type_id is provided")

        _validate_inputs(conn, rule_topic_id, query_date, query_date_type,
                         anchor_type_id, anchor_value)

        sql, raw_params = _candidate_query(anchor_type_id, anchor_value)
        params = []
        for item in raw_params:
            if item == "__RULE_TOPIC_ID__":
                params.append(rule_topic_id)
            elif item == "__QUERY_DATE__":
                params.append(query_date)
            else:
                params.append(item)

        candidates = conn.execute(sql, params).fetchall()

        if len(candidates) == 0:
            status = "no_match"
            winner = None
            trace = _make_decision_trace(status, rule_topic_id, query_date,
                query_date_type, anchor_type_id, anchor_value, candidates, winner,
                note="No published rule_versions matched the temporal filter.")
            log_resolution(conn, rule_topic_id, query_date, query_date_type,
                           candidates, winner, status, trace, anchor_type_id, anchor_value)
            return {
                "resolution_status": status,
                "winning_rule_version_id": None,
                "rule_text": None,
                "effective_start": None,
                "effective_end": None,
                "decision_trace": trace,
            }

        winner = candidates[0]

        if len(candidates) == 1:
            status = "resolved"
            trace = _make_decision_trace(status, rule_topic_id, query_date,
                query_date_type, anchor_type_id, anchor_value, candidates, winner,
                note="Exactly one candidate matched.")
            log_resolution(conn, rule_topic_id, query_date, query_date_type,
                           candidates, winner, status, trace, anchor_type_id, anchor_value)
            return {
                "resolution_status": status,
                "winning_rule_version_id": winner["rule_version_id"],
                "rule_text": winner["normalized_rule_text"],
                "effective_start": winner["effective_start_date"],
                "effective_end": winner["effective_end_date"],
                "decision_trace": trace,
            }

        # Multiple candidates — check if truly tied after ORDER BY
        second = candidates[1]
        top_tuple    = (winner["specificity_score"], winner["exception_flag"], winner["version_number"])
        second_tuple = (second["specificity_score"], second["exception_flag"], second["version_number"])

        if top_tuple == second_tuple:
            status = "ambiguous"
            winner = None
            trace = _make_decision_trace(status, rule_topic_id, query_date,
                query_date_type, anchor_type_id, anchor_value, candidates, winner,
                note="Multiple candidates tied after specificity, exception_flag, version_number.")
            log_resolution(conn, rule_topic_id, query_date, query_date_type,
                           candidates, winner, status, trace, anchor_type_id, anchor_value)
            return {
                "resolution_status": status,
                "winning_rule_version_id": None,
                "rule_text": None,
                "effective_start": None,
                "effective_end": None,
                "decision_trace": trace,
            }

        status = "resolved"
        trace = _make_decision_trace(status, rule_topic_id, query_date,
            query_date_type, anchor_type_id, anchor_value, candidates, winner,
            note="Resolved by deterministic ORDER BY tie-break.")
        log_resolution(conn, rule_topic_id, query_date, query_date_type,
                       candidates, winner, status, trace, anchor_type_id, anchor_value)
        return {
            "resolution_status": status,
            "winning_rule_version_id": winner["rule_version_id"],
            "rule_text": winner["normalized_rule_text"],
            "effective_start": winner["effective_start_date"],
            "effective_end": winner["effective_end_date"],
            "decision_trace": trace,
        }

    except Exception as exc:
        trace = _make_decision_trace("error", rule_topic_id, query_date,
            query_date_type, anchor_type_id, anchor_value, [], None,
            error_detail=f"{type(exc).__name__}: {exc}",
            note=traceback.format_exc())
        try:
            log_resolution(conn, rule_topic_id, query_date, query_date_type,
                           [], None, "error", trace, anchor_type_id, anchor_value)
        except Exception:
            pass
        return {
            "resolution_status": "error",
            "winning_rule_version_id": None,
            "rule_text": None,
            "effective_start": None,
            "effective_end": None,
            "decision_trace": trace,
        }


def resolve_all(conn, rule_topic_id, query_date, query_date_type):
    """
    Returns ALL billing rules active for a topic on a given date —
    one result per atomic_rule, best version selected deterministically.
    """
    try:
        _validate_inputs(conn, rule_topic_id, query_date, query_date_type, None, None)

        rows = conn.execute(
            """
            SELECT
                rv.rule_version_id,
                ar.rule_code,
                ar.rule_name                                     AS rule_title,
                ar.rule_topic_id,
                ('v' || CAST(rv.version_number AS TEXT))        AS version_label,
                rv.version_number,
                rv.normalized_rule_text                         AS display_rule_text,
                rv.effective_start_date,
                rv.effective_end_date,
                ar.specificity                                   AS specificity_score,
                rv.exception_flag,
                NULL                                             AS change_summary,
                CASE WHEN rv.superseded_by_version_id IS NOT NULL
                     THEN 1 ELSE 0 END                          AS is_superseded
            FROM rule_versions rv
            JOIN atomic_rules ar ON rv.atomic_rule_id = ar.atomic_rule_id
            WHERE rv.is_published = 1
              AND rv.lifecycle_status = 'published'
              AND ar.rule_topic_id = ?
              AND rv.effective_start_date <= ?
              AND (rv.effective_end_date IS NULL OR rv.effective_end_date >= ?)
              AND rv.rule_version_id = (
                  SELECT rv2.rule_version_id
                  FROM rule_versions rv2
                  JOIN atomic_rules ar2 ON rv2.atomic_rule_id = ar2.atomic_rule_id
                  WHERE rv2.atomic_rule_id = rv.atomic_rule_id
                    AND rv2.is_published = 1
                    AND rv2.lifecycle_status = 'published'
                    AND rv2.effective_start_date <= ?
                    AND (rv2.effective_end_date IS NULL OR rv2.effective_end_date >= ?)
                  ORDER BY ar2.specificity DESC, rv2.exception_flag DESC, rv2.version_number DESC
                  LIMIT 1
              )
            ORDER BY ar.rule_code ASC
            """,
            (rule_topic_id, query_date, query_date, query_date, query_date),
        ).fetchall()

        status = "resolved" if rows else "no_match"
        results = [
            {
                "rule_version_id":      row["rule_version_id"],
                "rule_code":            row["rule_code"],
                "rule_title":           row["rule_title"],
                "version_label":        row["version_label"],
                "rule_text":            row["display_rule_text"],
                "normalized_rule_text": row["display_rule_text"],
                "effective_start":      row["effective_start_date"],
                "effective_start_date": row["effective_start_date"],
                "effective_end":        row["effective_end_date"],
                "effective_end_date":   row["effective_end_date"],
                "change_summary":       row["change_summary"],
            }
            for row in rows
        ]

        trace = json.dumps({
            "resolution_status": status,
            "rule_topic_id": rule_topic_id,
            "query_date": query_date,
            "query_date_type": query_date_type,
            "mode": "resolve_all",
            "atomic_rules_found": len(results),
            "winning_rule_version_ids": [r["rule_version_id"] for r in results],
        }, ensure_ascii=False)

        try:
            topic_code = _get_topic_code(conn, rule_topic_id)
            conn.execute(
                """INSERT INTO resolution_logs (
                       query_topic_code, query_date, query_date_type,
                       candidate_rule_version_ids, winning_rule_version_id,
                       resolution_status, decision_trace, queried_at
                   ) VALUES (?,?,?,?,?,?,?,?)""",
                (
                    topic_code, query_date, query_date_type,
                    json.dumps([r["rule_version_id"] for r in results]),
                    results[0]["rule_version_id"] if results else None,
                    status, trace, utc_now_str(),
                ),
            )
            conn.commit()
        except Exception:
            pass

        return {
            "resolution_status": status,
            "query_date": query_date,
            "query_date_type": query_date_type,
            "rule_topic_id": rule_topic_id,
            "results": results,
            "decision_trace": trace,
        }

    except Exception as exc:
        trace = json.dumps({
            "resolution_status": "error",
            "rule_topic_id": rule_topic_id,
            "query_date": query_date,
            "error_detail": f"{type(exc).__name__}: {exc}",
            "note": traceback.format_exc(),
        }, ensure_ascii=False)
        return {
            "resolution_status": "error",
            "query_date": query_date,
            "query_date_type": query_date_type,
            "rule_topic_id": rule_topic_id,
            "results": [],
            "decision_trace": trace,
        }