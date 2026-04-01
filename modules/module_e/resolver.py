# modules/module_e/resolver.py
from __future__ import annotations

import json
import sqlite3
import traceback
from datetime import datetime, timezone
from typing import Any


def utc_now_str() -> str:
    """Return naive UTC timestamp in the locked project format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _get_topic_code(conn: sqlite3.Connection, rule_topic_id: int) -> str:
    row = conn.execute(
        """
        SELECT topic_code
        FROM rule_topics
        WHERE rule_topic_id = ?
        """,
        (rule_topic_id,),
    ).fetchone()
    return row["topic_code"] if row else str(rule_topic_id)


def _validate_inputs(
    conn: sqlite3.Connection,
    rule_topic_id: int,
    query_date: str,
    query_date_type: str,
    anchor_type_id: int | None,
    anchor_value: str | None,
) -> None:
    if not isinstance(rule_topic_id, int):
        raise ValueError("rule_topic_id must be an integer")

    if not isinstance(query_date, str) or len(query_date) != 10:
        raise ValueError("query_date must be a YYYY-MM-DD string")

    try:
        datetime.strptime(query_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("query_date must be a valid YYYY-MM-DD date") from exc

    if query_date_type not in ("date_of_service", "date_of_discharge"):
        raise ValueError(
            "query_date_type must be 'date_of_service' or 'date_of_discharge'"
        )

    topic_exists = conn.execute(
        """
        SELECT 1
        FROM rule_topics
        WHERE rule_topic_id = ?
        """,
        (rule_topic_id,),
    ).fetchone()
    if not topic_exists:
        raise ValueError(f"rule_topic_id {rule_topic_id} does not exist")

    if anchor_type_id is None and anchor_value is not None:
        raise ValueError("anchor_value cannot be provided without anchor_type_id")

    if anchor_type_id is not None:
        anchor_type_exists = conn.execute(
            """
            SELECT 1
            FROM ub04_anchor_types
            WHERE ub04_anchor_type_id = ?
            """,
            (anchor_type_id,),
        ).fetchone()
        if not anchor_type_exists:
            raise ValueError(f"anchor_type_id {anchor_type_id} does not exist")

        # NOTE: anchor_value is NOT required here.
        # resolve() adds its own check for anchor_value so it stays strict.
        # resolve_all_anchored() allows anchor_value=None (type-only filter).


def _candidate_query(
    anchor_type_id: int | None,
    anchor_value: str | None,
) -> tuple[str, list[Any]]:
    """
    Build the candidate query dynamically.

    Locked resolver rules:
    - NEVER filter on is_superseded
    - WHERE is_published = 1
      AND lifecycle_status = 'published'
      AND effective_start_date <= :query_date
      AND (effective_end_date IS NULL OR effective_end_date >= :query_date)
    - Tie-break ORDER BY:
      specificity_score DESC, exception_flag DESC, version_number DESC
    """
    params: list[Any] = []

    sql = """
        SELECT
            rv.rule_version_id,
            rv.atomic_rule_id,
            ar.rule_code,
            ar.rule_title,
            ar.rule_topic_id,
            rv.version_label,
            rv.version_number,
            rv.normalized_rule_text,
            COALESCE(rv.display_rule_text, rv.normalized_rule_text) AS display_rule_text,
            rv.effective_start_date,
            rv.effective_end_date,
            rv.specificity_score,
            rv.exception_flag,
            rv.resolver_priority,
            rv.change_type,
            rv.supersedes_rule_version_id,
            rv.is_superseded
        FROM rule_versions rv
        JOIN atomic_rules ar
          ON rv.atomic_rule_id = ar.atomic_rule_id
    """

    if anchor_type_id is not None and anchor_value is not None:
        sql += """
        JOIN rule_version_anchors rva
          ON rv.rule_version_id = rva.rule_version_id
        JOIN ub04_anchor_codes uac
          ON rva.ub04_anchor_code_id = uac.ub04_anchor_code_id
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
            rv.specificity_score DESC,
            rv.exception_flag DESC,
            rv.version_number DESC,
            rv.rule_version_id ASC
    """

    return sql, params


def _serialize_candidate_ids(rows: list[sqlite3.Row]) -> str:
    return json.dumps([row["rule_version_id"] for row in rows])


def _make_decision_trace(
    status: str,
    rule_topic_id: int,
    query_date: str,
    query_date_type: str,
    anchor_type_id: int | None,
    anchor_value: str | None,
    candidates: list[sqlite3.Row],
    winner: sqlite3.Row | None,
    note: str | None = None,
    error_detail: str | None = None,
) -> str:
    payload = {
        "resolution_status": status,
        "rule_topic_id": rule_topic_id,
        "query_date": query_date,
        "query_date_type": query_date_type,
        "anchor_type_id": anchor_type_id,
        "anchor_value": anchor_value,
        "candidate_rule_version_ids": [row["rule_version_id"] for row in candidates],
        "tie_break_order": [
            "specificity_score DESC",
            "exception_flag DESC",
            "version_number DESC",
        ],
        "winner_rule_version_id": winner["rule_version_id"] if winner else None,
        "winner_specificity_score": winner["specificity_score"] if winner else None,
        "winner_exception_flag": winner["exception_flag"] if winner else None,
        "winner_version_number": winner["version_number"] if winner else None,
        "note": note,
        "error_detail": error_detail,
    }
    return json.dumps(payload, ensure_ascii=False)


def log_resolution(
    conn: sqlite3.Connection,
    rule_topic_id: int,
    query_date: str,
    query_date_type: str,
    candidates: list[sqlite3.Row],
    winner: sqlite3.Row | None,
    status: str,
    decision_trace: str,
    anchor_type_id: int | None = None,
    anchor_value: str | None = None,
) -> None:
    topic_code = _get_topic_code(conn, rule_topic_id)

    user_input_anchor_type = None
    if anchor_type_id is not None:
        anchor_type_row = conn.execute(
            """
            SELECT anchor_type_code
            FROM ub04_anchor_types
            WHERE ub04_anchor_type_id = ?
            """,
            (anchor_type_id,),
        ).fetchone()
        if anchor_type_row:
            user_input_anchor_type = anchor_type_row["anchor_type_code"]

    conn.execute(
        """
        INSERT INTO resolution_logs (
            user_input_topic,
            user_input_rule_topic_id,
            user_input_date,
            user_input_date_type,
            user_input_anchor_type,
            user_input_anchor_type_id,
            user_input_anchor_value,
            candidate_rule_version_ids,
            winning_rule_version_id,
            resolution_status,
            decision_trace,
            queried_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            topic_code,
            rule_topic_id,
            query_date,
            query_date_type,
            user_input_anchor_type,
            anchor_type_id,
            anchor_value,
            _serialize_candidate_ids(candidates),
            winner["rule_version_id"] if winner else None,
            status,
            decision_trace,
            utc_now_str(),
        ),
    )
    conn.commit()


def resolve(
    conn: sqlite3.Connection,
    rule_topic_id: int,
    query_date: str,
    query_date_type: str,
    anchor_type_id: int | None = None,
    anchor_value: str | None = None,
) -> dict[str, Any]:
    """
    Deterministic resolver for DPKB.

    Returns a structured result dict with one of:
    - resolved
    - no_match
    - ambiguous
    - error

    Never raises a raw exception to the caller/UI.
    """
    try:
        # resolve() requires a specific anchor_value when anchor_type_id is given.
        # (resolve_all_anchored allows anchor_value=None - it has its own check.)
        if anchor_type_id is not None and (
            anchor_value is None or not str(anchor_value).strip()
        ):
            raise ValueError("anchor_value is required when anchor_type_id is provided")

        _validate_inputs(
            conn=conn,
            rule_topic_id=rule_topic_id,
            query_date=query_date,
            query_date_type=query_date_type,
            anchor_type_id=anchor_type_id,
            anchor_value=anchor_value,
        )

        sql, raw_params = _candidate_query(anchor_type_id, anchor_value)
        params: list[Any] = []
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
            decision_trace = _make_decision_trace(
                status=status,
                rule_topic_id=rule_topic_id,
                query_date=query_date,
                query_date_type=query_date_type,
                anchor_type_id=anchor_type_id,
                anchor_value=anchor_value,
                candidates=candidates,
                winner=winner,
                note="No published rule_versions matched the temporal filter.",
            )
            log_resolution(
                conn=conn,
                rule_topic_id=rule_topic_id,
                query_date=query_date,
                query_date_type=query_date_type,
                candidates=candidates,
                winner=winner,
                status=status,
                decision_trace=decision_trace,
                anchor_type_id=anchor_type_id,
                anchor_value=anchor_value,
            )
            return {
                "resolution_status": status,
                "winning_rule_version_id": None,
                "rule_text": None,
                "effective_start": None,
                "effective_end": None,
                "decision_trace": decision_trace,
            }

        winner = candidates[0]

        if len(candidates) == 1:
            status = "resolved"
            decision_trace = _make_decision_trace(
                status=status,
                rule_topic_id=rule_topic_id,
                query_date=query_date,
                query_date_type=query_date_type,
                anchor_type_id=anchor_type_id,
                anchor_value=anchor_value,
                candidates=candidates,
                winner=winner,
                note="Exactly one candidate matched.",
            )
            log_resolution(
                conn=conn,
                rule_topic_id=rule_topic_id,
                query_date=query_date,
                query_date_type=query_date_type,
                candidates=candidates,
                winner=winner,
                status=status,
                decision_trace=decision_trace,
                anchor_type_id=anchor_type_id,
                anchor_value=anchor_value,
            )
            return {
                "resolution_status": status,
                "winning_rule_version_id": winner["rule_version_id"],
                "rule_text": winner["normalized_rule_text"],
                "effective_start": winner["effective_start_date"],
                "effective_end": winner["effective_end_date"],
                "decision_trace": decision_trace,
            }

        # More than one candidate.
        # If the top-ranked fields are still tied after the locked ORDER BY criteria,
        # return ambiguous instead of silently choosing one.
        second = candidates[1]
        top_tuple = (
            winner["specificity_score"],
            winner["exception_flag"],
            winner["version_number"],
        )
        second_tuple = (
            second["specificity_score"],
            second["exception_flag"],
            second["version_number"],
        )

        if top_tuple == second_tuple:
            status = "ambiguous"
            winner = None
            decision_trace = _make_decision_trace(
                status=status,
                rule_topic_id=rule_topic_id,
                query_date=query_date,
                query_date_type=query_date_type,
                anchor_type_id=anchor_type_id,
                anchor_value=anchor_value,
                candidates=candidates,
                winner=winner,
                note=(
                    "Multiple candidates remained tied after "
                    "specificity_score, exception_flag, and version_number."
                ),
            )
            log_resolution(
                conn=conn,
                rule_topic_id=rule_topic_id,
                query_date=query_date,
                query_date_type=query_date_type,
                candidates=candidates,
                winner=winner,
                status=status,
                decision_trace=decision_trace,
                anchor_type_id=anchor_type_id,
                anchor_value=anchor_value,
            )
            return {
                "resolution_status": status,
                "winning_rule_version_id": None,
                "rule_text": None,
                "effective_start": None,
                "effective_end": None,
                "decision_trace": decision_trace,
            }

        status = "resolved"
        decision_trace = _make_decision_trace(
            status=status,
            rule_topic_id=rule_topic_id,
            query_date=query_date,
            query_date_type=query_date_type,
            anchor_type_id=anchor_type_id,
            anchor_value=anchor_value,
            candidates=candidates,
            winner=winner,
            note="Resolved by deterministic ORDER BY tie-break.",
        )
        log_resolution(
            conn=conn,
            rule_topic_id=rule_topic_id,
            query_date=query_date,
            query_date_type=query_date_type,
            candidates=candidates,
            winner=winner,
            status=status,
            decision_trace=decision_trace,
            anchor_type_id=anchor_type_id,
            anchor_value=anchor_value,
        )
        return {
            "resolution_status": status,
            "winning_rule_version_id": winner["rule_version_id"],
            "rule_text": winner["normalized_rule_text"],
            "effective_start": winner["effective_start_date"],
            "effective_end": winner["effective_end_date"],
            "decision_trace": decision_trace,
        }

    except Exception as exc:
        error_trace = _make_decision_trace(
            status="error",
            rule_topic_id=rule_topic_id,
            query_date=query_date,
            query_date_type=query_date_type,
            anchor_type_id=anchor_type_id,
            anchor_value=anchor_value,
            candidates=[],
            winner=None,
            error_detail=f"{type(exc).__name__}: {exc}",
            note=traceback.format_exc(),
        )

        try:
            # Log the error if the DB connection is still usable.
            log_resolution(
                conn=conn,
                rule_topic_id=rule_topic_id,
                query_date=query_date,
                query_date_type=query_date_type,
                candidates=[],
                winner=None,
                status="error",
                decision_trace=error_trace,
                anchor_type_id=anchor_type_id,
                anchor_value=anchor_value,
            )
        except Exception:
            # Never let logging failure escape to UI.
            pass

        return {
            "resolution_status": "error",
            "winning_rule_version_id": None,
            "rule_text": None,
            "effective_start": None,
            "effective_end": None,
            "decision_trace": error_trace,
        }

# ---------------------------------------------------------------------------
# resolve_topic - returns one best rule_version per atomic rule for a topic+date
# Used by the UI when no anchor filter is selected.
# The single resolve() function is used only when an anchor filter narrows
# the query to a specific UB-04 code.
# ---------------------------------------------------------------------------

def resolve_topic(
    conn: sqlite3.Connection,
    rule_topic_id: int,
    query_date: str,
    query_date_type: str,
) -> dict:
    """
    Return ALL active billing rules for a topic on a given date - one best
    version per atomic rule.

    Returns:
        {
            "resolution_status": "resolved" | "no_match" | "error",
            "query_date": str,
            "rule_topic_id": int,
            "results": [
                {
                    "rule_version_id": int,
                    "rule_code": str,
                    "rule_title": str,
                    "version_label": str,
                    "normalized_rule_text": str,
                    "effective_start_date": str,
                    "effective_end_date": str | None,
                }
                ...
            ]
        }
    """
    try:
        rows = conn.execute(
            """
            SELECT
                rv.rule_version_id,
                rv.atomic_rule_id,
                ar.rule_code,
                ar.rule_title,
                rv.version_label,
                rv.version_number,
                COALESCE(rv.display_rule_text, rv.normalized_rule_text) AS normalized_rule_text,
                rv.effective_start_date,
                rv.effective_end_date,
                rv.specificity_score,
                rv.exception_flag
            FROM rule_versions rv
            JOIN atomic_rules ar
              ON rv.atomic_rule_id = ar.atomic_rule_id
            WHERE rv.is_published = 1
              AND rv.lifecycle_status = 'published'
              AND ar.rule_topic_id = ?
              AND rv.effective_start_date <= ?
              AND (rv.effective_end_date IS NULL OR rv.effective_end_date >= ?)
            ORDER BY
                ar.rule_code ASC,
                rv.specificity_score DESC,
                rv.exception_flag DESC,
                rv.version_number DESC
            """,
            (rule_topic_id, query_date, query_date),
        ).fetchall()

        # One best version per atomic rule - first row wins due to ORDER BY
        seen: set = set()
        results = []
        for row in rows:
            if row["atomic_rule_id"] not in seen:
                seen.add(row["atomic_rule_id"])
                results.append({
                    "rule_version_id":    row["rule_version_id"],
                    "rule_code":          row["rule_code"],
                    "rule_title":         row["rule_title"],
                    "version_label":      row["version_label"],
                    "normalized_rule_text": row["normalized_rule_text"],
                    "effective_start_date": row["effective_start_date"],
                    "effective_end_date":   row["effective_end_date"],
                })

        if not results:
            return {
                "resolution_status": "no_match",
                "query_date": query_date,
                "rule_topic_id": rule_topic_id,
                "results": [],
            }

        return {
            "resolution_status": "resolved",
            "query_date": query_date,
            "rule_topic_id": rule_topic_id,
            "results": results,
        }

    except Exception as exc:
        return {
            "resolution_status": "error",
            "query_date": query_date,
            "rule_topic_id": rule_topic_id,
            "results": [],
            "error": str(exc),
        }


# ---------------------------------------------------------------------------
# resolve_all - returns one result per atomic_rule active on the query date.
# Used by the UI when NO anchor filter is specified.
# The single resolve() function remains unchanged for anchor-filtered queries
# and for the 6 retrieval tests.
# ---------------------------------------------------------------------------

def _best_version_per_rule_sql() -> str:
    """
    For each atomic_rule in the topic, return the single best rule_version
    active on query_date, using the same tie-break as resolve():
      specificity_score DESC, exception_flag DESC, version_number DESC.

    Uses a correlated subquery (compatible with SQLite 3.x without window fns).
    """
    return """
        SELECT
            rv.rule_version_id,
            ar.rule_code,
            ar.rule_title,
            ar.rule_topic_id,
            rv.version_label,
            rv.version_number,
            rv.normalized_rule_text,
            COALESCE(rv.display_rule_text, rv.normalized_rule_text) AS display_rule_text,
            rv.effective_start_date,
            rv.effective_end_date,
            rv.specificity_score,
            rv.exception_flag,
            rv.change_type,
            rv.change_summary,
            rv.is_superseded
        FROM rule_versions rv
        JOIN atomic_rules ar
          ON rv.atomic_rule_id = ar.atomic_rule_id
        WHERE rv.is_published = 1
          AND rv.lifecycle_status = 'published'
          AND ar.rule_topic_id = ?
          AND rv.effective_start_date <= ?
          AND (rv.effective_end_date IS NULL OR rv.effective_end_date >= ?)
          AND rv.rule_version_id = (
              SELECT rv2.rule_version_id
              FROM rule_versions rv2
              WHERE rv2.atomic_rule_id = rv.atomic_rule_id
                AND rv2.is_published = 1
                AND rv2.lifecycle_status = 'published'
                AND rv2.effective_start_date <= ?
                AND (rv2.effective_end_date IS NULL OR rv2.effective_end_date >= ?)
              ORDER BY rv2.specificity_score DESC,
                       rv2.exception_flag DESC,
                       rv2.version_number DESC
              LIMIT 1
          )
        ORDER BY ar.rule_code ASC
    """


def resolve_all(
    conn: sqlite3.Connection,
    rule_topic_id: int,
    query_date: str,
    query_date_type: str,
) -> dict[str, Any]:
    """
    Returns ALL billing rules active for a topic on a given date -
    one result per atomic_rule, best version selected deterministically.

    Used by the UI when no anchor filter is provided.
    Never raises; always returns a structured dict.

    Return shape
    ------------
    {
        "resolution_status": "resolved" | "no_match" | "error",
        "query_date": str,
        "query_date_type": str,
        "rule_topic_id": int,
        "results": [
            {
                "rule_version_id": int,
                "rule_code": str,
                "rule_title": str,
                "version_label": str,
                "rule_text": str,
                "effective_start": str,
                "effective_end": str | None,
                "change_summary": str | None,
            },
            ...
        ],
        "decision_trace": str   # JSON
    }
    """
    try:
        _validate_inputs(
            conn=conn,
            rule_topic_id=rule_topic_id,
            query_date=query_date,
            query_date_type=query_date_type,
            anchor_type_id=None,
            anchor_value=None,
        )

        rows = conn.execute(
            _best_version_per_rule_sql(),
            (rule_topic_id, query_date, query_date, query_date, query_date),
        ).fetchall()

        if not rows:
            status = "no_match"
            results = []
        else:
            status = "resolved"
            results = [
                {
                    "rule_version_id":  row["rule_version_id"],
                    "rule_code":        row["rule_code"],
                    "rule_title":       row["rule_title"],
                    "version_label":    row["version_label"],
                    "rule_text":        row["display_rule_text"],
                    "normalized_rule_text": row["display_rule_text"],
                    "effective_start":  row["effective_start_date"],
                    "effective_start_date": row["effective_start_date"],
                    "effective_end":    row["effective_end_date"],
                    "effective_end_date":   row["effective_end_date"],
                    "change_summary":   row["change_summary"],
                }
                for row in rows
            ]

        trace = json.dumps({
            "resolution_status":      status,
            "rule_topic_id":          rule_topic_id,
            "query_date":             query_date,
            "query_date_type":        query_date_type,
            "mode":                   "resolve_all",
            "atomic_rules_found":     len(results),
            "winning_rule_version_ids": [r["rule_version_id"] for r in results],
        }, ensure_ascii=False)

        # Log one entry covering all winners
        try:
            topic_code = _get_topic_code(conn, rule_topic_id)
            conn.execute(
                """
                INSERT INTO resolution_logs (
                    user_input_topic, user_input_rule_topic_id,
                    user_input_date, user_input_date_type,
                    candidate_rule_version_ids,
                    winning_rule_version_id,
                    resolution_status, decision_trace, queried_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    topic_code, rule_topic_id,
                    query_date, query_date_type,
                    json.dumps([r["rule_version_id"] for r in results]),
                    results[0]["rule_version_id"] if results else None,
                    status, trace, utc_now_str(),
                ),
            )
            conn.commit()
        except Exception:
            pass  # never let logging failure surface to UI

        return {
            "resolution_status": status,
            "query_date":        query_date,
            "query_date_type":   query_date_type,
            "rule_topic_id":     rule_topic_id,
            "results":           results,
            "decision_trace":    trace,
        }

    except Exception as exc:
        error_trace = json.dumps({
            "resolution_status": "error",
            "rule_topic_id":     rule_topic_id,
            "query_date":        query_date,
            "error_detail":      f"{type(exc).__name__}: {exc}",
            "note":              traceback.format_exc(),
        }, ensure_ascii=False)
        return {
            "resolution_status": "error",
            "query_date":        query_date,
            "query_date_type":   query_date_type,
            "rule_topic_id":     rule_topic_id,
            "results":           [],
            "decision_trace":    error_trace,
        }

# ---------------------------------------------------------------------------
# resolve_all_anchored
# anchor_value=None  → filter by anchor TYPE only (any code of this type)
# anchor_value="131" → filter by that exact anchor code
# ---------------------------------------------------------------------------

def resolve_all_anchored(
    conn: sqlite3.Connection,
    rule_topic_id: int,
    query_date: str,
    query_date_type: str,
    anchor_type_id: int,
    anchor_value: "str | None" = None,
) -> "dict[str, Any]":
    """
    Returns all active rules for a topic+date that carry the given anchor
    type, optionally narrowed to a specific anchor code.

    Does NOT call _validate_inputs (which would reject anchor_value=None).
    Return shape matches resolve_all() so render_all_results() works as-is.
    """
    try:
        if anchor_value is not None:
            anchor_code_clause = "AND uac.anchor_code = ?"
            params = (
                rule_topic_id, query_date, query_date,
                anchor_type_id, anchor_value,
                query_date, query_date,
            )
        else:
            anchor_code_clause = ""
            params = (
                rule_topic_id, query_date, query_date,
                anchor_type_id,
                query_date, query_date,
            )

        sql = f"""
            SELECT
                rv.rule_version_id,
                ar.rule_code,
                ar.rule_title,
                ar.rule_topic_id,
                rv.version_label,
                rv.version_number,
                COALESCE(rv.display_rule_text, rv.normalized_rule_text) AS display_rule_text,
                rv.effective_start_date,
                rv.effective_end_date,
                rv.specificity_score,
                rv.exception_flag,
                rv.change_type,
                rv.change_summary,
                rv.is_superseded
            FROM rule_versions rv
            JOIN atomic_rules ar
              ON rv.atomic_rule_id = ar.atomic_rule_id
            JOIN rule_version_anchors rva
              ON rv.rule_version_id = rva.rule_version_id
            JOIN ub04_anchor_codes uac
              ON rva.ub04_anchor_code_id = uac.ub04_anchor_code_id
            WHERE rv.is_published = 1
              AND rv.lifecycle_status = 'published'
              AND ar.rule_topic_id = ?
              AND rv.effective_start_date <= ?
              AND (rv.effective_end_date IS NULL OR rv.effective_end_date >= ?)
              AND uac.ub04_anchor_type_id = ?
              {anchor_code_clause}
              AND rv.rule_version_id = (
                  SELECT rv2.rule_version_id
                  FROM rule_versions rv2
                  WHERE rv2.atomic_rule_id = rv.atomic_rule_id
                    AND rv2.is_published = 1
                    AND rv2.lifecycle_status = 'published'
                    AND rv2.effective_start_date <= ?
                    AND (rv2.effective_end_date IS NULL OR rv2.effective_end_date >= ?)
                  ORDER BY rv2.specificity_score DESC,
                           rv2.exception_flag DESC,
                           rv2.version_number DESC
                  LIMIT 1
              )
            ORDER BY ar.rule_code ASC
        """

        rows   = conn.execute(sql, params).fetchall()
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
            "resolution_status":        status,
            "mode":                     "resolve_all_anchored",
            "rule_topic_id":            rule_topic_id,
            "query_date":               query_date,
            "anchor_type_id":           anchor_type_id,
            "anchor_value":             anchor_value,
            "atomic_rules_found":       len(results),
            "winning_rule_version_ids": [r["rule_version_id"] for r in results],
        }, ensure_ascii=False)

        try:
            topic_code = _get_topic_code(conn, rule_topic_id)
            arow = conn.execute(
                "SELECT anchor_type_code FROM ub04_anchor_types WHERE ub04_anchor_type_id=?",
                (anchor_type_id,),
            ).fetchone()
            conn.execute(
                """INSERT INTO resolution_logs (
                       user_input_topic, user_input_rule_topic_id,
                       user_input_date, user_input_date_type,
                       user_input_anchor_type, user_input_anchor_type_id,
                       user_input_anchor_value,
                       candidate_rule_version_ids, winning_rule_version_id,
                       resolution_status, decision_trace, queried_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    topic_code, rule_topic_id,
                    query_date, query_date_type,
                    arow["anchor_type_code"] if arow else None, anchor_type_id,
                    anchor_value,
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
            "query_date":        query_date,
            "query_date_type":   query_date_type,
            "rule_topic_id":     rule_topic_id,
            "results":           results,
            "decision_trace":    trace,
        }

    except Exception as exc:
        return {
            "resolution_status": "error",
            "query_date":        query_date,
            "query_date_type":   query_date_type,
            "rule_topic_id":     rule_topic_id,
            "results":           [],
            "decision_trace":    json.dumps({
                "resolution_status": "error",
                "error_detail":      f"{type(exc).__name__}: {exc}",
                "note":              traceback.format_exc(),
            }, ensure_ascii=False),
        }