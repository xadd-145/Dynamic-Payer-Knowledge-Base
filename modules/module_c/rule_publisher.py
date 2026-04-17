# modules/module_c/rule_publisher.py
from db_init import get_connection, utc_now


def publish_fragment(conn, policy_fragment_id: int, reviewed_by: str,
                     rule_code: str, rule_name: str, topic_code: str,
                     effective_start_date: str, effective_end_date=None,
                     notes: str = None) -> dict:
    now = utc_now()

    # Get topic_id
    topic = conn.execute(
        "SELECT rule_topic_id FROM rule_topics WHERE topic_code = ?", (topic_code,)
    ).fetchone()
    if not topic:
        return {"status": "error", "detail": f"Topic {topic_code} not found"}

    topic_id = topic[0]

    # Get fragment
    fragment = conn.execute(
        "SELECT * FROM policy_fragments WHERE policy_fragment_id = ?",
        (policy_fragment_id,)
    ).fetchone()
    if not fragment:
        return {"status": "error", "detail": "Fragment not found"}

    # Get or create atomic_rule
    existing_rule = conn.execute(
        "SELECT atomic_rule_id FROM atomic_rules WHERE rule_code = ?", (rule_code,)
    ).fetchone()

    if existing_rule:
        atomic_rule_id = existing_rule[0]
        version_number = conn.execute(
            "SELECT COUNT(*) FROM rule_versions WHERE atomic_rule_id = ?",
            (atomic_rule_id,)
        ).fetchone()[0] + 1
    else:
        atomic_rule_id = conn.execute(
            """INSERT INTO atomic_rules
            (rule_topic_id, rule_code, rule_name, is_exception, specificity, created_at)
            VALUES (?, ?, ?, 0, 5, ?)""",
            (topic_id, rule_code, rule_name, now)
        ).lastrowid
        version_number = 1

    # Create rule_version as draft then publish via trigger
    rv_id = conn.execute(
        """INSERT INTO rule_versions
        (atomic_rule_id, version_number, normalized_rule_text,
         effective_start_date, effective_end_date, lifecycle_status,
         is_published, exception_flag, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'draft', 0, 0, ?, ?)""",
        (atomic_rule_id, version_number, fragment["fragment_text_raw"],
         effective_start_date, effective_end_date, now, now)
    ).lastrowid

    # Evidence link
    conn.execute(
        """INSERT INTO rule_evidence_links
        (rule_version_id, source_document_id, policy_fragment_id,
         page_number_start, citation_text, created_at)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (rv_id, fragment["source_document_id"], policy_fragment_id,
         fragment["page_number_start"],
         f"Auto-published from fragment {policy_fragment_id}", now)
    )

    # Publish via trigger
    conn.execute(
        "UPDATE rule_versions SET lifecycle_status = 'published', updated_at = ? WHERE rule_version_id = ?",
        (now, rv_id)
    )

    # Mark fragment as approved
    conn.execute(
        """UPDATE policy_fragments SET review_status = 'approved',
        reviewed_by = ?, reviewed_at = ?, review_notes = ?
        WHERE policy_fragment_id = ?""",
        (reviewed_by, now, notes, policy_fragment_id)
    )

    # Fire notification
    conn.execute(
        """INSERT INTO notifications (message, rule_code, triggered_by, is_read, created_at)
        VALUES (?, ?, ?, 0, ?)""",
        (f"Rule {rule_code} published from reviewed fragment", rule_code, reviewed_by, now)
    )

    conn.commit()
    return {"status": "published", "rule_version_id": rv_id, "rule_code": rule_code}


def reject_fragment(conn, policy_fragment_id: int, reviewed_by: str, notes: str = None):
    now = utc_now()
    conn.execute(
        """UPDATE policy_fragments SET review_status = 'rejected',
        reviewed_by = ?, reviewed_at = ?, review_notes = ?
        WHERE policy_fragment_id = ?""",
        (reviewed_by, now, notes, policy_fragment_id)
    )
    conn.commit()
    return {"status": "rejected", "policy_fragment_id": policy_fragment_id}