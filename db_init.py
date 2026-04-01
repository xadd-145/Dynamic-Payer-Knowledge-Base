# db_init.py
# Initializes the DPKB SQLite database.
# Creates all 12 tables, named indexes, and the is_published sync triggers.
#
# Usage:
#   python db_init.py            -- create/verify db at default path (idempotent)
#   python db_init.py --reset    -- delete and recreate from scratch
#
# Every connection opened anywhere in the codebase MUST go through
# get_connection() to guarantee the three required PRAGMAs.

import sqlite3
import sys
from pathlib import Path

from constants import (
    ATOMIC_RULE_STATUS,
    CHANGE_TYPE,
    CONFIDENCE_TIER,
    DATE_ROLE_LABEL,
    EVIDENCE_ROLE,
    LIFECYCLE_STATUS,
    RESOLUTION_STATUS,
    REVIEW_STATUS,
    SOURCE_TYPE_CODE,
    USER_INPUT_DATE_TYPE,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DB_DIR   = BASE_DIR / "db"
DB_PATH  = DB_DIR   / "dpkb.db"


# ---------------------------------------------------------------------------
# Helper: build SQL IN-list from a constants tuple (no outer parens)
# Usage:  f"CHECK (field IN ({_in(LIFECYCLE_STATUS)}))"
# ---------------------------------------------------------------------------

def _in(values: tuple) -> str:
    return ", ".join(f"'{v}'" for v in values)


# ---------------------------------------------------------------------------
# Connection factory - MUST be used for every database connection
# ---------------------------------------------------------------------------

def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Open a SQLite connection and apply the three required PRAGMAs.
    Raises RuntimeError if WAL mode cannot be confirmed.
    Returns a connection with row_factory = sqlite3.Row.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA foreign_keys = ON;")

    mode = conn.execute("PRAGMA journal_mode = WAL;").fetchone()[0]
    if str(mode).lower() != "wal":
        conn.close()
        raise RuntimeError(
            f"WAL mode could not be enabled. Got journal_mode='{mode}'. "
            "Ensure the database file is not on a network or read-only filesystem."
        )

    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def init_db(db_path: Path = DB_PATH) -> None:
    """
    Create all 12 tables, indexes, and triggers.
    Safe to call on an existing database - uses IF NOT EXISTS throughout.
    Does NOT delete the database unless --reset is passed on the CLI.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)
    try:
        _create_tables(conn)
        _create_triggers(conn)
        _create_indexes(conn)
        conn.commit()
        print(f"[db_init] Database ready: {db_path}")
        _verify(conn)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Table creation - order is fixed by FK dependencies
# ---------------------------------------------------------------------------

def _create_tables(conn: sqlite3.Connection) -> None:

    # 1. source_types
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS source_types (
            source_type_id   INTEGER PRIMARY KEY,
            source_type_code TEXT    NOT NULL
                CHECK (source_type_code IN ({_in(SOURCE_TYPE_CODE)})),
            source_type_name TEXT    NOT NULL,
            description      TEXT,
            is_active        INTEGER NOT NULL DEFAULT 1
                CHECK (is_active IN (0, 1))
        )
    """)

    # 2. rule_topics  (self-ref FK for hierarchy)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rule_topics (
            rule_topic_id        INTEGER PRIMARY KEY,
            topic_code           TEXT    NOT NULL,
            topic_name           TEXT    NOT NULL,
            parent_rule_topic_id INTEGER
                REFERENCES rule_topics (rule_topic_id),
            description          TEXT,
            display_order        INTEGER,
            is_active            INTEGER NOT NULL DEFAULT 1
                CHECK (is_active IN (0, 1))
        )
    """)

    # 3. service_types
    conn.execute("""
        CREATE TABLE IF NOT EXISTS service_types (
            service_type_id   INTEGER PRIMARY KEY,
            service_type_code TEXT    NOT NULL,
            service_type_name TEXT    NOT NULL,
            description       TEXT,
            is_active         INTEGER NOT NULL DEFAULT 1
                CHECK (is_active IN (0, 1))
        )
    """)

    # 4. ub04_anchor_types
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ub04_anchor_types (
            ub04_anchor_type_id INTEGER PRIMARY KEY,
            anchor_type_code    TEXT    NOT NULL,
            anchor_type_name    TEXT    NOT NULL,
            description         TEXT,
            is_active           INTEGER NOT NULL DEFAULT 1
                CHECK (is_active IN (0, 1))
        )
    """)

    # 5. ub04_anchor_codes
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ub04_anchor_codes (
            ub04_anchor_code_id INTEGER PRIMARY KEY,
            ub04_anchor_type_id INTEGER NOT NULL
                REFERENCES ub04_anchor_types (ub04_anchor_type_id),
            anchor_code         TEXT     NOT NULL,
            anchor_label        TEXT     NOT NULL,
            anchor_description  TEXT,
            anchor_value_start  TEXT,
            anchor_value_end    TEXT,
            is_active           INTEGER  NOT NULL DEFAULT 1
                CHECK (is_active IN (0, 1)),
            created_at          DATETIME NOT NULL,
            updated_at          DATETIME NOT NULL
        )
    """)

    # 6. source_documents
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_documents (
            source_document_id            INTEGER  PRIMARY KEY,
            source_type_id                INTEGER  NOT NULL
                REFERENCES source_types (source_type_id),
            title                         TEXT     NOT NULL,
            source_url                    TEXT     NOT NULL,
            file_path                     TEXT,
            mime_type                     TEXT,
            sha256_hash                   TEXT     NOT NULL,
            retrieved_at                  DATETIME NOT NULL,
            published_date                DATE,
            document_effective_start_date DATE,
            document_effective_end_date   DATE,
            authority_rank                INTEGER  NOT NULL
                CHECK (authority_rank > 0),
            document_version_label        TEXT,
            is_active                     INTEGER  NOT NULL DEFAULT 1
                CHECK (is_active IN (0, 1)),
            notes                         TEXT,
            created_at                    DATETIME NOT NULL,
            updated_at                    DATETIME NOT NULL,
            CHECK (
                document_effective_end_date IS NULL
                OR document_effective_start_date IS NULL
                OR document_effective_end_date >= document_effective_start_date
            )
        )
    """)

    # 7. policy_fragments
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS policy_fragments (
            policy_fragment_id             INTEGER  PRIMARY KEY,
            source_document_id             INTEGER  NOT NULL
                REFERENCES source_documents (source_document_id),
            fragment_sequence              INTEGER  NOT NULL,
            fragment_text_raw              TEXT     NOT NULL,
            fragment_text_clean            TEXT,
            page_number_start              INTEGER,
            page_number_end                INTEGER,
            section_reference              TEXT,
            confidence_score               REAL
                CHECK (confidence_score IS NULL
                    OR (confidence_score >= 0.0 AND confidence_score <= 1.0)),
            confidence_tier                TEXT
                CHECK (confidence_tier IS NULL
                    OR confidence_tier IN ({_in(CONFIDENCE_TIER)})),
            date_role_label                TEXT
                CHECK (date_role_label IS NULL
                    OR date_role_label IN ({_in(DATE_ROLE_LABEL)})),
            extracted_effective_start_date DATE,
            extracted_effective_end_date   DATE,
            review_status                  TEXT     NOT NULL
                CHECK (review_status IN ({_in(REVIEW_STATUS)})),
            review_notes                   TEXT,
            created_at                     DATETIME NOT NULL,
            updated_at                     DATETIME NOT NULL,
            CHECK (
                extracted_effective_end_date IS NULL
                OR extracted_effective_start_date IS NULL
                OR extracted_effective_end_date >= extracted_effective_start_date
            )
        )
    """)

    # 8. atomic_rules
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS atomic_rules (
            atomic_rule_id       INTEGER  PRIMARY KEY,
            rule_code            TEXT     NOT NULL,
            rule_title           TEXT     NOT NULL,
            rule_topic_id        INTEGER  NOT NULL
                REFERENCES rule_topics (rule_topic_id),
            service_type_id      INTEGER
                REFERENCES service_types (service_type_id),
            rule_summary         TEXT,
            business_description TEXT,
            status               TEXT     NOT NULL
                CHECK (status IN ({_in(ATOMIC_RULE_STATUS)})),
            created_at           DATETIME NOT NULL,
            updated_at           DATETIME NOT NULL
        )
    """)

    # 9. rule_versions  (self-ref FK for supersession lineage)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS rule_versions (
            rule_version_id            INTEGER  PRIMARY KEY,
            atomic_rule_id             INTEGER  NOT NULL
                REFERENCES atomic_rules (atomic_rule_id),
            version_label              TEXT     NOT NULL,
            version_number             INTEGER  NOT NULL DEFAULT 1,
            normalized_rule_text       TEXT     NOT NULL,
            display_rule_text          TEXT,
            interpretation_notes       TEXT,
            publication_date           DATE,
            effective_start_date       DATE     NOT NULL,
            effective_end_date         DATE,
            change_type                TEXT     NOT NULL
                CHECK (change_type IN ({_in(CHANGE_TYPE)})),
            supersedes_rule_version_id INTEGER
                REFERENCES rule_versions (rule_version_id),
            is_superseded              INTEGER  NOT NULL DEFAULT 0
                CHECK (is_superseded IN (0, 1)),
            specificity_score          INTEGER  NOT NULL DEFAULT 0,
            exception_flag             INTEGER  NOT NULL DEFAULT 0
                CHECK (exception_flag IN (0, 1)),
            resolver_priority          INTEGER  NOT NULL DEFAULT 0,
            lifecycle_status           TEXT     NOT NULL
                CHECK (lifecycle_status IN ({_in(LIFECYCLE_STATUS)})),
            is_published               INTEGER  NOT NULL DEFAULT 0
                CHECK (is_published IN (0, 1)),
            published_at               DATETIME,
            approved_at                DATETIME,
            change_summary             TEXT,
            created_by                 TEXT,
            created_at                 DATETIME NOT NULL,
            updated_at                 DATETIME NOT NULL,
            CHECK (
                effective_end_date IS NULL
                OR effective_end_date >= effective_start_date
            )
        )
    """)

    # 10. rule_version_anchors
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rule_version_anchors (
            rule_version_anchor_id INTEGER  PRIMARY KEY,
            rule_version_id        INTEGER  NOT NULL
                REFERENCES rule_versions (rule_version_id),
            ub04_anchor_code_id    INTEGER  NOT NULL
                REFERENCES ub04_anchor_codes (ub04_anchor_code_id),
            is_primary             INTEGER  NOT NULL DEFAULT 0
                CHECK (is_primary IN (0, 1)),
            anchor_notes           TEXT,
            created_at             DATETIME NOT NULL
        )
    """)

    # 11. rule_evidence_links
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS rule_evidence_links (
            rule_evidence_link_id INTEGER  PRIMARY KEY,
            rule_version_id       INTEGER  NOT NULL
                REFERENCES rule_versions (rule_version_id),
            policy_fragment_id    INTEGER
                REFERENCES policy_fragments (policy_fragment_id),
            source_document_id    INTEGER  NOT NULL
                REFERENCES source_documents (source_document_id),
            evidence_role         TEXT     NOT NULL
                CHECK (evidence_role IN ({_in(EVIDENCE_ROLE)})),
            citation_text         TEXT,
            page_number_start     INTEGER,
            page_number_end       INTEGER,
            section_reference     TEXT,
            trust_note            TEXT,
            created_at            DATETIME NOT NULL
        )
    """)

    # 12. resolution_logs
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS resolution_logs (
            query_id                   INTEGER  PRIMARY KEY,
            user_input_topic           TEXT     NOT NULL,
            user_input_rule_topic_id   INTEGER
                REFERENCES rule_topics (rule_topic_id),
            user_input_date            DATE     NOT NULL,
            user_input_date_type       TEXT     NOT NULL
                CHECK (user_input_date_type IN ({_in(USER_INPUT_DATE_TYPE)})),
            user_input_anchor_type     TEXT,
            user_input_anchor_type_id  INTEGER
                REFERENCES ub04_anchor_types (ub04_anchor_type_id),
            user_input_anchor_value    TEXT,
            candidate_rule_version_ids TEXT,
            winning_rule_version_id    INTEGER
                REFERENCES rule_versions (rule_version_id),
            resolution_status          TEXT     NOT NULL
                CHECK (resolution_status IN ({_in(RESOLUTION_STATUS)})),
            decision_trace             TEXT,
            queried_at                 DATETIME NOT NULL
        )
    """)


# ---------------------------------------------------------------------------
# is_published sync triggers
#
# The guardrails lock the trigger spec as:
#   "fires on UPDATE OF lifecycle_status"
#
# This implementation adds an INSERT trigger as an intentional enhancement.
# Reason: the guardrails also state "never set is_published independently."
# An update-only trigger cannot satisfy that rule when rows are inserted
# directly with lifecycle_status='published' (as seed_sample_data.py does).
# Without the insert trigger, is_published stays 0 on those rows and the
# resolver never returns them, breaking all 6 retrieval tests.
#
# Two triggers are therefore the minimum implementation that satisfies both
# guardrail constraints simultaneously.
#
# NEVER set is_published directly in application code. It is derived state
# maintained exclusively by these two triggers.
# ---------------------------------------------------------------------------

def _create_triggers(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_sync_is_published_insert
        AFTER INSERT ON rule_versions
        BEGIN
            UPDATE rule_versions
            SET is_published = CASE
                WHEN NEW.lifecycle_status = 'published' THEN 1
                ELSE 0
            END
            WHERE rule_version_id = NEW.rule_version_id;
        END
    """)

    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_sync_is_published_update
        AFTER UPDATE OF lifecycle_status ON rule_versions
        BEGIN
            UPDATE rule_versions
            SET is_published = CASE
                WHEN NEW.lifecycle_status = 'published' THEN 1
                ELSE 0
            END
            WHERE rule_version_id = NEW.rule_version_id;
        END
    """)


# ---------------------------------------------------------------------------
# Named indexes - unique indexes first, then schema-export performance indexes
# ---------------------------------------------------------------------------

def _create_indexes(conn: sqlite3.Connection) -> None:
    stmts = [
        # Unique indexes - lookup table codes (from final schema export)
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_source_type_code"
        "    ON source_types (source_type_code)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_rule_topic_code"
        "    ON rule_topics (topic_code)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_service_type_code"
        "    ON service_types (service_type_code)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_anchor_type_code_lookup"
        "    ON ub04_anchor_types (anchor_type_code)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_anchor_type_code"
        "    ON ub04_anchor_codes (ub04_anchor_type_id, anchor_code)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_source_url_hash"
        "    ON source_documents (source_url, sha256_hash)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_fragment_sequence"
        "    ON policy_fragments (source_document_id, fragment_sequence)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_rule_code"
        "    ON atomic_rules (rule_code)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_rule_version_label"
        "    ON rule_versions (atomic_rule_id, version_label)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_version_anchor"
        "    ON rule_version_anchors (rule_version_id, ub04_anchor_code_id)",

        # Performance indexes - names preserved from schema export
        "CREATE INDEX IF NOT EXISTS source_documents_index_2"
        "    ON source_documents (source_type_id)",
        "CREATE INDEX IF NOT EXISTS source_documents_index_3"
        "    ON source_documents (published_date)",
        "CREATE INDEX IF NOT EXISTS source_documents_index_4"
        "    ON source_documents (authority_rank)",
        "CREATE INDEX IF NOT EXISTS policy_fragments_index_6"
        "    ON policy_fragments (source_document_id)",
        "CREATE INDEX IF NOT EXISTS policy_fragments_index_7"
        "    ON policy_fragments (review_status)",
        "CREATE INDEX IF NOT EXISTS policy_fragments_index_8"
        "    ON policy_fragments (extracted_effective_start_date)",
        "CREATE INDEX IF NOT EXISTS atomic_rules_index_9"
        "    ON atomic_rules (rule_topic_id)",
        "CREATE INDEX IF NOT EXISTS atomic_rules_index_10"
        "    ON atomic_rules (service_type_id)",
        "CREATE INDEX IF NOT EXISTS atomic_rules_index_11"
        "    ON atomic_rules (status)",
        "CREATE INDEX IF NOT EXISTS rule_versions_index_13"
        "    ON rule_versions (atomic_rule_id)",
        "CREATE INDEX IF NOT EXISTS rule_versions_index_14"
        "    ON rule_versions (effective_start_date)",
        "CREATE INDEX IF NOT EXISTS rule_versions_index_15"
        "    ON rule_versions (effective_end_date)",
        "CREATE INDEX IF NOT EXISTS idx_resolver_window"
        "    ON rule_versions (atomic_rule_id, effective_start_date, effective_end_date)",
        "CREATE INDEX IF NOT EXISTS idx_published_filter"
        "    ON rule_versions (lifecycle_status, is_published)",
        "CREATE INDEX IF NOT EXISTS rule_versions_index_18"
        "    ON rule_versions (publication_date)",
        "CREATE INDEX IF NOT EXISTS rule_versions_index_19"
        "    ON rule_versions (supersedes_rule_version_id)",
        "CREATE INDEX IF NOT EXISTS idx_primary_anchor"
        "    ON rule_version_anchors (rule_version_id, is_primary)",
        "CREATE INDEX IF NOT EXISTS rule_version_anchors_index_22"
        "    ON rule_version_anchors (ub04_anchor_code_id)",
        "CREATE INDEX IF NOT EXISTS idx_evidence_role"
        "    ON rule_evidence_links (rule_version_id, evidence_role)",
        "CREATE INDEX IF NOT EXISTS rule_evidence_links_index_24"
        "    ON rule_evidence_links (rule_version_id)",
        "CREATE INDEX IF NOT EXISTS rule_evidence_links_index_25"
        "    ON rule_evidence_links (policy_fragment_id)",
        "CREATE INDEX IF NOT EXISTS rule_evidence_links_index_26"
        "    ON rule_evidence_links (source_document_id)",
        "CREATE INDEX IF NOT EXISTS idx_queried_at"
        "    ON resolution_logs (queried_at)",
        "CREATE INDEX IF NOT EXISTS resolution_logs_index_28"
        "    ON resolution_logs (resolution_status)",
        "CREATE INDEX IF NOT EXISTS resolution_logs_index_29"
        "    ON resolution_logs (winning_rule_version_id)",
    ]
    for stmt in stmts:
        conn.execute(stmt)


# ---------------------------------------------------------------------------
# Post-init verification
# ---------------------------------------------------------------------------

EXPECTED_TABLES = {
    "source_types", "rule_topics", "service_types",
    "ub04_anchor_types", "ub04_anchor_codes", "source_documents",
    "policy_fragments", "atomic_rules", "rule_versions",
    "rule_version_anchors", "rule_evidence_links", "resolution_logs",
}

EXPECTED_TRIGGERS = {
    "trg_sync_is_published_insert",
    "trg_sync_is_published_update",
}


def _verify(conn: sqlite3.Connection) -> None:
    tables = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    missing_tables = EXPECTED_TABLES - tables
    if missing_tables:
        raise RuntimeError(f"Missing tables after init: {missing_tables}")

    triggers = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
    }
    missing_triggers = EXPECTED_TRIGGERS - triggers
    if missing_triggers:
        raise RuntimeError(f"Missing triggers after init: {missing_triggers}")

    indexes = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    print(
        f"[db_init] Verified: {len(tables)} tables, "
        f"{len(triggers)} triggers, {len(indexes)} indexes"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db_path = DB_PATH
    if "--reset" in sys.argv:
        if db_path.exists():
            db_path.unlink()
            print(f"[db_init] Removed existing database: {db_path}")
    init_db(db_path)
