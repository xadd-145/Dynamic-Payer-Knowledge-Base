# =============================================================================
# db_init.py - Database Initialization for DPKB CL_V2
#
# LOCKED CONTRACTS (never violate):
#   1. 3 PRAGMAs on EVERY connection: foreign_keys, journal_mode=WAL, busy_timeout
#   2. Python-only UTC timestamps - NEVER SQLite DEFAULT CURRENT_TIMESTAMP
#   3. is_published set ONLY by DB triggers - never by application code
#   4. Named CREATE UNIQUE INDEX statements (never inline UNIQUE on column)
#   5. All CHECK constraints must match constants.py exactly
# =============================================================================

import sqlite3
import os
from pathlib import Path
from datetime import datetime, timezone

from constants import (
    SOURCE_TYPE_CODES,
    RULE_TOPIC_CODES,
    SERVICE_TYPE_CODES,
    UB04_ANCHOR_TYPE_CODES,
    LIFECYCLE_STATUS_CODES,
    DATE_QUERY_TYPE_CODES,
    CONFIDENCE_TIER_CODES,
    REVIEW_STATUS_CODES,
    CRAWLER_RUN_STATUS_CODES,
    USER_ROLE,
    build_check_list,
    EXPECTED_TABLE_COUNT,
    EXPECTED_TRIGGER_COUNT,
)

# ---------------------------------------------------------------------------
# DB PATH
# ---------------------------------------------------------------------------
DB_DIR = Path(__file__).parent / 'db'
DB_PATH = DB_DIR / 'dpkb.db'


# ---------------------------------------------------------------------------
# CONNECTION FACTORY - 3 PRAGMAs on every connection, no exceptions
# ---------------------------------------------------------------------------
def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Return a SQLite connection with all required PRAGMAs applied.
    Row factory is set to sqlite3.Row for dict-like access.
    PRAGMA journal_mode=WAL is verified to return 'wal' - raises if not.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    conn.execute('PRAGMA foreign_keys = ON')

    result = conn.execute('PRAGMA journal_mode = WAL').fetchone()
    if result[0].lower() != 'wal':
        conn.close()
        raise RuntimeError(
            f"PRAGMA journal_mode=WAL failed - got '{result[0]}'. "
            "Cannot proceed without WAL mode."
        )

    conn.execute('PRAGMA busy_timeout = 5000')

    return conn


# ---------------------------------------------------------------------------
# SCHEMA DDL - 15 tables
# ---------------------------------------------------------------------------

_DDL_LOOKUP_TABLES = f"""
-- ============================================================
-- TABLE 1: source_types
-- ============================================================
CREATE TABLE IF NOT EXISTS source_types (
    source_type_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type_code TEXT    NOT NULL,
    source_type_name TEXT    NOT NULL,
    description      TEXT,
    created_at       TEXT    NOT NULL,
    CHECK (source_type_code IN ({build_check_list(SOURCE_TYPE_CODES)}))
);

-- ============================================================
-- TABLE 2: rule_topics
-- ============================================================
CREATE TABLE IF NOT EXISTS rule_topics (
    rule_topic_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_code    TEXT    NOT NULL,
    topic_name    TEXT    NOT NULL,
    description   TEXT,
    is_active     INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at    TEXT    NOT NULL,
    CHECK (topic_code IN ({build_check_list(RULE_TOPIC_CODES)}))
);

-- ============================================================
-- TABLE 3: service_types
-- ============================================================
CREATE TABLE IF NOT EXISTS service_types (
    service_type_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    service_type_code TEXT    NOT NULL,
    service_type_name TEXT    NOT NULL,
    description       TEXT,
    created_at        TEXT    NOT NULL,
    CHECK (service_type_code IN ({build_check_list(SERVICE_TYPE_CODES)}))
);

-- ============================================================
-- TABLE 4: ub04_anchor_types
-- ============================================================
CREATE TABLE IF NOT EXISTS ub04_anchor_types (
    ub04_anchor_type_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    anchor_type_code      TEXT    NOT NULL,
    anchor_type_name      TEXT    NOT NULL,
    description           TEXT,
    created_at            TEXT    NOT NULL,
    CHECK (anchor_type_code IN ({build_check_list(UB04_ANCHOR_TYPE_CODES)}))
);

-- ============================================================
-- TABLE 5: ub04_anchor_codes
-- ============================================================
CREATE TABLE IF NOT EXISTS ub04_anchor_codes (
    ub04_anchor_code_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    ub04_anchor_type_id   INTEGER NOT NULL REFERENCES ub04_anchor_types(ub04_anchor_type_id),
    anchor_code           TEXT    NOT NULL,
    anchor_label          TEXT    NOT NULL,
    anchor_description    TEXT,
    is_active             INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at            TEXT    NOT NULL
);
"""

_DDL_INGESTION_TABLES = f"""
-- ============================================================
-- TABLE 6: source_documents
-- ============================================================
CREATE TABLE IF NOT EXISTS source_documents (
    source_document_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type_id     INTEGER NOT NULL REFERENCES source_types(source_type_id),
    source_url         TEXT    NOT NULL,
    document_title     TEXT,
    document_version   TEXT,
    published_date     TEXT,
    retrieved_at       TEXT    NOT NULL,
    sha256_hash        TEXT    NOT NULL,
    file_path_local    TEXT,
    doc_type           TEXT,
    authority_rank     INTEGER NOT NULL DEFAULT 5 CHECK (authority_rank BETWEEN 1 AND 10),
    is_current         INTEGER NOT NULL DEFAULT 1 CHECK (is_current IN (0, 1)),
    created_at         TEXT    NOT NULL
);

-- ============================================================
-- TABLE 7: policy_fragments
-- ============================================================
CREATE TABLE IF NOT EXISTS policy_fragments (
    policy_fragment_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_document_id             INTEGER NOT NULL REFERENCES source_documents(source_document_id),
    fragment_text_raw              TEXT    NOT NULL,
    fragment_sequence_number       INTEGER,
    page_number_start              INTEGER,
    page_number_end                INTEGER,
    section_reference              TEXT,
    extracted_effective_start_date TEXT,
    extracted_effective_end_date   TEXT,
    date_role_label                TEXT,
    detected_anchor_type_code      TEXT,
    detected_anchor_value          TEXT,
    confidence_score               REAL    NOT NULL DEFAULT 0.0,
    confidence_tier                TEXT    NOT NULL DEFAULT 'TIER_3',
    review_status                  TEXT    NOT NULL DEFAULT 'pending',
    reviewed_by                    TEXT,
    reviewed_at                    TEXT,
    review_notes                   TEXT,
    created_at                     TEXT    NOT NULL,
    CHECK (confidence_tier IN ({build_check_list(CONFIDENCE_TIER_CODES)})),
    CHECK (review_status   IN ({build_check_list(REVIEW_STATUS_CODES)}))
);
"""

_DDL_CORE_RULE_TABLES = f"""
-- ============================================================
-- TABLE 8: atomic_rules
-- Stable rule identity that never changes across versions.
-- rule_code is permanent (e.g. ER-001). Versions track content changes.
-- ============================================================
CREATE TABLE IF NOT EXISTS atomic_rules (
    atomic_rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_topic_id  INTEGER NOT NULL REFERENCES rule_topics(rule_topic_id),
    service_type_id INTEGER REFERENCES service_types(service_type_id),
    rule_code      TEXT    NOT NULL,
    rule_name      TEXT    NOT NULL,
    is_exception   INTEGER NOT NULL DEFAULT 0 CHECK (is_exception IN (0, 1)),
    specificity    INTEGER NOT NULL DEFAULT 1 CHECK (specificity BETWEEN 1 AND 10),
    created_at     TEXT    NOT NULL
);

-- ============================================================
-- TABLE 9: rule_versions
-- Versioned content. effective_end_date = NULL means currently active.
-- is_published managed by DB trigger ONLY - never set by app code.
-- ============================================================
CREATE TABLE IF NOT EXISTS rule_versions (
    rule_version_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    atomic_rule_id         INTEGER NOT NULL REFERENCES atomic_rules(atomic_rule_id),
    version_number         INTEGER NOT NULL DEFAULT 1,
    normalized_rule_text   TEXT    NOT NULL,
    effective_start_date   TEXT    NOT NULL,
    effective_end_date     TEXT,
    lifecycle_status       TEXT    NOT NULL DEFAULT 'draft',
    is_published           INTEGER NOT NULL DEFAULT 0 CHECK (is_published IN (0, 1)),
    exception_flag         INTEGER NOT NULL DEFAULT 0 CHECK (exception_flag IN (0, 1)),
    superseded_by_version_id INTEGER REFERENCES rule_versions(rule_version_id),
    created_at             TEXT    NOT NULL,
    updated_at             TEXT    NOT NULL,
    CHECK (lifecycle_status IN ({build_check_list(LIFECYCLE_STATUS_CODES)}))
);

-- ============================================================
-- TABLE 10: rule_version_anchors
-- Links a rule version to its UB-04 form field anchor(s).
-- ============================================================
CREATE TABLE IF NOT EXISTS rule_version_anchors (
    rule_version_anchor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_version_id        INTEGER NOT NULL REFERENCES rule_versions(rule_version_id),
    ub04_anchor_code_id    INTEGER NOT NULL REFERENCES ub04_anchor_codes(ub04_anchor_code_id),
    is_primary             INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
    created_at             TEXT    NOT NULL
);

-- ============================================================
-- TABLE 11: rule_evidence_links
-- Full audit trail - every rule version traced to source document.
-- ============================================================
CREATE TABLE IF NOT EXISTS rule_evidence_links (
    rule_evidence_link_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_version_id        INTEGER NOT NULL REFERENCES rule_versions(rule_version_id),
    source_document_id     INTEGER NOT NULL REFERENCES source_documents(source_document_id),
    policy_fragment_id     INTEGER REFERENCES policy_fragments(policy_fragment_id),
    page_number_start      INTEGER,
    page_number_end        INTEGER,
    section_reference      TEXT,
    citation_text          TEXT,
    created_at             TEXT    NOT NULL
);

-- ============================================================
-- TABLE 12: resolution_logs
-- Full query audit trail - every resolver call logged.
-- ============================================================
CREATE TABLE IF NOT EXISTS resolution_logs (
    resolution_log_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    query_topic_code          TEXT    NOT NULL,
    query_date                TEXT    NOT NULL,
    query_date_type           TEXT    NOT NULL,
    query_anchor_type_code    TEXT,
    query_anchor_value        TEXT,
    resolution_status         TEXT    NOT NULL,
    candidate_rule_version_ids TEXT,
    winning_rule_version_id   INTEGER REFERENCES rule_versions(rule_version_id),
    decision_trace            TEXT,
    queried_by                TEXT,
    queried_at                TEXT    NOT NULL,
    CHECK (query_date_type IN ({build_check_list(DATE_QUERY_TYPE_CODES)}))
);
"""

_DDL_V2_TABLES = f"""
-- ============================================================
-- TABLE 13: users  (V2 NEW)
-- JWT auth - passwords stored as bcrypt hashes, never plaintext.
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    user_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    NOT NULL,
    password_hash TEXT    NOT NULL,
    role          TEXT    NOT NULL DEFAULT 'staff',
    is_active     INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL,
    CHECK (role IN ({build_check_list(USER_ROLE)}))
);

-- ============================================================
-- TABLE 14: notifications  (V2 NEW)
-- Real-time rule update alerts for Staff Portal banner.
-- ============================================================
CREATE TABLE IF NOT EXISTS notifications (
    notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message         TEXT    NOT NULL,
    rule_code       TEXT,
    triggered_by    TEXT,
    is_read         INTEGER NOT NULL DEFAULT 0 CHECK (is_read IN (0, 1)),
    created_at      TEXT    NOT NULL
);

-- ============================================================
-- TABLE 15: crawler_runs  (V2 NEW)
-- Log of every Module A execution including manual admin triggers.
-- ============================================================
CREATE TABLE IF NOT EXISTS crawler_runs (
    run_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at         TEXT    NOT NULL,
    completed_at       TEXT,
    documents_found    INTEGER NOT NULL DEFAULT 0,
    documents_added    INTEGER NOT NULL DEFAULT 0,
    documents_skipped  INTEGER NOT NULL DEFAULT 0,
    documents_failed   INTEGER NOT NULL DEFAULT 0,
    run_status         TEXT    NOT NULL DEFAULT 'running',
    notes              TEXT,
    CHECK (run_status IN ({build_check_list(CRAWLER_RUN_STATUS_CODES)}))
);
"""

# ---------------------------------------------------------------------------
# TRIGGERS - is_published sync (2 triggers, unchanged from V1)
# Application code NEVER sets is_published directly.
# ---------------------------------------------------------------------------

_DDL_TRIGGERS = """
-- ============================================================
-- TRIGGER 1: set is_published = 1 when lifecycle_status → 'published'
-- ============================================================
CREATE TRIGGER IF NOT EXISTS trg_set_is_published
AFTER UPDATE OF lifecycle_status ON rule_versions
WHEN NEW.lifecycle_status = 'published'
BEGIN
    UPDATE rule_versions
    SET    is_published = 1,
           updated_at   = NEW.updated_at
    WHERE  rule_version_id = NEW.rule_version_id;
END;

-- ============================================================
-- TRIGGER 2: set is_published = 0 when lifecycle_status leaves 'published'
-- ============================================================
CREATE TRIGGER IF NOT EXISTS trg_clear_is_published
AFTER UPDATE OF lifecycle_status ON rule_versions
WHEN OLD.lifecycle_status = 'published' AND NEW.lifecycle_status != 'published'
BEGIN
    UPDATE rule_versions
    SET    is_published = 0,
           updated_at   = NEW.updated_at
    WHERE  rule_version_id = NEW.rule_version_id;
END;
"""

# ---------------------------------------------------------------------------
# INDEXES - all named, no inline UNIQUE constraints on column definitions
# ---------------------------------------------------------------------------

_DDL_INDEXES = """
-- Source types
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_type_code
    ON source_types(source_type_code);

-- Rule topics
CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_code
    ON rule_topics(topic_code);

-- Service types
CREATE UNIQUE INDEX IF NOT EXISTS uq_service_type_code
    ON service_types(service_type_code);

-- UB-04 anchor types
CREATE UNIQUE INDEX IF NOT EXISTS uq_anchor_type_code
    ON ub04_anchor_types(anchor_type_code);

-- UB-04 anchor codes (composite - same code can belong to one type only)
CREATE UNIQUE INDEX IF NOT EXISTS uq_anchor_code_per_type
    ON ub04_anchor_codes(ub04_anchor_type_id, anchor_code);

-- Source documents - SHA-256 deduplication
CREATE UNIQUE INDEX IF NOT EXISTS uq_sha256_hash
    ON source_documents(sha256_hash);

CREATE INDEX IF NOT EXISTS idx_source_documents_retrieved_at
    ON source_documents(retrieved_at);

-- Policy fragments
CREATE INDEX IF NOT EXISTS idx_policy_fragments_source_document
    ON policy_fragments(source_document_id);

CREATE INDEX IF NOT EXISTS idx_policy_fragments_confidence_tier
    ON policy_fragments(confidence_tier);

CREATE INDEX IF NOT EXISTS idx_policy_fragments_review_status
    ON policy_fragments(review_status);

-- Atomic rules
CREATE UNIQUE INDEX IF NOT EXISTS uq_rule_code
    ON atomic_rules(rule_code);

CREATE INDEX IF NOT EXISTS idx_atomic_rules_topic
    ON atomic_rules(rule_topic_id);

-- Rule versions - primary temporal query index
CREATE INDEX IF NOT EXISTS idx_rule_versions_atomic_rule
    ON rule_versions(atomic_rule_id);

CREATE INDEX IF NOT EXISTS idx_rule_versions_effective_dates
    ON rule_versions(effective_start_date, effective_end_date);

CREATE INDEX IF NOT EXISTS idx_rule_versions_lifecycle_status
    ON rule_versions(lifecycle_status);

CREATE INDEX IF NOT EXISTS idx_rule_versions_is_published
    ON rule_versions(is_published);

-- Rule version anchors
CREATE INDEX IF NOT EXISTS idx_rule_version_anchors_version
    ON rule_version_anchors(rule_version_id);

-- Rule evidence links
CREATE INDEX IF NOT EXISTS idx_rule_evidence_links_version
    ON rule_evidence_links(rule_version_id);

-- Resolution logs
CREATE INDEX IF NOT EXISTS idx_resolution_logs_queried_at
    ON resolution_logs(queried_at);

CREATE INDEX IF NOT EXISTS idx_resolution_logs_topic
    ON resolution_logs(query_topic_code);

-- Users (V2)
CREATE UNIQUE INDEX IF NOT EXISTS uq_username
    ON users(username);

CREATE INDEX IF NOT EXISTS idx_user_role
    ON users(role);

-- Notifications (V2)
CREATE INDEX IF NOT EXISTS idx_notifications_is_read
    ON notifications(is_read);

CREATE INDEX IF NOT EXISTS idx_notifications_created_at
    ON notifications(created_at);

-- Crawler runs (V2)
CREATE INDEX IF NOT EXISTS idx_crawler_runs_started_at
    ON crawler_runs(started_at);

CREATE INDEX IF NOT EXISTS idx_crawler_runs_run_status
    ON crawler_runs(run_status);
"""


# ---------------------------------------------------------------------------
# FULL SCHEMA = all tables + triggers + indexes
# ---------------------------------------------------------------------------
FULL_SCHEMA = (
    _DDL_LOOKUP_TABLES
    + _DDL_INGESTION_TABLES
    + _DDL_CORE_RULE_TABLES
    + _DDL_V2_TABLES
    + _DDL_TRIGGERS
    + _DDL_INDEXES
)


# ---------------------------------------------------------------------------
# INIT FUNCTION
# ---------------------------------------------------------------------------
def init_db(db_path: Path = DB_PATH, verbose: bool = True) -> None:
    """
    Initialize the DPKB SQLite database.
    Creates the db/ directory if it does not exist.
    Applies full schema - all CREATE IF NOT EXISTS statements are idempotent.
    Validates expected table and trigger counts before returning.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection(db_path)
    try:
        conn.executescript(FULL_SCHEMA)
        conn.commit()

        # ---------------------------------------------------------------
        # Validation - count tables and triggers
        # ---------------------------------------------------------------
        table_rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        ).fetchall()
        trigger_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name"
        ).fetchall()
        index_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
        ).fetchall()

        table_count   = len(table_rows)
        trigger_count = len(trigger_rows)
        index_count   = len(index_rows)

        if verbose:
            print(f"\nDPKB Database Initialized: {db_path}")
            print(f"  Tables  : {table_count}  (expected {EXPECTED_TABLE_COUNT})")
            print(f"  Triggers: {trigger_count}  (expected {EXPECTED_TRIGGER_COUNT})")
            print(f"  Indexes : {index_count}")
            print()
            print("  Tables created:")
            for row in table_rows:
                print(f"    - {row['name']}")
            print()
            print("  Triggers created:")
            for row in trigger_rows:
                print(f"    - {row['name']}")

        if table_count != EXPECTED_TABLE_COUNT:
            raise RuntimeError(
                f"Table count mismatch: got {table_count}, "
                f"expected {EXPECTED_TABLE_COUNT}. "
                "Check FULL_SCHEMA for missing or duplicate CREATE TABLE statements."
            )

        if trigger_count != EXPECTED_TRIGGER_COUNT:
            raise RuntimeError(
                f"Trigger count mismatch: got {trigger_count}, "
                f"expected {EXPECTED_TRIGGER_COUNT}. "
                "Check _DDL_TRIGGERS for missing or duplicate CREATE TRIGGER statements."
            )

        if verbose:
            print(f"\n  VALIDATION PASSED: {table_count} tables, "
                  f"{trigger_count} triggers, {index_count} indexes.\n")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# TIMESTAMP HELPER - Python-only UTC timestamps, never SQLite DEFAULT
# ---------------------------------------------------------------------------
def utc_now() -> str:
    """Return current UTC time as ISO-8601 string: YYYY-MM-DDTHH:MM:SS"""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')


# ---------------------------------------------------------------------------
# MAIN - run directly to initialize or re-initialize the database
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    print("=" * 60)
    print(" DPKB - Database Initialization")
    print(" CL_V2  |  BIG x Salud Revenue Partners")
    print("=" * 60)

    init_db(verbose=True)
    print("Database ready. Run seed_sample_data.py --reset to populate.")