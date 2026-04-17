# =============================================================================
# constants.py - Single Source of Truth for ALL Enums and CHECK Constraints
# DPKB CL_V2  |  
# =============================================================================

# ---------------------------------------------------------------------------
# SOURCE TYPES
# ---------------------------------------------------------------------------
SOURCE_TYPE_CODES = ('MANUAL', 'BULLETIN', 'GUIDELINE')

# ---------------------------------------------------------------------------
# RULE TOPICS  (15 total - V1: ER, INPT, OUTPT, THER, LAB | V2 adds 10)
# ---------------------------------------------------------------------------
RULE_TOPIC_CODES = (
    'ER',
    'INPT',
    'OUTPT',
    'THER',
    'LAB',
    'OBS',
    'PSYCH',
    'REHAB',
    'NEWBORN',
    'MATERNITY',
    'DIALYSIS',
    'PHARMACY',
    'TRANSPORT',
    'PRIOR_AUTH',
    'DRG',
)

# ---------------------------------------------------------------------------
# SERVICE TYPES
# ---------------------------------------------------------------------------
SERVICE_TYPE_CODES = (
    'INPATIENT',
    'OUTPATIENT',
    'EMERGENCY',
    'AMBULATORY_SURGERY',
    'HOME_HEALTH',
)

# ---------------------------------------------------------------------------
# UB-04 ANCHOR TYPES  (10 types + ANCHOR_OTHER catch-all = 11 total)
# ---------------------------------------------------------------------------
UB04_ANCHOR_TYPE_CODES = (
    'revenue_code',
    'type_of_bill',
    'condition_code',
    'modifier',
    'occurrence_code',
    'occurrence_span_code',
    'value_code',
    'diagnosis_code',
    'procedure_code',
    'place_of_service',
    'ANCHOR_OTHER',
)

# ---------------------------------------------------------------------------
# RULE LIFECYCLE STATUS
# ---------------------------------------------------------------------------
LIFECYCLE_STATUS_CODES = (
    'draft',
    'under_review',
    'approved',
    'published',
    'rejected',
    'archived',
)

# ---------------------------------------------------------------------------
# DATE QUERY TYPE  (used by resolver and query UI)
# ---------------------------------------------------------------------------
DATE_QUERY_TYPE_CODES = (
    'date_of_service',
    'date_of_discharge',
)

# ---------------------------------------------------------------------------
# CONFIDENCE TIERS  (Module B scoring output)
# ---------------------------------------------------------------------------
CONFIDENCE_TIER_CODES = (
    'TIER_1',   # >= 75 pts - auto-publish
    'TIER_2',   # 40-74 pts - Admin review queue
    'TIER_3',   # < 40 pts  - SME escalation queue
)

# ---------------------------------------------------------------------------
# REVIEW STATUS  (policy_fragments.review_status)
# ---------------------------------------------------------------------------
REVIEW_STATUS_CODES = (
    'pending',
    'approved',
    'rejected',
    'escalated',
)

# ---------------------------------------------------------------------------
# CRAWLER RUN STATUS  (crawler_runs.run_status)
# ---------------------------------------------------------------------------
CRAWLER_RUN_STATUS_CODES = (
    'running',
    'completed',
    'failed',
    'partial',
    'audit',
)

# ---------------------------------------------------------------------------
# USER ROLES  (V2 NEW - users.role)
# ---------------------------------------------------------------------------
USER_ROLE = (
    'staff',
    'admin',
    'sme',
)

# ---------------------------------------------------------------------------
# RESOLUTION STATES  (resolver output states - Module E)
# ---------------------------------------------------------------------------
RESOLUTION_STATUS_CODES = (
    'RESOLVED',
    'NO_MATCH',
    'MULTIPLE_MATCH',
    'ERROR',
)

# ---------------------------------------------------------------------------
# AUTHORITY RANKS  (source_documents.authority_rank - higher = more authoritative)
# ---------------------------------------------------------------------------
AUTHORITY_RANK_MIN = 1
AUTHORITY_RANK_MAX = 10

# ---------------------------------------------------------------------------
# HELPER: build SQL IN-list string from a tuple of constants
# Usage: build_check_list(SOURCE_TYPE_CODES)  →  "'MANUAL','BULLETIN','GUIDELINE'"
# ---------------------------------------------------------------------------
def build_check_list(values: tuple) -> str:
    return ','.join(f"'{v}'" for v in values)


# ---------------------------------------------------------------------------
# EXPECTED TABLE COUNT - update whenever tables are added
# ---------------------------------------------------------------------------
EXPECTED_TABLE_COUNT = 15
EXPECTED_TRIGGER_COUNT = 2