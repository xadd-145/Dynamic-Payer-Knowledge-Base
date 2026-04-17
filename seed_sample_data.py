# =============================================================================
# seed_sample_data.py - Seed Sample Data for DPKB CL_V2
# BIG x Salud Revenue Partners
#
# Usage:
#   python seed_sample_data.py            ← safe insert (skips duplicates)
#   python seed_sample_data.py --reset    ← drops db, reinits, full reseed
#
# RULES:
#   - NEVER remove existing V1 seed rows - only ADD
#   - All timestamps use utc_now() from db_init - never SQLite DEFAULT
#   - Passwords stored as bcrypt hashes - never plaintext
#   - After seeding, all 6 V1 retrieval tests must still pass
# =============================================================================

import sys
import os
from pathlib import Path

import bcrypt

from db_init import get_connection, init_db, utc_now, DB_PATH
from constants import (
    SOURCE_TYPE_CODES,
    RULE_TOPIC_CODES,
    SERVICE_TYPE_CODES,
    UB04_ANCHOR_TYPE_CODES,
    USER_ROLE,
)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _hash_password(plain: str) -> str:
    """Return bcrypt hash of a plaintext password."""
    return bcrypt.hashpw(plain.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def _insert_or_skip(conn, table: str, unique_col: str, unique_val, row: dict) -> int:
    """
    Insert a row only if unique_col = unique_val does not already exist.
    Returns the rowid of the existing or newly inserted row.
    """
    existing = conn.execute(
        f"SELECT rowid FROM {table} WHERE {unique_col} = ?", (unique_val,)
    ).fetchone()
    if existing:
        return existing[0]
    cols = ', '.join(row.keys())
    placeholders = ', '.join('?' for _ in row)
    cursor = conn.execute(
        f"INSERT INTO {table} ({cols}) VALUES ({placeholders})",
        list(row.values())
    )
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# 1. SOURCE TYPES  (3 rows - V1 unchanged)
# ---------------------------------------------------------------------------
SOURCE_TYPES_DATA = [
    {
        'source_type_code': 'MANUAL',
        'source_type_name': 'Provider Manual',
        'description': 'Official eMedNY provider manuals (Inpatient, Outpatient/APG, General Institutional)',
        'created_at': utc_now(),
    },
    {
        'source_type_code': 'BULLETIN',
        'source_type_name': 'Medicaid Update Bulletin',
        'description': 'Monthly NY Medicaid Update bulletins from health.ny.gov',
        'created_at': utc_now(),
    },
    {
        'source_type_code': 'GUIDELINE',
        'source_type_name': 'Billing Guideline',
        'description': 'Supplementary billing guidelines and policy documents',
        'created_at': utc_now(),
    },
]


def seed_source_types(conn) -> dict:
    """Returns {code: source_type_id}"""
    id_map = {}
    for row in SOURCE_TYPES_DATA:
        rid = _insert_or_skip(conn, 'source_types', 'source_type_code', row['source_type_code'], row)
        id_map[row['source_type_code']] = rid
    print(f"  source_types     : {len(id_map)} rows")
    return id_map


# ---------------------------------------------------------------------------
# 2. SERVICE TYPES  (5 rows - V1 unchanged)
# ---------------------------------------------------------------------------
SERVICE_TYPES_DATA = [
    ('INPATIENT',         'Inpatient',          'Hospital inpatient admission services'),
    ('OUTPATIENT',        'Outpatient',         'Hospital outpatient department services'),
    ('EMERGENCY',         'Emergency',          'Emergency room and emergency department services'),
    ('AMBULATORY_SURGERY','Ambulatory Surgery', 'Ambulatory surgery center services'),
    ('HOME_HEALTH',       'Home Health',        'Home health agency services'),
]


def seed_service_types(conn) -> dict:
    id_map = {}
    for code, name, desc in SERVICE_TYPES_DATA:
        rid = _insert_or_skip(conn, 'service_types', 'service_type_code', code, {
            'service_type_code': code,
            'service_type_name': name,
            'description': desc,
            'created_at': utc_now(),
        })
        id_map[code] = rid
    print(f"  service_types    : {len(id_map)} rows")
    return id_map


# ---------------------------------------------------------------------------
# 3. RULE TOPICS  (15 total - V1: 5, V2 adds 10)
# ---------------------------------------------------------------------------
RULE_TOPICS_DATA = [
    # --- V1 topics ---
    ('ER',         'Emergency Room Services',      'Revenue code 0450, TOB 131, condition code G0'),
    ('INPT',       'Inpatient Admission',           'Two-midnight rule, TOB 111, DRG payment, prior auth'),
    ('OUTPT',      'Outpatient Surgery',            'TOB 131/831, revenue code 0360, modifier 73'),
    ('THER',       'Therapy PT/OT/ST',              'Modifiers GO/GP/GN, occurrence codes 17/29/30'),
    ('LAB',        'Laboratory and Radiology',      'Revenue codes 030x-037x, 040x-047x, 0351'),
    # --- V2 new topics ---
    ('OBS',        'Observation Services',          'Revenue code 0762, G0378 per hour, OBS vs INPT'),
    ('PSYCH',      'Psychiatric Care',              'Article 28, age restrictions, revenue codes 024x'),
    ('REHAB',      'Rehabilitation Services',       'Revenue codes 029x, distinct part unit billing'),
    ('NEWBORN',    'Newborn and Neonatal',          'Auto Medicaid enrollment, TOB 111, birth reporting'),
    ('MATERNITY',  'Maternity and Obstetrics',      'Revenue code 072x, C-section DRGs 370/371'),
    ('DIALYSIS',   'Dialysis Services',             'Revenue code 082x, composite rate, ESRD'),
    ('PHARMACY',   'Pharmacy and IV Drugs',         'Revenue codes 025x/063x, self-administered exclusions'),
    ('TRANSPORT',  'Transportation/Ambulance',      'Hospital responsibility, no separate Medicaid billing'),
    ('PRIOR_AUTH', 'Prior Authorization',           'Non-emergency inpatient PA, OOS prior approval'),
    ('DRG',        'DRG Payment and Billing',       'Top 20 DRGs, APR-DRG weights, transfer rules'),
]


def seed_rule_topics(conn) -> dict:
    id_map = {}
    for code, name, desc in RULE_TOPICS_DATA:
        rid = _insert_or_skip(conn, 'rule_topics', 'topic_code', code, {
            'topic_code': code,
            'topic_name': name,
            'description': desc,
            'is_active': 1,
            'created_at': utc_now(),
        })
        id_map[code] = rid
    print(f"  rule_topics      : {len(id_map)} rows  (expected 15)")
    return id_map


# ---------------------------------------------------------------------------
# 4. UB-04 ANCHOR TYPES  (10 + ANCHOR_OTHER = 11 total)
# ---------------------------------------------------------------------------
ANCHOR_TYPES_DATA = [
    # --- V1 types ---
    ('revenue_code',       'Revenue Code',           '4-digit service category code (e.g. 0450 ER, 0120 Room/Board)'),
    ('type_of_bill',       'Type of Bill',           '3-digit facility type + bill classification (e.g. 111, 131)'),
    ('condition_code',     'Condition Code',         '2-digit claim condition (e.g. G0 multiple ER visits, 44 IP to OP)'),
    ('modifier',           'Modifier',               '2-letter procedure modifier (e.g. GO Occupational, GP Physical)'),
    # --- V2 new types ---
    ('occurrence_code',       'Occurrence Code',        '2-digit event date code (e.g. 17 OT plan, 29 PT plan, 30 ST plan)'),
    ('occurrence_span_code',  'Occurrence Span Code',   '2-digit date range code (e.g. 74 Noncovered level of care)'),
    ('value_code',            'Value Code',             '2-digit monetary amount code (e.g. 80 Covered days)'),
    ('diagnosis_code',        'Diagnosis Code',         'ICD-10-CM code (e.g. Z38.00 Newborn, F32.9 Depression)'),
    ('procedure_code',        'Procedure Code',         'HCPCS/CPT code (e.g. G0378 Observation hour)'),
    ('place_of_service',      'Place of Service',       'Location indicator (e.g. 21 Inpatient, 22 Outpatient hospital)'),
    # --- Catch-all ---
    ('ANCHOR_OTHER',          'Other / Unclassified',   'Catch-all for codes not matching known anchor types - routes to SME'),
]


def seed_ub04_anchor_types(conn) -> dict:
    id_map = {}
    for code, name, desc in ANCHOR_TYPES_DATA:
        rid = _insert_or_skip(conn, 'ub04_anchor_types', 'anchor_type_code', code, {
            'anchor_type_code': code,
            'anchor_type_name': name,
            'description': desc,
            'created_at': utc_now(),
        })
        id_map[code] = rid
    print(f"  ub04_anchor_types: {len(id_map)} rows  (expected 11)")
    return id_map


# ---------------------------------------------------------------------------
# 5. UB-04 ANCHOR CODES  (specific values - V1 core set)
# ---------------------------------------------------------------------------
def seed_ub04_anchor_codes(conn, anchor_type_id_map: dict) -> dict:
    """
    Seed specific anchor code values into ub04_anchor_codes.
    Returns {anchor_code: ub04_anchor_code_id}
    """
    ANCHOR_CODES_DATA = [
        # Revenue codes
        ('revenue_code', '0450', 'Emergency Room',            'Emergency room services'),
        ('revenue_code', '0120', 'Room and Board - Semi',     'Semi-private room accommodation'),
        ('revenue_code', '0360', 'Operating Room',            'Operating room services'),
        ('revenue_code', '0762', 'Observation Room',          'Observation services - per hour billing'),
        ('revenue_code', '0300', 'Laboratory',                'Clinical laboratory services'),
        ('revenue_code', '0351', 'CT Scan',                   'Computed tomography scanning'),
        ('revenue_code', '0320', 'Radiology - Diagnostic',    'Diagnostic radiology services'),
        ('revenue_code', '0240', 'Psych - All',               'Psychiatric care services'),
        ('revenue_code', '0290', 'Rehabilitation',            'Physical rehabilitation services'),
        ('revenue_code', '0720', 'Labor and Delivery',        'Labor/delivery room charges'),
        ('revenue_code', '0820', 'Hemodialysis - Outpatient', 'Outpatient hemodialysis services'),
        ('revenue_code', '0250', 'Pharmacy',                  'General pharmacy charges'),
        ('revenue_code', '0636', 'IV Solutions',              'Intravenous drug administration'),
        # Type of bill
        ('type_of_bill', '111',  'Hospital Inpatient - Admit Through Discharge', 'Full inpatient claim'),
        ('type_of_bill', '131',  'Hospital Outpatient',                           'Outpatient hospital claim'),
        ('type_of_bill', '831',  'Ambulatory Surgery Center',                    'ASC facility claim'),
        ('type_of_bill', '761',  'Psychiatric Facility - Inpatient',             'Inpatient psychiatric claim'),
        # Condition codes
        ('condition_code', 'G0', 'Multiple ER Visits Same Day',   'Multiple ER visits on same date in same revenue center'),
        ('condition_code', '44', 'Inpatient to Outpatient',       'Admission changed from inpatient to outpatient'),
        ('condition_code', '41', 'Partial Hospitalization',       'Partial hospitalization program'),
        # Modifiers
        ('modifier', 'GO', 'Occupational Therapy',   'Services under outpatient OT plan of care'),
        ('modifier', 'GP', 'Physical Therapy',       'Services under outpatient PT plan of care'),
        ('modifier', 'GN', 'Speech-Language Pathology', 'Services under outpatient SLP plan of care'),
        ('modifier', '73', 'Discontinued Procedure', 'Procedure discontinued prior to anesthesia'),
        # Occurrence codes
        ('occurrence_code', '17', 'OT Plan Date',  'Date outpatient OT plan established or last reviewed'),
        ('occurrence_code', '29', 'PT Plan Date',  'Date outpatient PT plan established or last reviewed'),
        ('occurrence_code', '30', 'ST Plan Date',  'Date outpatient ST plan established or last reviewed'),
        ('occurrence_code', '44', 'OT Treatment Start', 'Date treatment started for OT'),
        ('occurrence_code', '39', 'PT Treatment Start', 'Date treatment started for PT'),
        ('occurrence_code', '45', 'ST Treatment Start', 'Date treatment started for ST'),
        # Procedure codes
        ('procedure_code', 'G0378', 'Hospital Observation Service', 'Observation care per hour - revenue code 0762'),
        ('procedure_code', 'G0379', 'Direct Admission to Observation', 'Direct referral for hospital observation'),
    ]

    id_map = {}
    count = 0
    for type_code, anchor_code, label, desc in ANCHOR_CODES_DATA:
        type_id = anchor_type_id_map.get(type_code)
        if not type_id:
            continue
        # Composite uniqueness: type_id + anchor_code
        existing = conn.execute(
            "SELECT ub04_anchor_code_id FROM ub04_anchor_codes "
            "WHERE ub04_anchor_type_id = ? AND anchor_code = ?",
            (type_id, anchor_code)
        ).fetchone()
        if existing:
            id_map[anchor_code] = existing[0]
            continue
        cursor = conn.execute(
            "INSERT INTO ub04_anchor_codes "
            "(ub04_anchor_type_id, anchor_code, anchor_label, anchor_description, is_active, created_at) "
            "VALUES (?, ?, ?, ?, 1, ?)",
            (type_id, anchor_code, label, desc, utc_now())
        )
        id_map[anchor_code] = cursor.lastrowid
        count += 1

    print(f"  ub04_anchor_codes: {len(id_map)} rows")
    return id_map


# ---------------------------------------------------------------------------
# 6. SOURCE DOCUMENTS  (2 sample documents - V1 unchanged)
# ---------------------------------------------------------------------------
def seed_source_documents(conn, source_type_id_map: dict) -> dict:
    manual_id = source_type_id_map['MANUAL']
    bulletin_id = source_type_id_map['BULLETIN']

    docs = [
        {
            'source_type_id': manual_id,
            'source_url': 'https://www.emedny.org/ProviderManuals/Inpatient/PDFS/Inpatient_Billing_Guidelines.pdf',
            'document_title': 'eMedNY Inpatient Billing Guidelines',
            'document_version': '2024',
            'published_date': '2024-01-01',
            'retrieved_at': utc_now(),
            'sha256_hash': 'aabbcc001122334455667788990011223344556677889900aabbcc0011223344',
            'file_path_local': 'data/raw/inpatient_billing_guidelines.pdf',
            'doc_type': 'MANUAL',
            'authority_rank': 9,
            'is_current': 1,
            'created_at': utc_now(),
        },
        {
            'source_type_id': bulletin_id,
            'source_url': 'https://www.health.ny.gov/health_care/medicaid/program/update/2023/no2023-12.pdf',
            'document_title': 'NY Medicaid Update - December 2023',
            'document_version': 'December 2023',
            'published_date': '2023-12-01',
            'retrieved_at': utc_now(),
            'sha256_hash': 'ddeeff001122334455667788990011223344556677889900ddeeff0011223344',
            'file_path_local': 'data/raw/medicaid_update_dec2023.pdf',
            'doc_type': 'BULLETIN',
            'authority_rank': 8,
            'is_current': 1,
            'created_at': utc_now(),
        },
    ]

    id_map = {}
    for doc in docs:
        rid = _insert_or_skip(conn, 'source_documents', 'sha256_hash', doc['sha256_hash'], doc)
        id_map[doc['document_title']] = rid

    print(f"  source_documents : {len(id_map)} rows")
    return id_map


# ---------------------------------------------------------------------------
# 7. ATOMIC RULES + RULE VERSIONS  (V1 seed rules - fully preserved)
#    5 rules across ER, INPT, OUTPT, THER, LAB
#    Each has at least one version. ER-001 has 2 versions (supersession test).
# ---------------------------------------------------------------------------
def seed_rules(conn, topic_id_map: dict, service_type_id_map: dict,
               source_doc_id_map: dict, anchor_code_id_map: dict) -> None:
    """
    Seed atomic_rules + rule_versions + rule_version_anchors + rule_evidence_links.
    All version lifecycle_status = 'published' (trigger sets is_published = 1).
    """
    now = utc_now()
    doc_id = list(source_doc_id_map.values())[0]  # inpatient manual

    # ------------------------------------------------------------------
    # Helper: insert atomic_rule + one or more versions
    # ------------------------------------------------------------------
    def _rule(rule_code, topic_code, service_code, rule_name, is_exception, specificity, versions):
        existing = conn.execute(
            "SELECT atomic_rule_id FROM atomic_rules WHERE rule_code = ?", (rule_code,)
        ).fetchone()
        if existing:
            return  # already seeded - skip

        rule_id = conn.execute(
            "INSERT INTO atomic_rules "
            "(rule_topic_id, service_type_id, rule_code, rule_name, is_exception, specificity, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                topic_id_map[topic_code],
                service_type_id_map.get(service_code),
                rule_code, rule_name, is_exception, specificity, now
            )
        ).lastrowid

        version_ids = []
        for v in versions:
            # Insert as 'draft' - will publish individually below
            rv_id = conn.execute(
                "INSERT INTO rule_versions "
                "(atomic_rule_id, version_number, normalized_rule_text, "
                " effective_start_date, effective_end_date, lifecycle_status, "
                " is_published, exception_flag, superseded_by_version_id, "
                " created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'draft', 0, ?, NULL, ?, ?)",
                (
                    rule_id, v['version_number'], v['text'],
                    v['start'], v.get('end'), v.get('exception_flag', 0),
                    now, now
                )
            ).lastrowid
            version_ids.append(rv_id)

            # Anchor
            anchor_code = v.get('anchor_code')
            if anchor_code and anchor_code in anchor_code_id_map:
                conn.execute(
                    "INSERT INTO rule_version_anchors "
                    "(rule_version_id, ub04_anchor_code_id, is_primary, created_at) "
                    "VALUES (?, ?, 1, ?)",
                    (rv_id, anchor_code_id_map[anchor_code], now)
                )

            # Evidence link
            conn.execute(
                "INSERT INTO rule_evidence_links "
                "(rule_version_id, source_document_id, policy_fragment_id, "
                " page_number_start, page_number_end, section_reference, "
                " citation_text, created_at) "
                "VALUES (?, ?, NULL, ?, ?, ?, ?, ?)",
                (
                    rv_id, doc_id,
                    v.get('page_start', 1), v.get('page_end', 1),
                    v.get('section', 'General'),
                    v.get('citation', f'See {rule_code} v{v["version_number"]}'),
                    now
                )
            )

        # Wire supersession chain: v[i] superseded_by v[i+1]
        for i in range(len(version_ids) - 1):
            conn.execute(
                "UPDATE rule_versions SET superseded_by_version_id = ? WHERE rule_version_id = ?",
                (version_ids[i + 1], version_ids[i])
            )

        # Publish each version individually - trigger fires once per version
        for rv_id in version_ids:
            conn.execute(
                "UPDATE rule_versions SET lifecycle_status = 'published', updated_at = ? "
                "WHERE rule_version_id = ?",
                (now, rv_id)
            )

    # ------------------------------------------------------------------
    # ER-001: Two versions - supersession chain (V1 resolver test target)
    # ------------------------------------------------------------------
    _rule(
        rule_code='ER-001',
        topic_code='ER',
        service_code='EMERGENCY',
        rule_name='Emergency Room Revenue Code Requirement',
        is_exception=0,
        specificity=8,
        versions=[
            {
                'version_number': 1,
                'text': (
                    'Emergency room services must be billed under revenue code 0450. '
                    'The Type of Bill must be 131 for outpatient emergency claims. '
                    'Effective for dates of service on or after January 1, 2022.'
                ),
                'start': '2022-01-01',
                'end': '2023-06-30',
                'anchor_code': '0450',
                'page_start': 14, 'page_end': 15,
                'section': 'Section 4.2 Emergency Room Billing',
                'citation': 'eMedNY Inpatient Billing Guidelines, Section 4.2, p. 14',
                'exception_flag': 0,
            },
            {
                'version_number': 2,
                'text': (
                    'Emergency room services must be billed under revenue code 0450. '
                    'The Type of Bill must be 131 for outpatient emergency claims. '
                    'Condition code G0 is required when a beneficiary presents to the '
                    'emergency room more than once on the same date of service. '
                    'Effective for dates of service on or after July 1, 2023.'
                ),
                'start': '2023-07-01',
                'end': None,
                'anchor_code': '0450',
                'page_start': 16, 'page_end': 17,
                'section': 'Section 4.2 Emergency Room Billing (Revised)',
                'citation': 'eMedNY Inpatient Billing Guidelines, Section 4.2 (Revised), p. 16',
                'exception_flag': 0,
            },
        ]
    )

    # ------------------------------------------------------------------
    # ER-002: Condition code G0 rule
    # ------------------------------------------------------------------
    _rule(
        rule_code='ER-002',
        topic_code='ER',
        service_code='EMERGENCY',
        rule_name='Multiple ER Visits - Condition Code G0',
        is_exception=0,
        specificity=7,
        versions=[
            {
                'version_number': 1,
                'text': (
                    'When a beneficiary has multiple medical visits to the emergency room '
                    'on the same date of service within the same revenue center, '
                    'condition code G0 must be reported on the second and subsequent claims. '
                    'Each visit may be submitted on a separate claim. '
                    'Effective for dates of service on or after January 1, 2022.'
                ),
                'start': '2022-01-01',
                'end': None,
                'anchor_code': 'G0',
                'page_start': 18, 'page_end': 18,
                'section': 'Section 4.3 Multiple ER Visits',
                'citation': 'eMedNY Inpatient Billing Guidelines, Section 4.3, p. 18',
            },
        ]
    )

    # ------------------------------------------------------------------
    # INPT-001: Two-midnight rule
    # ------------------------------------------------------------------
    _rule(
        rule_code='INPT-001',
        topic_code='INPT',
        service_code='INPATIENT',
        rule_name='Inpatient Admission - Two-Midnight Rule',
        is_exception=0,
        specificity=9,
        versions=[
            {
                'version_number': 1,
                'text': (
                    'An inpatient admission is appropriate when the admitting physician '
                    'expects the beneficiary to require hospital care spanning at least '
                    'two midnights. Claims must use Type of Bill 111. '
                    'DRG-based reimbursement applies. Prior authorization is required '
                    'for non-emergency admissions. '
                    'Effective for admissions on or after January 1, 2022.'
                ),
                'start': '2022-01-01',
                'end': None,
                'anchor_code': '111',
                'page_start': 22, 'page_end': 24,
                'section': 'Section 5.1 Inpatient Admission Criteria',
                'citation': 'eMedNY Inpatient Billing Guidelines, Section 5.1, p. 22',
            },
        ]
    )

    # ------------------------------------------------------------------
    # THER-001: Therapy modifier requirement
    # ------------------------------------------------------------------
    _rule(
        rule_code='THER-001',
        topic_code='THER',
        service_code='OUTPATIENT',
        rule_name='Outpatient Therapy Modifier Requirement',
        is_exception=0,
        specificity=8,
        versions=[
            {
                'version_number': 1,
                'text': (
                    'All outpatient therapy service claims must include a therapy modifier '
                    'identifying the plan of care under which services are provided: '
                    'modifier GO for Occupational Therapy, modifier GP for Physical Therapy, '
                    'modifier GN for Speech-Language Pathology. '
                    'Occurrence code 17 reports the date the OT plan was established or '
                    'last reviewed. Occurrence code 29 reports the PT plan date. '
                    'Occurrence code 30 reports the ST plan date. '
                    'Effective for dates of service on or after January 1, 2022.'
                ),
                'start': '2022-01-01',
                'end': None,
                'anchor_code': 'GO',
                'page_start': 35, 'page_end': 36,
                'section': 'Section 7.1 Therapy Billing Requirements',
                'citation': 'eMedNY Inpatient Billing Guidelines, Section 7.1, p. 35',
            },
        ]
    )

    # ------------------------------------------------------------------
    # LAB-001: Laboratory revenue codes
    # ------------------------------------------------------------------
    _rule(
        rule_code='LAB-001',
        topic_code='LAB',
        service_code='OUTPATIENT',
        rule_name='Laboratory Services Revenue Code Range',
        is_exception=0,
        specificity=7,
        versions=[
            {
                'version_number': 1,
                'text': (
                    'Clinical laboratory services must be billed under revenue codes '
                    '030x through 037x. Diagnostic radiology services use revenue codes '
                    '040x through 047x. CT scanning is reported under revenue code 0351. '
                    'All laboratory and radiology claims require a corresponding '
                    'HCPCS or CPT procedure code identifying the specific service. '
                    'Effective for dates of service on or after January 1, 2022.'
                ),
                'start': '2022-01-01',
                'end': None,
                'anchor_code': '0300',
                'page_start': 42, 'page_end': 43,
                'section': 'Section 8.1 Laboratory and Radiology Billing',
                'citation': 'eMedNY Inpatient Billing Guidelines, Section 8.1, p. 42',
            },
        ]
    )

    print(f"  atomic_rules     : 5 rules seeded (ER-001, ER-002, INPT-001, THER-001, LAB-001)")
    print(f"  rule_versions    : 6 versions seeded (ER-001 has v1+v2 supersession chain)")


# ---------------------------------------------------------------------------
# 8. USERS  (V2 NEW - 3 default users with bcrypt hashed passwords)
# ---------------------------------------------------------------------------
USERS_DATA = [
    ('staff_user', 'staff123', 'staff'),
    ('admin_user', 'admin123', 'admin'),
    ('sme_user',   'sme123',   'sme'),
]


def seed_users(conn) -> None:
    count = 0
    for username, password, role in USERS_DATA:
        existing = conn.execute(
            "SELECT user_id FROM users WHERE username = ?", (username,)
        ).fetchone()
        if existing:
            continue
        password_hash = _hash_password(password)
        now = utc_now()
        conn.execute(
            "INSERT INTO users (username, password_hash, role, is_active, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?)",
            (username, password_hash, role, now, now)
        )
        count += 1
    total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    print(f"  users            : {total} rows (expected 3)  - passwords bcrypt hashed")


# ---------------------------------------------------------------------------
# MAIN SEED FUNCTION
# ---------------------------------------------------------------------------
def seed_all(reset: bool = False) -> None:
    if reset:
        print("\nRESET MODE - deleting existing database and reinitializing...")
        if DB_PATH.exists():
            DB_PATH.unlink()
        init_db(verbose=False)
        print("Database reinitialized.\n")

    conn = get_connection()
    try:
        print("Seeding DPKB CL_V2 database...")
        print("-" * 50)

        source_type_ids  = seed_source_types(conn)
        service_type_ids = seed_service_types(conn)
        topic_ids        = seed_rule_topics(conn)
        anchor_type_ids  = seed_ub04_anchor_types(conn)
        anchor_code_ids  = seed_ub04_anchor_codes(conn, anchor_type_ids)
        source_doc_ids   = seed_source_documents(conn, source_type_ids)
        seed_rules(conn, topic_ids, service_type_ids, source_doc_ids, anchor_code_ids)
        seed_users(conn)
        seed_fragments(conn, source_doc_ids)

        conn.commit()

        print("-" * 50)
        print("Seed complete. Running validation counts...")
        print()

        # ---------------------------------------------------------------
        # Validation
        # ---------------------------------------------------------------
        checks = [
            ("rule_topics",       "SELECT COUNT(*) FROM rule_topics",       15, "expected 15"),
            ("ub04_anchor_types", "SELECT COUNT(*) FROM ub04_anchor_types", 11, "expected 11"),
            ("users",             "SELECT COUNT(*) FROM users",              3, "expected 3"),
            ("source_types",      "SELECT COUNT(*) FROM source_types",       3, "expected 3"),
            ("service_types",     "SELECT COUNT(*) FROM service_types",      5, "expected 5"),
            ("atomic_rules",      "SELECT COUNT(*) FROM atomic_rules",       5, "expected 5"),
            ("rule_versions",     "SELECT COUNT(*) FROM rule_versions",      6, "expected 6"),
        ]

        all_ok = True
        for label, sql, expected, note in checks:
            actual = conn.execute(sql).fetchone()[0]
            status = "✓" if actual >= expected else "✗ MISMATCH"
            if actual < expected:
                all_ok = False
            print(f"  {label:<22}: {actual:>3}  ({note})  {status}")

        # Verify bcrypt hashes (not plaintext)
        users = conn.execute("SELECT username, password_hash FROM users").fetchall()
        for u in users:
            assert u['password_hash'].startswith('$2b$'), \
                f"ERROR: {u['username']} password is not bcrypt hashed!"
        print(f"  password_hash format : bcrypt verified for all 3 users  ✓")

        print()
        if all_ok:
            print("VALIDATION PASSED. Run tests/test_retrieval.py to confirm V1 tests still pass.")
        else:
            print("VALIDATION FAILED - check seed data above.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR during seeding: {e}")
        raise
    finally:
        conn.close()



# ---------------------------------------------------------------------------
# NEW BLOCK FOR DEMO PURPOSES - NOT PART OF V1 SEED
# ---------------------------------------------------------------------------
def seed_fragments(conn, source_doc_id_map):
    doc_id = list(source_doc_id_map.values())[0]
    now = utc_now()

    fragments = [
        {
            'source_document_id': doc_id,
            'fragment_text_raw': 'Physical therapy outpatient claims must include modifier GP identifying the plan of care. Occurrence code 29 must report the date the PT plan was established or last reviewed. Effective for dates of service on or after January 1, 2024.',
            'fragment_sequence_number': 1,
            'page_number_start': 35,
            'page_number_end': 35,
            'section_reference': 'Section 7.2 PT Modifier Requirements',
            'extracted_effective_start_date': '2024-01-01',
            'extracted_effective_end_date': None,
            'date_role_label': 'effective_date',
            'detected_anchor_type_code': 'modifier',
            'detected_anchor_value': 'GP',
            'confidence_score': 62.0,
            'confidence_tier': 'TIER_2',
            'review_status': 'pending',
            'reviewed_by': None,
            'reviewed_at': None,
            'review_notes': None,
            'created_at': now,
        },
        {
            'source_document_id': doc_id,
            'fragment_text_raw': 'Observation services revenue code 0762. G0378 billed per hour. Status unclear — may apply to inpatient or outpatient depending on admission order.',
            'fragment_sequence_number': 2,
            'page_number_start': 48,
            'page_number_end': 48,
            'section_reference': 'Section 9.1 Observation',
            'extracted_effective_start_date': None,
            'extracted_effective_end_date': None,
            'date_role_label': None,
            'detected_anchor_type_code': 'ANCHOR_OTHER',
            'detected_anchor_value': '0762',
            'confidence_score': 28.0,
            'confidence_tier': 'TIER_3',
            'review_status': 'pending',
            'reviewed_by': None,
            'reviewed_at': None,
            'review_notes': None,
            'created_at': now,
        },
    ]

    count = conn.execute("SELECT COUNT(*) FROM policy_fragments").fetchone()[0]
    if count == 0:
        for f in fragments:
            conn.execute(
                """INSERT INTO policy_fragments
                (source_document_id, fragment_text_raw, fragment_sequence_number,
                 page_number_start, page_number_end, section_reference,
                 extracted_effective_start_date, extracted_effective_end_date,
                 date_role_label, detected_anchor_type_code, detected_anchor_value,
                 confidence_score, confidence_tier, review_status,
                 reviewed_by, reviewed_at, review_notes, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                list(f.values())
            )
        print(f"  policy_fragments  : 2 rows seeded (1 TIER_2, 1 TIER_3)")
    else:
        print(f"  policy_fragments  : {count} rows (skipped)")


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    reset_flag = '--reset' in sys.argv
    seed_all(reset=reset_flag)