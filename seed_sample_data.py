# seed_sample_data.py
from __future__ import annotations

import sys
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from db_init import DB_PATH, get_connection, init_db


# ---------------------------------------------------------------------------
# Timestamp helper
# ---------------------------------------------------------------------------

def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


# ---------------------------------------------------------------------------
# Exported ID constants - tests can import these directly
# ---------------------------------------------------------------------------

# rule_topics
ER_TOPIC_ID = 1
INPT_TOPIC_ID = 2
OUTPT_TOPIC_ID = 3
THER_TOPIC_ID = 4
LAB_TOPIC_ID = 5
TEST_TOPIC_ID = 6  # test-only

# source_documents
BASE_MANUAL_DOC_ID = 1
ER_BULLETIN_DOC_ID = 2
THER_BULLETIN_DOC_ID = 3
LAB_BULLETIN_DOC_ID = 4
GUIDELINE_DOC_ID = 5
ER_UPDATES_2024_DOC_ID = 6
INPT_UPDATES_2023_DOC_ID = 7
OUTPT_LAB_2023_DOC_ID = 8
COMPREHENSIVE_2025_DOC_ID = 9

# baseline atomic_rules
ER001_RULE_ID = 1
ER002_RULE_ID = 2
ER003_RULE_ID = 3
INPT001_RULE_ID = 4
INPT002_RULE_ID = 5
INPT003_RULE_ID = 6
OUTPT001_RULE_ID = 7
OUTPT002_RULE_ID = 8
OUTPT003_RULE_ID = 9
THER001_RULE_ID = 10
THER002_RULE_ID = 11
THER003_RULE_ID = 12
LAB001_RULE_ID = 13
LAB002_RULE_ID = 14
LAB003_RULE_ID = 15
TEST_TIE_RULE_ID = 16

# expansion atomic_rules
ER004_RULE_ID = 17
INPT004_RULE_ID = 18
INPT005_RULE_ID = 19
INPT006_RULE_ID = 20
INPT007_RULE_ID = 21
INPT008_RULE_ID = 22
OUTPT004_RULE_ID = 23
OUTPT005_RULE_ID = 24
LAB004_RULE_ID = 25
LAB005_RULE_ID = 26
LAB006_RULE_ID = 27
LAB007_RULE_ID = 28

# baseline rule_versions
ER001_V1_ID = 1
ER002_V1_ID = 2
ER003_V1_ID = 3
INPT001_V1_ID = 4
INPT002_V1_ID = 5
INPT003_V1_ID = 6
OUTPT001_V1_ID = 7
OUTPT002_V1_ID = 8
OUTPT003_V1_ID = 9
THER001_V1_ID = 10
THER002_V1_ID = 11
THER003_V1_ID = 12
LAB001_V1_ID = 13
LAB002_V1_ID = 14
LAB003_V1_ID = 15
ER001_V2_ID = 16
THER001_V2_ID = 17
LAB001_V2_ID = 18
TIE_V1_ID = 19
TIE_V2_ID = 20

# enrichment rule_versions
ER001_V3_ID = 21
ER002_V2_ID = 22
ER003_V2_ID = 23
INPT001_V2_ID = 24
INPT002_V2_ID = 25
INPT003_V2_ID = 26
OUTPT001_V2_ID = 27
OUTPT002_V2_ID = 28
LAB002_V2_ID = 29
LAB003_V2_ID = 30
THER002_V2_ID = 31

# expansion v1 rule_versions
ER004_V1_ID = 32
INPT004_V1_ID = 33
INPT005_V1_ID = 34
INPT006_V1_ID = 35
INPT007_V1_ID = 36
INPT008_V1_ID = 37
OUTPT004_V1_ID = 38
OUTPT005_V1_ID = 39
LAB004_V1_ID = 40
LAB005_V1_ID = 41
LAB006_V1_ID = 42
LAB007_V1_ID = 43

# ub04_anchor_types
REVENUE_CODE_TYPE_ID = 1
TYPE_OF_BILL_TYPE_ID = 2
CONDITION_CODE_TYPE_ID = 3
MODIFIER_TYPE_ID = 4

# baseline ub04_anchor_codes
RC_0120_ID = 1
RC_0360_ID = 2
RC_0301_ID = 3
RC_0324_ID = 4
RC_0351_ID = 5
RC_0450_ID = 6
TOB_131_ID = 7
TOB_111_ID = 8
TOB_831_ID = 9
CC_G0_ID = 10
CC_07_ID = 11
CC_17_ID = 12
CC_29_ID = 13
MOD_73_ID = 14
MOD_GO_ID = 15
MOD_GY_ID = 16

# expansion ub04_anchor_codes (12 more => total 28)
RC_0200_ID = 17
RC_0170_ID = 18
TOB_112_ID = 19
TOB_114_ID = 20
CC_16_ID = 21
CC_70_ID = 22
RC_0361_ID = 23
MOD_50_ID = 24
RC_0305_ID = 25
RC_0306_ID = 26
RC_0307_ID = 27
RC_0308_ID = 28


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def ensure_db_exists(db_path: Path = DB_PATH) -> None:
    if not db_path.exists():
        init_db(db_path)


def seed_lookup_tables(conn: sqlite3.Connection, now_ts: str) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO source_types (
            source_type_id, source_type_code, source_type_name, description, is_active
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            (1, "MANUAL", "Provider Manual", "Seeded MVP manual source type", 1),
            (2, "BULLETIN", "Medicaid Update Bulletin", "Seeded MVP bulletin source type", 1),
            (3, "GUIDELINE", "Billing Guideline", "Seeded MVP guideline source type", 1),
        ],
    )

    conn.executemany(
        """
        INSERT OR REPLACE INTO rule_topics (
            rule_topic_id, topic_code, topic_name, parent_rule_topic_id,
            description, display_order, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (ER_TOPIC_ID, "ER", "Emergency Room Services", None, "Facility billing rules for ER services", 1, 1),
            (INPT_TOPIC_ID, "INPT", "Inpatient Services", None, "Facility billing rules for inpatient services", 2, 1),
            (OUTPT_TOPIC_ID, "OUTPT", "Outpatient Surgery", None, "Facility billing rules for outpatient surgery", 3, 1),
            (THER_TOPIC_ID, "THER", "Therapy PT/OT/ST", None, "Facility billing rules for therapy services", 4, 1),
            (LAB_TOPIC_ID, "LAB", "Lab and Radiology", None, "Facility billing rules for lab and radiology", 5, 1),
            (TEST_TOPIC_ID, "TEST", "Resolver Test Cases", None, "Test-only topic for tie-break validation", 99, 1),
        ],
    )

    conn.executemany(
        """
        INSERT OR REPLACE INTO service_types (
            service_type_id, service_type_code, service_type_name, description, is_active
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            (1, "INPATIENT", "Inpatient", "Inpatient facility services", 1),
            (2, "OUTPATIENT", "Outpatient", "Outpatient facility services", 1),
            (3, "EMERGENCY", "Emergency", "Emergency room facility services", 1),
        ],
    )

    conn.executemany(
        """
        INSERT OR REPLACE INTO ub04_anchor_types (
            ub04_anchor_type_id, anchor_type_code, anchor_type_name, description, is_active
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            (REVENUE_CODE_TYPE_ID, "revenue_code", "Revenue Code", "UB-04 revenue code", 1),
            (TYPE_OF_BILL_TYPE_ID, "type_of_bill", "Type of Bill", "UB-04 type of bill code", 1),
            (CONDITION_CODE_TYPE_ID, "condition_code", "Condition Code", "UB-04 condition code", 1),
            (MODIFIER_TYPE_ID, "modifier", "Modifier", "Procedure/service modifier", 1),
        ],
    )

    anchor_codes = [
        # baseline 16
        (RC_0120_ID, REVENUE_CODE_TYPE_ID, "0120", "Inpatient Room and Board", "Room and board revenue code example", "0120", None, 1, now_ts, now_ts),
        (RC_0360_ID, REVENUE_CODE_TYPE_ID, "0360", "Operating Room", "General surgery operating room charges", "0360", None, 1, now_ts, now_ts),
        (RC_0301_ID, REVENUE_CODE_TYPE_ID, "0301", "Laboratory", "Laboratory service revenue code example", "0301", None, 1, now_ts, now_ts),
        (RC_0324_ID, REVENUE_CODE_TYPE_ID, "0324", "Diagnostic Radiology", "Diagnostic radiology revenue code", "0324", None, 1, now_ts, now_ts),
        (RC_0351_ID, REVENUE_CODE_TYPE_ID, "0351", "CT Scan", "CT scan revenue code", "0351", None, 1, now_ts, now_ts),
        (RC_0450_ID, REVENUE_CODE_TYPE_ID, "0450", "Emergency Room", "Emergency room revenue code", "0450", None, 1, now_ts, now_ts),
        (TOB_131_ID, TYPE_OF_BILL_TYPE_ID, "131", "Hospital Outpatient", "Initial outpatient bill", "131", None, 1, now_ts, now_ts),
        (TOB_111_ID, TYPE_OF_BILL_TYPE_ID, "111", "Hospital Inpatient", "Hospital inpatient admission through discharge", "111", None, 1, now_ts, now_ts),
        (TOB_831_ID, TYPE_OF_BILL_TYPE_ID, "831", "Ambulatory Surgery Center", "Ambulatory surgery center outpatient bill", "831", None, 1, now_ts, now_ts),
        (CC_G0_ID, CONDITION_CODE_TYPE_ID, "G0", "Multiple Same Day Visits", "Condition code G0", "G0", None, 1, now_ts, now_ts),
        (CC_07_ID, CONDITION_CODE_TYPE_ID, "07", "Treatment Authorization", "Condition code 07", "07", None, 1, now_ts, now_ts),
        (CC_17_ID, CONDITION_CODE_TYPE_ID, "17", "OT Plan Established", "Occurrence/condition seed code 17", "17", None, 1, now_ts, now_ts),
        (CC_29_ID, CONDITION_CODE_TYPE_ID, "29", "PT Plan Established", "Occurrence/condition seed code 29", "29", None, 1, now_ts, now_ts),
        (MOD_73_ID, MODIFIER_TYPE_ID, "73", "Discontinued Procedure", "Modifier 73", "73", None, 1, now_ts, now_ts),
        (MOD_GO_ID, MODIFIER_TYPE_ID, "GO", "Occupational Therapy", "Modifier GO", "GO", None, 1, now_ts, now_ts),
        (MOD_GY_ID, MODIFIER_TYPE_ID, "GY", "Statutorily Excluded", "Modifier GY", "GY", None, 1, now_ts, now_ts),

        # expansion 12 => total 28
        (RC_0200_ID, REVENUE_CODE_TYPE_ID, "0200", "Intensive Care", "Intensive care / critical care unit revenue code", "0200", None, 1, now_ts, now_ts),
        (RC_0170_ID, REVENUE_CODE_TYPE_ID, "0170", "Nursery - Newborn", "Routine newborn nursery services", "0170", None, 1, now_ts, now_ts),
        (TOB_112_ID, TYPE_OF_BILL_TYPE_ID, "112", "Hospital Inpatient - Transfer", "Inpatient hospital bill - transfer", "112", None, 1, now_ts, now_ts),
        (TOB_114_ID, TYPE_OF_BILL_TYPE_ID, "114", "Hospital Inpatient - Psychiatric Unit", "Inpatient psychiatric unit per diem claim", "114", None, 1, now_ts, now_ts),
        (CC_16_ID, CONDITION_CODE_TYPE_ID, "16", "Date Referral to Home Care", "Occurrence code 16 - date of referral to home health", "16", None, 1, now_ts, now_ts),
        (CC_70_ID, CONDITION_CODE_TYPE_ID, "70", "Readmission to Same Facility", "Condition code 70 - readmission within 30 days same facility", "70", None, 1, now_ts, now_ts),
        (RC_0361_ID, REVENUE_CODE_TYPE_ID, "0361", "Trauma Surgery Operating Room", "Trauma surgery operating room revenue code", "0361", None, 1, now_ts, now_ts),
        (MOD_50_ID, MODIFIER_TYPE_ID, "50", "Bilateral Procedure", "Modifier 50 - bilateral procedure", "50", None, 1, now_ts, now_ts),
        (RC_0305_ID, REVENUE_CODE_TYPE_ID, "0305", "Microbiology Lab", "Microbiology laboratory services", "0305", None, 1, now_ts, now_ts),
        (RC_0306_ID, REVENUE_CODE_TYPE_ID, "0306", "Hematology Lab", "Hematology laboratory services", "0306", None, 1, now_ts, now_ts),
        (RC_0307_ID, REVENUE_CODE_TYPE_ID, "0307", "Chemistry Lab", "Chemistry laboratory services", "0307", None, 1, now_ts, now_ts),
        (RC_0308_ID, REVENUE_CODE_TYPE_ID, "0308", "Pathology Lab", "Pathology laboratory services", "0308", None, 1, now_ts, now_ts),
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO ub04_anchor_codes (
            ub04_anchor_code_id, ub04_anchor_type_id, anchor_code, anchor_label,
            anchor_description, anchor_value_start, anchor_value_end, is_active,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        anchor_codes,
    )


def seed_source_documents(conn: sqlite3.Connection, now_ts: str) -> None:
    documents = [
        (
            BASE_MANUAL_DOC_ID, 1,
            "NY Medicaid Facility Billing Manual - MVP Base Rules",
            "https://example.com/dpkb/facility-billing-manual-2023.pdf",
            "data/raw/facility-billing-manual-2023.pdf",
            "application/pdf",
            "seed_sha256_manual_2023",
            now_ts,
            "2023-01-01",
            "2023-01-01",
            None,
            1,
            "v2023",
            1,
            "Synthetic seed source document for all baseline v1 rules",
            now_ts,
            now_ts,
        ),
        (
            ER_BULLETIN_DOC_ID, 2,
            "Medicaid Update Bulletin - ER Telehealth Expansion",
            "https://example.com/dpkb/medicaid-update-er-2024-06.pdf",
            "data/raw/medicaid-update-er-2024-06.pdf",
            "application/pdf",
            "seed_sha256_er_2024_06",
            now_ts,
            "2024-06-01",
            "2024-06-01",
            None,
            2,
            "2024-06",
            1,
            "Synthetic bulletin for ER-001 v2",
            now_ts,
            now_ts,
        ),
        (
            THER_BULLETIN_DOC_ID, 2,
            "Medicaid Update Bulletin - Therapy Modifier Update",
            "https://example.com/dpkb/medicaid-update-ther-2023-12.pdf",
            "data/raw/medicaid-update-ther-2023-12.pdf",
            "application/pdf",
            "seed_sha256_ther_2023_12",
            now_ts,
            "2023-12-01",
            "2023-12-01",
            None,
            2,
            "2023-12",
            1,
            "Synthetic bulletin for THER-001 v2",
            now_ts,
            now_ts,
        ),
        (
            LAB_BULLETIN_DOC_ID, 2,
            "Medicaid Update Bulletin - Lab Revenue Range Expansion",
            "https://example.com/dpkb/medicaid-update-lab-2024-03.pdf",
            "data/raw/medicaid-update-lab-2024-03.pdf",
            "application/pdf",
            "seed_sha256_lab_2024_03",
            now_ts,
            "2024-03-01",
            "2024-03-01",
            None,
            2,
            "2024-03",
            1,
            "Synthetic bulletin for LAB-001 v2",
            now_ts,
            now_ts,
        ),
        (
            GUIDELINE_DOC_ID, 3,
            "Facility Billing Guideline Supplement",
            "https://example.com/dpkb/facility-guideline-supplement.pdf",
            "data/raw/facility-guideline-supplement.pdf",
            "application/pdf",
            "seed_sha256_guideline_support",
            now_ts,
            "2023-01-01",
            "2023-01-01",
            None,
            3,
            "v1",
            1,
            "Synthetic supporting guideline document",
            now_ts,
            now_ts,
        ),
        (
            ER_UPDATES_2024_DOC_ID, 2,
            "Medicaid Update Bulletin - ER Policy Updates Jan 2024",
            "https://example.com/dpkb/medicaid-update-er-2024-01.pdf",
            "data/raw/medicaid-update-er-2024-01.pdf",
            "application/pdf",
            "seed_sha256_er_2024_01",
            now_ts,
            "2024-01-01",
            "2024-01-01",
            None,
            2,
            "2024-01",
            1,
            "Synthetic bulletin for ER and LAB updates",
            now_ts,
            now_ts,
        ),
        (
            INPT_UPDATES_2023_DOC_ID, 2,
            "Medicaid Update Bulletin - Inpatient Billing Updates 2023",
            "https://example.com/dpkb/medicaid-update-inpt-2023.pdf",
            "data/raw/medicaid-update-inpt-2023.pdf",
            "application/pdf",
            "seed_sha256_inpt_2023",
            now_ts,
            "2023-07-01",
            "2023-07-01",
            None,
            2,
            "2023-07",
            1,
            "Synthetic bulletin for inpatient updates",
            now_ts,
            now_ts,
        ),
        (
            OUTPT_LAB_2023_DOC_ID, 2,
            "Medicaid Update Bulletin - Outpatient and Lab Updates Oct 2023",
            "https://example.com/dpkb/medicaid-update-outpt-lab-2023-10.pdf",
            "data/raw/medicaid-update-outpt-lab-2023-10.pdf",
            "application/pdf",
            "seed_sha256_outpt_lab_2023",
            now_ts,
            "2023-10-01",
            "2023-10-01",
            None,
            2,
            "2023-10",
            1,
            "Synthetic bulletin for outpatient and lab updates",
            now_ts,
            now_ts,
        ),
        (
            COMPREHENSIVE_2025_DOC_ID, 2,
            "Medicaid Update Bulletin - Comprehensive Policy Update Jan 2025",
            "https://example.com/dpkb/medicaid-update-comprehensive-2025-01.pdf",
            "data/raw/medicaid-update-comprehensive-2025-01.pdf",
            "application/pdf",
            "seed_sha256_comprehensive_2025",
            now_ts,
            "2025-01-01",
            "2025-01-01",
            None,
            2,
            "2025-01",
            1,
            "Synthetic bulletin for comprehensive 2025 updates",
            now_ts,
            now_ts,
        ),
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO source_documents (
            source_document_id, source_type_id, title, source_url, file_path, mime_type,
            sha256_hash, retrieved_at, published_date, document_effective_start_date,
            document_effective_end_date, authority_rank, document_version_label, is_active,
            notes, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        documents,
    )


def seed_policy_fragments(conn: sqlite3.Connection, now_ts: str) -> None:
    fragments = [
        # baseline 18
        (1, BASE_MANUAL_DOC_ID, 1, "Condition Code G0 required when multiple visits occur same day same revenue center.", None, 10, 10, "ER-001", 0.95, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (2, BASE_MANUAL_DOC_ID, 2, "All ER services billed under revenue code 0450. Observation services under 0762.", None, 11, 11, "ER-002", 0.95, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (3, BASE_MANUAL_DOC_ID, 3, "Outpatient ER claims must use TOB 131 for initial outpatient bill.", None, 12, 12, "ER-003", 0.95, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),

        (4, BASE_MANUAL_DOC_ID, 4, "Room and board charges billed under revenue codes 0100-0219 based on accommodation type.", None, 20, 20, "INPT-001", 0.94, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (5, BASE_MANUAL_DOC_ID, 5, "Hospital inpatient admission through discharge billed under TOB 111.", None, 21, 21, "INPT-002", 0.94, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (6, BASE_MANUAL_DOC_ID, 6, "All non-emergency inpatient admissions require prior authorization before service delivery.", None, 22, 22, "INPT-003", 0.94, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),

        (7, BASE_MANUAL_DOC_ID, 7, "Operating room charges billed under revenue code 0360 for general surgery.", None, 30, 30, "OUTPT-001", 0.93, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (8, BASE_MANUAL_DOC_ID, 8, "Ambulatory surgery center claims use TOB 831 for outpatient surgery.", None, 31, 31, "OUTPT-002", 0.93, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (9, BASE_MANUAL_DOC_ID, 9, "Modifier 73 required when procedure discontinued after patient prep but before anesthesia.", None, 32, 32, "OUTPT-003", 0.93, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),

        (10, BASE_MANUAL_DOC_ID, 10, "All outpatient therapy claims must include therapy modifier: GO (OT), GP (PT), GN (ST).", None, 40, 40, "THER-001", 0.92, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (11, BASE_MANUAL_DOC_ID, 11, "Occurrence code 17 required - date outpatient OT plan established or last reviewed.", None, 41, 41, "THER-002", 0.92, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (12, BASE_MANUAL_DOC_ID, 12, "Occurrence code 29 required - date outpatient PT plan established or last reviewed.", None, 42, 42, "THER-003", 0.92, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),

        (13, BASE_MANUAL_DOC_ID, 13, "Laboratory services billed under revenue codes 0300-0319 based on lab type.", None, 50, 50, "LAB-001", 0.91, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (14, BASE_MANUAL_DOC_ID, 14, "Diagnostic radiology billed under revenue code 0324. Therapeutic radiology under 0333.", None, 51, 51, "LAB-002", 0.91, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (15, BASE_MANUAL_DOC_ID, 15, "CT scan services billed under revenue code 0351. 25 modifier required for significant separately identifiable procedure.", None, 52, 52, "LAB-003", 0.91, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),

        (16, ER_BULLETIN_DOC_ID, 1, "Condition Code G0 required when multiple visits occur same day same revenue center, including telehealth visits.", None, 5, 5, "ER-001 v2", 0.97, "TIER_1", "effective_date", "2024-06-01", None, "approved", None, now_ts, now_ts),
        (17, THER_BULLETIN_DOC_ID, 1, "All outpatient therapy claims must include therapy modifier: GO (OT), GP (PT), GN (ST), and GY for services deemed not medically necessary.", None, 6, 6, "THER-001 v2", 0.97, "TIER_1", "effective_date", "2023-12-01", None, "approved", None, now_ts, now_ts),
        (18, LAB_BULLETIN_DOC_ID, 1, "Laboratory services billed under revenue codes 0300-0329 based on lab type.", None, 7, 7, "LAB-001 v2", 0.97, "TIER_1", "effective_date", "2024-03-01", None, "approved", None, now_ts, now_ts),

        # enrichment 11 => total 29
        (19, COMPREHENSIVE_2025_DOC_ID, 1, "Condition Code G0 required when multiple visits occur same day in same revenue center, including telehealth and remote patient monitoring visits.", None, 3, 3, "ER-001 v3", 0.97, "TIER_1", "effective_date", "2025-01-01", None, "approved", None, now_ts, now_ts),
        (20, ER_UPDATES_2024_DOC_ID, 2, "All ER services billed under revenue code 0450. Observation services explicitly billed under revenue code 0762; these two codes are mutually exclusive on the same claim.", None, 4, 4, "ER-002 v2", 0.96, "TIER_1", "effective_date", "2024-01-01", None, "approved", None, now_ts, now_ts),
        (21, ER_UPDATES_2024_DOC_ID, 3, "Outpatient ER initial claims use TOB 131. Continuation outpatient ER claims use TOB 132.", None, 5, 5, "ER-003 v2", 0.96, "TIER_1", "effective_date", "2024-09-01", None, "approved", None, now_ts, now_ts),
        (22, INPT_UPDATES_2023_DOC_ID, 1, "Room and board charges billed under revenue codes 0100-0219. Sub-classifications: 0120 semi-private, 0130 private, 0160 deluxe private.", None, 8, 8, "INPT-001 v2", 0.95, "TIER_1", "effective_date", "2024-04-01", None, "approved", None, now_ts, now_ts),
        (23, INPT_UPDATES_2023_DOC_ID, 2, "Hospital inpatient admission through discharge billed under TOB 111. Interim bills for long-stay patients use TOB 117.", None, 9, 9, "INPT-002 v2", 0.95, "TIER_1", "effective_date", "2023-07-01", None, "approved", None, now_ts, now_ts),
        (24, INPT_UPDATES_2023_DOC_ID, 3, "All non-emergency inpatient admissions require prior authorization. Authorization must be obtained at least 72 hours before scheduled admission.", None, 10, 10, "INPT-003 v2", 0.95, "TIER_1", "effective_date", "2024-01-01", None, "approved", None, now_ts, now_ts),
        (25, OUTPT_LAB_2023_DOC_ID, 1, "Operating room charges for general surgery billed under revenue code 0360. Trauma surgery cases use revenue code 0361.", None, 6, 6, "OUTPT-001 v2", 0.94, "TIER_1", "effective_date", "2023-10-01", None, "approved", None, now_ts, now_ts),
        (26, OUTPT_LAB_2023_DOC_ID, 2, "Ambulatory surgery center initial claims use TOB 831. Continuation ASC claims use TOB 832.", None, 7, 7, "OUTPT-002 v2", 0.94, "TIER_1", "effective_date", "2024-02-01", None, "approved", None, now_ts, now_ts),
        (27, ER_UPDATES_2024_DOC_ID, 4, "Diagnostic radiology billed under revenue code 0324. Therapeutic radiology: external beam under 0333, brachytherapy under 0341.", None, 11, 11, "LAB-002 v2", 0.93, "TIER_1", "effective_date", "2024-06-01", None, "approved", None, now_ts, now_ts),
        (28, OUTPT_LAB_2023_DOC_ID, 3, "CT scan services billed under revenue code 0351. Modifier 25 required when a significant separately identifiable evaluation and management service is rendered on the same date.", None, 12, 12, "LAB-003 v2", 0.93, "TIER_1", "effective_date", "2023-09-01", None, "approved", None, now_ts, now_ts),
        (29, COMPREHENSIVE_2025_DOC_ID, 2, "Occurrence code 17 required to report the date the outpatient OT plan was established or last reviewed. Effective 2024-03-01 this requirement also applies to telehealth OT services.", None, 13, 13, "THER-002 v2", 0.96, "TIER_1", "effective_date", "2024-03-01", None, "approved", None, now_ts, now_ts),

        # expansion 12 => total 41
        (30, BASE_MANUAL_DOC_ID, 30, "ICU and critical care services provided following emergency department treatment are billed under revenue code 0200.", None, 60, 60, "ER-004", 0.93, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (31, BASE_MANUAL_DOC_ID, 31, "Routine newborn nursery services are billed under revenue code 0170. Level II special care nursery uses 0171. Level III NICU uses 0172.", None, 70, 70, "INPT-004", 0.92, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (32, BASE_MANUAL_DOC_ID, 32, "When an inpatient is transferred to a skilled nursing facility upon discharge, the inpatient claim is billed with Type of Bill 112 and patient status code 03.", None, 71, 71, "INPT-005", 0.92, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (33, BASE_MANUAL_DOC_ID, 33, "Inpatient psychiatric unit services are billed under Type of Bill 114 and are subject to per diem reimbursement under rate code 2852.", None, 72, 72, "INPT-006", 0.92, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (34, BASE_MANUAL_DOC_ID, 34, "Occurrence code 16 must be reported to indicate the date of referral to home health agency for all discharges with patient status code 06.", None, 73, 73, "INPT-007", 0.91, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (35, BASE_MANUAL_DOC_ID, 35, "When a Medicaid inpatient is readmitted to the same facility within 30 days of discharge for the same or related condition, condition code 70 must be reported.", None, 74, 74, "INPT-008", 0.91, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (36, BASE_MANUAL_DOC_ID, 36, "Pre-admission diagnostic and clinically related non-diagnostic services provided within three days before an inpatient admission are included in the inpatient claim and may not be billed separately as outpatient services.", None, 75, 75, "OUTPT-004", 0.92, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (37, BASE_MANUAL_DOC_ID, 37, "Modifier 50 is required when the same outpatient surgical procedure is performed bilaterally during the same operative session. Reimbursement is 150 percent of the base fee schedule amount when bilateral surgery rules apply.", None, 76, 76, "OUTPT-005", 0.92, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (38, BASE_MANUAL_DOC_ID, 38, "Microbiology laboratory services including bacteriology, mycology, parasitology, and virology testing are billed under revenue code 0305.", None, 90, 90, "LAB-004", 0.90, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (39, BASE_MANUAL_DOC_ID, 39, "Hematology laboratory services including CBC, coagulation studies, and bone marrow evaluations are billed under revenue code 0306.", None, 91, 91, "LAB-005", 0.90, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (40, BASE_MANUAL_DOC_ID, 40, "Clinical chemistry laboratory services including metabolic panels, electrolytes, liver function studies, and enzyme testing are billed under revenue code 0307.", None, 92, 92, "LAB-006", 0.90, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
        (41, BASE_MANUAL_DOC_ID, 41, "Surgical pathology and tissue examination services including gross and microscopic evaluation are billed under revenue code 0308.", None, 93, 93, "LAB-007", 0.90, "TIER_1", "effective_date", "2023-01-01", None, "approved", None, now_ts, now_ts),
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO policy_fragments (
            policy_fragment_id, source_document_id, fragment_sequence, fragment_text_raw,
            fragment_text_clean, page_number_start, page_number_end, section_reference,
            confidence_score, confidence_tier, date_role_label,
            extracted_effective_start_date, extracted_effective_end_date,
            review_status, review_notes, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        fragments,
    )


def seed_atomic_rules(conn: sqlite3.Connection, now_ts: str) -> None:
    rows = [
        # baseline 16
        (ER001_RULE_ID, "ER-001", "Multiple ER Visits Same Day", ER_TOPIC_ID, 3, "Same-day multiple ER visit billing requirement", "Emergency room condition code rule", "active", now_ts, now_ts),
        (ER002_RULE_ID, "ER-002", "ER Revenue Code Requirement", ER_TOPIC_ID, 3, "Emergency room revenue code requirement", "Emergency room revenue code rule", "active", now_ts, now_ts),
        (ER003_RULE_ID, "ER-003", "ER Type of Bill Code", ER_TOPIC_ID, 3, "Emergency room type of bill requirement", "Emergency room TOB rule", "active", now_ts, now_ts),

        (INPT001_RULE_ID, "INPT-001", "Inpatient Room and Board Revenue Code", INPT_TOPIC_ID, 1, "Room and board revenue code requirement", "Inpatient revenue code rule", "active", now_ts, now_ts),
        (INPT002_RULE_ID, "INPT-002", "Inpatient Admission TOB Code", INPT_TOPIC_ID, 1, "Inpatient type of bill requirement", "Inpatient TOB rule", "active", now_ts, now_ts),
        (INPT003_RULE_ID, "INPT-003", "Inpatient Prior Authorization", INPT_TOPIC_ID, 1, "Prior authorization requirement", "Inpatient authorization rule", "active", now_ts, now_ts),

        (OUTPT001_RULE_ID, "OUTPT-001", "Outpatient Surgery Revenue Code", OUTPT_TOPIC_ID, 2, "Outpatient surgery revenue code requirement", "Outpatient revenue code rule", "active", now_ts, now_ts),
        (OUTPT002_RULE_ID, "OUTPT-002", "Outpatient Surgery TOB Code", OUTPT_TOPIC_ID, 2, "Outpatient surgery TOB requirement", "Outpatient TOB rule", "active", now_ts, now_ts),
        (OUTPT003_RULE_ID, "OUTPT-003", "Outpatient Surgery Modifier", OUTPT_TOPIC_ID, 2, "Outpatient surgery modifier rule", "Outpatient modifier rule", "active", now_ts, now_ts),

        (THER001_RULE_ID, "THER-001", "Therapy Modifier Requirement", THER_TOPIC_ID, 2, "Therapy modifier requirement", "Therapy modifier rule", "active", now_ts, now_ts),
        (THER002_RULE_ID, "THER-002", "Therapy Occurrence Code - OT Plan", THER_TOPIC_ID, 2, "OT plan code requirement", "Therapy OT plan rule", "active", now_ts, now_ts),
        (THER003_RULE_ID, "THER-003", "Therapy Occurrence Code - PT Plan", THER_TOPIC_ID, 2, "PT plan code requirement", "Therapy PT plan rule", "active", now_ts, now_ts),

        (LAB001_RULE_ID, "LAB-001", "Lab Revenue Code Range", LAB_TOPIC_ID, 2, "Lab revenue code range rule", "Laboratory revenue code rule", "active", now_ts, now_ts),
        (LAB002_RULE_ID, "LAB-002", "Radiology Revenue Code", LAB_TOPIC_ID, 2, "Radiology revenue code rule", "Radiology revenue rule", "active", now_ts, now_ts),
        (LAB003_RULE_ID, "LAB-003", "CT Scan Revenue Code", LAB_TOPIC_ID, 2, "CT scan revenue code rule", "CT scan rule", "active", now_ts, now_ts),

        (TEST_TIE_RULE_ID, "TEST-TIE-001", "Tie-Breaker Specificity Test Rule", TEST_TOPIC_ID, None, "Specificity tie-break test", "Test-only resolver rule", "active", now_ts, now_ts),

        # expansion 12 => total 28
        (ER004_RULE_ID, "ER-004", "ICU/Critical Care Revenue Code", ER_TOPIC_ID, 3, "ICU billing after ER treatment", "ER critical care revenue code rule", "active", now_ts, now_ts),
        (INPT004_RULE_ID, "INPT-004", "Newborn Nursery Revenue Code", INPT_TOPIC_ID, 1, "Nursery revenue code by level of care", "Newborn nursery billing rule", "active", now_ts, now_ts),
        (INPT005_RULE_ID, "INPT-005", "SNF Transfer Type of Bill", INPT_TOPIC_ID, 1, "TOB 112 for SNF transfer on discharge", "SNF transfer TOB rule", "active", now_ts, now_ts),
        (INPT006_RULE_ID, "INPT-006", "Psychiatric Unit Type of Bill", INPT_TOPIC_ID, 1, "TOB 114 for inpatient psychiatric per diem", "Psychiatric unit TOB rule", "active", now_ts, now_ts),
        (INPT007_RULE_ID, "INPT-007", "Discharge to Home Health Occurrence Code", INPT_TOPIC_ID, 1, "Occurrence code 16 for home health discharge", "Home health referral occurrence code rule", "active", now_ts, now_ts),
        (INPT008_RULE_ID, "INPT-008", "Readmission Within 30 Days Condition Code", INPT_TOPIC_ID, 1, "Condition code 70 for same-facility readmission", "Readmission condition code rule", "active", now_ts, now_ts),
        (OUTPT004_RULE_ID, "OUTPT-004", "Three-Day Pre-Admission Window Rule", OUTPT_TOPIC_ID, 2, "Outpatient pre-admission services bundled into inpatient DRG", "Three-day window outpatient bundling rule", "active", now_ts, now_ts),
        (OUTPT005_RULE_ID, "OUTPT-005", "Bilateral Surgery Modifier Requirement", OUTPT_TOPIC_ID, 2, "Modifier 50 bilateral procedure rule", "Outpatient bilateral surgery modifier rule", "active", now_ts, now_ts),
        (LAB004_RULE_ID, "LAB-004", "Microbiology Revenue Code", LAB_TOPIC_ID, 2, "Revenue code 0305 for microbiology tests", "Microbiology revenue code rule", "active", now_ts, now_ts),
        (LAB005_RULE_ID, "LAB-005", "Hematology Revenue Code", LAB_TOPIC_ID, 2, "Revenue code 0306 for hematology services", "Hematology revenue code rule", "active", now_ts, now_ts),
        (LAB006_RULE_ID, "LAB-006", "Chemistry Revenue Code", LAB_TOPIC_ID, 2, "Revenue code 0307 for chemistry laboratory services", "Chemistry revenue code rule", "active", now_ts, now_ts),
        (LAB007_RULE_ID, "LAB-007", "Pathology Revenue Code", LAB_TOPIC_ID, 2, "Revenue code 0308 for pathology services", "Pathology revenue code rule", "active", now_ts, now_ts),
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO atomic_rules (
            atomic_rule_id, rule_code, rule_title, rule_topic_id, service_type_id,
            rule_summary, business_description, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def seed_rule_versions(conn: sqlite3.Connection, now_ts: str) -> None:
    rows = [
        # baseline 20
        (ER001_V1_ID, ER001_RULE_ID, "v1", 1, "Condition Code G0 required when multiple visits occur same day same revenue center.", None, None, "2023-01-01", "2023-01-01", "2024-05-31", "new", None, 1, 100, 0, 0, "published", now_ts, now_ts, "Superseded by June 2024 update", "seed_sample_data.py", now_ts, now_ts),
        (ER002_V1_ID, ER002_RULE_ID, "v1", 1, "All ER services billed under revenue code 0450. Observation services under 0762.", None, None, "2023-01-01", "2023-01-01", "2023-12-31", "new", None, 1, 90, 0, 0, "published", now_ts, now_ts, "Superseded by January 2024 clarification", "seed_sample_data.py", now_ts, now_ts),
        (ER003_V1_ID, ER003_RULE_ID, "v1", 1, "Outpatient ER claims must use TOB 131 for initial outpatient bill.", None, None, "2023-01-01", "2023-01-01", "2024-08-31", "new", None, 1, 80, 0, 0, "published", now_ts, now_ts, "Superseded by TOB continuation guidance", "seed_sample_data.py", now_ts, now_ts),

        (INPT001_V1_ID, INPT001_RULE_ID, "v1", 1, "Room and board charges billed under revenue codes 0100-0219 based on accommodation type.", None, None, "2023-01-01", "2023-01-01", "2024-03-31", "new", None, 1, 100, 0, 0, "published", now_ts, now_ts, "Superseded by accommodation sub-code clarification", "seed_sample_data.py", now_ts, now_ts),
        (INPT002_V1_ID, INPT002_RULE_ID, "v1", 1, "Hospital inpatient admission through discharge billed under TOB 111.", None, None, "2023-01-01", "2023-01-01", "2023-06-30", "new", None, 1, 90, 0, 0, "published", now_ts, now_ts, "Superseded by interim billing update", "seed_sample_data.py", now_ts, now_ts),
        (INPT003_V1_ID, INPT003_RULE_ID, "v1", 1, "All non-emergency inpatient admissions require prior authorization before service delivery.", None, None, "2023-01-01", "2023-01-01", "2023-12-31", "new", None, 1, 80, 0, 0, "published", now_ts, now_ts, "Superseded by 72-hour window guidance", "seed_sample_data.py", now_ts, now_ts),

        (OUTPT001_V1_ID, OUTPT001_RULE_ID, "v1", 1, "Operating room charges billed under revenue code 0360 for general surgery.", None, None, "2023-01-01", "2023-01-01", "2023-09-30", "new", None, 1, 100, 0, 0, "published", now_ts, now_ts, "Superseded by trauma surgery clarification", "seed_sample_data.py", now_ts, now_ts),
        (OUTPT002_V1_ID, OUTPT002_RULE_ID, "v1", 1, "Ambulatory surgery center claims use TOB 831 for outpatient surgery.", None, None, "2023-01-01", "2023-01-01", "2024-01-31", "new", None, 1, 90, 0, 0, "published", now_ts, now_ts, "Superseded by continuation ASC billing guidance", "seed_sample_data.py", now_ts, now_ts),
        (OUTPT003_V1_ID, OUTPT003_RULE_ID, "v1", 1, "Modifier 73 required when procedure discontinued after patient prep but before anesthesia.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 80, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),

        (THER001_V1_ID, THER001_RULE_ID, "v1", 1, "All outpatient therapy claims must include therapy modifier: GO (OT), GP (PT), GN (ST).", None, None, "2023-01-01", "2023-01-01", "2023-11-30", "new", None, 1, 100, 0, 0, "published", now_ts, now_ts, "Superseded by December 2023 modifier update", "seed_sample_data.py", now_ts, now_ts),
        (THER002_V1_ID, THER002_RULE_ID, "v1", 1, "Occurrence code 17 required - date outpatient OT plan established or last reviewed.", None, None, "2023-01-01", "2023-01-01", "2024-02-28", "new", None, 1, 90, 0, 0, "published", now_ts, now_ts, "Superseded by telehealth extension", "seed_sample_data.py", now_ts, now_ts),
        (THER003_V1_ID, THER003_RULE_ID, "v1", 1, "Occurrence code 29 required - date outpatient PT plan established or last reviewed.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 80, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),

        (LAB001_V1_ID, LAB001_RULE_ID, "v1", 1, "Laboratory services billed under revenue codes 0300-0319 based on lab type.", None, None, "2023-01-01", "2023-01-01", "2024-02-29", "new", None, 1, 100, 0, 0, "published", now_ts, now_ts, "Superseded by March 2024 range expansion", "seed_sample_data.py", now_ts, now_ts),
        (LAB002_V1_ID, LAB002_RULE_ID, "v1", 1, "Diagnostic radiology billed under revenue code 0324. Therapeutic radiology under 0333.", None, None, "2023-01-01", "2023-01-01", "2024-05-31", "new", None, 1, 90, 0, 0, "published", now_ts, now_ts, "Superseded by therapeutic radiology split", "seed_sample_data.py", now_ts, now_ts),
        (LAB003_V1_ID, LAB003_RULE_ID, "v1", 1, "CT scan services billed under revenue code 0351. 25 modifier required for significant separately identifiable procedure.", None, None, "2023-01-01", "2023-01-01", "2023-08-31", "new", None, 1, 80, 0, 0, "published", now_ts, now_ts, "Superseded by modifier documentation clarification", "seed_sample_data.py", now_ts, now_ts),

        (ER001_V2_ID, ER001_RULE_ID, "v2", 2, "Condition Code G0 required when multiple visits occur same day same revenue center, including telehealth visits.", None, None, "2024-06-01", "2024-06-01", "2024-12-31", "modified", ER001_V1_ID, 1, 100, 0, 0, "published", now_ts, now_ts, "Expanded to telehealth visits", "seed_sample_data.py", now_ts, now_ts),
        (THER001_V2_ID, THER001_RULE_ID, "v2", 2, "All outpatient therapy claims must include therapy modifier: GO (OT), GP (PT), GN (ST), and GY for services deemed not medically necessary.", None, None, "2023-12-01", "2023-12-01", None, "modified", THER001_V1_ID, 0, 100, 0, 0, "published", now_ts, now_ts, "Added GY modifier scenario", "seed_sample_data.py", now_ts, now_ts),
        (LAB001_V2_ID, LAB001_RULE_ID, "v2", 2, "Laboratory services billed under revenue codes 0300-0329 based on lab type.", None, None, "2024-03-01", "2024-03-01", None, "modified", LAB001_V1_ID, 0, 100, 0, 0, "published", now_ts, now_ts, "Expanded lab revenue code range", "seed_sample_data.py", now_ts, now_ts),

        (TIE_V1_ID, TEST_TIE_RULE_ID, "v1", 1, "Tie-break test rule v1 - specificity_score=10.", None, None, "2024-01-01", "2024-01-01", None, "new", None, 0, 10, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (TIE_V2_ID, TEST_TIE_RULE_ID, "v2", 2, "Tie-break test rule v2 - specificity_score=5.", None, None, "2024-01-01", "2024-01-01", None, "modified", None, 0, 5, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),

        # enrichment 11 => total 31
        (ER001_V3_ID, ER001_RULE_ID, "v3", 3, "Condition Code G0 required when multiple medical visits occur on the same day in the same revenue center, including telehealth visits and remote patient monitoring encounters. Report G0 on the second claim.", None, None, "2025-01-01", "2025-01-01", None, "modified", ER001_V2_ID, 0, 100, 0, 0, "published", now_ts, now_ts, "G0 code expanded to include remote patient monitoring.", "seed_sample_data.py", now_ts, now_ts),
        (ER002_V2_ID, ER002_RULE_ID, "v2", 2, "All ER services must be billed under revenue code 0450. Observation services must be billed under revenue code 0762. These two codes are mutually exclusive and may not appear together on the same claim.", None, None, "2024-01-01", "2024-01-01", None, "modified", ER002_V1_ID, 0, 90, 0, 0, "published", now_ts, now_ts, "Added explicit mutual exclusivity rule for codes 0450 and 0762.", "seed_sample_data.py", now_ts, now_ts),
        (ER003_V2_ID, ER003_RULE_ID, "v2", 2, "Outpatient ER initial claims must use Type of Bill 131. Continuation outpatient ER claims submitted for the same episode must use Type of Bill 132.", None, None, "2024-09-01", "2024-09-01", None, "modified", ER003_V1_ID, 0, 80, 0, 0, "published", now_ts, now_ts, "TOB 132 added for ER continuation claims.", "seed_sample_data.py", now_ts, now_ts),
        (INPT001_V2_ID, INPT001_RULE_ID, "v2", 2, "Room and board charges must be billed under revenue codes 0100-0219 based on accommodation type. Sub-classifications: semi-private (0120), private (0130), deluxe private (0160).", None, None, "2024-04-01", "2024-04-01", None, "modified", INPT001_V1_ID, 0, 100, 0, 0, "published", now_ts, now_ts, "Sub-code examples added for common accommodation types.", "seed_sample_data.py", now_ts, now_ts),
        (INPT002_V2_ID, INPT002_RULE_ID, "v2", 2, "Hospital inpatient admission through discharge must be billed under Type of Bill 111. For long-stay patients requiring interim billing, use Type of Bill 117 for interim claims with patient status code 30.", None, None, "2023-07-01", "2023-07-01", None, "modified", INPT002_V1_ID, 0, 90, 0, 0, "published", now_ts, now_ts, "TOB 117 added for interim inpatient billing.", "seed_sample_data.py", now_ts, now_ts),
        (INPT003_V2_ID, INPT003_RULE_ID, "v2", 2, "All non-emergency inpatient admissions require prior authorization before service delivery. Authorization must be obtained at least 72 hours in advance of the scheduled admission date.", None, None, "2024-01-01", "2024-01-01", None, "modified", INPT003_V1_ID, 0, 80, 0, 0, "published", now_ts, now_ts, "Explicit 72-hour advance authorization window added.", "seed_sample_data.py", now_ts, now_ts),
        (OUTPT001_V2_ID, OUTPT001_RULE_ID, "v2", 2, "Operating room charges for general surgery must be billed under revenue code 0360. Trauma surgery cases requiring specialized facilities must use revenue code 0361.", None, None, "2023-10-01", "2023-10-01", None, "modified", OUTPT001_V1_ID, 0, 100, 0, 0, "published", now_ts, now_ts, "Revenue code 0361 added for trauma surgery cases.", "seed_sample_data.py", now_ts, now_ts),
        (OUTPT002_V2_ID, OUTPT002_RULE_ID, "v2", 2, "Ambulatory surgery center initial claims for outpatient surgery must use Type of Bill 831. Continuation ASC claims for the same episode use Type of Bill 832.", None, None, "2024-02-01", "2024-02-01", None, "modified", OUTPT002_V1_ID, 0, 90, 0, 0, "published", now_ts, now_ts, "TOB 832 added for ASC continuation billing.", "seed_sample_data.py", now_ts, now_ts),
        (LAB002_V2_ID, LAB002_RULE_ID, "v2", 2, "Diagnostic radiology services must be billed under revenue code 0324. Therapeutic radiology services are sub-classified: external beam radiation under 0333, brachytherapy services under 0341.", None, None, "2024-06-01", "2024-06-01", None, "modified", LAB002_V1_ID, 0, 90, 0, 0, "published", now_ts, now_ts, "Therapeutic radiology split into 0333 and 0341.", "seed_sample_data.py", now_ts, now_ts),
        (LAB003_V2_ID, LAB003_RULE_ID, "v2", 2, "CT scan services must be billed under revenue code 0351. Modifier 25 is required when a significant, separately identifiable evaluation and management service is rendered on the same date and documented separately.", None, None, "2023-09-01", "2023-09-01", None, "modified", LAB003_V1_ID, 0, 80, 0, 0, "published", now_ts, now_ts, "Modifier 25 documentation requirement clarified.", "seed_sample_data.py", now_ts, now_ts),
        (THER002_V2_ID, THER002_RULE_ID, "v2", 2, "Occurrence code 17 is required to report the date the outpatient Occupational Therapy plan of care was established or last reviewed. This requirement applies to both in-person and telehealth OT services.", None, None, "2024-03-01", "2024-03-01", None, "modified", THER002_V1_ID, 0, 90, 0, 0, "published", now_ts, now_ts, "Occurrence code 17 requirement extended to telehealth OT services.", "seed_sample_data.py", now_ts, now_ts),

        # expansion 12 => total 43
        (ER004_V1_ID, ER004_RULE_ID, "v1", 1, "When an emergency department patient requires intensive care following ER treatment, ICU/critical care services are billed under revenue code 0200.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 70, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (INPT004_V1_ID, INPT004_RULE_ID, "v1", 1, "Routine newborn nursery services are billed under revenue code 0170. Level II special care nursery uses 0171. Level III NICU uses 0172.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 70, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (INPT005_V1_ID, INPT005_RULE_ID, "v1", 1, "When an inpatient is transferred to a skilled nursing facility upon discharge, the inpatient claim is billed with Type of Bill 112 and patient status code 03.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 60, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (INPT006_V1_ID, INPT006_RULE_ID, "v1", 1, "Inpatient psychiatric unit services are billed under Type of Bill 114 and are subject to per diem reimbursement under rate code 2852.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 50, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (INPT007_V1_ID, INPT007_RULE_ID, "v1", 1, "Occurrence code 16 must be reported to indicate the date of referral to home health agency for all discharges with patient status code 06.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 40, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (INPT008_V1_ID, INPT008_RULE_ID, "v1", 1, "When a Medicaid inpatient is readmitted to the same facility within 30 days of discharge for the same or related condition, condition code 70 must be reported.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 30, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (OUTPT004_V1_ID, OUTPT004_RULE_ID, "v1", 1, "Pre-admission diagnostic and clinically related non-diagnostic services provided within three days before an inpatient admission are included in the inpatient claim and may not be billed separately as outpatient services.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 70, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (OUTPT005_V1_ID, OUTPT005_RULE_ID, "v1", 1, "Modifier 50 is required when the same outpatient surgical procedure is performed bilaterally during the same operative session. Reimbursement is 150 percent of the base fee schedule amount when bilateral surgery rules apply.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 60, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (LAB004_V1_ID, LAB004_RULE_ID, "v1", 1, "Microbiology laboratory services including bacteriology, mycology, parasitology, and virology testing are billed under revenue code 0305.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 70, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (LAB005_V1_ID, LAB005_RULE_ID, "v1", 1, "Hematology laboratory services including CBC, coagulation studies, and bone marrow evaluations are billed under revenue code 0306.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 60, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (LAB006_V1_ID, LAB006_RULE_ID, "v1", 1, "Clinical chemistry laboratory services including metabolic panels, electrolytes, liver function studies, and enzyme testing are billed under revenue code 0307.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 50, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
        (LAB007_V1_ID, LAB007_RULE_ID, "v1", 1, "Surgical pathology and tissue examination services including gross and microscopic evaluation are billed under revenue code 0308.", None, None, "2023-01-01", "2023-01-01", None, "new", None, 0, 40, 0, 0, "published", now_ts, now_ts, None, "seed_sample_data.py", now_ts, now_ts),
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO rule_versions (
            rule_version_id, atomic_rule_id, version_label, version_number,
            normalized_rule_text, display_rule_text, interpretation_notes,
            publication_date, effective_start_date, effective_end_date,
            change_type, supersedes_rule_version_id, is_superseded,
            specificity_score, exception_flag, resolver_priority,
            lifecycle_status, published_at, approved_at, change_summary,
            created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def seed_rule_version_anchors(conn: sqlite3.Connection, now_ts: str) -> None:
    rows = [
        # baseline 21
        (1, ER001_V1_ID, CC_G0_ID, 1, "Primary condition code anchor for ER-001 v1", now_ts),
        (2, ER002_V1_ID, RC_0450_ID, 1, "Primary revenue code anchor for ER-002 v1", now_ts),
        (3, ER003_V1_ID, TOB_131_ID, 1, "Primary TOB anchor for ER-003 v1", now_ts),

        (4, INPT001_V1_ID, RC_0120_ID, 1, "Primary revenue code anchor for INPT-001 v1", now_ts),
        (5, INPT002_V1_ID, TOB_111_ID, 1, "Primary TOB anchor for INPT-002 v1", now_ts),
        (6, INPT003_V1_ID, CC_07_ID, 1, "Primary condition code anchor for INPT-003 v1", now_ts),

        (7, OUTPT001_V1_ID, RC_0360_ID, 1, "Primary revenue code anchor for OUTPT-001 v1", now_ts),
        (8, OUTPT002_V1_ID, TOB_831_ID, 1, "Primary TOB anchor for OUTPT-002 v1", now_ts),
        (9, OUTPT003_V1_ID, MOD_73_ID, 1, "Primary modifier anchor for OUTPT-003 v1", now_ts),

        (10, THER001_V1_ID, MOD_GO_ID, 1, "Primary modifier anchor for THER-001 v1", now_ts),
        (11, THER002_V1_ID, CC_17_ID, 1, "Primary condition code anchor for THER-002 v1", now_ts),
        (12, THER003_V1_ID, CC_29_ID, 1, "Primary condition code anchor for THER-003 v1", now_ts),

        (13, LAB001_V1_ID, RC_0301_ID, 1, "Primary revenue code anchor for LAB-001 v1", now_ts),
        (14, LAB002_V1_ID, RC_0324_ID, 1, "Primary revenue code anchor for LAB-002 v1", now_ts),
        (15, LAB003_V1_ID, RC_0351_ID, 1, "Primary revenue code anchor for LAB-003 v1", now_ts),

        (16, ER001_V2_ID, CC_G0_ID, 1, "Primary condition code anchor for ER-001 v2", now_ts),
        (17, THER001_V2_ID, MOD_GO_ID, 1, "Primary modifier anchor for THER-001 v2", now_ts),
        (18, LAB001_V2_ID, RC_0301_ID, 1, "Primary revenue code anchor for LAB-001 v2", now_ts),
        (19, THER001_V2_ID, MOD_GY_ID, 0, "Secondary modifier anchor for added GY scenario", now_ts),

        (20, TIE_V1_ID, RC_0450_ID, 1, "Primary anchor for tie-break test v1", now_ts),
        (21, TIE_V2_ID, RC_0450_ID, 1, "Primary anchor for tie-break test v2", now_ts),

        # enrichment 11 => total 32
        (22, ER001_V3_ID, CC_G0_ID, 1, "Primary anchor for ER-001 v3", now_ts),
        (23, ER002_V2_ID, RC_0450_ID, 1, "Primary anchor for ER-002 v2", now_ts),
        (24, ER003_V2_ID, TOB_131_ID, 1, "Primary anchor for ER-003 v2", now_ts),
        (25, INPT001_V2_ID, RC_0120_ID, 1, "Primary anchor for INPT-001 v2", now_ts),
        (26, INPT002_V2_ID, TOB_111_ID, 1, "Primary anchor for INPT-002 v2", now_ts),
        (27, INPT003_V2_ID, CC_07_ID, 1, "Primary anchor for INPT-003 v2", now_ts),
        (28, OUTPT001_V2_ID, RC_0360_ID, 1, "Primary anchor for OUTPT-001 v2", now_ts),
        (29, OUTPT002_V2_ID, TOB_831_ID, 1, "Primary anchor for OUTPT-002 v2", now_ts),
        (30, LAB002_V2_ID, RC_0324_ID, 1, "Primary anchor for LAB-002 v2", now_ts),
        (31, LAB003_V2_ID, RC_0351_ID, 1, "Primary anchor for LAB-003 v2", now_ts),
        (32, THER002_V2_ID, CC_17_ID, 1, "Primary anchor for THER-002 v2", now_ts),

        # expansion 12 => total 44
        (33, ER004_V1_ID, RC_0200_ID, 1, "Primary anchor for ER-004 v1", now_ts),
        (34, INPT004_V1_ID, RC_0170_ID, 1, "Primary anchor for INPT-004 v1", now_ts),
        (35, INPT005_V1_ID, TOB_112_ID, 1, "Primary anchor for INPT-005 v1", now_ts),
        (36, INPT006_V1_ID, TOB_114_ID, 1, "Primary anchor for INPT-006 v1", now_ts),
        (37, INPT007_V1_ID, CC_16_ID, 1, "Primary anchor for INPT-007 v1", now_ts),
        (38, INPT008_V1_ID, CC_70_ID, 1, "Primary anchor for INPT-008 v1", now_ts),
        (39, OUTPT004_V1_ID, TOB_111_ID, 1, "Primary anchor for OUTPT-004 v1", now_ts),
        (40, OUTPT005_V1_ID, MOD_50_ID, 1, "Primary anchor for OUTPT-005 v1", now_ts),
        (41, LAB004_V1_ID, RC_0305_ID, 1, "Primary anchor for LAB-004 v1", now_ts),
        (42, LAB005_V1_ID, RC_0306_ID, 1, "Primary anchor for LAB-005 v1", now_ts),
        (43, LAB006_V1_ID, RC_0307_ID, 1, "Primary anchor for LAB-006 v1", now_ts),
        (44, LAB007_V1_ID, RC_0308_ID, 1, "Primary anchor for LAB-007 v1", now_ts),
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO rule_version_anchors (
            rule_version_anchor_id, rule_version_id, ub04_anchor_code_id,
            is_primary, anchor_notes, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def seed_rule_evidence_links(conn: sqlite3.Connection, now_ts: str) -> None:
    rows = [
        # baseline 21
        (1, ER001_V1_ID, 1, BASE_MANUAL_DOC_ID, "primary", "ER-001 v1 primary evidence", 10, 10, "ER-001", None, now_ts),
        (2, ER002_V1_ID, 2, BASE_MANUAL_DOC_ID, "primary", "ER-002 v1 primary evidence", 11, 11, "ER-002", None, now_ts),
        (3, ER003_V1_ID, 3, BASE_MANUAL_DOC_ID, "primary", "ER-003 v1 primary evidence", 12, 12, "ER-003", None, now_ts),

        (4, INPT001_V1_ID, 4, BASE_MANUAL_DOC_ID, "primary", "INPT-001 v1 primary evidence", 20, 20, "INPT-001", None, now_ts),
        (5, INPT002_V1_ID, 5, BASE_MANUAL_DOC_ID, "primary", "INPT-002 v1 primary evidence", 21, 21, "INPT-002", None, now_ts),
        (6, INPT003_V1_ID, 6, BASE_MANUAL_DOC_ID, "primary", "INPT-003 v1 primary evidence", 22, 22, "INPT-003", None, now_ts),

        (7, OUTPT001_V1_ID, 7, BASE_MANUAL_DOC_ID, "primary", "OUTPT-001 v1 primary evidence", 30, 30, "OUTPT-001", None, now_ts),
        (8, OUTPT002_V1_ID, 8, BASE_MANUAL_DOC_ID, "primary", "OUTPT-002 v1 primary evidence", 31, 31, "OUTPT-002", None, now_ts),
        (9, OUTPT003_V1_ID, 9, BASE_MANUAL_DOC_ID, "primary", "OUTPT-003 v1 primary evidence", 32, 32, "OUTPT-003", None, now_ts),

        (10, THER001_V1_ID, 10, BASE_MANUAL_DOC_ID, "primary", "THER-001 v1 primary evidence", 40, 40, "THER-001", None, now_ts),
        (11, THER002_V1_ID, 11, BASE_MANUAL_DOC_ID, "primary", "THER-002 v1 primary evidence", 41, 41, "THER-002", None, now_ts),
        (12, THER003_V1_ID, 12, BASE_MANUAL_DOC_ID, "primary", "THER-003 v1 primary evidence", 42, 42, "THER-003", None, now_ts),

        (13, LAB001_V1_ID, 13, BASE_MANUAL_DOC_ID, "primary", "LAB-001 v1 primary evidence", 50, 50, "LAB-001", None, now_ts),
        (14, LAB002_V1_ID, 14, BASE_MANUAL_DOC_ID, "primary", "LAB-002 v1 primary evidence", 51, 51, "LAB-002", None, now_ts),
        (15, LAB003_V1_ID, 15, BASE_MANUAL_DOC_ID, "primary", "LAB-003 v1 primary evidence", 52, 52, "LAB-003", None, now_ts),

        (16, ER001_V2_ID, 16, ER_BULLETIN_DOC_ID, "primary", "ER-001 v2 primary evidence", 5, 5, "ER-001 v2", None, now_ts),
        (17, THER001_V2_ID, 17, THER_BULLETIN_DOC_ID, "primary", "THER-001 v2 primary evidence", 6, 6, "THER-001 v2", None, now_ts),
        (18, LAB001_V2_ID, 18, LAB_BULLETIN_DOC_ID, "primary", "LAB-001 v2 primary evidence", 7, 7, "LAB-001 v2", None, now_ts),

        (19, ER002_V1_ID, None, GUIDELINE_DOC_ID, "supporting", "Supporting guideline reference for ER billing workflow", 2, 2, "ER guideline", "Supporting evidence example", now_ts),

        (20, TIE_V1_ID, None, BASE_MANUAL_DOC_ID, "primary", "Tie-break test v1 evidence", 1, 1, "TEST-TIE-001 v1", None, now_ts),
        (21, TIE_V2_ID, None, BASE_MANUAL_DOC_ID, "primary", "Tie-break test v2 evidence", 1, 1, "TEST-TIE-001 v2", None, now_ts),

        # enrichment 11 => total 32
        (22, ER001_V3_ID, 19, COMPREHENSIVE_2025_DOC_ID, "primary", "ER-001 v3 evidence", 3, 3, "ER-001 v3", None, now_ts),
        (23, ER002_V2_ID, 20, ER_UPDATES_2024_DOC_ID, "primary", "ER-002 v2 evidence", 4, 4, "ER-002 v2", None, now_ts),
        (24, ER003_V2_ID, 21, ER_UPDATES_2024_DOC_ID, "primary", "ER-003 v2 evidence", 5, 5, "ER-003 v2", None, now_ts),
        (25, INPT001_V2_ID, 22, INPT_UPDATES_2023_DOC_ID, "primary", "INPT-001 v2 evidence", 8, 8, "INPT-001 v2", None, now_ts),
        (26, INPT002_V2_ID, 23, INPT_UPDATES_2023_DOC_ID, "primary", "INPT-002 v2 evidence", 9, 9, "INPT-002 v2", None, now_ts),
        (27, INPT003_V2_ID, 24, INPT_UPDATES_2023_DOC_ID, "primary", "INPT-003 v2 evidence", 10, 10, "INPT-003 v2", None, now_ts),
        (28, OUTPT001_V2_ID, 25, OUTPT_LAB_2023_DOC_ID, "primary", "OUTPT-001 v2 evidence", 6, 6, "OUTPT-001 v2", None, now_ts),
        (29, OUTPT002_V2_ID, 26, OUTPT_LAB_2023_DOC_ID, "primary", "OUTPT-002 v2 evidence", 7, 7, "OUTPT-002 v2", None, now_ts),
        (30, LAB002_V2_ID, 27, ER_UPDATES_2024_DOC_ID, "primary", "LAB-002 v2 evidence", 11, 11, "LAB-002 v2", None, now_ts),
        (31, LAB003_V2_ID, 28, OUTPT_LAB_2023_DOC_ID, "primary", "LAB-003 v2 evidence", 12, 12, "LAB-003 v2", None, now_ts),
        (32, THER002_V2_ID, 29, COMPREHENSIVE_2025_DOC_ID, "primary", "THER-002 v2 evidence", 13, 13, "THER-002 v2", None, now_ts),

        # expansion 12 => total 44
        (33, ER004_V1_ID, 30, BASE_MANUAL_DOC_ID, "primary", "ER-004 v1 evidence", 60, 60, "ER-004", None, now_ts),
        (34, INPT004_V1_ID, 31, BASE_MANUAL_DOC_ID, "primary", "INPT-004 v1 evidence", 70, 70, "INPT-004", None, now_ts),
        (35, INPT005_V1_ID, 32, BASE_MANUAL_DOC_ID, "primary", "INPT-005 v1 evidence", 71, 71, "INPT-005", None, now_ts),
        (36, INPT006_V1_ID, 33, BASE_MANUAL_DOC_ID, "primary", "INPT-006 v1 evidence", 72, 72, "INPT-006", None, now_ts),
        (37, INPT007_V1_ID, 34, BASE_MANUAL_DOC_ID, "primary", "INPT-007 v1 evidence", 73, 73, "INPT-007", None, now_ts),
        (38, INPT008_V1_ID, 35, BASE_MANUAL_DOC_ID, "primary", "INPT-008 v1 evidence", 74, 74, "INPT-008", None, now_ts),
        (39, OUTPT004_V1_ID, 36, BASE_MANUAL_DOC_ID, "primary", "OUTPT-004 v1 evidence", 75, 75, "OUTPT-004", None, now_ts),
        (40, OUTPT005_V1_ID, 37, BASE_MANUAL_DOC_ID, "primary", "OUTPT-005 v1 evidence", 76, 76, "OUTPT-005", None, now_ts),
        (41, LAB004_V1_ID, 38, BASE_MANUAL_DOC_ID, "primary", "LAB-004 v1 evidence", 90, 90, "LAB-004", None, now_ts),
        (42, LAB005_V1_ID, 39, BASE_MANUAL_DOC_ID, "primary", "LAB-005 v1 evidence", 91, 91, "LAB-005", None, now_ts),
        (43, LAB006_V1_ID, 40, BASE_MANUAL_DOC_ID, "primary", "LAB-006 v1 evidence", 92, 92, "LAB-006", None, now_ts),
        (44, LAB007_V1_ID, 41, BASE_MANUAL_DOC_ID, "primary", "LAB-007 v1 evidence", 93, 93, "LAB-007", None, now_ts),
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO rule_evidence_links (
            rule_evidence_link_id, rule_version_id, policy_fragment_id, source_document_id,
            evidence_role, citation_text, page_number_start, page_number_end,
            section_reference, trust_note, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def verify_seed(conn: sqlite3.Connection) -> bool:
    def count(table_name: str) -> int:
        return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    checks = [
        ("source_types", 3),
        ("rule_topics", 6),
        ("service_types", 3),
        ("ub04_anchor_types", 4),
        ("ub04_anchor_codes", 28),
        ("source_documents", 9),
        ("policy_fragments", 41),
        ("atomic_rules", 28),
        ("rule_versions", 43),
        ("rule_version_anchors", 44),
        ("rule_evidence_links", 44),
    ]

    all_ok = True
    print("\n[seed] Row count checks")
    for table_name, expected in checks:
        actual = count(table_name)
        ok = actual == expected
        all_ok = all_ok and ok
        status = "PASS" if ok else "FAIL"
        print(f"  {status:<4} {table_name:<22} {actual} rows (expected {expected})")

    supersession_checks = [
        (ER001_V1_ID, ER001_V2_ID, "2024-05-31"),
        (THER001_V1_ID, THER001_V2_ID, "2023-11-30"),
        (LAB001_V1_ID, LAB001_V2_ID, "2024-02-29"),
    ]
    print("\n[seed] Supersession checks")
    for v1_id, v2_id, expected_end in supersession_checks:
        v1 = conn.execute(
            "SELECT effective_end_date, is_superseded FROM rule_versions WHERE rule_version_id = ?",
            (v1_id,),
        ).fetchone()
        v2 = conn.execute(
            "SELECT supersedes_rule_version_id, is_published FROM rule_versions WHERE rule_version_id = ?",
            (v2_id,),
        ).fetchone()
        ok = (
            v1 is not None
            and v2 is not None
            and v1["effective_end_date"] == expected_end
            and v1["is_superseded"] == 1
            and v2["supersedes_rule_version_id"] == v1_id
            and v2["is_published"] == 1
        )
        all_ok = all_ok and ok
        status = "PASS" if ok else "FAIL"
        print(f"  {status:<4} v1={v1_id} → v2={v2_id} (v1 ends {expected_end})")

    published = conn.execute(
        """
        SELECT COUNT(*)
        FROM rule_versions
        WHERE lifecycle_status = 'published' AND is_published = 1
        """
    ).fetchone()[0]
    expected_published = 43
    ok = published == expected_published
    all_ok = all_ok and ok
    status = "PASS" if ok else "FAIL"
    print("\n[seed] Published queryable versions")
    print(f"  {status:<4} published+queryable rule_versions: {published} (expected {expected_published})")

    expected_topic_counts = {
        "ER": 4,
        "INPT": 8,
        "OUTPT": 5,
        "THER": 3,
        "LAB": 7,
    }
    print("\n[seed] Topic rule counts")
    topic_rows = conn.execute(
        """
        SELECT rt.topic_code, COUNT(*) AS rule_count
        FROM atomic_rules ar
        JOIN rule_topics rt
          ON ar.rule_topic_id = rt.rule_topic_id
        WHERE rt.topic_code != 'TEST'
        GROUP BY rt.topic_code
        ORDER BY rt.display_order
        """
    ).fetchall()

    for row in topic_rows:
        expected = expected_topic_counts[row["topic_code"]]
        actual = row["rule_count"]
        ok = actual == expected
        all_ok = all_ok and ok
        status = "PASS" if ok else "FAIL"
        print(f"  {status:<4} {row['topic_code']:<5} {actual} rules (expected {expected})")

    print(f"\n[seed] {'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
    return all_ok


def seed_database(db_path: Path = DB_PATH) -> None:
    ensure_db_exists(db_path)
    now_ts = utc_now_str()

    conn = get_connection(db_path)
    try:
        seed_lookup_tables(conn, now_ts)
        seed_source_documents(conn, now_ts)
        seed_policy_fragments(conn, now_ts)
        seed_atomic_rules(conn, now_ts)
        seed_rule_versions(conn, now_ts)
        seed_rule_version_anchors(conn, now_ts)
        seed_rule_evidence_links(conn, now_ts)
        conn.commit()

        print(f"[seed] Seed data loaded into: {db_path}")
        verify_seed(conn)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    db_path = DB_PATH

    if "--reset" in sys.argv:
        if db_path.exists():
            db_path.unlink()
            print(f"[seed] Removed existing database: {db_path}")
        init_db(db_path)

    seed_database(db_path)