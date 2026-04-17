# Dynamic Payer Knowledge Base (DPKB) - CL_V2

**BIG Consulting**
Project Manager: Aditi Patil | April 2026

---

## What This Is

DPKB is a versioned, deterministic rule retrieval system for New York Medicaid facility billing (UB-04/CMS-1450 claims).

It answers one question:
**"Which NY Medicaid facility billing rule governed this claim on this date of service or discharge?"**

---

## Tech Stack

- **Backend:** FastAPI + uvicorn (port 8000)
- **Frontend:** React + Vite (port 5173)
- **Database:** SQLite WAL mode - 15 tables
- **Auth:** JWT (python-jose) + bcrypt
- **PDF Extraction:** pdfplumber (primary) + PyMuPDF (fallback)
- **NLP:** Regex + python-dateutil

No vector database. No chatbot. No LLM. Fully deterministic.

---

## Folder Structure

```
CL_V2/
├── backend/
│   ├── main.py
│   ├── dependencies.py
│   ├── routers/
│   │   ├── auth.py
│   │   ├── rules.py
│   │   ├── notifications.py
│   │   ├── fragments.py
│   │   └── crawler.py
│   └── schemas/
│       ├── user.py
│       └── rule.py
├── frontend/
│   └── src/
│       ├── App.jsx
│       ├── api/client.js
│       ├── pages/
│       │   ├── Login.jsx
│       │   ├── StaffPortal.jsx
│       │   ├── AdminPortal.jsx
│       │   └── SMEPortal.jsx
│       └── components/
│           ├── RuleCard.jsx
│           └── VersionHistory.jsx
├── modules/
│   ├── module_a/crawler.py
│   ├── module_c/rule_publisher.py
│   └── module_e/resolver.py
├── db/dpkb.db
├── data/raw/
├── tests/
├── constants.py
├── db_init.py
├── seed_sample_data.py
├── requirements.txt
└── README.md
```

---

## How To Run

### 1. Install dependencies
```
pip install -r requirements.txt
```

### 2. Initialize database
```
python db_init.py
```

### 3. Seed sample data
```
python seed_sample_data.py --reset
```

### 4. Start backend (Terminal 1)
```
python -m uvicorn backend.main:app --reload
```

### 5. Start frontend (Terminal 2)
```
cd frontend
npm install
npm run dev
```

### 6. Open browser
```
http://localhost:5173
```

---

## Demo Credentials

| Username   | Password  | Role  | Access                                      |
|------------|-----------|-------|---------------------------------------------|
| staff_user | staff123  | Staff | Query rules by topic and date               |
| admin_user | admin123  | Admin | Review TIER 2 fragments, trigger crawler    |
| sme_user   | sme123    | SME   | Resolve TIER 3 fragments                    |

---

## Demo Flow

### Staff Portal
1. Login as staff_user
2. Select Emergency Room Services
3. Set date to 2025-01-01 - returns ER-001 v2 (current rule)
4. Set date to 2022-09-15 - returns ER-001 v1 (superseded - historical retrieval works)
5. Set date to 2020-01-01 - No billing rules found
6. Click Version History on any rule - shows full supersession chain

### Admin Portal
1. Login as admin_user
2. TIER 2 fragment shows physical therapy modifier rule
3. Fill: Rule Code THER-002, Topic THER, Effective Start 2024-01-01
4. Click Approve and Publish
5. Click Staff View - search Therapy topic - THER-002 appears
6. Notification banner fires on Staff Portal within 30 seconds

### SME Portal
1. Login as sme_user
2. TIER 3 fragment shows ambiguous observation services fragment
3. Fill: Rule Code OBS-001, Topic OBS, Effective Start 2022-01-01
4. Add interpretation notes
5. Click Approve and Publish - OBS-001 now queryable in Staff Portal

---

## Key Guarantees

- Historical rules are never deleted
- A query for any past date returns the rule active on that exact date
- Every rule traces back to its source document and page
- Same inputs always produce same output - fully deterministic
- No rule published without human or SME review

---

## Running Tests

```
python tests/test_retrieval.py
```

Expected output:
```
ALL 6 TESTS PASSED
```

```
python tests/test_v2_vertical_slice.py
```

Expected output:
```
Ran 4 tests

OK
```

---

## API Endpoints

| Method | Endpoint                    | Role       | Description                          |
|--------|-----------------------------|------------|--------------------------------------|
| POST   | /api/auth/login             | Public     | Login - returns JWT                  |
| GET    | /api/rules/topics           | Staff+     | List all 15 billing topics           |
| GET    | /api/rules/anchors          | Staff+     | List UB-04 anchor types              |
| POST   | /api/rules/resolve          | Staff+     | Resolve rules by topic + date        |
| GET    | /api/rules/history/{code}   | Staff+     | Version history for a rule code      |
| GET    | /api/fragments/tier2        | Admin      | TIER 2 review queue                  |
| GET    | /api/fragments/tier3        | Admin, SME | TIER 3 escalation queue              |
| POST   | /api/fragments/approve      | Admin, SME | Approve fragment - publishes rule    |
| POST   | /api/fragments/escalate     | Admin      | Escalate TIER 2 to SME              |
| POST   | /api/fragments/reject       | Admin, SME | Reject fragment                      |
| POST   | /api/crawler/run            | Admin      | Trigger mock crawler run             |
| GET    | /api/crawler/status         | Admin      | Status of most recent crawler run    |
| GET    | /api/notifications/unread   | Staff+     | Unread notification count and list   |
| POST   | /api/notifications/read     | Staff+     | Mark all notifications as read       |
| GET    | /api/health                 | Public     | Health check                         |

---

## Database Schema - 15 Tables

| # | Table                 | Purpose                                      |
|---|-----------------------|----------------------------------------------|
| 1 | source_types          | Lookup - MANUAL, BULLETIN, GUIDELINE         |
| 2 | rule_topics           | Lookup - 15 billing topics                   |
| 3 | service_types         | Lookup - INPATIENT, OUTPATIENT, EMERGENCY    |
| 4 | ub04_anchor_types     | Lookup - 10 UB-04 field types + ANCHOR_OTHER |
| 5 | ub04_anchor_codes     | Lookup - specific code values                |
| 6 | source_documents      | Every PDF downloaded by crawler              |
| 7 | policy_fragments      | Raw extracted text from Module B             |
| 8 | atomic_rules          | Stable rule identity across versions         |
| 9 | rule_versions         | Versioned rule content with effective dates  |
| 10| rule_version_anchors  | Links rule to UB-04 form field               |
| 11| rule_evidence_links   | Traces rule to source document               |
| 12| resolution_logs       | Full query audit trail                       |
| 13| users                 | Role-based auth - bcrypt hashed passwords    |
| 14| notifications         | Real-time update alerts                      |
| 15| crawler_runs          | Crawler execution log                        |

---

## Core Architecture Decision

DPKB V2 uses deterministic SQL temporal filtering, not vector search or RAG.

Billing rule retrieval requires that the same query on the same date always returns the same rule. Vector similarity search is probabilistic and cannot satisfy this requirement. Temporal SQL with effective date windows is the correct and sufficient architecture for this use case.

A vector database migration path is documented for future phases when natural language querying is required.

---

## What Is Not Yet Complete

- Real eMedNY PDF extraction pipeline (Module B)
- 200+ real curated rules from production eMedNY documents
- Real crawler with HTML bulletin traversal (health.ny.gov)
- PostgreSQL migration
- Production deployment on Azure

These are Phase 3 items and are not blockers for demonstrating the V2 architecture and workflow.

---

## Locked Contracts - Never Violate

- 3 PRAGMAs on every DB connection: foreign_keys=ON, journal_mode=WAL, busy_timeout=5000
- Python-only UTC timestamps - never SQLite DEFAULT CURRENT_TIMESTAMP
- is_published set only by DB triggers - never by application code
- Never delete superseded rules - effective_end_date closes a version, not deletion
- constants.py is the single source of truth for all enums