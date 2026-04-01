# Dynamic Payer Knowledge Base (DPKB)
`Midpoint MVP - April 1, 2026`

---

## What Is This?

DPKB is a versioned, structured query tool for **New York Medicaid facility billing rules** (UB-04 / CMS-1450 claims). It lets billing staff select a topic and enter a date to retrieve the exact rule version that was in effect on that date - eliminating the need to manually cross-reference outdated payer manuals.

> **This is not a chatbot.** It is a deterministic, date-driven rule retrieval system.

---

## The Problem

Billing rules change monthly or quarterly. Staff  manage accounts up to 3 years old and must apply the rule version active on the **date of service or discharge** - not the current rule. Today, that research is done manually via OneNote notebooks that are not automatically updated, leading to:
- Avoidable claim denials
- Delayed payments
- Administrative rework on retroactive accounts

---

## Scope

| Dimension | In Scope |
|---|---|
| Payer | New York Medicaid only |
| State | New York only |
| Billing Form | UB-04 (CMS-1450) - facility billing only |
| Data Sources | Publicly accessible NY Medicaid website (no paywalls) |
| Interface | Streamlit query UI |
| Database | SQLite (MVP) |

**Explicitly out of scope (this semester):** other states, other payers, physician/professional billing (CMS-1500), chatbot interface, production-grade UI.

---

## System Architecture

The system is organized into six modules (A–F):

```
[Module A: Crawler]  →  [Module B: Extractor]  →  [Module C: Curation UI]
                                                           ↓
                                          [Module D: SQLite Knowledge Base]
                                                           ↓
                                            [Module E: Temporal Resolver]
                                                           ↓
                                           [Module F: Staff Query Interface]
```

| Module | Role | Status |
|---|---|---|
| A | Web crawler - downloads PDFs from eMedNY and health.ny.gov | MVP (basic) |
| B | NLP extractor - parses PDFs into policy fragments | Phase 3 (post-midterm) |
| C | Manual curation UI - Streamlit data entry form with Pydantic validation | MVP |
| D | SQLite knowledge base - 12-table schema with full version history | Complete |
| E | Temporal resolver - returns exact rule version active on a given date | MVP |
| F | Staff query interface - dropdown + date picker, result panel, version history | MVP |

---

## Database Schema (MVP - 12 Tables)

Core tables:

- `lookup_*` - reference data: rule topics, TOB codes, revenue codes, condition codes, source types
- `source_documents` - every PDF ingested, with SHA-256 hash and retrieval timestamp
- `policy_fragments` - raw text blocks extracted from source documents
- `atomic_rules` - smallest independently applicable billing instructions
- `rule_versions` - versioned snapshots of each atomic rule with effective windows
- `rule_version_anchors` - links rule versions to UB-04 field codes (revenue codes, TOB codes, etc.)
- `rule_evidence_links` - traces each rule version to its source document and page
- `resolution_logs` - full audit trail of every query evaluated
- `resolver_snapshots` - point-in-time snapshots for audit and appeals

**Key design constraint:** Superseded rules are never deleted. A query for a historical date returns the rule active on that date, not the current version.

---

## MVP Billing Topics (Seed Data)

| Code | Topic |
|---|---|
| ER | Emergency Room Services |
| INPT | Inpatient Admission |
| OUTPT | Outpatient Services |
| THER | Therapy (PT / OT / ST) |
| LAB | Laboratory & Radiology |

Each topic includes a minimum of 3 supersession chains (rule A → rule B → rule C with different effective date windows).

---

## Temporal Resolution Logic

The resolver follows a strict priority hierarchy when returning a rule for a given query date:

1. Filter: `effective_start ≤ query_date AND (effective_end IS NULL OR effective_end ≥ query_date)`
2. Tie-break: specificity → exception precedence → authority rank → version number

**Four resolver output states:** `MATCH` | `NO_MATCH` | `MULTIPLE_CANDIDATES` | `REVIEW_REQUIRED`

All queries are logged to `resolution_logs` for full auditability.

---

## Project Structure

```
dpkb/
├── constants.py          # Enums and reference constants (single source of truth)
├── db_init.py            # Schema creation + PRAGMA configuration
├── seed_data.py          # 5 topics, 3+ supersession chains per topic
├── modules/
│   ├── A_crawler.py      # eMedNY document fetcher
│   ├── C_curation.py     # Streamlit data entry + Pydantic validation
│   ├── E_resolver.py     # Temporal rule resolution engine
│   └── F_query_ui.py     # Staff-facing Streamlit query interface
├── tests/
│   └── test_resolver.py  # 6-scenario minimum test pack
├── data/
│   └── raw/              # Downloaded PDFs
└── db/
    └── dpkb.db           # SQLite database
```

---

## Running the MVP

**Requirements:** Python 3.10+, pip

```bash
# Install dependencies
pip install streamlit pdfplumber pydantic sqlalchemy spacy requests beautifulsoup4 python-dateutil

# Initialize database and seed data
python db_init.py
python seed_data.py

# Launch query interface
streamlit run modules/F_query_ui.py
```

---

## Running Tests

```bash
python -m pytest tests/test_resolver.py -v
```

The test pack covers six scenarios: exact date match, supersession boundary, retroactive query, no-match (future date), no-match (before any rule), and multiple-candidate tie-break.

---

## Data Sources

All sources are publicly accessible - no login, CAPTCHA, or paywall.

| Source | URL |
|---|---|
| eMedNY Inpatient Manual | emedny.org/providermanuals/ |
| APG Outpatient Manual | emedny.org/providermanuals/ |
| General Institutional Billing Guidelines | emedny.org |
| Monthly Medicaid Update Bulletins | health.ny.gov/health_care/medicaid/program/update/ |

---

## Core Guarantees

- **Historical rules are never deleted.** A query for any date returns the rule active on that date.
- **Temporal resolution is deterministic.** The same inputs always produce the same output.
- **Every result is auditable.** Each query logs the candidates evaluated and the decision path.
- **3-year retention.** Rules are retained from at least March 2023 onward.

---


---

## Timeline

| Milestone | Date |
|---|---|
| Kickoff | March 10, 2026 |
| Schema finalized | March 21, 2026 |
| Midpoint MVP + Presentation | April 1, 2026 |
| Automation sprint (Module B) | April 1–15, 2026 |
| Final presentation | Late April 2026 |
| All deliverables due | May 10, 2026 |