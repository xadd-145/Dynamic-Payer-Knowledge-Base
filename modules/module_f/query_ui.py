# modules/module_f/query_ui.py
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Any
import sqlite3

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st

from db_init import DB_PATH, get_connection
from modules.module_e.resolver import resolve_all, resolve_all_anchored

# ---------------------------------------------------------------------------
# Brand CSS
# ---------------------------------------------------------------------------

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

/* ── Page shell ── */
.stApp { background: #f4f5f0; }
section[data-testid="stSidebar"] { background: #1e2a2b; }

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 0 !important; max-width: 1200px; }

/* ── Top banner ── */
.dpkb-banner {
    background: linear-gradient(135deg, #419599 0%, #2d6e72 100%);
    padding: 22px 32px 18px;
    border-radius: 0 0 16px 16px;
    margin-bottom: 28px;
    display: flex; align-items: center; justify-content: space-between;
}
.dpkb-banner h1 {
    color: #fff; font-size: 22px; font-weight: 700; margin: 0; letter-spacing: -0.3px;
}
.dpkb-banner p { color: rgba(255,255,255,0.78); font-size: 13px; margin: 4px 0 0; }
.dpkb-logo-pill {
    background: rgba(255,255,255,0.15); border-radius: 8px;
    padding: 6px 14px; color: #fff; font-size: 12px; font-weight: 600;
    letter-spacing: 0.5px;
}

/* ── Form card ── */
.form-card {
    background: #fff; border: 1px solid #e2e6e2;
    border-radius: 12px; padding: 22px 20px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}
.form-section-label {
    font-size: 10px; font-weight: 700; letter-spacing: 1.2px;
    text-transform: uppercase; color: #419599; margin-bottom: 6px;
}

/* ── Result header strip ── */
.results-header {
    background: #fff; border: 1px solid #e2e6e2; border-radius: 10px;
    padding: 14px 18px; margin-bottom: 16px;
    display: flex; align-items: center; gap: 10px;
}
.results-count {
    background: #96be59; color: #fff; border-radius: 6px;
    padding: 3px 10px; font-size: 13px; font-weight: 700;
}
.results-meta { font-size: 13px; color: #555; }

/* ── Rule card ── */
.rule-card {
    background: #fff; border: 1px solid #e2e6e2;
    border-left: 4px solid #96be59; border-radius: 10px;
    padding: 18px 20px; margin-bottom: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    transition: box-shadow 0.2s;
}
.rule-card:hover { box-shadow: 0 3px 10px rgba(65,149,153,0.12); }
.rule-card-top {
    display: flex; align-items: center; gap: 8px; margin-bottom: 10px; flex-wrap: wrap;
}
.badge-code {
    font-family: 'DM Mono', monospace;
    background: #419599; color: #fff;
    padding: 2px 9px; border-radius: 5px; font-size: 12px; font-weight: 500;
}
.badge-ver {
    background: #96be59; color: #fff;
    padding: 2px 8px; border-radius: 5px; font-size: 11px; font-weight: 600;
}
.badge-superseded {
    background: #e8e8e8; color: #888;
    padding: 2px 8px; border-radius: 5px; font-size: 11px;
}
.badge-active {
    background: #e6f4e6; color: #3a7a3a;
    padding: 2px 8px; border-radius: 5px; font-size: 11px; font-weight: 600;
}
.rule-title { font-size: 15px; font-weight: 600; color: #1a2a2b; }
.rule-text {
    font-size: 14px; color: #3a4a4a; line-height: 1.65;
    background: #f8faf8; border-radius: 8px; padding: 12px 14px; margin: 10px 0;
}
.field-label {
    font-size: 10px; font-weight: 700; letter-spacing: 1px;
    text-transform: uppercase; color: #419599; margin: 12px 0 4px;
}
.window-chip {
    display: inline-block; background: #eef6f6; border: 1px solid #c8e0e1;
    color: #2d6e72; border-radius: 6px; padding: 4px 12px;
    font-family: 'DM Mono', monospace; font-size: 12px;
}

/* ── Citation box ── */
.citation-box {
    background: #f0f7ef; border: 1px solid #c8dfc6; border-radius: 8px;
    padding: 10px 14px; font-size: 13px; color: #2d4a2d;
}
.citation-box a { color: #419599; text-decoration: none; font-weight: 500; }
.citation-box a:hover { text-decoration: underline; }

/* ── Version history ── */
.vh-row {
    border-left: 3px solid #d0d8d0; padding: 10px 14px; margin: 8px 0;
    border-radius: 0 6px 6px 0; background: #fafafa;
}
.vh-row.current { border-left-color: #96be59; background: #f5fbf0; }
.vh-row.superseded { border-left-color: #d0d0d0; opacity: 0.82; }
.vh-label { font-weight: 700; font-size: 13px; color: #1a2a2b; }
.vh-window { font-family: 'DM Mono', monospace; font-size: 11px; color: #555; }
.vh-change { font-size: 12px; color: #3a4a4a; margin-top: 4px; }
.vh-cite { font-size: 11px; color: #419599; margin-top: 4px; }

/* ── No match ── */
.no-match-box {
    background: #fff8f0; border: 1px solid #f0d8b0; border-radius: 10px;
    padding: 24px; text-align: center; margin-top: 8px;
}
.no-match-box h3 { color: #b06010; margin: 0 0 6px; font-size: 16px; }
.no-match-box p { color: #6a5040; margin: 0; font-size: 13px; }

/* ── Anchor result ── */
.anchor-result-card {
    background: #fff; border: 1px solid #e2e6e2; border-left: 4px solid #419599;
    border-radius: 10px; padding: 20px 22px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}

/* Streamlit overrides */
.stExpander { border: 1px solid #e2e6e2 !important; border-radius: 8px !important; }
div[data-testid="stExpander"] > div:first-child {
    background: #f8faf8 !important; border-radius: 8px !important;
}
.stButton > button {
    background: #419599 !important; color: #fff !important;
    border: none !important; border-radius: 8px !important;
    font-weight: 600 !important; font-size: 14px !important;
    padding: 10px 24px !important; width: 100% !important;
    transition: background 0.2s !important;
}
.stButton > button:hover { background: #2d7a7e !important; }
.stSelectbox > div > div { border-radius: 8px !important; border-color: #d0d8d0 !important; }
.stDateInput > div > div { border-radius: 8px !important; }
.stTextInput > div > div { border-radius: 8px !important; }
div[data-baseweb="radio"] { gap: 6px; }
</style>
"""

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def fetch_topics(conn):
    return conn.execute("""
        SELECT rule_topic_id, topic_code, topic_name
        FROM rule_topics
        WHERE is_active = 1 AND topic_code != 'TEST'
        ORDER BY display_order ASC, topic_code ASC
    """).fetchall()


def fetch_anchor_types(conn):
    return conn.execute("""
        SELECT ub04_anchor_type_id, anchor_type_code, anchor_type_name
        FROM ub04_anchor_types WHERE is_active = 1
        ORDER BY ub04_anchor_type_id ASC
    """).fetchall()


def fetch_rule_details(conn, rule_version_id):
    return conn.execute("""
        SELECT rv.rule_version_id, rv.atomic_rule_id, ar.rule_code, ar.rule_title,
               rv.version_label, rv.version_number,
               COALESCE(rv.display_rule_text, rv.normalized_rule_text) AS rule_text,
               rv.effective_start_date, rv.effective_end_date,
               rv.change_type, rv.change_summary, rv.is_superseded
        FROM rule_versions rv
        JOIN atomic_rules ar ON rv.atomic_rule_id = ar.atomic_rule_id
        WHERE rv.rule_version_id = ?
    """, (rule_version_id,)).fetchone()


def fetch_evidence(conn, rule_version_id):
    return conn.execute("""
        SELECT sd.title AS source_title, sd.source_url,
               rel.citation_text, rel.page_number_start,
               rel.page_number_end, rel.section_reference, rel.evidence_role
        FROM rule_evidence_links rel
        JOIN source_documents sd ON rel.source_document_id = sd.source_document_id
        WHERE rel.rule_version_id = ?
        ORDER BY CASE WHEN rel.evidence_role='primary' THEN 1
                      WHEN rel.evidence_role='supporting' THEN 2 ELSE 3 END
        LIMIT 1
    """, (rule_version_id,)).fetchone()


def fetch_anchors(conn, rule_version_id):
    return conn.execute("""
        SELECT uat.anchor_type_name, uac.anchor_code, uac.anchor_label, rva.is_primary
        FROM rule_version_anchors rva
        JOIN ub04_anchor_codes uac ON rva.ub04_anchor_code_id = uac.ub04_anchor_code_id
        JOIN ub04_anchor_types uat ON uac.ub04_anchor_type_id = uat.ub04_anchor_type_id
        WHERE rva.rule_version_id = ?
        ORDER BY rva.is_primary DESC
    """, (rule_version_id,)).fetchall()


def fetch_version_history(conn, atomic_rule_id):
    """Latest version first, with evidence for each version."""
    versions = conn.execute("""
        SELECT rv.rule_version_id, rv.version_label, rv.version_number,
               rv.effective_start_date, rv.effective_end_date,
               rv.change_type, rv.change_summary, rv.is_superseded
        FROM rule_versions rv
        WHERE rv.atomic_rule_id = ?
        ORDER BY rv.version_number DESC
    """, (atomic_rule_id,)).fetchall()

    result = []
    for v in versions:
        ev = fetch_evidence(conn, v["rule_version_id"])
        result.append({"version": v, "evidence": ev})
    return result


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

def _window_str(start, end):
    return f"{start} → {end if end else 'Active'}"


def _render_citation(evidence):
    if not evidence:
        st.markdown('<div class="citation-box">No source evidence linked.</div>',
                    unsafe_allow_html=True)
        return
    parts = []
    ps, pe = evidence["page_number_start"], evidence["page_number_end"]
    if ps and pe:
        parts.append(f"Pages {ps}–{pe}" if ps != pe else f"Page {ps}")
    elif ps:
        parts.append(f"Page {ps}")
    if evidence["section_reference"]:
        parts.append(f"§ {evidence['section_reference']}")
    cite_str = " · ".join(parts) if parts else "Citation not specified"
    url = evidence["source_url"] or "#"
    title = evidence["source_title"] or "Source document"
    note = evidence["citation_text"] or ""
    st.markdown(
        f'<div class="citation-box">'
        f'📄 <a href="{url}" target="_blank">{title}</a><br>'
        f'<span style="font-family:monospace;font-size:11px;">{cite_str}</span>'
        f'{"<br><em>" + note + "</em>" if note else ""}'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_version_history(conn, atomic_rule_id, current_version_id):
    history = fetch_version_history(conn, atomic_rule_id)
    if len(history) <= 1:
        st.caption("Only one version exists for this rule.")
        return
    for item in history:
        v = item["version"]
        ev = item["evidence"]
        is_current = v["rule_version_id"] == current_version_id
        is_superseded = v["is_superseded"] == 1
        row_class = "current" if is_current else ("superseded" if is_superseded else "")
        status_badge = (
            '<span class="badge-active">Current</span>' if is_current
            else '<span class="badge-superseded">Superseded</span>'
        )
        window = _window_str(v["effective_start_date"], v["effective_end_date"])
        change_html = f'<div class="vh-change">↳ {v["change_summary"]}</div>' if v["change_summary"] else ""

        # Evidence cite for this version
        if ev:
            ps, pe = ev["page_number_start"], ev["page_number_end"]
            page_str = ""
            if ps and pe:
                page_str = f"p.{ps}–{pe}" if ps != pe else f"p.{ps}"
            elif ps:
                page_str = f"p.{ps}"
            sec = f"§{ev['section_reference']}" if ev["section_reference"] else ""
            cite_parts = " · ".join(filter(None, [page_str, sec]))
            url = ev["source_url"] or "#"
            src_title = (ev["source_title"] or "Source")[:48]
            cite_html = f'<div class="vh-cite">📄 <a href="{url}" target="_blank">{src_title}</a>{" - " + cite_parts if cite_parts else ""}</div>'
        else:
            cite_html = ""

        st.markdown(
            f'<div class="vh-row {row_class}">'
            f'<span class="vh-label">{v["version_label"]}</span> {status_badge} '
            f'<span class="vh-window">{window}</span>'
            f'{change_html}{cite_html}'
            f'</div>',
            unsafe_allow_html=True,
        )


def render_rule_card(conn, rule, query_key, idx):
    """Render one rule card - closed by default, keyed to query_key for reset."""
    r_id    = rule["rule_version_id"]
    code    = rule["rule_code"]
    title   = rule["rule_title"]
    ver     = rule["version_label"]
    start   = rule.get("effective_start") or rule.get("effective_start_date", "")
    end     = rule.get("effective_end") or rule.get("effective_end_date")
    text    = rule.get("rule_text") or rule.get("normalized_rule_text", "")
    end_str = end if end else "Active"
    status  = "Active" if not end else "Superseded"

    status_badge = (
        '<span class="badge-active">Active</span>' if not end
        else '<span class="badge-superseded">Superseded</span>'
    )

    # Always collapsed on fresh query via key change
    with st.expander(
        f"{code} - {title}  |  {ver}  |  {start} → {end_str}",
        expanded=False,
    ):
        # Header badges
        st.markdown(
            f'<div class="rule-card-top">'
            f'<span class="badge-code">{code}</span>'
            f'<span class="badge-ver">{ver}</span>'
            f'{status_badge}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Rule text
        st.markdown('<div class="field-label">Billing Rule</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="rule-text">{text}</div>', unsafe_allow_html=True)

        # Effective window
        st.markdown('<div class="field-label">Effective Window</div>', unsafe_allow_html=True)
        st.markdown(
            f'<span class="window-chip">{start} → {end_str}</span>',
            unsafe_allow_html=True,
        )

        # UB-04 Anchors
        anchors = fetch_anchors(conn, r_id)
        if anchors:
            st.markdown('<div class="field-label">UB-04 Anchors</div>', unsafe_allow_html=True)
            for a in anchors:
                pri = " ★" if a["is_primary"] else ""
                st.markdown(
                    f'<span style="font-size:13px;color:#2d4a4a;">'
                    f'<code style="background:#eef6f6;padding:2px 6px;border-radius:4px;'
                    f'color:#419599;font-family:DM Mono,monospace;">{a["anchor_code"]}</code> '
                    f'{a["anchor_type_name"]} - {a["anchor_label"]}{pri}</span><br>',
                    unsafe_allow_html=True,
                )

        # Source citation
        st.markdown('<div class="field-label">Source Citation</div>', unsafe_allow_html=True)
        _render_citation(fetch_evidence(conn, r_id))

        # Version history
        details = fetch_rule_details(conn, r_id)
        if details:
            history = fetch_version_history(conn, details["atomic_rule_id"])
            n = len(history)
            st.markdown('<div class="field-label">Version History</div>', unsafe_allow_html=True)
            if n > 1:
                with st.expander(f"{n} versions - latest first", expanded=False):
                    _render_version_history(conn, details["atomic_rule_id"], r_id)
            else:
                st.caption("No prior versions.")


def render_all_results(conn, topic_result, query_key):
    status = topic_result["resolution_status"]

    if status == "no_match":
        st.markdown(
            '<div class="no-match-box">'
            '<h3>No rules found</h3>'
            '<p>No billing rules are on record for this topic on the selected date. '
            'The date may be outside the 3-year coverage window, or no rules have been '
            'curated for this topic yet.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    if status == "error":
        st.error(f"Resolver error: {topic_result.get('decision_trace', 'Unknown')}")
        return

    results = topic_result["results"]
    qdate   = topic_result["query_date"]
    n = len(results)

    st.markdown(
        f'<div class="results-header">'
        f'<span class="results-count">{n}</span>'
        f'<span class="results-meta">active billing rule{"s" if n != 1 else ""} '
        f'in effect on <strong>{qdate}</strong> - '
        f'click a rule to expand</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    for i, rule in enumerate(results):
        render_rule_card(conn, rule, query_key, i)


def render_anchor_result(conn, result, query_key):
    status = result["resolution_status"]

    if status == "no_match":
        st.markdown(
            '<div class="no-match-box">'
            '<h3>No rule found</h3>'
            '<p>No billing rule with this anchor code is on record for the selected '
            'topic and date. Check the anchor type and code, or try without the filter '
            'to see all rules for this topic.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    if status in ("ambiguous", "error"):
        st.error(f"Resolver status: {status}")
        with st.expander("Decision trace"):
            st.code(result.get("decision_trace", ""), language="json")
        return

    r_id    = result["winning_rule_version_id"]
    details = fetch_rule_details(conn, r_id)
    if not details:
        st.error("Rule version resolved but details not found.")
        return

    anchors  = fetch_anchors(conn, r_id)
    evidence = fetch_evidence(conn, r_id)

    st.markdown(
        f'<div class="rule-card-top" style="margin-bottom:10px;">'
        f'<span class="badge-code">{details["rule_code"]}</span>'
        f'<span class="badge-ver">{details["version_label"]}</span>'
        f'{"<span class=badge-active>Active</span>" if not details["effective_end_date"] else "<span class=badge-superseded>Superseded</span>"}'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown(f'### {details["rule_title"]}')

    st.markdown('<div class="field-label">Billing Rule</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="rule-text">{details["rule_text"]}</div>', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="field-label">Effective Window</div>', unsafe_allow_html=True)
        st.markdown(
            f'<span class="window-chip">'
            f'{_window_str(details["effective_start_date"], details["effective_end_date"])}'
            f'</span>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown('<div class="field-label">UB-04 Anchors</div>', unsafe_allow_html=True)
        for a in anchors:
            pri = " ★" if a["is_primary"] else ""
            st.markdown(
                f'<code style="background:#eef6f6;padding:2px 6px;border-radius:4px;'
                f'color:#419599;font-size:12px;">{a["anchor_code"]}</code> '
                f'<span style="font-size:12px;">{a["anchor_type_name"]}{pri}</span><br>',
                unsafe_allow_html=True,
            )

    st.markdown('<div class="field-label">Source Citation</div>', unsafe_allow_html=True)
    _render_citation(evidence)

    st.markdown('<div class="field-label">Version History</div>', unsafe_allow_html=True)
    history = fetch_version_history(conn, details["atomic_rule_id"])
    n = len(history)
    if n > 1:
        with st.expander(f"{n} versions - latest first", expanded=False):
            _render_version_history(conn, details["atomic_rule_id"], r_id)
    else:
        st.caption("No prior versions.")

    with st.expander("Decision trace", expanded=False):
        st.code(result.get("decision_trace", ""), language="json")


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="DPKB - NY Medicaid Billing Rules",
        page_icon="📋",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(CSS, unsafe_allow_html=True)

    st.markdown(
        '<div class="dpkb-banner">'
        '<div>'
        '<h1>Dynamic Payer Knowledge Base</h1>'
        '<p>NY Medicaid facility billing rules · UB-04 · Deterministic date-based retrieval</p>'
        '</div>'
        '<div class="dpkb-logo-pill">BIG × Salud Revenue Partners</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    # ── Session state ──────────────────────────────────────────────────────
    # sq_* = "saved query" - set once when Retrieve Rules is clicked.
    # The anchor widgets live outside the saved query so changing them
    # immediately re-renders results without another button click.
    for key, default in [
        ("sq_topic_id", None),
        ("sq_date_str", None),
        ("sq_date_type", None),
        ("sq_count",    0),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    try:
        conn = get_connection(DB_PATH)
    except Exception as exc:
        st.error(f"Database connection failed: {exc}")
        return

    try:
        topics       = fetch_topics(conn)
        anchor_types = fetch_anchor_types(conn)

        if not topics:
            st.warning("No topics found. Run `python seed_sample_data.py --reset` first.")
            return

        topic_options = {
            f"{r['topic_code']} - {r['topic_name']}": r["rule_topic_id"] for r in topics
        }
        anchor_type_options = {
            "- None (show all rules for topic) -": None,
            **{
                f"{r['anchor_type_name']} ({r['anchor_type_code']})": r["ub04_anchor_type_id"]
                for r in anchor_types
            },
        }

        col_form, col_results = st.columns([1, 2], gap="large")

        with col_form:
            # ── ONE unified card - query + anchor + button ─────────────────
            # Using st.container(border=True) so the entire left panel has
            # a single consistent border. No st.form() - that created an
            # inner box that clashed visually with the anchor section below.
            with st.container(border=True):

                # ── QUERY section ──────────────────────────────────────────
                st.markdown(
                    '<div class="form-section-label">Query</div>',
                    unsafe_allow_html=True,
                )

                topic_label = st.selectbox(
                    "Billing Topic",
                    options=list(topic_options.keys()),
                    key="dpkb_topic",
                )
                topic_id = topic_options[topic_label]

                query_date = st.date_input(
                    "Date of Service / Discharge",
                    value=date(2025, 1, 1),
                    key="dpkb_date",
                )
                date_str = query_date.strftime("%Y-%m-%d")

                date_type = st.radio(
                    "Date Type",
                    options=["date_of_service", "date_of_discharge"],
                    format_func=lambda x: (
                        "Date of Service" if x == "date_of_service" else "Date of Discharge"
                    ),
                    horizontal=False,
                    key="dpkb_date_type",
                )

                st.divider()

                # ── ANCHOR FILTER section ──────────────────────────────────
                # These widgets are NOT inside a form so changing them
                # triggers an immediate Streamlit rerun → results update
                # automatically without clicking Retrieve Rules again.
                st.markdown(
                    '<div class="form-section-label">Anchor Filter (optional)</div>',
                    unsafe_allow_html=True,
                )
                st.caption("Select an anchor type - results update instantly.")

                anchor_type_label = st.selectbox(
                    "UB-04 Anchor Type",
                    options=list(anchor_type_options.keys()),
                    key="dpkb_anchor_type",
                )
                current_anchor_type_id = anchor_type_options[anchor_type_label]

                selected_anchor_code = None   # resolved below
                if current_anchor_type_id is not None:
                    # ── Topic-filtered code list ───────────────────────────
                    # Only show codes that are actually linked to published
                    # rule versions for the SELECTED TOPIC. This prevents
                    # the dropdown from showing e.g. Inpatient codes when
                    # the user has selected Emergency Room.
                    code_rows = conn.execute(
                        """
                        SELECT DISTINCT uac.anchor_code, uac.anchor_label
                        FROM ub04_anchor_codes uac
                        JOIN rule_version_anchors rva
                          ON uac.ub04_anchor_code_id = rva.ub04_anchor_code_id
                        JOIN rule_versions rv
                          ON rva.rule_version_id = rv.rule_version_id
                        JOIN atomic_rules ar
                          ON rv.atomic_rule_id = ar.atomic_rule_id
                        WHERE uac.ub04_anchor_type_id = ?
                          AND ar.rule_topic_id = ?
                          AND rv.is_published = 1
                          AND rv.lifecycle_status = 'published'
                        ORDER BY uac.anchor_code ASC
                        """,
                        (current_anchor_type_id, topic_id),
                    ).fetchall()

                    if code_rows:
                        code_options = {
                            "- All (any code of this type) -": None,
                            **{
                                f"{r['anchor_code']} - {r['anchor_label']}": r["anchor_code"]
                                for r in code_rows
                            },
                        }
                        code_label = st.selectbox(
                            "Anchor Code",
                            options=list(code_options.keys()),
                            key="dpkb_anchor_code",
                        )
                        selected_anchor_code = code_options[code_label]
                    else:
                        anchor_type_name = anchor_type_label.split(" (")[0]
                        st.info(
                            f"No {anchor_type_name} codes are linked to any "
                            f"{topic_label.split(' - ')[1]} rules."
                        )
                else:
                    st.caption("No anchor filter - all rules for the topic will be shown.")

                st.divider()

                # ── RETRIEVE RULES button (bottom of card) ─────────────────
                if st.button("🔍  Retrieve Rules", use_container_width=True, type="primary"):
                    st.session_state["sq_topic_id"]  = topic_id
                    st.session_state["sq_date_str"]  = date_str
                    st.session_state["sq_date_type"] = date_type
                    st.session_state["sq_count"]    += 1

        # ── Results column ─────────────────────────────────────────────────
        with col_results:
            if st.session_state["sq_topic_id"] is not None:
                # Base query values come from session state (set by button).
                # Anchor values come from current widget state so they apply
                # instantly whenever the dropdowns above are changed.
                bq_topic = st.session_state["sq_topic_id"]
                bq_date  = st.session_state["sq_date_str"]
                bq_dtype = st.session_state["sq_date_type"]
                qkey     = st.session_state["sq_count"]

                if current_anchor_type_id is not None:
                    # Anchor filter is active - show context badge
                    anchor_type_name = anchor_type_label.split(" (")[0]
                    code_display = (
                        selected_anchor_code if selected_anchor_code
                        else "all codes"
                    )
                    st.markdown(
                        f'<div style="font-size:12px;color:#419599;font-weight:600;'
                        f'letter-spacing:0.5px;margin-bottom:8px;">'
                        f'🔎 Filtered by {anchor_type_name}: '
                        f'<code style="background:#eef6f6;padding:1px 6px;'
                        f'border-radius:4px;">{code_display}</code>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                    result = resolve_all_anchored(
                        conn=conn,
                        rule_topic_id=bq_topic,
                        query_date=bq_date,
                        query_date_type=bq_dtype,
                        anchor_type_id=current_anchor_type_id,
                        anchor_value=selected_anchor_code,  # None = type-only
                    )
                else:
                    result = resolve_all(
                        conn=conn,
                        rule_topic_id=bq_topic,
                        query_date=bq_date,
                        query_date_type=bq_dtype,
                    )

                render_all_results(conn, result, qkey)

            else:
                st.markdown(
                    '<div style="text-align:center;padding:60px 20px;color:#888;">'
                    '<div style="font-size:40px;margin-bottom:12px;">📋</div>'
                    '<div style="font-size:15px;font-weight:600;color:#419599;">'
                    'Select a topic and date, then click Retrieve Rules</div>'
                    '<div style="font-size:13px;margin-top:6px;">'
                    'Use the anchor filter to narrow by UB-04 code</div>'
                    '</div>',
                    unsafe_allow_html=True,
                )

    finally:
        conn.close()


if __name__ == "__main__":
    main()