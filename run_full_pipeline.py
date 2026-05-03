# run_full_pipeline.py
"""
DPKB Full Pipeline Runner

Runs Module B over all Medicaid Update PDFs in data/raw.

Pipeline:
  PDF
  → extractor.py
  → date_classifier.py
  → confidence_scorer.py
  → fragment_writer.py
  → policy_fragments table

Step 2-compatible:
  - uses inherited_date from extractor.py
  - passes date_source into confidence_scorer.py
  - writes final confidence_score into DB
  - reports direct vs inherited vs no-date counts
  - reports semantic_used vs semantic_skipped
  - prints DB routing summary after run

Important:
  This script does NOT approve or publish rules.
  It only writes extracted/scored fragments into policy_fragments.

Expected routing after Step 1:
  TIER_1 → pending
  TIER_2 → pending
  TIER_3 → escalated
"""

import sys
import sqlite3
from pathlib import Path
from collections import Counter


# ---------------------------------------------------------------------------
# PROJECT ROOT
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# IMPORT MODULE B PIPELINE
# ---------------------------------------------------------------------------

from modules.module_b.extractor import extract_fragments
from modules.module_b.date_classifier import classify_dates
from modules.module_b.confidence_scorer import score_fragment
from modules.module_b.fragment_writer import write_fragment


DATA_DIR = PROJECT_ROOT / "data" / "raw"
DB_PATH = PROJECT_ROOT / "db" / "dpkb.db"


# ---------------------------------------------------------------------------
# DB SUMMARY
# ---------------------------------------------------------------------------

def _print_db_summary() -> None:
    conn = sqlite3.connect(str(DB_PATH))

    total_fragments = conn.execute(
        "SELECT COUNT(*) FROM policy_fragments"
    ).fetchone()[0]

    total_sources = conn.execute(
        "SELECT COUNT(*) FROM source_documents"
    ).fetchone()[0]

    tier_breakdown = conn.execute("""
        SELECT confidence_tier, review_status, COUNT(*) AS n
        FROM policy_fragments
        GROUP BY confidence_tier, review_status
        ORDER BY confidence_tier, review_status
    """).fetchall()

    score_summary = conn.execute("""
        SELECT
            COUNT(*) AS n,
            ROUND(AVG(confidence_score), 2) AS avg_score,
            MIN(confidence_score) AS min_score,
            MAX(confidence_score) AS max_score
        FROM policy_fragments
    """).fetchone()

    source_summary = conn.execute("""
        SELECT sd.document_title, COUNT(pf.policy_fragment_id) AS n
        FROM source_documents sd
        LEFT JOIN policy_fragments pf
          ON pf.source_document_id = sd.source_document_id
        GROUP BY sd.source_document_id, sd.document_title
        HAVING n > 0
        ORDER BY n DESC, sd.document_title
        LIMIT 15
    """).fetchall()

    conn.close()

    print("\n" + "=" * 70)
    print("DATABASE SUMMARY AFTER RUN")
    print("=" * 70)

    print(f"  Total fragments in DB : {total_fragments}")
    print(f"  Total source docs     : {total_sources}")

    print(
        f"  Score summary         : n={score_summary[0]}, "
        f"avg={score_summary[1]}, min={score_summary[2]}, max={score_summary[3]}"
    )

    print("\n  DB TIER / REVIEW STATUS BREAKDOWN:")
    for tier, status, n in tier_breakdown:
        print(f"    {tier:7s} | {status:10s} | {n}")

    print("\n  Top source documents by fragment count:")
    for title, n in source_summary:
        print(f"    {n:4d} | {title}")


# ---------------------------------------------------------------------------
# OPTIONAL DB PRECHECK
# ---------------------------------------------------------------------------

def _db_exists_or_fail() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}\n"
            "Run:\n"
            "  python db_init.py\n"
            "  python seed_sample_data.py --reset"
        )


# ---------------------------------------------------------------------------
# FULL PIPELINE
# ---------------------------------------------------------------------------

def main() -> None:
    _db_exists_or_fail()

    pdfs = sorted(DATA_DIR.glob("*_pr.pdf"))

    if not pdfs:
        print(f"No *_pr.pdf files found in: {DATA_DIR}")
        sys.exit(1)

    print("=" * 70)
    print("DPKB FULL PIPELINE — MODULE B")
    print("=" * 70)
    print(f"Project root   : {PROJECT_ROOT}")
    print(f"Data directory : {DATA_DIR}")
    print(f"PDFs found     : {len(pdfs)}")
    print(f"Database       : {DB_PATH}")
    print("=" * 70)

    totals = {
        "pdfs_processed": 0,
        "pdfs_skipped": 0,
        "fragments_seen": 0,
        "fragments_written": 0,
        "errors": 0,
        "TIER_1": 0,
        "TIER_2": 0,
        "TIER_3": 0,
    }

    date_source_counts = Counter()
    date_role_counts = Counter()
    semantic_counts = Counter()
    extractor_stats_total = Counter()
    file_summaries = []

    for pdf_num, pdf in enumerate(pdfs, start=1):
        print("\n" + "=" * 70)
        print(f"[{pdf_num}/{len(pdfs)}] {pdf.name}")
        print("=" * 70)

        file_counts = Counter()
        file_date_sources = Counter()
        file_semantic = Counter()

        try:
            result = extract_fragments(str(pdf))
            totals["pdfs_processed"] += 1

            fragments = result.get("fragments", [])
            totals["fragments_seen"] += len(fragments)

            # Support both extractor stats styles.
            if "stats" in result and isinstance(result["stats"], dict):
                for key, value in result["stats"].items():
                    if isinstance(value, int):
                        extractor_stats_total[key] += value

            if "hard_rejected" in result:
                extractor_stats_total["hard_rejected"] += result.get("hard_rejected", 0)

            if "total_fragments" in result:
                extractor_stats_total["total_fragments_reported"] += result.get("total_fragments", 0)

            print(f"  Fragments to score: {len(fragments)}")

            for frag_num, frag in enumerate(fragments, start=1):
                try:
                    fragment_text = frag["fragment_text"]

                    # ------------------------------------------------------
                    # Step 2 support:
                    # Use inherited section date if extractor provided it.
                    # ------------------------------------------------------
                    date_result = classify_dates(
                        fragment_text,
                        frag.get("inherited_date"),
                    )

                    date_source = date_result.get("date_source", "direct")
                    date_role = date_result.get("date_role", "none")

                    date_source_counts[date_source] += 1
                    date_role_counts[date_role] += 1
                    file_date_sources[date_source] += 1

                    # ------------------------------------------------------
                    # Step 2 support:
                    # Pass date_source into scorer so inherited date gets
                    # partial date credit.
                    # ------------------------------------------------------
                    score_result = score_fragment(
                        fragment_text,
                        date_role,
                        date_source,
                    )

                    semantic_key = (
                        "semantic_skipped"
                        if score_result.get("semantic_skipped")
                        else "semantic_used"
                    )

                    semantic_counts[semantic_key] += 1
                    file_semantic[semantic_key] += 1

                    tier = score_result["tier"]
                    totals[tier] += 1
                    file_counts[tier] += 1

                    # ------------------------------------------------------
                    # Step 1 support:
                    # Write final combined confidence_score to DB.
                    # fragment_writer maps:
                    #   TIER_1/TIER_2 → pending
                    #   TIER_3        → escalated
                    # ------------------------------------------------------
                    write_fragment(
                        fragment_text=fragment_text,
                        page_number=frag["page_number"],
                        source_filename=result["source_filename"],
                        sha256=result["sha256_hash"],
                        extracted_date=date_result["extracted_date"],
                        date_role=date_role,
                        structural_score=score_result["structural_score"],
                        semantic_score=score_result["semantic_score"],
                        semantic_skipped=score_result["semantic_skipped"],
                        tier=tier,
                        confidence_score=score_result["final_score"],
                    )

                    totals["fragments_written"] += 1

                except Exception as e:
                    totals["errors"] += 1
                    print(
                        f"  Fragment error: file={pdf.name}, "
                        f"fragment={frag_num}, "
                        f"{type(e).__name__}: {e}"
                    )

            file_summaries.append({
                "filename": pdf.name,
                "fragments": len(fragments),
                "tier1": file_counts["TIER_1"],
                "tier2": file_counts["TIER_2"],
                "tier3": file_counts["TIER_3"],
                "direct": file_date_sources["direct"],
                "inherited": file_date_sources["inherited_section"],
                "none": file_date_sources["none"],
                "semantic_used": file_semantic["semantic_used"],
                "semantic_skipped": file_semantic["semantic_skipped"],
            })

            print(
                f"  File summary: fragments={len(fragments)} | "
                f"TIER_1={file_counts['TIER_1']} | "
                f"TIER_2={file_counts['TIER_2']} | "
                f"TIER_3={file_counts['TIER_3']} | "
                f"direct={file_date_sources['direct']} | "
                f"inherited={file_date_sources['inherited_section']} | "
                f"semantic_used={file_semantic['semantic_used']} | "
                f"semantic_skipped={file_semantic['semantic_skipped']}"
            )

        except Exception as e:
            totals["pdfs_skipped"] += 1
            print(f"  PDF FAILED: {type(e).__name__}: {e}")

    # -----------------------------------------------------------------------
    # RUN SUMMARY
    # -----------------------------------------------------------------------

    print("\n" + "=" * 70)
    print("FULL PIPELINE COMPLETE")
    print("=" * 70)

    print("\nRUN SUMMARY")
    print("-" * 70)
    print(f"  PDFs processed       : {totals['pdfs_processed']}")
    print(f"  PDFs skipped/failed  : {totals['pdfs_skipped']}")
    print(f"  Fragments seen       : {totals['fragments_seen']}")
    print(f"  Fragments written    : {totals['fragments_written']}")
    print(f"  Fragment errors      : {totals['errors']}")

    print("\nTIER COUNTS FROM THIS RUN")
    print("-" * 70)
    print(f"  TIER_1               : {totals['TIER_1']}")
    print(f"  TIER_2               : {totals['TIER_2']}")
    print(f"  TIER_3               : {totals['TIER_3']}")

    print("\nDATE SOURCE COUNTS FROM THIS RUN")
    print("-" * 70)
    if date_source_counts:
        for key in sorted(date_source_counts):
            print(f"  {key:20s}: {date_source_counts[key]}")
    else:
        print("  No date sources counted.")

    print("\nDATE ROLE COUNTS FROM THIS RUN")
    print("-" * 70)
    if date_role_counts:
        for key in sorted(date_role_counts):
            print(f"  {key:20s}: {date_role_counts[key]}")
    else:
        print("  No date roles counted.")

    print("\nSEMANTIC COUNTS FROM THIS RUN")
    print("-" * 70)
    if semantic_counts:
        for key in sorted(semantic_counts):
            print(f"  {key:20s}: {semantic_counts[key]}")
    else:
        print("  No semantic counts.")

    if extractor_stats_total:
        print("\nEXTRACTOR STATS TOTAL")
        print("-" * 70)
        for key in sorted(extractor_stats_total):
            print(f"  {key:28s}: {extractor_stats_total[key]}")

    print("\nTOP FILE SUMMARIES")
    print("-" * 70)
    for row in sorted(file_summaries, key=lambda r: r["fragments"], reverse=True)[:20]:
        print(
            f"  {row['filename']:30s} | "
            f"frags={row['fragments']:3d} | "
            f"T1={row['tier1']:2d} T2={row['tier2']:2d} T3={row['tier3']:2d} | "
            f"direct={row['direct']:2d} inherited={row['inherited']:2d} | "
            f"sem_used={row['semantic_used']:2d} sem_skip={row['semantic_skipped']:2d}"
        )

    _print_db_summary()

    print("\nNEXT CHECKS")
    print("-" * 70)
    print("  1. Confirm TIER_1/TIER_2 are pending.")
    print("  2. Confirm TIER_3 is escalated.")
    print("  3. Inspect sample fragments before sending to Arjav/Admin.")
    print("  4. Do not auto-approve anything.")
    print("  5. Do not modify resolver.py.")


if __name__ == "__main__":
    main()