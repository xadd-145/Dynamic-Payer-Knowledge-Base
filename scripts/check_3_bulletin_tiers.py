import sys
sys.path.insert(0, ".")

from modules.module_b.extractor import extract_fragments
from modules.module_b.date_classifier import classify_dates
from modules.module_b.confidence_scorer import score_fragment

bulletins = [
    "data/raw/mu_no5_may22_speced_pr.pdf",
    "data/raw/mu_no6_may22_pr.pdf",
    "data/raw/mu_no05_may25_pr.pdf",
]

total = tier1 = tier2 = tier3 = direct = inherited = no_date = 0
examples = {"TIER_1": [], "TIER_2": [], "TIER_3": []}

for path in bulletins:
    result = extract_fragments(path)

    for frag in result["fragments"]:
        dr = classify_dates(frag["fragment_text"], frag.get("inherited_date"))

        sr = score_fragment(
            frag["fragment_text"],
            dr["date_role"],
            dr.get("date_source", "direct"),
        )

        total += 1

        tier = sr["tier"]
        if tier == "TIER_1":
            tier1 += 1
        elif tier == "TIER_2":
            tier2 += 1
        else:
            tier3 += 1

        src = dr.get("date_source", "none")
        if src == "direct":
            direct += 1
        elif src == "inherited_section":
            inherited += 1
        else:
            no_date += 1

        if len(examples[tier]) < 3:
            examples[tier].append({
                "file": result["source_filename"],
                "page": frag["page_number"],
                "score": sr["final_score"],
                "structural": sr["structural_score"],
                "semantic": sr["semantic_score"],
                "semantic_skipped": sr["semantic_skipped"],
                "date_source": src,
                "date": dr["extracted_date"],
                "text": frag["fragment_text"][:350].replace("\n", " "),
            })

print()
print("=== TIER DISTRIBUTION — 3 TEST BULLETINS ===")
print(f"  Total fragments      : {total}")
print(f"  TIER_1               : {tier1}")
print(f"  TIER_2               : {tier2}")
print(f"  TIER_3               : {tier3}")

print()
print("=== DATE SOURCE BREAKDOWN ===")
print(f"  direct               : {direct}")
print(f"  inherited_section    : {inherited}")
print(f"  no date / other      : {no_date}")

print()
print("=== SAMPLE FRAGMENTS BY TIER ===")
for tier_name, rows in examples.items():
    print()
    print(f"--- {tier_name} ---")
    if not rows:
        print("  No examples")
        continue

    for row in rows:
        print(f"  File: {row['file']} | Page: {row['page']}")
        print(f"  Score: {row['score']} | Structural: {row['structural']} | Semantic: {row['semantic']} | Skipped: {row['semantic_skipped']}")
        print(f"  Date: {row['date']} | Date source: {row['date_source']}")
        print(f"  Text: {row['text']}")
        print()