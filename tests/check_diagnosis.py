import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from modules.module_b.extractor import extract_fragments
from modules.module_b.date_classifier import classify_dates
from modules.module_b.confidence_scorer import score_fragment

data_dir = Path('data/raw')

# Test across 6 bulletins from different years
test_files = [
    'mu_no3_mar22_pr.pdf',
    'mu_no6_mar23_pr.pdf',
    'mu_no3_mar24_pr.pdf',
    'mu_no8_aug24_pr.pdf',
    'mu_no01_jan25_pr.pdf',
    'mu_no01_jan26_pr.pdf',
]

all_scores = []
all_fragments = []

for filename in test_files:
    pdf = data_dir / filename
    if not pdf.exists():
        print(f"MISSING: {filename}")
        continue
    print(f"Processing: {filename}")
    result = extract_fragments(str(pdf))
    for frag in result['fragments']:
        dr = classify_dates(frag['fragment_text'])
        sr = score_fragment(frag['fragment_text'], dr['date_role'])
        all_scores.append(sr['final_score'])
        all_fragments.append({
            'file': filename,
            'score': sr['final_score'],
            'structural': sr['structural_score'],
            'semantic': sr['semantic_score'],
            'date_role': dr['date_role'],
            'tier': sr['tier'],
            'text': frag['fragment_text'][:150]
        })

print(f"\n{'='*60}")
print(f"SCORE DISTRIBUTION ACROSS {len(all_fragments)} FRAGMENTS")
print(f"{'='*60}")

ranges = [(80,100), (60,79), (40,59), (20,39), (0,19)]
for lo, hi in ranges:
    count = sum(1 for s in all_scores if lo <= s <= hi)
    pct = count/len(all_scores)*100
    print(f"  {lo:3d}-{hi:3d}: {count:4d} fragments ({pct:.1f}%)")

print(f"\nMIN: {min(all_scores)}  MAX: {max(all_scores)}  AVG: {sum(all_scores)/len(all_scores):.1f}")

print(f"\nSAMPLE FRAGMENTS BY SCORE RANGE:")
shown = {}
for frag in sorted(all_fragments, key=lambda x: x['score'], reverse=True):
    bucket = (frag['score'] // 20) * 20
    if bucket not in shown:
        shown[bucket] = 0
    if shown[bucket] < 2:
        print(f"\n  Score {frag['score']} | Semantic: {frag['semantic']} | Date: {frag['date_role']}")
        print(f"  File: {frag['file']}")
        print(f"  Text: {frag['text']}...")
        shown[bucket] += 1