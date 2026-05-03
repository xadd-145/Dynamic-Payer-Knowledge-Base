# scripts/verify_step2.py
"""
Step 2E — Verification on 3 test bulletins only.
Run: python scripts/verify_step2.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from modules.module_b.extractor import extract_fragments
from modules.module_b.date_classifier import classify_dates

TEST_BULLETINS = [
    'mu_no5_may22_speced_pr.pdf',   # NYIA — expect 0 valid fragments
    'mu_no6_may22_pr.pdf',          # Mixed
    'mu_no05_may25_pr.pdf',         # Strong UB-04 — RPM + Newborn
]

data_dir = PROJECT_ROOT / 'data' / 'raw'
results = {}

for filename in TEST_BULLETINS:
    pdf_path = data_dir / filename
    if not pdf_path.exists():
        print(f"SKIP (not found): {filename}")
        continue
    results[filename] = extract_fragments(str(pdf_path))

print("\n" + "="*60)
print("STEP 2E VERIFICATION RESULTS")
print("="*60)

passed = 0
failed = 0

def check(label, condition):
    global passed, failed
    status = "PASS" if condition else "FAIL"
    if condition:
        passed += 1
    else:
        failed += 1
    print(f"  [{status}] {label}")

# --- NYIA bulletin ---
if 'mu_no5_may22_speced_pr.pdf' in results:
    r = results['mu_no5_may22_speced_pr.pdf']
    check("NYIA bulletin: 0 valid fragments (all out-of-scope)", r['total_fragments'] == 0)
    check("NYIA bulletin: hard_rejected > 0", r['hard_rejected'] > 0)

# --- May 2025 bulletin ---
if 'mu_no05_may25_pr.pdf' in results:
    r = results['mu_no05_may25_pr.pdf']
    check("May 2025: has at least 1 fragment", r['total_fragments'] > 0)

    rpm_found = any(
        'rpm' in f['fragment_text'].lower() or '99457' in f['fragment_text']
        for f in r['fragments']
    )
    check("May 2025: RPM / CPT 99457 fragment present", rpm_found)

    newborn_found = any(
        'newborn' in f['fragment_text'].lower() or
        'value code 54' in f['fragment_text'].lower()
        for f in r['fragments']
    )
    check("May 2025: Newborn / Value Code 54 fragment present", newborn_found)

    all_have_key = all('inherited_date' in f for f in r['fragments'])
    check("May 2025: all fragments have inherited_date key", all_have_key)

    date_source_ok = True
    direct_count = inherited_count = 0
    for frag in r['fragments']:
        dr = classify_dates(frag['fragment_text'], frag.get('inherited_date'))
        if 'date_source' not in dr:
            date_source_ok = False
            break
        if dr['date_source'] == 'direct':
            direct_count += 1
        elif dr['date_source'] == 'inherited_section':
            inherited_count += 1

    check("classify_dates returns date_source on all fragments", date_source_ok)

    no_date_count = r['total_fragments'] - direct_count - inherited_count
    print(f"\n  Date source breakdown (May 2025):")
    print(f"    direct            : {direct_count}")
    print(f"    inherited_section : {inherited_count}")
    print(f"    no date           : {no_date_count}")

print(f"\n{'='*60}")
print(f"  {passed} passed  |  {failed} failed")
print(f"{'='*60}\n")

if failed > 0:
    sys.exit(1)