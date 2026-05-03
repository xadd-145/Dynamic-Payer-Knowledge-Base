from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))

from modules.module_b.extractor import extract_fragments
from modules.module_b.date_classifier import classify_dates

data_dir = Path('data/raw')
pdfs = list(data_dir.glob('*.pdf'))

for pdf_path in pdfs:
    print(f"\n{'='*60}")
    print(f"PDF: {pdf_path.name}")
    result = extract_fragments(str(pdf_path))
    print(f"Fragments extracted: {result['total_fragments']}")

    effective_count = 0
    no_date_count = 0
    ambiguous_count = 0
    other_count = 0

    for frag in result['fragments']:
        dr = classify_dates(frag['fragment_text'])
        if dr['date_role'] == 'effective_date':
            effective_count += 1
        elif dr['date_role'] == 'none':
            no_date_count += 1
        elif dr['date_role'] == 'ambiguous':
            ambiguous_count += 1
        else:
            other_count += 1

    print(f"  Effective date   : {effective_count}")
    print(f"  No date found    : {no_date_count}")
    print(f"  Ambiguous        : {ambiguous_count}")
    print(f"  Other role       : {other_count}")

    print(f"\n  Sample effective date fragments:")
    shown = 0
    for frag in result['fragments']:
        if shown >= 3:
            break
        dr = classify_dates(frag['fragment_text'])
        if dr['date_role'] == 'effective_date':
            print(f"    Page {frag['page_number']} — {dr['raw_date_string']} → {dr['extracted_date']}")
            print(f"    {frag['fragment_text'][:150]}...")
            shown += 1