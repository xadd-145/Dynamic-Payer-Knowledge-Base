# modules/module_b/date_classifier.py
"""
Module B — Step 2 Updated: Date Classifier
Deterministic — regex + python-dateutil only. No LLM.

Step 2C change:
  classify_dates() now accepts inherited_date from section context.
  Returns date_source: 'direct' | 'inherited_section' | 'none' | 'ambiguous'
"""

import re

try:
    from dateutil import parser as dateutil_parser
    from dateutil.parser import ParserError
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    print("WARNING: python-dateutil not installed.")

EFFECTIVE_DATE_KEYWORDS = [
    'effective', 'on and after', 'on or after', 'for dates of service',
    'for date of service', 'for discharges', 'for discharge', 'beginning',
    'effective date', 'effective for', 'takes effect', 'for services on',
    'for services rendered', 'applicable', 'applies to',
    'occurring on and after', 'for claims with discharge dates',
    'for services provided', 'is effective for', 'are effective for',
    'new table is effective', 'rate is effective', 'payment is effective',
    'policy is effective', 'for inpatient', 'for outpatient',
    'for discharges beginning', 'for discharges on', 'for dates of discharge',
    'beginning january', 'beginning december', 'beginning april',
    'beginning july', 'beginning october',
]

PUBLICATION_DATE_KEYWORDS = [
    'published', 'issued', 'dated', 'release', 'updated', 'revised',
    'bulletin date', 'document date', 'as of', 'print date', 'version',
]

DEADLINE_DATE_KEYWORDS = [
    'no later than', 'deadline', 'must be submitted', 'must submit',
    'prior to', 'not later than', 'submit by', 'due date',
]

CONTEXT_WINDOW = 200

DATE_PATTERNS = [
    r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b',
    r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4}\b',
    r'\b\d{1,2}/\d{1,2}/\d{4}\b',
    r'\b\d{4}-\d{2}-\d{2}\b',
    r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b',
]

COMBINED_PATTERN = '|'.join(f'({p})' for p in DATE_PATTERNS)


def _find_dates_in_text(text: str) -> list:
    found = []
    for match in re.finditer(COMBINED_PATTERN, text, re.IGNORECASE):
        found.append({
            'raw_date_string': match.group().strip(),
            'start_pos': match.start(),
            'end_pos': match.end(),
        })
    return found


def _parse_date(raw_date_string: str) -> str | None:
    if not DATEUTIL_AVAILABLE:
        return None
    try:
        parsed = dateutil_parser.parse(raw_date_string, dayfirst=False)
        if not (2000 <= parsed.year <= 2030):
            return None
        return parsed.strftime('%Y-%m-%d')
    except (ParserError, ValueError, OverflowError):
        return None


def _classify_date_role(text: str, date_start_pos: int) -> str:
    context_start = max(0, date_start_pos - CONTEXT_WINDOW)
    context = text[context_start:date_start_pos].lower()

    for keyword in EFFECTIVE_DATE_KEYWORDS:
        if keyword in context:
            return 'effective_date'
    for keyword in DEADLINE_DATE_KEYWORDS:
        if keyword in context:
            return 'deadline_date'
    for keyword in PUBLICATION_DATE_KEYWORDS:
        if keyword in context:
            return 'publication_date'
    return 'unknown'


def classify_dates(fragment_text: str, inherited_date: str | None = None) -> dict:
    """
    Find dates in fragment and return best effective date.

    Args:
        fragment_text:  raw text of one policy fragment
        inherited_date: YYYY-MM-DD from section context (Step 2B),
                        or None if section had no effective date.

    Returns:
        extracted_date  : str | None  (YYYY-MM-DD)
        date_role       : str         (effective_date | publication_date |
                                       deadline_date | ambiguous | none)
        raw_date_string : str | None
        date_source     : str         (direct | inherited_section | none | ambiguous)
        all_dates_found : list
    """
    if not fragment_text or not fragment_text.strip():
        return {
            'extracted_date': None,
            'date_role': 'none',
            'raw_date_string': None,
            'date_source': 'none',
            'all_dates_found': [],
        }

    # Strip URLs before date detection so links do not create fake dates
    text_for_dates = re.sub(r'https?://\S+', ' ', fragment_text)
    text_for_dates = re.sub(r'www\.\S+', ' ', text_for_dates)

    raw_dates = _find_dates_in_text(text_for_dates)

    # No dates found in fragment — fall back to inherited
    if not raw_dates:
        if inherited_date:
            return {
                'extracted_date': inherited_date,
                'date_role': 'effective_date',
                'raw_date_string': None,
                'date_source': 'inherited_section',
                'all_dates_found': [],
            }
        return {
            'extracted_date': None,
            'date_role': 'none',
            'raw_date_string': None,
            'date_source': 'none',
            'all_dates_found': [],
        }

    classified = []
    for date_match in raw_dates:
        parsed = _parse_date(date_match['raw_date_string'])
        if not parsed:
            continue
        role = _classify_date_role(text_for_dates, date_match['start_pos'])
        classified.append({
            'raw_date_string': date_match['raw_date_string'],
            'extracted_date': parsed,
            'date_role': role,
        })

    if not classified:
        if inherited_date:
            return {
                'extracted_date': inherited_date,
                'date_role': 'effective_date',
                'raw_date_string': None,
                'date_source': 'inherited_section',
                'all_dates_found': [],
            }
        return {
            'extracted_date': None,
            'date_role': 'none',
            'raw_date_string': None,
            'date_source': 'none',
            'all_dates_found': [],
        }

    # Direct effective date found — highest priority
    effective_dates = [d for d in classified if d['date_role'] == 'effective_date']
    if effective_dates:
        best = max(effective_dates, key=lambda d: d['extracted_date'])
        return {
            'extracted_date': best['extracted_date'],
            'date_role': 'effective_date',
            'raw_date_string': best['raw_date_string'],
            'date_source': 'direct',
            'all_dates_found': classified,
        }

    # Multiple competing roles = ambiguous — fall back to inherited if available
    roles = set(d['date_role'] for d in classified)
    if len(roles) > 1:
        if inherited_date:
            return {
                'extracted_date': inherited_date,
                'date_role': 'effective_date',
                'raw_date_string': None,
                'date_source': 'inherited_section',
                'all_dates_found': classified,
            }
        return {
            'extracted_date': None,
            'date_role': 'ambiguous',
            'raw_date_string': None,
            'date_source': 'ambiguous',
            'all_dates_found': classified,
        }

    # Single non-effective role
    best = classified[0]
    return {
        'extracted_date': best['extracted_date'],
        'date_role': best['date_role'],
        'raw_date_string': best['raw_date_string'],
        'date_source': 'direct',
        'all_dates_found': classified,
    }


# ---------------------------------------------------------------------------
# STANDALONE TEST
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from modules.module_b.extractor import extract_fragments

    data_dir = Path(__file__).resolve().parents[2] / 'data' / 'raw'
    pdfs = list(data_dir.glob('*.pdf'))
    if not pdfs:
        print("No PDFs found in data/raw/")
        sys.exit(1)

    result = extract_fragments(str(pdfs[0]))
    fragments = result['fragments']
    print(f"\nRunning date classifier on {len(fragments)} fragments...\n")

    direct = inherited = ambiguous = no_date = 0
    for frag in fragments:
        dr = classify_dates(frag['fragment_text'], frag.get('inherited_date'))
        src = dr['date_source']
        if src == 'direct':
            direct += 1
        elif src == 'inherited_section':
            inherited += 1
        elif src == 'ambiguous':
            ambiguous += 1
        else:
            no_date += 1

    print("DATE CLASSIFICATION RESULTS:")
    print(f"  Total             : {len(fragments)}")
    print(f"  Direct            : {direct}")
    print(f"  Inherited section : {inherited}")
    print(f"  Ambiguous         : {ambiguous}")
    print(f"  No date           : {no_date}")