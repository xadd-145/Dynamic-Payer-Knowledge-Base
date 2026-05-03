# modules/module_b/extractor.py
"""
Module B — Step 1 + Step 2: PDF Text Extractor
Step 2 additions:
  2A — Hard reject lists (structural noise + out-of-scope)
  2B — Section context tracking for inherited effective date
       Inherited date updates from ANY effective-date line, not only headers.
       Hard reject runs BEFORE soft filter.
"""

import re
import hashlib
from pathlib import Path

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

try:
    import fitz
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False


# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

MIN_FRAGMENT_WORDS = 20
MAX_FRAGMENT_WORDS = 500

BILLING_VERBS = [
    'must', 'shall', 'required', 'may not', 'billed under',
    'effective for', 'billed', 'submit', 'report', 'claim',
    'reimburse', 'covered', 'not covered', 'excluded', 'allowed',
    'prohibited', 'requires', 'should', 'applies'
]

HEADER_PATTERNS = [
    r'^\s*\d+\.\d+[\s\t]+[A-Z]',
    r'^\s*[A-Z][A-Z\s]{5,}$',
    r'^\s*[A-Z][a-z].*:$',
    r'^\s*\d+\.\s+[A-Z]',
    r'^\s*[•\-\*]\s+[A-Z]',
]


# ---------------------------------------------------------------------------
# STEP 2A — HARD REJECT LISTS
# Deterministic. Runs BEFORE soft filter. No GPT involved.
# ---------------------------------------------------------------------------

STRUCTURAL_NOISE_PATTERNS = [
    r'provider\s+directory',
    r'directory\s+of\s+providers',
    r'governor\s+of\s+the\s+state',
    r'commissioner\s+of\s+health',
    r'acting\s+commissioner',
    r'new\s+york\s+state\s+department\s+of\s+health',
    r'table\s+of\s+contents',
    r'^\s*\.{5,}',
    r'webinar\s+(will\s+be\s+held|is\s+scheduled|registration)',
    r'survey\s+(will\s+be|is\s+being)\s+(conducted|distributed)',
    r'annual\s+provider\s+survey',
    r'provider\s+satisfaction\s+survey',
    r'telehealth\s+consumer\s+survey',
    r'\bsurvey\s+is\s+available\b',
    r'\btake\s+the\s+survey\b',
    r'\bscan\s+the\s+qr\s+code\b',
    r'\busing\s+your\s+mobile\s+device\b',
    r'for\s+questions\s+(please\s+)?contact',
    r'questions\s+may\s+be\s+(directed|submitted)\s+to',
    r'please\s+direct\s+(all\s+)?questions',
    r'questions\s+should\s+be\s+directed',
    r'questions\s+should\s+be\s+sent',
    r'mmc\s+plan\s+contact',
    r'plan\s+directory\s+information',
    r'information\s+for\s+all\s+providers',
    r'new\s+york\s+medicaid\s+update',
    r'this\s+publication\s+is\s+produced',
    r'medicaid\s+program\s+changes\s+are\s+detailed',
    r'in\s+this\s+issue',
    r'provider\s+training',
    r'fraud\s+hotline',
    r'comments\s+and\s+suggestions',
    r'\*{10,}',
]

OUT_OF_SCOPE_PATTERNS = [
    r'\bnyia\b',
    r'\bcdpas\b',
    r'consumer.directed\s+personal\s+assistance',
    r'personal\s+care\s+services?\s+(program|aide)',
    r'\bmltc\b',
    r'managed\s+long.term\s+care',
    r'independent\s+review\s+of\s+high\s+needs\s+cases',
    r'high\s+needs\s+cases',
    r'plans?\s+of\s+care',
    r'\bcha\s+result\b',
    r'\bcha\s+and\s+po\b',
    r'\bpo\s+will\s+be\s+valid\s+for\s+12\s+months',
    r'\bvalid\s+for\s+12\s+months\b',
    r'\bcdt\s+code',
    r'dental\s+procedure\s+code',
    r'oral\s+health\s+service',
    r'pharmacy\s+enrollment',
    r'pecos\s+enrollment',
    r'provider\s+enrollment\s+and\s+credentialing',
    r'\bpcmh\b',
    r'patient.centered\s+medical\s+home',
    r'\bpmpm\b',
    r'per\s+member\s+per\s+month',
    r'\bcms.1500\b',
    r'\bprofessional\s+claims?\b(?!.*facility)',
    r'\bphysician\s+billing\b(?!.*facility)',
]

_STRUCTURAL_NOISE_RE = [re.compile(p, re.IGNORECASE) for p in STRUCTURAL_NOISE_PATTERNS]
_OUT_OF_SCOPE_RE = [re.compile(p, re.IGNORECASE) for p in OUT_OF_SCOPE_PATTERNS]

# ---------------------------------------------------------------------------
# PROTECTED POLICY SIGNALS
# If a fragment contains one of these, do not reject it only because it also
# contains provider manual links, contact language, directory text, or URLs.
# ---------------------------------------------------------------------------

PROTECTED_POLICY_PATTERNS = [
    r'\bvalue\s+code\s+54\b',
    r'\bvalue\s+code\b',
    r'\bbirth\s+weight\b',
    r'\bnewborn\b',
    r'\bapr-?drg\b',
    r'\bdrg\b',
    r'\binpatient\s+claims?\b',
    r'\binpatient\s+billing\b',
    r'\bhospitals?\s+must\s+accurately\s+report\b',
    r'\bdiagnosis\s+codes?\b',
    r'\bomig\b',
    r'\bub-04\b',
    r'\bcms-1450\b',
    r'\b99457\b',
    r'\bapg\b',
    r'\bremote\s+(patient|physiologic)\s+monitoring\b',
    r'\boutpatient\s+departments?\b',
    r'\bfqhcs?\b',
    r'\bfederally\s+qualified\s+health\s+centers?\b',
]

_PROTECTED_POLICY_RE = [
    re.compile(p, re.IGNORECASE) for p in PROTECTED_POLICY_PATTERNS
]

CONTEXT_ONLY_NOISE_MARKERS = [
    'questions', 'contact', 'directory', 'information', 'provider',
    'manual', 'manuals', 'located', 'https', 'www', 'plan', 'mmc',
    'asterisk', 'in_this_issue', 'publication', '\\*', '*',
]


def _has_protected_policy_signal(fragment_text: str) -> bool:
    return any(pattern.search(fragment_text) for pattern in _PROTECTED_POLICY_RE)


def _is_context_only_noise_pattern(pattern_text: str) -> bool:
    pattern_text = pattern_text.lower()
    return any(marker in pattern_text for marker in CONTEXT_ONLY_NOISE_MARKERS)


def _hard_reject(fragment_text: str) -> tuple:
    """
    Returns (should_reject: bool, reason: str)

    Logic:
      1. Check if fragment contains strong UB-04 / facility billing signals.
      2. Reject structural noise — but if the fragment is a protected policy
         fragment, skip noise patterns that are context-only (links, directories,
         manual references, contact language, asterisk dividers).
      3. Always reject out-of-scope content (NYIA, CDPAS, dental, pharmacy, PCMH).
    """
    text = fragment_text.lower()
    protected_policy = _has_protected_policy_signal(fragment_text)

    for pattern in _STRUCTURAL_NOISE_RE:
        if pattern.search(text):
            if protected_policy and _is_context_only_noise_pattern(pattern.pattern):
                continue
            return True, f"structural_noise: {pattern.pattern[:40]}"

    for pattern in _OUT_OF_SCOPE_RE:
        if pattern.search(text):
            return True, f"out_of_scope: {pattern.pattern[:40]}"

    return False, ""


# ---------------------------------------------------------------------------
# STEP 2B — SECTION CONTEXT TRACKING
# Inherited date updates from ANY line with effective-date language.
# ---------------------------------------------------------------------------

_SECTION_DATE_PATTERN = re.compile(
    r'\b(?:January|February|March|April|May|June|July|August|'
    r'September|October|November|December)\s+\d{1,2},?\s+\d{4}\b'
    r'|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4}\b'
    r'|\b\d{1,2}/\d{1,2}/\d{4}\b'
    r'|\b\d{4}-\d{2}-\d{2}\b',
    re.IGNORECASE
)

_EFFECTIVE_KEYWORDS = [
    'effective', 'on and after', 'on or after', 'for dates of service',
    'beginning', 'takes effect', 'applicable', 'for discharges'
]


def _extract_section_effective_date(line_text: str) -> str | None:
    """
    Try to extract an effective date from any line that contains
    effective-date language. Not limited to formal section headers.
    Returns YYYY-MM-DD or None.
    """
    text_lower = line_text.lower()
    if not any(kw in text_lower for kw in _EFFECTIVE_KEYWORDS):
        return None
    match = _SECTION_DATE_PATTERN.search(line_text)
    if not match:
        return None
    try:
        from dateutil import parser as dateutil_parser
        parsed = dateutil_parser.parse(match.group(), dayfirst=False)
        if 2000 <= parsed.year <= 2030:
            return parsed.strftime('%Y-%m-%d')
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# PDFPLUMBER EXTRACTION (PRIMARY)
# ---------------------------------------------------------------------------

def _extract_with_pdfplumber(pdf_path: Path) -> list:
    pages = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text(x_tolerance=3, y_tolerance=3)
            if text and text.strip():
                pages.append({'page_number': i, 'raw_text': text.strip()})
    return pages


# ---------------------------------------------------------------------------
# PYMUPDF EXTRACTION (FALLBACK)
# ---------------------------------------------------------------------------

def _extract_with_pymupdf(pdf_path: Path) -> list:
    pages = []
    doc = fitz.open(str(pdf_path))
    for i, page in enumerate(doc, start=1):
        text = page.get_text("text")
        if text and text.strip():
            pages.append({'page_number': i, 'raw_text': text.strip()})
    doc.close()
    return pages


# ---------------------------------------------------------------------------
# FRAGMENT BOUNDARY DETECTION
# ---------------------------------------------------------------------------

def _is_section_header(line: str) -> bool:
    line = line.strip()
    if not line:
        return False
    for pattern in HEADER_PATTERNS:
        if re.match(pattern, line):
            return True
    return False


def _split_into_fragments(page_number: int, raw_text: str,
                          current_section_date: list) -> list:
    """
    Split page text into fragments.
    current_section_date is a 1-element list — mutable reference so
    section date persists across pages.

    Step 2B fix: inherited date updates from ANY line with effective-date
    language, not only formal section headers.
    """
    raw_lines = raw_text.split('\n')
    lines = []
    for line in raw_lines:
        s = line.strip()
        if not s:
            lines.append(line)
            continue
        if 'Version 20' in s and ('Page' in s or 'May' in s or
                                   'June' in s or 'July' in s):
            continue
        if s.startswith('Page ') and ' of ' in s:
            continue
        if s in ('INPATIENT HOSPITAL', 'C L A I M S U B MI S S I O N',
                 'T A B LE O F C O N T EN T S', 'P U R P O S E S T A T E M EN T',
                 'E M E D N Y IN F O R M A TI O N'):
            continue
        lines.append(line)

    fragments = []
    current_block = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current_block:
                block_text = ' '.join(current_block).strip()
                if block_text:
                    fragments.append({
                        'fragment_text': block_text,
                        'page_number': page_number,
                        'inherited_date': current_section_date[0],
                    })
                current_block = []
            continue

        # Step 2B: update inherited date from ANY line that contains
        # effective-date language — not only formal section headers.
        line_date = _extract_section_effective_date(stripped)
        if line_date:
            current_section_date[0] = line_date

        if _is_section_header(stripped) and current_block:
            block_text = ' '.join(current_block).strip()
            if block_text:
                fragments.append({
                    'fragment_text': block_text,
                    'page_number': page_number,
                    'inherited_date': current_section_date[0],
                })
            current_block = [stripped]
        else:
            current_block.append(stripped)

    if current_block:
        block_text = ' '.join(current_block).strip()
        if block_text:
            fragments.append({
                'fragment_text': block_text,
                'page_number': page_number,
                'inherited_date': current_section_date[0],
            })

    return fragments


# ---------------------------------------------------------------------------
# SOFT FRAGMENT FILTER
# ---------------------------------------------------------------------------

def _is_valid_fragment(fragment_text: str) -> bool:
    text = fragment_text.strip()
    words = text.split()
    word_count = len(words)

    if word_count < MIN_FRAGMENT_WORDS:
        return False
    if word_count > MAX_FRAGMENT_WORDS:
        return False

    digit_ratio = sum(1 for c in text if c.isdigit()) / max(len(text), 1)
    if digit_ratio > 0.4:
        return False

    text_lower = text.lower()
    if not any(verb in text_lower for verb in BILLING_VERBS):
        return False

    NOISE_ONLY_PHRASES = [
        'questions should be directed',
        'questions should be sent',
        'please contact',
        'for more information visit',
        'can be found at',
        'please visit the',
        'call the emedny',
        'located at: https',
        'located at: http',
        'https://',
        'http://',
        'mmc plan contact',
        'plan directory information',
        'information for all providers',
        'provider manuals',
        'scan the qr code',
        'take the survey',
        'survey is available',
    ]
    noise_hits = sum(1 for p in NOISE_ONLY_PHRASES if p in text_lower)
    policy_hits = sum(1 for v in BILLING_VERBS if v in text_lower)
    if noise_hits >= 2 and policy_hits <= 1:
        return False

    return True


# ---------------------------------------------------------------------------
# SHA-256
# ---------------------------------------------------------------------------

def _sha256(pdf_path: Path) -> str:
    h = hashlib.sha256()
    with open(str(pdf_path), 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# MAIN EXTRACTOR
# ---------------------------------------------------------------------------

def extract_fragments(pdf_path: str) -> dict:
    """
    Main entry point for Module B extraction.

    Step 2 changes:
      - hard_rejected count returned
      - Each fragment carries inherited_date
      - Hard reject runs BEFORE soft filter
    """
    pdf_path = Path(pdf_path)

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    print(f"\nExtracting: {pdf_path.name}")
    print(f"  File size: {pdf_path.stat().st_size / 1024:.1f} KB")

    sha256 = _sha256(pdf_path)
    print(f"  SHA-256: {sha256[:16]}...")

    pages = []
    if PDFPLUMBER_AVAILABLE:
        try:
            pages = _extract_with_pdfplumber(pdf_path)
            print(f"  Extractor: pdfplumber — {len(pages)} pages extracted")
        except Exception as e:
            print(f"  pdfplumber failed: {e} — trying PyMuPDF")

    if not pages and PYMUPDF_AVAILABLE:
        try:
            pages = _extract_with_pymupdf(pdf_path)
            print(f"  Extractor: PyMuPDF (fallback) — {len(pages)} pages extracted")
        except Exception as e:
            raise RuntimeError(f"Both extractors failed. Last error: {e}")

    if not pages:
        raise RuntimeError("No text extracted from PDF.")

    current_section_date = [None]
    raw_fragments = []

    for page in pages:
        page_fragments = _split_into_fragments(
            page_number=page['page_number'],
            raw_text=page['raw_text'],
            current_section_date=current_section_date,
        )
        raw_fragments.extend(page_fragments)

    all_fragments = []
    hard_rejected_count = 0
    sequence_number = 0

    for frag in raw_fragments:
        # Step 2A: hard reject FIRST — before soft filter
        should_reject, _ = _hard_reject(frag['fragment_text'])
        if should_reject:
            hard_rejected_count += 1
            continue

        # Soft filter second
        if not _is_valid_fragment(frag['fragment_text']):
            continue

        sequence_number += 1
        all_fragments.append({
            'fragment_text': frag['fragment_text'],
            'page_number': frag['page_number'],
            'source_filename': pdf_path.name,
            'sequence_number': sequence_number,
            'inherited_date': frag.get('inherited_date'),
        })

    print(f"  Raw fragments:           {len(raw_fragments)}")
    print(f"  Hard rejected (2A):      {hard_rejected_count}")
    print(f"  Valid fragments (final): {len(all_fragments)}")

    return {
        'source_filename': pdf_path.name,
        'sha256_hash': sha256,
        'total_pages': len(pages),
        'total_fragments': len(all_fragments),
        'hard_rejected': hard_rejected_count,
        'fragments': all_fragments,
    }


# ---------------------------------------------------------------------------
# STANDALONE TEST
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import sys
    data_dir = Path(__file__).resolve().parents[2] / 'data' / 'raw'
    pdfs = list(data_dir.glob('*.pdf'))
    if not pdfs:
        print(f"No PDFs found in {data_dir}")
        sys.exit(1)

    result = extract_fragments(str(pdfs[0]))
    print(f"\nRESULTS:")
    print(f"  Source:          {result['source_filename']}")
    print(f"  Pages:           {result['total_pages']}")
    print(f"  Hard rejected:   {result['hard_rejected']}")
    print(f"  Valid fragments: {result['total_fragments']}")
    print(f"\nFIRST 3 FRAGMENTS:")
    for frag in result['fragments'][:3]:
        print(f"\n  --- Fragment (page {frag['page_number']}) ---")
        print(f"  Inherited date: {frag['inherited_date']}")
        print(f"  {frag['fragment_text'][:200]}...")