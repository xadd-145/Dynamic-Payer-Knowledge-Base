# modules/module_b/confidence_scorer.py
"""
Module B — Step 3 + Step 2D: Confidence Scorer

Step 2D change:
  _score_effective_date now takes date_source:
    direct / None / ''    = 40 pts  (backward compatible default)
    inherited_section     = 30 pts
    anything else         =  0 pts

  score_fragment default changed from 'none' to 'direct' so old callers
  that pass only (text, date_role) still get 40 pts for effective_date.
"""

import re
import os
import sys
import json
import urllib.request
import urllib.error
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / '.env')

from constants import (
    TIER_1_THRESHOLD,
    TIER_2_THRESHOLD,
    CONFIDENCE_WEIGHTS,
    SEMANTIC_TIER_CAP,
    OPENAI_MODEL,
)

OPENAI_API_KEY  = os.environ.get('OPENAI_API_KEY', '')
OPENAI_ENDPOINT = 'https://api.openai.com/v1/chat/completions'


# ---------------------------------------------------------------------------
# SIGNAL 1 — EFFECTIVE DATE (40 pts max)
# Step 2D fix: defaults to 'direct' so old callers are not penalized.
# ---------------------------------------------------------------------------

def _score_effective_date(date_role: str, date_source: str = 'direct') -> int:
    """
    direct / None / ''    -> 40 pts  (backward compatible)
    inherited_section     -> 30 pts
    anything else         ->  0 pts
    """
    if date_role != 'effective_date':
        return 0

    if date_source in ('direct', None, ''):
        return 40

    if date_source == 'inherited_section':
        return 30

    return 0


# ---------------------------------------------------------------------------
# SIGNAL 2 — UB-04 ANCHOR PATTERNS (35 pts)
# ---------------------------------------------------------------------------

REVENUE_CODE_PATTERN = re.compile(r'\b0[0-9]{3}\b')
CPT_PATTERN          = re.compile(r'\b\d{5}\b')
HCPCS_PATTERN        = re.compile(r'\b[A-Z]\d{4}\b')

ANCHOR_KEYWORDS = [
    'revenue code', 'type of bill', 'tob', 'hcpcs', 'cpt code', 'rate code',
    'value code', 'occurrence code', 'condition code', 'form locator', 'fl ',
    '837i', '837', 'place of service', 'modifier', 'procedure code',
    'diagnosis code', 'icd-10', 'icd-9', 'drg', 'apr-drg', 'apg',
    'category of service', 'cos ',
]


def _score_ub04_anchor(fragment_text: str) -> int:
    text_lower   = fragment_text.lower()
    keyword_hits = sum(1 for kw in ANCHOR_KEYWORDS if kw in text_lower)
    score        = 0
    if keyword_hits >= 2:
        score += 25
    elif keyword_hits == 1:
        score += 15

    has_revenue = bool(REVENUE_CODE_PATTERN.search(fragment_text))
    has_hcpcs   = bool(HCPCS_PATTERN.search(fragment_text))
    has_cpt     = bool(CPT_PATTERN.search(fragment_text))

    if has_revenue or has_hcpcs:
        score += 10
    elif has_cpt:
        score += 5

    return min(score, 35)


# ---------------------------------------------------------------------------
# SIGNAL 3 — STRUCTURAL CLARITY (25 pts)
# ---------------------------------------------------------------------------

POLICY_PHRASES = [
    'must be submitted', 'must be billed', 'shall be', 'is required',
    'are required', 'is not covered', 'are not covered', 'will be reimbursed',
    'will not be reimbursed', 'eligible for reimbursement',
    'will be eligible for reimbursement', 'effective for dates of service',
    'effective for claims', 'providers must', 'providers are required',
    'claims must', 'claims submitted', 'prior authorization', 'prior approval',
    'fee-for-service', 'managed care', 'medicaid will', 'medicaid does not',
    'reimbursement is available', 'reimbursement is not available',
    'covered under', 'not covered under', 'billed using', 'billed with',
    'submit claims', 'billing guidance', 'billing requirements',
    'must accurately contain', 'must appear on the claim', 'cannot bill',
    'can not bill', 'not a standalone procedure', 'may submit an apg claim',
    'will no longer receive reimbursement', 'failure to provide',
    'subject to review and recovery',
]

NOISE_PHRASES = [
    'questions should be directed', 'please contact', 'for more information',
    'for additional information', 'can be found at', 'located at',
    'visit the', 'please visit', 'call ', 'email ', 'fax ',
]


def _score_structural_clarity(fragment_text: str) -> int:
    text_lower  = fragment_text.lower()
    policy_hits = sum(1 for p in POLICY_PHRASES if p in text_lower)
    noise_hits  = sum(1 for n in NOISE_PHRASES  if n in text_lower)
    if policy_hits >= 3:
        base = 25
    elif policy_hits == 2:
        base = 20
    elif policy_hits == 1:
        base = 12
    else:
        base = 0
    return max(base - min(noise_hits * 5, 15), 0)


# ---------------------------------------------------------------------------
# SEMANTIC LAYER — GPT-4o-mini
# ---------------------------------------------------------------------------

SEMANTIC_PROMPT = """You are a New York Medicaid UB-04 facility billing policy reviewer.

Score this extracted policy fragment on three dimensions from 0 to 100:

1. meaning_accuracy: Is this a clear, actionable Medicaid billing or policy rule?
2. completeness: Is the fragment complete enough as a standalone policy candidate?
3. anchor_alignment: Does it reference claim fields, UB-04, CPT, HCPCS, APG, APR-DRG,
   revenue code, or facility billing instructions?

Respond ONLY with JSON — no preamble, no markdown:
{{"meaning_accuracy": N, "completeness": N, "anchor_alignment": N}}

Fragment:
{fragment_text}"""


def _safe_int_score(value) -> int:
    try:
        return max(0, min(int(value), 100))
    except Exception:
        return 0


def _score_semantic(fragment_text: str) -> dict | None:
    if not OPENAI_API_KEY:
        print('  [semantic] OPENAI_API_KEY not set — skipping')
        return None
    try:
        payload = json.dumps({
            "model":           OPENAI_MODEL,
            "temperature":     0,
            "max_tokens":      120,
            "response_format": {"type": "json_object"},
            "messages": [{
                "role":    "user",
                "content": SEMANTIC_PROMPT.format(
                    fragment_text=fragment_text[:1200]
                ),
            }],
        }).encode('utf-8')
        req = urllib.request.Request(
            OPENAI_ENDPOINT, data=payload,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {OPENAI_API_KEY}",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"  [semantic] HTTP {e.code}")
        return None
    except Exception as e:
        print(f"  [semantic] Error — {type(e).__name__}: {e}")
        return None

    try:
        content  = json.loads(data['choices'][0]['message']['content'])
        meaning  = _safe_int_score(content.get('meaning_accuracy'))
        complete = _safe_int_score(content.get('completeness'))
        anchor   = _safe_int_score(content.get('anchor_alignment'))
        return {
            'semantic_avg': round((meaning + complete + anchor) / 3),
            'detail': {
                'meaning_accuracy': meaning,
                'completeness':     complete,
                'anchor_alignment': anchor,
            },
        }
    except Exception as e:
        print(f"  [semantic] Parse error — {e}")
        return None


# ---------------------------------------------------------------------------
# TIER ASSIGNMENT
# ---------------------------------------------------------------------------

def _assign_tier(score: int, semantic_skipped: bool = False) -> str:
    if semantic_skipped and score >= TIER_1_THRESHOLD:
        return SEMANTIC_TIER_CAP
    if score >= TIER_1_THRESHOLD:
        return 'TIER_1'
    elif score >= TIER_2_THRESHOLD:
        return 'TIER_2'
    else:
        return 'TIER_3'


# ---------------------------------------------------------------------------
# MAIN SCORING FUNCTION
# Step 2D: date_source defaults to 'direct' for backward compatibility.
# ---------------------------------------------------------------------------

def score_fragment(fragment_text: str, date_role: str,
                   date_source: str = 'direct') -> dict:
    """
    Score one fragment.

    Args:
        fragment_text : raw fragment text
        date_role     : from classify_dates()
        date_source   : from classify_dates() — Step 2D
                        'direct' | 'inherited_section' | 'none' | 'ambiguous'
                        Defaults to 'direct' so old callers are not penalized.
    """
    s1 = _score_effective_date(date_role, date_source)
    s2 = _score_ub04_anchor(fragment_text)
    s3 = _score_structural_clarity(fragment_text)

    structural_total = min(s1 + s2 + s3, 100)
    semantic_result  = _score_semantic(fragment_text)
    semantic_skipped = semantic_result is None

    if semantic_skipped:
        semantic_score = None
        final_score    = structural_total
    else:
        semantic_score = semantic_result['semantic_avg']
        final_score    = round(
            structural_total * CONFIDENCE_WEIGHTS['structural']
            + semantic_score  * CONFIDENCE_WEIGHTS['semantic']
        )

    tier = _assign_tier(final_score, semantic_skipped)

    return {
        'structural_score': structural_total,
        'semantic_score':   semantic_score,
        'semantic_skipped': semantic_skipped,
        'final_score':      final_score,
        'confidence_score': final_score,
        'tier':             tier,
        'signal_breakdown': {
            'effective_date':     s1,
            'ub04_anchor':        s2,
            'structural_clarity': s3,
            'date_source':        date_source,
        },
    }


# ---------------------------------------------------------------------------
# STANDALONE TEST
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    from modules.module_b.extractor import extract_fragments
    from modules.module_b.date_classifier import classify_dates

    data_dir = Path(__file__).resolve().parents[2] / 'data' / 'raw'
    pdfs     = sorted(data_dir.glob('*_pr.pdf'))
    if not pdfs:
        print('No _pr.pdf files in data/raw/')
        sys.exit(1)

    result    = extract_fragments(str(pdfs[0]))
    fragments = result['fragments']
    tier1 = tier2 = tier3 = 0
    scores = []

    for frag in fragments:
        dr  = classify_dates(frag['fragment_text'], frag.get('inherited_date'))
        sr  = score_fragment(
            frag['fragment_text'],
            dr['date_role'],
            dr.get('date_source', 'direct'),
        )
        scores.append(sr['final_score'])
        if sr['tier'] == 'TIER_1':   tier1 += 1
        elif sr['tier'] == 'TIER_2': tier2 += 1
        else:                         tier3 += 1

    print(f'\nSCORING RESULTS:')
    print(f'  Total : {len(fragments)}')
    print(f'  TIER_1: {tier1}')
    print(f'  TIER_2: {tier2}')
    print(f'  TIER_3: {tier3}')
    if scores:
        print(f'  Avg: {sum(scores)/len(scores):.1f}  '
              f'Max: {max(scores)}  Min: {min(scores)}')