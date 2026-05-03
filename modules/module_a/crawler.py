# modules/module_a/crawler.py
"""
Module A — Medicaid Update Bulletin Crawler
Auto-downloads NY Medicaid Update PDFs from 2022 to present.
Uses SHA-256 to skip unchanged files and detect new/modified ones.
Logs each run to crawler_runs table in DB.
"""

import hashlib
import sqlite3
import re
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# PROJECT ROOT
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / '.env')

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

CRAWL_START_YEAR = 2022
CRAWL_END_YEAR   = datetime.now().year

BASE_URL = "https://www.health.ny.gov/health_care/medicaid/program/update/{year}/"

PDF_PATTERNS = [r'mu_no.*_pr\.pdf']

DATA_RAW_DIR = PROJECT_ROOT / 'data' / 'raw'
DB_PATH      = PROJECT_ROOT / 'db' / 'dpkb.db'

REQUEST_TIMEOUT = 30
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; DPKB-Crawler/1.0)'}


# ---------------------------------------------------------------------------
# SHA-256
# ---------------------------------------------------------------------------

def _sha256_of_file(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(str(file_path), 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def _sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# FIND PDF LINKS ON A YEAR PAGE
# ---------------------------------------------------------------------------

def _get_pdf_links(year: int) -> list:
    url = BASE_URL.format(year=year)
    print(f"\n  Checking: {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 404:
            print(f"  Page not found — year {year} may not exist yet")
            return []
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Failed to fetch {url}: {e}")
        return []

    soup      = BeautifulSoup(resp.text, 'html.parser')
    pdf_links = []

    for a_tag in soup.find_all('a', href=True):
        href     = a_tag['href']
        filename = href.split('/')[-1].lower()

        if not href.lower().endswith('.pdf'):
            continue
        if not any(re.search(p, filename) for p in PDF_PATTERNS):
            continue

        if href.startswith('http'):
            full_url = href
        elif href.startswith('/'):
            full_url = f"https://www.health.ny.gov{href}"
        else:
            full_url = f"{BASE_URL.format(year=year)}{href}"

        pdf_links.append({'url': full_url, 'filename': href.split('/')[-1]})

    print(f"  Found {len(pdf_links)} bulletin PDFs")
    return pdf_links


# ---------------------------------------------------------------------------
# DOWNLOAD A SINGLE PDF
# ---------------------------------------------------------------------------

def _download_pdf(url: str, filename: str) -> dict:
    save_path = DATA_RAW_DIR / filename

    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {'filename': filename, 'status': 'error',
                'error': str(e), 'sha256': None}

    new_hash = _sha256_of_bytes(resp.content)

    if save_path.exists():
        existing_hash = _sha256_of_file(save_path)
        if existing_hash == new_hash:
            return {'filename': filename, 'status': 'skipped',
                    'sha256': new_hash, 'file_path': str(save_path)}
        save_path.write_bytes(resp.content)
        return {'filename': filename, 'status': 'updated',
                'sha256': new_hash, 'file_path': str(save_path)}

    save_path.write_bytes(resp.content)
    return {'filename': filename, 'status': 'downloaded',
            'sha256': new_hash, 'file_path': str(save_path)}


# ---------------------------------------------------------------------------
# LOG TO DB — column names match crawler_runs schema exactly
# ---------------------------------------------------------------------------

def _log_crawler_run(results: list, total_found: int):
    """Write crawler run summary to crawler_runs table."""
    if not DB_PATH.exists():
        print("  [crawler log] DB not found — skipping log")
        return

    downloaded = sum(1 for r in results if r['status'] == 'downloaded')
    updated    = sum(1 for r in results if r['status'] == 'updated')
    skipped    = sum(1 for r in results if r['status'] == 'skipped')
    errors     = sum(1 for r in results if r['status'] == 'error')
    now        = datetime.now().isoformat()

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode = WAL')
        conn.execute("""
            INSERT INTO crawler_runs
                (started_at, completed_at,
                 documents_found, documents_added,
                 documents_skipped, documents_failed,
                 run_status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now, now,
            total_found,
            downloaded + updated,
            skipped,
            errors,
            'completed',
            (
                f"Auto-crawl {CRAWL_START_YEAR}-{CRAWL_END_YEAR}. "
                f"Downloaded: {downloaded}, Updated: {updated}, "
                f"Skipped: {skipped}, Errors: {errors}"
            ),
        ))
        conn.commit()
        conn.close()
        print("  [crawler log] Run logged to DB successfully")
    except Exception as e:
        print(f"  [crawler log] DB log failed: {e}")


# ---------------------------------------------------------------------------
# MAIN CRAWL FUNCTION
# ---------------------------------------------------------------------------

def run_crawl() -> dict:
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"DPKB CRAWLER — NY Medicaid Update Bulletins")
    print(f"Range: {CRAWL_START_YEAR} to {CRAWL_END_YEAR}")
    print(f"Save dir: {DATA_RAW_DIR}")
    print(f"{'='*60}")

    all_results = []
    total_found = 0

    for year in range(CRAWL_START_YEAR, CRAWL_END_YEAR + 1):
        pdf_links    = _get_pdf_links(year)
        total_found += len(pdf_links)

        for pdf in pdf_links:
            print(f"    → {pdf['filename']} ... ", end='', flush=True)
            result = _download_pdf(pdf['url'], pdf['filename'])
            all_results.append(result)
            print(result['status'].upper())

    downloaded = sum(1 for r in all_results if r['status'] == 'downloaded')
    updated    = sum(1 for r in all_results if r['status'] == 'updated')
    skipped    = sum(1 for r in all_results if r['status'] == 'skipped')
    errors     = sum(1 for r in all_results if r['status'] == 'error')

    print(f"\n{'='*60}")
    print(f"CRAWL COMPLETE")
    print(f"  Total found   : {total_found}")
    print(f"  Downloaded    : {downloaded}")
    print(f"  Updated       : {updated}")
    print(f"  Skipped       : {skipped}")
    print(f"  Errors        : {errors}")
    print(f"{'='*60}")

    _log_crawler_run(all_results, total_found)

    return {
        'total_found': total_found,
        'downloaded':  downloaded,
        'updated':     updated,
        'skipped':     skipped,
        'errors':      errors,
        'results':     all_results,
    }


if __name__ == '__main__':
    run_crawl()