# modules/module_a/crawler.py
from __future__ import annotations

import hashlib
import mimetypes
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import requests

from db_init import DB_PATH, get_connection

# ---------------------------------------------------------------------------
# MVP URL manifest
# Updated to live eMedNY PDF endpoints
# ---------------------------------------------------------------------------

URLS = [
    "https://www.emedny.org/ProviderManuals/inpatient/pdfs/inpatient_billing_guidelines.pdf",
    "https://www.emedny.org/ProviderManuals/Inpatient/Pdfs/Inpatient_Policy_Guidelines.pdf",
]

BASE_DIR = PROJECT_ROOT
RAW_DIR = BASE_DIR / "data" / "raw"

REQUEST_TIMEOUT_SECONDS = 30
USER_AGENT = "DPKB-MVP-Crawler/1.1"

# ---------------------------------------------------------------------------
# Timestamp + hashing helpers
# ---------------------------------------------------------------------------

def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\\\|?*]+', "_", name).strip()
    return cleaned or "downloaded_file"


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    raw_name = Path(parsed.path).name
    if raw_name:
        return sanitize_filename(raw_name)
    return "downloaded_file.pdf"


def infer_mime_type(url: str, response_content_type: str | None) -> str:
    if response_content_type:
        return response_content_type.split(";")[0].strip()
    guessed, _ = mimetypes.guess_type(url)
    return guessed or "application/octet-stream"


def infer_title_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    title = stem.replace("_", " ").replace("-", " ").strip()
    return title or filename


def is_pdf_response(content: bytes, content_type: str | None) -> tuple[bool, str]:
    """
    Accept only real PDFs.
    Checks:
    1. Content-Type contains application/pdf
    2. File bytes start with %PDF
    """
    normalized_content_type = (content_type or "").lower()

    if "application/pdf" not in normalized_content_type:
        return False, f"Expected application/pdf content-type, got {content_type!r}"

    if not content.startswith(b"%PDF"):
        preview = content[:100].decode("utf-8", errors="replace")
        return False, f"Response did not start with %PDF. First bytes preview: {preview!r}"

    return True, "Valid PDF response"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_manual_source_type_id(conn) -> int:
    row = conn.execute(
        """
        SELECT source_type_id
        FROM source_types
        WHERE source_type_code = 'MANUAL'
        """
    ).fetchone()

    if row is None:
        raise RuntimeError(
            "source_types is missing source_type_code='MANUAL'. "
            "Run seed_sample_data.py first."
        )
    return row["source_type_id"]


def existing_document_by_hash(conn, url: str, sha256_hash: str):
    return conn.execute(
        """
        SELECT source_document_id, source_url, sha256_hash, file_path
        FROM source_documents
        WHERE source_url = ? AND sha256_hash = ?
        """
        ,
        (url, sha256_hash),
    ).fetchone()


def next_source_document_id(conn) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(source_document_id), 0) + 1 AS next_id FROM source_documents"
    ).fetchone()
    return int(row["next_id"])


def insert_source_document(
    conn,
    *,
    source_document_id: int,
    source_type_id: int,
    title: str,
    source_url: str,
    file_path: str,
    mime_type: str,
    sha256_hash: str,
    retrieved_at: str,
    published_date: str | None,
    authority_rank: int,
    document_version_label: str | None,
    notes: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO source_documents (
            source_document_id,
            source_type_id,
            title,
            source_url,
            file_path,
            mime_type,
            sha256_hash,
            retrieved_at,
            published_date,
            document_effective_start_date,
            document_effective_end_date,
            authority_rank,
            document_version_label,
            is_active,
            notes,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_document_id,
            source_type_id,
            title,
            source_url,
            file_path,
            mime_type,
            sha256_hash,
            retrieved_at,
            published_date,
            None,
            None,
            authority_rank,
            document_version_label,
            1,
            notes,
            retrieved_at,
            retrieved_at,
        ),
    )


# ---------------------------------------------------------------------------
# Cleanup helpers for bad HTML downloads from the old crawler
# ---------------------------------------------------------------------------

def delete_bad_source_document_rows(conn, source_document_ids: list[int]) -> int:
    if not source_document_ids:
        return 0

    placeholders = ",".join("?" for _ in source_document_ids)
    conn.execute(
        f"DELETE FROM source_documents WHERE source_document_id IN ({placeholders})",
        source_document_ids,
    )
    return len(source_document_ids)


def remove_bad_raw_files(file_names: list[str]) -> int:
    removed = 0
    for name in file_names:
        file_path = RAW_DIR / name
        if file_path.exists():
            file_path.unlink()
            removed += 1
    return removed


def cleanup_old_bad_downloads() -> None:
    """
    Remove the two bad HTML-not-PDF rows/files created by the old crawler run.
    Safe to run even if they were already removed.
    """
    bad_ids = [6, 7]
    bad_files = ["MANCMS_FacInPt.pdf", "MANCMS_FacOutPt.pdf"]

    conn = get_connection(DB_PATH)
    try:
        deleted_rows = delete_bad_source_document_rows(conn, bad_ids)
        conn.commit()
    finally:
        conn.close()

    removed_files = remove_bad_raw_files(bad_files)

    print("[crawler] Cleanup")
    print(f"  deleted source_documents rows: {deleted_rows}")
    print(f"  removed raw files            : {removed_files}")


# ---------------------------------------------------------------------------
# Network / file helpers
# ---------------------------------------------------------------------------

def download_url(url: str) -> tuple[bytes, str]:
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "")
    if not response.content:
        raise RuntimeError(f"Downloaded empty response for URL: {url}")

    return response.content, content_type


def save_content(raw_dir: Path, filename: str, content: bytes) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    file_path = raw_dir / filename
    file_path.write_bytes(content)
    return file_path


# ---------------------------------------------------------------------------
# Core crawl logic
# ---------------------------------------------------------------------------

def crawl_one(conn, url: str) -> dict:
    retrieved_at = utc_now_str()
    source_type_id = get_manual_source_type_id(conn)

    try:
        content, response_content_type = download_url(url)
    except Exception as exc:
        return {
            "url": url,
            "status": "error",
            "message": f"Download failed: {type(exc).__name__}: {exc}",
        }

    is_pdf, validation_message = is_pdf_response(content, response_content_type)
    if not is_pdf:
        return {
            "url": url,
            "status": "rejected",
            "message": f"URL returned non-PDF content. {validation_message}",
        }

    sha256_hash = sha256_bytes(content)

    existing = existing_document_by_hash(conn, url, sha256_hash)
    if existing is not None:
        return {
            "url": url,
            "status": "skipped",
            "message": (
                f"Hash already exists for this URL "
                f"(source_document_id={existing['source_document_id']})."
            ),
            "source_document_id": existing["source_document_id"],
            "sha256_hash": sha256_hash,
            "file_path": existing["file_path"],
        }

    filename = filename_from_url(url)
    mime_type = infer_mime_type(url, response_content_type)
    title = infer_title_from_filename(filename)

    try:
        saved_path = save_content(RAW_DIR, filename, content)
    except Exception as exc:
        return {
            "url": url,
            "status": "error",
            "message": f"File save failed: {type(exc).__name__}: {exc}",
        }

    source_document_id = next_source_document_id(conn)

    try:
        insert_source_document(
            conn,
            source_document_id=source_document_id,
            source_type_id=source_type_id,
            title=title,
            source_url=url,
            file_path=str(saved_path.relative_to(BASE_DIR)),
            mime_type=mime_type,
            sha256_hash=sha256_hash,
            retrieved_at=retrieved_at,
            published_date=None,
            authority_rank=1,
            document_version_label=None,
            notes="Downloaded by Module A MVP crawler",
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return {
            "url": url,
            "status": "error",
            "message": f"DB insert failed: {type(exc).__name__}: {exc}",
        }

    return {
        "url": url,
        "status": "inserted",
        "message": "Downloaded, validated as PDF, saved, and registered in source_documents.",
        "source_document_id": source_document_id,
        "sha256_hash": sha256_hash,
        "file_path": str(saved_path.relative_to(BASE_DIR)),
    }


def crawl_all(urls: list[str] | None = None) -> list[dict]:
    urls = urls or URLS

    conn = get_connection(DB_PATH)
    try:
        results = []
        for url in urls:
            results.append(crawl_one(conn, url))
        return results
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    if "--cleanup-bad" in sys.argv:
        cleanup_old_bad_downloads()
        return

    urls = [arg for arg in sys.argv[1:] if not arg.startswith("--")] or URLS

    print("[crawler] Starting Module A MVP crawl...")
    print(f"[crawler] Target count: {len(urls)}")

    results = crawl_all(urls)

    inserted = sum(1 for r in results if r["status"] == "inserted")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    rejected = sum(1 for r in results if r["status"] == "rejected")
    errors = sum(1 for r in results if r["status"] == "error")

    print()
    for result in results:
        print(f"[{result['status'].upper():8}] {result['url']}")
        print(f"           {result['message']}")
        if "source_document_id" in result:
            print(f"           source_document_id={result['source_document_id']}")
        if "file_path" in result:
            print(f"           file_path={result['file_path']}")
        if "sha256_hash" in result:
            print(f"           sha256={result['sha256_hash']}")
        print()

    print("[crawler] Summary")
    print(f"  inserted: {inserted}")
    print(f"  skipped : {skipped}")
    print(f"  rejected: {rejected}")
    print(f"  errors  : {errors}")


if __name__ == "__main__":
    main()