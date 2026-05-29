"""
Job Board Scraper — Greenhouse + Ashby
Fetches new jobs, deduplicates, posts to Discord & Notion.
Supports large company lists (7k+ slugs) via concurrent requests.
"""

import json
import os
import sys
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import timedelta

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
SEEN_JOBS_PATH = BASE_DIR / "seen_jobs.json"

# JSON files containing your slug lists — place them next to scraper.py
# Format: ["slug1", "slug2", ...] OR [{"slug": "slug1"}, ...]
GREENHOUSE_FILE = BASE_DIR / "greenhouse_companies.json"
ASHBY_FILE      = BASE_DIR / "ashby_companies.json"

# Credentials from GitHub Actions secrets
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
NOTION_TOKEN        = os.environ.get("NOTION_TOKEN", "")
NOTION_DATABASE_ID  = os.environ.get("NOTION_DATABASE_ID", "")

# "greenhouse", "ashby", or "all" (default — runs both, useful for local testing)
SCRAPER_MODE = os.environ.get("SCRAPER_MODE", "all").lower()

# ── Concurrency & rate limiting ───────────────────────────────────────────────
MAX_WORKERS           = 5     # reduced from 10 — Ashby rate limits aggressively
MAX_RETRIES           = 1     # bad/rate-limited slugs don't improve on retry

GREENHOUSE_DELAY_S    = 0.1   # Greenhouse handles concurrency fine
GREENHOUSE_TIMEOUT    = 15    # seconds

ASHBY_DELAY_S         = 0.5   # Ashby needs more breathing room between requests
ASHBY_TIMEOUT         = 90    # seconds — Ashby boards can take 60-90s to respond

# ── Optional Filters ──────────────────────────────────────────────────────────
INCLUDE_KEYWORDS: list[str] = []   # e.g. ["engineer", "software", "backend"]
EXCLUDE_KEYWORDS: list[str] = []   # e.g. ["senior", "staff", "principal", "intern"]
REMOTE_ONLY: bool = False

# Max age in days. Jobs older than this are ignored.
# Greenhouse: filters on updated_at (last time the post was edited)
# Ashby:      filters on publishedAt (actual date the role went live)
# Set to 0 to disable.
MAX_AGE_DAYS: int = 14


# ─────────────────────────────────────────────────────────────────────────────
# HTTP session with retries
# ─────────────────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ─────────────────────────────────────────────────────────────────────────────
# Company list loader
# ─────────────────────────────────────────────────────────────────────────────

def load_slugs(filepath: Path) -> list[str]:
    """
    Load slugs from a JSON file. Handles two common formats:
      - ["slug1", "slug2", ...]
      - [{"slug": "slug1"}, ...]
      - [{"board_token": "slug1"}, ...]  (Greenhouse export format)
    """
    if not filepath.exists():
        log.warning(f"Company file not found: {filepath} — skipping")
        return []

    data = json.loads(filepath.read_text())

    if not data:
        return []

    # Already a flat list of strings
    if isinstance(data[0], str):
        return data

    # List of dicts — try common key names
    for key in ("slug", "board_token", "company_slug", "name", "id"):
        if key in data[0]:
            return [item[key] for item in data if item.get(key)]

    log.warning(f"Could not detect slug key in {filepath}. Keys found: {list(data[0].keys())}")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Seen-jobs store
# ─────────────────────────────────────────────────────────────────────────────

def load_seen() -> set[str]:
    if SEEN_JOBS_PATH.exists():
        data = json.loads(SEEN_JOBS_PATH.read_text())
        return set(data.get("seen", []))
    return set()


def save_seen(seen: set[str]) -> None:
    SEEN_JOBS_PATH.write_text(
        json.dumps(
            {"seen": sorted(seen), "updated_at": datetime.now(timezone.utc).isoformat()},
            indent=2,
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fetchers
# ─────────────────────────────────────────────────────────────────────────────

def fetch_greenhouse(slug: str, session: requests.Session) -> list[dict]:
    time.sleep(GREENHOUSE_DELAY_S)
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    try:
        r = session.get(url, timeout=GREENHOUSE_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        log.debug(f"Greenhouse [{slug}]: {e}")
        return []

    jobs = []
    for job in r.json().get("jobs", []):
        location = (job.get("location") or {}).get("name") or "Unknown"
        jobs.append({
            "id":        f"gh-{slug}-{job['id']}",
            "title":     job.get("title", ""),
            "company":   slug.replace("-", " ").title(),
            "location":  location,
            "url":       job.get("absolute_url", ""),
            "source":    "Greenhouse",
            "posted_at": job.get("updated_at", ""),
        })
    return jobs


def fetch_ashby(slug: str, session: requests.Session) -> list[dict]:
    time.sleep(ASHBY_DELAY_S)
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    try:
        r = session.get(url, timeout=ASHBY_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        log.debug(f"Ashby [{slug}]: {e}")
        return []

    jobs = []
    for job in r.json().get("jobPostings", []):
        location = job.get("locationName") or job.get("location") or "Unknown"
        jobs.append({
            "id":        f"ashby-{slug}-{job['id']}",
            "title":     job.get("title", ""),
            "company":   slug.replace("-", " ").title(),
            "location":  location,
            "url":       job.get("jobUrl", f"https://jobs.ashbyhq.com/{slug}/{job['id']}"),
            "source":    "Ashby",
            "posted_at": job.get("publishedAt", ""),
        })
    return jobs


def fetch_all_concurrent(
    greenhouse_slugs: list[str],
    ashby_slugs: list[str],
) -> list[dict]:
    """Fetch all companies concurrently with a shared thread pool."""
    all_jobs: list[dict] = []
    total = len(greenhouse_slugs) + len(ashby_slugs)
    completed = 0
    failed = 0

    session = make_session()

    tasks = (
        [(fetch_greenhouse, slug) for slug in greenhouse_slugs]
        + [(fetch_ashby,    slug) for slug in ashby_slugs]
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fn, slug, session): slug
            for fn, slug in tasks
        }
        for future in as_completed(futures):
            completed += 1
            try:
                jobs = future.result()
                all_jobs.extend(jobs)
            except Exception as e:
                failed += 1
                log.debug(f"Worker error: {e}")

            if completed % 500 == 0 or completed == total:
                log.info(f"Progress: {completed}/{total} companies fetched ({failed} failed)")

    log.info(f"Fetch complete — {len(all_jobs)} total jobs, {failed} companies unreachable")
    return all_jobs


# ─────────────────────────────────────────────────────────────────────────────
# Filters
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(date_str: str) -> "datetime | None":
    """
    Parse any ISO 8601 timestamp into a timezone-aware datetime.
    Handles all real-world variants:
      - 2024-01-15T08:00:00Z                  (Greenhouse)
      - 2024-01-15T08:00:00.000Z              (Ashby with ms)
      - 2024-01-15T08:00:00+00:00             (offset form)
      - 2024-01-15T08:00:00.123456+05:30      (full precision + offset)
    Uses python-dateutil for robust parsing instead of manual truncation.
    """
    if not date_str:
        return None
    try:
        from dateutil import parser as dtparser
        dt = dtparser.parse(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def is_fresh(job: dict) -> bool:
    """Return True if the job is within MAX_AGE_DAYS. Always True if MAX_AGE_DAYS=0."""
    if not MAX_AGE_DAYS:
        return True
    dt = _parse_date(job.get("posted_at", ""))
    if dt is None:
        # Can't parse date — let it through rather than silently drop it
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    return dt >= cutoff


def passes_filters(job: dict) -> bool:
    title    = (job.get("title") or "").lower()
    location = (job.get("location") or "").lower()

    if not is_fresh(job):
        return False
    if INCLUDE_KEYWORDS and not any(kw.lower() in title for kw in INCLUDE_KEYWORDS):
        return False
    if EXCLUDE_KEYWORDS and any(kw.lower() in title for kw in EXCLUDE_KEYWORDS):
        return False
    if REMOTE_ONLY and "remote" not in location:
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Discord
# ─────────────────────────────────────────────────────────────────────────────

DISCORD_COLOR = {
    "Greenhouse": 0x23A559,
    "Ashby":      0x5865F2,
}


def _deliver_discord(jobs: list[dict]) -> set[str]:
    """Post new jobs to Discord. Returns set of job IDs successfully sent."""
    delivered: set[str] = set()

    if not DISCORD_WEBHOOK_URL:
        log.warning("DISCORD_WEBHOOK_URL not set — skipping Discord.")
        return delivered
    if not jobs:
        return delivered

    session = make_session()

    for i, chunk in enumerate(_chunks(jobs, 10)):
        embeds = [
            {
                "title": job["title"],
                "url":   job["url"],
                "color": DISCORD_COLOR.get(job["source"], 0x95A5A6),
                "fields": [
                    {"name": "🏢 Company",  "value": job["company"],  "inline": True},
                    {"name": "📍 Location", "value": job["location"], "inline": True},
                    {"name": "🔗 Source",   "value": job["source"],   "inline": True},
                ],
                "footer": {
                    "text": f"Posted: {job['posted_at'][:10] if job['posted_at'] else 'Unknown'}"
                },
            }
            for job in chunk
        ]

        payload = {
            "username":   "Job Scout 🤖",
            "avatar_url": "https://i.imgur.com/4M34hi2.png",
            "content":    f"🆕 **{len(jobs)} new job(s) found**" if i == 0 else "",
            "embeds":     embeds,
        }

        try:
            r = session.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
            r.raise_for_status()
            delivered.update(job["id"] for job in chunk)
            time.sleep(0.5)   # avoid Discord rate limit (50 req/s global)
        except requests.RequestException as e:
            log.error(f"Discord post failed (chunk {i}): {e}")

    log.info(f"Discord: {len(delivered)}/{len(jobs)} jobs delivered")
    return delivered


# ─────────────────────────────────────────────────────────────────────────────
# Notion
# ─────────────────────────────────────────────────────────────────────────────

NOTION_API     = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


def notion_headers() -> dict:
    return {
        "Authorization":  f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type":   "application/json",
    }


def _deliver_notion(jobs: list[dict]) -> set[str]:
    """Add one row per new job to the Notion database. Returns set of job IDs successfully written."""
    delivered: set[str] = set()

    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        log.warning("Notion credentials not set — skipping Notion.")
        return delivered

    session = make_session()
    headers = notion_headers()

    for job in jobs:
        # ── Adjust property names to match YOUR Notion database schema ──
        properties = {
            "Name": {
                "title": [{"text": {"content": job["title"]}}]
            },
            "Company": {
                "rich_text": [{"text": {"content": job["company"]}}]
            },
            "Location": {
                "rich_text": [{"text": {"content": job["location"]}}]
            },
            "URL": {
                "url": job["url"]
            },
            "Source": {
                "select": {"name": job["source"]}
            },
            "Status": {
                "select": {"name": "Inbox"}
            },
        }

        payload = {
            "parent":     {"database_id": NOTION_DATABASE_ID},
            "properties": properties,
        }

        try:
            r = session.post(
                f"{NOTION_API}/pages",
                headers=headers,
                json=payload,
                timeout=10,
            )
            r.raise_for_status()
            delivered.add(job["id"])
            time.sleep(0.34)   # Notion rate limit: ~3 req/s
        except requests.RequestException as e:
            log.error(f"Notion failed for '{job['title']}': {e}")
            if hasattr(e, "response") and e.response is not None:
                log.error(f"  Response: {e.response.text}")

    log.info(f"Notion: {len(delivered)}/{len(jobs)} rows added")
    return delivered


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("─── Job Scraper Starting ───")
    start = time.time()

    # Load company lists based on mode
    greenhouse_slugs = load_slugs(GREENHOUSE_FILE) if SCRAPER_MODE in ("greenhouse", "all") else []
    ashby_slugs      = load_slugs(ASHBY_FILE)      if SCRAPER_MODE in ("ashby", "all")      else []

    log.info(f"Mode: {SCRAPER_MODE} — {len(greenhouse_slugs)} Greenhouse, {len(ashby_slugs)} Ashby slugs")
    log.info(f"Freshness filter: {'disabled' if not MAX_AGE_DAYS else f'last {MAX_AGE_DAYS} days'}")

    if not greenhouse_slugs and not ashby_slugs:
        log.error(
            f"No slugs loaded. Check that {GREENHOUSE_FILE} and/or {ASHBY_FILE} exist."
        )
        sys.exit(1)

    # Load seen IDs
    seen = load_seen()
    log.info(f"Loaded {len(seen)} previously seen job IDs")

    # Fetch all jobs concurrently
    all_jobs = fetch_all_concurrent(greenhouse_slugs, ashby_slugs)

    # Deduplicate + filter
    unseen   = [j for j in all_jobs if j["id"] not in seen]
    fresh    = [j for j in unseen if is_fresh(j)]
    new_jobs = [j for j in fresh if passes_filters(j)]

    stale_count    = len(unseen) - len(fresh)
    filtered_count = len(fresh) - len(new_jobs)
    log.info(
        f"Unseen: {len(unseen)} | Stale (>{MAX_AGE_DAYS}d): {stale_count} | "
        f"Keyword/remote filtered: {filtered_count} | Sending: {len(new_jobs)}"
    )

    if not new_jobs:
        log.info("Nothing new. Done.")
        return

    # Deliver — mark each job seen only after it has been successfully sent
    # to at least one destination, so partial failures don't cause silent drops.
    discord_ok = _deliver_discord(new_jobs)
    notion_ok  = _deliver_notion(new_jobs)

    delivered = discord_ok | notion_ok   # union: sent to at least one destination
    failed    = set(j["id"] for j in new_jobs) - delivered

    if failed:
        log.warning(
            f"{len(failed)} job(s) failed delivery to ALL destinations — "
            "they will be retried on the next run."
        )

    seen.update(delivered)
    save_seen(seen)

    elapsed = time.time() - start
    log.info(f"─── Done in {elapsed:.0f}s. {len(new_jobs)} new job(s) dispatched. ───")


if __name__ == "__main__":
    main()