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
import re as _re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

GREENHOUSE_FILE = BASE_DIR / "greenhouse_companies.json"
ASHBY_FILE      = BASE_DIR / "ashby_companies.json"

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
NOTION_TOKEN        = os.environ.get("NOTION_TOKEN", "")
NOTION_DATABASE_ID  = os.environ.get("NOTION_DATABASE_ID", "")

SCRAPER_MODE = os.environ.get("SCRAPER_MODE", "all").lower()

# ── Concurrency & rate limiting ───────────────────────────────────────────────
MAX_WORKERS        = 5
MAX_RETRIES        = 1

GREENHOUSE_DELAY_S = 0.1
GREENHOUSE_TIMEOUT = 15

ASHBY_DELAY_S      = 0.5
ASHBY_TIMEOUT      = 90

# ── Filters ───────────────────────────────────────────────────────────────────
INCLUDE_KEYWORDS: list[str] = [
    "software", "engineer", "engineering", "developer", "machine learning",
    "ml", " ai ", "data scientist", "data engineer", "backend", "frontend",
    "full stack", "fullstack", "platform", "infrastructure", "devops",
    "mlops", "llm", "research scientist", "applied scientist",
]

EXCLUDE_KEYWORDS: list[str] = [
    "manager", "principal", "account manager", "sales", "maintenance",
    "technician", "personal", "trainer", "editor", "freelance", "mechanical",
    "physical", "hardware", "packaging", "asic", "director", "broadcast",
    "accountant", "account executive", "actuarial", "campaign manager",
    "clinical", "commercial", "compliance", "consultant", "counsel",
    "customer success", "customer support", "designer", "electrical engineer",
    "enrollment", "finance", "firmware", "gastroenterology", "head of",
    "legal", "liaison", "marketing", "medical", "propulsion", "vice president",
    "vp", "training", "supply chain",
]

REMOTE_ONLY: bool = False
US_ONLY:     bool = True
MAX_AGE_DAYS: int = 14
MAX_JOBS_PER_RUN: int = 100


# ─────────────────────────────────────────────────────────────────────────────
# US location filter — explicit non-US blocklist + US allowlist
# ─────────────────────────────────────────────────────────────────────────────

# Step 1 — reject immediately if any of these appear in the location
_NON_US = {
    "canada", "ontario", "toronto", "vancouver", "montreal", "calgary", "british columbia",
    "uk", "united kingdom", "england", "london", "manchester", "edinburgh", "glasgow",
    "germany", "berlin", "munich", "hamburg", "frankfurt",
    "france", "paris", "lyon",
    "netherlands", "amsterdam",
    "spain", "madrid", "barcelona",
    "italy", "milan", "rome",
    "sweden", "stockholm",
    "norway", "oslo",
    "denmark", "copenhagen",
    "finland", "helsinki",
    "switzerland", "zurich", "geneva",
    "austria", "vienna",
    "belgium", "brussels",
    "poland", "warsaw",
    "portugal", "lisbon",
    "ireland", "dublin",
    "india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad", "pune", "chennai",
    "singapore",
    "australia", "sydney", "melbourne", "brisbane",
    "new zealand", "auckland",
    "japan", "tokyo", "osaka",
    "china", "beijing", "shanghai", "shenzhen",
    "south korea", "seoul",
    "brazil", "são paulo", "sao paulo", "rio de janeiro",
    "mexico", "mexico city", "guadalajara",
    "argentina", "buenos aires",
    "israel", "tel aviv",
    "uae", "dubai", "abu dhabi",
    "emea", "apac", "latam", "worldwide", "global", "international", "anywhere",
}

# Step 2 — accept if any of these appear
_US_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania",
    "rhode island", "south carolina", "south dakota", "tennessee", "texas",
    "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming", "district of columbia",
}

_US_CITIES = {
    "san francisco", "new york", "los angeles", "seattle", "austin",
    "boston", "chicago", "denver", "atlanta", "miami", "dallas",
    "houston", "portland", "san jose", "san diego", "phoenix",
    "minneapolis", "detroit", "philadelphia", "brooklyn", "manhattan",
    "las vegas", "nashville", "salt lake city", "pittsburgh", "raleigh",
    "charlotte", "baltimore", "st. louis", "kansas city", "columbus",
    "indianapolis", "memphis", "louisville", "richmond", "sacramento",
    "san antonio", "el paso", "fort worth", "oklahoma city", "tucson",
    "albuquerque", "fresno", "mesa", "omaha", "cleveland", "honolulu",
    "arlington", "new orleans", "wichita", "bakersfield", "tampa",
    "sunnyvale", "santa clara", "palo alto", "menlo park", "mountain view",
    "redwood city", "bellevue", "kirkland", "redmond", "cambridge",
}

_US_ABBREVS = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga",
    "hi","id","il","in","ia","ks","ky","la","me","md",
    "ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc",
    "sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc",
}
# Match state abbreviations as standalone tokens only (not inside words)
_ABBREV_RE = _re.compile(
    r'(?<![a-z])(' + '|'.join(_US_ABBREVS) + r')(?![a-z])'
)


def _is_us_or_remote(location: str) -> bool:
    loc = location.lower().strip()

    # Blank / unknown — let through rather than silently drop
    if not loc or loc == "unknown":
        return True

    # Reject if any non-US signal present
    if any(indicator in loc for indicator in _NON_US):
        return False

    # Explicit US country terms
    if any(t in loc for t in ("united states", "u.s.", "usa", "u.s.a")):
        return True

    # Full state name
    if any(state in loc for state in _US_STATE_NAMES):
        return True

    # Major US city
    if any(city in loc for city in _US_CITIES):
        return True

    # State abbreviation as standalone token (e.g. "Austin, TX" or "Remote, CA")
    if _ABBREV_RE.search(loc):
        return True

    # Plain "remote" with no country context — accept since our company list
    # is US-focused and we already rejected all known non-US remote signals above
    if "remote" in loc:
        return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# HTTP session
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
    if not filepath.exists():
        log.warning(f"Company file not found: {filepath} — skipping")
        return []
    data = json.loads(filepath.read_text())
    if not data:
        return []
    if isinstance(data[0], str):
        return data
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

    data = r.json()

    # Debug: log the top-level keys on the first slug to catch format changes
    if not hasattr(fetch_ashby, "_logged_keys"):
        fetch_ashby._logged_keys = True
        log.info(f"Ashby response keys for '{slug}': {list(data.keys())}")
        postings = data.get("jobPostings") or data.get("results") or data.get("jobs") or []
        if postings:
            log.info(f"Ashby sample posting keys: {list(postings[0].keys())}")
        else:
            log.info(f"Ashby raw response (truncated): {str(data)[:500]}")

    jobs = []
    # Try both known key names in case Ashby changed their API
    postings = data.get("jobPostings") or data.get("results") or data.get("jobs") or []
    for job in postings:
        location = job.get("locationName") or job.get("location") or job.get("city") or "Unknown"
        jobs.append({
            "id":        f"ashby-{slug}-{job.get('id', job.get('jobId', ''))}",
            "title":     job.get("title", ""),
            "company":   slug.replace("-", " ").title(),
            "location":  location,
            "url":       job.get("jobUrl", f"https://jobs.ashbyhq.com/{slug}/{job.get('id', '')}"),
            "source":    "Ashby",
            "posted_at": job.get("publishedAt", job.get("createdAt", "")),
        })
    return jobs


def fetch_all_concurrent(greenhouse_slugs: list[str], ashby_slugs: list[str]) -> list[dict]:
    all_jobs: list[dict] = []
    total = len(greenhouse_slugs) + len(ashby_slugs)
    completed = 0
    failed = 0
    session = make_session()
    tasks = (
        [(fetch_greenhouse, slug) for slug in greenhouse_slugs]
        + [(fetch_ashby, slug) for slug in ashby_slugs]
    )
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fn, slug, session): slug for fn, slug in tasks}
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
    if not MAX_AGE_DAYS:
        return True
    dt = _parse_date(job.get("posted_at", ""))
    if dt is None:
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
    if US_ONLY and not _is_us_or_remote(location):
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Discord
# ─────────────────────────────────────────────────────────────────────────────

DISCORD_COLOR = {"Greenhouse": 0x23A559, "Ashby": 0x5865F2}


def _deliver_discord(jobs: list[dict]) -> set[str]:
    delivered: set[str] = set()
    if not DISCORD_WEBHOOK_URL:
        log.warning("DISCORD_WEBHOOK_URL not set — skipping Discord.")
        return delivered
    if not jobs:
        return delivered

    session = make_session()
    total_chunks = (len(jobs) + 9) // 10
    log.info(f"Discord: sending {len(jobs)} jobs in {total_chunks} chunks")

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
                "footer": {"text": f"Posted: {job['posted_at'][:10] if job['posted_at'] else 'Unknown'}"},
            }
            for job in chunk
        ]
        payload = {
            "username":   "Job Scout 🤖",
            "avatar_url": "https://i.imgur.com/4M34hi2.png",
            "content":    f"🆕 **{len(jobs)} new job(s) found**" if i == 0 else "",
            "embeds":     embeds,
        }
        for attempt in range(5):
            try:
                r = session.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
                if r.status_code == 429:
                    retry_after = float(r.json().get("retry_after", 5))
                    log.warning(f"Discord rate limited — waiting {retry_after:.1f}s (chunk {i})")
                    time.sleep(retry_after + 0.5)
                    continue
                r.raise_for_status()
                delivered.update(job["id"] for job in chunk)
                if i % 50 == 0:
                    log.info(f"Discord: {i}/{total_chunks} chunks sent")
                time.sleep(2)
                break
            except requests.RequestException as e:
                log.error(f"Discord post failed (chunk {i}, attempt {attempt+1}): {e}")
                time.sleep(5)
        else:
            log.error(f"Discord chunk {i} failed after 5 attempts — skipping")

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
    delivered: set[str] = set()
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        log.warning("Notion credentials not set — skipping Notion.")
        return delivered

    session = make_session()
    headers = notion_headers()

    for job in jobs:
        posted_at = job.get("posted_at", "")
        properties = {
            "Name":     {"title": [{"text": {"content": job["title"]}}]},
            "Company":  {"rich_text": [{"text": {"content": job["company"]}}]},
            "Location": {"rich_text": [{"text": {"content": job["location"]}}]},
            "URL":      {"url": job["url"]},
            "Source":   {"select": {"name": job["source"]}},
            "Status":   {"select": {"name": "Inbox"}},
        }
        if posted_at:
            properties["Date Posted"] = {"date": {"start": posted_at[:10]}}

        payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}

        for attempt in range(4):
            try:
                r = session.post(f"{NOTION_API}/pages", headers=headers, json=payload, timeout=30)
                if r.status_code == 429:
                    retry_after = float(r.headers.get("Retry-After", 10))
                    log.warning(f"Notion rate limited — waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                if r.status_code in (500, 502, 503, 504):
                    log.warning(f"Notion {r.status_code} for '{job['title']}' — retrying (attempt {attempt+1})")
                    time.sleep(5 * (attempt + 1))
                    continue
                r.raise_for_status()
                delivered.add(job["id"])
                time.sleep(0.34)
                break
            except requests.exceptions.Timeout:
                log.warning(f"Notion timeout for '{job['title']}' — retrying (attempt {attempt+1})")
                time.sleep(5 * (attempt + 1))
            except requests.RequestException as e:
                log.error(f"Notion failed for '{job['title']}': {e}")
                break
        else:
            log.error(f"Notion: '{job['title']}' failed after 4 attempts — will retry next run")

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

    greenhouse_slugs = load_slugs(GREENHOUSE_FILE) if SCRAPER_MODE in ("greenhouse", "all") else []
    ashby_slugs      = load_slugs(ASHBY_FILE)      if SCRAPER_MODE in ("ashby", "all")      else []

    log.info(f"Mode: {SCRAPER_MODE} — {len(greenhouse_slugs)} Greenhouse, {len(ashby_slugs)} Ashby slugs")
    log.info(f"Filters: age={MAX_AGE_DAYS}d, US_ONLY={US_ONLY}, REMOTE_ONLY={REMOTE_ONLY}, cap={MAX_JOBS_PER_RUN}")

    if not greenhouse_slugs and not ashby_slugs:
        log.error(f"No slugs loaded. Check that {GREENHOUSE_FILE} and/or {ASHBY_FILE} exist.")
        sys.exit(1)

    seen = load_seen()
    log.info(f"Loaded {len(seen)} previously seen job IDs")

    all_jobs = fetch_all_concurrent(greenhouse_slugs, ashby_slugs)

    unseen   = [j for j in all_jobs if j["id"] not in seen]
    fresh    = [j for j in unseen if is_fresh(j)]
    new_jobs = [j for j in fresh if passes_filters(j)]

    log.info(
        f"Unseen: {len(unseen)} | Stale: {len(unseen)-len(fresh)} | "
        f"Filtered: {len(fresh)-len(new_jobs)} | Sending: {len(new_jobs)}"
    )

    if not new_jobs:
        log.info("Nothing new. Done.")
        return

    # Sort newest first so the cap always takes the most recently posted jobs
    new_jobs.sort(
        key=lambda j: _parse_date(j.get("posted_at", "")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    if MAX_JOBS_PER_RUN and len(new_jobs) > MAX_JOBS_PER_RUN:
        log.info(f"Capping at {MAX_JOBS_PER_RUN} jobs ({len(new_jobs)-MAX_JOBS_PER_RUN} deferred)")
        new_jobs = new_jobs[:MAX_JOBS_PER_RUN]

    discord_ok = _deliver_discord(new_jobs)
    notion_ok  = _deliver_notion(new_jobs)

    delivered = discord_ok | notion_ok
    failed    = set(j["id"] for j in new_jobs) - delivered

    if failed:
        log.warning(f"{len(failed)} job(s) failed all destinations — will retry next run")

    seen.update(delivered)
    save_seen(seen)

    elapsed = time.time() - start
    log.info(f"─── Done in {elapsed:.0f}s. {len(new_jobs)} job(s) dispatched. ───")


if __name__ == "__main__":
    main()