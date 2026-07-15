"""
Job Board Scraper — Greenhouse + Ashby + Lever
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
LEVER_FILE      = BASE_DIR / "lever_companies.json"

WORKDAY_FILE         = BASE_DIR / "workday.json"
WORKDAY_DELAY_S      = 0.5
WORKDAY_TIMEOUT      = 30
WORKDAY_MAX_PAGES    = 5
WORKDAY_SEARCH_TEXT  = "software engineer"
WORKDAY_BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", "1000"))
WORKDAY_BATCH_OFFSET = int(os.environ.get("BATCH_OFFSET", "0"))

DISCORD_WEBHOOK_URL          = os.environ.get("DISCORD_WEBHOOK_URL", "")
PRIORITY_DISCORD_WEBHOOK_URL = os.environ.get("PRIORITY_DISCORD_WEBHOOK_URL", "")
NOTION_TOKEN                 = os.environ.get("NOTION_TOKEN", "")
NOTION_DATABASE_ID           = os.environ.get("NOTION_DATABASE_ID", "")

SCRAPER_MODE = os.environ.get("SCRAPER_MODE", "all").lower()

PRIORITY_FILE = BASE_DIR / "priority_companies.json"
AMAZON_DELAY_S = 0.5
AMAZON_TIMEOUT = 30
AMAZON_MAX_RESULTS = 100

APPLE_DELAY_S = 0.5
APPLE_TIMEOUT = 30
APPLE_MAX_PAGES = 5

ICIMS_FILE = BASE_DIR / "icims_companies.json"
ICIMS_DELAY_S = 0.5
ICIMS_TIMEOUT = 30

# ── Concurrency & rate limiting ───────────────────────────────────────────────
MAX_WORKERS        = 5
MAX_RETRIES        = 1

GREENHOUSE_DELAY_S = 0.1
GREENHOUSE_TIMEOUT = 15

ASHBY_DELAY_S      = 0.5
ASHBY_TIMEOUT      = 90
LEVER_DELAY_S      = 0.2
LEVER_TIMEOUT      = 20

# ── Filters ───────────────────────────────────────────────────────────────────
INCLUDE_KEYWORDS: list[str] = [
    "software", "engineer", "engineering", "developer", "machine learning",
    "ml", " ai ", "data scientist", "data engineer", "backend", "frontend",
    "full stack", "fullstack", "platform", "infrastructure", "devops",
    "mlops", "llm", "research scientist", "applied scientist", "forward deployed"
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
    "vp", "training", "supply chain", "facilities", "manufacturing", "senior",
    "construction", "civil", "embedded", "architect", "chief", "design", "lead",
    "fulfillment", "controls", "process", "cae", "connectivity", "copv", "electrical",
    "wastewater", "water", "avionics", "structural", "roadway", "transportation",
    "roadway", "hydraulics", "bridge", "controls", "electromagnetic", "thermal",
    "part-time", "thermo", "buyer", "supplier", "electronics", "fire", "building",
    "environmental", "bioinformetics", "ts/sci", "fluid", "welding", "commissioning",
    "chemical", "purity", 
]

REMOTE_ONLY: bool = False
US_ONLY:     bool = True
MAX_AGE_DAYS: int = 7
MAX_JOBS_PER_RUN: int = 500


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
    "emea", "apac", "latam", "worldwide", "global", "international", "anywhere", "pakistan",
    "prague", "valencia", "europe", "johannesburg", "latvia", "romania", "bulgaria", "estonia",
    "lithuania", "jerusalem", "quebec", "nigeria", "deutschland", "bogota", "colombia", "cyprus",
    "armenia", "turkey", "serbia", "costa rica", 
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
        allowed_methods=["GET", "POST"],
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


def load_workday_entries(filepath: Path) -> list[tuple[str, str, str]]:
    """Load Workday company entries from a JSON file of pipe-delimited strings."""
    if not filepath.exists():
        log.warning(f"Workday file not found: {filepath} — returning empty list")
        return []
    try:
        data = json.loads(filepath.read_text())
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning(f"Failed to parse JSON from {filepath}: {exc}")
        return []
    if not data:
        log.warning(f"Workday file is empty: {filepath}")
        return []
    entries: list[tuple[str, str, str]] = []
    for raw in data:
        parts = str(raw).split("|")
        if len(parts) != 3:
            log.warning(f"Malformed workday entry (expected 2 pipes): {raw!r}")
            continue
        company, version, site_id = (p.strip() for p in parts)
        if not company or not version or not site_id:
            log.warning(f"Workday entry has blank field: {raw!r}")
            continue
        entries.append((company, version, site_id))
    return entries


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


def _normalize_posted_at(value) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        if value > 10**12:
            value = value / 1000
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    return str(value)


def fetch_lever(slug: str, session: requests.Session) -> list[dict]:
    time.sleep(LEVER_DELAY_S)
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        r = session.get(url, timeout=LEVER_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        log.debug(f"Lever [{slug}]: {e}")
        return []

    data = r.json()
    postings = data.get("postings") if isinstance(data, dict) else data
    if postings is None:
        postings = []

    jobs = []
    for job in postings:
        categories = job.get("categories") or {}
        location = categories.get("location") or job.get("location") or "Unknown"
        hosted_url = job.get("hostedUrl") or job.get("hosted_url") or job.get("applyUrl") or job.get("apply_url") or ""
        posted_at = _normalize_posted_at(job.get("createdAt") or job.get("created_at") or job.get("updatedAt") or job.get("updated_at"))
        job_id = job.get("id") or job.get("leverId") or hosted_url or job.get("text") or ""
        jobs.append({
            "id":        f"lever-{slug}-{job_id}",
            "title":     job.get("text", job.get("title", "")),
            "company":   slug.replace("-", " ").title(),
            "location":  location,
            "url":       hosted_url,
            "source":    "Lever",
            "posted_at": posted_at,
        })
    return jobs


def _parse_workday_posted_on(posted_on: str) -> str:
    """Convert Workday's human-readable 'postedOn' to an ISO date string.

    Examples: "Posted 3 Days Ago", "Posted 30+ Days Ago", "Posted Yesterday",
              "Posted Today", "Posted 1 Day Ago"
    Returns an ISO date string (YYYY-MM-DD) or empty string if unparseable.
    """
    if not posted_on:
        return ""
    text = posted_on.lower().strip()
    today = datetime.now(timezone.utc).date()

    if "today" in text:
        return today.isoformat()
    if "yesterday" in text:
        return (today - timedelta(days=1)).isoformat()

    # Match patterns like "3 days ago", "30+ days ago", "1 day ago"
    m = _re.search(r"(\d+)\+?\s*days?\s*ago", text)
    if m:
        days = int(m.group(1))
        return (today - timedelta(days=days)).isoformat()

    return ""


def fetch_workday(entry: tuple[str, str, str], session: requests.Session) -> list[dict]:
    """Fetch job postings from Workday API for a single company/site with pagination."""
    company, version, site_id = entry
    url = f"https://{company}.{version}.myworkdayjobs.com/wday/cxs/{company}/{site_id}/jobs"
    jobs: list[dict] = []

    for page in range(WORKDAY_MAX_PAGES):
        if page > 0:
            time.sleep(WORKDAY_DELAY_S)

        offset = page * 20
        body = {"limit": 20, "offset": offset, "searchText": WORKDAY_SEARCH_TEXT}

        try:
            r = session.post(url, json=body, timeout=WORKDAY_TIMEOUT)
            r.raise_for_status()
        except requests.RequestException as e:
            if page == 0:
                log.debug(f"Workday [{company}/{site_id}]: {e}")
                return []
            else:
                log.warning(f"Workday [{company}/{site_id}] page {page} failed: {e}")
                return jobs  # Return partial results from successful pages

        data = r.json()
        postings = data.get("jobPostings", [])

        if not postings:
            break

        for posting in postings:
            external_path = posting.get("externalPath", "")
            posted_at = posting.get("startDate", "") or _parse_workday_posted_on(posting.get("postedOn", ""))
            jobs.append({
                "id":        f"wd-{company}-{site_id}-{external_path}",
                "title":     posting.get("title", ""),
                "company":   company.replace("-", " ").title(),
                "location":  posting.get("locationsText", "Unknown"),
                "url":       f"https://{company}.{version}.myworkdayjobs.com/{site_id}{external_path}",
                "source":    "Workday",
                "posted_at": posted_at,
            })

        total = data.get("total", 0)
        if total <= offset + 20:
            break

        if page == WORKDAY_MAX_PAGES - 1:
            log.warning(
                f"Workday [{company}/{site_id}]: max pages ({WORKDAY_MAX_PAGES}) reached"
            )

    return jobs


AMAZON_DEFAULT_QUERIES = ["software engineer", "software development engineer"]


def fetch_amazon(session: requests.Session, queries: list[str] | None = None) -> list[dict]:
    """Fetch software engineering jobs from Amazon's public JSON API.

    Runs one search per query term and merges results, deduplicating by id.
    Amazon ranks by relevance, so multiple targeted queries (e.g. "software
    engineer" and "software development engineer") capture role variants that
    a single query would rank out of the result window.
    """
    queries = queries or AMAZON_DEFAULT_QUERIES
    url = "https://www.amazon.jobs/en/search.json"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
    by_id: dict[str, dict] = {}

    for i, query in enumerate(queries):
        if i > 0:
            time.sleep(AMAZON_DELAY_S)
        params = {
            "base_query": query,
            "country": "USA",
            "offset": 0,
            "result_limit": AMAZON_MAX_RESULTS,
        }
        try:
            r = session.get(url, params=params, timeout=AMAZON_TIMEOUT, headers=headers)
            r.raise_for_status()
        except requests.RequestException as e:
            log.debug(f"Amazon [{query!r}]: {e}")
            continue

        data = r.json()
        for job in data.get("jobs", []):
            posted_date = job.get("posted_date", "")
            # Parse human-readable date like "September 25, 2025" to ISO
            parsed_date = ""
            if posted_date:
                try:
                    from dateutil import parser as dtparser
                    dt = dtparser.parse(posted_date)
                    parsed_date = dt.strftime("%Y-%m-%d")
                except Exception:
                    parsed_date = posted_date

            job_path = job.get("job_path", "")
            job_id = job.get("id_icims") or job.get("id", "")
            record_id = f"amzn-{job_id}"
            by_id[record_id] = {
                "id":        record_id,
                "title":     job.get("title", ""),
                "company":   "Amazon",
                "location":  job.get("normalized_location") or job.get("location") or "Unknown",
                "url":       f"https://www.amazon.jobs{job_path}" if job_path else "",
                "source":    "Amazon",
                "posted_at": parsed_date,
            }

    jobs = list(by_id.values())
    log.info(f"Amazon: fetched {len(jobs)} jobs across {len(queries)} queries")
    return jobs


APPLE_DEFAULT_QUERIES = ["software engineer"]


def fetch_apple(session: requests.Session, queries: list[str] | None = None) -> list[dict]:
    """Fetch software engineering jobs from Apple's careers site.

    Apple's search page server-renders job data as JSON embedded in
    `window.__staticRouterHydrationData`. We parse that rather than
    calling the authenticated API.

    Runs one paginated search per query term and merges results,
    deduplicating by reqId.
    """
    queries = queries or APPLE_DEFAULT_QUERIES
    base = "https://jobs.apple.com/en-us/search"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }
    by_id: dict[str, dict] = {}
    first_request = True

    for query in queries:
        for page in range(1, APPLE_MAX_PAGES + 1):
            if not first_request:
                time.sleep(APPLE_DELAY_S)
            first_request = False

            params = {
                "search": query,
                "location": "united-states-USA",
                "sort": "newest",
                "page": page,
            }
            try:
                r = session.get(base, params=params, headers=headers, timeout=APPLE_TIMEOUT)
                r.raise_for_status()
            except requests.RequestException as e:
                log.warning(f"Apple [{query!r}] page {page} failed: {e}")
                break

            m = _re.search(r'window\.__staticRouterHydrationData = JSON\.parse\("(.*?)"\);', r.text)
            if not m:
                log.warning(f"Apple: could not find embedded job data on page {page}")
                break

            try:
                decoded = json.loads('"' + m.group(1) + '"')
                data = json.loads(decoded)
                results = data["loaderData"]["search"]["searchResults"]
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                log.warning(f"Apple: failed to parse embedded data on page {page}: {e}")
                break

            if not results:
                break

            for job in results:
                req_id = job.get("reqId", "")
                slug = job.get("transformedPostingTitle", "")
                team = job.get("team") or {}
                team_code = team.get("teamCode", "")
                locations = job.get("locations") or []
                location = ", ".join(l.get("name", "") for l in locations if l.get("name")) or "Unknown"
                url = f"https://jobs.apple.com/en-us/details/{req_id}/{slug}"
                if team_code:
                    url += f"?team={team_code}"
                by_id[f"apple-{req_id}"] = {
                    "id":        f"apple-{req_id}",
                    "title":     job.get("postingTitle", ""),
                    "company":   "Apple",
                    "location":  location,
                    "url":       url,
                    "source":    "Apple",
                    "posted_at": job.get("postDateInGMT", ""),
                }

    jobs = list(by_id.values())
    log.info(f"Apple: fetched {len(jobs)} jobs across {len(queries)} queries")
    return jobs


def fetch_icims(company: str, session: requests.Session) -> list[dict]:
    """Fetch jobs from an iCIMS career site via its sitemap.xml.

    iCIMS has no public JSON API, but every board exposes a sitemap listing
    all job URLs with `lastmod` timestamps. One request yields job id, URL,
    a title slug, and a posted/updated date — no pagination or per-job
    detail fetches required.

    `company` is the iCIMS subdomain, e.g. "careers-bcore" for
    https://careers-bcore.icims.com
    """
    import xml.etree.ElementTree as ET

    time.sleep(ICIMS_DELAY_S)
    url = f"https://{company}.icims.com/sitemap.xml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/xml",
    }
    try:
        r = session.get(url, headers=headers, timeout=ICIMS_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        log.debug(f"iCIMS [{company}]: {e}")
        return []

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        log.warning(f"iCIMS [{company}]: failed to parse sitemap: {e}")
        return []

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    jobs: list[dict] = []
    # URL pattern: https://{company}.icims.com/jobs/{id}/{title-slug}/job
    job_re = _re.compile(r"/jobs/(\d+)/([^/]+)/job")

    for url_el in root.findall("ns:url", ns):
        loc_el = url_el.find("ns:loc", ns)
        if loc_el is None or not loc_el.text:
            continue
        loc = loc_el.text
        m = job_re.search(loc)
        if not m:
            continue  # skip non-job URLs like /jobs/intro
        job_id, slug = m.group(1), m.group(2)
        title = slug.replace("-", " ").title()

        lastmod_el = url_el.find("ns:lastmod", ns)
        posted_at = _normalize_posted_at(lastmod_el.text) if lastmod_el is not None else ""

        jobs.append({
            "id":        f"icims-{company}-{job_id}",
            "title":     title,
            "company":   company.replace("careers-", "").replace("-", " ").title(),
            "location":  "Unknown",
            "url":       f"{loc}?in_iframe=1",
            "source":    "iCIMS",
            "posted_at": posted_at,
        })

    log.info(f"iCIMS [{company}]: fetched {len(jobs)} jobs")
    return jobs


def load_priority_companies(filepath: Path) -> list[dict]:
    """Load priority company entries from JSON file."""
    if not filepath.exists():
        log.warning(f"Priority file not found: {filepath}")
        return []
    try:
        data = json.loads(filepath.read_text())
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning(f"Failed to parse priority file {filepath}: {exc}")
        return []
    return data if isinstance(data, list) else []


def fetch_priority(entries: list[dict], session: requests.Session) -> list[dict]:
    """Fetch jobs for priority companies, dispatching to the correct fetcher."""
    all_jobs: list[dict] = []
    for entry in entries:
        source = entry.get("source", "").lower()
        slug = entry.get("slug", "")
        if not slug:
            continue
        if source == "ashby":
            jobs = fetch_ashby(slug, session)
        elif source == "greenhouse":
            jobs = fetch_greenhouse(slug, session)
        elif source == "lever":
            jobs = fetch_lever(slug, session)
        elif source == "workday":
            # Expect slug in pipe-delimited format: "company|version|site_id"
            parts = slug.split("|")
            if len(parts) == 3:
                jobs = fetch_workday(tuple(parts), session)
            else:
                log.warning(f"Priority workday entry malformed: {slug}")
                continue
        elif source == "amazon":
            jobs = fetch_amazon(session, entry.get("queries"))
        elif source == "apple":
            jobs = fetch_apple(session, entry.get("queries"))
        elif source == "icims":
            jobs = fetch_icims(slug, session)
        else:
            log.warning(f"Unknown source '{source}' for priority entry: {slug}")
            continue
        all_jobs.extend(jobs)
    return all_jobs


def _deliver_priority_discord(jobs: list[dict]) -> set[str]:
    """Deliver jobs to the priority Discord channel with @here ping."""
    delivered: set[str] = set()
    webhook = PRIORITY_DISCORD_WEBHOOK_URL or DISCORD_WEBHOOK_URL
    if not webhook:
        log.warning("No Discord webhook set for priority — skipping.")
        return delivered
    if not jobs:
        return delivered

    session = make_session()
    total_chunks = (len(jobs) + 9) // 10
    log.info(f"Priority Discord: sending {len(jobs)} jobs in {total_chunks} chunks")

    for i, chunk in enumerate(_chunks(jobs, 10)):
        embeds = [
            {
                "title": f"🚨 {job['title']}",
                "url":   job["url"],
                "color": DISCORD_COLOR.get(job["source"], 0xFF4500),
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
            "username":   "Priority Job Alert 🚨",
            "avatar_url": "https://i.imgur.com/4M34hi2.png",
            "content":    f"@here 🚨 **{len(jobs)} new priority job(s) found!**" if i == 0 else "",
            "embeds":     embeds,
        }
        for attempt in range(5):
            try:
                r = session.post(webhook, json=payload, timeout=10)
                if r.status_code == 429:
                    retry_after = float(r.json().get("retry_after", 5))
                    log.warning(f"Priority Discord rate limited — waiting {retry_after:.1f}s")
                    time.sleep(retry_after + 0.5)
                    continue
                r.raise_for_status()
                delivered.update(job["id"] for job in chunk)
                time.sleep(2)
                break
            except requests.RequestException as e:
                log.error(f"Priority Discord failed (chunk {i}, attempt {attempt+1}): {e}")
                time.sleep(5)
        else:
            log.error(f"Priority Discord chunk {i} failed after 5 attempts — skipping")

    log.info(f"Priority Discord: {len(delivered)}/{len(jobs)} jobs delivered")
    return delivered


def fetch_all_concurrent(
    greenhouse_slugs: list[str],
    ashby_slugs: list[str],
    lever_slugs: list[str],
    workday_entries: list[tuple[str, str, str]] | None = None,
    icims_slugs: list[str] | None = None,
) -> list[dict]:
    all_jobs: list[dict] = []
    workday_entries = workday_entries or []
    icims_slugs = icims_slugs or []
    total = (len(greenhouse_slugs) + len(ashby_slugs) + len(lever_slugs)
             + len(workday_entries) + len(icims_slugs))
    completed = 0
    failed = 0
    session = make_session()
    tasks = (
        [(fetch_greenhouse, slug) for slug in greenhouse_slugs]
        + [(fetch_ashby, slug) for slug in ashby_slugs]
        + [(fetch_lever, slug) for slug in lever_slugs]
        + [(fetch_workday, entry) for entry in workday_entries]
        + [(fetch_icims, slug) for slug in icims_slugs]
    )
    log.info(
        f"Starting concurrent fetch: {len(greenhouse_slugs)} Greenhouse, "
        f"{len(ashby_slugs)} Ashby, {len(lever_slugs)} Lever, "
        f"{len(workday_entries)} Workday, {len(icims_slugs)} iCIMS — {total} total"
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

DISCORD_COLOR = {
    "Greenhouse": 0x23A559,
    "Ashby":      0x5865F2,
    "Lever":      0xF59E0B,
    "Workday":    0xE34F26,  # Orange — Workday brand color
    "Amazon":     0xFF9900,  # Amazon orange
    "Apple":      0xA2AAAD,  # Apple silver
    "iCIMS":      0x00A0DF,  # iCIMS blue
}


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
            properties["Date Posted"] = {"date": {"start": posted_at}}

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

    # Validate SCRAPER_MODE
    valid_modes = ("workday", "greenhouse", "ashby", "lever", "icims", "all", "priority")
    if SCRAPER_MODE not in valid_modes:
        log.error(f"Unrecognized SCRAPER_MODE={SCRAPER_MODE!r}. Must be one of: {', '.join(valid_modes)}")
        sys.exit(1)

    # ── Priority mode: fast check for priority companies only ──
    if SCRAPER_MODE == "priority":
        priority_entries = load_priority_companies(PRIORITY_FILE)
        if not priority_entries:
            log.error(f"No priority companies loaded from {PRIORITY_FILE}")
            sys.exit(1)

        log.info(f"Priority mode — checking {len(priority_entries)} companies")
        session = make_session()
        all_jobs = fetch_priority(priority_entries, session)

        seen = load_seen()
        log.info(f"Loaded {len(seen)} previously seen job IDs")

        unseen   = [j for j in all_jobs if j["id"] not in seen]
        fresh    = [j for j in unseen if is_fresh(j)]
        new_jobs = [j for j in fresh if passes_filters(j)]

        log.info(
            f"Unseen: {len(unseen)} | Stale: {len(unseen)-len(fresh)} | "
            f"Filtered: {len(fresh)-len(new_jobs)} | Sending: {len(new_jobs)}"
        )

        if not new_jobs:
            log.info("No new priority jobs. Done.")
            elapsed = time.time() - start
            log.info(f"─── Done in {elapsed:.0f}s ───")
            return

        # Sort newest first
        new_jobs.sort(
            key=lambda j: _parse_date(j.get("posted_at", "")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

        # Deliver to priority Discord channel
        discord_ok = _deliver_priority_discord(new_jobs)
        # Also deliver to Notion
        notion_ok = _deliver_notion(new_jobs)

        delivered = discord_ok | notion_ok
        seen.update(delivered)
        save_seen(seen)

        elapsed = time.time() - start
        log.info(f"─── Done in {elapsed:.0f}s. {len(new_jobs)} priority job(s) dispatched. ───")
        return

    greenhouse_slugs = load_slugs(GREENHOUSE_FILE) if SCRAPER_MODE in ("greenhouse", "all") else []
    ashby_slugs      = load_slugs(ASHBY_FILE)      if SCRAPER_MODE in ("ashby", "all")      else []
    lever_slugs      = load_slugs(LEVER_FILE)      if SCRAPER_MODE in ("lever", "all")      else []
    icims_slugs      = load_slugs(ICIMS_FILE)      if SCRAPER_MODE in ("icims", "all")      else []

    # Load workday entries when mode is "workday" or "all"
    workday_entries = load_workday_entries(WORKDAY_FILE) if SCRAPER_MODE in ("workday", "all") else []

    # Apply batch slicing with wrap-around
    if workday_entries:
        total_entries = len(workday_entries)
        offset = WORKDAY_BATCH_OFFSET % total_entries if total_entries > 0 else 0
        end = offset + WORKDAY_BATCH_SIZE
        if end <= total_entries:
            workday_entries = workday_entries[offset:end]
        else:
            workday_entries = workday_entries[offset:] + workday_entries[:end - total_entries]
        # Cap to WORKDAY_BATCH_SIZE in case wrap-around gives more
        workday_entries = workday_entries[:WORKDAY_BATCH_SIZE]

    log.info(
        f"Mode: {SCRAPER_MODE} — {len(greenhouse_slugs)} Greenhouse, "
        f"{len(ashby_slugs)} Ashby, {len(lever_slugs)} Lever slugs, "
        f"{len(workday_entries)} Workday entries, {len(icims_slugs)} iCIMS"
    )
    log.info(f"Filters: age={MAX_AGE_DAYS}d, US_ONLY={US_ONLY}, REMOTE_ONLY={REMOTE_ONLY}, cap={MAX_JOBS_PER_RUN}")

    if not greenhouse_slugs and not ashby_slugs and not lever_slugs and not workday_entries and not icims_slugs:
        log.error(f"No slugs/entries loaded. Check that {GREENHOUSE_FILE}, {ASHBY_FILE}, {LEVER_FILE}, {WORKDAY_FILE}, and/or {ICIMS_FILE} exist.")
        sys.exit(1)

    seen = load_seen()
    log.info(f"Loaded {len(seen)} previously seen job IDs")

    all_jobs = fetch_all_concurrent(greenhouse_slugs, ashby_slugs, lever_slugs, workday_entries=workday_entries, icims_slugs=icims_slugs)

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