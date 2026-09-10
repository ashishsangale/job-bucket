"""
Digest + Streak — reduces doom-scrolling and gives a visible momentum signal.

Two modes, run on separate schedules via GitHub Actions:

  python digest.py digest   -> posts a compact "top N fresh matches" digest
                                to Discord at a fixed time, instead of relying
                                on constant real-time pings.
  python digest.py streak   -> reads your Notion tracker (Status = Applied)
                                and posts your current daily-application
                                streak + this week's count. Pure momentum
                                signal, decoupled from interview/offer outcomes.

Both reuse the same Notion database the scraper already writes to — no new
schema required, just an existing "Status" select property.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
NOTION_TOKEN       = os.environ.get("NOTION_TOKEN", "")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "")
NOTION_API         = "https://api.notion.com/v1"
NOTION_VERSION     = "2022-06-28"

DISCORD_WEBHOOK_URL        = os.environ.get("DISCORD_WEBHOOK_URL", "")
DIGEST_DISCORD_WEBHOOK_URL = os.environ.get("DIGEST_DISCORD_WEBHOOK_URL", "")

# "Status" select value that means "I applied". Configurable because this repo
# doesn't control your Notion workflow — change via env if yours differs.
APPLIED_STATUS_NAME = os.environ.get("APPLIED_STATUS_NAME", "Applied")
INBOX_STATUS_NAME   = os.environ.get("INBOX_STATUS_NAME", "Inbox")

DIGEST_LOOKBACK_HOURS = int(os.environ.get("DIGEST_LOOKBACK_HOURS", "12"))
DIGEST_MAX_ITEMS      = int(os.environ.get("DIGEST_MAX_ITEMS", "8"))
STREAK_LOOKBACK_DAYS  = int(os.environ.get("STREAK_LOOKBACK_DAYS", "60"))


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def notion_headers() -> dict:
    return {
        "Authorization":  f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type":   "application/json",
    }


def notion_query_all(filter_obj: dict, session: requests.Session) -> list[dict]:
    """Query the Notion database, following pagination, returning all matching pages."""
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        log.warning("Notion credentials not set — returning no pages.")
        return []

    pages: list[dict] = []
    body: dict = {"filter": filter_obj, "page_size": 100}
    while True:
        try:
            r = session.post(
                f"{NOTION_API}/databases/{NOTION_DATABASE_ID}/query",
                headers=notion_headers(), json=body, timeout=30,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            log.error(f"Notion query failed: {e}")
            break
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        body["start_cursor"] = data.get("next_cursor")
    return pages


def _plain_text(prop: dict) -> str:
    rich = prop.get("title") or prop.get("rich_text") or []
    return "".join(t.get("plain_text", "") for t in rich)


# ─────────────────────────────────────────────────────────────────────────────
# Digest — curated shortlist instead of a firehose
# ─────────────────────────────────────────────────────────────────────────────

def get_recent_inbox_jobs(session: requests.Session) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=DIGEST_LOOKBACK_HOURS)).isoformat()
    filter_obj = {
        "and": [
            {"property": "Status", "select": {"equals": INBOX_STATUS_NAME}},
            {"timestamp": "created_time", "created_time": {"after": cutoff}},
        ]
    }
    pages = notion_query_all(filter_obj, session)

    jobs = []
    for page in pages:
        props = page.get("properties", {})
        jobs.append({
            "title":      _plain_text(props.get("Name", {})),
            "company":    _plain_text(props.get("Company", {})),
            "location":   _plain_text(props.get("Location", {})),
            "url":        (props.get("URL") or {}).get("url", ""),
            "source":     ((props.get("Source") or {}).get("select") or {}).get("name", ""),
            "created_at": page.get("created_time", ""),
        })
    # Newest first
    jobs.sort(key=lambda j: j["created_at"], reverse=True)
    return jobs


def post_digest(jobs: list[dict]) -> bool:
    webhook = DIGEST_DISCORD_WEBHOOK_URL or DISCORD_WEBHOOK_URL
    if not webhook:
        log.warning("No Discord webhook set for digest — skipping.")
        return False
    if not jobs:
        log.info("No jobs in digest window — nothing to send.")
        return True

    shortlist = jobs[:DIGEST_MAX_ITEMS]
    embeds = [
        {
            "title": job["title"] or "Untitled role",
            "url":   job["url"] or None,
            "fields": [
                {"name": "🏢 Company",  "value": job["company"] or "Unknown",  "inline": True},
                {"name": "📍 Location", "value": job["location"] or "Unknown", "inline": True},
                {"name": "🔗 Source",   "value": job["source"] or "Unknown",   "inline": True},
            ],
        }
        for job in shortlist
    ]
    remainder = len(jobs) - len(shortlist)
    content = f"📋 **Daily digest — {len(shortlist)} focused match(es)**"
    if remainder > 0:
        content += f" (+{remainder} more in Notion)"

    payload = {
        "username": "Job Digest 📋",
        "content":  content,
        "embeds":   embeds,
    }
    session = make_session()
    try:
        r = session.post(webhook, json=payload, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        log.error(f"Digest post failed: {e}")
        return False
    log.info(f"Digest sent: {len(shortlist)} job(s) ({remainder} deferred)")
    return True


def run_digest() -> None:
    session = make_session()
    jobs = get_recent_inbox_jobs(session)
    post_digest(jobs)


# ─────────────────────────────────────────────────────────────────────────────
# Streak — visible momentum, independent of interview/offer outcomes
# ─────────────────────────────────────────────────────────────────────────────

def get_applied_dates(session: requests.Session) -> set[str]:
    """Return the set of UTC calendar dates (YYYY-MM-DD) on which a page's
    Status was last edited to APPLIED_STATUS_NAME, within the lookback window.

    Uses last_edited_time as a proxy for "date applied" — accurate as long as
    you mark a role Applied on the same day you apply, which is the normal flow.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=STREAK_LOOKBACK_DAYS)).isoformat()
    filter_obj = {
        "and": [
            {"property": "Status", "select": {"equals": APPLIED_STATUS_NAME}},
            {"timestamp": "last_edited_time", "last_edited_time": {"after": cutoff}},
        ]
    }
    pages = notion_query_all(filter_obj, session)
    dates: set[str] = set()
    for page in pages:
        edited = page.get("last_edited_time", "")
        if edited:
            dates.add(edited[:10])  # YYYY-MM-DD
    return dates


def compute_streak(applied_dates: set[str], today: "datetime.date | None" = None) -> int:
    """Current consecutive-day streak ending today or yesterday.

    A day without an application breaks the streak, but if today has no
    entry *yet* (you haven't applied yet today), the streak isn't broken
    until tomorrow — so we check backward from today, falling back to
    yesterday if today has nothing recorded.
    """
    today = today or datetime.now(timezone.utc).date()
    day = today
    if day.isoformat() not in applied_dates:
        day = today - timedelta(days=1)

    streak = 0
    while day.isoformat() in applied_dates:
        streak += 1
        day -= timedelta(days=1)
    return streak


def compute_week_count(applied_dates: set[str], today: "datetime.date | None" = None) -> int:
    """Count of distinct applied-days in the trailing 7 days (including today)."""
    today = today or datetime.now(timezone.utc).date()
    window = {(today - timedelta(days=i)).isoformat() for i in range(7)}
    return len(applied_dates & window)


def post_streak(streak: int, week_count: int) -> bool:
    webhook = DIGEST_DISCORD_WEBHOOK_URL or DISCORD_WEBHOOK_URL
    if not webhook:
        log.warning("No Discord webhook set for streak — skipping.")
        return False

    if streak == 0:
        content = "🌱 No active streak yet — applying today starts a new one."
    else:
        content = f"🔥 **{streak}-day streak** — {week_count} day(s) with applications this week."

    payload = {"username": "Momentum Tracker 🔥", "content": content}
    session = make_session()
    try:
        r = session.post(webhook, json=payload, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        log.error(f"Streak post failed: {e}")
        return False
    log.info(f"Streak posted: {streak} days, {week_count} active day(s) this week")
    return True


def run_streak() -> None:
    session = make_session()
    applied_dates = get_applied_dates(session)
    streak = compute_streak(applied_dates)
    week_count = compute_week_count(applied_dates)
    post_streak(streak, week_count)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "digest"
    if mode == "digest":
        run_digest()
    elif mode == "streak":
        run_streak()
    else:
        log.error(f"Unknown mode {mode!r}. Use 'digest' or 'streak'.")
        sys.exit(1)


if __name__ == "__main__":
    main()
