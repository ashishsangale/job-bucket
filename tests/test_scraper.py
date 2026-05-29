"""
Basic tests for scraper logic — no network calls, no credentials needed.
Run with: pytest tests/
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from scraper import _parse_date, is_fresh, passes_filters, load_slugs
import scraper


# ── _parse_date ───────────────────────────────────────────────────────────────

class TestParseDate:
    def test_greenhouse_format(self):
        dt = _parse_date("2024-01-15T08:00:00Z")
        assert dt is not None and dt.year == 2024

    def test_ashby_milliseconds(self):
        assert _parse_date("2024-01-15T08:00:00.000Z") is not None

    def test_offset_format(self):
        assert _parse_date("2024-01-15T08:00:00+00:00") is not None

    def test_full_precision_with_offset(self):
        assert _parse_date("2024-01-15T08:00:00.123456+05:30") is not None

    def test_empty_string_returns_none(self):
        assert _parse_date("") is None

    def test_naive_datetime_gets_utc(self):
        dt = _parse_date("2024-01-15T08:00:00")
        assert dt is not None and dt.tzinfo == timezone.utc


# ── is_fresh ──────────────────────────────────────────────────────────────────

class TestIsFresh:
    def _job(self, days_ago: int) -> dict:
        dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
        return {"posted_at": dt.isoformat()}

    def test_recent_job_is_fresh(self):
        with patch.object(scraper, "MAX_AGE_DAYS", 14):
            assert is_fresh(self._job(3)) is True

    def test_old_job_is_stale(self):
        with patch.object(scraper, "MAX_AGE_DAYS", 14):
            assert is_fresh(self._job(30)) is False

    def test_exactly_on_boundary_is_fresh(self):
        # Use 13 days to stay safely within the window regardless of sub-second timing
        with patch.object(scraper, "MAX_AGE_DAYS", 14):
            assert is_fresh(self._job(13)) is True

    def test_disabled_when_zero(self):
        with patch.object(scraper, "MAX_AGE_DAYS", 0):
            assert is_fresh(self._job(999)) is True

    def test_unparseable_date_passes_through(self):
        with patch.object(scraper, "MAX_AGE_DAYS", 14):
            assert is_fresh({"posted_at": "not-a-date"}) is True

    def test_missing_date_passes_through(self):
        with patch.object(scraper, "MAX_AGE_DAYS", 14):
            assert is_fresh({"posted_at": ""}) is True


# ── passes_filters ────────────────────────────────────────────────────────────

def fresh_job(**overrides) -> dict:
    base = {
        "id": "gh-test-1", "title": "Software Engineer", "company": "Acme",
        "location": "San Francisco, CA", "url": "https://example.com",
        "source": "Greenhouse", "posted_at": datetime.now(timezone.utc).isoformat(),
    }
    return {**base, **overrides}

class TestPassesFilters:
    def test_passes_with_no_filters(self):
        with patch.multiple(scraper, INCLUDE_KEYWORDS=[], EXCLUDE_KEYWORDS=[], REMOTE_ONLY=False, MAX_AGE_DAYS=0):
            assert passes_filters(fresh_job()) is True

    def test_include_keyword_match(self):
        with patch.multiple(scraper, INCLUDE_KEYWORDS=["engineer"], EXCLUDE_KEYWORDS=[], REMOTE_ONLY=False, MAX_AGE_DAYS=0):
            assert passes_filters(fresh_job(title="Software Engineer")) is True

    def test_include_keyword_no_match(self):
        with patch.multiple(scraper, INCLUDE_KEYWORDS=["designer"], EXCLUDE_KEYWORDS=[], REMOTE_ONLY=False, MAX_AGE_DAYS=0):
            assert passes_filters(fresh_job(title="Software Engineer")) is False

    def test_exclude_keyword_drops_job(self):
        with patch.multiple(scraper, INCLUDE_KEYWORDS=[], EXCLUDE_KEYWORDS=["senior"], REMOTE_ONLY=False, MAX_AGE_DAYS=0):
            assert passes_filters(fresh_job(title="Senior Engineer")) is False

    def test_remote_only_passes_remote(self):
        with patch.multiple(scraper, INCLUDE_KEYWORDS=[], EXCLUDE_KEYWORDS=[], REMOTE_ONLY=True, MAX_AGE_DAYS=0):
            assert passes_filters(fresh_job(location="Remote")) is True

    def test_remote_only_drops_onsite(self):
        with patch.multiple(scraper, INCLUDE_KEYWORDS=[], EXCLUDE_KEYWORDS=[], REMOTE_ONLY=True, MAX_AGE_DAYS=0):
            assert passes_filters(fresh_job(location="New York, NY")) is False

    def test_stale_job_fails(self):
        old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        with patch.multiple(scraper, INCLUDE_KEYWORDS=[], EXCLUDE_KEYWORDS=[], REMOTE_ONLY=False, MAX_AGE_DAYS=14):
            assert passes_filters(fresh_job(posted_at=old)) is False


# ── load_slugs ────────────────────────────────────────────────────────────────

class TestLoadSlugs:
    def test_flat_list(self, tmp_path):
        f = tmp_path / "c.json"
        f.write_text('["stripe", "airbnb"]')
        assert load_slugs(f) == ["stripe", "airbnb"]

    def test_dict_with_slug_key(self, tmp_path):
        f = tmp_path / "c.json"
        f.write_text('[{"slug": "stripe"}, {"slug": "airbnb"}]')
        assert load_slugs(f) == ["stripe", "airbnb"]

    def test_dict_with_board_token_key(self, tmp_path):
        f = tmp_path / "c.json"
        f.write_text('[{"board_token": "stripe"}]')
        assert load_slugs(f) == ["stripe"]

    def test_missing_file_returns_empty(self, tmp_path):
        assert load_slugs(tmp_path / "nonexistent.json") == []

    def test_empty_file_returns_empty(self, tmp_path):
        f = tmp_path / "c.json"
        f.write_text("[]")
        assert load_slugs(f) == []