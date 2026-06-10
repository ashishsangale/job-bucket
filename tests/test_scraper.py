"""
Basic tests for scraper logic — no network calls, no credentials needed.
Run with: pytest tests/
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from scraper import _parse_date, is_fresh, passes_filters, load_slugs, fetch_workday
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
            assert passes_filters(fresh_job(location="Remote - San Francisco, CA")) is True

    def test_remote_only_drops_onsite(self):
        with patch.multiple(scraper, INCLUDE_KEYWORDS=[], EXCLUDE_KEYWORDS=[], REMOTE_ONLY=True, MAX_AGE_DAYS=0):
            assert passes_filters(fresh_job(location="New York, NY")) is False

    def test_remote_only_drops_non_us_remote(self):
        with patch.multiple(scraper, INCLUDE_KEYWORDS=[], EXCLUDE_KEYWORDS=[], REMOTE_ONLY=True, MAX_AGE_DAYS=0):
            assert passes_filters(fresh_job(location="Remote - EMEA")) is False

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


# ── fetch_workday ─────────────────────────────────────────────────────────────

class TestFetchWorkday:
    """Tests for the fetch_workday function."""

    def _mock_response(self, total: int, postings: list[dict], status_code: int = 200):
        """Create a mock response object."""
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = {"total": total, "jobPostings": postings}
        if status_code >= 400:
            import requests
            resp.raise_for_status.side_effect = requests.HTTPError(
                response=resp
            )
        else:
            resp.raise_for_status.return_value = None
        return resp

    def _sample_posting(self, idx: int = 1):
        return {
            "title": f"Software Engineer {idx}",
            "externalPath": f"/job/City/Role_R{idx:06d}",
            "locationsText": "San Jose, CA",
            "startDate": "2024-12-15",
        }

    def test_single_page_success(self):
        """Fetches a single page of results when total <= 20."""
        postings = [self._sample_posting(i) for i in range(3)]
        session = MagicMock()
        session.post.return_value = self._mock_response(total=3, postings=postings)

        entry = ("adobe", "wd5", "external_experienced")
        result = fetch_workday(entry, session)

        assert len(result) == 3
        assert result[0]["source"] == "Workday"
        assert result[0]["id"] == "wd-adobe-external_experienced-/job/City/Role_R000000"
        assert result[0]["title"] == "Software Engineer 0"
        assert result[0]["company"] == "Adobe"
        assert result[0]["location"] == "San Jose, CA"
        assert result[0]["url"] == "https://adobe.wd5.myworkdayjobs.com/external_experienced/job/City/Role_R000000"
        assert result[0]["posted_at"] == "2024-12-15"
        session.post.assert_called_once()

    def test_multi_page_pagination(self):
        """Paginates across multiple pages when total exceeds single page."""
        page1_postings = [self._sample_posting(i) for i in range(20)]
        page2_postings = [self._sample_posting(i + 20) for i in range(5)]

        session = MagicMock()
        session.post.side_effect = [
            self._mock_response(total=25, postings=page1_postings),
            self._mock_response(total=25, postings=page2_postings),
        ]

        entry = ("meta", "wd5", "external_careers")
        with patch.object(scraper, "WORKDAY_DELAY_S", 0):
            result = fetch_workday(entry, session)

        assert len(result) == 25
        assert session.post.call_count == 2

    def test_http_error_on_first_page_returns_empty(self):
        """Returns empty list when first request fails."""
        import requests as req
        session = MagicMock()
        session.post.side_effect = req.RequestException("Connection refused")

        entry = ("badcompany", "wd1", "jobs")
        result = fetch_workday(entry, session)

        assert result == []

    def test_http_error_mid_pagination_returns_partial(self):
        """Returns partial results when failure occurs after first page."""
        import requests as req
        page1_postings = [self._sample_posting(i) for i in range(20)]

        session = MagicMock()
        session.post.side_effect = [
            self._mock_response(total=60, postings=page1_postings),
            req.RequestException("Timeout"),
        ]

        entry = ("adobe", "wd5", "careers")
        with patch.object(scraper, "WORKDAY_DELAY_S", 0):
            result = fetch_workday(entry, session)

        assert len(result) == 20  # Only first page's results retained

    def test_empty_postings_stops_pagination(self):
        """Stops when API returns empty jobPostings array."""
        session = MagicMock()
        session.post.return_value = self._mock_response(total=100, postings=[])

        entry = ("empty-co", "wd3", "jobs")
        result = fetch_workday(entry, session)

        assert result == []
        session.post.assert_called_once()

    def test_max_pages_reached(self):
        """Stops at WORKDAY_MAX_PAGES and logs warning."""
        postings = [self._sample_posting(i) for i in range(20)]
        session = MagicMock()
        # Always return full pages with high total to trigger max pages
        session.post.return_value = self._mock_response(total=500, postings=postings)

        entry = ("big-corp", "wd5", "careers")
        with patch.object(scraper, "WORKDAY_MAX_PAGES", 3), \
             patch.object(scraper, "WORKDAY_DELAY_S", 0):
            result = fetch_workday(entry, session)

        assert len(result) == 60  # 3 pages x 20 postings
        assert session.post.call_count == 3

    def test_missing_startdate_sets_empty_string(self):
        """Sets posted_at to empty string when startDate is missing."""
        posting = {"title": "Engineer", "externalPath": "/job/x", "locationsText": "NYC"}
        session = MagicMock()
        session.post.return_value = self._mock_response(total=1, postings=[posting])

        entry = ("test-co", "wd1", "jobs")
        result = fetch_workday(entry, session)

        assert result[0]["posted_at"] == ""

    def test_company_name_formatting(self):
        """Hyphens in company name are replaced with spaces and title-cased."""
        postings = [self._sample_posting(1)]
        session = MagicMock()
        session.post.return_value = self._mock_response(total=1, postings=postings)

        entry = ("bank-of-america", "wd1", "careers")
        result = fetch_workday(entry, session)

        assert result[0]["company"] == "Bank Of America"
