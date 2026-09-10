"""
Tests for digest.py streak/digest logic — no network calls, no credentials needed.
Run with: pytest tests/
"""

from datetime import date, timedelta

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from digest import compute_streak, compute_week_count, post_digest, post_streak
import digest


def _dates(today: date, days_ago: list[int]) -> set[str]:
    return {(today - timedelta(days=d)).isoformat() for d in days_ago}


class TestComputeStreak:
    def test_no_applications_is_zero_streak(self):
        today = date(2026, 8, 24)
        assert compute_streak(set(), today) == 0

    def test_applied_today_only(self):
        today = date(2026, 8, 24)
        applied = _dates(today, [0])
        assert compute_streak(applied, today) == 1

    def test_consecutive_days_including_today(self):
        today = date(2026, 8, 24)
        applied = _dates(today, [0, 1, 2, 3])
        assert compute_streak(applied, today) == 4

    def test_gap_breaks_streak(self):
        today = date(2026, 8, 24)
        applied = _dates(today, [0, 1, 3, 4])  # gap at day 2
        assert compute_streak(applied, today) == 2

    def test_no_application_today_but_yesterday_keeps_streak_alive(self):
        # Haven't applied yet today, but applied every day up through yesterday.
        today = date(2026, 8, 24)
        applied = _dates(today, [1, 2, 3])
        assert compute_streak(applied, today) == 3

    def test_missed_yesterday_and_today_is_zero(self):
        today = date(2026, 8, 24)
        applied = _dates(today, [2, 3, 4])
        assert compute_streak(applied, today) == 0


class TestComputeWeekCount:
    def test_empty_is_zero(self):
        today = date(2026, 8, 24)
        assert compute_week_count(set(), today) == 0

    def test_counts_days_within_trailing_week(self):
        today = date(2026, 8, 24)
        applied = _dates(today, [0, 2, 6])
        assert compute_week_count(applied, today) == 3

    def test_ignores_days_outside_window(self):
        today = date(2026, 8, 24)
        applied = _dates(today, [0, 8, 10])  # only day 0 is within trailing 7
        assert compute_week_count(applied, today) == 1


class TestPostDigestNoWebhook:
    def test_returns_false_when_no_webhook(self, monkeypatch):
        monkeypatch.setattr(digest, "DISCORD_WEBHOOK_URL", "")
        monkeypatch.setattr(digest, "DIGEST_DISCORD_WEBHOOK_URL", "")
        assert post_digest([{"title": "x", "company": "y", "location": "z", "url": "", "source": "Greenhouse"}]) is False

    def test_returns_true_for_empty_job_list(self, monkeypatch):
        monkeypatch.setattr(digest, "DISCORD_WEBHOOK_URL", "https://example.com/webhook")
        assert post_digest([]) is True


class TestPostStreakNoWebhook:
    def test_returns_false_when_no_webhook(self, monkeypatch):
        monkeypatch.setattr(digest, "DISCORD_WEBHOOK_URL", "")
        monkeypatch.setattr(digest, "DIGEST_DISCORD_WEBHOOK_URL", "")
        assert post_streak(3, 5) is False
