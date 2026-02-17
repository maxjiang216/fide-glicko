"""Tests for get_federations scraper."""

import pytest

from get_federations import get_federations_with_retries


class TestGetFederations:
    """Tests for federation list scraper."""

    @pytest.mark.live
    def test_returns_207_federations(self):
        """
        Smoke test: FIDE federation list should return 207 countries.
        https://ratings.fide.com/top_federations.phtml
        (get_federations scrapes rated_tournaments.phtml which has the same country selector)
        Run with: pytest -m live
        Skip in CI: pytest -m "not live"
        """
        federations = get_federations_with_retries()

        assert (
            len(federations) == 207
        ), f"Expected 207 federations, got {len(federations)}"
        assert all("code" in f and "name" in f for f in federations)
        assert federations[0]["code"]  # Non-empty codes
        assert federations[0]["name"]  # Non-empty names
