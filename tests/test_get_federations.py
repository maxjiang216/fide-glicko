"""Tests for get_federations scraper."""

import pytest

from get_federations import get_federations_with_retries


class TestGetFederations:
    """Tests for federation list scraper."""

    @pytest.mark.online
    def test_returns_207_federations(self):
        """
        Smoke test: FIDE federation list should return 207 countries.
        https://ratings.fide.com/top_federations.phtml
        (get_federations scrapes rated_tournaments.phtml which has the same country selector)
        Run with: pytest -m online
        Skip in CI: pytest -m "not online"
        """
        federations = get_federations_with_retries()

        assert (
            len(federations) == 208
        ), f"Expected 208 federations (207 scraped + CGO fallback), got {len(federations)}"
        assert all("code" in f and "name" in f for f in federations)
        assert federations[0]["code"]  # Non-empty codes
        assert federations[0]["name"]  # Non-empty names

    @pytest.mark.online
    def test_cgo_in_federations(self):
        """
        CGO (Republic of Congo) is hard-coded in get_federations when missing
        from FIDE's country selector. Ensures CGO is available for player list validation.
        Run with: pytest -m online
        """
        federations = get_federations_with_retries()
        codes = {f["code"].upper() for f in federations}
        assert "CGO" in codes, "CGO (Republic of Congo) should be in federations"

    @pytest.mark.online
    def test_live_endpoint_returns_non_empty_with_expected_format(self):
        """
        Endpoint check: federation list returns non-empty data with code/name structure.
        Run with: pytest -m online
        """
        federations = get_federations_with_retries()

        assert len(federations) > 0
        required_keys = {"code", "name"}
        for f in federations:
            assert required_keys <= set(f.keys()), f"Missing keys: {f}"
            assert f["code"], f"Empty code: {f}"
            assert f["name"], f"Empty name: {f}"
