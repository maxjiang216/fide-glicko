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


def test_failed_refresh_falls_back_to_previous_list(tmp_path, monkeypatch):
    import os
    import time

    import get_federations as gf

    prev = tmp_path / gf.FEDERATIONS_DATA_PREFIX / "federations_20200101-000000.csv"
    prev.parent.mkdir(parents=True)
    prev.write_text("code,name\nISL,Iceland\n")
    old = time.time() - 60 * 86400  # stale, so a refresh is attempted
    os.utime(prev, (old, old))

    def boom(*a, **k):
        raise ConnectionError(
            "Connection to ratings.fide.com timed out. (connect timeout=55)"
        )

    monkeypatch.setattr(gf, "get_federations_with_retries", boom)
    assert gf.run_shared(bucket=None, local_root=str(tmp_path)) == str(prev)


def test_failed_refresh_without_previous_list_raises_block(tmp_path, monkeypatch):
    import get_federations as gf
    from fide_http import FideBlocked

    def boom(*a, **k):
        raise ConnectionError(
            "Connection to ratings.fide.com timed out. (connect timeout=55)"
        )

    monkeypatch.setattr(gf, "get_federations_with_retries", boom)
    with pytest.raises(FideBlocked):
        gf.run_shared(bucket=None, local_root=str(tmp_path))
