"""Tests for get_tournaments scraper."""

import asyncio
import tempfile
from pathlib import Path

import pytest

aiohttp = pytest.importorskip("aiohttp")

from get_tournaments import (
    fetch_federation_tournaments,
    is_valid_tournament_id,
    read_federations,
)


class TestValidationHelpers:
    """Tests for validation helpers."""

    def test_is_valid_tournament_id_numeric(self):
        assert is_valid_tournament_id("123456") is True
        assert is_valid_tournament_id("399495") is True

    def test_is_valid_tournament_id_non_numeric(self):
        assert is_valid_tournament_id("abc") is False
        assert is_valid_tournament_id("12a34") is False
        assert is_valid_tournament_id("") is False
        assert is_valid_tournament_id("  ") is False


class TestReadFederations:
    """Tests for read_federations()."""

    def test_empty_file_raises(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            f.write("code,name\n")
            path = Path(f.name)
        try:
            with pytest.raises(ValueError, match="empty or has no valid"):
                read_federations(path)
        finally:
            path.unlink()

    def test_headers_only_raises(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            f.write("code,name\n")
            path = Path(f.name)
        try:
            with pytest.raises(ValueError, match="empty or has no valid"):
                read_federations(path)
        finally:
            path.unlink()

    def test_valid_federations(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            f.write("code,name\nUSA,United States\nRUS,Russia\n")
            path = Path(f.name)
        try:
            result = read_federations(path)
            assert len(result) == 2
            assert result[0] == ("USA", "United States")
            assert result[1] == ("RUS", "Russia")
        finally:
            path.unlink()


class TestGetTournaments:
    """Tests for tournament listing scraper."""

    @pytest.mark.online
    def test_usa_december_2025_returns_93_tournaments(self):
        """
        Smoke test: USA Dec 2025 should return 93 tournaments.
        https://ratings.fide.com/rated_tournaments.phtml?country=USA&period=2025-12-01
        Run with: pytest -m online
        Skip in CI: pytest -m "not online"
        """

        async def _fetch():
            semaphore = asyncio.Semaphore(1)
            async with aiohttp.ClientSession() as session:
                code, name, tournaments, error, _ = await fetch_federation_tournaments(
                    session, semaphore, "USA", "United States of America", 2025, 12
                )
                return tournaments, error

        tournaments, error = asyncio.run(_fetch())

        assert error is None, f"Fetch failed: {error}"
        assert (
            len(tournaments) == 93
        ), f"Expected 93 tournaments for USA Dec 2025, got {len(tournaments)}"

    @pytest.mark.online
    def test_live_endpoint_returns_non_empty_with_expected_format(self):
        """
        Endpoint check: tournament list returns non-empty data with expected structure.
        Run with: pytest -m online
        """

        async def _fetch():
            semaphore = asyncio.Semaphore(1)
            async with aiohttp.ClientSession() as session:
                code, name, tournaments, error, _ = await fetch_federation_tournaments(
                    session, semaphore, "USA", "United States of America", 2025, 12
                )
                return tournaments, error

        tournaments, error = asyncio.run(_fetch())

        assert error is None, f"Fetch failed: {error}"
        assert len(tournaments) > 0
        t = tournaments[0]
        assert t.tournament_id
        assert t.name
        assert t.location is not None
        assert t.time_control
        assert t.start_date
        assert t.end_date
        assert t.federation == "USA"
