"""Tests for get_tournament_details scraper."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from get_tournament_details import fetch_tournament_details


class TestFixtureBasedParsing:
    """Tests using real FIDE HTML fixture. Validates parser against actual format."""

    def test_parses_candidates_24_fixture(self):
        """Parse real FIDE Candidates 2024 details HTML. Catches format drift."""
        fixture_path = Path(__file__).parent / "fixtures" / "candidates_24_details.html"
        fixture_html = fixture_path.read_bytes()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = fixture_html

        session = MagicMock()
        session.get.return_value = mock_response

        details, error, _ = fetch_tournament_details("368261", session)

        assert error is None
        assert details is not None
        assert details.get("event_code") == "368261"
        assert details.get("tournament_name") == "FIDE Candidates Tournament 2024"
        assert details.get("city") == "Toronto"
        assert details.get("country") == "CAN"
        assert details.get("number_of_players") == "8"
        assert details.get("start_date") == "2024-04-03"
        assert details.get("end_date") == "2024-04-23"
        assert "Chief Arbiter" in str(details) or "chief_arbiter" in details

    @pytest.mark.live
    def test_live_fetch_matches_fixture(self):
        """
        Smoke test: fetching live from FIDE gives same parsed result as fixture.
        https://ratings.fide.com/tournament_information.phtml?event=368261
        Run with: pytest -m live
        Skip in CI: pytest -m "not live"
        """
        # Parse fixture
        fixture_path = Path(__file__).parent / "fixtures" / "candidates_24_details.html"
        fixture_html = fixture_path.read_bytes()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = fixture_html

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        details_fixture, error_fixture, _ = fetch_tournament_details(
            "368261", mock_session
        )
        assert error_fixture is None
        assert details_fixture is not None

        # Fetch live from FIDE
        live_session = requests.Session()
        details_live, error_live, _ = fetch_tournament_details("368261", live_session)

        assert error_live is None, f"Live fetch failed: {error_live}"
        assert details_live is not None

        # Same result as fixture
        assert details_live == details_fixture, (
            "Live fetch produced different result than fixture. "
            "FIDE may have updated the page—consider refreshing the fixture."
        )
