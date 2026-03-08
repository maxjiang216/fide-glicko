"""Unit tests for pure parsing functions in get_tournament_reports."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from get_tournament_reports import (
    extract_forfeit_indicator,
    fetch_tournament_report,
    flatten_result,
    flatten_to_games,
    format_duration,
    infer_date_format,
    parse_date_to_iso,
    parse_details_date_to_iso,
    parse_round_date,
    parse_score,
    results_to_games_dataframe,
    results_to_players_dataframe,
)


class TestParseScore:
    """Tests for parse_score()."""

    def test_valid_scores(self):
        assert parse_score("1.0") == 1.0
        assert parse_score("0.5") == 0.5
        assert parse_score("0") == 0.0
        assert parse_score("1") == 1.0

    def test_score_with_whitespace(self):
        assert parse_score("  0.5  ") == 0.5
        assert parse_score("1.0 ") == 1.0

    def test_score_case_insensitive_for_forfeit_check(self):
        # "forfeit" triggers None
        assert parse_score("Forfeit") is None
        assert parse_score("FORFEIT") is None

    def test_forfeit_returns_none(self):
        assert parse_score("forfeit") is None
        assert parse_score("-") is None
        assert parse_score("+") is None
        assert parse_score("forfeit -") is None
        assert parse_score("forfeit +") is None
        assert parse_score("Forfeit (-)") is None
        assert parse_score("Forfeit (+)") is None

    def test_empty_or_none(self):
        assert parse_score("") is None
        assert parse_score(None) is None

    def test_invalid_score_returns_none(self):
        # Scores must be exactly 0, 0.5, or 1.0
        assert parse_score("0.25") is None
        assert parse_score("2.0") is None
        assert parse_score("1.5") is None
        assert parse_score("abc") is None


class TestExtractForfeitIndicator:
    """Tests for extract_forfeit_indicator()."""

    def test_forfeit_with_dash(self):
        assert extract_forfeit_indicator("forfeit") == "-"
        assert extract_forfeit_indicator("forfeit -") == "-"
        assert extract_forfeit_indicator("Forfeit -") == "-"

    def test_forfeit_with_plus(self):
        assert extract_forfeit_indicator("forfeit +") == "+"
        assert extract_forfeit_indicator("Forfeit +") == "+"

    def test_standard_fide_forfeit_format(self):
        """FIDE uses 'Forfeit (-)' and 'Forfeit (+)' as standard strings."""
        assert extract_forfeit_indicator("Forfeit (-)") == "-"
        assert extract_forfeit_indicator("Forfeit (+)") == "+"

    def test_bare_dash_or_plus(self):
        assert extract_forfeit_indicator("-") == "-"
        assert extract_forfeit_indicator("+") == "+"

    def test_empty_returns_empty(self):
        assert extract_forfeit_indicator("") == ""
        assert extract_forfeit_indicator(None) == ""

    def test_normal_score_returns_empty(self):
        assert extract_forfeit_indicator("1.0") == ""
        assert extract_forfeit_indicator("0.5") == ""


class TestParseDetailsDateToIso:
    """Tests for parse_details_date_to_iso()."""

    def test_yyyy_mm_dd_with_dots(self):
        assert parse_details_date_to_iso("2024.12.30") == "2024-12-30"
        assert parse_details_date_to_iso("2024.1.5") == "2024-01-05"

    def test_yyyy_mm_dd_with_dashes(self):
        assert parse_details_date_to_iso("2024-12-30") == "2024-12-30"

    def test_dd_mm_yyyy(self):
        assert parse_details_date_to_iso("30.12.2024") == "2024-12-30"
        assert parse_details_date_to_iso("5.1.2024") == "2024-01-05"

    def test_invalid_returns_none(self):
        assert parse_details_date_to_iso("") is None
        assert parse_details_date_to_iso(None) is None
        assert parse_details_date_to_iso("not-a-date") is None
        assert parse_details_date_to_iso("2024-13-01") is None  # invalid month
        assert parse_details_date_to_iso("2024-12-32") is None  # invalid day


class TestInferDateFormat:
    """Tests for infer_date_format()."""

    def test_empty_returns_default(self):
        assert infer_date_format([]) == "yy/mm/dd"
        assert infer_date_format([""]) == "yy/mm/dd"
        assert infer_date_format(["not-a-date"]) == "yy/mm/dd"

    def test_yy_mm_dd_ambiguity(self):
        # "24/12/30" could be yy/mm/dd = 2024-12-30 or dd/mm/yy = 2030-12-24
        # With no bounds, format with smaller range wins
        result = infer_date_format(["24/12/30"])
        assert result in ("yy/mm/dd", "dd/mm/yy")

    def test_prefers_format_within_tournament_bounds(self):
        # If tournament is 2024-12-01 to 2024-12-31, 24/12/25 as yy/mm/dd = 2024-12-25 (in range)
        # as dd/mm/yy = 2025-12-24 (out of range)
        fmt = infer_date_format(
            ["24/12/25"],
            start_iso="2024-12-01",
            end_iso="2024-12-31",
        )
        assert fmt == "yy/mm/dd"

    def test_multiple_dates(self):
        fmt = infer_date_format(
            ["24/11/01", "24/11/15", "24/11/30"],
            start_iso="2024-11-01",
            end_iso="2024-11-30",
        )
        assert fmt == "yy/mm/dd"


class TestParseDateToIso:
    """Tests for parse_date_to_iso()."""

    def test_with_format_yy_mm_dd(self):
        assert parse_date_to_iso("24/12/30", "yy/mm/dd") == "2024-12-30"
        assert parse_date_to_iso("49/01/15", "yy/mm/dd") == "2049-01-15"
        assert parse_date_to_iso("50/01/15", "yy/mm/dd") == "1950-01-15"

    def test_with_format_dd_mm_yy(self):
        assert parse_date_to_iso("30/12/24", "dd/mm/yy") == "2024-12-30"

    def test_empty_input(self):
        assert parse_date_to_iso("") == ""
        assert parse_date_to_iso("", "yy/mm/dd") == ""

    def test_fallback_without_format(self):
        # Tries yy/mm/dd first, then dd/mm/yy
        result = parse_date_to_iso("24/12/30")
        assert result in ("2024-12-30", "2030-12-24")


class TestParseRoundDate:
    """Tests for parse_round_date()."""

    def test_round_with_date(self):
        assert parse_round_date("1   25/11/22") == (1, "25/11/22")
        assert parse_round_date("5  01/06/24") == (5, "01/06/24")

    def test_round_only(self):
        assert parse_round_date("1") == (1, None)
        assert parse_round_date("  12  ") == (12, None)

    def test_empty_returns_none(self):
        assert parse_round_date("") == (None, None)
        assert parse_round_date(None) == (None, None)


class TestFormatDuration:
    """Tests for format_duration()."""

    def test_seconds(self):
        assert format_duration(30.5) == "30.5s"
        assert format_duration(0.1) == "0.1s"

    def test_minutes(self):
        assert format_duration(90) == "1m 30s"
        assert format_duration(60) == "1m 0s"

    def test_hours(self):
        assert format_duration(3661) == "1h 1m"
        assert format_duration(7200) == "2h 0m"


class TestFlattenResult:
    """Tests for flatten_result()."""

    def test_failed_result(self):
        result = {
            "tournament_code": "123",
            "success": False,
            "error": "no data found",
        }
        flattened = flatten_result(result)
        assert len(flattened) == 1
        assert flattened[0]["tournament_code"] == "123"
        assert flattened[0]["success"] is False
        assert flattened[0]["error"] == "no data found"
        assert flattened[0]["player_id"] == ""

    def test_successful_result_with_rounds(self):
        result = {
            "tournament_code": "393912",
            "success": True,
            "players": [
                {
                    "id": "100",
                    "name": "Player A",
                    "country": "USA",
                    "rating": 1800,
                    "total": 5.0,
                    "rounds": [
                        {
                            "round": 1,
                            "date": "25/11/24",
                            "opp_name": "Player B",
                            "opp_id": "101",
                            "color": "white",
                            "opp_fed": "USA",
                            "title": "",
                            "wtitle": "",
                            "opp_rating": 1750,
                            "score": 1.0,
                            "forfeit": "",
                        },
                    ],
                },
            ],
        }
        flattened = flatten_result(result)
        assert len(flattened) == 1
        row = flattened[0]
        assert row["tournament_code"] == "393912"
        assert row["success"] is True
        assert row["player_id"] == "100"
        assert row["round"] == 1
        assert row["opp_id"] == "101"
        assert row.get("score") == 1.0
        assert row.get("opp_name") == "Player B"

    def test_player_with_no_rounds(self):
        result = {
            "tournament_code": "TC",
            "success": True,
            "players": [
                {
                    "id": "200",
                    "name": "Bye Player",
                    "country": "FED",
                    "rating": 0,
                    "total": 0.0,
                    "rounds": [],
                },
            ],
        }
        flattened = flatten_result(result)
        assert len(flattened) == 1
        assert flattened[0]["player_id"] == "200"
        assert flattened[0]["round"] is None


class TestFlattenToGames:
    """Tests for flatten_to_games()."""

    def test_deduplicates_games(self):
        # Same game from white and black perspective - should appear once
        flattened = [
            {
                "tournament_code": "TC",
                "success": True,
                "player_id": "100",
                "opp_id": "101",
                "round": 1,
                "round_date": "24/11/25",
                "color": "white",
                "score": 1.0,
                "forfeit": "",
            },
            {
                "tournament_code": "TC",
                "success": True,
                "player_id": "101",
                "opp_id": "100",
                "round": 1,
                "round_date": "24/11/25",
                "color": "black",
                "score": 0.0,
                "forfeit": "",
            },
        ]
        games = flatten_to_games(flattened, tournament_code="TC")
        assert len(games) == 1
        g = games[0]
        assert g["white_id"] == "100"
        assert g["black_id"] == "101"
        assert g["white_score"] == 1.0
        assert g["forfeit"] is False
        assert g["round"] == 1

    def test_forfeit_plus_white_wins(self):
        flattened = [
            {
                "tournament_code": "TC",
                "success": True,
                "player_id": "100",
                "opp_id": "101",
                "round": 1,
                "round_date": "24/11/25",
                "color": "white",
                "score": None,
                "forfeit": "+",
            },
        ]
        games = flatten_to_games(flattened, tournament_code="TC")
        assert len(games) == 1
        assert games[0]["white_score"] == 1.0
        assert games[0]["forfeit"] is True

    def test_forfeit_minus_black_wins(self):
        flattened = [
            {
                "tournament_code": "TC",
                "success": True,
                "player_id": "100",
                "opp_id": "101",
                "round": 1,
                "round_date": "24/11/25",
                "color": "white",
                "score": None,
                "forfeit": "-",
            },
        ]
        games = flatten_to_games(flattened, tournament_code="TC")
        assert len(games) == 1
        assert games[0]["white_score"] == 0.0
        assert games[0]["forfeit"] is True

    def test_skips_byes(self):
        # No opp_id = bye, skipped
        flattened = [
            {
                "tournament_code": "TC",
                "success": True,
                "player_id": "100",
                "opp_id": "",
                "round": 1,
                "round_date": "24/11/25",
                "color": "white",
                "score": 1.0,
                "forfeit": "",
            },
        ]
        games = flatten_to_games(flattened, tournament_code="TC")
        assert len(games) == 0

    def test_uses_details_map_for_date_inference(self):
        flattened = [
            {
                "tournament_code": "TC",
                "success": True,
                "player_id": "100",
                "opp_id": "101",
                "round": 1,
                "round_date": "25/12/24",
                "color": "white",
                "score": 0.5,
                "forfeit": "",
            },
        ]
        details_map = {"TC": ("2024-12-01", "2024-12-31")}
        games = flatten_to_games(
            flattened, tournament_code="TC", details_map=details_map
        )
        assert len(games) == 1
        assert games[0]["date"] == "2024-12-25"


class TestFixtureBasedParsing:
    """Tests using real FIDE HTML fixture. Validates parser against actual format."""

    def test_parses_world_cup_25_report_fixture(self):
        """Parse real FIDE World Cup 2025 report HTML. Catches format drift."""
        fixture_path = Path(__file__).parent / "fixtures" / "world_cup_25_report.html"
        fixture_html = fixture_path.read_bytes()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = fixture_html

        session = MagicMock()
        session.get.return_value = mock_response

        report, error, _ = fetch_tournament_report("449502", session)

        assert error is None
        assert report is not None
        assert report["tournament_code"] == "449502"
        assert "players" in report
        assert len(report["players"]) > 0

        # First player is Esipenko (FIDE World Cup 2025)
        first = report["players"][0]
        assert first["id"] == "24175439"
        assert "Esipenko" in first["name"]
        assert first["rating"] == 2681
        assert first["total"] == 9.0

        # Rounds 1-2 were forfeit-without-opponent (byes) - not added to rounds.
        # First round with opponent is round 3+.
        rounds = first["rounds"]
        assert len(rounds) >= 1
        # All rounds in output must have opp_id (forfeit-without-opponent are skipped)
        for r in rounds:
            assert r.get("opp_id"), "All rounds must have opponent (forfeit-without-opponent skipped)"

        # Round 3+ have real games (opp_id present, score/forfeit from actual play)
        game_rounds = [r for r in rounds if r.get("opp_id") and r.get("forfeit") == ""]
        assert len(game_rounds) >= 1
        assert game_rounds[0]["score"] == 1.0
        assert game_rounds[0]["color"] in ("white", "black")

        # Esipenko had byes in rounds 1-2 (forfeit-without-opponent) - those rounds are not
        # in his rounds list. Other players played in rounds 1-2, so games exist for those rounds.

    def test_418871_forfeit_without_opponent_produces_no_game(self):
        """
        Tournament 418871 (Blitz Playoff): forfeit rounds with no opponent (empty href)
        must NOT produce games. Only rounds with opp_id produce games.
        """
        fixture_path = Path(__file__).parent / "fixtures" / "blitz_playoff_418871_report.html"
        fixture_html = fixture_path.read_bytes()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = fixture_html

        session = MagicMock()
        session.get.return_value = mock_response

        report, error, _ = fetch_tournament_report("418871", session)

        assert error is None
        assert report is not None
        assert report["tournament_code"] == "418871"

        # Forfeit-without-opponent rounds should not be in player rounds (we only add when has_opponent)
        result = {**report, "success": True}
        flattened = flatten_result(result)
        games = flatten_to_games(flattened, tournament_code="418871")

        # Expected: 3 games (r1 Pragg-Firouzja, r2 Firouzja-MVL, r3 Pragg-MVL).
        # Forfeit-without-opponent rounds (Pragg r2, Firouzja r3, MVL r1) must NOT produce games.
        assert len(games) == 3
        rounds = {g["round"] for g in games}
        assert rounds == {1, 2, 3}

    def test_397341_forfeit_with_opponent_produces_game(self):
        """
        Tournament 397341 (World Blitz): Niemann-Dubov round 10 forfeit WITH opponent
        must produce a game with forfeit=True. Niemann had Forfeit (+) (won), Dubov had Forfeit (-).
        """
        fixture_path = Path(__file__).parent / "fixtures" / "world_blitz_397341_report.html"
        fixture_html = fixture_path.read_bytes()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = fixture_html

        session = MagicMock()
        session.get.return_value = mock_response

        report, error, _ = fetch_tournament_report("397341", session)

        assert error is None
        assert report is not None
        assert report["tournament_code"] == "397341"

        result = {**report, "success": True}
        flattened = flatten_result(result)
        games = flatten_to_games(flattened, tournament_code="397341")

        # One game: Niemann (2093596) vs Dubov (24126055), round 10, forfeit
        assert len(games) == 1
        g = games[0]
        assert g["round"] == 10
        assert g["forfeit"] is True
        assert set([g["white_id"], g["black_id"]]) == {"2093596", "24126055"}
        # Niemann won (forfeit +), so if Niemann was white: white_score=1.0, else 0.0
        assert g["white_score"] in (0.0, 1.0)

    @pytest.mark.online
    def test_live_fetch_matches_fixture(self):
        """
        Smoke test: fetching live from FIDE gives same parsed result as fixture.
        Run with: pytest -m online
        Skip in CI: pytest -m "not online"
        """
        # Parse fixture
        fixture_path = Path(__file__).parent / "fixtures" / "world_cup_25_report.html"
        fixture_html = fixture_path.read_bytes()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = fixture_html

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        report_fixture, error_fixture, _ = fetch_tournament_report(
            "449502", mock_session
        )
        assert error_fixture is None
        assert report_fixture is not None

        # Fetch live from FIDE
        live_session = requests.Session()
        report_live, error_live, _ = fetch_tournament_report("449502", live_session)

        assert error_live is None, f"Live fetch failed: {error_live}"
        assert report_live is not None

        # Same result as fixture
        assert report_live == report_fixture, (
            "Live fetch produced different result than fixture. "
            "FIDE may have updated the page—consider refreshing the fixture."
        )

    @pytest.mark.online
    def test_live_endpoint_returns_non_empty_with_expected_format(self):
        """
        Endpoint check: tournament report returns non-empty data with expected structure.
        Run with: pytest -m online
        """
        live_session = requests.Session()
        report, error, _ = fetch_tournament_report("449502", live_session)

        assert error is None, f"Fetch failed: {error}"
        assert report is not None
        assert report["tournament_code"] == "449502"
        assert "players" in report
        assert len(report["players"]) > 0

        player = report["players"][0]
        required = {"id", "name", "country", "rating", "total", "rounds"}
        assert required <= set(
            player.keys()
        ), f"Missing keys: {required - set(player.keys())}"
        assert player["id"]
        assert player["name"]
        assert player["total"] is not None
