"""Tests for get_player_list scraper."""

from io import BytesIO
import zipfile

import pytest

from get_player_list import (
    DOWNLOAD_URL,
    parse_xml_content,
    process_zip,
    get_player_list,
    download_player_list,
)


class TestParsePlayerList:
    """Tests for parsing logic using fixtures."""

    def test_parse_xml_content_parses_valid_player(self):
        """Valid player element is parsed correctly."""
        xml = b"""<?xml version="1.0"?>
<playerslist>
<player>
<fideid>10292519</fideid>
<name>A A M Imtiaz, Chowdhury</name>
<country>BAN</country>
<sex>M</sex>
<title></title>
<w_title></w_title>
<o_title></o_title>
<foa_title></foa_title>
<rating>0</rating>
<games>0</games>
<k>0</k>
<rapid_rating>0</rapid_rating>
<rapid_games>0</rapid_games>
<rapid_k>0</rapid_k>
<blitz_rating>0</blitz_rating>
<blitz_games>0</blitz_games>
<blitz_k>0</blitz_k>
<birthday>1975</birthday>
<flag></flag>
</player>
</playerslist>"""
        players = parse_xml_content(xml)
        assert len(players) == 1
        assert players[0]["id"] == 10292519
        assert players[0]["name"] == "A A M Imtiaz, Chowdhury"
        assert players[0]["fed"] == "BAN"
        assert players[0]["sex"] == "M"
        assert players[0]["byear"] == 1975

    def test_parse_xml_content_normalizes_title_and_fed(self):
        """Title (g->GM) and fed (uppercase) are normalized."""
        xml = b"""<?xml version="1.0"?>
<playerslist>
<player>
<fideid>10292519</fideid>
<name>Test Player</name>
<country>usa</country>
<sex>M</sex>
<title>g</title>
<birthday>1990</birthday>
</player>
</playerslist>"""
        players = parse_xml_content(xml)
        assert len(players) == 1
        assert players[0]["fed"] == "USA"
        assert players[0]["title"] == "GM"

    def test_parse_xml_content_skips_invalid_id(self):
        """Players with invalid fideid are skipped."""
        xml = b"""<?xml version="1.0"?>
<playerslist>
<player>
<fideid></fideid>
<name>No ID</name>
<country>USA</country>
<birthday>1990</birthday>
</player>
</playerslist>"""
        players = parse_xml_content(xml)
        assert len(players) == 0

    def test_parse_xml_content_processes_multiple_players(self):
        """Multiple players are parsed with correct types."""
        xml = b"""<?xml version="1.0"?>
<playerslist>
<player><fideid>10292519</fideid><name>Player One</name><country>BAN</country><sex>M</sex><birthday>1975</birthday></player>
<player><fideid>537001345</fideid><name>A Arbhin Vanniarajan</name><country>IND</country><sex>M</sex><birthday>2010</birthday></player>
</playerslist>"""
        players = parse_xml_content(xml)
        assert len(players) == 2
        assert players[0]["id"] == 10292519
        assert players[0]["byear"] == 1975
        assert players[1]["id"] == 537001345
        assert players[1]["byear"] == 2010

    def test_process_zip_extracts_and_parses(self):
        """process_zip extracts XML from zip and parses."""
        xml_content = b"""<?xml version="1.0"?>
<playerslist>
<player>
<fideid>10292519</fideid>
<name>Test Player</name>
<country>USA</country>
<sex>M</sex>
<title>g</title>
<birthday>1990</birthday>
</player>
</playerslist>"""
        buf = BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("players_list_xml_foa.xml", xml_content)

        players = process_zip(buf.getvalue())
        assert len(players) == 1
        assert players[0]["id"] == 10292519
        assert players[0]["name"] == "Test Player"
        assert players[0]["fed"] == "USA"
        assert players[0]["title"] == "GM"
        assert players[0]["byear"] == 1990


class TestGetPlayerListOnline:
    """Online tests - run with pytest -m online."""

    @pytest.mark.online
    def test_download_url_returns_zip(self):
        """
        Smoke test: FIDE download URL returns valid zip with XML content.
        https://ratings.fide.com/download_lists.phtml
        Run with: pytest -m online
        """
        zip_bytes = download_player_list()
        assert len(zip_bytes) > 1_000_000  # ~45 MB
        with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
            names = zf.namelist()
            assert any(n.endswith(".xml") for n in names)

    @pytest.mark.online
    def test_get_player_list_returns_non_empty_with_expected_format(self):
        """
        Endpoint check: full pipeline returns players with expected structure.
        Run with: pytest -m online
        """
        players = get_player_list()
        assert len(players) > 100_000
        p = players[0]
        required = {"id", "name", "byear", "sex", "fed", "title"}
        assert required <= set(p.keys()), f"Missing keys: {required - set(p.keys())}"
        assert isinstance(p["id"], int)
        assert p["name"]
        assert len(p["fed"]) <= 3
        assert p["sex"] in ("M", "F", None)

    @pytest.mark.online
    def test_player_list_field_validity(self):
        """
        Validate field constraints on downloaded player list.
        Run with: pytest -m online
        """
        players = get_player_list()
        assert len(players) > 0

        current_year = __import__("datetime").datetime.now().year
        VALID_TITLES = frozenset(
            {"g", "wg", "m", "wm", "f", "wf", "c", "wc"}
            | {"gm", "im", "fm", "cm", "wgm", "wim", "wfm", "wcm"}
        )

        for p in players:
            assert isinstance(p["id"], int), f"Invalid id: {p['id']!r}"

            if p.get("byear") is not None:
                assert (
                    1900 <= p["byear"] < current_year
                ), f"byear out of range: {p['byear']} for id={p['id']}"

            if p.get("title"):
                assert (
                    p["title"].lower() in VALID_TITLES
                ), f"Invalid title: {p['title']!r} for id={p['id']}"

            if p.get("sex") is not None:
                assert p["sex"] in (
                    "M",
                    "F",
                ), f"Invalid sex: {p['sex']!r} for id={p['id']}"

    @pytest.mark.online
    def test_player_list_fed_codes_in_federations(self):
        """
        Every fed code in the player list must be a valid FIDE federation code.
        Run with: pytest -m online
        """
        from get_federations import get_federations_with_retries

        federations = get_federations_with_retries()
        valid_codes = frozenset(c.upper() for c in (f["code"] for f in federations))
        special_codes = frozenset({"FID", "NON"})
        valid_codes = valid_codes | special_codes

        players = get_player_list()
        unique_feds = {p["fed"].upper() for p in players if p.get("fed")}

        invalid = unique_feds - valid_codes
        assert not invalid, f"FED codes not in FIDE federation list: {invalid}"
