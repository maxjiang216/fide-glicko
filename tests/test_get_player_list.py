"""Tests for get_player_list scraper."""

from io import BytesIO
import zipfile

import pytest

from get_player_list import (
    DOWNLOAD_URL,
    parse_line,
    parse_txt_content,
    process_zip,
    get_player_list,
    download_player_list,
)


class TestParsePlayerList:
    """Tests for parsing logic using fixtures."""

    def test_parse_line_skips_header(self):
        """Header line returns None."""
        assert parse_line("ID Number      Name ...") is None

    def test_parse_line_parses_valid_data_line(self):
        """Valid data line is parsed correctly."""
        # Fixed-width: name 15:76 (61 chars), fed 76:79, gap at 79, sex 80:84, etc.
        line = (
            "10292519       "
            + "A A M Imtiaz, Chowdhury".ljust(61)
            + "BAN"
            + " "
            + "M".ljust(4)
            + "".ljust(5)
            + "".ljust(5)
            + "".ljust(15)
            + "0".ljust(4)
            + "0".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)
            + "0".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)
            + "0".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)
            + "1975"
            + "".ljust(6)
        )
        parsed = parse_line(line)
        assert parsed is not None
        assert parsed["id"] == "10292519"
        assert parsed["name"] == "A A M Imtiaz, Chowdhury"
        assert parsed["fed"] == "BAN"
        assert parsed["sex"] == "M"
        assert parsed["bday"] == "1975"

    def test_parse_line_skips_short_lines(self):
        """Lines shorter than 100 chars return None."""
        assert parse_line("short") is None

    def test_parse_txt_content_processes_multiple_lines(self):
        """parse_txt_content returns list of dicts with correct types."""
        line1 = (
            "10292519       "
            + "A A M Imtiaz, Chowdhury".ljust(61)
            + "BAN"
            + " "
            + "M".ljust(4)
            + "".ljust(5)
            + "".ljust(5)
            + "".ljust(15)
            + "0".ljust(4)
            + "0".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)
            + "0".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)
            + "0".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)
            + "1975"
            + "".ljust(6)
        )
        line2 = (
            "537001345      "
            + "A Arbhin Vanniarajan".ljust(61)
            + "IND"
            + " "
            + "M".ljust(4)
            + "".ljust(5)
            + "".ljust(5)
            + "".ljust(15)
            + "0".ljust(4)  # tit, wtit, otit, foa
            + "1464".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)  # srtng, sgm, sk
            + "0".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)  # rrtng, rgm, rk
            + "0".ljust(6)
            + "0".ljust(4)
            + "0".ljust(3)  # brtng, bgm, bk
            + "2010"
            + "".ljust(6)  # bday, flag
        )
        content = (
            "ID Number      Name                                                         Fed Sex Tit  WTit OTit           FOA SRtng SGm SK RRtng RGm Rk BRtng BGm BK B-day Flag\n"
            + line1
            + "\n"
            + line2
            + "\n"
        )
        players = parse_txt_content(content)
        assert len(players) == 2
        assert players[0]["id"] == 10292519
        assert players[0]["name"] == "A A M Imtiaz, Chowdhury"
        assert players[0]["fed"] == "BAN"
        assert players[0]["byear"] == 1975
        assert players[1]["byear"] == 2010

    def test_process_zip_extracts_and_parses(self):
        """process_zip extracts TXT from zip and parses."""
        # Fixed-width line matching COLUMN_SPEC (162 chars, gap between fed and sex)
        data_line = (
            "10292519       "  # id 0:15
            + "Test Player".ljust(61)  # name 15:76
            + "USA"
            + " "  # fed 76:79, gap 79
            + "M".ljust(4)  # sex 80:84
            + "g".ljust(5)
            + "".ljust(5)  # tit 84:89, wtit 89:94
            + "".ljust(15)
            + "".ljust(4)  # otit 94:109, foa 109:113
            + "2800".ljust(6)
            + "50".ljust(4)
            + "40".ljust(3)  # srtng, sgm, sk
            + "2700".ljust(6)
            + "30".ljust(4)
            + "40".ljust(3)  # rrtng, rgm, rk
            + "2650".ljust(6)
            + "20".ljust(4)
            + "40".ljust(3)  # brtng, bgm, bk
            + "1990".ljust(4)  # bday 152:156
            + "".ljust(6)  # flag 156:162
            + "\n"
        )
        content = (
            b"ID Number      Name                                                         Fed Sex Tit  WTit OTit           FOA SRtng SGm SK RRtng RGm Rk BRtng BGm BK B-day Flag\n"
            + data_line.encode("utf-8")
        )
        buf = BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("players_list_foa.txt", content)

        players = process_zip(buf.getvalue())
        assert len(players) == 1
        assert players[0]["id"] == 10292519
        assert players[0]["name"] == "Test Player"
        assert players[0]["fed"] == "USA"
        assert players[0]["title"] == "g"
        assert players[0]["byear"] == 1990


class TestGetPlayerListOnline:
    """Online tests - run with pytest -m online."""

    @pytest.mark.online
    def test_download_url_returns_zip(self):
        """
        Smoke test: FIDE download URL returns valid zip with expected content.
        https://ratings.fide.com/download_lists.phtml
        Run with: pytest -m online
        """
        zip_bytes = download_player_list()
        assert len(zip_bytes) > 1_000_000  # ~40 MB
        with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
            names = zf.namelist()
            assert any(n.endswith(".txt") for n in names)

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
