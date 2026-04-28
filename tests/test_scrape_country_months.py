"""
Tests for scripts/scrape_country_months.py.

Offline: CSV conversion logic.
Online:  Playwright scraping of the #archive dropdown for one federation.
"""

import csv
import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "scraper"))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from scrape_country_months import from_csv


# ---------------------------------------------------------------------------
# Offline: CSV conversion
# ---------------------------------------------------------------------------

_SAMPLE_CSV = """\
country,year,month,num_tournaments
USA,2006,1,45
USA,2006,2,38
FRA,2006,3,12
FRA,2006,3,5
RUS,2024,12,200
AFG,2010,1,0
"""


def _csv_path(tmp_path: Path) -> Path:
    p = tmp_path / "sample.csv"
    p.write_text(_SAMPLE_CSV, encoding="utf-8")
    return p


def test_from_csv_basic(tmp_path):
    result = from_csv(_csv_path(tmp_path))
    assert "USA" in result
    assert "2006-01" in result["USA"]
    assert "2006-02" in result["USA"]
    assert "FRA" in result
    assert "2006-03" in result["FRA"]
    assert "RUS" in result
    assert "2024-12" in result["RUS"]


def test_from_csv_excludes_zero_count(tmp_path):
    result = from_csv(_csv_path(tmp_path))
    # AFG has num_tournaments=0 — should not appear
    assert "AFG" not in result or result.get("AFG") == []


def test_from_csv_deduplicates(tmp_path):
    # FRA 2006-03 appears twice in CSV — should appear once in output
    result = from_csv(_csv_path(tmp_path))
    assert result["FRA"].count("2006-03") == 1


def test_from_csv_sorted(tmp_path):
    result = from_csv(_csv_path(tmp_path))
    for code, months in result.items():
        assert months == sorted(months), f"{code} months not sorted"


def test_from_csv_real_file():
    """Use the existing exploratory CSV if present; skip otherwise."""
    csv_path = Path(__file__).parent.parent / "exploratory" / "data" / "tournaments_by_country_month.csv"
    if not csv_path.exists():
        pytest.skip("exploratory/data/tournaments_by_country_month.csv not found")

    result = from_csv(csv_path)
    assert len(result) > 100, "Expected data for >100 federations"
    assert "USA" in result
    assert "RUS" in result
    # Should go back to at least 2006
    all_months = [m for months in result.values() for m in months]
    assert min(all_months) <= "2006-12", f"Expected data back to 2006, got {min(all_months)}"


# ---------------------------------------------------------------------------
# Online: Playwright scraping smoke test
# ---------------------------------------------------------------------------

@pytest.mark.online
def test_scrape_one_federation_live():
    """Scrape the #archive dropdown for FRA and verify expected months are present."""
    import asyncio
    from scrape_country_months import scrape_one, CONCURRENCY

    async def _run():
        semaphore = asyncio.Semaphore(CONCURRENCY)
        return await scrape_one(semaphore, "FRA")

    code, months = asyncio.run(_run())
    assert code == "FRA"
    assert len(months) > 50, f"Expected >50 months for FRA, got {len(months)}"
    # FRA should have data back to at least 2006
    assert any(m <= "2006-12" for m in months), f"Expected months back to 2006 for FRA, got earliest={min(months) if months else 'none'}"
    # All values should be YYYY-MM format
    for m in months:
        assert len(m) == 7 and m[4] == "-", f"Unexpected month format: {m!r}"
