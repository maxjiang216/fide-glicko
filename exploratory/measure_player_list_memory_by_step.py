#!/usr/bin/env python3
"""
Run player list pipeline with memory snapshots at each step.

Usage:
  python exploratory/measure_player_list_memory_by_step.py

Shows RSS after each major step to identify memory hotspots.
"""

import resource
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRAPER_DIR = REPO_ROOT / "src" / "scraper"
sys.path.insert(0, str(SCRAPER_DIR))

from get_player_list import (
    DOWNLOAD_URL,
    _process_zip_internal,
    build_report,
    download_player_list,
    load_federations,
)
from s3_io import write_output


def _read_proc_status() -> dict[str, int]:
    """Read /proc/self/status (Linux). Returns dict with vmrss, vmpeak in KB."""
    result: dict[str, int] = {}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    result["vmrss"] = int(line.split()[1])
                elif line.startswith("VmPeak:"):
                    result["vmpeak"] = int(line.split()[1])
    except (FileNotFoundError, OSError):
        pass
    return result


def rss_mb() -> float:
    """Current RSS in MB (Linux). Fallback: peak from resource (less accurate for step deltas)."""
    proc = _read_proc_status()
    if proc.get("vmrss"):
        return proc["vmrss"] / 1024
    usage = resource.getrusage(resource.RUSAGE_SELF)
    kb = usage.ru_maxrss
    return (kb / (1024 * 1024) if sys.platform == "darwin" and kb < 1024 * 1024 else kb / 1024)


def peak_mb() -> float:
    """Peak memory in MB."""
    proc = _read_proc_status()
    if proc.get("vmpeak"):
        return proc["vmpeak"] / 1024
    usage = resource.getrusage(resource.RUSAGE_SELF)
    kb = usage.ru_maxrss
    return (kb / (1024 * 1024) if sys.platform == "darwin" and kb < 1024 * 1024 else kb / 1024)


def main() -> int:
    baseline = rss_mb()
    print(f"Baseline: {baseline:.1f} MB\n")

    # 1. Download
    print("Downloading...")
    zip_bytes = download_player_list()
    m1 = rss_mb()
    print(f"  After download: {m1:.1f} MB (+{m1 - baseline:.1f})")
    print(f"  zip size: {len(zip_bytes) / 1024 / 1024:.1f} MB\n")

    # 2. Unzip
    import zipfile
    from io import BytesIO

    with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        xml_name = next((n for n in names if n.endswith(".xml")), names[0])
        with zf.open(xml_name) as f:
            xml_content = f.read()
    m2 = rss_mb()
    print(f"  After unzip: {m2:.1f} MB (+{m2 - m1:.1f})")
    print(f"  xml size: {len(xml_content) / 1024 / 1024:.1f} MB\n")

    # 3. Parse XML (ET.fromstring + build rows)
    from get_player_list import parse_xml_content

    players, parse_stats = parse_xml_content(xml_content)
    m3 = rss_mb()
    print(f"  After parse_xml_content: {m3:.1f} MB (+{m3 - m2:.1f})")
    print(f"  players: {len(players)} rows\n")

    # 4. Build report
    federations_path = REPO_ROOT / "data" / "federations.csv"
    if not federations_path.exists():
        federations_path = None
    report = build_report(players, parse_stats, federations_path)
    m4 = rss_mb()
    print(f"  After build_report: {m4:.1f} MB (+{m4 - m3:.1f})\n")

    # 5. DataFrame + save
    import pandas as pd
    from io import BytesIO

    df = pd.DataFrame(players)[["byear", "id", "fed", "name", "sex", "title", "w_title"]]
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    m5 = rss_mb()
    print(f"  After DataFrame + to_parquet: {m5:.1f} MB (+{m5 - m4:.1f})\n")

    # Write outputs
    out_dir = REPO_ROOT / "src" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    write_output(buf.getvalue(), str(out_dir / "players_list.parquet"))
    write_output(xml_content, str(out_dir / "players_list.xml"))
    m6 = rss_mb()
    print(f"  After write: {m6:.1f} MB\n")

    # Summary
    print("=" * 50)
    print("Summary (RSS delta from previous step):")
    steps = [
        ("download", m1 - baseline),
        ("unzip", m2 - m1),
        ("parse_xml (ET.fromstring + rows)", m3 - m2),
        ("build_report", m4 - m3),
        ("DataFrame + to_parquet", m5 - m4),
    ]
    for name, delta in sorted(steps, key=lambda x: -x[1]):
        print(f"  {name}: +{delta:.1f} MB")
    print(f"\nPeak: {m6:.1f} MB (likely during parse or DataFrame)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
