#!/usr/bin/env python3
"""
Benchmark compression methods on players_list.xml.

Compares gzip, zstd, brotli, lzma for size and speed.
Run from repo root with an existing players_list.xml, e.g.:
  # Input must be uncompressed XML (gunzip -c raw/players_list.xml.gz > /tmp/players_list.xml if needed):
  python scripts/benchmark_xml_compression.py /tmp/players_list.xml
"""

import gzip
import io
import sys
import time
from pathlib import Path


def compress_gzip(data: bytes, level: int = 9) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level) as z:
        z.write(data)
    return buf.getvalue()


def compress_zstd(data: bytes, level: int = 3) -> bytes:
    import zstandard as zstd

    cctx = zstd.ZstdCompressor(level=level)
    return cctx.compress(data)


def compress_brotli(data: bytes, quality: int = 11) -> bytes:
    import brotli

    return brotli.compress(data, quality=quality)


def main() -> int:
    if len(sys.argv) < 2:
        print(
            "Usage: python scripts/benchmark_xml_compression.py <path_to_players_list.xml>"
        )
        return 1

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"File not found: {path}")
        return 1

    data = path.read_bytes()
    orig_size = len(data)
    print(f"Input: {path} ({orig_size:,} bytes / {orig_size / 1_000_000:.2f} MB)\n")
    print(
        f"{'Method':<25} {'Size (bytes)':>14} {'Size (MB)':>10} {'Ratio %':>8} {'Time (ms)':>10}"
    )
    print("-" * 70)

    results = []

    # gzip levels 6 (default) and 9
    for level in (6, 9):
        t0 = time.perf_counter()
        out = compress_gzip(data, level=level)
        t = (time.perf_counter() - t0) * 1000
        ratio = 100 * len(out) / orig_size
        label = f"gzip (level {level})"
        results.append((label, len(out), ratio, t))
        print(
            f"{label:<25} {len(out):>14,} {len(out)/1e6:>10.3f} {ratio:>7.1f}% {t:>10.1f}"
        )

    # zstd levels 3 (default), 9
    try:
        for level in (3, 9):
            t0 = time.perf_counter()
            out = compress_zstd(data, level=level)
            t = (time.perf_counter() - t0) * 1000
            ratio = 100 * len(out) / orig_size
            label = f"zstd (level {level})"
            results.append((label, len(out), ratio, t))
            print(
                f"{label:<25} {len(out):>14,} {len(out)/1e6:>10.3f} {ratio:>7.1f}% {t:>10.1f}"
            )
    except ImportError:
        print("zstd: install zstandard for zstd support")

    # brotli quality 6 (fast; 11 is very slow on large files)
    try:
        t0 = time.perf_counter()
        out = compress_brotli(data, quality=6)
        t = (time.perf_counter() - t0) * 1000
        ratio = 100 * len(out) / orig_size
        label = "brotli (quality 6)"
        results.append((label, len(out), ratio, t))
        print(
            f"{label:<25} {len(out):>14,} {len(out)/1e6:>10.3f} {ratio:>7.1f}% {t:>10.1f}"
        )
    except ImportError:
        print("brotli: install brotli for brotli support")

    # Summary: best ratio
    by_ratio = sorted(results, key=lambda r: r[2])
    best = by_ratio[0]
    print()
    print(f"Smallest: {best[0]} ({best[2]:.1f}% of original)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
