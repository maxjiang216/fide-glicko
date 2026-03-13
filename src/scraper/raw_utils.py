"""
Utilities for concatenated raw HTML chunk format.

Format: gzip(delim_1 + html_1 + delim_2 + html_2 + ...)
Delimiter: \\n!!FIDE!!id={id}!!\\n

One file per chunk instead of per-tournament to reduce S3 PUT costs.
"""

import gzip
import io
import re
from pathlib import Path
from typing import List, Optional, Tuple, Union

DELIMITER_PREFIX = b"\n!!FIDE!!id="
DELIMITER_SUFFIX = b"!!\n"


def build_concatenated_gzip(items: List[Tuple[str, bytes]]) -> bytes:
    """
    Build gzipped concatenation of (id, html) pairs.

    Format: delim_id1 + html1 + delim_id2 + html2 + ...
    Returns empty bytes if items is empty.
    """
    if not items:
        return b""
    buf = io.BytesIO()
    for id_val, html in items:
        buf.write(DELIMITER_PREFIX + id_val.encode("utf-8") + DELIMITER_SUFFIX + html)
    compressed = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed, mode="wb", compresslevel=9) as z:
        z.write(buf.getvalue())
    return compressed.getvalue()


def extract_tournament(
    gz_path_or_bytes: Union[str, Path, bytes], tournament_id: str
) -> Optional[bytes]:
    """
    Extract one tournament's HTML from a concatenated gzip chunk.

    Args:
        gz_path_or_bytes: Path to .html.gz file, or raw gzipped bytes.
        tournament_id: Tournament ID/code to extract.

    Returns:
        Raw HTML bytes for that tournament, or None if not found.
    """
    if isinstance(gz_path_or_bytes, (str, Path)):
        with gzip.open(gz_path_or_bytes, "rb") as f:
            data = f.read()
    else:
        data = gzip.decompress(gz_path_or_bytes)
    # Match id= followed by any chars until !!
    pattern = rb"\n!!FIDE!!id=([^!]+)!!\n"
    parts = re.split(pattern, data)
    for i in range(1, len(parts), 2):
        if i + 1 < len(parts) and parts[i].decode("utf-8") == tournament_id:
            return parts[i + 1]
    return None
