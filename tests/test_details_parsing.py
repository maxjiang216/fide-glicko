"""Time-control parsing and details dedupe (audit fixes)."""

import pyarrow as pa
import pytest

from coverage import first_row_per_tournament
from get_tournament_details import parse_time_control


@pytest.mark.parametrize(
    "raw,code",
    [
        ("Standard: 60' + 30''", "S"),
        ("Rapid: 15-15 min/all", "R"),
        ("Blitz: 5 min", "B"),
        ("standard", "S"),
        ("RAPID 25 min", "R"),
    ],
)
def test_parse_time_control_real_values(raw, code):
    assert parse_time_control(raw) == (code, False)


@pytest.mark.parametrize("raw", ["", None, "   ", "Classical: 90 min", "??"])
def test_unknown_time_control_is_not_standard(raw):
    assert parse_time_control(raw) == (None, True)


def test_first_row_per_tournament():
    t = pa.Table.from_pylist(
        [
            {"tournament_id": "1", "v": "a"},
            {"tournament_id": "2", "v": "b"},
            {"tournament_id": "1", "v": "c"},
        ]
    )
    assert first_row_per_tournament(t).to_pylist() == [
        {"tournament_id": "1", "v": "a"},
        {"tournament_id": "2", "v": "b"},
    ]
