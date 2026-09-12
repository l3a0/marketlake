"""Capture spans on disk: the parquet round-trip and the read failure modes."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake.capture_spans import (
    CaptureSpan,
    CaptureSpans,
    SpansUnreadable,
    spans_path,
)

START = datetime(2019, 1, 2, 14, 30, tzinfo=UTC)


def test_write_then_read_round_trips_open_and_closed_spans(tmp_path: Path):
    spans = CaptureSpans()
    spans.open_span(1, START, options=True)
    spans.close_span(1, datetime(2019, 1, 10, 21, 0, tzinfo=UTC))
    spans.open_span(1, datetime(2019, 1, 20, 14, 30, tzinfo=UTC), options=True)
    spans.open_span(2, START, options=False)

    path = spans_path(tmp_path)
    spans.write(path)
    assert path == tmp_path / "reference" / "capture_spans.parquet"

    read_back = CaptureSpans.read(path)
    assert set(read_back.spans) == set(spans.spans)
    assert read_back.spans_of(1)[0] == CaptureSpan(
        1, START, datetime(2019, 1, 10, 21, 0, tzinfo=UTC), True
    )
    assert read_back.has_open_span(1) is True
    assert read_back.has_open_span(2) is True


def test_write_leaves_no_temp_file_behind(tmp_path: Path):
    spans = CaptureSpans()
    spans.open_span(1, START, options=False)
    path = spans_path(tmp_path)
    spans.write(path)
    leftovers = list(path.parent.glob("*"))
    assert leftovers == [path]


def test_a_torn_file_raises_spans_unreadable(tmp_path: Path):
    path = spans_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not parquet at all")
    with pytest.raises(SpansUnreadable):
        CaptureSpans.read(path)


def test_an_absent_file_raises_oserror(tmp_path: Path):
    with pytest.raises(OSError):
        CaptureSpans.read(spans_path(tmp_path))
