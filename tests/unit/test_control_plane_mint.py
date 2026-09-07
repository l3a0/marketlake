"""``read_token_mint`` decides from one field of one file, so it is a unit test.

The token file is ``schwab-py``'s shape: ``creation_timestamp`` beside a ``token``
object. Only the timestamp is read. Every failure is a ``ValueError`` the Sunday job
turns into a named problem, never a skip.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from lake.control_plane import read_token_mint

MINTED = 1756596600  # 2026-08-30 23:30:00 UTC


def test_reads_creation_timestamp_as_an_aware_utc_datetime(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"creation_timestamp": MINTED, "token": {"access_token": "x"}}))
    mint = read_token_mint(path)
    assert mint == datetime.fromtimestamp(MINTED, tz=UTC)
    assert mint.tzinfo is not None


def test_a_missing_file_raises(tmp_path):
    with pytest.raises(ValueError, match="unreadable"):
        read_token_mint(tmp_path / "absent.json")


def test_a_file_that_is_not_json_raises(tmp_path):
    path = tmp_path / "token.json"
    path.write_text("not json")
    with pytest.raises(ValueError, match="not JSON"):
        read_token_mint(path)


def test_a_file_without_the_field_raises(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"token": {}}))
    with pytest.raises(ValueError, match="creation_timestamp"):
        read_token_mint(path)


def test_a_non_numeric_timestamp_raises(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"creation_timestamp": "soon"}))
    with pytest.raises(ValueError, match="epoch second"):
        read_token_mint(path)


def test_an_out_of_range_timestamp_raises(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"creation_timestamp": 1e20}))
    with pytest.raises(ValueError, match="epoch second"):
        read_token_mint(path)


def test_a_bool_or_numeric_string_is_refused(tmp_path):
    path = tmp_path / "token.json"
    for stamp in (True, "1756596600"):
        path.write_text(json.dumps({"creation_timestamp": stamp}))
        with pytest.raises(ValueError, match="epoch second"):
            read_token_mint(path)
