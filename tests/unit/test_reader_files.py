"""``paths.is_reader_file``, the files any reader's lake root must hold beside its partitions.

Marketlake #949, Acceptance 7. The predicate is public so a consumer reading a mirror of the
bucket can check the mirror holds them. It must answer as ``bucket._reading_plan`` planned its
support files before the predicate existed: the actions ledger, the quarantine ledger and every
file under ``reference/``. The expected set is written out below rather than read from either
function.
"""

from __future__ import annotations

from datetime import date

import pytest

from lake import bucket
from lake.paths import is_reader_file

# Every kind of key a bucket manifest records, and a few that only look like a reader's file.
KEYS = (
    "actions/corporate_actions.jsonl",
    "quarantine.jsonl",
    "reference/security_master.parquet",
    "reference/capture_spans.parquet",
    "reference/schema_versions.parquet",
    "reference/split_checkpoint.parquet",
    "chains/ticker=SPY/date=2026-08-25.parquet",
    "quotes/ticker=SPY/date=2026-08-25.parquet",
    "bars/ticker=SPY/freq=1d/date=2026-08-25.parquet",
    "journal/date=2026-08-26/surface=chains/ticker=QQQ/seg-20260826T133000Z-4242.arrows",
    "reports/close_guard/date=2026-08-25/run.json",
    "trimmed.jsonl",
    "manifest.jsonl",
    "actions/other.jsonl",
    "references/security_master.parquet",
    "reference",
    "quarantine.jsonl.bak",
    "chains/quarantine.jsonl",
)

READER_FILES = {
    "actions/corporate_actions.jsonl",
    "quarantine.jsonl",
    "reference/security_master.parquet",
    "reference/capture_spans.parquet",
    "reference/schema_versions.parquet",
    "reference/split_checkpoint.parquet",
}


@pytest.mark.parametrize("rel", KEYS)
def test_is_reader_file_names_the_two_ledgers_and_everything_under_reference(rel: str):
    """Catches either ledger left out, the ``reference/`` prefix dropped or loosened to a bare
    ``reference``, and a match on a ledger's name in any directory.
    """
    assert is_reader_file(rel) is (rel in READER_FILES)


def test_the_reading_plan_plans_exactly_the_reader_files_as_support():
    """Every key ``_reading_plan`` planned as support is a reader file, and every reader file it
    was handed is planned. Catches the plan keeping a rule of its own beside the predicate.
    """
    latest = {rel: {"partition": rel} for rel in KEYS}

    roles, _ = bucket._reading_plan(
        latest, surface="chains", ticker="SPY", first=date(2026, 8, 25), last=date(2026, 8, 25)
    )

    support = {rel for rel, role in roles.items() if role == bucket._SUPPORT_ROLE}
    assert support == READER_FILES
    assert {rel for rel in KEYS if is_reader_file(rel)} == support


def test_the_reading_plan_asks_the_predicate(monkeypatch):
    """Catches ``_reading_plan`` spelling the set again rather than calling ``is_reader_file``,
    which would let the two drift with every test above still green.
    """
    monkeypatch.setattr(bucket, "is_reader_file", lambda rel: rel == "trimmed.jsonl")
    latest = {rel: {"partition": rel} for rel in KEYS}

    roles, _ = bucket._reading_plan(
        latest, surface="chains", ticker="SPY", first=date(2026, 8, 25), last=date(2026, 8, 25)
    )

    assert {rel for rel, role in roles.items() if role == bucket._SUPPORT_ROLE} == {"trimmed.jsonl"}
