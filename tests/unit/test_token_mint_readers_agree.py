"""The two token readers must accept and refuse the same values.

``control_plane.read_token_mint`` reads ``creation_timestamp`` off the token file on disk.
``schwab.SchwabVendor.token_mint_time`` reads the same field off the live client's token
metadata. Both call ``token_epoch.epoch_second_to_utc`` to convert it, so this drives one
list of values through both and asserts neither parts from the other on any of them. A
later change to only one reader's guard, or a reader routed through a different helper,
would show up here even if that reader's own tests still passed.
"""

from __future__ import annotations

import json

import pytest

from lake.control_plane import read_token_mint
from lake.schwab import SchwabVendor
from tests.support.schwab import FakeSchwabClient

VALUES = [
    1756596600,  # a plain valid epoch second
    1756596600.5,  # a valid epoch second with a fractional part
    True,
    False,
    "1756596600",  # a numeric string: the token-file policy, not the vendor-payload one
    "soon",
    1e20,  # out of range for the platform clock
    10**400,  # too large to convert to float at all
]


def _control_plane_reads(tmp_path, value) -> tuple[bool, object]:
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"creation_timestamp": value}))
    try:
        return True, read_token_mint(path)
    except ValueError:
        return False, None


def _schwab_reads(value) -> tuple[bool, object]:
    vendor = SchwabVendor(FakeSchwabClient(creation_timestamp=value))
    try:
        return True, vendor.token_mint_time()
    except Exception:  # noqa: BLE001 - any raise from this reader means "refused"
        return False, None


@pytest.mark.parametrize("value", VALUES)
def test_the_two_readers_accept_and_refuse_the_same_values(tmp_path, value):
    control_plane_accepted, control_plane_result = _control_plane_reads(tmp_path, value)
    schwab_accepted, schwab_result = _schwab_reads(value)
    assert control_plane_accepted == schwab_accepted, (
        f"value={value!r} control_plane accepted={control_plane_accepted} "
        f"schwab accepted={schwab_accepted}"
    )
    if control_plane_accepted:
        assert control_plane_result == schwab_result
