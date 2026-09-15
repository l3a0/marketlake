"""The epoch-second policy shared by the two readers of a refresh token's mint stamp.

``schwab-py`` writes ``creation_timestamp`` as a plain epoch second beside the refresh
token. Two readers convert it to a UTC datetime: ``control_plane.read_token_mint`` off the
token file on disk, and ``schwab.SchwabVendor.token_mint_time`` off the live client's token
metadata. The value is untrusted either way. A bool is an ``int`` to Python, so
``float(True)`` is ``1.0`` and would land as a plausible 1970 timestamp with nothing raised.
A numeric string is a ``float`` to ``float()``, so it would convert too. Neither shape is
one ``schwab-py`` writes.

Writing that guard twice is what let it drift once already. Two capture surfaces got a bool
guard on their own epoch conversions before a third path, the calendar probe, still let one
through. This module is the one place the guard and the conversion live, so the two token
readers cannot disagree again.

Each reader keeps its own failure contract. A stamp ``control_plane`` cannot read is a
problem the Sunday job names in its own way, while one ``schwab.py`` cannot read is the
vendor failing to say when its own token was minted. Only the check that decides whether a
value is an epoch second is shared, and it always raises ``ValueError``; each caller
translates that into whatever its own reader promises.
"""

from __future__ import annotations

from datetime import UTC, datetime


def epoch_second_to_utc(value: object) -> datetime:
    """``value`` as a UTC datetime, read as an epoch second.

    Refuses a bool and a numeric string before converting, since neither is a shape
    ``schwab-py`` writes into ``creation_timestamp``. Also refuses whatever the conversion
    itself rejects, such as an instant too far out for the platform clock to represent.
    Every refusal raises ``ValueError``.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("creation_timestamp is not an epoch second")
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("creation_timestamp is not an epoch second") from exc
