"""The journal metadata stamp: what the daemon knows and the panel cannot see.

Three facts the Now panel shows are not measurements, so no captured row carries them:
when the refresh token was minted, which tickers the daemon is capturing, and when the
dead-man ping last fired. The *dead-man check* is an external timer that alerts when
expected pings stop, so its last ping is the daemon's own liveness, not the market's.

The dashboard never reads ``~/.config``. It is a read-only query service over the lake,
and the token file beside the config is a full brokerage credential. So each of those
three facts has to arrive through the lake, and this module is how. The daemon stamps
them into the journal's metadata, and the panel reads the stamp from under
``lake_root``. That is the design's rule for the token age, for the roster, and for the
ticker list that must never come from ``tickers.yaml``.

**The mint stamp is a timestamp, never token material.** It is the epoch second the
refresh token was minted, rendered as an ISO instant. Nothing else from the token file
is read, written, or carried here.

The stamp lives at ``journal/metadata.json``, and both halves of that path are load
bearing.

1. Under ``journal/``, because the reverse scrub excludes the whole journal tree from
   its every-file-needs-a-manifest-entry pass. A stamp rewritten every minute cannot
   carry a manifest entry, and naming a new exclusion for it would widen the pass that
   catches an unmanifested file.
2. At the journal root rather than inside a ``date=`` directory, because compaction
   prunes a sealed day's directories once they are empty. A file inside one would keep
   that shell alive forever.

Two writers share the file, and each replaces only its own keys. The capture cycle
stamps the mint time and the roster. The dead-man stamps its landed ping. Both run in
the daemon's single loop thread, so the read-modify-write below is never concurrent
with itself. The write is atomic all the same: a temp file beside the target, a flush,
then one rename. So a reader meets the old stamp or the new one, never half of either.

Nothing here is durable in the journal's sense. A capture cycle counts as captured only
once its segment is fsynced, because a lost cycle is unrecoverable. A lost stamp is
rewritten the next minute, so it is worth no more than the ordinary atomic write.

Reading is total. An absent file, an unreadable one, a corrupt one, or a naive
timestamp with no offset all read as an empty record, and an empty record renders as
"not recorded yet" on the panel. A stamp that cannot be parsed must never break the
page that would have shown the capture failing.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from lake.gap import surfaces_for
from lake.paths import LakePaths
from lake.tickers import Roster

# The stamp's four keys. They are spelled once here, so a writer and the reader cannot
# drift apart on a name.
STAMPED_AT = "stamped_at"
TOKEN_MINTED_AT = "token_minted_at"
TICKERS = "tickers"
DEAD_MAN_LAST_PING = "dead_man_last_ping"


@dataclass(frozen=True)
class JournalMetadata:
    """One reading of the stamp. Every field is optional, because every writer is.

    ``stamped_at`` is when the capture side last stamped, so a reader can tell a fresh
    stamp from one left by a daemon that died hours ago. ``tickers`` maps each rostered
    ticker to the surfaces it is captured on, which is what lets the panel show a
    ticker that journaled nothing at all.
    """

    stamped_at: datetime | None = None
    token_minted_at: datetime | None = None
    tickers: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    dead_man_last_ping: datetime | None = None


def metadata_path(lake_root: Path | str) -> Path:
    """The stamp's path under one lake root."""
    return LakePaths(Path(lake_root)).journal_metadata_path


def stamp_cycle(
    lake_root: Path | str,
    *,
    at: datetime,
    token_minted_at: datetime,
    roster: Roster,
) -> None:
    """Stamp one cycle's token mint time and roster.

    ``at`` is the cycle's own instant. ``token_minted_at`` is when the refresh token in
    use was minted. ``roster`` is the tickers the cycle covered, stored as the surfaces
    each one is captured on. ``surfaces_for`` decides those, the same rule capture
    plans a cycle by and gap marking marks by, so the panel expects exactly the
    surfaces the daemon writes.

    The dead-man's ping is carried forward untouched.
    """
    _merge(
        lake_root,
        {
            STAMPED_AT: at.isoformat(),
            TOKEN_MINTED_AT: token_minted_at.isoformat(),
            TICKERS: {entry.ticker: list(surfaces_for(entry)) for entry in roster},
        },
    )


def stamp_ping(lake_root: Path | str, *, at: datetime) -> None:
    """Stamp the instant the dead-man ping last landed. Everything else carries forward."""
    _merge(lake_root, {DEAD_MAN_LAST_PING: at.isoformat()})


def read_metadata(lake_root: Path | str) -> JournalMetadata:
    """The stamp, or an empty record when there is nothing readable to report."""
    raw = _read_raw(metadata_path(lake_root))
    tickers: dict[str, tuple[str, ...]] = {}
    stamped = raw.get(TICKERS)
    if isinstance(stamped, Mapping):
        for ticker, surfaces in stamped.items():
            if isinstance(ticker, str) and isinstance(surfaces, list):
                tickers[ticker] = tuple(item for item in surfaces if isinstance(item, str))
    return JournalMetadata(
        stamped_at=_instant(raw.get(STAMPED_AT)),
        token_minted_at=_instant(raw.get(TOKEN_MINTED_AT)),
        tickers=tickers,
        dead_man_last_ping=_instant(raw.get(DEAD_MAN_LAST_PING)),
    )


def _instant(value: object) -> datetime | None:
    """An ISO string as an aware datetime, or ``None``.

    A naive stamp is refused rather than assumed local. The panel subtracts these from
    its own aware instant, and a wrong guess at the offset would report an age hours off
    rather than saying nothing.
    """
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.utcoffset() is not None else None


def _read_raw(path: Path) -> dict:
    """The stamp's JSON object, or an empty one. No failure escapes."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _merge(lake_root: Path | str, updates: Mapping[str, object]) -> None:
    """Replace the named keys and write the whole stamp back atomically.

    The temp file sits beside the target and carries the writer's pid, so two processes
    never share one. It is removed on any failure, and the rename is what publishes the
    new stamp.
    """
    target = metadata_path(lake_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {**_read_raw(target), **updates}
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    tmp = target.with_name(f"{target.name}.tmp-{os.getpid()}")
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


__all__ = [
    "DEAD_MAN_LAST_PING",
    "STAMPED_AT",
    "TICKERS",
    "TOKEN_MINTED_AT",
    "JournalMetadata",
    "metadata_path",
    "read_metadata",
    "stamp_cycle",
    "stamp_ping",
]
