"""The cassette format.

A cassette is a saved vendor response replayed offline, so a test never touches the
network. This module defines the on-disk format and the matching rule. The
cassette-backed fake vendor that replays it lives under ``tests/support``. The live
recorder that captures a real Schwab call into this format is a by-hand tool that a
later deliverable adds. Both sides share this one format.

A cassette is JSON. It holds a version, an optional token mint time the fake vendor
replays, and a list of interactions. Each interaction pairs a request key with the
verbatim response the vendor gave for it.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

CASSETTE_VERSION = 1


class CassetteError(Exception):
    """Raised for a malformed cassette or a request with no recorded interaction."""


@dataclass(frozen=True)
class Interaction:
    """One recorded request and its verbatim response.

    ``endpoint`` names the vendor call, like ``"chains"`` or ``"quotes"``. ``params``
    are the identifying request parameters that key the match, like
    ``{"symbol": "SPY"}`` or ``{"symbols": ["SPY", "QQQ"]}``. The match is exact on
    both, so a cassette replays deterministically.
    """

    endpoint: str
    params: dict
    status: int
    body: dict
    headers: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Cassette:
    """A set of recorded interactions, replayed offline."""

    interactions: tuple[Interaction, ...]
    token_mint_time: str | None = None
    cassette_version: int = CASSETTE_VERSION

    def find(self, endpoint: str, params: dict) -> Interaction:
        """The interaction recorded for this request, or an error.

        The match is exact on endpoint and params. A miss raises rather than falling
        back, so a test can never silently reach past its recording.
        """
        for interaction in self.interactions:
            if interaction.endpoint == endpoint and interaction.params == params:
                return interaction
        raise CassetteError(f"no recorded interaction for endpoint={endpoint!r} params={params!r}")


def _canonical_params(params: dict) -> dict:
    """Normalize params so a recorded list and a queried sequence compare equal."""
    out: dict = {}
    for key, value in params.items():
        out[key] = list(value) if isinstance(value, (list, tuple)) else value
    return out


def load_cassette(path: str | Path) -> Cassette:
    """Read a cassette from disk."""
    raw = json.loads(Path(path).read_text())
    version = raw.get("cassette_version", CASSETTE_VERSION)
    if version != CASSETTE_VERSION:
        raise CassetteError(f"unsupported cassette_version {version!r}")
    interactions = tuple(
        Interaction(
            endpoint=item["endpoint"],
            params=_canonical_params(item.get("params", {})),
            status=item["status"],
            body=item["body"],
            headers=item.get("headers", {}),
        )
        for item in raw["interactions"]
    )
    return Cassette(
        interactions=interactions,
        token_mint_time=raw.get("token_mint_time"),
        cassette_version=version,
    )


def dump_cassette(cassette: Cassette, path: str | Path) -> None:
    """Write a cassette to disk as formatted JSON."""
    payload = {
        "cassette_version": cassette.cassette_version,
        "token_mint_time": cassette.token_mint_time,
        "interactions": [asdict(interaction) for interaction in cassette.interactions],
    }
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
