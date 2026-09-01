"""Offline response and cassette diagnostics.

This is a by-hand offline tool. It reads a recorded cassette, the same JSON format the
recorder writes, and prints diagnostics for each interaction. It touches no network and
never imports ``schwab-py``. A cassette is a saved vendor response replayed offline. Its
format lives in ``lake.cassette``.

For each interaction the tool prints the endpoint, the request params, and the HTTP
status. Then it prints one of two things.

1. If the body is an error, it prints the error plainly. A Schwab gateway fault has the
   shape ``{"fault": {"detail": {"errorcode": ...}, "faultstring": ...}}``. The tool
   prints the error code and the fault string. This is how the ``protocol.http.TooBigBody``
   502 was diagnosed.
2. Otherwise it prints a field-structure dump. The dump names each field and its type,
   so a schema drift is visible without reading the whole body. For any key whose name
   contains ``time`` or ``date`` it also shows the value, so an int epoch in
   milliseconds is told apart from an ISO string.

The dump is shaped per surface. A surface is one captured data kind, either ``chains``
or ``quotes``. For the quotes surface, per symbol envelope it prints the top-level keys
and then each block: ``quote``, ``fundamental``, ``regular``, ``extended``, and
``reference``. For the chains surface it prints the top-level body keys, the
``underlying`` block, and exactly one contract. That contract is reached by walking
``callExpDateMap`` to its first expiration, then its first strike, then its first
contract. All contracts are never dumped.

Run it by hand against a recorded cassette::

    python -m lake.inspect_cassette path/to/cassette.json

Pass ``--surface chains`` or ``--surface quotes`` to limit the output to one surface.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence

from lake.cassette import Cassette, Interaction, load_cassette

# The quote blocks dumped per symbol envelope, in the order the design pins them.
QUOTE_BLOCKS = ("quote", "fundamental", "regular", "extended", "reference")
# A key whose name contains one of these has its value shown, so epoch-vs-ISO is visible.
_TIME_HINTS = ("time", "date")


def _type_name(value: object) -> str:
    """The plain type name of a value, like ``int`` or ``str``."""
    return type(value).__name__


def _is_time_key(key: str) -> bool:
    """Whether a field name looks like a time or date field."""
    lowered = key.lower()
    return any(hint in lowered for hint in _TIME_HINTS)


def _error_summary(body: Mapping[str, object]) -> str | None:
    """A one-line error summary, or ``None`` when the body is not an error.

    A Schwab gateway fault is recognized first and reported by its error code and fault
    string. A plainer ``{"error": ...}`` body is reported by its message.
    """
    fault = body.get("fault")
    if isinstance(fault, Mapping):
        detail = fault.get("detail")
        code = detail.get("errorcode") if isinstance(detail, Mapping) else None
        return f"fault: errorcode={code!r} faultstring={fault.get('faultstring')!r}"
    error = body.get("error")
    if error is not None:
        return f"error: {error!r}"
    return None


def _dump_fields(mapping: Mapping[str, object], indent: str) -> list[str]:
    """One ``field: type`` line per key, with the value shown for time and date keys."""
    lines: list[str] = []
    for key in mapping:
        value = mapping[key]
        line = f"{indent}{key}: {_type_name(value)}"
        if _is_time_key(str(key)):
            line += f" = {value!r}"
        lines.append(line)
    return lines


def _dump_block(name: str, block: object, indent: str) -> list[str]:
    """A named block's field dump, or a note when it is not a mapping."""
    if not isinstance(block, Mapping):
        return [f"{indent}{name}: <not a block: {_type_name(block)}>"]
    return [f"{indent}{name}:", *_dump_fields(block, indent + "  ")]


def _dump_quotes(body: Mapping[str, object], indent: str) -> list[str]:
    """The quotes-surface dump: per symbol envelope, its keys and its blocks."""
    lines: list[str] = []
    for symbol in body:
        envelope = body[symbol]
        lines.append(f"{indent}{symbol}:")
        if not isinstance(envelope, Mapping):
            lines.append(f"{indent}  <not an envelope: {_type_name(envelope)}>")
            continue
        lines.append(f"{indent}  keys: {', '.join(str(key) for key in envelope)}")
        for block_name in QUOTE_BLOCKS:
            if block_name in envelope:
                lines.extend(_dump_block(block_name, envelope[block_name], indent + "  "))
    return lines


def _first_contract(body: Mapping[str, object]) -> tuple[str, str, object] | None:
    """Walk ``callExpDateMap`` to the first expiration, strike, and contract.

    Returns the expiration key, the strike key, and the contract, or ``None`` when the
    map is missing or empty at any level.
    """
    exp_map = body.get("callExpDateMap")
    if not isinstance(exp_map, Mapping) or not exp_map:
        return None
    exp_key = next(iter(exp_map))
    strikes = exp_map[exp_key]
    if not isinstance(strikes, Mapping) or not strikes:
        return None
    strike_key = next(iter(strikes))
    contracts = strikes[strike_key]
    if not isinstance(contracts, Sequence) or isinstance(contracts, (str, bytes)) or not contracts:
        return None
    return str(exp_key), str(strike_key), contracts[0]


def _dump_chains(body: Mapping[str, object], indent: str) -> list[str]:
    """The chains-surface dump: top-level keys, the underlying, and one contract."""
    lines = [f"{indent}keys: {', '.join(str(key) for key in body)}"]
    if "underlying" in body:
        lines.extend(_dump_block("underlying", body["underlying"], indent))
    found = _first_contract(body)
    if found is None:
        lines.append(f"{indent}contract: <none found>")
        return lines
    exp_key, strike_key, contract = found
    lines.append(f"{indent}contract [{exp_key} / {strike_key}]:")
    if isinstance(contract, Mapping):
        lines.extend(_dump_fields(contract, indent + "  "))
    else:
        lines.append(f"{indent}  <not a contract: {_type_name(contract)}>")
    return lines


def inspect_interaction(interaction: Interaction, surface: str | None = None) -> list[str]:
    """Diagnose one interaction, or return no lines when a surface filter excludes it."""
    if surface is not None and interaction.endpoint != surface:
        return []
    lines = [
        f"[{interaction.endpoint}] params={interaction.params} status={interaction.status}",
    ]
    body = interaction.body
    if not isinstance(body, Mapping):
        lines.append(f"  <body is not a mapping: {_type_name(body)}>")
        return lines
    error = _error_summary(body)
    if error is not None:
        lines.append(f"  {error}")
        return lines
    if interaction.endpoint == "quotes":
        lines.extend(_dump_quotes(body, "  "))
    elif interaction.endpoint == "chains":
        lines.extend(_dump_chains(body, "  "))
    else:
        lines.append(f"  <unknown surface: {interaction.endpoint}>")
    return lines


def inspect_cassette(cassette: Cassette, surface: str | None = None) -> str:
    """Diagnose every interaction in a cassette and join the output into one string."""
    blocks: list[str] = []
    for interaction in cassette.interactions:
        lines = inspect_interaction(interaction, surface=surface)
        if lines:
            blocks.append("\n".join(lines))
    return "\n".join(blocks)


def build_parser() -> argparse.ArgumentParser:
    """The command-line contract for the by-hand inspector."""
    parser = argparse.ArgumentParser(
        prog="python -m lake.inspect_cassette",
        description="Inspect a recorded cassette's responses offline (no network, no schwab-py).",
    )
    parser.add_argument("path", help="Path to the cassette JSON to inspect.")
    parser.add_argument(
        "--surface",
        choices=("chains", "quotes"),
        default=None,
        help="Limit the output to one surface. Defaults to every surface.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Read a cassette from disk and print its diagnostics. Fully offline."""
    args = build_parser().parse_args(argv)
    cassette = load_cassette(args.path)
    print(inspect_cassette(cassette, surface=args.surface))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
