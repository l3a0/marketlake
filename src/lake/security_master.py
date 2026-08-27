"""The security master.

Every symbol the market uses is mutable. Tickers change: FB became META, and the
Nasdaq-100 ETF traded as QQQQ until 2011. Option symbols change too. The OCC, the
Options Clearing Corporation, reissues a contract's symbol when it adjusts the
contract after a corporate action. So no external symbol is a stable primary key.

The *security master* is the reference table that fixes this. It assigns every
instrument an internal ``instrument_id`` that never changes. It maps that id to the
external identifiers the world uses, each with a validity date range. The external
identifiers are the ticker, the FIGI, and the OCC option symbol. A FIGI, the
Financial Instrument Global Identifier, is the one globally-standard security id that
is free and openly licensed. All joins in the lake run on ``instrument_id``. A ticker
rename or an OCC re-symboling adds a new *mapping row* under the same
``instrument_id`` instead of orphaning history. The same instrument then threads
through under one key.

A *mapping row* pairs one external identifier with one instrument over one date
range. The range is *half-open*: ``valid_from`` is the first day the mapping holds,
and ``valid_to`` is the first day it no longer holds. A half-open interval is written
``[valid_from, valid_to)``, so the start is included and the end is excluded. A null
``valid_to`` means the mapping is still open. This convention makes a rename exact.
The old row's ``valid_to`` and the new row's ``valid_from`` are the same date, so at
every instant exactly one row of a given kind is valid, with no overlap and no gap.

Two granularities live side by side, and each matches what it measures. A validity
range is dated, because a rename or a re-symboling is a dated event. The
``capture_start`` epoch is a moment in time, because capture begins at a specific
minute. ``capture_start`` is the instant the pipeline first recorded an instrument.
All session-slot denominators, coverage checks, and gap accounting clamp to it.
Sessions and minutes before it are out of scope, never gaps. So onboarding day reads
"onboarded 11:00," not 40 percent missing.

``kind`` and ``capture_start`` are properties of the instrument, not of a single
mapping. The table stores them on every row of the instrument, so the file stays
self-describing. Registration stamps them, and a later remapping copies them forward.

CUSIP and ISIN are deliberately excluded from the master. Schwab's equity payloads
carry a CUSIP. It stays in the raw rows under the vendor-verbatim rule. It is never
promoted to a join key here, because CUSIP is licensed IP and a database keyed on it
would owe fees to redistribute.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# The pinned schema version for this reference table. A file stamps it on every row.
MASTER_SCHEMA_VERSION = 1

# The external-identifier kinds the master maps. FIGI and ticker name an equity. The
# OCC symbol names an option contract.
ID_TYPE_TICKER = "ticker"
ID_TYPE_FIGI = "figi"
ID_TYPE_OCC = "occ_symbol"
ID_TYPES = frozenset({ID_TYPE_TICKER, ID_TYPE_FIGI, ID_TYPE_OCC})

# The instrument kinds. An equity is the underlying. An option is one contract.
KIND_EQUITY = "equity"
KIND_OPTION = "option"
KINDS = frozenset({KIND_EQUITY, KIND_OPTION})

# The lake's reference directory and the master's filename within it. This mirrors the
# fixture-lake convention: reference tables live under ``reference/``.
REFERENCE_DIR = "reference"
MASTER_FILENAME = "security_master.parquet"

# The pinned pyarrow schema. Validity is date-grained. ``capture_start`` is a UTC
# instant. ``valid_to`` is nullable, where null means an open-ended mapping.
MASTER_SCHEMA = pa.schema(
    [
        ("instrument_id", pa.int64()),
        ("id_type", pa.string()),
        ("id_value", pa.string()),
        ("valid_from", pa.date32()),
        ("valid_to", pa.date32()),
        ("kind", pa.string()),
        ("capture_start", pa.timestamp("us", tz="UTC")),
        ("schema_version", pa.int32()),
    ]
)


class SecurityMasterError(Exception):
    """Base class for every security-master error."""


class UnknownInstrument(SecurityMasterError):
    """Raised when an operation names an ``instrument_id`` the master does not hold."""

    def __init__(self, instrument_id: int) -> None:
        super().__init__(f"unknown instrument_id {instrument_id}")
        self.instrument_id = instrument_id


class AmbiguousSymbol(SecurityMasterError):
    """Raised when one symbol resolves to more than one instrument on a date.

    Clean data never triggers this. It signals a corrupt master, where two
    instruments claim the same external symbol over overlapping ranges.
    """

    def __init__(self, symbol: str, on: date, instrument_ids: list[int]) -> None:
        super().__init__(f"symbol {symbol!r} on {on.isoformat()} resolves to {instrument_ids}")
        self.symbol = symbol
        self.on = on
        self.instrument_ids = instrument_ids


class UnsupportedSchemaVersion(SecurityMasterError):
    """Raised when a file on disk carries a schema version this code cannot read."""

    def __init__(self, found: int) -> None:
        super().__init__(f"master schema version {found}, this code reads {MASTER_SCHEMA_VERSION}")
        self.found = found


@dataclass(frozen=True)
class Mapping:
    """One mapping row: one external identifier tied to one instrument over a range.

    ``valid_to`` is exclusive, and ``None`` means the mapping is still open.
    ``kind`` and ``capture_start`` repeat the instrument's own values on every row.
    ``capture_start`` is always timezone-aware and normalized to UTC.
    """

    instrument_id: int
    id_type: str
    id_value: str
    valid_from: date
    valid_to: date | None
    kind: str
    capture_start: datetime

    def valid_on(self, day: date) -> bool:
        """Whether this mapping is valid on ``day``, honoring the half-open range."""
        if day < self.valid_from:
            return False
        return self.valid_to is None or day < self.valid_to


def is_in_scope(instant: datetime, capture_start: datetime) -> bool:
    """Whether ``instant`` is at or after capture began.

    This is the clamping rule the doc pins. An instant before ``capture_start`` is
    out of scope, never a gap. Both arguments must be timezone-aware.
    """
    return instant >= capture_start


def master_path(lake_root: Path | str) -> Path:
    """The master's parquet path under a lake root: ``reference/security_master.parquet``."""
    return Path(lake_root) / REFERENCE_DIR / MASTER_FILENAME


def _require_utc(when: datetime, label: str) -> datetime:
    """Reject a naive datetime and normalize an aware one to UTC."""
    if when.tzinfo is None or when.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return when.astimezone(UTC)


class SecurityMaster:
    """The in-memory master: a set of mapping rows plus its offline operations.

    Every operation here is pure over values. Registration and remapping mutate the
    row set. Resolution reads it. Nothing reads the clock, and nothing touches disk
    except the explicit ``read`` and ``write`` entry points.
    """

    def __init__(self, mappings: Iterable[Mapping] = ()) -> None:
        self._mappings: list[Mapping] = list(mappings)

    # -- inspection ----------------------------------------------------------

    @property
    def mappings(self) -> tuple[Mapping, ...]:
        """The mapping rows, as an immutable snapshot."""
        return tuple(self._mappings)

    def __len__(self) -> int:
        return len(self._mappings)

    def __iter__(self) -> Iterator[Mapping]:
        return iter(self._mappings)

    def instrument_ids(self) -> set[int]:
        """Every ``instrument_id`` the master holds."""
        return {m.instrument_id for m in self._mappings}

    def has_instrument(self, instrument_id: int) -> bool:
        """Whether the master already holds this ``instrument_id``."""
        return any(m.instrument_id == instrument_id for m in self._mappings)

    def next_instrument_id(self) -> int:
        """The id a new instrument would take: one past the highest in use, or 1."""
        return max((m.instrument_id for m in self._mappings), default=0) + 1

    # -- registration and remapping -----------------------------------------

    def register(
        self,
        *,
        kind: str,
        capture_start: datetime,
        valid_from: date,
        ticker: str | None = None,
        figi: str | None = None,
        occ_symbol: str | None = None,
        instrument_id: int | None = None,
    ) -> int:
        """Register a new instrument and return its ``instrument_id``.

        This assigns a fresh, stable id and stamps ``capture_start``, the epoch this
        instrument's capture begins at. It then opens one mapping row per external
        identifier supplied, each starting at ``valid_from`` with an open range. At
        least one of ``ticker``, ``figi``, or ``occ_symbol`` is required. Pass an
        explicit ``instrument_id`` only to reserve a specific new id. Registering an
        id the master already holds is an error, because ids never change and
        registration is for new instruments alone. Use ``remap`` to add a symbol to
        an existing instrument.
        """
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {sorted(KINDS)}, got {kind!r}")
        stamped = _require_utc(capture_start, "capture_start")
        supplied = {
            ID_TYPE_TICKER: ticker,
            ID_TYPE_FIGI: figi,
            ID_TYPE_OCC: occ_symbol,
        }
        present = {t: v for t, v in supplied.items() if v is not None}
        if not present:
            raise ValueError("register needs at least one external identifier")

        if instrument_id is None:
            instrument_id = self.next_instrument_id()
        elif self.has_instrument(instrument_id):
            raise ValueError(
                f"instrument_id {instrument_id} already exists; register is for new instruments"
            )

        for id_type, id_value in present.items():
            self._mappings.append(
                Mapping(
                    instrument_id=instrument_id,
                    id_type=id_type,
                    id_value=id_value,
                    valid_from=valid_from,
                    valid_to=None,
                    kind=kind,
                    capture_start=stamped,
                )
            )
        return instrument_id

    def remap(
        self,
        instrument_id: int,
        id_type: str,
        new_value: str,
        effective: date,
    ) -> None:
        """Thread a symbol change onto an existing instrument, keeping its id.

        A ticker rename and an OCC re-symboling are the same operation over different
        identifier kinds. Both close the instrument's currently-open mapping of that
        kind at ``effective`` and open a new one from ``effective``. Because the range
        is half-open, the two rows share the boundary date. The instrument's history
        stays intact under one ``instrument_id``. ``effective`` must fall after the
        open mapping's ``valid_from``.
        """
        if id_type not in ID_TYPES:
            raise ValueError(f"id_type must be one of {sorted(ID_TYPES)}, got {id_type!r}")
        if not self.has_instrument(instrument_id):
            raise UnknownInstrument(instrument_id)

        open_index = self._open_mapping_index(instrument_id, id_type)
        if open_index is None:
            raise SecurityMasterError(
                f"instrument {instrument_id} has no open {id_type} mapping to remap"
            )
        current = self._mappings[open_index]
        if effective <= current.valid_from:
            raise ValueError(
                f"effective {effective.isoformat()} must be after the open mapping's "
                f"valid_from {current.valid_from.isoformat()}"
            )

        self._mappings[open_index] = replace(current, valid_to=effective)
        self._mappings.append(
            Mapping(
                instrument_id=instrument_id,
                id_type=id_type,
                id_value=new_value,
                valid_from=effective,
                valid_to=None,
                kind=current.kind,
                capture_start=current.capture_start,
            )
        )

    def _open_mapping_index(self, instrument_id: int, id_type: str) -> int | None:
        """The list index of the instrument's open mapping of this kind, or ``None``."""
        for index, m in enumerate(self._mappings):
            if m.instrument_id == instrument_id and m.id_type == id_type and m.valid_to is None:
                return index
        return None

    # -- as-of resolution ----------------------------------------------------

    def resolve(self, symbol: str, on: date, id_type: str | None = None) -> int | None:
        """Resolve an external symbol to its ``instrument_id`` as of ``on``.

        This honors validity ranges, so an old ticker resolves against the date it was
        current. Pass ``id_type`` to disambiguate a symbol that could appear as more
        than one kind. Returns ``None`` when no mapping is valid on ``on``. Raises
        ``AmbiguousSymbol`` when a corrupt master maps one symbol to several
        instruments on the same date.
        """
        matches = {
            m.instrument_id
            for m in self._mappings
            if m.id_value == symbol and (id_type is None or m.id_type == id_type) and m.valid_on(on)
        }
        if not matches:
            return None
        if len(matches) > 1:
            raise AmbiguousSymbol(symbol, on, sorted(matches))
        return next(iter(matches))

    def symbol_at(self, instrument_id: int, on: date, id_type: str = ID_TYPE_TICKER) -> str | None:
        """Resolve an ``instrument_id`` back to its symbol of ``id_type`` valid on ``on``.

        Returns ``None`` when the instrument had no mapping of that kind on ``on``.
        """
        for m in self._mappings:
            if m.instrument_id == instrument_id and m.id_type == id_type and m.valid_on(on):
                return m.id_value
        return None

    # -- capture_start -------------------------------------------------------

    def capture_start_of(self, instrument_id: int) -> datetime:
        """The instrument's ``capture_start`` epoch, timezone-aware in UTC."""
        for m in self._mappings:
            if m.instrument_id == instrument_id:
                return m.capture_start
        raise UnknownInstrument(instrument_id)

    def in_scope(self, instrument_id: int, instant: datetime) -> bool:
        """Whether ``instant`` is in scope for the instrument: at or after capture began."""
        return is_in_scope(instant, self.capture_start_of(instrument_id))

    # -- parquet round-trip --------------------------------------------------

    def to_table(self) -> pa.Table:
        """Render the master as a pyarrow table in the pinned schema."""
        return pa.table(
            {
                "instrument_id": [m.instrument_id for m in self._mappings],
                "id_type": [m.id_type for m in self._mappings],
                "id_value": [m.id_value for m in self._mappings],
                "valid_from": [m.valid_from for m in self._mappings],
                "valid_to": [m.valid_to for m in self._mappings],
                "kind": [m.kind for m in self._mappings],
                "capture_start": [m.capture_start for m in self._mappings],
                "schema_version": [MASTER_SCHEMA_VERSION] * len(self._mappings),
            },
            schema=MASTER_SCHEMA,
        )

    @classmethod
    def from_table(cls, table: pa.Table) -> SecurityMaster:
        """Build a master from a pyarrow table in the pinned schema."""
        rows = table.to_pylist()
        for row in rows:
            version = row["schema_version"]
            if version != MASTER_SCHEMA_VERSION:
                raise UnsupportedSchemaVersion(version)
        return cls(
            Mapping(
                instrument_id=row["instrument_id"],
                id_type=row["id_type"],
                id_value=row["id_value"],
                valid_from=row["valid_from"],
                valid_to=row["valid_to"],
                kind=row["kind"],
                capture_start=row["capture_start"],
            )
            for row in rows
        )

    def write(self, path: Path | str) -> Path:
        """Write the master to a parquet file at ``path``, creating parent dirs."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(self.to_table(), path)
        return path

    @classmethod
    def read(cls, path: Path | str) -> SecurityMaster:
        """Read a master from a parquet file at ``path``."""
        return cls.from_table(pq.read_table(Path(path)))
