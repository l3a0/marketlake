"""A record of the path every config, roster and token read receives.

The installed daemon plist starts ``python -m lake.daemon`` with no arguments, so in
production the loop receives ``None`` for its config, roster and token paths and hands
that ``None`` on to every helper that reads one of the three files. Each helper accepts
``None`` today. What a test has to catch is a later change that mishandles it, for example
by turning it into the string ``"None"``. Most helpers answer a file that will not load by
standing down in silence, so watching their effects misses the change. Watching the reads
catches it: every read under the plist's shape has to receive ``None``, or the default
token path where the code resolves the token itself.

``PathReads.install`` wraps the three readers in every loaded ``lake`` module that binds
one, under whatever name the module binds it. The three are ``lake.config.load_config``,
``lake.tickers.load_tickers`` and ``lake.control_plane.read_token_mint``. Each wrapper
records the path it was handed and the function that called it, so a failure names the
helper rather than only the file, and then calls the real reader with the same arguments.
``install`` wraps the defining modules too, so the record also catches a call spelled
``config.load_config(...)``, and a module first imported during the run binds the wrapper.
``monkeypatch`` puts every binding it replaced back when the test ends.

What it does not put back is a binding it never made. A module first imported during a
spied test keeps that test's wrapper afterwards, and the wrapper still calls the real
reader but records into a list nothing reads any more. So ``install`` wraps a binding
whose innermost function is a real reader, not only the real reader itself, and
``assert_no_reader_escaped`` fails on a binding that is a real reader or another spy's
wrapper. Either one is a read the test cannot see.

The price is named here rather than hidden. A reader held somewhere other than a module
attribute, such as a default argument, a closure or a class attribute, escapes the spy
and the check alike. None exists in ``src/lake`` today.

The vendor's own token read goes through ``SchwabVendor.from_token``. Every daemon test
that runs the production cycle runner or the close+5 fill replaces it, because the real
one builds the ``schwab-py`` client. ``vendor_factory`` builds that replacement and
records its path in the same list.
"""

from __future__ import annotations

import functools
import inspect
import sys
from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType

import pytest

from lake.config import load_config
from lake.control_plane import read_token_mint
from lake.tickers import load_tickers

# Each reader under the name the record uses for it. The parameter that carries the path
# is the reader's first, whatever it is called.
READERS: dict[str, Callable[..., object]] = {
    "load_config": load_config,
    "load_tickers": load_tickers,
    "read_token_mint": read_token_mint,
}
FROM_TOKEN = "SchwabVendor.from_token"


@dataclass(frozen=True)
class Read:
    """One read: which reader, the function that called it, and the path it was handed."""

    reader: str
    caller: str
    path: object


def _caller(depth: int) -> str:
    """The module and qualified name of the function ``depth`` frames above this one."""
    frame = sys._getframe(depth + 1)
    return f"{frame.f_globals.get('__name__')}.{frame.f_code.co_qualname}"


def _innermost(value: object) -> object:
    """The function a spy's wrapper calls, or ``value`` itself when it wraps nothing.

    One layer is all there is. ``install`` replaces a wrapper rather than wrapping it, so
    no binding ever carries two.
    """
    return getattr(value, "__wrapped__", value)


def _lake_modules() -> list[ModuleType]:
    return [
        module
        for name, module in list(sys.modules.items())
        if module is not None and (name == "lake" or name.startswith("lake."))
    ]


class PathReads:
    """The reads one test made, in order, through the wrapped readers."""

    def __init__(self) -> None:
        self.reads: list[Read] = []
        self._wrappers: set[int] = set()

    @classmethod
    def install(cls, monkeypatch: pytest.MonkeyPatch) -> PathReads:
        """Wrap every binding of the three readers in every loaded ``lake`` module."""
        spy = cls()
        wrappers = {id(real): spy._wrap(name, real) for name, real in READERS.items()}
        for module in _lake_modules():
            for attribute, value in list(vars(module).items()):
                # The innermost function, so a wrapper an earlier test left behind on a
                # module it imported is replaced like the real reader it calls.
                wrapper = wrappers.get(id(_innermost(value)))
                if wrapper is not None:
                    monkeypatch.setattr(module, attribute, wrapper)
        spy._wrappers = {id(wrapper) for wrapper in wrappers.values()}
        return spy

    def _wrap(self, name: str, real: Callable[..., object]) -> Callable[..., object]:
        signature = inspect.signature(real)
        first = next(iter(signature.parameters))

        @functools.wraps(real)
        def read(*args: object, **kwargs: object) -> object:
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            self.reads.append(Read(name, _caller(1), bound.arguments[first]))
            return real(*args, **kwargs)

        return read

    def vendor_factory(self, vendor: object) -> type:
        """A stand-in for ``SchwabVendor`` whose ``from_token`` records its path.

        It hands back ``vendor`` and ignores the path otherwise, so a test that records
        the default token path never reads or writes the file there.
        """
        reads = self.reads

        class _Stub:
            @staticmethod
            def from_token(path, *, api_key, app_secret, clock=None):
                reads.append(Read(FROM_TOKEN, _caller(1), path))
                return vendor

        return _Stub

    def of(self, *readers: str) -> list[Read]:
        """The reads the named readers received, in order."""
        return [read for read in self.reads if read.reader in readers]

    def callers(self, *readers: str) -> set[str]:
        """The functions that called the named readers."""
        return {read.caller for read in self.of(*readers)}

    def assert_no_reader_escaped(self) -> None:
        """Fail when a loaded ``lake`` module binds a reader this spy does not record.

        That is the real reader, or a wrapper another spy made. Call this after the run,
        when every module the run imported is loaded.
        """
        real = {id(reader): name for name, reader in READERS.items()}
        escaped = sorted(
            f"{module.__name__}.{attribute} reaches {real[id(_innermost(value))]} unrecorded"
            for module in _lake_modules()
            for attribute, value in vars(module).items()
            if id(_innermost(value)) in real and id(value) not in self._wrappers
        )
        assert not escaped, f"reads the spy cannot see: {escaped}"
