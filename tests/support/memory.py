"""A measurement of the most Arrow memory a read holds at once.

Two reads were rebuilt to stream in batches so a ticker-day fits a 2 GiB host: compaction's
verify and re-tune (marketlake #660) and the battery's entitlement read (marketlake #670).
Their tests bound the peak through :func:`measured`, which installs a proxy pool that counts
every byte Arrow requests through it.

Four rules keep that measurement honest. The first three were measured on pyarrow 25.0.1 for
marketlake #660.

1. Install the proxy before the read opens its file. A proxy installed afterwards counts
   nothing of that file's allocations.
2. Keep every proxy referenced for the life of the test process, in ``PROXIES``. Memory a
   threaded read allocated through a proxy can be freed later from a background thread, and
   a proxy freed before that crashed the interpreter in a later, unrelated test.
3. Take any reference read a test makes itself with ``use_threads=False``, for the same
   reason.
4. Restore the previous pool when the measurement ends, even when the read raises. With the
   restore removed, every later test allocated through the proxy and no test failed.

:func:`measured` enforces the second and fourth. The first and third are the caller's. The
first holds only when the caller opens the file inside the ``with``, and only the caller knows
which read is its reference.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pyarrow as pa

# Every proxy pool a test installs, kept alive until the process exits, per the second rule
# above.
PROXIES: list[pa.MemoryPool] = []


@contextmanager
def measured() -> Iterator[pa.MemoryPool]:
    """Route every Arrow allocation through a fresh proxy pool, then restore the pool."""
    original = pa.default_memory_pool()
    proxy = pa.proxy_memory_pool(original)
    PROXIES.append(proxy)
    pa.set_memory_pool(proxy)
    try:
        yield proxy
    finally:
        pa.set_memory_pool(original)
