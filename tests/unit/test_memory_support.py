"""The proxy-pool measurement the #660, #670 and #671 memory tests depend on.

Every memory test reads only ``max_memory()`` inside one ``with``, so none of them notices when
``measured`` leaves its proxy installed, forgets it, or wraps the wrong pool. These tests check
the helper's own four rules from outside.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from tests.support.memory import PROXIES, measured

MIB = 1 << 20


def _routes_nothing_through(proxy: pa.MemoryPool) -> bool:
    """Whether an allocation made now through the default pool leaves ``proxy`` untouched."""
    before = proxy.bytes_allocated()
    buffer = pa.allocate_buffer(MIB)
    untouched = proxy.bytes_allocated() == before
    del buffer
    return untouched


def test_measured_counts_through_a_proxy_of_the_pool_it_replaces_and_then_restores_it():
    backend = pa.default_memory_pool().backend_name

    with measured() as proxy:
        assert proxy.backend_name == backend, "the proxy wraps the pool it replaces"
        buffer = pa.allocate_buffer(MIB)
        assert proxy.max_memory() >= MIB, "an allocation inside the block is counted"
        del buffer

    assert any(kept is proxy for kept in PROXIES), "the proxy outlives the block"
    assert pa.default_memory_pool().backend_name == backend, "the original pool is back"
    assert _routes_nothing_through(proxy), "nothing after the block is counted"


def test_measured_restores_the_pool_when_the_block_raises():
    backend = pa.default_memory_pool().backend_name

    with pytest.raises(RuntimeError), measured() as proxy:
        raise RuntimeError("the read failed")

    assert pa.default_memory_pool().backend_name == backend
    assert _routes_nothing_through(proxy)
