"""Tests for the cross-thread publish path in webui.events_bus."""
from __future__ import annotations
import asyncio
import importlib


def _fresh_bus():
    import webui.events_bus as b
    importlib.reload(b)
    return b


def test_publish_same_loop_delivers_directly():
    bus = _fresh_bus()

    async def scenario():
        q = bus.subscribe()
        bus.publish("jobs-changed")
        event, _ = await asyncio.wait_for(q.get(), timeout=0.5)
        return event

    got = asyncio.run(scenario())
    assert got == "jobs-changed"


def test_publish_from_worker_thread_is_delivered_via_call_soon_threadsafe():
    """Regression: publish() used to call put_nowait directly on every
    queue regardless of thread. With a subscriber waiting, that races
    the event loop's internal state. The bus must now hop the payload
    back onto the owner loop via call_soon_threadsafe."""
    bus = _fresh_bus()

    async def scenario():
        q = bus.subscribe()
        # Dispatch publish() from a worker thread; must not corrupt q.
        await asyncio.to_thread(bus.publish, "wayback-state-changed")
        event, _ = await asyncio.wait_for(q.get(), timeout=0.5)
        return event

    got = asyncio.run(scenario())
    assert got == "wayback-state-changed"


def test_publish_with_no_subscribers_is_noop():
    """If nobody is listening (e.g. at import time before the SSE route
    is hit), publish shouldn't raise or capture a loop."""
    bus = _fresh_bus()
    bus.publish("whatever")
    # No assertion needed — reaching here without error is the test.


def test_unsubscribe_last_resets_owner_loop():
    """When every SSE client disconnects, the captured owner loop must
    clear so the next subscribe() — which may come from a fresh loop in
    tests or a reloaded worker — captures the right one."""
    bus = _fresh_bus()

    async def first():
        q = bus.subscribe()
        assert bus._owner_loop is not None
        bus.unsubscribe(q)
        assert bus._owner_loop is None

    asyncio.run(first())
