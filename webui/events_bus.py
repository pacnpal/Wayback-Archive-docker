"""In-process asyncio fan-out bus for Server-Sent Events.

Subscribers (SSE clients) each get an asyncio.Queue; publishers call
`publish(event)` which pushes non-blocking to every queue. Queues that
fill up drop the oldest items so one stuck client can't starve others.

Thread model: ``asyncio.Queue`` is not thread-safe. Subscribers capture
the event loop at ``subscribe()`` time; ``publish()`` routes the put
back onto that loop with ``call_soon_threadsafe`` when it's invoked
from a worker thread (e.g. ``asyncio.to_thread``). Same-thread publishes
stay on the fast path.
"""
from __future__ import annotations
import asyncio
import threading

_subscribers: set[asyncio.Queue] = set()
_MAX_QUEUE = 64
# The loop that owns the subscriber queues. Captured lazily at the first
# subscribe() call; all subsequent publishes route through this loop
# when called from a non-loop thread. Worker processes / tests that
# never subscribe leave this None and publish becomes a no-op.
_owner_loop: "asyncio.AbstractEventLoop | None" = None
# Guards _subscribers and _owner_loop. The GIL would prevent crashes on
# its own, but we now have cross-thread publishes (manual retry via
# asyncio.to_thread), so an explicit lock makes the thread contract
# visible and future-proofs against free-threaded CPython.
_registry_lock = threading.Lock()


def subscribe() -> asyncio.Queue:
    global _owner_loop
    q: asyncio.Queue = asyncio.Queue(maxsize=_MAX_QUEUE)
    with _registry_lock:
        if _owner_loop is None:
            try:
                _owner_loop = asyncio.get_running_loop()
            except RuntimeError:
                _owner_loop = None
        _subscribers.add(q)
    return q


def unsubscribe(q: asyncio.Queue) -> None:
    global _owner_loop
    with _registry_lock:
        _subscribers.discard(q)
        # Reset the captured loop when the last subscriber leaves so a
        # later subscribe() on a different loop (fresh TestClient, new
        # uvicorn worker, reloaded tests) captures the right one.
        if not _subscribers:
            _owner_loop = None


def _deliver(q: asyncio.Queue, payload: tuple) -> None:
    """Single-queue put that drops the oldest item when full. Must run
    on the loop that owns ``q``."""
    try:
        q.put_nowait(payload)
    except asyncio.QueueFull:
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            q.put_nowait(payload)
        except Exception:
            pass


def publish(event: str, data: str = "1") -> None:
    """Fire-and-forget from sync or async code, including worker threads.
    Cross-thread publishes are re-scheduled on the owning loop so queue
    mutation stays on a single thread."""
    with _registry_lock:
        subs = list(_subscribers)
        target = _owner_loop
    if not subs:
        return
    try:
        current = asyncio.get_running_loop()
    except RuntimeError:
        current = None
    payload = (event, data)
    if target is not None and current is not target:
        # Different thread (or no running loop here) → hop to the owner.
        for q in subs:
            try:
                target.call_soon_threadsafe(_deliver, q, payload)
            except RuntimeError:
                # Loop is closed — nothing useful to do.
                pass
        return
    # Fast path: we're on the same loop that owns the queues.
    for q in subs:
        _deliver(q, payload)
