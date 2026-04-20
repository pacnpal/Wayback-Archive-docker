"""Heartbeat probe for web.archive.org CDX.

Runs a cheap CDX query on a schedule and tracks up/down state with
hysteresis so a single blip doesn't pause the worker. When CDX is down,
``webui.jobs.worker_loop`` stops popping new work; in-flight jobs that
fail during the outage are rescheduled with an escalating backoff
(5/10/15/20/30/45/60/120 min, then doubling 240/480/960, capped at 24h).
When CDX comes back up, deferred jobs are released in one pass.

State lives in the ``settings`` table under the keys
``wayback_probe_state`` (JSON with ``state``/``consecutive_fails``/
``consecutive_ok``) and ``wayback_state_since`` (ISO timestamp of the
last flip). It survives restarts and is visible to the dashboard banner.
"""
from __future__ import annotations
import asyncio
import dataclasses
import json
import random
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from . import events_bus, log as _log

logger = _log.get("wayback.probe")


PROBE_URL = (
    "https://web.archive.org/cdx/search/cdx"
    "?url=example.com&limit=1&output=json"
)
PROBE_TIMEOUT = 45.0    # default seconds per probe request; overridable via settings.
# IA's CDX occasionally serves trivial 1-row lookups in ~30s during slow
# periods. 45s leaves headroom above that and still sits below the 60s
# probe interval, so back-to-back probes don't overlap.
PROBE_TIMEOUT_MIN = 1.0
PROBE_TIMEOUT_MAX = 120.0
PROBE_INTERVAL = 60.0
PROBE_JITTER = 10.0
FAIL_THRESHOLD = 3      # consecutive probe failures before flipping to "down"
OK_THRESHOLD = 2        # consecutive successes before flipping back to "up"


@dataclasses.dataclass
class ProbeState:
    state: str = "unknown"
    consecutive_fails: int = 0
    consecutive_ok: int = 0

    def observe(self, ok: bool) -> Optional[str]:
        """Feed one probe result. Returns the new state name if it just
        flipped, else None."""
        if ok:
            self.consecutive_ok += 1
            self.consecutive_fails = 0
            if self.state != "up" and self.consecutive_ok >= OK_THRESHOLD:
                self.state = "up"
                return "up"
        else:
            self.consecutive_fails += 1
            self.consecutive_ok = 0
            if self.state != "down" and self.consecutive_fails >= FAIL_THRESHOLD:
                self.state = "down"
                return "down"
        return None


def get_probe_timeout() -> float:
    """Per-probe timeout in seconds. Reads the persisted setting (settable
    via the dashboard) and falls back to ``PROBE_TIMEOUT`` when unset or
    garbage. Clamped to ``[PROBE_TIMEOUT_MIN, PROBE_TIMEOUT_MAX]``."""
    from . import jobs
    with jobs.connect() as c:
        row = c.execute(
            "SELECT value FROM settings WHERE key='wayback_probe_timeout'"
        ).fetchone()
    if not row or row["value"] is None:
        return PROBE_TIMEOUT
    try:
        v = float(row["value"])
    except (TypeError, ValueError):
        return PROBE_TIMEOUT
    return max(PROBE_TIMEOUT_MIN, min(PROBE_TIMEOUT_MAX, v))


def set_probe_timeout(seconds: float) -> float:
    """Persist ``seconds`` after clamping. Returns the value actually written."""
    from . import jobs
    try:
        v = float(seconds)
    except (TypeError, ValueError):
        v = PROBE_TIMEOUT
    bounded = max(PROBE_TIMEOUT_MIN, min(PROBE_TIMEOUT_MAX, v))
    with jobs.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_probe_timeout", str(bounded)),
        )
    return bounded


def probe_once(url: str = PROBE_URL, timeout: "float | None" = None) -> bool:
    """One probe request. True iff CDX answered HTTP 200 within timeout.
    If the response object is missing both ``.status`` and ``.getcode()``
    we treat the probe as failed — this is a health check, so the safe
    default is "not up" rather than the previous fail-open behavior.
    ``timeout=None`` reads the persisted setting (falls back to
    ``PROBE_TIMEOUT``)."""
    if timeout is None:
        timeout = get_probe_timeout()
    req = urllib.request.Request(url, headers={"User-Agent": "Wayback-Archive-Dashboard/probe"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status = getattr(r, "status", None)
            if status is None:
                try:
                    status = r.getcode()
                except Exception:
                    status = None
            return status == 200
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        logger.debug("probe fail: %s", e)
        return False


_BACKOFF_MINUTES: tuple[int, ...] = (
    5, 10, 15, 20, 30, 45, 60, 120, 240, 480, 960, 1440,
)


def backoff_seconds(attempts: int) -> int:
    """Escalating wait between retry attempts when CDX is unreachable:
    5m, 10m, 15m, 20m, 30m, 45m, 60m, 120m, then roughly doubles
    (240, 480, 960) and caps at 24h."""
    if attempts < 0:
        attempts = 0
    if attempts >= len(_BACKOFF_MINUTES):
        return _BACKOFF_MINUTES[-1] * 60
    return _BACKOFF_MINUTES[attempts] * 60


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_state() -> ProbeState:
    """Read persisted state from the ``settings`` table."""
    from . import jobs  # deferred import to avoid circular
    with jobs.connect() as c:
        row = c.execute(
            "SELECT key,value FROM settings WHERE key IN ('wayback_probe_state')"
        ).fetchone()
    if not row or not row["value"]:
        return ProbeState()
    try:
        data = json.loads(row["value"])
        return ProbeState(
            state=data.get("state", "unknown"),
            consecutive_fails=int(data.get("consecutive_fails", 0)),
            consecutive_ok=int(data.get("consecutive_ok", 0)),
        )
    except Exception:
        return ProbeState()


def save_state(s: ProbeState, since_iso: Optional[str] = None) -> None:
    from . import jobs
    payload = json.dumps({
        "state": s.state,
        "consecutive_fails": s.consecutive_fails,
        "consecutive_ok": s.consecutive_ok,
    })
    # Connection is in autocommit mode (isolation_level=None), so each
    # execute is its own transaction by default. Wrap the two writes so
    # state and state_since can't desync if the process crashes between
    # them.
    with jobs.connect() as c:
        c.execute("BEGIN")
        try:
            c.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("wayback_probe_state", payload),
            )
            if since_iso:
                c.execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("wayback_state_since", since_iso),
                )
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise


def is_wayback_up() -> bool:
    """Fail-open: return True unless state has flipped to 'down'.
    'unknown' is treated as up so a fresh install runs jobs immediately."""
    return load_state().state != "down"


def run_probe_and_update() -> dict:
    """User-initiated probe ("Try now"). This is a check-only path — a
    failing manual probe must NOT push the state toward 'down' or shorten
    any deferred-job not_before, and it must NOT count against the
    scheduled backoff. A succeeding manual probe IS allowed to flip
    state up (because that's strictly good news) and release deferred
    jobs immediately.

    Returns a ``get_status()`` snapshot plus ``probe_ok`` for this
    specific attempt so the banner can render a transient hint."""
    ok = probe_once()
    flipped = None
    if ok:
        state = load_state()
        flipped = state.observe(True)
        if flipped == "up":
            logger.warning("wayback state flip -> up (manual retry)")
            save_state(state, since_iso=_now_iso())
            from . import jobs
            released = jobs.release_deferred()
            if released:
                logger.info("released %d deferred jobs (manual retry)", released)
        else:
            # Saw a success but haven't hit the 2-in-a-row threshold yet.
            # Persist the bumped ok counter, but don't touch state_since.
            save_state(state)
    # Always publish so the banner refreshes its countdown / shows the
    # user's click was received. A failing manual probe falls through
    # here without mutating any counters.
    events_bus.publish("wayback-state-changed")
    snap = get_status()
    snap["probe_ok"] = ok
    snap["flipped_to"] = flipped
    return snap


def get_status() -> dict:
    """Snapshot for the UI / logs. Reads both settings keys in a single
    SQLite connection so a concurrent ``save_state()`` commit can't
    tear the snapshot across two reads."""
    from . import jobs
    with jobs.connect() as c:
        rows = {
            r["key"]: r["value"] for r in c.execute(
                "SELECT key,value FROM settings "
                "WHERE key IN ('wayback_probe_state','wayback_state_since')"
            ).fetchall()
        }
    try:
        data = json.loads(rows.get("wayback_probe_state") or "{}")
    except (TypeError, json.JSONDecodeError):
        data = {}
    return {
        "state": data.get("state", "unknown"),
        "since": rows.get("wayback_state_since"),
        "consecutive_fails": int(data.get("consecutive_fails", 0)),
        "consecutive_ok": int(data.get("consecutive_ok", 0)),
    }


async def probe_loop(stop: asyncio.Event) -> None:
    initial = load_state()
    logger.info("probe loop start state=%s fails=%d ok=%d",
                initial.state, initial.consecutive_fails, initial.consecutive_ok)
    while not stop.is_set():
        # Reload each iteration so a concurrent manual retry (which
        # writes to the same settings keys) can't be clobbered by a
        # stale in-memory counter. Cheap single-row SELECT.
        state = load_state()
        ok = await asyncio.to_thread(probe_once)
        flipped = state.observe(ok)
        if flipped:
            logger.warning("wayback state flip -> %s (fails=%d ok=%d)",
                           flipped, state.consecutive_fails, state.consecutive_ok)
            save_state(state, since_iso=_now_iso())
            events_bus.publish("wayback-state-changed")
            if flipped == "up":
                from . import jobs
                released = jobs.release_deferred()
                if released:
                    logger.info("released %d deferred jobs", released)
        else:
            # Persist counters even without a flip so the dashboard
            # shows accurate fail/ok streaks.
            save_state(state)
        delay = PROBE_INTERVAL + random.uniform(-PROBE_JITTER, PROBE_JITTER)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass