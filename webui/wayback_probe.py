"""Outage-state helpers for the Wayback worker gate.

This module used to run an active heartbeat probe against CDX on a
60-second cadence. That heartbeat was itself CDX traffic we had to
budget around IA's 60 req/min ceiling, and a user mashing the "Try
now" button could push us closer to a 429. Both paths are now gone:
state transitions are driven passively by ``webui.rate_limit`` from
the outcome of real caller traffic (``observe_ok`` / ``observe_429``).

What stays here:

- ``ProbeState`` + ``load_state`` / ``save_state`` — persisted state
  flags consumed by ``is_wayback_up`` below and by the dashboard
  banner.
- ``is_wayback_up`` — fail-open gate: "True unless state=='down'".
  The worker loop consults this before popping pending jobs; when a
  429 has installed a hard block, this returns False and jobs that
  fail during the block get deferred via the escalating-backoff
  machinery. When the block expires, a successful CDX call flips the
  state back up and ``release_deferred()`` drains the queue.
- ``backoff_seconds`` — the escalating retry schedule used by
  ``jobs.defer_for_outage`` (5/10/15/20/30/45/60/120m, doubling,
  capped at 24h). Unchanged.
- ``get_status`` — snapshot used by ``/api/wayback-status``.

The ``FAIL_THRESHOLD`` / ``OK_THRESHOLD`` constants stay available for
``rate_limit._mark_wayback_state`` to satisfy when flipping state —
those thresholds used to gate the active probe so a single blip
didn't pause the worker. With passive observation, a 429 is never a
blip, so the rate-limit module flips state immediately while still
writing the thresholds into the persisted counters so anyone reading
the raw DB row still sees a consistent picture.
"""
from __future__ import annotations
import dataclasses
import json
from datetime import datetime, timezone
from typing import Optional

from . import log as _log

logger = _log.get("wayback.probe")


FAIL_THRESHOLD = 3
OK_THRESHOLD = 2


@dataclasses.dataclass
class ProbeState:
    state: str = "unknown"
    consecutive_fails: int = 0
    consecutive_ok: int = 0

    def observe(self, ok: bool) -> Optional[str]:
        """Feed one probe result. Returns the new state name if it just
        flipped, else None. Retained for callers (tests, legacy paths)
        that still track ok/fail streaks."""
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
    from . import jobs
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
