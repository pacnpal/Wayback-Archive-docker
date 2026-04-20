"""Process-wide rate limiter + outage gate for Internet Archive CDX.

Enforces a hard local ceiling on CDX requests so a single dashboard
instance cannot trip IA's 60 req/min cap — going past it trades a few
slow hours for a firewall-level IP ban that doubles on each repeat
(1h, 2h, 4h, ...). Coordination is via the dashboard SQLite DB so the
FastAPI process and every spawned shim subprocess share the same
ceiling.

Two layers:

1. **Sliding-window gate**: caps CDX calls at ``CDX_LIMIT_PER_MIN``
   (default 50, below IA's 60) over any trailing 60-second window.
   ``acquire()`` blocks until a slot is available; if the ``BEGIN
   IMMEDIATE`` lock is busy it retries, so concurrent gate crossings
   never double-count. Events are stored in a dedicated table so the
   check is a single ``COUNT(*)`` against a small time-window index.

2. **Hard block on 429 / outage**: when any caller observes a 429 from
   CDX (or a Retry-After header points into the future), every
   subsequent acquire blocks until ``cdx_block_until`` elapses. The
   block tier escalates per IA's published behavior (1h, 2h, 4h, ...
   capped at 24h) so repeated offenses back off more aggressively than
   a single blip. A successful CDX response that happens at least an
   hour after the last 429 clears the tier back to 0.

The legacy active heartbeat probe that used to drive outage detection
has been removed — it was itself CDX traffic we had to budget around.
State transitions are now driven purely by the outcome of real caller
traffic: ``observe_429()`` marks the gate down, ``observe_ok()`` clears
it when IA is back. This keeps our in-flight budget 100% user-visible
work instead of spending requests on probing.
"""
from __future__ import annotations
import json
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional

from . import log as _log

logger = _log.get("wayback.ratelimit")


# 10 req/min below IA's 60/min cap — a safety margin for the lag
# between our sliding-window prune and the server's count, for retries
# the upstream library may fire inside a single acquire, and for any
# request that arrives between the gate releasing a caller and the
# HTTP call actually landing at IA.
CDX_LIMIT_PER_MIN = 50
CDX_WINDOW_SECONDS = 60.0

# Exponential hard-block ladder per IA's published rule: "each
# subsequent violation doubles the blocking time". Tier N = 1h ×
# 2^(N-1), so 1h, 2h, 4h, 8h, 16h, 32h, ... The first 429 locks out
# all CDX traffic for a full 60 minutes to match IA's firewall block
# duration; the user explicitly directed that we wait the full hour
# on a fresh 429 rather than probing again sooner.
_BLOCK_TIER_BASE_SECONDS = 3600      # first tier = 1h
# Sanity cap on the exponential — after ~8 consecutive violations
# (256h = 10.6d) the doubling would grow indefinitely. Seven days is
# longer than any plausible IA outage and gives operators a clear
# "something's very wrong, investigate" signal if the ladder walks
# all the way up. Still a hard stop so misbehaving state can't wedge
# the gate shut forever.
_BLOCK_TIER_CAP_SECONDS = 7 * 24 * 3600

# If a 429 hasn't happened in this long, the next one resets the tier
# to 1 instead of escalating — so an overnight IA outage doesn't leave
# tomorrow's first blip with a multi-day cooldown.
_TIER_DECAY_SECONDS = 24 * 3600


def _seconds_for_tier(tier: int) -> int:
    """1h × 2^(tier-1), capped at _BLOCK_TIER_CAP_SECONDS. Tier < 1
    shouldn't happen (the ladder starts at 1), but clamp defensively."""
    if tier < 1:
        tier = 1
    # 2 ** 30 hours would overflow before practical doubling does,
    # but cap the exponent anyway so an out-of-range DB row can't
    # produce a nonsense int.
    exponent = min(tier - 1, 30)
    return min(_BLOCK_TIER_BASE_SECONDS * (2 ** exponent), _BLOCK_TIER_CAP_SECONDS)

# Acquire() polls this often when waiting for a slot. Short enough
# that stop/shutdown feels responsive, long enough that contention on
# the SQLite lock stays low under normal load.
_POLL_INTERVAL_SECONDS = 1.0

# Upper bound on any single call to ``acquire()`` so an HTTP-serving
# thread doesn't hang forever behind a long IP block. Callers that
# exceed this raise ``RateLimitTimeout`` — the dashboard surfaces that
# to the user; the worker loop treats it like any other outage and
# defers the job via the existing backoff machinery.
ACQUIRE_TIMEOUT_SECONDS = 90.0


class RateLimitTimeout(RuntimeError):
    """``acquire()`` waited ``ACQUIRE_TIMEOUT_SECONDS`` without a slot."""


def _connect() -> sqlite3.Connection:
    # Imported here to avoid a circular at import time — jobs imports
    # wayback which imports rate_limit.
    from . import jobs
    return jobs.connect()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().replace(microsecond=0).isoformat()


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _ensure_schema(c: sqlite3.Connection) -> None:
    c.execute(
        "CREATE TABLE IF NOT EXISTS cdx_rate_events ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "ts REAL NOT NULL)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_cdx_rate_events_ts "
        "ON cdx_rate_events(ts)"
    )


def _set(c: sqlite3.Connection, key: str, value: str) -> None:
    c.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _get(c: sqlite3.Connection, key: str) -> Optional[str]:
    row = c.execute(
        "SELECT value FROM settings WHERE key=?", (key,)
    ).fetchone()
    return row["value"] if row and row["value"] else None


def _block_until(c: sqlite3.Connection) -> Optional[datetime]:
    return _parse_iso(_get(c, "cdx_block_until"))


def is_blocked() -> bool:
    """Hard-block gate: True iff a 429-driven cooldown is still in
    the future. Fast path — single SELECT."""
    with _connect() as c:
        _ensure_schema(c)
        bu = _block_until(c)
    return bool(bu and bu > _now_utc())


def block_remaining_seconds() -> Optional[int]:
    """Seconds left in the current hard block, or None if not
    blocked. UI/logging convenience."""
    with _connect() as c:
        _ensure_schema(c)
        bu = _block_until(c)
    if not bu:
        return None
    delta = (bu - _now_utc()).total_seconds()
    return int(delta) if delta > 0 else None


def get_status() -> dict:
    """Snapshot for the dashboard banner + operator logs."""
    with _connect() as c:
        _ensure_schema(c)
        bu = _block_until(c)
        tier_raw = _get(c, "cdx_block_tier") or "0"
        last_429 = _get(c, "cdx_last_429_iso")
        # Window count — same cutoff the gate uses.
        cutoff = time.time() - CDX_WINDOW_SECONDS
        row = c.execute(
            "SELECT COUNT(*) AS n FROM cdx_rate_events WHERE ts >= ?",
            (cutoff,),
        ).fetchone()
        in_flight = int(row["n"]) if row else 0
    try:
        tier = int(tier_raw)
    except ValueError:
        tier = 0
    return {
        "block_until": bu.isoformat() if bu else None,
        "block_remaining_seconds": (
            int((bu - _now_utc()).total_seconds())
            if bu and bu > _now_utc() else None
        ),
        "block_tier": tier,
        "last_429_iso": last_429,
        "limit_per_min": CDX_LIMIT_PER_MIN,
        "in_window": in_flight,
    }


def acquire(timeout: float = ACQUIRE_TIMEOUT_SECONDS) -> None:
    """Block until we may issue one CDX request. Raises
    ``RateLimitTimeout`` if we'd have to wait longer than ``timeout``
    seconds (either because the sliding window is saturated or because
    a hard block is in effect)."""
    deadline = time.monotonic() + timeout
    while True:
        now_wall = time.time()
        try:
            with _connect() as c:
                _ensure_schema(c)
                # BEGIN IMMEDIATE grabs the write lock before anyone
                # else can race past our count. SQLite serializes
                # writers — reads by other processes still proceed, so
                # this is not a global stall.
                c.execute("BEGIN IMMEDIATE")
                # 1) Hard block?
                bu = _block_until(c)
                if bu and bu > _now_utc():
                    wait = (bu - _now_utc()).total_seconds()
                    c.execute("COMMIT")
                    remaining = deadline - time.monotonic()
                    if wait > remaining:
                        logger.warning(
                            "cdx gate timed out under hard block "
                            "(wait=%.1fs remaining=%.1fs)", wait, remaining,
                        )
                        raise RateLimitTimeout(
                            f"Wayback CDX is hard-blocked for "
                            f"{int(wait)}s; try again later."
                        )
                    sleep_s = min(wait, _POLL_INTERVAL_SECONDS, remaining)
                    if sleep_s > 0:
                        time.sleep(sleep_s)
                    continue
                # 2) Sliding window.
                cutoff = now_wall - CDX_WINDOW_SECONDS
                c.execute(
                    "DELETE FROM cdx_rate_events WHERE ts < ?", (cutoff,)
                )
                cnt_row = c.execute(
                    "SELECT COUNT(*) AS n FROM cdx_rate_events"
                ).fetchone()
                cnt = int(cnt_row["n"]) if cnt_row else 0
                if cnt < CDX_LIMIT_PER_MIN:
                    c.execute(
                        "INSERT INTO cdx_rate_events(ts) VALUES(?)",
                        (now_wall,),
                    )
                    c.execute("COMMIT")
                    logger.debug(
                        "cdx gate acquired in_window=%d/%d",
                        cnt + 1, CDX_LIMIT_PER_MIN,
                    )
                    return
                # 3) Saturated — sleep until the oldest slot ages out.
                oldest_row = c.execute(
                    "SELECT MIN(ts) AS t FROM cdx_rate_events"
                ).fetchone()
                oldest = float(oldest_row["t"]) if oldest_row and oldest_row["t"] else now_wall
                c.execute("COMMIT")
        except sqlite3.OperationalError as e:
            # "database is locked" under heavy concurrency — brief
            # retry. Never escalate: losing a race here is benign.
            logger.debug("cdx gate lock busy (%s), retrying", e)
            time.sleep(0.05)
            continue
        wait = (oldest + CDX_WINDOW_SECONDS) - now_wall + 0.05
        remaining = deadline - time.monotonic()
        if wait > remaining:
            logger.warning(
                "cdx gate timed out under sliding window "
                "(wait=%.1fs remaining=%.1fs in_window=%d)",
                wait, remaining, cnt,
            )
            raise RateLimitTimeout(
                f"Wayback CDX rate limit: {cnt} requests in the last "
                f"{int(CDX_WINDOW_SECONDS)}s, slot in {int(wait)}s."
            )
        logger.debug(
            "cdx gate waiting=%0.2fs in_window=%d/%d",
            wait, cnt, CDX_LIMIT_PER_MIN,
        )
        time.sleep(min(max(wait, 0.0), _POLL_INTERVAL_SECONDS, remaining))


def _tier_for_next_block(c: sqlite3.Connection) -> int:
    """Pick the 429-escalation tier for a fresh 429. Decays to 1 if
    the last 429 was more than ``_TIER_DECAY_SECONDS`` ago, else
    ``current_tier + 1``. No upper bound here — the seconds-per-tier
    computation caps the actual wait at ``_BLOCK_TIER_CAP_SECONDS``."""
    prev_tier_raw = _get(c, "cdx_block_tier") or "0"
    try:
        prev_tier = int(prev_tier_raw)
    except ValueError:
        prev_tier = 0
    last_429 = _parse_iso(_get(c, "cdx_last_429_iso"))
    if last_429 is None:
        return 1
    age = (_now_utc() - last_429).total_seconds()
    if age > _TIER_DECAY_SECONDS:
        logger.info(
            "cdx tier decayed (%.1fh since last 429) — resetting to 1",
            age / 3600,
        )
        return 1
    return prev_tier + 1


def _mark_wayback_state(c: sqlite3.Connection, state: str) -> None:
    """Sync the legacy ``wayback_probe_state`` key so the worker's
    ``is_wayback_up()`` check + dashboard banner react without needing
    their own probe. Stored with the threshold counters already
    satisfied — the probe used to need consecutive observations, but
    we've seen the real outage directly, so flip immediately.
    """
    from . import wayback_probe as wp
    if state == "down":
        payload = {
            "state": "down",
            "consecutive_fails": wp.FAIL_THRESHOLD,
            "consecutive_ok": 0,
        }
    else:
        payload = {
            "state": "up",
            "consecutive_fails": 0,
            "consecutive_ok": wp.OK_THRESHOLD,
        }
    _set(c, "wayback_probe_state", json.dumps(payload))
    _set(c, "wayback_state_since", _now_iso())


def observe_429(retry_after_seconds: Optional[float] = None) -> int:
    """Record a CDX 429 (or explicit Retry-After) and install a hard
    block. Returns the block duration in seconds. Safe to call from
    any thread or subprocess — write is atomic."""
    with _connect() as c:
        _ensure_schema(c)
        c.execute("BEGIN IMMEDIATE")
        try:
            tier = _tier_for_next_block(c)
            tier_seconds = _seconds_for_tier(tier)
            # Respect a Retry-After hint — take the larger of the two
            # so an IA-specified longer cooldown isn't undershot by
            # our exponential ladder.
            if retry_after_seconds and retry_after_seconds > tier_seconds:
                duration = int(retry_after_seconds)
            else:
                duration = tier_seconds
            until = _now_utc() + timedelta(seconds=duration)
            _set(c, "cdx_block_until", until.replace(microsecond=0).isoformat())
            _set(c, "cdx_block_tier", str(tier))
            _set(c, "cdx_last_429_iso", _now_iso())
            _mark_wayback_state(c, "down")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
    logger.warning(
        "cdx 429 observed — hard block %ds (tier=%d) retry_after=%s",
        duration, tier, retry_after_seconds,
    )
    # Fire the event bus so connected dashboards update the banner
    # without a page refresh. Import here to avoid a circular at
    # module load time.
    try:
        from . import events_bus
        events_bus.publish("wayback-state-changed")
    except Exception:
        pass
    return duration


def observe_ok() -> None:
    """A CDX request just succeeded. Clears the hard block if it had
    expired, and decays the tier if it's been at least
    ``_TIER_DECAY_SECONDS`` since the last 429."""
    changed = False
    with _connect() as c:
        _ensure_schema(c)
        c.execute("BEGIN IMMEDIATE")
        try:
            bu = _block_until(c)
            if bu and bu <= _now_utc():
                # Block expired — clear it and flip the probe state
                # back up so the worker drains deferred jobs.
                c.execute("DELETE FROM settings WHERE key='cdx_block_until'")
                _mark_wayback_state(c, "up")
                changed = True
            last_429 = _parse_iso(_get(c, "cdx_last_429_iso"))
            if last_429 is not None:
                age = (_now_utc() - last_429).total_seconds()
                if age > _TIER_DECAY_SECONDS:
                    c.execute("DELETE FROM settings WHERE key='cdx_block_tier'")
                    c.execute("DELETE FROM settings WHERE key='cdx_last_429_iso'")
                    changed = True
            # If we're currently flagged 'down' but there's no active
            # block (e.g. an earlier observation flipped us without an
            # active block or the block just expired), flip to 'up'.
            from . import wayback_probe as wp
            current = wp.load_state()
            if current.state == "down" and not bu:
                _mark_wayback_state(c, "up")
                changed = True
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
    if changed:
        logger.info("cdx ok — cleared block/tier")
        try:
            from . import events_bus
            events_bus.publish("wayback-state-changed")
        except Exception:
            pass
        # Release any jobs that were deferred during the block.
        try:
            from . import jobs as _jobs
            n = _jobs.release_deferred()
            if n:
                logger.info("released %d deferred jobs after cdx recovery", n)
        except Exception as e:
            logger.debug("release_deferred failed: %s", e)


def retry_after_to_seconds(raw: Optional[str]) -> Optional[float]:
    """HTTP Retry-After can be an integer delta or an HTTP-date.
    Returns seconds from now, or None if absent/unparseable."""
    if not raw:
        return None
    raw = raw.strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - _now_utc()).total_seconds())
    except Exception:
        return None


def cdx_urlopen(req_or_url, timeout: float = 30.0):
    """Rate-gated ``urllib.request.urlopen``. Handles the gate, the
    429-observation wiring, and the success-observation wiring around
    a single CDX call. Callers consume the response inside a ``with``
    block the same way they would urlopen's.

    On 429 this raises ``urllib.error.HTTPError`` *after* installing a
    hard block, so callers see a real HTTPError (preserving the
    existing retry-then-fail semantics) while the gate is now closed
    for everyone else. Any other HTTP error propagates unchanged.
    """
    acquire()
    try:
        resp = urllib.request.urlopen(req_or_url, timeout=timeout)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            retry_after = retry_after_to_seconds(
                e.headers.get("Retry-After") if e.headers else None
            )
            observe_429(retry_after_seconds=retry_after)
        raise
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        # Connection-level failure — don't penalize the gate, but log
        # so operators can tell these apart from 429s.
        logger.debug("cdx network error: %s", e)
        raise
    # Success path — observe before returning so a fast-exiting caller
    # (or one that forgets to close the response) still recovers state.
    try:
        status = getattr(resp, "status", None)
        if status is None:
            status = resp.getcode()
    except Exception:
        status = None
    if status and 200 <= status < 300:
        observe_ok()
    elif status == 429:
        # Some HTTP stacks don't raise on 429. Handle that path too.
        retry_after = None
        try:
            retry_after = retry_after_to_seconds(resp.headers.get("Retry-After"))
        except Exception:
            pass
        observe_429(retry_after_seconds=retry_after)
        try:
            resp.close()
        except Exception:
            pass
        raise urllib.error.HTTPError(
            getattr(resp, "url", ""), 429, "Too Many Requests",
            resp.headers, None,
        )
    return resp
