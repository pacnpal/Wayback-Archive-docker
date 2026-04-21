"""Job queue (SQLite) + subprocess runner for wayback_archive CLI."""
from __future__ import annotations
import asyncio
import json
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Lines emitted by webui.wayback_resume_shim's logger already start with an
# ISO8601 timestamp (its fmt is "%(asctime)s ... " with datefmt
# "%Y-%m-%dT%H:%M:%SZ"). Don't double-stamp those.
_TS_LINE_RE = re.compile(rb"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?\s")

from . import wayback

OUTPUT_ROOT = Path(os.environ.get("OUTPUT_DIR", "/app/output"))
DB_PATH = OUTPUT_ROOT / ".dashboard.db"


# Hard ceiling on concurrent archive jobs. This is NOT a user
# preference — it's a cap that protects Internet Archive (and us)
# from bursting into rate-limit territory on the playback endpoint.
# Each concurrent job may fan out to FETCH_WORKERS parallel playback
# fetches, so MAX × FETCH is the real wall-clock request pressure.
# The CDX endpoint is independently rate-gated in webui.rate_limit at
# 50 req/min no matter how high this is, so the cap here is purely
# to keep playback traffic inside sane bounds. Clamped in
# get_max_concurrent() so DB-level tampering or a stale settings row
# can't bypass the ceiling.
MAX_CONCURRENT_CEILING = 10


def _max_concurrent() -> int:
    try:
        v = max(1, int(os.environ.get("MAX_CONCURRENT", "3")))
    except ValueError:
        return 3
    return min(v, MAX_CONCURRENT_CEILING)


MAX_CONCURRENT_DEFAULT = _max_concurrent()

UPSTREAM_FLAGS = [
    "OPTIMIZE_HTML", "OPTIMIZE_IMAGES", "MINIFY_JS", "MINIFY_CSS",
    "REMOVE_TRACKERS", "REMOVE_ADS", "REMOVE_CLICKABLE_CONTACTS",
    "REMOVE_EXTERNAL_IFRAMES", "REMOVE_EXTERNAL_LINKS_KEEP_ANCHORS",
    "REMOVE_EXTERNAL_LINKS_REMOVE_ANCHORS", "MAKE_INTERNAL_LINKS_RELATIVE",
    "MAKE_NON_WWW", "MAKE_WWW", "KEEP_REDIRECTIONS", "MAX_FILES",
    # Shim-level flags (consumed by webui.wayback_resume_shim, not upstream).
    "USE_PLAYWRIGHT", "FETCH_WORKERS",
]

from . import log as _log
from . import events_bus
logger = _log.get("jobs")

_running: dict[int, asyncio.subprocess.Process] = {}
_cancelled: set[int] = set()


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect() -> sqlite3.Connection:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with connect() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_url TEXT NOT NULL,
            timestamp TEXT,
            wayback_url TEXT NOT NULL,
            host TEXT NOT NULL,
            site_dir TEXT NOT NULL,
            log_path TEXT NOT NULL,
            flags_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            schedule_id INTEGER,
            repair_paths_json TEXT
        );
        -- idempotent ALTER for existing databases

        CREATE TABLE IF NOT EXISTS _col_migrate (id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_url TEXT NOT NULL,
            cron_expr TEXT NOT NULL,
            flags_json TEXT NOT NULL DEFAULT '{}',
            enabled INTEGER NOT NULL DEFAULT 1,
            next_run_at TEXT,
            last_run_at TEXT,
            last_job_id INTEGER,
            created_at TEXT NOT NULL
        );
        """)
        # Make sure repair_paths_json exists on DBs that pre-date it.
        try:
            c.execute("ALTER TABLE jobs ADD COLUMN repair_paths_json TEXT")
        except sqlite3.OperationalError:
            pass
        # Outage-aware deferral columns — added 2026-04.
        for ddl in (
            "ALTER TABLE jobs ADD COLUMN not_before TEXT",
            "ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
        ):
            try:
                c.execute(ddl)
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "duplicate column" in msg:
                    pass
                else:
                    raise
        # Composite index for the worker's hot-path queries
        # (pick_ready_pending, earliest_deferred_not_before). Covers the
        # WHERE status='pending' AND not_before predicate.
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status_not_before "
            "ON jobs(status, not_before)"
        )
        # Recover orphans: jobs that were mid-run when the container stopped
        # go back to pending so the worker picks them up again on startup.
        orphans = [r[0] for r in c.execute(
            "SELECT id FROM jobs WHERE status='running'"
        ).fetchall()]
        if orphans:
            logger.info("resumed %d orphaned running jobs", len(orphans))
            c.execute(
                "UPDATE jobs SET status='pending', started_at=NULL "
                "WHERE status='running'"
            )
            for jid in orphans:
                row = c.execute("SELECT log_path FROM jobs WHERE id=?", (jid,)).fetchone()
                if row and row["log_path"]:
                    try:
                        with open(row["log_path"], "a") as f:
                            f.write(f"\n[dashboard] container restarted at {now_iso()} — job re-queued to resume\n")
                    except Exception:
                        pass
        _migrate_legacy_probe_state(c)


def _migrate_legacy_probe_state(c: sqlite3.Connection) -> None:
    """One-shot cleanup for v0.3.x → v0.4.x migration.

    v0.3.x ran an active heartbeat probe that incremented
    ``consecutive_fails`` on every tick during an outage; a 4-hour
    IA blip left behind a settings row like
    ``{"state":"down","consecutive_fails":240,...}``. v0.4.0 removed
    the probe and flipped to passive state-tracking — but if that
    legacy row is still in the DB on first v0.4.x boot,
    ``is_wayback_up()`` returns False and the worker gates itself
    shut. Without the old probe to ever flip the state back up, the
    dashboard gets wedged "down" until someone manually deletes the
    row. Users migrating across the version boundary shouldn't have
    to know about the internal schema.

    Signals that a row is v0.3.x legacy:

    - ``consecutive_fails > FAIL_THRESHOLD`` or
      ``consecutive_ok > OK_THRESHOLD`` — v0.4.x's passive state
      flipper only writes counters at exactly the threshold values,
      so anything above is a heartbeat leftover.
    - ``state == "down"`` with no active ``cdx_block_until`` — in
      v0.4.x, "down" is always paired with an active rate-limit
      block. A "down" state with no block is either a legacy row or
      a state/block pair that desync'd; either way, clear it so
      fresh traffic can drive the real state.

    Also drops the orphaned ``wayback_probe_timeout`` setting since
    v0.4.x removed the probe-timeout UI. Safe to re-run: if no
    legacy rows exist this is a no-op.
    """
    from . import wayback_probe
    row = c.execute(
        "SELECT value FROM settings WHERE key='wayback_probe_state'"
    ).fetchone()
    # The orphan timeout setting is obsolete regardless of probe
    # state — v0.4.x has no code path that reads it. Drop it
    # unconditionally so the settings table doesn't carry dead keys.
    c.execute("DELETE FROM settings WHERE key='wayback_probe_timeout'")
    if not row or not row["value"]:
        return
    try:
        data = json.loads(row["value"])
    except (TypeError, ValueError):
        # Corrupt JSON — drop defensively so the reader doesn't keep
        # reconstructing a default ProbeState() and masking the bad
        # row's existence.
        logger.info("clearing corrupt wayback_probe_state settings row")
        c.execute(
            "DELETE FROM settings WHERE key IN "
            "('wayback_probe_state','wayback_state_since')"
        )
        return
    fails = int(data.get("consecutive_fails", 0) or 0)
    oks = int(data.get("consecutive_ok", 0) or 0)
    state = data.get("state", "unknown")
    legacy_counters = (
        fails > wayback_probe.FAIL_THRESHOLD
        or oks > wayback_probe.OK_THRESHOLD
    )
    orphan_down = False
    if state == "down":
        bu_row = c.execute(
            "SELECT value FROM settings WHERE key='cdx_block_until'"
        ).fetchone()
        if not bu_row or not bu_row["value"]:
            orphan_down = True
    if legacy_counters or orphan_down:
        logger.info(
            "migrating legacy wayback_probe_state: state=%s fails=%d ok=%d "
            "legacy_counters=%s orphan_down=%s — clearing",
            state, fails, oks, legacy_counters, orphan_down,
        )
        c.execute(
            "DELETE FROM settings WHERE key IN "
            "('wayback_probe_state','wayback_state_since')"
        )


def _normalize_target(url: str) -> str:
    """Ensure URL has a path so upstream's HTML/asset detection works.
    `https://example.com` → `https://example.com/` (empty path breaks link extraction)."""
    from urllib.parse import urlparse, urlunparse
    p = urlparse(url)
    if p.scheme and p.netloc and not p.path:
        p = p._replace(path="/")
        return urlunparse(p)
    return url


def enqueue(target_url: str, timestamp: Optional[str], flags: dict, schedule_id: Optional[int] = None) -> int:
    logger.debug("enqueue enter target=%r ts=%r flags=%s schedule_id=%s",
                 target_url, timestamp, flags, schedule_id)
    target_url = _normalize_target(target_url)
    if timestamp:
        resolved_ts, resolved_url = timestamp, target_url
        logger.debug("enqueue using provided ts=%s (skipping CDX lookup)",
                     resolved_ts)
    else:
        logger.debug("enqueue resolving latest snapshot via CDX for %s",
                     target_url)
        latest = wayback.latest_snapshot(target_url)
        if not latest:
            raise ValueError(f"No Wayback snapshots found for {target_url}")
        resolved_ts, resolved_url = latest
        resolved_url = _normalize_target(resolved_url)
        logger.debug("enqueue CDX resolved ts=%s url=%s",
                     resolved_ts, resolved_url)
    host = wayback.host_of(resolved_url)
    site_dir = str(OUTPUT_ROOT / host / resolved_ts)
    log_path = str(Path(site_dir) / ".log")
    wb = f"https://web.archive.org/web/{resolved_ts}/{resolved_url}"
    with connect() as c:
        cur = c.execute(
            """INSERT INTO jobs
               (target_url, timestamp, wayback_url, host, site_dir, log_path,
                flags_json, status, created_at, schedule_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)""",
            (target_url, resolved_ts, wb, host, site_dir, log_path,
             json.dumps(flags), now_iso(), schedule_id),
        )
        jid = cur.lastrowid
    logger.info("enqueue job=%d url=%s ts=%s", jid, target_url, resolved_ts)
    logger.debug(
        "enqueue persisted job=%d host=%s site_dir=%s log=%s wb=%s flags=%s",
        jid, host, site_dir, log_path, wb, flags,
    )
    events_bus.publish("jobs-changed")
    return jid


def enqueue_repair(host: str, timestamp: str, rel_paths: list[str], flags: Optional[dict] = None) -> int:
    """Queue a repair job that re-fetches specific missing rel paths for an
    existing snapshot (host/timestamp). If an identical repair job is already
    pending or running for the same (host, ts), returns that job's id instead
    of queueing a duplicate."""
    import json as _json
    if not rel_paths:
        raise ValueError("no rel_paths")
    with connect() as c:
        dup = c.execute(
            "SELECT id FROM jobs "
            "WHERE host=? AND timestamp=? AND repair_paths_json IS NOT NULL "
            "AND status IN ('pending','running') "
            "ORDER BY id DESC LIMIT 1",
            (host, timestamp),
        ).fetchone()
    if dup:
        logger.info("enqueue repair dedup host=%s ts=%s existing_job=%d",
                    host, timestamp, dup["id"])
        return dup["id"]
    site_dir = str(OUTPUT_ROOT / host / timestamp)
    log_path = str(Path(site_dir) / ".log")
    wb = f"https://web.archive.org/web/{timestamp}/https://{host}/"
    flags = flags or {}
    with connect() as c:
        cur = c.execute(
            """INSERT INTO jobs
               (target_url, timestamp, wayback_url, host, site_dir, log_path,
                flags_json, status, created_at, repair_paths_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)""",
            (f"https://{host}/", timestamp, wb, host, site_dir, log_path,
             _json.dumps(flags), now_iso(), _json.dumps(rel_paths)),
        )
        jid = cur.lastrowid
    logger.info("enqueue repair job=%d host=%s ts=%s paths=%d",
                jid, host, timestamp, len(rel_paths))
    events_bus.publish("jobs-changed")
    return jid


JOB_SORT_COLS = {
    "id": "id",
    "url": "target_url",
    "ts": "timestamp",
    "status": "status",
    "started": "started_at",
    "finished": "finished_at",
    "host": "host",
    "type": "CASE WHEN repair_paths_json IS NULL THEN 0 ELSE 1 END",
}


_VALID_STATUSES = {"pending", "running", "ok", "error", "cancelled"}
_VALID_TYPES = {"archive", "repair"}


def _filter_clauses(statuses: Optional[list[str]], types: Optional[list[str]]) -> tuple[str, list]:
    parts: list[str] = []
    args: list = []
    if statuses:
        clean = [s for s in statuses if s in _VALID_STATUSES]
        if clean:
            parts.append(f"status IN ({','.join('?' * len(clean))})")
            args.extend(clean)
    if types:
        clean_t = [t for t in types if t in _VALID_TYPES]
        if clean_t and set(clean_t) != _VALID_TYPES:
            if clean_t == ["archive"]:
                parts.append("repair_paths_json IS NULL")
            elif clean_t == ["repair"]:
                parts.append("repair_paths_json IS NOT NULL")
    where = ("WHERE " + " AND ".join(parts)) if parts else ""
    return where, args


def list_jobs(limit: int = 25, offset: int = 0, status: Optional[str] = None,
              sort: str = "id", dir: str = "desc",
              statuses: Optional[list[str]] = None,
              types: Optional[list[str]] = None) -> list[sqlite3.Row]:
    if status and not statuses:
        statuses = [status]
    col = JOB_SORT_COLS.get(sort, "id")
    direction = "ASC" if dir == "asc" else "DESC"
    where, args = _filter_clauses(statuses, types)
    args = args + [limit, offset]
    with connect() as c:
        return c.execute(
            f"SELECT * FROM jobs {where} ORDER BY {col} {direction}, id DESC "
            f"LIMIT ? OFFSET ?", args
        ).fetchall()


def count_jobs(status: Optional[str] = None,
               statuses: Optional[list[str]] = None,
               types: Optional[list[str]] = None) -> int:
    if status and not statuses:
        statuses = [status]
    where, args = _filter_clauses(statuses, types)
    with connect() as c:
        return c.execute(f"SELECT COUNT(*) FROM jobs {where}", args).fetchone()[0]


def delete_many(ids: list[int]) -> int:
    """Delete job rows. Leaves any archived files on disk — use /snapshots to remove them."""
    if not ids:
        return 0
    logger.info("delete jobs=%s", ids)
    events_bus.publish("jobs-changed")
    # Cancel any still-active runs first so we don't leave orphaned subprocesses.
    for jid in ids:
        with connect() as c:
            r = c.execute("SELECT status FROM jobs WHERE id=?", (jid,)).fetchone()
        if r and r["status"] in ("pending", "running"):
            cancel_job(jid)
    qmarks = ",".join("?" * len(ids))
    with connect() as c:
        return c.execute(f"DELETE FROM jobs WHERE id IN ({qmarks})", ids).rowcount


def delete_jobs_for_host(host: str) -> int:
    with connect() as c:
        n = c.execute("DELETE FROM jobs WHERE host=?", (host,)).rowcount
    if n:
        logger.info("delete jobs host=%s count=%d", host, n)
        events_bus.publish("jobs-changed")
    return n


def cancel_many(ids: list[int]) -> int:
    if not ids:
        return 0
    logger.info("cancel jobs=%s", ids)
    events_bus.publish("jobs-changed")
    cancelled = 0
    for jid in ids:
        _cancelled.add(jid)
        proc = _running.get(jid)
        if proc and proc.returncode is None:
            try:
                proc.send_signal(signal.SIGTERM)
                cancelled += 1
            except ProcessLookupError:
                pass
    qmarks = ",".join("?" * len(ids))
    with connect() as c:
        r = c.execute(
            f"UPDATE jobs SET status='cancelled', finished_at=? "
            f"WHERE status='pending' AND id IN ({qmarks})",
            [now_iso(), *ids],
        ).rowcount
    return cancelled + r


def _debug_sample_rows(rows: list[sqlite3.Row], label: str) -> None:
    if not _log.is_debug() or not rows:
        return
    sample = [(r["id"], r["host"], r["timestamp"], r["status"]) for r in rows[:5]]
    logger.debug("%s rows=%d sample=%s%s",
                 label, len(rows), sample, " ..." if len(rows) > 5 else "")


def pick_ready_pending(limit: int) -> list[sqlite3.Row]:
    """Pending jobs whose ``not_before`` has elapsed (or is NULL).
    Repair jobs (``repair_paths_json`` non-null) sort before full-archive
    jobs: repairs hand the shim an explicit path list so they finish in
    seconds-to-minutes, while a full archive can run for hours — letting
    repairs skip the line keeps the worker slots useful and shrinks the
    missing-asset queue surfaced by the dashboard."""
    now = now_iso()
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM jobs "
            "WHERE status='pending' "
            "AND (not_before IS NULL OR not_before <= ?) "
            "ORDER BY CASE WHEN repair_paths_json IS NOT NULL THEN 0 ELSE 1 END, "
            "id ASC LIMIT ?",
            (now, limit),
        ).fetchall()
    _debug_sample_rows(rows, "pick_ready_pending")
    return rows


def defer_for_outage(job_id: int, now: Optional[datetime] = None) -> None:
    """Reschedule a job that failed during a wayback outage: status back
    to pending, attempts++, not_before = now + backoff(old_attempts)."""
    from . import wayback_probe
    now = now or datetime.now(timezone.utc)
    with connect() as c:
        row = c.execute("SELECT attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
        old_attempts = int(row["attempts"] or 0) if row else 0
        delay_s = wayback_probe.backoff_seconds(old_attempts)
        nb = (now + timedelta(seconds=delay_s)).replace(microsecond=0).isoformat()
        logger.debug(
            "defer compute job=%d old_attempts=%d backoff=%ds (%.1fm) not_before=%s",
            job_id, old_attempts, delay_s, delay_s / 60, nb,
        )
        c.execute(
            "UPDATE jobs SET status='pending', started_at=NULL, finished_at=NULL, "
            "attempts=?, not_before=? WHERE id=?",
            (old_attempts + 1, nb, job_id),
        )
    logger.info("defer job=%d attempts=%d not_before=%s", job_id, old_attempts + 1, nb)
    events_bus.publish("jobs-changed")


def earliest_deferred_not_before() -> Optional[str]:
    """ISO timestamp of the soonest *future* deferred-job retry, or None
    if no pending jobs have a ``not_before`` still in the future.

    Past-due deferrals are excluded because during an outage the worker
    gate holds them back even after their timer elapses — showing the
    banner a stale/past timestamp would make the ETA render as
    "any moment now" perpetually while nothing is actually running."""
    now = now_iso()
    with connect() as c:
        row = c.execute(
            "SELECT MIN(not_before) AS nb FROM jobs "
            "WHERE status='pending' AND not_before IS NOT NULL "
            "AND not_before > ?",
            (now,),
        ).fetchone()
    return row["nb"] if row and row["nb"] else None


def release_deferred() -> int:
    """Clear ``not_before`` on all pending jobs — called when the probe
    flips back to up so queued work drains in one pass."""
    with connect() as c:
        n = c.execute(
            "UPDATE jobs SET not_before=NULL "
            "WHERE status='pending' AND not_before IS NOT NULL"
        ).rowcount
    if n:
        logger.info("released %d deferred jobs", n)
        events_bus.publish("jobs-changed")
    return n


def cancel_all_pending() -> int:
    with connect() as c:
        ids = [r[0] for r in c.execute("SELECT id FROM jobs WHERE status='pending'").fetchall()]
        for jid in ids:
            _cancelled.add(jid)
        return c.execute(
            "UPDATE jobs SET status='cancelled', finished_at=? WHERE status='pending'",
            (now_iso(),),
        ).rowcount


def get_job(job_id: int) -> Optional[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()


def cancel_job(job_id: int) -> bool:
    logger.debug("cancel_job enter job=%d", job_id)
    _cancelled.add(job_id)
    proc = _running.get(job_id)
    if proc and proc.returncode is None:
        try:
            logger.debug("cancel_job sending SIGTERM job=%d pid=%s",
                         job_id, proc.pid)
            proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            logger.debug("cancel_job pid gone job=%d", job_id)
        return True
    with connect() as c:
        n = c.execute(
            "UPDATE jobs SET status='cancelled', finished_at=? WHERE id=? AND status='pending'",
            (now_iso(), job_id),
        ).rowcount
    logger.debug("cancel_job job=%d not running — pending_cancelled_rows=%d",
                 job_id, n)
    return False


async def _pump_log_with_timestamps(
    reader: asyncio.StreamReader, log_f, job_id: int | None = None,
) -> None:
    """Read subprocess stdout line-by-line and append each line to log_f
    prefixed with a wall-clock timestamp. Lines that already start with an
    ISO8601 timestamp (the shim's own logger) pass through unchanged so we
    don't double-stamp them."""
    lines = 0
    bytes_written = 0
    logger.debug("pump start job=%s", job_id)
    while True:
        try:
            line = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError as e:
            # Subprocess closed without a trailing newline — flush what's left.
            if e.partial:
                log_f.write(e.partial)
                bytes_written += len(e.partial)
            logger.debug("pump eof job=%s lines=%d bytes=%d (incomplete read)",
                         job_id, lines, bytes_written)
            return
        lines += 1
        if _TS_LINE_RE.match(line):
            log_f.write(line)
            bytes_written += len(line)
        else:
            stamped = now_iso().encode() + b" " + line
            log_f.write(stamped)
            bytes_written += len(stamped)
        # Avoid crushing DEBUG with per-line noise — sample every 100 lines.
        if _log.is_debug() and lines % 100 == 0:
            logger.debug("pump job=%s lines=%d bytes=%d (rolling)",
                         job_id, lines, bytes_written)


async def _run_one(job: sqlite3.Row) -> None:
    Path(job["site_dir"]).mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["WAYBACK_URL"] = job["wayback_url"]
    env["OUTPUT_DIR"] = job["site_dir"]
    # Flip upstream defaults that change the archived bytes: keep host
    # formatting (no www./non-www. rewrite) and preserve raw HTML formatting
    # unless the user explicitly opts in via flags_json.
    user_flags = json.loads(job["flags_json"])
    for k, default in (("MAKE_NON_WWW", "0"), ("OPTIMIZE_HTML", "0")):
        if k not in user_flags:
            env[k] = default
    for k, v in user_flags.items():
        if k in UPSTREAM_FLAGS and v not in (None, ""):
            env[k] = str(v)
    # Hard cap per-job prefetch parallelism. MAX_CONCURRENT jobs ×
    # FETCH_WORKERS threads defines the total playback-request
    # pressure we can put on Internet Archive. The CDX gate
    # independently caps CDX traffic, but playback is only protected
    # by keeping these two numbers sane. Enforced here (not just in
    # the dashboard input) so a flag passed via the API, a legacy DB
    # row, or the enqueue form cannot exceed the ceiling.
    FETCH_WORKERS_CEILING = 8
    fw_raw = env.get("FETCH_WORKERS")
    if fw_raw:
        try:
            env["FETCH_WORKERS"] = str(max(1, min(FETCH_WORKERS_CEILING, int(fw_raw))))
        except ValueError:
            env.pop("FETCH_WORKERS", None)
    # Repair mode: spawn the repair shim instead of the resume shim.
    repair_raw = None
    try:
        repair_raw = job["repair_paths_json"]
    except (KeyError, IndexError):
        pass
    entry_module = "webui.wayback_resume_shim"
    if repair_raw:
        try:
            paths = json.loads(repair_raw)
            if paths:
                # Write the rel-path list to a sidecar file instead of
                # passing it as an env var: Linux caps a single env or
                # argv string at PAGE_SIZE*32 (128 KB) via
                # MAX_ARG_STRLEN, and a single 1997-era snapshot can
                # have ~5000 missing rels = ~235 KB, which makes execve
                # return E2BIG and the job terminal-errors before the
                # shim ever starts. The file form has no size limit.
                paths_file = Path(job["site_dir"]) / ".repair-paths"
                paths_file.write_text("\n".join(paths), encoding="utf-8")
                env["REPAIR_PATHS_FILE"] = str(paths_file)
                entry_module = "webui.wayback_repair_shim"
        except Exception:
            pass
    start_time = time.monotonic()
    with connect() as c:
        c.execute(
            "UPDATE jobs SET status='running', started_at=? WHERE id=?",
            (now_iso(), job["id"]),
        )
    logger.info("start job=%d host=%s ts=%s", job["id"], job["host"], job["timestamp"])
    if _log.is_debug():
        passthrough = {k: env[k] for k in UPSTREAM_FLAGS if k in env}
        logger.debug(
            "spawn job=%d module=%s wayback_url=%s site_dir=%s "
            "log_path=%s flags=%s",
            job["id"], entry_module, env["WAYBACK_URL"], env["OUTPUT_DIR"],
            job["log_path"], passthrough,
        )
    log_f = open(job["log_path"], "ab", buffering=0)
    try:
        # `-u` forces line-buffered stdout so the pump doesn't sit on a 4KB
        # block waiting for it to fill. limit=1MB lets the StreamReader hold
        # very long lines (e.g. JSON dumps from upstream) without raising.
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", "-m", entry_module,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=1 << 20,
        )
        logger.debug("subprocess pid=%s started for job=%d", proc.pid, job["id"])
        _running[job["id"]] = proc
        pump = asyncio.create_task(
            _pump_log_with_timestamps(proc.stdout, log_f, job["id"])
        )
        rc = await proc.wait()
        logger.debug("subprocess pid=%s exit rc=%s job=%d — draining pump (<=10s)",
                     proc.pid, rc, job["id"])
        # Drain any lines still buffered in the pipe after the subprocess
        # exited. Bounded wait so a stuck pump can't hang the worker.
        try:
            await asyncio.wait_for(pump, timeout=10.0)
        except asyncio.TimeoutError:
            logger.debug("pump drain timed out job=%d — cancelling", job["id"])
            pump.cancel()
    finally:
        log_f.close()
        _running.pop(job["id"], None)
        logger.debug("job=%d cleanup done (log closed, _running slot freed)",
                     job["id"])
    if job["id"] in _cancelled:
        status = "cancelled"
        _cancelled.discard(job["id"])
    else:
        status = "ok" if rc == 0 else "error"
    if status == "ok":
        try:
            from . import sites_index
            sites_index.refresh_index(job["host"], [Path(job["site_dir"]).name])
        except Exception:
            pass
        # Skip auto-repair if THIS was already a repair job (avoid ping-pong).
        try:
            is_repair = bool(job["repair_paths_json"])
        except (KeyError, IndexError):
            is_repair = False
        if not is_repair:
            try:
                from . import asset_audit
                ts_name = Path(job["site_dir"]).name
                data = asset_audit.get_audit(Path(job["site_dir"]), force=True)
                rel_paths = [m["rel"] for m in data["missing"]]
                if rel_paths:
                    logger.info(
                        "auto-repair triggered job=%d missing=%d",
                        job["id"], len(rel_paths),
                    )
                    enqueue_repair(job["host"], ts_name, rel_paths)
            except Exception as e:
                logger.warning("auto-audit/repair failed job=%d err=%s",
                               job["id"], e)
    # If the job failed and the probe says CDX is down, this is almost
    # certainly an outage, not a real content failure. Defer instead of
    # going terminal; release_deferred() or the elapsed not_before will
    # retry us when IA comes back. defer_for_outage publishes the SSE
    # event itself, so don't double-publish here.
    if status == "error":
        try:
            from . import wayback_probe
            if not wayback_probe.is_wayback_up():
                defer_for_outage(job["id"])
                dur = time.monotonic() - start_time
                logger.info("deferred job=%d duration=%.1fs rc=%s (wayback down)",
                            job["id"], dur, rc)
                return
        except Exception as e:
            logger.warning("defer check failed job=%d err=%s", job["id"], e)
    with connect() as c:
        c.execute(
            "UPDATE jobs SET status=?, finished_at=? WHERE id=?",
            (status, now_iso(), job["id"]),
        )
    dur = time.monotonic() - start_time
    logger.info("done job=%d status=%s duration=%.1fs rc=%s",
                job["id"], status, dur, rc)
    events_bus.publish("jobs-changed")


def get_setting(key: str, default: str) -> str:
    """Single-key read from the ``settings`` table with a default.
    Centralized so cross-module consumers (e.g. wayback_probe) don't
    embed their own SELECT and key-string knowledge."""
    with connect() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


# Backwards-compat alias — older internal callers still use this name.
_get_setting = get_setting


def set_setting(key: str, value: str) -> None:
    with connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
    logger.info("setting %s=%s", key, value)


def get_max_concurrent() -> int:
    raw = get_setting("max_concurrent", str(MAX_CONCURRENT_DEFAULT))
    try:
        v = max(1, min(MAX_CONCURRENT_CEILING, int(raw)))
    except ValueError:
        v = MAX_CONCURRENT_DEFAULT
    # Noisy — worker_loop calls this every tick — keep at DEBUG only.
    if _log.is_debug():
        logger.debug("get_max_concurrent raw=%r -> %d (default=%d ceiling=%d)",
                     raw, v, MAX_CONCURRENT_DEFAULT, MAX_CONCURRENT_CEILING)
    return v


async def _run_job(row):
    try:
        await _run_one(row)
    except Exception as e:
        with connect() as c:
            c.execute(
                "UPDATE jobs SET status='error', finished_at=? WHERE id=?",
                (now_iso(), row["id"]),
            )
        try:
            with open(row["log_path"], "a") as f:
                f.write(f"\n{now_iso()} [dashboard] worker error: {e}\n")
        except Exception:
            pass


async def worker_loop(stop: asyncio.Event) -> None:
    from . import wayback_probe
    active: set[asyncio.Task] = set()
    tick = 0
    logger.debug("worker loop start max_concurrent_default=%d", MAX_CONCURRENT_DEFAULT)
    while not stop.is_set():
        tick += 1
        limit = get_max_concurrent()
        # Reap finished tasks
        done = {t for t in active if t.done()}
        if done:
            logger.debug("worker tick=%d reaped=%d finished tasks", tick, len(done))
        active -= done
        # Authoritative throttle: pull at most (limit - in-flight) pending jobs
        # per tick. This is where `max_concurrent` is actually enforced.
        headroom = max(0, limit - len(active))
        logger.debug(
            "worker tick=%d limit=%d active=%d headroom=%d",
            tick, limit, len(active), headroom,
        )
        if headroom == 0:
            logger.debug("worker tick=%d saturated — waiting up to 2s for a slot",
                         tick)
            try:
                await asyncio.wait(active, timeout=2.0, return_when=asyncio.FIRST_COMPLETED)
            except ValueError:
                await asyncio.sleep(1)
            continue
        # Outage gate: if the probe says CDX is down, don't pop new jobs.
        # In-flight jobs continue so they can finish/fail on their own.
        if not wayback_probe.is_wayback_up():
            logger.debug(
                "worker tick=%d outage gate closed (wayback down) — "
                "active=%d sleeping 5s", tick, len(active),
            )
            try:
                if active:
                    await asyncio.wait(active, timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
                else:
                    await asyncio.wait_for(stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            continue
        rows = pick_ready_pending(headroom)
        logger.debug("worker tick=%d pick_ready_pending(headroom=%d) -> %d rows",
                     tick, headroom, len(rows))
        if not rows:
            logger.debug(
                "worker tick=%d idle — sleeping 2s (active=%d)",
                tick, len(active),
            )
            try:
                if active:
                    await asyncio.wait(active, timeout=2.0, return_when=asyncio.FIRST_COMPLETED)
                else:
                    await asyncio.wait_for(stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            continue
        for row in rows:
            logger.debug(
                "worker tick=%d launching job=%d host=%s ts=%s repair=%s",
                tick, row["id"], row["host"], row["timestamp"],
                bool(row["repair_paths_json"]) if "repair_paths_json" in row.keys() else False,
            )
            active.add(asyncio.create_task(_run_job(row)))
    # Shutdown: wait for in-flight to finish
    logger.debug("worker loop exit after tick=%d active=%d — awaiting drain",
                 tick, len(active))
    if active:
        await asyncio.gather(*active, return_exceptions=True)
    logger.debug("worker loop fully drained")