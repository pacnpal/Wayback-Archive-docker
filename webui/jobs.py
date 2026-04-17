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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Lines emitted by webui.wayback_resume_shim's logger already start with an
# ISO8601 timestamp (its fmt is "%(asctime)s ... " with datefmt
# "%Y-%m-%dT%H:%M:%SZ"). Don't double-stamp those.
_TS_LINE_RE = re.compile(rb"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?\s")

from . import wayback

OUTPUT_ROOT = Path(os.environ.get("OUTPUT_DIR", "/app/output"))
DB_PATH = OUTPUT_ROOT / ".dashboard.db"


def _max_concurrent() -> int:
    try:
        return max(1, int(os.environ.get("MAX_CONCURRENT", "3")))
    except ValueError:
        return 3


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
    target_url = _normalize_target(target_url)
    if timestamp:
        resolved_ts, resolved_url = timestamp, target_url
    else:
        latest = wayback.latest_snapshot(target_url)
        if not latest:
            raise ValueError(f"No Wayback snapshots found for {target_url}")
        resolved_ts, resolved_url = latest
        resolved_url = _normalize_target(resolved_url)
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
    _cancelled.add(job_id)
    proc = _running.get(job_id)
    if proc and proc.returncode is None:
        try:
            proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass
        return True
    with connect() as c:
        c.execute(
            "UPDATE jobs SET status='cancelled', finished_at=? WHERE id=? AND status='pending'",
            (now_iso(), job_id),
        )
    return False


async def _pump_log_with_timestamps(
    reader: asyncio.StreamReader, log_f
) -> None:
    """Read subprocess stdout line-by-line and append each line to log_f
    prefixed with a wall-clock timestamp. Lines that already start with an
    ISO8601 timestamp (the shim's own logger) pass through unchanged so we
    don't double-stamp them."""
    while True:
        try:
            line = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError as e:
            # Subprocess closed without a trailing newline — flush what's left.
            if e.partial:
                log_f.write(e.partial)
            return
        if _TS_LINE_RE.match(line):
            log_f.write(line)
        else:
            log_f.write(now_iso().encode() + b" " + line)


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
                env["REPAIR_PATHS"] = "|".join(paths)
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
        _running[job["id"]] = proc
        pump = asyncio.create_task(_pump_log_with_timestamps(proc.stdout, log_f))
        rc = await proc.wait()
        # Drain any lines still buffered in the pipe after the subprocess
        # exited. Bounded wait so a stuck pump can't hang the worker.
        try:
            await asyncio.wait_for(pump, timeout=10.0)
        except asyncio.TimeoutError:
            pump.cancel()
    finally:
        log_f.close()
        _running.pop(job["id"], None)
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
    with connect() as c:
        c.execute(
            "UPDATE jobs SET status=?, finished_at=? WHERE id=?",
            (status, now_iso(), job["id"]),
        )
    dur = time.monotonic() - start_time
    logger.info("done job=%d status=%s duration=%.1fs rc=%s",
                job["id"], status, dur, rc)
    events_bus.publish("jobs-changed")


def _get_setting(key: str, default: str) -> str:
    with connect() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def set_setting(key: str, value: str) -> None:
    with connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
    logger.info("setting %s=%s", key, value)


def get_max_concurrent() -> int:
    try:
        return max(1, min(20, int(_get_setting("max_concurrent", str(MAX_CONCURRENT_DEFAULT)))))
    except ValueError:
        return MAX_CONCURRENT_DEFAULT


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
    active: set[asyncio.Task] = set()
    while not stop.is_set():
        limit = get_max_concurrent()
        # Reap finished tasks
        done = {t for t in active if t.done()}
        active -= done
        # Authoritative throttle: pull at most (limit - in-flight) pending jobs
        # per tick. This is where `max_concurrent` is actually enforced.
        headroom = max(0, limit - len(active))
        if headroom == 0:
            try:
                await asyncio.wait(active, timeout=2.0, return_when=asyncio.FIRST_COMPLETED)
            except ValueError:
                await asyncio.sleep(1)
            continue
        with connect() as c:
            rows = c.execute(
                "SELECT * FROM jobs WHERE status='pending' ORDER BY id ASC LIMIT ?",
                (headroom,),
            ).fetchall()
        if not rows:
            try:
                if active:
                    await asyncio.wait(active, timeout=2.0, return_when=asyncio.FIRST_COMPLETED)
                else:
                    await asyncio.wait_for(stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            continue
        for row in rows:
            active.add(asyncio.create_task(_run_job(row)))
    # Shutdown: wait for in-flight to finish
    if active:
        await asyncio.gather(*active, return_exceptions=True)
