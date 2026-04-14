"""Job queue (SQLite) + subprocess runner for wayback_archive CLI."""
from __future__ import annotations
import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import wayback

OUTPUT_ROOT = Path(os.environ.get("OUTPUT_DIR", "/app/output"))
DB_PATH = OUTPUT_ROOT / ".dashboard.db"

UPSTREAM_FLAGS = [
    "OPTIMIZE_HTML", "OPTIMIZE_IMAGES", "MINIFY_JS", "MINIFY_CSS",
    "REMOVE_TRACKERS", "REMOVE_ADS", "REMOVE_CLICKABLE_CONTACTS",
    "REMOVE_EXTERNAL_IFRAMES", "REMOVE_EXTERNAL_LINKS_KEEP_ANCHORS",
    "REMOVE_EXTERNAL_LINKS_REMOVE_ANCHORS", "MAKE_INTERNAL_LINKS_RELATIVE",
    "MAKE_NON_WWW", "MAKE_WWW", "KEEP_REDIRECTIONS", "MAX_FILES",
]

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
            schedule_id INTEGER
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
        # Recover orphans
        c.execute(
            "UPDATE jobs SET status='error', finished_at=? WHERE status='running'",
            (now_iso(),),
        )


def enqueue(target_url: str, timestamp: Optional[str], flags: dict, schedule_id: Optional[int] = None) -> int:
    if timestamp:
        resolved_ts, resolved_url = timestamp, target_url
    else:
        latest = wayback.latest_snapshot(target_url)
        if not latest:
            raise ValueError(f"No Wayback snapshots found for {target_url}")
        resolved_ts, resolved_url = latest
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
        return cur.lastrowid


def list_jobs(limit: int = 25, offset: int = 0, status: Optional[str] = None) -> list[sqlite3.Row]:
    where = ""
    args: list = []
    if status:
        where = "WHERE status=?"
        args.append(status)
    args.extend([limit, offset])
    with connect() as c:
        return c.execute(
            f"SELECT * FROM jobs {where} ORDER BY id DESC LIMIT ? OFFSET ?", args
        ).fetchall()


def count_jobs(status: Optional[str] = None) -> int:
    with connect() as c:
        if status:
            return c.execute("SELECT COUNT(*) FROM jobs WHERE status=?", (status,)).fetchone()[0]
        return c.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]


def delete_many(ids: list[int]) -> int:
    """Delete job rows. Leaves any archived files on disk — use /sites to remove snapshots."""
    if not ids:
        return 0
    # Cancel any still-active runs first so we don't leave orphaned subprocesses.
    for jid in ids:
        with connect() as c:
            r = c.execute("SELECT status FROM jobs WHERE id=?", (jid,)).fetchone()
        if r and r["status"] in ("pending", "running"):
            cancel_job(jid)
    qmarks = ",".join("?" * len(ids))
    with connect() as c:
        return c.execute(f"DELETE FROM jobs WHERE id IN ({qmarks})", ids).rowcount


def cancel_many(ids: list[int]) -> int:
    if not ids:
        return 0
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


async def _run_one(job: sqlite3.Row) -> None:
    Path(job["site_dir"]).mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["WAYBACK_URL"] = job["wayback_url"]
    env["OUTPUT_DIR"] = job["site_dir"]
    for k, v in json.loads(job["flags_json"]).items():
        if k in UPSTREAM_FLAGS and v not in (None, ""):
            env[k] = str(v)
    with connect() as c:
        c.execute(
            "UPDATE jobs SET status='running', started_at=? WHERE id=?",
            (now_iso(), job["id"]),
        )
    log_f = open(job["log_path"], "ab", buffering=0)
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "wayback_archive.cli",
            env=env, stdout=log_f, stderr=asyncio.subprocess.STDOUT,
        )
        _running[job["id"]] = proc
        rc = await proc.wait()
    finally:
        log_f.close()
        _running.pop(job["id"], None)
    if job["id"] in _cancelled:
        status = "cancelled"
        _cancelled.discard(job["id"])
    else:
        status = "ok" if rc == 0 else "error"
    with connect() as c:
        c.execute(
            "UPDATE jobs SET status=?, finished_at=? WHERE id=?",
            (status, now_iso(), job["id"]),
        )


async def worker_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        with connect() as c:
            row = c.execute(
                "SELECT * FROM jobs WHERE status='pending' ORDER BY id ASC LIMIT 1"
            ).fetchone()
        if row is None:
            try:
                await asyncio.wait_for(stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            await _run_one(row)
        except Exception as e:
            with connect() as c:
                c.execute(
                    "UPDATE jobs SET status='error', finished_at=? WHERE id=?",
                    (now_iso(), row["id"]),
                )
            with open(row["log_path"], "a") as f:
                f.write(f"\n[dashboard] worker error: {e}\n")
