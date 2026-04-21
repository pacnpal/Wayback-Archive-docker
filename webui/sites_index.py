"""On-disk metadata cache for per-host snapshot directories.

CodeQL's Python path-injection query has two sanitizer patterns it
recognizes reliably:

  1. ``os.path.abspath()`` on the joined path followed by an inline
     ``startswith(base + os.sep)`` check, in the same function as
     the sink.
  2. A regex fullmatch on the raw string, with the match result
     assigned to a new variable and that variable used downstream —
     not the original argument.

Function-boundary indirection (``_valid_host(host)`` → bool; callers
re-use the original ``host``) does NOT work: CodeQL loses the
sanitizer tag when the helper returns. Prior iterations of this file
tried both ``_under_root()`` → ``Path`` helpers and ``_valid_host()``
→ ``bool`` helpers; both generated dozens of repeat alerts.

This rewrite inlines both sanitizers at every single filesystem
sink. Every public function:

  1. Runs ``_HOST_RE.fullmatch(host)`` / ``_TS_RE.fullmatch(ts)``
     inline, captures ``m.group(0)`` into a new local. Downstream
     uses the match-derived local, never the raw argument.
  2. Builds path strings with ``os.path.join`` + ``os.path.abspath``
     and checks ``.startswith(root_prefix)`` before every sink call.
  3. Only then converts to ``Path`` for the sink.

It's verbose but each sink's barrier is mechanical for CodeQL to
prove.
"""
from __future__ import annotations
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import jobs, log as _log

logger = _log.get("sites_index")


INDEX_NAME = ".index.json"

# Stamped on every cache entry. Bump when _measure semantics change so the
# lazy refresh in get_index() recomputes pre-existing rows. Missing/older
# `v` values also trigger a recompute, which retires entries written by
# pre-staleness-check builds (those numbers froze at the snapshot's first
# measurement and never tracked subsequent file additions).
SNAPSHOT_VERSION = 2

# RFC-952/1123-style host (letters, digits, dots, hyphens). No slashes,
# no ``..``, no null bytes, no CRLF, no unicode escapes. Fullmatch only.
_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-]{0,253}$")
# Exactly 14 ASCII digits — Wayback snapshot timestamp.
_TS_RE = re.compile(r"^\d{14}$")


def is_snapshot_ts(name: str) -> bool:
    """Snapshot dir names are always `YYYYMMDDHHMMSS` (14 digits)."""
    return _TS_RE.fullmatch(name) is not None


def _root_abs() -> str:
    """``OUTPUT_ROOT`` resolved via abspath. Used as the prefix every
    sanitized path must start with."""
    return os.path.abspath(str(jobs.OUTPUT_ROOT))


def _safe_under_root(*parts: str) -> Optional[str]:
    """Join ``parts`` under ``OUTPUT_ROOT``, absolutize, and verify the
    result is inside the root. Returns the abspath string on success,
    None on escape. Each part must already have been regex-validated
    by the caller — this function is the final abspath+startswith
    barrier CodeQL recognizes, not a replacement for the regex check.
    """
    root = _root_abs()
    candidate = os.path.abspath(os.path.join(root, *parts))
    if candidate == root:
        return candidate
    if candidate.startswith(root + os.sep):
        return candidate
    return None


def _load(host: str) -> dict:
    m = _HOST_RE.fullmatch(host)
    if m is None:
        return {}
    safe_host = m.group(0)
    safe = _safe_under_root(safe_host, INDEX_NAME)
    if safe is None:
        return {}
    p = Path(safe)
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _atomic_write(host: str, data: dict) -> None:
    m = _HOST_RE.fullmatch(host)
    if m is None:
        return
    safe_host = m.group(0)
    safe_dir = _safe_under_root(safe_host)
    if safe_dir is None:
        return
    d = Path(safe_dir)
    if not d.is_dir():
        return
    fd, tmp_raw = tempfile.mkstemp(prefix=".index.", dir=safe_dir)
    # mkstemp constructs its return value from its tainted ``dir=``
    # arg. Even though ``dir=safe_dir`` is barrier-verified, re-derive
    # the tmp path from the basename under safe_dir so CodeQL tracks
    # it through a known-safe join rather than mkstemp's output.
    tmp_name = os.path.basename(tmp_raw)
    safe_tmp = _safe_under_root(safe_host, tmp_name)
    safe_target = _safe_under_root(safe_host, INDEX_NAME)
    if safe_tmp is None or safe_target is None:
        os.close(fd)
        try:
            os.unlink(tmp_raw)
        except OSError:
            pass
        return
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(safe_tmp, safe_target)
    except Exception:
        try:
            os.unlink(safe_tmp)
        except OSError:
            pass


def _measure_host_snapshot(host: str, ts: str) -> dict:
    """Walk the snapshot dir for `host/ts`; return {size_bytes, file_count,
    mtime, v}. Empty dict on escape or missing dir. A subdir vanishing
    mid-walk is tolerated — partial counts are returned instead of zero."""
    mh = _HOST_RE.fullmatch(host)
    mt = _TS_RE.fullmatch(ts)
    if mh is None or mt is None:
        return {}
    safe_host = mh.group(0)
    safe_ts = mt.group(0)
    safe = _safe_under_root(safe_host, safe_ts)
    if safe is None:
        return {}
    import time as _time
    t0 = _time.monotonic()
    if not os.path.isdir(safe):
        logger.debug("measure skip (not a dir) dir=%s", safe)
        return {}
    logger.debug("measure start dir=%s", safe)
    root_prefix = _root_abs() + os.sep
    size = 0
    files = 0
    # Walk stack stores strings; we re-abspath and re-verify each
    # popped dir stays inside OUTPUT_ROOT before the scandir sink so
    # a symlink inside the tree can't leak out mid-walk.
    stack: list[str] = [safe]
    while stack:
        cur = stack.pop()
        cur_abs = os.path.abspath(cur)
        if cur_abs != _root_abs() and not cur_abs.startswith(root_prefix):
            continue
        try:
            with os.scandir(cur_abs) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        child = os.path.abspath(entry.path)
                        if child != _root_abs() and not child.startswith(root_prefix):
                            continue
                        stack.append(child)
                    elif entry.is_file(follow_symlinks=False):
                        files += 1
                        try:
                            size += entry.stat(follow_symlinks=False).st_size
                        except OSError:
                            pass
        except FileNotFoundError:
            continue
    try:
        mtime = os.stat(safe).st_mtime
        mtime_iso = datetime.fromtimestamp(mtime, tz=timezone.utc).replace(microsecond=0).isoformat()
    except OSError:
        mtime_iso = None
    dur_ms = (_time.monotonic() - t0) * 1000
    logger.debug("measure done dir=%s files=%d size=%d mtime=%s duration=%.1fms",
                 safe, files, size, mtime_iso, dur_ms)
    return {"size_bytes": size, "file_count": files, "mtime": mtime_iso,
            "v": SNAPSHOT_VERSION}


def _list_snapshot_ts(host: str) -> list[str]:
    """Enumerate valid snapshot timestamps under ``host``."""
    m = _HOST_RE.fullmatch(host)
    if m is None:
        return []
    safe_host = m.group(0)
    safe = _safe_under_root(safe_host)
    if safe is None:
        return []
    if not os.path.isdir(safe):
        return []
    out: list[str] = []
    root_prefix = _root_abs() + os.sep
    try:
        with os.scandir(safe) as it:
            for entry in it:
                child = os.path.abspath(entry.path)
                if not child.startswith(root_prefix):
                    continue
                if entry.is_dir(follow_symlinks=False) and _TS_RE.fullmatch(entry.name):
                    out.append(entry.name)
    except OSError:
        return out
    return out


def _snapshot_mtime_iso(host: str, ts: str) -> Optional[str]:
    mh = _HOST_RE.fullmatch(host)
    mt = _TS_RE.fullmatch(ts)
    if mh is None or mt is None:
        return None
    safe = _safe_under_root(mh.group(0), mt.group(0))
    if safe is None:
        return None
    try:
        return datetime.fromtimestamp(
            os.stat(safe).st_mtime, tz=timezone.utc,
        ).replace(microsecond=0).isoformat()
    except OSError:
        return None


def _snapshot_is_dir(host: str, ts: str) -> bool:
    mh = _HOST_RE.fullmatch(host)
    mt = _TS_RE.fullmatch(ts)
    if mh is None or mt is None:
        return False
    safe = _safe_under_root(mh.group(0), mt.group(0))
    if safe is None:
        return False
    return os.path.isdir(safe)


def refresh_index(host: str, timestamps: Optional[list[str]] = None) -> dict:
    """Refresh index entries for `timestamps` (or all snapshots if None)."""
    logger.debug("refresh_index host=%s timestamps=%s",
                 host, timestamps if timestamps else "<all>")
    if _HOST_RE.fullmatch(host) is None:
        return {}
    snaps = timestamps if timestamps is not None else _list_snapshot_ts(host)
    if timestamps is None and not snaps:
        return {}
    idx = _load(host)
    changed = False
    for ts in snaps:
        if _TS_RE.fullmatch(ts) is None or not _snapshot_is_dir(host, ts):
            if ts in idx:
                del idx[ts]
                changed = True
            continue
        m = _measure_host_snapshot(host, ts)
        if m and idx.get(ts) != m:
            idx[ts] = m
            changed = True
    if changed:
        _atomic_write(host, idx)
        logger.debug("refresh_index host=%s wrote %d entries", host, len(idx))
    else:
        logger.debug("refresh_index host=%s no changes", host)
    return idx


def get_index(host: str) -> dict:
    """Return cached index, lazily refreshing entries whose dir mtime moved
    or whose cache predates SNAPSHOT_VERSION."""
    if _HOST_RE.fullmatch(host) is None:
        return {}
    on_disk = set(_list_snapshot_ts(host))
    idx = _load(host)
    if not on_disk and not idx:
        return {}
    dirty = False
    # Drop index entries whose dir was removed.
    for ts in list(idx.keys()):
        if ts not in on_disk:
            del idx[ts]
            dirty = True
    for ts in on_disk:
        if not _snapshot_is_dir(host, ts):
            continue
        cached = idx.get(ts)
        if cached and cached.get("v") == SNAPSHOT_VERSION:
            cur_mtime = _snapshot_mtime_iso(host, ts)
            if cur_mtime == cached.get("mtime"):
                continue
        m = _measure_host_snapshot(host, ts)
        if m and idx.get(ts) != m:
            idx[ts] = m
            dirty = True
    if dirty:
        _atomic_write(host, idx)
    return idx


def drop_entry(host: str, timestamp: str) -> None:
    if (_HOST_RE.fullmatch(host) is None
            or _TS_RE.fullmatch(timestamp) is None):
        return
    idx = _load(host)
    if timestamp in idx:
        del idx[timestamp]
        _atomic_write(host, idx)
