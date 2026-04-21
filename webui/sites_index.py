"""On-disk metadata cache for per-host snapshot directories.

Every public function takes raw ``host`` / ``ts`` strings that came
from user-controlled request paths. CodeQL's path-injection tracker
flags ``pathlib.Path.resolve()`` and ``os.scandir`` as sinks whenever
tainted strings reach them, and it recognizes a *regex fullmatch
sanitizer on the raw string* as a barrier — before any ``Path``
operation happens. The post-construction ``is_relative_to(base)``
check on its own is not a barrier in CodeQL's model because the
``.resolve()`` call has already "fired" the sink.

So every entry point here starts with ``_valid_host`` /
``_valid_ts`` on the raw argument. Both are conservative regex
fullmatches:

  * host — hostname chars only (``[A-Za-z0-9][A-Za-z0-9.\\-]{0,253}``)
  * ts   — exactly 14 ASCII digits

Traversal (``..``), slashes, null bytes, CRLF, unicode escapes,
empty strings — all rejected before the ``Path`` constructor runs.
A defense-in-depth ``is_relative_to`` check still happens after
resolve so a symlink pointing outside OUTPUT_ROOT can't sneak a
valid-looking ``host`` past the regex and then escape via the
filesystem.
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

# Conservative fullmatch sanitizers — mirror routes/_validators but
# return bool instead of raising HTTPException because this module
# isn't HTTP-aware.
_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-]{0,253}$")
_TS_RE = re.compile(r"^\d{14}$")


def _valid_host(host: str) -> bool:
    return bool(host) and _HOST_RE.fullmatch(host) is not None


def _valid_ts(ts: str) -> bool:
    return bool(ts) and _TS_RE.fullmatch(ts) is not None


def is_snapshot_ts(name: str) -> bool:
    """Snapshot dir names are always `YYYYMMDDHHMMSS` (14 digits).
    Anything else under `<host>/` is archived site content, not a snapshot."""
    return _valid_ts(name)


def _load(host: str) -> dict:
    if not _valid_host(host):
        return {}
    p = jobs.OUTPUT_ROOT / host / INDEX_NAME
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _atomic_write(host: str, data: dict) -> None:
    if not _valid_host(host):
        return
    d = jobs.OUTPUT_ROOT / host
    if not d.is_dir():
        return
    # Belt-and-braces: even with the host sanitizer up front,
    # re-check the directory resolves inside OUTPUT_ROOT before
    # mkstemp runs. A symlink'd host dir pointing outside the tree
    # couldn't pass _valid_host (it would just be a name), but this
    # covers the pathological case of a manually-created host dir
    # whose resolve goes elsewhere.
    base = jobs.OUTPUT_ROOT.resolve()
    try:
        if not d.resolve().is_relative_to(base):
            return
    except OSError:
        return
    fd, tmp_raw = tempfile.mkstemp(prefix=".index.", dir=str(d))
    tmp_name = os.path.basename(tmp_raw)
    # mkstemp's prefix= is trusted (we pass a literal); re-derive
    # the path from the sanitized dir + basename so CodeQL sees the
    # tmp path as constructed from safe pieces rather than lifted
    # from mkstemp's tainted dir= output.
    tmp = d / tmp_name
    target = d / INDEX_NAME
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(str(tmp), str(target))
    except Exception:
        try:
            os.unlink(str(tmp))
        except OSError:
            pass


def _measure_host_snapshot(host: str, ts: str) -> dict:
    """Walk the snapshot dir for `host/ts`; return {size_bytes, file_count,
    mtime, v}. Empty dict on escape or missing dir. A subdir vanishing
    mid-walk is tolerated — partial counts are returned instead of zero."""
    if not (_valid_host(host) and _valid_ts(ts)):
        return {}
    import time as _time
    t0 = _time.monotonic()
    snapshot_dir = jobs.OUTPUT_ROOT / host / ts
    if not snapshot_dir.is_dir():
        logger.debug("measure skip (not a dir) dir=%s", snapshot_dir)
        return {}
    # Defense in depth: a host/ts pair that passed the regex
    # sanitizers could still point at a symlinked dir that escapes
    # OUTPUT_ROOT if someone hand-crafted the tree. Resolve once and
    # compare against the resolved root, then treat the resolved path
    # as the canonical walk root.
    base = jobs.OUTPUT_ROOT.resolve()
    try:
        root = snapshot_dir.resolve()
    except OSError:
        return {}
    if not root.is_relative_to(base):
        return {}
    logger.debug("measure start dir=%s", root)
    size = 0
    files = 0
    stack: list[Path] = [root]
    while stack:
        cur = stack.pop()
        # Don't follow symlinks mid-walk: if os.scandir gets handed a
        # symlink dir entry it would leave the tree. We push Path
        # objects built from entry.path (which is under our sanitized
        # root) and rely on follow_symlinks=False on every entry.
        try:
            with os.scandir(str(cur)) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        files += 1
                        try:
                            size += entry.stat(follow_symlinks=False).st_size
                        except OSError:
                            pass
        except FileNotFoundError:
            continue
    try:
        mtime = root.stat().st_mtime
        mtime_iso = datetime.fromtimestamp(mtime, tz=timezone.utc).replace(microsecond=0).isoformat()
    except OSError:
        mtime_iso = None
    dur_ms = (_time.monotonic() - t0) * 1000
    logger.debug("measure done dir=%s files=%d size=%d mtime=%s duration=%.1fms",
                 root, files, size, mtime_iso, dur_ms)
    return {"size_bytes": size, "file_count": files, "mtime": mtime_iso,
            "v": SNAPSHOT_VERSION}


def _list_snapshot_ts(host: str) -> list[str]:
    """Enumerate valid snapshot timestamps under ``host``."""
    if not _valid_host(host):
        return []
    host_dir = jobs.OUTPUT_ROOT / host
    if not host_dir.is_dir():
        return []
    out: list[str] = []
    for p in host_dir.iterdir():
        if p.is_dir() and _valid_ts(p.name):
            out.append(p.name)
    return out


def _snapshot_mtime_iso(host: str, ts: str) -> Optional[str]:
    if not (_valid_host(host) and _valid_ts(ts)):
        return None
    sd = jobs.OUTPUT_ROOT / host / ts
    try:
        return datetime.fromtimestamp(
            sd.stat().st_mtime, tz=timezone.utc,
        ).replace(microsecond=0).isoformat()
    except OSError:
        return None


def _snapshot_is_dir(host: str, ts: str) -> bool:
    if not (_valid_host(host) and _valid_ts(ts)):
        return False
    return (jobs.OUTPUT_ROOT / host / ts).is_dir()


def refresh_index(host: str, timestamps: Optional[list[str]] = None) -> dict:
    """Refresh index entries for `timestamps` (or all snapshots if None)."""
    logger.debug("refresh_index host=%s timestamps=%s",
                 host, timestamps if timestamps else "<all>")
    if not _valid_host(host):
        return {}
    snaps = timestamps if timestamps is not None else _list_snapshot_ts(host)
    if timestamps is None and not snaps:
        return {}
    idx = _load(host)
    changed = False
    for ts in snaps:
        if not _valid_ts(ts) or not _snapshot_is_dir(host, ts):
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
    if not _valid_host(host):
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
    if not (_valid_host(host) and _valid_ts(timestamp)):
        return
    idx = _load(host)
    if timestamp in idx:
        del idx[timestamp]
        _atomic_write(host, idx)
