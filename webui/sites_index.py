"""On-disk metadata cache for per-host snapshot directories."""
from __future__ import annotations
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import jobs
from .safe_path import safe_output_child

INDEX_NAME = ".index.json"

# Stamped on every cache entry. Bump when _measure semantics change so the
# lazy refresh in get_index() recomputes pre-existing rows. Missing/older
# `v` values also trigger a recompute, which retires entries written by
# pre-staleness-check builds (those numbers froze at the snapshot's first
# measurement and never tracked subsequent file additions).
SNAPSHOT_VERSION = 2

_TS_RE = re.compile(r"^\d{14}$")


def is_snapshot_ts(name: str) -> bool:
    """Snapshot dir names are always `YYYYMMDDHHMMSS` (14 digits).
    Anything else under `<host>/` is archived site content, not a snapshot."""
    return bool(_TS_RE.match(name))


def _index_path(host: str) -> Path:
    try:
        return safe_output_child(host) / INDEX_NAME
    except ValueError:
        # Return an unreadable placeholder so downstream .is_file() fails.
        return jobs.OUTPUT_ROOT / "_invalid_" / INDEX_NAME


def _load(host: str) -> dict:
    p = _index_path(host)
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _atomic_write(host: str, data: dict) -> None:
    try:
        d = safe_output_child(host)
    except ValueError:
        return
    if not d.is_dir():
        return
    fd, tmp = tempfile.mkstemp(prefix=".index.", dir=str(d))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp, str(d / INDEX_NAME))
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass


def _measure(snapshot_dir: Path) -> dict:
    """Walk a snapshot dir; return {size_bytes, file_count, mtime, v}.
    A subdir vanishing mid-walk is tolerated — we keep the partial counts
    instead of throwing them away and returning {}."""
    size = 0
    files = 0
    if not snapshot_dir.is_dir():
        return {}
    stack: list[Path] = [snapshot_dir]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
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
        mtime = snapshot_dir.stat().st_mtime
        mtime_iso = datetime.fromtimestamp(mtime, tz=timezone.utc).replace(microsecond=0).isoformat()
    except OSError:
        mtime_iso = None
    return {"size_bytes": size, "file_count": files, "mtime": mtime_iso,
            "v": SNAPSHOT_VERSION}


def refresh_index(host: str, timestamps: Optional[list[str]] = None) -> dict:
    """Refresh index entries for `timestamps` (or all snapshots if None)."""
    try:
        host_dir = safe_output_child(host)
    except ValueError:
        return {}
    if not host_dir.is_dir():
        return {}
    idx = _load(host)
    snaps = timestamps
    if snaps is None:
        snaps = [p.name for p in host_dir.iterdir() if p.is_dir() and is_snapshot_ts(p.name)]
    changed = False
    for ts in snaps:
        sd = host_dir / ts
        if not sd.is_dir():
            if ts in idx:
                del idx[ts]
                changed = True
            continue
        m = _measure(sd)
        if m and idx.get(ts) != m:
            idx[ts] = m
            changed = True
    if changed:
        _atomic_write(host, idx)
    return idx


def get_index(host: str) -> dict:
    """Return cached index, lazily refreshing entries whose dir mtime moved
    or whose cache predates SNAPSHOT_VERSION."""
    try:
        host_dir = safe_output_child(host)
    except ValueError:
        return {}
    if not host_dir.is_dir():
        return {}
    idx = _load(host)
    on_disk = {p.name for p in host_dir.iterdir() if p.is_dir() and is_snapshot_ts(p.name)}
    dirty = False
    # Drop index entries whose dir was removed.
    for ts in list(idx.keys()):
        if ts not in on_disk:
            del idx[ts]
            dirty = True
    for ts in on_disk:
        sd = host_dir / ts
        cached = idx.get(ts)
        if cached and cached.get("v") == SNAPSHOT_VERSION:
            try:
                cur_mtime = datetime.fromtimestamp(
                    sd.stat().st_mtime, tz=timezone.utc
                ).replace(microsecond=0).isoformat()
            except OSError:
                cur_mtime = None
            if cur_mtime == cached.get("mtime"):
                continue
        m = _measure(sd)
        if m and idx.get(ts) != m:
            idx[ts] = m
            dirty = True
    if dirty:
        _atomic_write(host, idx)
    return idx


def drop_entry(host: str, timestamp: str) -> None:
    idx = _load(host)
    if timestamp in idx:
        del idx[timestamp]
        _atomic_write(host, idx)
