"""On-disk metadata cache for per-host snapshot directories."""
from __future__ import annotations
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import jobs

INDEX_NAME = ".index.json"


def _index_path(host: str) -> Path:
    return jobs.OUTPUT_ROOT / host / INDEX_NAME


def _load(host: str) -> dict:
    p = _index_path(host)
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _atomic_write(host: str, data: dict) -> None:
    d = jobs.OUTPUT_ROOT / host
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
    size = 0
    files = 0
    try:
        stack = [snapshot_dir]
        while stack:
            cur = stack.pop()
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
        return {}
    try:
        mtime = snapshot_dir.stat().st_mtime
        mtime_iso = datetime.fromtimestamp(mtime, tz=timezone.utc).replace(microsecond=0).isoformat()
    except OSError:
        mtime_iso = None
    return {"size_bytes": size, "file_count": files, "mtime": mtime_iso}


def refresh_index(host: str, timestamps: Optional[list[str]] = None) -> dict:
    """Refresh index entries for `timestamps` (or all snapshots if None)."""
    host_dir = jobs.OUTPUT_ROOT / host
    if not host_dir.is_dir():
        return {}
    idx = _load(host)
    snaps = timestamps
    if snaps is None:
        snaps = [p.name for p in host_dir.iterdir() if p.is_dir()]
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
    """Return cached index, lazily refreshing entries whose dir mtime changed."""
    host_dir = jobs.OUTPUT_ROOT / host
    if not host_dir.is_dir():
        return {}
    idx = _load(host)
    on_disk = {p.name for p in host_dir.iterdir() if p.is_dir()}
    stale: list[str] = []
    # Drop index entries whose dir was removed.
    for ts in list(idx.keys()):
        if ts not in on_disk:
            del idx[ts]
            stale.append(ts)
    # Find snapshots missing from the cache entirely.
    missing = [ts for ts in on_disk if ts not in idx]
    if missing:
        for ts in missing:
            m = _measure(host_dir / ts)
            if m:
                idx[ts] = m
        stale.extend(missing)
    if stale:
        _atomic_write(host, idx)
    return idx


def drop_entry(host: str, timestamp: str) -> None:
    idx = _load(host)
    if timestamp in idx:
        del idx[timestamp]
        _atomic_write(host, idx)
