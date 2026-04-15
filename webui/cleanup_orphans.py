"""Move files/dirs that leaked outside their snapshot folder into a quarantine
sub-folder so they can be reviewed or deleted by the operator.

Two leak patterns are addressed:

1. **Files at the OUTPUT_ROOT itself** (e.g. `/app/output/index.html`), left
   there by crawler runs that joined relative URLs against a bogus base. Moved
   into `<OUTPUT_ROOT>/_orphaned/`.

2. **Path-named dirs and loose files under `<host>/`** (e.g.
   `<host>/images/`, `<host>/index.html`) that should have been inside one of
   the timestamped snapshot folders. Moved into `<host>/_orphaned/`.

The operation is idempotent: re-running is safe.
"""
from __future__ import annotations
import shutil
import time
from pathlib import Path

from .sites_index import is_snapshot_ts

ORPHAN_DIR = "_orphaned"
# Things we keep at OUTPUT_ROOT — everything else there is a leak.
_ROOT_KEEP = {".dashboard.db", ".dashboard.db-wal", ".dashboard.db-shm",
              ".index.json", ORPHAN_DIR}
# Things we keep under <host>/ — snapshot dirs (14-digit timestamps), the
# per-host index, and the quarantine dir.
_HOST_KEEP_FILES = {".index.json"}


def _safe_move(src: Path, dst_dir: Path) -> Path | None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    target = dst_dir / src.name
    if target.exists():
        target = dst_dir / f"{src.name}.{int(time.time())}"
    try:
        shutil.move(str(src), str(target))
        return target
    except Exception:
        return None


def cleanup_output_root(root: Path) -> dict:
    """Move stray files/dirs at OUTPUT_ROOT into OUTPUT_ROOT/_orphaned/.
    Host dirs (anything that contains at least one snapshot-ts child) are
    left in place."""
    moved = []
    quarantine = root / ORPHAN_DIR
    for child in root.iterdir():
        if child.name in _ROOT_KEEP:
            continue
        if child.is_dir():
            # Looks like a host dir if it contains any snapshot-ts subdir.
            has_ts = any(
                p.is_dir() and is_snapshot_ts(p.name)
                for p in child.iterdir()
            )
            if has_ts:
                continue
        t = _safe_move(child, quarantine)
        if t:
            moved.append({"src": str(child), "dst": str(t)})
    return {"moved": moved, "count": len(moved)}


def cleanup_host(host_dir: Path) -> dict:
    """Move non-snapshot entries under a single host dir into
    `<host>/_orphaned/`."""
    if not host_dir.is_dir():
        return {"moved": [], "count": 0}
    quarantine = host_dir / ORPHAN_DIR
    moved = []
    for child in host_dir.iterdir():
        if child.name == ORPHAN_DIR:
            continue
        if child.is_dir() and is_snapshot_ts(child.name):
            continue
        if child.is_file() and child.name in _HOST_KEEP_FILES:
            continue
        t = _safe_move(child, quarantine)
        if t:
            moved.append({"src": str(child), "dst": str(t)})
    return {"moved": moved, "count": len(moved)}


def cleanup_all(root: Path) -> dict:
    """Quarantine orphans at OUTPUT_ROOT and under every host dir."""
    summary: dict = {"root": cleanup_output_root(root), "hosts": {}}
    for child in root.iterdir():
        if not child.is_dir() or child.name == ORPHAN_DIR:
            continue
        summary["hosts"][child.name] = cleanup_host(child)
    total = summary["root"]["count"] + sum(
        h["count"] for h in summary["hosts"].values()
    )
    summary["total"] = total
    return summary


if __name__ == "__main__":
    import json
    import os
    import sys
    root = Path(os.environ.get("OUTPUT_DIR", "/app/output"))
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        # Show what *would* move without moving anything.
        from .sites_index import is_snapshot_ts as _ts
        plan = {"root": [], "hosts": {}}
        for c in root.iterdir():
            if c.name in _ROOT_KEEP:
                continue
            if c.is_dir() and any(p.is_dir() and _ts(p.name) for p in c.iterdir()):
                continue
            plan["root"].append(str(c))
        for c in root.iterdir():
            if not c.is_dir() or c.name == ORPHAN_DIR:
                continue
            items = []
            for cc in c.iterdir():
                if cc.name == ORPHAN_DIR:
                    continue
                if cc.is_dir() and _ts(cc.name):
                    continue
                if cc.is_file() and cc.name in _HOST_KEEP_FILES:
                    continue
                items.append(str(cc))
            if items:
                plan["hosts"][c.name] = items
        print(json.dumps(plan, indent=2))
    else:
        print(json.dumps(cleanup_all(root), indent=2))
