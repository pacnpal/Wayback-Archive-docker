"""Audit archived snapshots for missing referenced assets."""
from __future__ import annotations
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Optional

from .link_rewrite import _URL_ATTR_RE, _CSS_URL_RE

AUDIT_NAME = ".audit.json"
_HTML_EXTS = {".html", ".htm"}
_CSS_EXTS = {".css"}
_SKIP_PREFIX = ("//", "#", "mailto:", "tel:", "javascript:", "data:", "/web/")


def _skip(ref: str) -> bool:
    r = ref.strip()
    if not r:
        return True
    if "://" in r:
        return True
    for p in _SKIP_PREFIX:
        if r.startswith(p):
            return True
    return False


def _referenced(text: str, is_html: bool) -> list[str]:
    out: list[str] = []
    if is_html:
        for m in _URL_ATTR_RE.finditer(text):
            attr_chunk = m.group(1).lower()
            val = m.group(2)
            if "srcset" in attr_chunk:
                for part in val.split(","):
                    tok = part.strip().split(None, 1)[0] if part.strip() else ""
                    if tok:
                        out.append(tok)
            else:
                out.append(val)
    for m in _CSS_URL_RE.finditer(text):
        out.append(m.group(2))
    return out


def _resolve(file_rel: str, ref: str) -> Optional[str]:
    """Return the snapshot-relative path this ref points at, or None if
    it's outside the snapshot."""
    ref = ref.split("#", 1)[0].split("?", 1)[0].strip()
    if not ref or _skip(ref):
        return None
    # Absolute path → relative to snapshot root
    if ref.startswith("/"):
        target = ref.lstrip("/")
    else:
        base_dir = os.path.dirname(file_rel)
        target = os.path.normpath(os.path.join(base_dir, ref)) if base_dir else ref
    if target in ("", "."):
        target = "index.html"
    if target.endswith("/"):
        target += "index.html"
    if target.startswith("..") or target.startswith("/"):
        return None
    return target.replace("\\", "/")


def audit_snapshot(snapshot_dir: Path) -> dict:
    if not snapshot_dir.is_dir():
        return {"total_refs": 0, "present": 0, "missing": []}
    missing: dict[str, list[str]] = {}
    total = 0
    present = 0
    for p in snapshot_dir.rglob("*"):
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext not in _HTML_EXTS and ext not in _CSS_EXTS:
            continue
        rel_file = str(p.relative_to(snapshot_dir)).replace("\\", "/")
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        for raw_ref in _referenced(text, ext in _HTML_EXTS):
            resolved = _resolve(rel_file, raw_ref)
            if resolved is None:
                continue
            total += 1
            if (snapshot_dir / resolved).is_file():
                present += 1
            else:
                missing.setdefault(resolved, []).append(rel_file)
    return {
        "total_refs": total,
        "present": present,
        "missing": [
            {"rel": k, "referenced_by": sorted(set(v))[:10]}
            for k, v in sorted(missing.items())
        ],
    }


def _audit_path(snapshot_dir: Path) -> Path:
    return snapshot_dir / AUDIT_NAME


def _atomic_write(path: Path, data: dict) -> None:
    fd, tmp = tempfile.mkstemp(prefix=".audit.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp, str(path))
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass


def get_audit(snapshot_dir: Path, force: bool = False) -> dict:
    """Return cached audit for a snapshot; recompute if missing/forced."""
    if not snapshot_dir.is_dir():
        return {"total_refs": 0, "present": 0, "missing": []}
    p = _audit_path(snapshot_dir)
    if p.is_file() and not force:
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    result = audit_snapshot(snapshot_dir)
    _atomic_write(p, result)
    return result


def drop_audit(snapshot_dir: Path) -> None:
    p = _audit_path(snapshot_dir)
    if p.exists():
        try:
            p.unlink()
        except Exception:
            pass
