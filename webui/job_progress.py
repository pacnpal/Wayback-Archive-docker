"""Parse upstream wayback_archive log tails for a live progress reading."""
from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Optional

_STEP_RE = re.compile(r"\[(\d+)(?:\s*\(limit:\s*\d+\))?\]")
_QUEUE_RE = re.compile(r"Queue:\s*(\d+)\s+files remaining")
_DONE_RE = re.compile(r"Download Complete!", re.IGNORECASE)
_TAIL_BYTES = 16384


def read_progress(log_path: str, max_files: Optional[int] = None) -> Optional[dict]:
    p = Path(log_path)
    if not p.is_file():
        return None
    try:
        size = p.stat().st_size
        with p.open("rb") as f:
            if size > _TAIL_BYTES:
                f.seek(-_TAIL_BYTES, os.SEEK_END)
            data = f.read()
    except OSError:
        return None
    text = data.decode("utf-8", errors="replace")

    downloaded = max((int(m.group(1)) for m in _STEP_RE.finditer(text)), default=0)
    queue_matches = _QUEUE_RE.findall(text)
    queued = int(queue_matches[-1]) if queue_matches else 0
    done = bool(_DONE_RE.search(text))

    if max_files:
        total = max_files
    elif downloaded or queued:
        total = downloaded + queued
    else:
        total = 0
    if total <= 0:
        percent = 0
    else:
        percent = int(downloaded * 100 / total)
    if done:
        percent = 100
    elif percent >= 100:
        percent = 99
    return {
        "done": done,
        "downloaded": downloaded,
        "queued": queued,
        "total": total,
        "percent": percent,
    }
