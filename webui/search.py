"""TF-IDF full-text search over a snapshot's HTML.

Replaces the dead 1990s product-search CGIs (qfind.exe / vtopic.exe /
ASearch.cgi / cfilter.cgi / showprefs.cgi) — all they did was map a
query to a ranked list of pages on the same site, and we have every
archived page on disk. Indexes titles / headings / meta descriptions /
visible body text with a small English stoplist, weights titles
higher, serializes the index to `<snapshot>/.search.json` (atomic
write, invalidated by snapshot mtime like the audit cache).
"""
from __future__ import annotations
import json
import math
import os
import re
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

from bs4 import BeautifulSoup

from .asset_audit import _snapshot_mtime

INDEX_NAME = ".search.json"
_HTML_EXTS = {".html", ".htm"}

_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "he", "in", "is", "it", "its", "of", "on", "or", "she",
    "that", "the", "this", "to", "was", "were", "will", "with", "you",
    "your", "but", "not", "can", "have", "had", "do", "does", "did",
    "if", "so", "than", "then", "we", "our", "us", "they", "them",
}

_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9\-]{1,}")

# Weighted regions of the HTML.
_TITLE_WEIGHT = 5.0
_HEADING_WEIGHT = 3.0
_META_DESC_WEIGHT = 2.0
_BODY_WEIGHT = 1.0


def _tokens(text: str) -> list[str]:
    if not text:
        return []
    return [t.lower() for t in _TOKEN_RE.findall(text) if t.lower() not in _STOPWORDS]


def _extract_regions(html: str) -> dict[str, float]:
    """Return a dict {token → accumulated weight} for one HTML document."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for bad in soup(["script", "style", "noscript"]):
        bad.decompose()
    tf: dict[str, float] = defaultdict(float)
    title = (soup.title.string if soup.title else "") or ""
    for tok in _tokens(title):
        tf[tok] += _TITLE_WEIGHT
    for h in soup.find_all(["h1", "h2", "h3"]):
        for tok in _tokens(h.get_text(" ", strip=True)):
            tf[tok] += _HEADING_WEIGHT
    for m in soup.find_all("meta"):
        name = (m.get("name") or "").strip().lower()
        if name in ("description", "keywords"):
            for tok in _tokens(m.get("content") or ""):
                tf[tok] += _META_DESC_WEIGHT
    body_text = (soup.body.get_text(" ", strip=True) if soup.body
                 else soup.get_text(" ", strip=True))
    for tok in _tokens(body_text):
        tf[tok] += _BODY_WEIGHT
    return dict(tf), (title.strip() or "")


def build_index(snapshot_dir: Path) -> dict:
    """Walk HTML files under `snapshot_dir`, return a serializable dict:
        {
          "n_docs": int,
          "docs": [{"rel": str, "title": str, "len": float}, ...],
          "postings": {token: [[doc_idx, weight], ...]},
          "built_at": float (mtime at build time),
        }
    """
    docs: list[dict] = []
    postings: dict[str, list[list]] = defaultdict(list)
    for p in snapshot_dir.rglob("*"):
        if not p.is_file() or p.suffix.lower() not in _HTML_EXTS:
            continue
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        tf, title = _extract_regions(text)
        if not tf:
            continue
        doc_idx = len(docs)
        rel = str(p.relative_to(snapshot_dir)).replace("\\", "/")
        docs.append({
            "rel": rel, "title": title,
            "len": math.sqrt(sum(w * w for w in tf.values())) or 1.0,
        })
        for tok, weight in tf.items():
            postings[tok].append([doc_idx, weight])
    return {
        "n_docs": len(docs), "docs": docs, "postings": dict(postings),
        "built_at": _snapshot_mtime(snapshot_dir),
    }


def _index_path(snapshot_dir: Path) -> Path:
    return snapshot_dir / INDEX_NAME


def _atomic_write(path: Path, data: dict) -> None:
    fd, tmp = tempfile.mkstemp(prefix=".search.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        os.replace(tmp, str(path))
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass


def get_index(snapshot_dir: Path, force: bool = False) -> dict:
    if not snapshot_dir.is_dir():
        return {"n_docs": 0, "docs": [], "postings": {}, "built_at": 0.0}
    p = _index_path(snapshot_dir)
    if p.is_file() and not force:
        try:
            idx = json.loads(p.read_text())
            if idx.get("built_at", 0) >= _snapshot_mtime(snapshot_dir):
                return idx
        except Exception:
            pass
    idx = build_index(snapshot_dir)
    _atomic_write(p, idx)
    return idx


def query(idx: dict, q: str, limit: int = 50) -> list[dict]:
    """Rank documents in `idx` against query `q` using TF-IDF cosine-ish
    scoring. Returns up to `limit` hits as [{rel, title, score}]."""
    tokens = _tokens(q)
    if not tokens or not idx.get("n_docs"):
        return []
    n = idx["n_docs"]
    postings = idx["postings"]
    scores: dict[int, float] = defaultdict(float)
    for tok in tokens:
        plist = postings.get(tok)
        if not plist:
            continue
        df = len(plist)
        idf = math.log(1 + n / df)
        for doc_idx, w in plist:
            scores[doc_idx] += w * idf
    # Normalize by document length (crude cosine).
    docs = idx["docs"]
    ranked = [
        {"rel": docs[i]["rel"], "title": docs[i]["title"],
         "score": s / max(docs[i].get("len", 1.0), 1.0)}
        for i, s in scores.items()
    ]
    ranked.sort(key=lambda d: d["score"], reverse=True)
    return ranked[:limit]


def drop_index(snapshot_dir: Path) -> None:
    p = _index_path(snapshot_dir)
    if p.exists():
        try:
            p.unlink()
        except Exception:
            pass
