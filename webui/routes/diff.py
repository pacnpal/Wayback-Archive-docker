"""Tree + content diff between two snapshots of the same host."""
from __future__ import annotations
import difflib
import hashlib
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .. import jobs
from ._validators import valid_host, valid_ts

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

TEXT_EXTS = {".html", ".htm", ".css", ".js", ".mjs", ".json", ".xml", ".svg", ".txt", ".md"}


def _sha1(p: Path) -> str:
    h = hashlib.sha1()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _walk(root: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for p in root.rglob("*"):
        if p.is_file():
            out[str(p.relative_to(root))] = p
    return out


def _snapshot_root(host: str, ts: str) -> Path:
    root = (jobs.OUTPUT_ROOT / host / ts).resolve()
    base = jobs.OUTPUT_ROOT.resolve()
    if base not in root.parents or not root.is_dir():
        raise HTTPException(404)
    return root


@router.get("/sites/{host}/diff", response_class=HTMLResponse)
async def diff(request: Request, host: str, a: str, b: str, path: str = ""):
    host = valid_host(host)
    a = valid_ts(a)
    b = valid_ts(b)
    ra = _snapshot_root(host, a)
    rb = _snapshot_root(host, b)
    if not path:
        fa, fb = _walk(ra), _walk(rb)
        added = sorted(set(fb) - set(fa))
        removed = sorted(set(fa) - set(fb))
        modified = sorted(
            k for k in set(fa) & set(fb)
            if fa[k].stat().st_size != fb[k].stat().st_size or _sha1(fa[k]) != _sha1(fb[k])
        )
        return templates.TemplateResponse("diff_tree.html", {
            "request": request, "host": host, "a": a, "b": b,
            "added": added, "removed": removed, "modified": modified,
        })
    pa = (ra / path).resolve()
    pb = (rb / path).resolve()
    if ra not in pa.parents or rb not in pb.parents:
        raise HTTPException(400)
    ext = pa.suffix.lower()
    if ext not in TEXT_EXTS:
        return templates.TemplateResponse("diff_file.html", {
            "request": request, "host": host, "a": a, "b": b, "path": path,
            "binary": True,
            "hash_a": _sha1(pa) if pa.exists() else "—",
            "hash_b": _sha1(pb) if pb.exists() else "—",
            "size_a": pa.stat().st_size if pa.exists() else 0,
            "size_b": pb.stat().st_size if pb.exists() else 0,
        })
    ta = pa.read_text(encoding="utf-8", errors="replace").splitlines() if pa.exists() else []
    tb = pb.read_text(encoding="utf-8", errors="replace").splitlines() if pb.exists() else []
    html = difflib.HtmlDiff(wrapcolumn=100).make_table(ta, tb, f"A: {a}", f"B: {b}", context=True, numlines=3)
    return templates.TemplateResponse("diff_file.html", {
        "request": request, "host": host, "a": a, "b": b, "path": path,
        "binary": False, "diff_html": html,
    })
