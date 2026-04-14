"""Browse + edit archived sites."""
from __future__ import annotations
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates

from .. import jobs

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

TEXT_EXTS = {".html", ".htm", ".css", ".js", ".mjs", ".json", ".xml", ".svg", ".txt", ".md"}
MODE_MAP = {".html": "htmlmixed", ".htm": "htmlmixed", ".css": "css",
            ".js": "javascript", ".mjs": "javascript", ".json": "javascript",
            ".xml": "xml", ".svg": "xml", ".txt": "text", ".md": "text"}


def _host_dir(host: str) -> Path:
    d = (jobs.OUTPUT_ROOT / host).resolve()
    if not d.is_dir() or jobs.OUTPUT_ROOT.resolve() not in d.parents and d != jobs.OUTPUT_ROOT.resolve():
        raise HTTPException(404)
    return d


def _safe_path(base: Path, rel: str) -> Path:
    p = (base / rel).resolve()
    if base.resolve() != p and base.resolve() not in p.parents:
        raise HTTPException(400, "path escape")
    return p


@router.get("/sites", response_class=HTMLResponse)
async def sites(request: Request):
    hosts = []
    root = jobs.OUTPUT_ROOT
    if root.exists():
        for h in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")):
            snaps = sorted(s.name for s in h.iterdir() if s.is_dir())
            if snaps:
                hosts.append((h.name, snaps))
    return templates.TemplateResponse("sites.html", {"request": request, "hosts": hosts})


@router.get("/sites/{host}/tree", response_class=HTMLResponse)
async def tree(request: Request, host: str, ts: str, path: str = ""):
    base = _host_dir(host) / ts
    if not base.is_dir():
        raise HTTPException(404)
    cur = _safe_path(base, path) if path else base
    if not cur.is_dir():
        raise HTTPException(404)
    entries = []
    for p in sorted(cur.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
        rel = str(p.relative_to(base))
        entries.append({"name": p.name, "rel": rel, "is_dir": p.is_dir(),
                        "size": p.stat().st_size if p.is_file() else 0})
    parent = None
    if path:
        parent_rel = str(Path(path).parent)
        parent = "" if parent_rel == "." else parent_rel
    return templates.TemplateResponse("tree.html", {
        "request": request, "host": host, "ts": ts, "entries": entries, "parent": parent,
    })


@router.get("/sites/{host}/view")
async def view(host: str, ts: str, path: str = "index.html"):
    base = _host_dir(host) / ts
    f = _safe_path(base, path)
    if f.is_dir():
        f = f / "index.html"
    if not f.exists():
        raise HTTPException(404)
    return FileResponse(f)


@router.get("/sites/{host}/edit", response_class=HTMLResponse)
async def edit_get(request: Request, host: str, ts: str, path: str):
    base = _host_dir(host) / ts
    f = _safe_path(base, path)
    if not f.is_file():
        raise HTTPException(404)
    ext = f.suffix.lower()
    if ext not in TEXT_EXTS:
        raise HTTPException(415, "Not a text file")
    content = f.read_text(encoding="utf-8", errors="replace")
    parent_rel = str(Path(path).parent)
    parent = "" if parent_rel == "." else parent_rel
    return templates.TemplateResponse("editor.html", {
        "request": request, "host": host, "ts": ts, "path": path,
        "content": content, "mode": MODE_MAP.get(ext, "text"), "parent": parent,
    })


@router.post("/sites/{host}/edit")
async def edit_post(host: str, ts: str, path: str, content: str = Form(...)):
    base = _host_dir(host) / ts
    f = _safe_path(base, path)
    if not f.is_file():
        raise HTTPException(404)
    f.write_text(content, encoding="utf-8")
    return RedirectResponse(f"/sites/{host}/edit?ts={ts}&path={path}", status_code=303)
