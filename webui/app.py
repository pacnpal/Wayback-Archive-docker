"""FastAPI app entrypoint."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from . import jobs, scheduler
from .routes import dashboard, browser, schedules as schedules_routes, diff, sites as sites_routes

BASE = Path(__file__).parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    jobs.init_db()
    stop = asyncio.Event()
    t1 = asyncio.create_task(jobs.worker_loop(stop))
    t2 = asyncio.create_task(scheduler.scheduler_loop(stop))
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(t1, t2, return_exceptions=True)


app = FastAPI(title="Wayback Archive Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")
app.mount("/archives", StaticFiles(directory=jobs.OUTPUT_ROOT, html=True), name="archives")

@app.get("/web/{rest:path}")
async def wayback_local(rest: str):
    """Archived HTML often contains Wayback-rooted paths like
    /web/<ts>[flags]/http://host/path. Serve the asset from our local snapshot
    directory if we have it; 404 otherwise. Mapping:
      /web/<ts>[im_|if_|cs_|...]/http(s)://<host>/<path>
      → <OUTPUT_ROOT>/<host>/<ts>/<path>
    """
    import re
    from fastapi import HTTPException
    from fastapi.responses import FileResponse
    m = re.match(r"^(\d{4,14})[a-z_]*/(https?://)([^/]+)(/.*)?$", rest)
    if not m:
        raise HTTPException(404)
    ts, _scheme, host, path = m.group(1), m.group(2), m.group(3), m.group(4) or "/"
    base = (jobs.OUTPUT_ROOT / host / ts).resolve()
    if not base.is_dir():
        raise HTTPException(404)
    rel = path.lstrip("/") or "index.html"
    target = (base / rel).resolve()
    if base != target and base not in target.parents:
        raise HTTPException(400)
    if target.is_dir():
        target = target / "index.html"
    if not target.is_file():
        raise HTTPException(404)
    return FileResponse(target)


@app.get("/health")
async def health():
    try:
        with jobs.connect() as c:
            c.execute("SELECT 1").fetchone()
    except Exception as e:
        from fastapi.responses import JSONResponse
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=503)
    return {"status": "ok"}


app.include_router(dashboard.router)
app.include_router(browser.router)
app.include_router(schedules_routes.router)
app.include_router(diff.router)
app.include_router(sites_routes.router)
