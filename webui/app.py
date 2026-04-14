"""FastAPI app entrypoint."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import asyncio as _asyncio
import json as _json
from . import jobs, scheduler, log as log_mod, job_progress, events_bus
from .routes import dashboard, browser, schedules as schedules_routes, diff, sites as sites_routes, events as events_routes

BASE = Path(__file__).parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_mod.configure()
    jobs.init_db()
    stop = asyncio.Event()
    t1 = asyncio.create_task(jobs.worker_loop(stop))
    t2 = asyncio.create_task(scheduler.scheduler_loop(stop))
    t3 = asyncio.create_task(_progress_logger(stop))
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(t1, t2, t3, return_exceptions=True)


async def _progress_logger(stop: _asyncio.Event) -> None:
    """Every 10 s, log one line per running job and publish a jobs-changed
    SSE event so connected clients re-fetch the jobs tbody (keeps the
    progress bars updating)."""
    lg = log_mod.get("progress")
    while not stop.is_set():
        try:
            with jobs.connect() as c:
                rows = c.execute(
                    "SELECT id, host, timestamp, log_path, flags_json "
                    "FROM jobs WHERE status='running' ORDER BY id"
                ).fetchall()
            if rows:
                events_bus.publish("jobs-changed")
                for r in rows:
                    try:
                        mf = _json.loads(r["flags_json"] or "{}").get("MAX_FILES")
                        mf = int(mf) if mf and str(mf).isdigit() else None
                    except Exception:
                        mf = None
                    p = job_progress.read_progress(r["log_path"], mf)
                    if p is None:
                        lg.info("job=%d host=%s ts=%s (no log yet)",
                                r["id"], r["host"], r["timestamp"])
                    else:
                        lg.info(
                            "job=%d host=%s ts=%s downloaded=%d queued=%d total=%s percent=%d%%",
                            r["id"], r["host"], r["timestamp"],
                            p["downloaded"], p["queued"],
                            p["total"] or "?", p["percent"],
                        )
        except Exception as e:
            lg.warning("progress tick failed: %s", e)
        try:
            await _asyncio.wait_for(stop.wait(), timeout=10.0)
        except _asyncio.TimeoutError:
            pass


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


_FAVICON_SVG = (
    b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">'
    b'<text y=".9em" font-size="90">\xf0\x9f\x97\x84</text></svg>'
)


@app.get("/favicon.svg")
@app.get("/favicon.ico")
async def favicon():
    from fastapi.responses import Response
    return Response(_FAVICON_SVG, media_type="image/svg+xml",
                    headers={"Cache-Control": "public, max-age=86400"})


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
app.include_router(events_routes.router)
