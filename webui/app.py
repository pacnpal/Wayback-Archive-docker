"""FastAPI app entrypoint."""
from __future__ import annotations
import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

# Version is baked in at Docker-build time via ARG APP_VERSION in the
# Dockerfile (release workflow passes github.ref_name). Falls back to "dev"
# for local/ad-hoc runs.
APP_VERSION = os.environ.get("APP_VERSION", "dev")
GITHUB_URL = "https://github.com/pacnpal/Wayback-Archive-Dashboard"

import asyncio as _asyncio
import json as _json
from . import jobs, scheduler, log as log_mod, job_progress, events_bus, wayback_probe
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
    t4 = asyncio.create_task(wayback_probe.probe_loop(stop))
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(t1, t2, t3, t4, return_exceptions=True)


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
app.state.version = APP_VERSION
app.state.github_url = GITHUB_URL
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
    from .routes._validators import valid_host, valid_ts
    m = re.match(r"^(\d{4,14})[a-z_]*/(https?://)([^/]+)(/.*)?$", rest)
    if not m:
        raise HTTPException(404)
    ts_raw, _scheme, host_raw, path = m.group(1), m.group(2), m.group(3), m.group(4) or "/"
    # Only 14-digit timestamps are valid snapshot dirs on disk; validate both.
    host = valid_host(host_raw)
    if len(ts_raw) != 14:
        raise HTTPException(404)
    ts = valid_ts(ts_raw)
    root = jobs.OUTPUT_ROOT.resolve()
    base = (jobs.OUTPUT_ROOT / host / ts).resolve()
    if not base.is_dir() or not base.is_relative_to(root):
        raise HTTPException(404)
    rel = path.lstrip("/") or "index.html"
    target = (base / rel).resolve()
    if not target.is_relative_to(base):
        raise HTTPException(400)
    if target.is_dir():
        target = target / "index.html"
    if not target.is_file():
        raise HTTPException(404)
    return FileResponse(target)


@app.get("/favicon.ico")
async def favicon():
    from fastapi.responses import FileResponse
    return FileResponse(BASE / "static/favicon.ico", media_type="image/x-icon",
                        headers={"Cache-Control": "public, max-age=86400"})


@app.get("/health")
async def health():
    try:
        with jobs.connect() as c:
            c.execute("SELECT 1").fetchone()
    except Exception:
        # Don't leak exception text to clients; log it for operators.
        log_mod.get("health").exception("health check failed")
        from fastapi.responses import JSONResponse
        return JSONResponse({"status": "error"}, status_code=503)
    return {"status": "ok"}


app.include_router(dashboard.router)
app.include_router(browser.router)
app.include_router(schedules_routes.router)
app.include_router(diff.router)
app.include_router(sites_routes.router)
app.include_router(events_routes.router)
