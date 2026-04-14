"""FastAPI app entrypoint."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
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
async def wayback_passthrough(rest: str, request: Request):
    """Archived HTML sometimes contains un-rewritten Wayback URLs like
    /web/<ts>/http://example.com/img.gif. Browsers resolve those against our
    own origin (giving 404s inside the viewer iframe). Redirect them to the
    real Wayback Machine so the content still loads."""
    from fastapi.responses import RedirectResponse
    qs = request.url.query
    target = f"https://web.archive.org/web/{rest}" + (f"?{qs}" if qs else "")
    return RedirectResponse(target, status_code=302)


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
