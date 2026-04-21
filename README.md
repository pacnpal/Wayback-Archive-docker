# Wayback Archive Dashboard

[![GitHub release](https://img.shields.io/github/v/release/pacnpal/Wayback-Archive-Dashboard?logo=github&label=release)](https://github.com/pacnpal/Wayback-Archive-Dashboard/releases/latest)
[![Build](https://img.shields.io/github/actions/workflow/status/pacnpal/Wayback-Archive-Dashboard/docker-publish.yml?logo=github&label=build)](https://github.com/pacnpal/Wayback-Archive-Dashboard/actions/workflows/docker-publish.yml)
[![Docker Hub](https://img.shields.io/docker/v/pacnpal/wayback-archive-dashboard?logo=docker&label=docker%20hub&sort=semver)](https://hub.docker.com/r/pacnpal/wayback-archive-dashboard)
[![Docker pulls](https://img.shields.io/docker/pulls/pacnpal/wayback-archive-dashboard?logo=docker)](https://hub.docker.com/r/pacnpal/wayback-archive-dashboard)
[![GHCR](https://img.shields.io/badge/ghcr-pacnpal%2Fwayback--archive--dashboard-2496ED?logo=github)](https://github.com/pacnpal/Wayback-Archive-Dashboard/pkgs/container/wayback-archive-dashboard)

A FastAPI + htmx web dashboard around
[GeiserX/Wayback-Archive](https://github.com/GeiserX/Wayback-Archive).
Queue archive jobs, schedule recurring snapshots, browse/repair
archived sites — all from the browser.

Packaged as a single Docker image with the upstream CLI, emoji
favicon, SSE-driven live updates, and a resume-aware job queue.

## Features

- **Job queue** — parallel archive runs with a global concurrency
  control (defaults to 3, hard-capped at 10), live `%` progress bar
  per row, sortable filterable jobs table, bulk-cancel and
  bulk-delete.
- **Rate-limit gate** — a process-wide sliding window caps all CDX
  traffic at 50 req/min (Internet Archive's ceiling is 60) and
  installs IA's published exponential hard block on any `429` —
  1h → 2h → 4h → 8h → … Coordinated across the FastAPI process and
  every shim subprocess via the dashboard DB so no combination of
  settings can push past the limit. See
  [Rate limiting](#rate-limiting).
- **Sites + Snapshots** — per-host overview showing size / file-count
  / asset-health per snapshot, bulk delete, audit-details view,
  in-place link rewriting for served archives.
- **Snapshot picker + date range** — enqueue a specific Wayback
  timestamp or fan out a date range with year/month/day/every
  sampling.
- **Scheduler** — cron-style recurring archives; fires with proper
  crash/restart recovery.
- **Missing-asset audit + repair** — after each successful archive
  the app diffs referenced-but-missing files out of the HTML/CSS and
  auto-queues a repair job that re-fetches just the gaps (with
  multi-timestamp CDX fallback).
- **Resume on restart** — the worker wraps the upstream CLI with a
  shim that serves from disk for already-downloaded files and purges
  any file that was mid-write when the process was killed.
- **Structured logs** — `LOG_LEVEL=INFO|DEBUG|WARNING|ERROR` to
  `docker logs`, with per-job progress ticks every 10 s.
- **SSE-driven UI** — jobs list updates instantly (no polling) on any
  mutation; 30 s fallback poll as a safety net.
- **htmx 4 native** — morph swaps, view transitions, preload on nav,
  error toast on 4xx/5xx, partial-only refresh scoped to
  `#jobs-tbody`.

## Quick start

Prebuilt multi-arch images are published on every release and every
push to `master`:

- Docker Hub: `pacnpal/wayback-archive-dashboard:latest` (or
  `:X.Y.Z` for a pinned release)
- GHCR: `ghcr.io/pacnpal/wayback-archive-dashboard:latest`

### docker run

```bash
docker run -d --name wayback-archive \
  -p 8765:8765 \
  -v /mnt/user/appdata/wayback-archive:/app/output \
  --restart unless-stopped \
  pacnpal/wayback-archive-dashboard:latest
# open http://<host>:8765
```

### docker compose

```yaml
services:
  wayback-archive:
    image: pacnpal/wayback-archive-dashboard:latest
    container_name: wayback-archive
    ports:
      - "8765:8765"
    environment:
      OUTPUT_DIR: /app/output
      # LOG_LEVEL: INFO        # DEBUG | INFO | WARNING | ERROR
      # MAX_CONCURRENT: 3
    volumes:
      - /mnt/user/appdata/wayback-archive:/app/output
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8765/health', timeout=3).status==200 else 1)"]
      interval: 30s
      timeout: 5s
      start_period: 10s
      retries: 3
```

Pin to a specific release by swapping `:latest` for `:1.2.3` (or
whatever tag the GitHub release produced).

### Build from source

```bash
git clone https://github.com/pacnpal/Wayback-Archive-Dashboard.git
cd Wayback-Archive-Dashboard
docker compose up -d --build
```

Archives land in `/mnt/user/appdata/wayback-archive/` on the host
(change the bind mount if you prefer a different path). The dashboard
remembers its SQLite state in `<OUTPUT_DIR>/.dashboard.db` so job
history and schedules survive container rebuilds.

## Releases

Tagged releases (`vX.Y.Z` via GitHub Releases) automatically build and
publish multi-arch images (linux/amd64, linux/arm64) to both Docker
Hub and GHCR with the following tags:

- `X.Y.Z`    — the exact release version
- `X.Y`      — rolling tag for the minor line
- `latest`   — always points at the most recent build on the default branch
- `sha-abc1234` — immutable commit pin

## Configuration

Environment variables set in `docker-compose.yml`:

| Var | Default | Purpose |
| --- | --- | --- |
| `OUTPUT_DIR` | `/app/output` | Where archived snapshots + the SQLite DB live. Bind-mount this to a host directory. |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` — controls both app logs and uvicorn's access log. |
| `MAX_CONCURRENT` | `3` | Default parallel-downloads cap; also configurable at runtime via the Dashboard UI. **Hard-capped at 10** in code — higher values are clamped on read. |

Upstream `wayback_archive` flags (OPTIMIZE_HTML, REMOVE_ADS,
MAKE_INTERNAL_LINKS_RELATIVE, MAX_FILES, etc.) are chosen per-job from
the dashboard. See
[GeiserX/Wayback-Archive](https://github.com/GeiserX/Wayback-Archive)
for the full list of what they do.

Per-job `FETCH_WORKERS` (asset prefetch threads) is **hard-capped at 8**
server-side in `_run_one`, so a flag set via the API or a legacy DB
row cannot raise it above that.

## Rate limiting

Internet Archive's CDX endpoint allows an average of **60 requests per
minute**. When the limit is exceeded, the server returns `429 Too Many
Requests`. If the client continues to ignore `429` responses for more
than a minute, the IP is blocked at the firewall level for **one
hour**, and **each subsequent violation doubles** the blocking time
(1h → 2h → 4h → 8h → …). Many third-party download scripts don't
account for this and get their users banned.

The dashboard handles it automatically:

- **Process-wide sliding-window gate** — all CDX requests go through
  `webui.rate_limit.acquire()`, which enforces a 50 req/min ceiling
  (10 req/min safety margin below IA's 60). State is stored in the
  dashboard SQLite DB with `BEGIN IMMEDIATE` locking, so the FastAPI
  process and every shim subprocess share one budget. There is no
  setting that raises the ceiling.
- **Passive outage detection** — on `429`, the gate installs an
  exponential hard block (1h, 2h, 4h, 8h, 16h, …, capped at 7 days)
  and flips the worker's outage state to `down`. In-flight jobs that
  fail during the block are requeued with escalating backoff. A
  successful CDX response after the block expires clears it and
  releases deferred jobs. No heartbeat probe is used — outage state
  reflects only the outcome of real caller traffic, so the dashboard
  never contributes its own requests to the budget.
- **Tier decay** — a clean 24 hours with no `429` resets the tier
  back to 1 on the next offense, so an overnight IA outage doesn't
  leave tomorrow's first blip with a multi-day cooldown.
- **`Retry-After` honored** — if IA sends `Retry-After: N` and `N`
  is longer than the current tier's wait, the gate honors `N`.
- **Stern user-facing banner** — when a `429` installs a block, the
  dashboard shows a live per-second countdown to cooldown clear and
  blunt copy explaining that each additional `429` doubles the wait.
  On offense #2+, the banner goes severe-red with explicit "you are
  making this worse — stop queueing jobs, stop clicking retry, wait
  for the timer" copy.

**TL;DR**: you cannot, by configuration or UI action, cause this
dashboard to exceed IA's 60 req/min CDX cap. If IA ever does
rate-limit you anyway (e.g. because someone else on your IP is
hammering it), the dashboard goes cleanly quiet until the ban
expires.

## Architecture

```
┌───────────── browser ─────────────┐
│  htmx 4 + hx-sse + preload         │
│  live countdown in outage banner   │
└─────────┬─────────────────────────┘
          │  /events (SSE)
          │  /jobs/list (partial, morph)
          │  /api/wayback-status (banner)
          ▼
┌───────────── FastAPI app ──────────┐      ┌── worker loop ──┐
│  routes/dashboard.py               │      │ spawns          │
│  routes/sites.py  routes/schedules.│◄────►│  webui.wayback_ │
│  routes/browser.py                 │      │  resume_shim    │──► web.archive.org
│  routes/events.py (SSE fan-out)    │      │  webui.wayback_ │   (playback)
└─────────┬──────────────────────────┘      │  repair_shim    │
          │                                 └────────┬────────┘
          │  rate_limit gate (50 req/min CDX)        │
          │  ┌──── every CDX call ─────────┐         │
          │◄─┤  webui.rate_limit.acquire()  ├────────┤──► web.archive.org
          │  │  BEGIN IMMEDIATE on DB       │         │   /cdx/search/cdx
          │  │  429 → exponential hard block│         │
          │  └──────────────────────────────┘         │
          │ SQLite (.dashboard.db)                    │
          │ SQLite.WAL                                │
          ▼
  /mnt/user/appdata/wayback-archive/
    <host>/
      .index.json         ← per-host size + file-count cache
      <YYYYMMDDHHMMSS>/   ← one snapshot
        index.html, …
        .log              ← upstream stdout + shim messages
        .audit.json       ← ref-vs-disk audit cache
```

Key modules under `webui/`:

- `jobs.py` — SQLite-backed queue, worker loop, enqueue /
  enqueue_repair / cancel / delete helpers; clamps `MAX_CONCURRENT`
  and `FETCH_WORKERS` at their server-side ceilings.
- `rate_limit.py` — process-wide CDX gate, 429-driven exponential
  hard block, coordinated across FastAPI + shim subprocesses via the
  dashboard DB.
- `wayback_probe.py` — outage-state helpers (`ProbeState`,
  `is_wayback_up`, `backoff_seconds`). **No active heartbeat** —
  state is driven passively by `rate_limit` observations.
- `wayback_resume_shim.py` — wraps upstream CLI, disk-cache hits,
  in-flight-file purge on resume.
- `wayback_repair_shim.py` — targeted asset refetch with CDX
  multi-timestamp fallback.
- `asset_audit.py` — walks HTML/CSS, records missing rel paths.
- `link_rewrite.py` — one-shot absolute → relative rewriter so local
  viewing works.
- `events_bus.py` — asyncio fan-out for SSE frames.
- `sites_index.py` — `.index.json` sidecar reader/writer.

## Credits

Upstream engine:
[GeiserX/Wayback-Archive](https://github.com/GeiserX/Wayback-Archive).
This project just bakes it into a Docker image with a FastAPI queue +
htmx dashboard around it. Two small upstream behavior tweaks ride on
top as a runtime shim (resume-from-disk + in-flight purge); the
is_html-detection fix is in
[upstream PR #6](https://github.com/GeiserX/Wayback-Archive/pull/6).
