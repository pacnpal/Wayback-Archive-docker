"""Targeted re-fetch of specific missing assets for an already-completed
snapshot. Driven by REPAIR_PATHS env (comma-separated snapshot-relative
paths). Uses upstream WaybackDownloader's download_file and writes to
<OUTPUT_DIR>/<rel_path> atomically."""
from __future__ import annotations
import logging
import os
import sys
import tempfile
import time
from pathlib import Path


def _setup_logger() -> logging.Logger:
    lg = logging.getLogger("wayback.repair")
    if lg.handlers:
        return lg
    lg.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s wayback.repair: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    h1 = logging.StreamHandler(sys.stderr)
    h1.setFormatter(fmt)
    lg.addHandler(h1)
    try:
        f = open("/proc/1/fd/1", "w", buffering=1)
        h2 = logging.StreamHandler(f)
        h2.setFormatter(fmt)
        lg.addHandler(h2)
    except Exception:
        pass
    return lg


log = _setup_logger()


from .cdx import alt_timestamps as _alt_timestamps, raw_fetch as _download_from_wayback


def _write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".part.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, str(path))
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise


def main() -> int:
    out_dir = os.environ.get("OUTPUT_DIR")
    wayback_url = os.environ.get("WAYBACK_URL", "")
    paths_env = os.environ.get("REPAIR_PATHS", "")
    if not out_dir or not wayback_url or not paths_env:
        log.error("missing OUTPUT_DIR / WAYBACK_URL / REPAIR_PATHS")
        return 2
    rel_paths = [p.strip() for p in paths_env.split("|") if p.strip()]
    if not rel_paths:
        log.error("no paths to repair")
        return 2

    # Derive host + timestamp from WAYBACK_URL.
    import re
    m = re.match(r"https?://web\.archive\.org/web/(\d+)[^/]*/(https?://)([^/]+)", wayback_url)
    if not m:
        log.error("cannot parse WAYBACK_URL: %s", wayback_url)
        return 2
    scheme = m.group(2).rstrip("/")
    host = m.group(3)

    from wayback_archive.config import Config
    from wayback_archive.downloader import WaybackDownloader
    cfg = Config()
    d = WaybackDownloader(cfg)
    ts_primary = d.original_timestamp

    ok = 0
    failed = 0
    fallback_hits = 0
    t0 = time.monotonic()
    total = len(rel_paths)
    log.info("start host=%s ts=%s paths=%d", host, ts_primary, total)
    print(f"\n{'='*70}\nAsset repair: {host}\n"
          f"Paths to fetch: {total}\n{'='*70}\n", flush=True)

    for i, rel in enumerate(rel_paths, 1):
        local = (Path(out_dir) / rel).resolve()
        if Path(out_dir).resolve() not in local.parents:
            log.warning("skip unsafe path=%s", rel)
            continue
        orig_url = f"{scheme}//{host}/{rel.lstrip('/')}"
        print(f"[{i}/{total}] Downloading Asset: {orig_url}", flush=True)

        # 1) Try the exact timestamp via upstream (handles its own fallbacks).
        try:
            content = d.download_file(orig_url)
        except Exception as e:
            content = None
            log.debug("primary fetch error rel=%s err=%s", rel, e)

        # 2) Still missing → ask CDX for other timestamps that have this URL
        #    with a 200, sorted by proximity, and try each id_ fetch.
        used_ts = ts_primary if content else None
        if not content:
            alts = _alt_timestamps(orig_url, ts_primary, limit=30)
            if alts:
                print(f"         🔍 trying {len(alts)} alt snapshot(s)…", flush=True)
            for alt in alts[:10]:
                data = _download_from_wayback(d.session, alt, orig_url)
                if data:
                    content = data
                    used_ts = alt
                    fallback_hits += 1
                    print(f"         ✓ matched alt snapshot {alt}", flush=True)
                    break

        if not content:
            print("         ⚠️  Failed to download", flush=True)
            log.info("repair rel=%s status=fail", rel)
            failed += 1
            continue
        try:
            _write_atomic(local, content)
            size_kb = len(content) / 1024
            note = "" if used_ts == ts_primary else f" (from {used_ts})"
            print(f"         ✓ Downloaded ({size_kb:.1f} KB){note}", flush=True)
            log.info("repair rel=%s status=ok bytes=%d ts=%s",
                     rel, len(content), used_ts)
            ok += 1
        except Exception as e:
            log.warning("write failed rel=%s err=%s", rel, e)
            failed += 1

    dur = time.monotonic() - t0
    print(f"\n{'='*70}\nRepair complete\nFiles successfully downloaded: {ok}\n"
          f"  (of those, {fallback_hits} came from a different snapshot)\n"
          f"Files failed: {failed}\n{'='*70}\n", flush=True)
    log.info("end ok=%d fallback=%d failed=%d duration=%.1fs",
             ok, fallback_hits, failed, dur)
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
