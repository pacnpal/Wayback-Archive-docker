"""Tests for the http/https scheme handling that v0.4.2 added.

Two fixes covered:

  1. ``enqueue_repair`` infers scheme from the parent archive job's
     ``target_url`` instead of hardcoding ``https://``. Pre-HTTPS-era
     captures are all ``http://`` in CDX, so the hardcode silently
     broke auto-repair for 1990s/early-2000s sites.

  2. ``wayback.probe_scheme`` checks CDX for captures under each
     scheme so the enqueue routes stop blindly prepending
     ``https://`` when the user types a bare host. Returns ``http``
     when only http captures exist, ``https`` otherwise.
"""
from __future__ import annotations

import importlib
import json


def _fresh_jobs(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    return j


def _seed_archive_job(j, *, host: str, timestamp: str, scheme: str) -> int:
    """Insert a non-repair archive job for ``(host, ts)`` so
    enqueue_repair has a parent to infer scheme from."""
    site_dir = str(j.OUTPUT_ROOT / host / timestamp)
    with j.connect() as c:
        cur = c.execute(
            "INSERT INTO jobs (target_url, timestamp, wayback_url, host, "
            "site_dir, log_path, flags_json, status, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (f"{scheme}://{host}/", timestamp,
             f"https://web.archive.org/web/{timestamp}/{scheme}://{host}/",
             host, site_dir, site_dir + "/.log", "{}", "ok", j.now_iso()),
        )
    return cur.lastrowid


def test_repair_inherits_http_scheme_from_parent(tmp_path, monkeypatch):
    """Auto-repair of a 1996 snapshot must use http:// (the scheme
    its parent archive job used), not the previous hardcoded
    https://. Otherwise alt_timestamps lookups find zero captures
    and the repair marks everything unrecoverable."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    _seed_archive_job(j, host="www.compaq.com",
                      timestamp="19961225051932", scheme="http")
    repair_id = j.enqueue_repair(
        "www.compaq.com", "19961225051932", ["index.html", "logo.gif"]
    )
    with j.connect() as c:
        row = c.execute(
            "SELECT target_url, wayback_url FROM jobs WHERE id=?",
            (repair_id,),
        ).fetchone()
    assert row["target_url"] == "http://www.compaq.com/"
    assert row["wayback_url"] == (
        "https://web.archive.org/web/19961225051932/http://www.compaq.com/"
    )


def test_repair_inherits_https_scheme_from_parent(tmp_path, monkeypatch):
    """Modern site case — parent was https, repair must match."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    _seed_archive_job(j, host="example.com",
                      timestamp="20240101000000", scheme="https")
    repair_id = j.enqueue_repair(
        "example.com", "20240101000000", ["index.html"]
    )
    with j.connect() as c:
        row = c.execute(
            "SELECT target_url FROM jobs WHERE id=?", (repair_id,),
        ).fetchone()
    assert row["target_url"] == "https://example.com/"


def test_repair_falls_back_to_https_when_no_parent(tmp_path, monkeypatch):
    """Manual repair enqueue with no prior archive for the (host, ts)
    pair — can't infer, so pick the modern default. Edge case in
    real use but must not crash."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    repair_id = j.enqueue_repair(
        "unknown.example", "20240101000000", ["index.html"]
    )
    with j.connect() as c:
        row = c.execute(
            "SELECT target_url FROM jobs WHERE id=?", (repair_id,),
        ).fetchone()
    assert row["target_url"] == "https://unknown.example/"


def test_repair_prefers_most_recent_parent(tmp_path, monkeypatch):
    """If multiple archive jobs exist for the same (host, ts) — e.g.
    an original http:// run plus a later user-retriggered https://
    duplicate — prefer the most recent one (largest id)."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    _seed_archive_job(j, host="www.compaq.com",
                      timestamp="19961225051932", scheme="http")
    _seed_archive_job(j, host="www.compaq.com",
                      timestamp="19961225051932", scheme="https")
    repair_id = j.enqueue_repair(
        "www.compaq.com", "19961225051932", ["index.html"]
    )
    with j.connect() as c:
        row = c.execute(
            "SELECT target_url FROM jobs WHERE id=?", (repair_id,),
        ).fetchone()
    # Most recent is the https duplicate — that's what inherits.
    # (The real fix for the user's scheme confusion happens at
    # enqueue time via probe_scheme; once that's in place, no
    # https duplicates get created in the first place.)
    assert row["target_url"] == "https://www.compaq.com/"


def test_probe_scheme_picks_https_when_available(tmp_path, monkeypatch):
    """probe_scheme must prefer https when captures exist there —
    the modern-site common case."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback as wb
    importlib.reload(wb)

    seen: list[str] = []

    def fake_list_snapshots(url, **kw):
        seen.append(url)
        if url.startswith("https://"):
            return [{"timestamp": "20240101000000", "original": url}]
        return []

    monkeypatch.setattr(wb, "list_snapshots", fake_list_snapshots)
    assert wb.probe_scheme("example.com") == "https"
    # Should only probe https (the https lookup returned captures, no
    # fallback needed).
    assert seen == ["https://example.com/"]


def test_probe_scheme_falls_back_to_http(tmp_path, monkeypatch):
    """probe_scheme must detect http-only sites (pre-HTTPS-era) and
    pick http:// — the bug class the user hit with compaq.com."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback as wb
    importlib.reload(wb)

    seen: list[str] = []

    def fake_list_snapshots(url, **kw):
        seen.append(url)
        if url.startswith("http://"):
            return [{"timestamp": "19961225051932", "original": url}]
        return []

    monkeypatch.setattr(wb, "list_snapshots", fake_list_snapshots)
    assert wb.probe_scheme("www.compaq.com") == "http"
    assert seen == ["https://www.compaq.com/", "http://www.compaq.com/"]


def test_probe_scheme_preserves_explicit_scheme(tmp_path, monkeypatch):
    """If the user typed the scheme themselves, trust it — don't
    round-trip to CDX for no reason."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback as wb
    importlib.reload(wb)

    def fake_list_snapshots(url, **kw):
        raise AssertionError("probe should skip CDX when scheme is explicit")

    monkeypatch.setattr(wb, "list_snapshots", fake_list_snapshots)
    assert wb.probe_scheme("http://example.com/") == "http"
    assert wb.probe_scheme("https://example.com/") == "https"


def test_probe_scheme_defaults_https_when_no_captures(tmp_path, monkeypatch):
    """If neither scheme has captures, pick the modern default — the
    downstream enqueue will fail with a clear error anyway. Must not
    raise from the probe itself."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback as wb
    importlib.reload(wb)

    monkeypatch.setattr(wb, "list_snapshots", lambda *a, **kw: [])
    assert wb.probe_scheme("never-archived.example") == "https"


def test_probe_scheme_bubbles_up_cdx_unreachable(tmp_path, monkeypatch):
    """Rate-limited CDX must not silently return a guessed scheme —
    bubble the error so the caller can show it to the user instead
    of queueing a job with the wrong scheme and wasting cycles."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback as wb
    importlib.reload(wb)

    def down(*a, **kw):
        raise wb.WaybackUnreachable("gate refused")

    monkeypatch.setattr(wb, "list_snapshots", down)
    try:
        wb.probe_scheme("example.com")
    except wb.WaybackUnreachable:
        return
    raise AssertionError("probe_scheme should have raised WaybackUnreachable")
