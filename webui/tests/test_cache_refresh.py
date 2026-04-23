"""Tests for manual cache-refresh helpers and the /api/cache/refresh endpoint."""
from __future__ import annotations

import asyncio
import json

import pytest

from webui import jobs, sites_index, wayback
from webui.routes.dashboard import api_cache_refresh


# ---------------------------------------------------------------------------
# wayback.clear_cache()
# ---------------------------------------------------------------------------

def test_clear_cache_empties_cdx_entries(monkeypatch):
    """clear_cache() wipes all in-memory CDX entries and returns the count."""
    import time

    # Seed the module-level _CACHE directly.
    fake_entries = {
        "https://example.com|None|None|500|8": (time.time(), [{"timestamp": "20240101000000"}]),
        "https://other.com|None|None|500|8": (time.time(), []),
    }
    monkeypatch.setattr(wayback, "_CACHE", fake_entries)

    count = wayback.clear_cache()

    assert count == 2
    assert len(wayback._CACHE) == 0


def test_clear_cache_when_already_empty(monkeypatch):
    monkeypatch.setattr(wayback, "_CACHE", {})
    assert wayback.clear_cache() == 0


# ---------------------------------------------------------------------------
# sites_index.refresh_all_hosts()
# ---------------------------------------------------------------------------

def _write(p, data=b"x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        p.write_text(data)
    else:
        p.write_bytes(data)


def test_refresh_all_hosts_returns_host_snapshot_counts(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)

    _write(tmp_path / "example.com" / "20240101000000" / "index.html")
    _write(tmp_path / "example.com" / "20240202000000" / "index.html")
    _write(tmp_path / "other.org" / "20230601000000" / "index.html")

    result = sites_index.refresh_all_hosts()

    assert result == {"example.com": 2, "other.org": 1}


def test_refresh_all_hosts_skips_invalid_dirs(tmp_path, monkeypatch):
    """Directories whose names don't match the host regex are silently ignored."""
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)

    _write(tmp_path / "example.com" / "20240101000000" / "index.html")
    (tmp_path / "_orphaned").mkdir()          # leading underscore → not a host
    (tmp_path / "..invalid").mkdir()          # fails HOST_RE
    (tmp_path / ".hidden").mkdir()            # dot-prefix → fails HOST_RE

    result = sites_index.refresh_all_hosts()

    assert set(result.keys()) == {"example.com"}


def test_refresh_all_hosts_empty_root(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)
    assert sites_index.refresh_all_hosts() == {}


def test_refresh_all_hosts_writes_index_json(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)
    _write(tmp_path / "example.com" / "20240101000000" / "page.html", b"hello")

    sites_index.refresh_all_hosts()

    idx_path = tmp_path / "example.com" / sites_index.INDEX_NAME
    assert idx_path.is_file()
    data = json.loads(idx_path.read_text())
    assert "20240101000000" in data
    assert data["20240101000000"]["file_count"] == 1


# ---------------------------------------------------------------------------
# POST /api/cache/refresh (route)
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def _patch_cache_deps(tmp_path, monkeypatch):
    """Patch OUTPUT_ROOT and pre-seed a CDX cache entry for route tests."""
    import time

    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)
    seed = {"https://example.com|None|None|500|8": (time.time(), [])}
    monkeypatch.setattr(wayback, "_CACHE", seed)

    _write(tmp_path / "example.com" / "20240101000000" / "index.html")
    return tmp_path


def test_api_cache_refresh_clears_cdx_and_reindexes(_patch_cache_deps, monkeypatch):
    """The route must clear the CDX cache and re-index host dirs."""
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", _patch_cache_deps)

    from starlette.requests import Request as StarletteRequest
    from starlette.datastructures import Headers

    # Build a minimal mock request (the endpoint doesn't inspect it).
    class _FakeRequest:
        headers = Headers(raw=[])
        app = None

    resp = _run(api_cache_refresh(_FakeRequest()))

    assert resp.status_code == 200
    body = resp.body.decode()
    assert "✓" in body
    assert "CDX" in body
    assert "re-indexed" in body
    # CDX cache was cleared.
    assert len(wayback._CACHE) == 0


def test_api_cache_refresh_via_test_client(tmp_path, monkeypatch):
    """End-to-end: POST /api/cache/refresh through the full ASGI app."""
    import time
    import importlib

    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()

    # Seed a CDX entry so we can verify it disappears.
    seed = {"https://example.com|None|None|500|8": (time.time(), [])}
    import webui.wayback as wb
    importlib.reload(wb)
    wb._CACHE.update(seed)

    _write(tmp_path / "example.com" / "20240101000000" / "index.html")

    from webui import app as app_mod
    importlib.reload(app_mod)
    from starlette.testclient import TestClient

    with TestClient(app_mod.app) as client:
        r = client.post("/api/cache/refresh")

    assert r.status_code == 200
    assert "✓" in r.text
    assert "CDX" in r.text
