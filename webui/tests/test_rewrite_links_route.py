"""Regression tests for the previously-broken rewrite_links route. Before the
fix, `host_dir.is_dir()` being True left `resp` unbound; when it was False,
the dead code below the `return resp` meant the rewrite never happened.
"""
import asyncio

import pytest

from webui import jobs
from webui.routes.sites import rewrite_links


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


def test_nonexistent_host_redirects(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)
    resp = _run(rewrite_links(host="ghost.example.com"))
    # Previously raised UnboundLocalError; now returns a 303 redirect.
    assert resp.status_code == 303
    assert resp.headers["location"] == "/sites/ghost.example.com"
    assert resp.headers.get("HX-Trigger") == "jobs-changed"


def test_existing_host_runs_rewrite(tmp_path, monkeypatch):
    # Build a minimal host + one snapshot with an absolute-path ref.
    host = tmp_path / "example.com"
    snap = host / "20240101000000"
    snap.mkdir(parents=True)
    (snap / "index.html").write_text('<a href="/about.html">x</a>')
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)

    resp = _run(rewrite_links(host="example.com"))
    assert resp.status_code == 303
    assert "rewrite_done=1" in resp.headers["location"]
    # Query params include the counters the UI banner reads.
    for expected in ("snapshots=1", "refs_rewritten=1", "files_changed=1"):
        assert expected in resp.headers["location"]
    assert resp.headers.get("HX-Trigger") == "jobs-changed"


def test_single_ts_rewrite(tmp_path, monkeypatch):
    host = tmp_path / "example.com"
    snap1 = host / "20240101000000"
    snap2 = host / "20240202000000"
    snap1.mkdir(parents=True)
    snap2.mkdir(parents=True)
    (snap1 / "index.html").write_text('<img src="/a.gif">')
    (snap2 / "index.html").write_text('<img src="/b.gif">')
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)

    resp = _run(rewrite_links(host="example.com", ts="20240101000000"))
    assert "snapshots=1" in resp.headers["location"]  # only the targeted snap


def test_orphaned_folder_excluded_from_bulk(tmp_path, monkeypatch):
    host = tmp_path / "example.com"
    snap = host / "20240101000000"
    orphan = host / "_orphaned"
    snap.mkdir(parents=True)
    orphan.mkdir(parents=True)
    (snap / "index.html").write_text('<a href="/x">x</a>')
    (orphan / "stray.html").write_text('<a href="/y">y</a>')
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)

    _run(rewrite_links(host="example.com"))
    # The orphan HTML must not have been touched.
    assert (orphan / "stray.html").read_text() == '<a href="/y">y</a>'
