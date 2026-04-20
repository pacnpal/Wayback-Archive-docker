"""Snapshot index lazily refreshes when the cache went stale.

Pre-fix bug: get_index() only re-measured snapshots whose entries were
missing or whose dirs were deleted. Snapshots whose contents grew after
the first measurement (partial download captured by an early page-load,
followed by repair / link-rewrite / search-index / completion) kept
showing the old low file_count and size_bytes forever."""
import json
import os
import time

from webui import jobs, sites_index


def _write(p, data=b"x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        p.write_text(data)
    else:
        p.write_bytes(data)


def test_measure_counts_every_file_in_tree(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)
    snap = tmp_path / "example.com" / "20240101000000"
    _write(snap / "index.html", b"<html></html>")    # 13 B
    _write(snap / "img" / "a.png", b"GIF89a" * 10)   # 60 B
    _write(snap / ".log", b"job log " * 100)         # 800 B (job sidecar still counts)

    m = sites_index._measure(snap)
    assert m["file_count"] == 3
    assert m["size_bytes"] == 13 + 60 + 800
    assert m["v"] == sites_index.SNAPSHOT_VERSION


def test_get_index_refreshes_when_cache_predates_version_marker(tmp_path, monkeypatch):
    """Old caches written before the staleness check exist on real
    deployments and froze numbers at the snapshot's first measurement.
    Missing `v` field forces re-measure on next page load."""
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)
    host = tmp_path / "example.com"
    snap = host / "20240101000000"
    _write(snap / "index.html", b"hello")
    _write(snap / "img" / "a.png", b"GIF89a")

    stale = {"20240101000000": {"size_bytes": 5, "file_count": 1,
                                "mtime": "1970-01-01T00:00:00+00:00"}}
    (host / sites_index.INDEX_NAME).write_text(json.dumps(stale))

    idx = sites_index.get_index("example.com")
    assert idx["20240101000000"]["file_count"] == 2
    assert idx["20240101000000"]["size_bytes"] == 5 + 6
    assert idx["20240101000000"]["v"] == sites_index.SNAPSHOT_VERSION

    on_disk = json.loads((host / sites_index.INDEX_NAME).read_text())
    assert on_disk["20240101000000"]["file_count"] == 2


def test_get_index_refreshes_when_dir_mtime_moves(tmp_path, monkeypatch):
    """A repair / search-index / cleanup run that touches the snapshot dir
    bumps its mtime; the next get_index() call must re-measure rather than
    serve the cached numbers."""
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)
    snap = tmp_path / "example.com" / "20240101000000"
    _write(snap / "index.html", b"hi")

    first = sites_index.get_index("example.com")["20240101000000"]
    assert first["file_count"] == 1
    assert first["v"] == sites_index.SNAPSHOT_VERSION

    _write(snap / "img" / "a.png", b"GIF89a")
    new_mtime = time.time() + 5  # bump past coarse-fs mtime resolution
    os.utime(snap, (new_mtime, new_mtime))

    second = sites_index.get_index("example.com")["20240101000000"]
    assert second["file_count"] == 2
    assert second["size_bytes"] == 2 + 6


def test_get_index_serves_cache_when_nothing_changed(tmp_path, monkeypatch):
    """Stable snapshots must not be re-measured on every page load."""
    monkeypatch.setattr(jobs, "OUTPUT_ROOT", tmp_path)
    snap = tmp_path / "example.com" / "20240101000000"
    _write(snap / "index.html", b"hi")

    sites_index.get_index("example.com")  # populate cache
    sites_index._measure = lambda *_a, **_kw: (_ for _ in ()).throw(
        AssertionError("re-measure of an unchanged snapshot")
    )
    try:
        idx = sites_index.get_index("example.com")
    finally:
        # Restore the real implementation for any later tests in the run.
        from importlib import reload
        reload(sites_index)
    assert idx["20240101000000"]["file_count"] == 1
