from pathlib import Path

from webui.cleanup_orphans import (
    cleanup_output_root,
    cleanup_host,
    cleanup_all,
    ORPHAN_DIR,
)


def _populate(root: Path):
    """Build a realistic layout with one well-formed host + one leaked file
    at root + stray entries under the host."""
    host = root / "example.com"
    snap = host / "20240101000000"
    snap.mkdir(parents=True)
    (snap / "index.html").write_text("<html></html>")

    # Leaked at OUTPUT_ROOT level:
    (root / "leaked.html").write_text("<html>stray</html>")
    (root / "stray_dir").mkdir()  # no snapshot-ts inside → orphan

    # Kept at OUTPUT_ROOT level:
    (root / ".dashboard.db").write_text("")
    (root / ".index.json").write_text("{}")

    # Stray under host (not a snapshot-ts):
    (host / "images").mkdir()
    (host / "images" / "a.gif").write_bytes(b"GIF89a")
    (host / "index.html").write_text("<html>loose</html>")
    # Kept under host:
    (host / ".index.json").write_text("{}")


def test_cleanup_output_root_moves_only_root_leaks(tmp_path):
    _populate(tmp_path)
    summary = cleanup_output_root(tmp_path)
    assert summary["count"] == 2
    moved_names = {Path(m["dst"]).name for m in summary["moved"]}
    assert "leaked.html" in moved_names
    assert "stray_dir" in moved_names
    # The host dir and kept files stay put.
    assert (tmp_path / "example.com" / "20240101000000").is_dir()
    assert (tmp_path / ".dashboard.db").is_file()
    assert (tmp_path / ORPHAN_DIR).is_dir()


def test_cleanup_host_moves_non_snapshot_entries(tmp_path):
    _populate(tmp_path)
    host = tmp_path / "example.com"
    summary = cleanup_host(host)
    assert summary["count"] == 2
    moved_names = {Path(m["dst"]).name for m in summary["moved"]}
    assert "images" in moved_names
    assert "index.html" in moved_names
    # Snapshot dir + kept index stay put.
    assert (host / "20240101000000").is_dir()
    assert (host / ".index.json").is_file()
    assert (host / ORPHAN_DIR).is_dir()


def test_cleanup_all_aggregates(tmp_path):
    _populate(tmp_path)
    summary = cleanup_all(tmp_path)
    assert summary["total"] == 4  # 2 root + 2 host
    assert summary["root"]["count"] == 2
    assert summary["hosts"]["example.com"]["count"] == 2


def test_cleanup_all_idempotent(tmp_path):
    _populate(tmp_path)
    cleanup_all(tmp_path)
    # Second run should find nothing to move.
    summary = cleanup_all(tmp_path)
    assert summary["total"] == 0


def test_quarantine_collision_gets_timestamp_suffix(tmp_path):
    # First run quarantines `leaked.html` into OUTPUT_ROOT/_orphaned/.
    _populate(tmp_path)
    cleanup_output_root(tmp_path)
    # Create a new leak with the same name, run again — must not clobber.
    (tmp_path / "leaked.html").write_text("<html>v2</html>")
    summary = cleanup_output_root(tmp_path)
    assert summary["count"] == 1
    dst_name = Path(summary["moved"][0]["dst"]).name
    # Second move got a .<timestamp> suffix.
    assert dst_name != "leaked.html"
    assert dst_name.startswith("leaked.html.")
