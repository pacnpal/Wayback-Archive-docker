"""Tests for outage-aware job deferral in webui.jobs."""
from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


@pytest.fixture
def jobs_db(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    # Re-import so OUTPUT_ROOT picks up the env var.
    import importlib
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    return j


def _insert_pending(j, **overrides):
    defaults = dict(
        target_url="https://example.com/",
        timestamp="20200101000000",
        wayback_url="https://web.archive.org/web/20200101000000/https://example.com/",
        host="example.com",
        site_dir=str(j.OUTPUT_ROOT / "example.com" / "20200101000000"),
        log_path=str(j.OUTPUT_ROOT / "example.com" / "20200101000000" / ".log"),
        flags_json="{}",
        status="pending",
        created_at=j.now_iso(),
    )
    defaults.update(overrides)
    cols = ",".join(defaults.keys())
    binds = ",".join(f":{k}" for k in defaults.keys())
    with j.connect() as c:
        cur = c.execute(f"INSERT INTO jobs ({cols}) VALUES ({binds})", defaults)
        return cur.lastrowid


def test_migration_adds_not_before_and_attempts(jobs_db):
    with jobs_db.connect() as c:
        cols = {r["name"] for r in c.execute("PRAGMA table_info(jobs)").fetchall()}
    assert "not_before" in cols
    assert "attempts" in cols


def test_pick_ready_pending_prioritizes_repair_over_archive(jobs_db):
    archive1 = _insert_pending(jobs_db)
    repair1 = _insert_pending(jobs_db, repair_paths_json='["a.html"]')
    archive2 = _insert_pending(jobs_db)
    repair2 = _insert_pending(jobs_db, repair_paths_json='["b.html"]')
    ready = jobs_db.pick_ready_pending(limit=10)
    ids = [r["id"] for r in ready]
    # Repairs drain first (in id order); archives follow (in id order).
    assert ids == [repair1, repair2, archive1, archive2]


def test_pick_ready_pending_repair_priority_still_respects_not_before(jobs_db):
    """A deferred repair job (future not_before) must NOT jump the queue —
    the not_before filter applies before the repair-priority ordering."""
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(hours=6)).replace(microsecond=0).isoformat()
    deferred_repair = _insert_pending(jobs_db, repair_paths_json='["a.html"]',
                                      not_before=future)
    archive = _insert_pending(jobs_db)
    ready = jobs_db.pick_ready_pending(limit=10)
    ids = [r["id"] for r in ready]
    assert deferred_repair not in ids
    assert ids == [archive]


def test_pick_pending_skips_future_not_before(jobs_db):
    future = (datetime.now(timezone.utc) + timedelta(hours=6)).replace(microsecond=0).isoformat()
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(microsecond=0).isoformat()
    jid_future = _insert_pending(jobs_db, not_before=future)
    jid_past = _insert_pending(jobs_db, not_before=past)
    jid_null = _insert_pending(jobs_db)
    ready = jobs_db.pick_ready_pending(limit=10)
    ids = [r["id"] for r in ready]
    assert jid_future not in ids
    assert jid_past in ids
    assert jid_null in ids


def test_defer_for_outage_sets_not_before_and_increments_attempts(jobs_db):
    jid = _insert_pending(jobs_db, status="running", started_at=jobs_db.now_iso())
    before = datetime.now(timezone.utc)
    jobs_db.defer_for_outage(jid, now=before)
    with jobs_db.connect() as c:
        row = c.execute("SELECT status, attempts, not_before FROM jobs WHERE id=?", (jid,)).fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    nb = datetime.fromisoformat(row["not_before"])
    # First retry is 5 minutes out.
    assert nb > before + timedelta(minutes=4, seconds=30)
    assert nb < before + timedelta(minutes=5, seconds=30)


def test_defer_for_outage_backs_off_on_repeat(jobs_db):
    jid = _insert_pending(jobs_db, status="running", attempts=1)
    before = datetime.now(timezone.utc)
    jobs_db.defer_for_outage(jid, now=before)
    with jobs_db.connect() as c:
        row = c.execute("SELECT attempts, not_before FROM jobs WHERE id=?", (jid,)).fetchone()
    assert row["attempts"] == 2
    nb = datetime.fromisoformat(row["not_before"])
    # attempts was 1 → second retry → 10 minutes
    assert nb > before + timedelta(minutes=9, seconds=30)
    assert nb < before + timedelta(minutes=10, seconds=30)


def test_release_deferred_clears_not_before(jobs_db):
    future = (datetime.now(timezone.utc) + timedelta(hours=6)).replace(microsecond=0).isoformat()
    jid_deferred = _insert_pending(jobs_db, not_before=future)
    jid_normal = _insert_pending(jobs_db)
    n = jobs_db.release_deferred()
    assert n == 1
    with jobs_db.connect() as c:
        deferred_row = c.execute("SELECT not_before FROM jobs WHERE id=?", (jid_deferred,)).fetchone()
        normal_row = c.execute("SELECT not_before FROM jobs WHERE id=?", (jid_normal,)).fetchone()
    assert deferred_row["not_before"] is None
    assert normal_row["not_before"] is None


def test_earliest_deferred_not_before_returns_future_only(jobs_db):
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(microsecond=0).isoformat()
    _insert_pending(jobs_db, not_before=past)
    _insert_pending(jobs_db, not_before=future)
    assert jobs_db.earliest_deferred_not_before() == future


def test_earliest_deferred_not_before_none_when_all_past(jobs_db):
    """Past-due deferrals are filtered out so the banner doesn't render
    a misleading 'any moment now' ETA during a sustained outage."""
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    _insert_pending(jobs_db, not_before=past)
    assert jobs_db.earliest_deferred_not_before() is None


def test_release_deferred_only_touches_pending_jobs(jobs_db):
    future = (datetime.now(timezone.utc) + timedelta(hours=6)).replace(microsecond=0).isoformat()
    jid_done = _insert_pending(jobs_db, status="error", not_before=future,
                               finished_at=jobs_db.now_iso())
    jobs_db.release_deferred()
    with jobs_db.connect() as c:
        row = c.execute("SELECT status, not_before FROM jobs WHERE id=?", (jid_done,)).fetchone()
    assert row["status"] == "error"
    assert row["not_before"] == future
