"""The shim and the viewer must compute the same hash for the same query —
otherwise the crawler writes to one filename and the viewer looks for another.
"""
from webui.query_hash import suffix_for_query
from webui import wayback_resume_shim


def test_shim_and_viewer_agree():
    for q in ("v=1", "foo=bar&baz=qux", "🤖=1", ""):
        assert suffix_for_query(q) == wayback_resume_shim._suffix_for_query(q)


def test_suffix_format():
    s = suffix_for_query("v=1")
    assert s.startswith(".q-")
    assert len(s) == 3 + 8  # .q- + 8-char hash


def test_empty_suffix():
    assert suffix_for_query("") == ""


def test_different_queries_differ():
    assert suffix_for_query("a=1") != suffix_for_query("a=2")


def test_stable():
    assert suffix_for_query("v=1") == suffix_for_query("v=1")
