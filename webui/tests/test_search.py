"""TF-IDF search backend replacing the archived site's dead search CGI."""
import pytest

from webui.search import build_index, get_index, query, drop_index, INDEX_NAME


def _mk(snap, files):
    snap.mkdir(parents=True, exist_ok=True)
    for rel, html in files.items():
        p = snap / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(html, encoding="utf-8")


def test_build_index_counts_docs(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {
        "a.html": "<html><title>Presario</title><body>Compaq Presario 2000 desktop.</body></html>",
        "b.html": "<html><title>ProLiant</title><body>Server rack for enterprise.</body></html>",
        "nope.txt": "ignored",
    })
    idx = build_index(snap)
    assert idx["n_docs"] == 2
    rels = sorted(d["rel"] for d in idx["docs"])
    assert rels == ["a.html", "b.html"]


def test_query_title_heavier_than_body(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {
        "title-match.html": "<html><title>Presario notebook</title><body>A laptop.</body></html>",
        "body-match.html": "<html><title>Catalog</title><body>Presario is one of many.</body></html>",
    })
    idx = build_index(snap)
    hits = query(idx, "presario")
    assert len(hits) == 2
    assert hits[0]["rel"] == "title-match.html"


def test_query_multi_token(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {
        "a.html": "<html><title>Rack</title><body>ProLiant rack server.</body></html>",
        "b.html": "<html><title>Desktop</title><body>Presario home PC.</body></html>",
    })
    idx = build_index(snap)
    hits = query(idx, "ProLiant server")
    assert hits[0]["rel"] == "a.html"


def test_query_stopwords_ignored(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {"a.html": "<html><title>x</title><body>the and of is</body></html>"})
    idx = build_index(snap)
    # "the and of is" are all stopwords → no hits.
    assert query(idx, "the and of is") == []


def test_query_no_match(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {"a.html": "<html><title>x</title><body>foo bar</body></html>"})
    idx = build_index(snap)
    assert query(idx, "nothingmatches") == []


def test_query_empty(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {"a.html": "<html><title>x</title><body>y</body></html>"})
    idx = build_index(snap)
    assert query(idx, "") == []
    assert query(idx, "   ") == []


def test_get_index_caches_and_invalidates(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {"a.html": "<html><title>orig</title><body>orig text</body></html>"})
    idx1 = get_index(snap)
    assert (snap / INDEX_NAME).is_file()
    assert any(d["title"] == "orig" for d in idx1["docs"])

    # Rewrite the file and bump its mtime → cache should invalidate.
    import os, time
    (snap / "a.html").write_text(
        "<html><title>updated</title><body>updated text</body></html>",
        encoding="utf-8",
    )
    future = time.time() + 2
    os.utime(snap / "a.html", (future, future))

    idx2 = get_index(snap)
    assert any(d["title"] == "updated" for d in idx2["docs"])


def test_drop_index(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {"a.html": "<html>x</html>"})
    get_index(snap)
    assert (snap / INDEX_NAME).is_file()
    drop_index(snap)
    assert not (snap / INDEX_NAME).exists()


def test_strips_script_and_style(tmp_path):
    snap = tmp_path / "snap"
    _mk(snap, {
        "a.html": (
            "<html><title>real</title>"
            "<script>presario_should_not_match</script>"
            "<style>notebook_also_not_matching {}</style>"
            "<body>real content</body></html>"
        ),
    })
    idx = build_index(snap)
    hits = query(idx, "presario")
    assert hits == []   # script body excluded
    hits = query(idx, "notebook")
    assert hits == []   # style body excluded
    hits = query(idx, "real")
    assert len(hits) == 1
