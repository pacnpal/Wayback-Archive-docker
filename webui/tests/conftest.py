from pathlib import Path
import pytest


@pytest.fixture
def make_snapshot(tmp_path: Path):
    """Return a factory that materializes files inside a fresh snapshot dir.
    Usage:
        snap = make_snapshot({"index.html": "<html>…</html>",
                              "css/a.css": ".x{}", "img/a.png": b"GIF89a..."})
    """
    def _make(files: dict) -> Path:
        root = tmp_path / "snap"
        root.mkdir()
        for rel, content in files.items():
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, bytes):
                p.write_bytes(content)
            else:
                p.write_text(content, encoding="utf-8")
        return root
    return _make
