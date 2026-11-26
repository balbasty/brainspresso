"""Tests for `scripts/fetch_nii_headers.py` (S3 listing, no network)."""
import importlib.util
import io
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location(
    "fetch_nii_headers", ROOT / "scripts" / "fetch_nii_headers.py")
fnh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fnh)

P = fnh.PROJECTS


def _pages(monkeypatch, pages):
    """Fake S3 listing: `pages` of (escaped) keys, in order."""
    markers = []

    def urlopen(url):
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        marker = q.get("marker", [""])[0]
        markers.append(marker)
        i = len(markers) - 1
        xml = "".join(f"<Key>{P}{k}</Key>" for k in pages[i])
        if i + 1 < len(pages):
            xml += "<IsTruncated>true</IsTruncated>"
        return io.BytesIO(xml.encode())

    monkeypatch.setattr(fnh.urllib.request, "urlopen", urlopen)
    return markers


def test_list_grep_while_paging(monkeypatch):
    pages = [[f"A/x{i}.txt" for i in range(3)],
             ["A/rest.nii.gz", "A/y.txt"],
             ["A/rest2.nii.gz"]]
    markers = _pages(monkeypatch, pages)
    # matches beyond the first `limit` keys are found
    assert fnh.list_keys("A/", grep=r"rest", limit=2) == [
        "A/rest.nii.gz", "A/rest2.nii.gz"]
    assert len(markers) == 3
    # the limit counts matches only
    markers.clear()
    assert fnh.list_keys("A/", grep=r"rest", limit=1) == ["A/rest.nii.gz"]
    assert len(markers) == 2


def test_list_unescapes_keys(monkeypatch):
    markers = _pages(monkeypatch, [["A/a&amp;b.nii.gz"], ["A/c.nii.gz"]])
    assert fnh.list_keys("A/") == ["A/a&b.nii.gz", "A/c.nii.gz"]
    assert markers == ["", P + "A/a&b.nii.gz"]
