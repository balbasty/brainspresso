"""
Shared helpers for roast characterization tests.

All assertions about the raw BIDS dataset go through `raw_tree` /
`raw_file`, which resolve the raw dataset with `Layout.detect`
(`<DS>/rawbids/` for a study, `<DS>/` for a flat dataset), so that the
same assertions hold in both layouts. Fixtures parametrized with
`LAYOUTS` run each test under both.
"""
import gzip
import io
import tarfile
from pathlib import Path

import nibabel as nib
import numpy as np

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import STATE_DIR, Layout, make_layout

LAYOUTS = ("study", "flat")
_EXCLUDED = ("derivatives", "sourcedata", STATE_DIR)


def init_layout(ds_root: Path, kind: str) -> Layout:
    """Record a layout in the manifest, as `harvest --layout` does"""
    layout = make_layout(kind, ds_root)
    record_run(layout, "harvest", {"test": True})
    return layout


def layout_of(ds_root: Path) -> Layout:
    """Layout of a roasted dataset (it must be detectable)"""
    layout = Layout.detect(ds_root)
    assert layout is not None, f"no layout detected in {ds_root}"
    return layout


def raw_tree(ds_root: Path) -> set[str]:
    """Files of the raw BIDS dataset, relative to the raw dataset root."""
    layout = Layout.detect(ds_root)
    if layout is None or not layout.raw.is_dir():
        return set()
    raw = layout.raw
    out = set()
    for p in raw.rglob("*"):
        if not p.is_file():
            continue
        parts = p.relative_to(raw).parts
        if parts[0] in _EXCLUDED:
            continue
        out.add("/".join(parts))
    return out


def raw_file(ds_root: Path, rel: str) -> Path:
    """Path to a file of the raw BIDS dataset, given its relative name."""
    return layout_of(ds_root).raw / rel


def nii_bytes(shape=(2, 2, 2), gz=True, value=1) -> bytes:
    img = nib.Nifti1Image(
        np.full(shape, value, dtype=np.int16), np.diag([2., 2., 2., 1.])
    )
    data = img.to_bytes()
    return gzip.compress(data) if gz else data


def analyze_pair(shape=(2, 2, 2), data=None):
    """Return (hdr_bytes, img_bytes) of a tiny Analyze image."""
    if data is None:
        data = np.arange(np.prod(shape), dtype=np.int16).reshape(shape)
    img = nib.AnalyzeImage(data, np.eye(4))
    hdr = io.BytesIO()
    imgb = io.BytesIO()
    img.to_file_map({
        "header": nib.FileHolder(fileobj=hdr),
        "image": nib.FileHolder(fileobj=imgb),
    })
    return hdr.getvalue(), imgb.getvalue()


def make_tar(path: Path, members: dict, mode="w:gz", dirs=()):
    """Write a tarball. `members` maps member name -> bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, mode) as tar:
        for d in dirs:
            info = tarfile.TarInfo(d)
            info.type = tarfile.DIRTYPE
            tar.addfile(info)
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return path


def read_tsv(path: Path) -> list[list[str]]:
    return [
        line.split("\t")
        for line in Path(path).read_text().splitlines() if line
    ]
