"""
Shared helpers for roast characterization tests.

All assertions about the raw BIDS dataset go through `raw_tree` /
`raw_file`, so that only this module changes if the raw dataset moves
from `<DS>/rawdata/` (+ top-level files at `<DS>/`) to `<DS>/rawbids/`
or to `<DS>/` itself.
"""
import gzip
import io
import tarfile
from pathlib import Path

import nibabel as nib
import numpy as np

_RAW_DIRS = ("rawdata", "rawbids")
_EXCLUDED = ("derivatives", "sourcedata")


def raw_tree(ds_root: Path) -> set[str]:
    """Files of the raw BIDS dataset, relative to the raw dataset root."""
    ds_root = Path(ds_root)
    out = set()
    for p in ds_root.rglob("*"):
        if not p.is_file():
            continue
        parts = p.relative_to(ds_root).parts
        if parts[0] in _RAW_DIRS:
            parts = parts[1:]
        if not parts or parts[0] in _EXCLUDED or parts[0] in _RAW_DIRS:
            continue
        out.add("/".join(parts))
    return out


def raw_file(ds_root: Path, rel: str) -> Path:
    """Path to a file of the raw BIDS dataset, given its relative name."""
    ds_root = Path(ds_root)
    for base in (ds_root / d for d in _RAW_DIRS):
        if (base / rel).exists():
            return base / rel
    return ds_root / rel


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
