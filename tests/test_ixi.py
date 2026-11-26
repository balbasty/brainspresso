"""
IXI: harvest from NITRC-IR (fake XNAT server modelled on the real
listing) and roast, from folders (NITRC) and from tarballs (original
release).
"""
import asyncio
import gzip
import io
import socket
import tarfile
import threading
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest
from aiohttp import web

from brainspresso.bids.layout import Layout
from brainspresso.download import DownloadManager
from brainspresso.datasets.IXI import roaster
from roast_utils import LAYOUTS

# Templates of the installed package (CI tests the installed package)
TEMPLATES = roaster.TEMPLATES


def nifti(shape, value) -> bytes:
    img = nib.Nifti1Image(np.full(shape, value, dtype=np.int16), np.eye(4))
    return gzip.compress(img.to_bytes())


# experiment -> scan -> collection -> {name: bytes}
SUBJECTS = {
    ("NITRC_IR_E10452", "2_Guys"): {
        "T1": {"NIfTI": {"IXI002-Guys-0828-T1.nii.gz": nifti((4, 4, 4), 1)}},
        "DTI": {
            "NIfTI": {
                f"IXI002-Guys-0828-DTI-{i:02d}.nii.gz": nifti((4, 4, 4), i)
                for i in range(3)
            },
            "SNAPSHOTS": {"qc.gif": b"GIF89a"},
            "bval": {"bvals.txt": b"0 1000 1000\n"},
            "bvec": {"bvecs.txt": b"0 1 0\n0 0 1\n0 0 0\n"},
        },
    },
    ("NITRC_IR_E10453", "12_HH"): {
        "T1": {"NIfTI": {"IXI012-HH-1211-T1.nii.gz": nifti((4, 4, 4), 2)}},
        "T2": {"NIfTI": {"IXI012-HH-1211-T2.nii.gz": nifti((4, 4, 4), 3)}},
    },
}


@pytest.fixture(scope="module")
def xnat():
    requests = {"files": 0, "auth": 0}
    files = {}       # URI -> bytes
    listings = {}    # experiment ID -> files listing
    for (eid, label), scans in SUBJECTS.items():
        rows, cat = [], 77000
        for scan, collections in scans.items():
            for collection, content in collections.items():
                cat += 1
                for name, data in content.items():
                    uri = (f"/data/experiments/{eid}/scans/{scan}/"
                           f"resources/{cat}/files/{name}")
                    files[uri] = data
                    rows.append({
                        "Size": str(len(data)), "collection": collection,
                        "URI": uri, "Name": name, "digest": "",
                    })
        listings[eid] = rows

    async def experiments(request):
        if request.headers.get("Authorization"):
            requests["auth"] += 1
        return web.json_response({"ResultSet": {"Result": [
            {"ID": eid, "label": label, "date": "2005-06-01",
             "URI": f"/data/experiments/{eid}"}
            for eid, label in SUBJECTS
        ]}})

    async def file_listing(request):
        requests["files"] += 1
        eid = request.match_info["eid"]
        return web.json_response({"ResultSet": {"Result": listings[eid]}})

    async def get_file(request):
        uri = "/data/" + request.match_info["path"]
        if uri not in files:
            raise web.HTTPNotFound()
        return web.Response(body=files[uri])

    app = web.Application()
    app.router.add_get("/ir/data/projects/ixi/experiments", experiments)
    app.router.add_get(
        "/ir/data/experiments/{eid}/scans/ALL/files", file_listing
    )
    app.router.add_get("/ir/data/{path:.+}", get_file)

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    loop = asyncio.new_event_loop()
    runner = web.AppRunner(app)
    started = threading.Event()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner.setup())
        loop.run_until_complete(web.SockSite(runner, sock).start())
        started.set()
        loop.run_forever()

    threading.Thread(target=run, daemon=True).start()
    started.wait(10)
    yield f"http://127.0.0.1:{port}/ir", requests
    loop.call_soon_threadsafe(loop.stop)


def harvest(server, src, **kwargs):
    from brainspresso.datasets.IXI.download import nitrc_downloaders
    kwargs.setdefault("keys", {"T1w", "T2w", "PDw", "angio", "dwi"})
    DownloadManager(
        nitrc_downloaders(src, server=server, **kwargs),
        on_error="raise",
    ).run()


def files(root: Path) -> set[str]:
    return {
        p.relative_to(root).as_posix()
        for p in root.rglob("*") if p.is_file()
    }


def test_harvest_nitrc(xnat, tmp_path):
    server, requests = xnat
    src = tmp_path / "IXI" / "sourcedata"
    harvest(server, src)
    assert files(src) == {
        "nitrc_experiments.json",
        "IXI-T1/IXI002-Guys-0828-T1.nii.gz",
        "IXI-T1/IXI012-HH-1211-T1.nii.gz",
        "IXI-T2/IXI012-HH-1211-T2.nii.gz",
        "IXI-DTI/IXI002-Guys-0828-DTI-00.nii.gz",
        "IXI-DTI/IXI002-Guys-0828-DTI-01.nii.gz",
        "IXI-DTI/IXI002-Guys-0828-DTI-02.nii.gz",
        "IXI-DTI/IXI002-Guys-0828-bvals.txt",
        "IXI-DTI/IXI002-Guys-0828-bvecs.txt",
    }
    data = SUBJECTS[("NITRC_IR_E10452", "2_Guys")]
    assert (src / "IXI-T1/IXI002-Guys-0828-T1.nii.gz").read_bytes() == \
        data["T1"]["NIfTI"]["IXI002-Guys-0828-T1.nii.gz"]
    assert requests["files"] == 2       # one listing per experiment


def test_harvest_nitrc_filters(xnat, tmp_path):
    server, requests = xnat
    src = tmp_path / "IXI" / "sourcedata"
    before = requests["files"]
    harvest(server, src, keys={"T1w"}, subs={12})
    assert files(src) == {
        "nitrc_experiments.json", "IXI-T1/IXI012-HH-1211-T1.nii.gz",
    }
    # experiments are filtered before listing their files
    assert requests["files"] == before + 1


def check_roast(root: Path, bval_per_subject: bool, layout: str):
    found = Layout.detect(root)
    assert found.kind == layout
    raw = found.raw
    out = {p for p in files(raw)
           if not p.startswith(("sourcedata/", ".brainspresso/"))}
    assert {
        "sub-002/anat/sub-002_T1w.nii.gz",
        "sub-002/anat/sub-002_T1w.json",
        "sub-012/anat/sub-012_T1w.nii.gz",
        "sub-012/anat/sub-012_T2w.nii.gz",
        "sub-002/dwi/sub-002_dwi.nii.gz",
        "sub-002/dwi/sub-002_dwi.json",
    } <= out
    dwi = nib.load(raw / "sub-002/dwi/sub-002_dwi.nii.gz")
    assert dwi.shape == (4, 4, 4, 3)
    assert [int(dwi.dataobj[0, 0, 0, i]) for i in range(3)] == [0, 1, 2]
    if bval_per_subject:
        assert "sub-002/dwi/sub-002_dwi.bval" in out
        assert "dwi.bval" not in out
    else:
        assert {"dwi.bval", "dwi.bvec"} <= out


@pytest.mark.parametrize("layout", LAYOUTS)
def test_roast_from_nitrc_folders(xnat, tmp_path, monkeypatch, layout):
    server, _ = xnat
    root = tmp_path / "IXI"
    harvest(server, root / "sourcedata")

    # Without bundled bvals/bvecs: per-subject files from NITRC are used
    empty = tmp_path / "empty_templates"
    empty.mkdir()
    for sub in ("Guys", "HH", "IOP"):
        (empty / sub).symlink_to(TEMPLATES / sub)
    monkeypatch.setattr(roaster, "TEMPLATES", empty)
    roaster.roast(root, keys={"T1w", "T2w", "dwi"}, layout=layout)
    check_roast(root, bval_per_subject=True, layout=layout)


@pytest.mark.parametrize("layout", LAYOUTS)
def test_roast_bundled_metadata(xnat, tmp_path, layout):
    server, _ = xnat
    root = tmp_path / "IXI"
    harvest(server, root / "sourcedata")
    roaster.roast(root, keys={"meta", "T1w", "T2w", "dwi"}, layout=layout)
    # Global bvals/bvecs and IXI.xls come from the package templates
    check_roast(root, bval_per_subject=False, layout=layout)
    raw = Layout.detect(root).raw
    assert (raw / "dwi.bval").read_bytes() == \
        (TEMPLATES / "bvals.txt").read_bytes()
    lines = (raw / "participants.tsv").read_text().splitlines()
    assert lines[0].startswith("participant_id\tsite\tage\tsex")
    ids = {line.split("\t")[0] for line in lines[1:]}
    assert ids == {"sub-002", "sub-012"}


@pytest.mark.parametrize("layout", LAYOUTS)
def test_roast_from_tarballs(xnat, tmp_path, layout):
    """The original tarballs are still supported"""
    root = tmp_path / "IXI"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    members = {}
    for (_, _), scans in SUBJECTS.items():
        for scan, collections in scans.items():
            for name, data in collections.get("NIfTI", {}).items():
                members.setdefault(scan, {})[name] = data
    for scan, content in members.items():
        with tarfile.open(src / f"IXI-{scan}.tar", "w") as tar:
            for name, data in content.items():
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
    roaster.roast(root, keys={"T1w", "T2w", "dwi"}, layout=layout)
    check_roast(root, bval_per_subject=False, layout=layout)


def test_roast_shards_in_processes(tmp_path):
    """One shard per modality, run in a process pool"""
    root = tmp_path / "IXI"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    for (_, _), scans in SUBJECTS.items():
        for scan, collections in scans.items():
            folder = src / f"IXI-{scan}"
            folder.mkdir(exist_ok=True)
            for name, data in collections.get("NIfTI", {}).items():
                (folder / name).write_bytes(data)
    report = roaster.roast(root, keys={"T1w", "T2w", "dwi"}, json="no",
                           exclude_subs=[12], jobs=2, display=False)
    raw = Layout.detect(root).raw
    out = {p for p in files(raw)
           if not p.startswith(("sourcedata/", ".brainspresso/"))}
    assert out == {
        "dwi.bval", "dwi.bvec", "sub-002/anat/sub-002_T1w.nii.gz",
        "sub-002/dwi/sub-002_dwi.nii.gz",
    }
    assert report.counts["dwi"]["done"] == 3
    assert not report.errors
