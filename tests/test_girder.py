"""
Girder backend and the Girder-backed datasets (UNC-Wisconsin, Bullitt),
against a fake Girder server modelled on data.kitware.com.
"""
import asyncio
import contextlib
import hashlib
import io
import itertools
import json
import socket
import threading
import zlib
from pathlib import Path, PurePosixPath

import numpy as np
import pytest
from aiohttp import web
from cli_utils import Harvest

from brainspresso.bids.layout import Layout
from brainspresso.download import DownloadManager
from brainspresso.sources import girder as girder_mod
from brainspresso.sources.base import mirror
from brainspresso.sources.girder import Girder, parse_girder_url

nrrd = pytest.importorskip("nrrd")

LAYOUTS = ("study", "flat")

_ids = (f"{i:024x}" for i in itertools.count(1))


def nrrd_bytes(data, dwi=False) -> bytes:
    header = {
        "space": "left-posterior-superior",
        "encoding": "gzip",
        "space origin": np.zeros(3),
    }
    if dwi:
        header["space directions"] = np.asarray(
            [[1, 0, 0], [0, 1, 0], [0, 0, 2], [np.nan] * 3], float
        )
        header["kinds"] = ["space", "space", "space", "list"]
        header["modality"] = "DWMRI"
        header["DWMRI_b-value"] = "1000"
        for i, g in enumerate([(0, 0, 0), (1, 0, 0), (0, 1, 0)]):
            header[f"DWMRI_gradient_{i:04d}"] = " ".join(map(str, g))
    else:
        header["space directions"] = np.diag([1.0, 1.0, 2.0])
    buf = io.BytesIO()
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        nrrd.write(f"{d}/x.nrrd", data, header)
        buf.write(Path(f"{d}/x.nrrd").read_bytes())
    return buf.getvalue()


T1 = nrrd_bytes(np.full((4, 4, 3), 1, np.int16))
T2 = nrrd_bytes(np.full((4, 4, 3), 2, np.int16))
DWI = nrrd_bytes(np.ones((4, 4, 2, 3), np.int16), dwi=True)


def scan(sub, age, weight, dwi=True):
    folder = {
        "meta": {"folder_type": "scan", "scan_age": f"{age}months",
                 "scan_date": "2011-06-01", "scan_weight_kg": weight,
                 "subject_id": sub},
        "folders": {
            "sMRI": {"folders": {
                "ORIG": {"items": {
                    f"{sub}_{age}months_T1_ORIG.nrrd": T1,
                    f"{sub}_{age}months_T2_ORIG.nrrd": T2,
                }},
                "Reg2Atlas": {"items": {
                    f"{sub}_{age}months_T1_Reg2Atlas.nrrd": T1,
                }},
            }},
            "DTI": {"folders": {
                "ORIG": {"items": {}},
                "AUTOQC": {"items": {
                    f"{sub}_{age}months_AutoQCed_DTI_FA.nrrd": T1,
                }},
            }},
        },
    }
    if dwi:
        # item named *.nrrd holding a differently-named file (as on Kitware)
        folder["folders"]["DTI"]["folders"]["ORIG"]["items"][
            f"{sub}_{age}months_DWI_ORIG.nrrd"
        ] = {f"{sub}_{age}months_dwi.nrrd": DWI}
    return folder


TREE = {   # scan_data
    "folders": {
        "001": {
            "meta": {"folder_type": "subject", "sex": "F",
                     "dob": "2010-06-06", "subject_id": "001"},
            "folders": {"12months": scan("001", 12, 1.9),
                        "16months": scan("001", 16, 2.6)},
        },
        "002": {
            "meta": {"folder_type": "subject", "sex": "M",
                     "dob": "2010-07-01", "subject_id": "002"},
            "folders": {"12months": scan("002", 12, 2.1, dwi=False)},
        },
    },
    "items": {"README.txt": b"top-level item, ignored by keep"},
}


class FakeGirder:
    def __init__(self, tree):
        self.folders = {}   # id -> dict(name, parentId, meta)
        self.items = {}     # id -> dict(name, folderId)
        self.files = {}     # id -> dict(name, itemId, data)
        self.requests = 0
        self.root = self._add_folder("scan_data", "collection-id", tree, {})

    def _add_folder(self, name, parent, node, meta):
        fid = next(_ids)
        self.folders[fid] = {"_id": fid, "name": name, "parentId": parent,
                             "meta": meta}
        for sub, child in node.get("folders", {}).items():
            self._add_folder(sub, fid, child, child.get("meta", {}))
        for item, content in node.get("items", {}).items():
            iid = next(_ids)
            self.items[iid] = {"_id": iid, "name": item, "folderId": fid,
                               "meta": {}}
            if isinstance(content, bytes):
                content = {item: content}
            for fname, data in content.items():
                xid = next(_ids)
                self.files[xid] = {
                    "_id": xid, "name": fname, "itemId": iid,
                    "size": len(data), "data": data,
                    "sha512": hashlib.sha512(data).hexdigest(),
                }
        return fid

    def page(self, request, rows):
        limit = int(request.query.get("limit", 50))
        offset = int(request.query.get("offset", 0))
        return web.json_response(rows[offset:offset + limit])

    def app(self):
        async def folder_list(request):
            self.requests += 1
            pid = request.query["parentId"]
            rows = [f for f in self.folders.values() if f["parentId"] == pid]
            return self.page(request, rows)

        async def folder_get(request):
            return web.json_response(self.folders[request.match_info["id"]])

        async def item_list(request):
            self.requests += 1
            fid = request.query["folderId"]
            rows = [i for i in self.items.values() if i["folderId"] == fid]
            return self.page(request, rows)

        async def item_files(request):
            self.requests += 1
            iid = request.match_info["id"]
            rows = [{k: v for k, v in f.items() if k != "data"}
                    for f in self.files.values() if f["itemId"] == iid]
            return self.page(request, rows)

        async def download(request):
            f = self.files[request.match_info["id"]]
            return web.Response(body=f["data"])

        app = web.Application()
        app.router.add_get("/api/v1/folder", folder_list)
        app.router.add_get("/api/v1/folder/{id}", folder_get)
        app.router.add_get("/api/v1/item", item_list)
        app.router.add_get("/api/v1/item/{id}/files", item_files)
        app.router.add_get("/api/v1/file/{id}/download", download)
        return app


@contextlib.contextmanager
def serve(tree):
    fake = FakeGirder(tree)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    loop = asyncio.new_event_loop()
    runner = web.AppRunner(fake.app())
    started = threading.Event()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner.setup())
        loop.run_until_complete(web.SockSite(runner, sock).start())
        started.set()
        loop.run_forever()

    threading.Thread(target=run, daemon=True).start()
    started.wait(10)
    fake.api = f"http://127.0.0.1:{port}/api/v1"
    yield fake
    loop.call_soon_threadsafe(loop.stop)


@pytest.fixture(scope="module")
def server():
    with serve(TREE) as fake:
        yield fake


def files(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix()
            for p in root.rglob("*") if p.is_file()}


# ----------------------------------------------------------------------
#   Girder client
# ----------------------------------------------------------------------

def test_parse_girder_url():
    assert parse_girder_url(
        "https://data.kitware.com/#collection/54b582c38d777f4362aa9cb3"
    ) == ("https://data.kitware.com/api/v1", "collection",
          "54b582c38d777f4362aa9cb3")
    assert parse_girder_url(
        "https://data.kitware.com/#collection/abc/folder/def"
    ) == ("https://data.kitware.com/api/v1", "folder", "def")
    with pytest.raises(ValueError):
        parse_girder_url("https://data.kitware.com/#item/abc")


def test_walk_paths_metadata_and_pagination(server, monkeypatch):
    monkeypatch.setattr(girder_mod, "PAGE", 2)     # force pagination
    folders = []
    listing = Girder(server.api).walk("folder", server.root, folders=folders)
    paths = {str(f.path) for f in listing}
    assert "README.txt" in paths
    # single-file item: <folders>/<item>
    assert "001/12months/sMRI/ORIG/001_12months_T1_ORIG.nrrd" in paths
    # item holding a differently-named file: <folders>/<item>/<file>
    assert ("001/12months/DTI/ORIG/001_12months_DWI_ORIG.nrrd/"
            "001_12months_dwi.nrrd") in paths
    assert len(paths) == len(server.files)
    assert all(f.digests["sha512"] and f.size for f in listing)
    meta = {str(f.path): f.meta for f in folders}
    assert meta["001"]["sex"] == "F"
    assert meta["002/12months"]["scan_weight_kg"] == 2.1


def test_walk_prunes_folders(server):
    before = server.requests
    listing = Girder(server.api).walk(
        "folder", server.root,
        keep_folder=lambda p: p.parts[0] == "002",
    )
    assert listing and all(
        f.relpath.parts[0] in ("002", "README.txt") for f in listing
    )
    pruned = server.requests - before
    total_before = server.requests
    Girder(server.api).walk("folder", server.root)
    assert pruned < server.requests - total_before


def test_mirror_verifies_sha512(server, tmp_path):
    g = Girder(server.api)
    dls = list(mirror(g.walk("folder", server.root), tmp_path,
                        keep=lambda f: f.name == "README.txt"))
    assert len(dls) == 1 and dls[0].digests
    # tamper with the expected checksum: the download is rejected
    dls[0].digests = {"sha512": "0" * 128}
    statuses = []

    async def run():
        from brainspresso.download import new_session
        async with new_session() as s:
            dls[0].session = s
            async for st in dls[0]:
                statuses.append(st)

    asyncio.run(run())
    assert statuses[-1]["status"] == "error"
    assert statuses[-1]["checksum"] == "differs"
    assert not (tmp_path / "README.txt").exists()


# ----------------------------------------------------------------------
#   UNC-Wisconsin
# ----------------------------------------------------------------------

@pytest.fixture
def uncwisc(server, monkeypatch):
    from brainspresso.datasets.unc_wisconsin import harvest as dl
    monkeypatch.setattr(dl, "KITWARE", server.api)
    monkeypatch.setattr(dl, "SCAN_DATA", server.root)
    return Harvest("unc-wisconsin", dl)


def test_uncwisc_harvest(uncwisc, tmp_path):
    uncwisc.download(tmp_path, subs=["001"], sessions=["12months"])
    src = tmp_path / "UNC-Wisconsin" / "sourcedata"
    assert files(src) == {
        "folders.json",
        "scan_data/001/12months/sMRI/ORIG/001_12months_T1_ORIG.nrrd",
        "scan_data/001/12months/sMRI/ORIG/001_12months_T2_ORIG.nrrd",
        "scan_data/001/12months/DTI/ORIG/001_12months_DWI_ORIG.nrrd/"
        "001_12months_dwi.nrrd",
    }
    meta = json.loads((src / "folders.json").read_text())
    assert set(meta) == {"001", "001/12months"}


def test_uncwisc_harvest_keys(uncwisc, tmp_path):
    uncwisc.download(tmp_path, keys=["T1w", "dti-qc"], subs=["002"])
    src = tmp_path / "UNC-Wisconsin" / "sourcedata" / "scan_data"
    assert files(src) == {
        "002/12months/sMRI/ORIG/002_12months_T1_ORIG.nrrd",
        "002/12months/DTI/AUTOQC/002_12months_AutoQCed_DTI_FA.nrrd",
    }


@pytest.mark.parametrize("layout", LAYOUTS)
def test_uncwisc_roast(uncwisc, tmp_path, layout):
    import nibabel as nib

    from brainspresso.datasets.unc_wisconsin.roaster import roast
    uncwisc.download(tmp_path, layout=layout)   # raw + meta, all subjects
    root = tmp_path / "UNC-Wisconsin"
    roast(root)                                 # layout from the manifest
    found = Layout.detect(root)
    assert found.kind == layout
    raw = found.raw
    out = files(raw)
    for sub, ses in [("001", "12months"), ("001", "16months"),
                     ("002", "12months")]:
        base = f"sub-{sub}/ses-{ses}"
        assert f"{base}/anat/sub-{sub}_ses-{ses}_T1w.nii.gz" in out
        assert f"{base}/anat/sub-{sub}_ses-{ses}_T2w.nii.gz" in out
    dwi = "sub-001/ses-12months/dwi/sub-001_ses-12months_dwi"
    assert {f"{dwi}.nii.gz", f"{dwi}.bval", f"{dwi}.bvec"} <= out
    assert not any("sub-002/ses-12months/dwi" in p for p in out)
    assert nib.load(raw / f"{dwi}.nii.gz").shape == (4, 4, 2, 3)
    assert (raw / f"{dwi}.bval").read_text().split() == ["0", "1000", "1000"]
    t1 = nib.load(
        raw / "sub-002/ses-12months/anat/"
        "sub-002_ses-12months_T1w.nii.gz"
    )
    assert nib.aff2axcodes(t1.affine) == ("L", "P", "S")

    assert (raw / "participants.tsv").read_text().splitlines() == [
        "participant_id\tspecies\tsex\tbirth_date",
        "sub-001\tMacaca mulatta\tF\t2010-06-06",
        "sub-002\tMacaca mulatta\tM\t2010-07-01",
    ]
    assert (
        raw / "sub-001/sub-001_sessions.tsv"
    ).read_text().splitlines() == [
        "session_id\tacq_time\tage\tweight",
        "ses-12months\t2011-06-01\t12\t1.9",
        "ses-16months\t2011-06-01\t16\t2.6",
    ]
    for name in ("README", "dataset_description.json",
                 "participants.json", "sessions.json"):
        assert name in out
    assert json.loads(
        (raw / "dataset_description.json").read_text()
    )["DatasetType"] == "raw"
    if layout == "study":
        study = json.loads((root / "dataset_description.json").read_text())
        assert study["DatasetType"] == "study"


def test_download_manager_integration(server, tmp_path):
    # mirror(listing) yields Downloaders that the manager runs concurrently
    g = Girder(server.api)
    DownloadManager(
        mirror(g.walk("folder", server.root), tmp_path,
                 keep=lambda f: f.relpath.parts[0] == "002"),
        jobs=3, on_error="raise",
    ).run()
    assert all(
        PurePosixPath(p).parts[0] == "002" for p in files(tmp_path)
    )
    assert len(files(tmp_path)) == sum(
        1 for f in server.files.values()
        for i in [server.items[f["itemId"]]]
        if "002" in _folder_path(server, i["folderId"])
    )


def _folder_path(server, fid):
    parts = []
    while fid in server.folders and fid != server.root:
        parts.append(server.folders[fid]["name"])
        fid = server.folders[fid]["parentId"]
    return "/".join(reversed(parts))


@pytest.mark.parametrize("layout", LAYOUTS)
def test_uncwisc_roast_derivatives(uncwisc, tmp_path, layout):
    from brainspresso.datasets.unc_wisconsin.roaster import (
        SPACE,
        _age_months,
        roast,
    )
    uncwisc.download(tmp_path, keys=["derivatives", "meta"], subs=["001"],
                     layout=layout)
    root = tmp_path / "UNC-Wisconsin"
    roast(root)
    found = Layout.detect(root)
    drv = found.derivative("unc-wisconsin").root
    out = files(drv)
    assert {"README", "dataset_description.json"} <= out
    for ses in ("12months", "16months"):
        base = f"sub-001/ses-{ses}"
        assert (
            f"{base}/anat/sub-001_ses-{ses}_space-{SPACE}_T1w.nii.gz" in out
        )
        assert (
            f"{base}/dwi/sub-001_ses-{ses}_"
            f"model-tensor_param-fa_desc-autoqc_dwimap.nii.gz" in out
        )
        # required derivative metadata
        sidecar = json.loads((drv / (
            f"{base}/anat/sub-001_ses-{ses}_space-{SPACE}_T1w.json"
        )).read_text())
        assert sidecar["SkullStripped"] is False
        assert "SpatialReference" in sidecar
    assert ".bidsignore" in out
    # raw data was not harvested
    assert not (found.raw / "sub-001" / "ses-12months").exists()
    description = json.loads((drv / "dataset_description.json").read_text())
    assert description["DatasetType"] == "derivative"
    # the local raw dataset comes first, then the hosted one
    url = {"study": "../../rawbids/", "flat": "../../"}[layout]
    assert description["SourceDatasets"][0] == {"URL": url}
    assert (drv / url / "participants.tsv").exists()
    assert any("kitware" in s.get("URL", "")
               for s in description["SourceDatasets"][1:])

    assert _age_months("12months") == "12"
    assert _age_months("02weeks") == "0.46"
    assert _age_months("unknown") == "n/a"


# ----------------------------------------------------------------------
#   Bullitt
# ----------------------------------------------------------------------

def mha_bytes(value) -> bytes:
    data = np.full((4, 3, 2), value, np.int16)
    raw = zlib.compress(data.T.tobytes())
    head = (
        "ObjectType = Image\nNDims = 3\nBinaryData = True\n"
        "BinaryDataByteOrderMSB = False\nCompressedData = True\n"
        f"CompressedDataSize = {len(raw)}\n"
        "TransformMatrix = 1 0 0 0 1 0 0 0 1\nOffset = 0 0 0\n"
        "ElementSpacing = 1 1 1\nDimSize = 4 3 2\n"
        "ElementType = MET_SHORT\nElementDataFile = LOCAL\n"
    )
    return head.encode() + raw


def normal(n, t1="T1-Flash", mprage="T1-MPRage"):
    folders = {
        t1: {"items": {f"Normal{n}-T1-Flash.mha": mha_bytes(1)}},
        "T2": {"items": {f"Normal{n}-T2.mha": mha_bytes(2)}},
        "MRA": {"items": {f"Normal{n}-MRA.mha": mha_bytes(3)}},
        "DTI": {"items": {f"Normal{n}-DTI.mha": mha_bytes(4)}},
        "AuxillaryData": {"items": {
            "SkullStripped-T1-Flash.mha": mha_bytes(5),
            "VascularNetwork.tre": b"ObjectType = Scene\n",
            "Something-else.txt": b"not harvested",
        }},
    }
    if mprage:
        folders[mprage] = {"items": {f"Normal{n}-MPRage.mha": mha_bytes(6)}}
    return {"folders": folders}


BULLITT_TREE = {
    "folders": {
        "Normal-001": normal("001"),
        "Normal-002": normal("002", t1="T!-Flash", mprage=None),
        "Normal-003": normal("003", mprage="MPRage"),
        "Administration": {"items": {"NormalDatabase2.pdf": b"%PDF"}},
        "Phantoms": {"items": {"phantom.mha": mha_bytes(7)}},
    },
}


@pytest.fixture(scope="module")
def bullitt_server():
    with serve(BULLITT_TREE) as fake:
        yield fake


@pytest.fixture
def bullitt(bullitt_server, monkeypatch):
    from brainspresso.datasets.bullitt import harvest as dl
    monkeypatch.setattr(dl, "KITWARE", bullitt_server.api)
    monkeypatch.setattr(dl, "ROOT", bullitt_server.root)
    return Harvest("bullitt", dl)


def test_bullitt_subject_labels():
    from brainspresso.datasets.bullitt.harvest import subject_label
    from brainspresso.sources.base import subject_labels
    assert subject_label("Normal-042") == "042"
    assert subject_label("Administration") is None
    assert subject_labels(["1", "sub-2", "003"], 3) == {"001", "002", "003"}


def test_bullitt_harvest(bullitt, tmp_path):
    bullitt.download(tmp_path, subs=["1", "2"])
    src = tmp_path / "Bullitt" / "sourcedata"
    assert files(src) == {
        "Administration/NormalDatabase2.pdf",
        *(f"Normal-001/{x}" for x in (
            "T1-Flash/Normal001-T1-Flash.mha",
            "T1-MPRage/Normal001-MPRage.mha",
            "T2/Normal001-T2.mha", "MRA/Normal001-MRA.mha",
            "DTI/Normal001-DTI.mha",
            "AuxillaryData/SkullStripped-T1-Flash.mha",
            "AuxillaryData/VascularNetwork.tre",
        )),
        *(f"Normal-002/{x}" for x in (
            "T!-Flash/Normal002-T1-Flash.mha",
            "T2/Normal002-T2.mha", "MRA/Normal002-MRA.mha",
            "DTI/Normal002-DTI.mha",
            "AuxillaryData/SkullStripped-T1-Flash.mha",
            "AuxillaryData/VascularNetwork.tre",
        )),
    }


def test_bullitt_harvest_keys(bullitt, tmp_path):
    bullitt.download(tmp_path, keys=["mprage", "vessels", "phantoms"])
    src = tmp_path / "Bullitt" / "sourcedata"
    assert files(src) == {
        "Normal-001/T1-MPRage/Normal001-MPRage.mha",
        "Normal-003/MPRage/Normal003-MPRage.mha",
        *(f"Normal-{n}/AuxillaryData/VascularNetwork.tre"
          for n in ("001", "002", "003")),
        "Phantoms/phantom.mha",
    }


@pytest.mark.parametrize("layout", LAYOUTS)
def test_bullitt_roast(bullitt, tmp_path, layout):
    import nibabel as nib

    from brainspresso.datasets.bullitt.roaster import roast
    bullitt.download(tmp_path, exclude_subs=["003"], layout=layout)
    root = tmp_path / "Bullitt"
    roast(root, display=False)
    found = Layout.detect(root)
    raw = found.raw
    out = {p for p in files(raw) if not p.startswith((
        "sourcedata/", "derivatives/", ".brainspresso/"))}

    for name in ("README", "dataset_description.json",
                 "participants.json", "participants.tsv"):
        assert name in out
    anat = "sub-001/anat/sub-001"
    for suffix in ("acq-flash_T1w", "acq-mprage_T1w", "T2w", "angio"):
        assert {f"{anat}_{suffix}.nii.gz", f"{anat}_{suffix}.json"} <= out
    # "T!-Flash" typo on the server; no MPRAGE for this subject
    assert "sub-002/anat/sub-002_acq-flash_T1w.nii.gz" in out
    assert not any("sub-002_acq-mprage" in p for p in out)
    # DTI is not converted
    assert not any("/dwi/" in p for p in out)

    t2 = nib.load(raw / f"{anat}_T2w.nii.gz")
    assert t2.shape == (4, 3, 2)
    assert np.all(np.asarray(t2.dataobj) == 2)
    assert nib.aff2axcodes(t2.affine) == ("L", "P", "S")
    sidecar = json.loads((raw / f"{anat}_T2w.json").read_text())
    assert sidecar["MagneticFieldStrength"] == 3

    drv = found.derivative("bullitt")
    assert drv.root == root / "derivatives" / "bullitt"
    out = files(drv.root)
    for name in ("README", "dataset_description.json", ".bidsignore"):
        assert name in out
    assert drv.bidsignore.exists()
    brain = "sub-001/anat/sub-001_acq-flash_desc-brain_T1w"
    assert {f"{brain}.nii.gz", f"{brain}.json"} <= out
    assert json.loads((drv.root / f"{brain}.json").read_text()) == {
        "SkullStripped": True
    }
    assert "sub-002/anat/sub-002_desc-vessels_tubes.tre" in out


@pytest.mark.parametrize("layout", LAYOUTS)
def test_bullitt_roast_keys(bullitt, tmp_path, layout):
    from brainspresso.datasets.bullitt.roaster import roast
    bullitt.download(tmp_path, subs=["001"], layout=layout)
    root = tmp_path / "Bullitt"
    roast(root, keys=["T2w"], json="no", display=False)
    raw = Layout.detect(root).raw
    assert {p for p in files(raw) if not p.startswith((
        "sourcedata/", ".brainspresso/"))} == {
        "sub-001/anat/sub-001_T2w.nii.gz"
    }
    assert not (root / "derivatives").exists()


def test_compost_commands():
    from brainspresso.cli import app
    assert "compost" in app["unc-wisconsin"]._commands
    assert "compost" in app["bullitt"]._commands


@pytest.mark.parametrize("layout", LAYOUTS)
def test_bullitt_compost_sourcedata(bullitt, tmp_path, layout):
    from brainspresso.bids.compost import compost
    from brainspresso.datasets.bullitt.roaster import roast
    bullitt.download(tmp_path, subs=["001"], layout=layout)
    root = tmp_path / "Bullitt"
    roast(root, display=False)
    compost(root, yes=True)
    assert not (root / "sourcedata").exists()
    raw = Layout.detect(root).raw
    assert (raw / "sub-001/anat/sub-001_T2w.nii.gz").exists()
