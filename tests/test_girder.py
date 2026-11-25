"""
Girder backend and the UNC-Wisconsin dataset, against a fake Girder
server modelled on data.kitware.com.
"""
import asyncio
import hashlib
import io
import itertools
import json
import socket
import threading
from pathlib import Path, PurePosixPath

import numpy as np
import pytest
from aiohttp import web

from brainspresso.download import DownloadManager
from brainspresso.sources import girder as girder_mod
from brainspresso.sources.girder import Girder, parse_girder_url

nrrd = pytest.importorskip("nrrd")

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


@pytest.fixture(scope="module")
def server():
    fake = FakeGirder(TREE)
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
    assert all(f.sha512 and f.size for f in listing)
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
        f.path.parts[0] in ("002", "README.txt") for f in listing
    )
    pruned = server.requests - before
    total_before = server.requests
    Girder(server.api).walk("folder", server.root)
    assert pruned < server.requests - total_before


def test_mirror_verifies_sha512(server, tmp_path):
    g = Girder(server.api)
    dls = list(g.mirror("folder", server.root, tmp_path,
                        keep=lambda f: f.path.name == "README.txt"))
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
    from brainspresso.datasets.UNCWisconsin import download as dl
    monkeypatch.setattr(dl, "KITWARE", server.api)
    monkeypatch.setattr(dl, "SCAN_DATA", server.root)
    return dl


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


def test_uncwisc_roast(uncwisc, tmp_path):
    import nibabel as nib
    from brainspresso.datasets.UNCWisconsin.bidsifier import Bidsifier
    uncwisc.download(tmp_path)          # raw + meta, all subjects
    root = tmp_path / "UNC-Wisconsin"
    Bidsifier(root).run()
    out = files(root)
    for sub, ses in [("001", "12months"), ("001", "16months"),
                     ("002", "12months")]:
        base = f"rawdata/sub-{sub}/ses-{ses}"
        assert f"{base}/anat/sub-{sub}_ses-{ses}_T1w.nii.gz" in out
        assert f"{base}/anat/sub-{sub}_ses-{ses}_T2w.nii.gz" in out
    dwi = "rawdata/sub-001/ses-12months/dwi/sub-001_ses-12months_dwi"
    assert {f"{dwi}.nii.gz", f"{dwi}.bval", f"{dwi}.bvec"} <= out
    assert not any("sub-002/ses-12months/dwi" in p for p in out)
    assert nib.load(root / f"{dwi}.nii.gz").shape == (4, 4, 2, 3)
    assert (root / f"{dwi}.bval").read_text().split() == ["0", "1000", "1000"]
    t1 = nib.load(
        root / "rawdata/sub-002/ses-12months/anat/"
        "sub-002_ses-12months_T1w.nii.gz"
    )
    assert nib.aff2axcodes(t1.affine) == ("L", "P", "S")

    assert (root / "participants.tsv").read_text().splitlines() == [
        "participant_id\tspecies\tsex\tbirth_date",
        "sub-001\tMacaca mulatta\tF\t2010-06-06",
        "sub-002\tMacaca mulatta\tM\t2010-07-01",
    ]
    assert (
        root / "rawdata/sub-001/sub-001_sessions.tsv"
    ).read_text().splitlines() == [
        "session_id\tacq_time\tage\tweight",
        "ses-12months\t2011-06-01\t12\t1.9",
        "ses-16months\t2011-06-01\t16\t2.6",
    ]
    for name in ("README", "dataset_description.json",
                 "participants.json", "sessions.json"):
        assert name in out


def test_download_manager_integration(server, tmp_path):
    # mirror() yields Downloaders that the manager runs concurrently
    g = Girder(server.api)
    DownloadManager(
        g.mirror("folder", server.root, tmp_path,
                 keep=lambda f: f.path.parts[0] == "002"),
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
