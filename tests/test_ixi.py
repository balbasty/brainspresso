"""
IXI: harvest from NITRC-IR (fake XNAT server modelled on the real
listing) and roast, from folders (NITRC) and from tarballs (original
release).
"""
import asyncio
import gzip
import io
import json
import re
import socket
import tarfile
import threading
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest
from aiohttp import web
from cli_utils import bx
from roast_utils import LAYOUTS

from brainspresso.bids.layout import Layout
from brainspresso.datasets.ixi import roaster
from brainspresso.download import DownloadManager

# Templates of the installed package (CI tests the installed package)
TEMPLATES = roaster.TEMPLATES


def nifti(shape, value) -> bytes:
    if np.isscalar(value):
        value = np.full(shape, value, dtype=np.int16)
    img = nib.Nifti1Image(np.asarray(value), np.eye(4))
    return gzip.compress(img.to_bytes())


def dti_volumes(extra="geometric", count=17, seed=0) -> list[np.ndarray]:
    """Tiny DTI series: bright b0, 15 darker DW volumes, then (if
    count == 17) a 17th volume: their geometric or arithmetic mean, or
    noise"""
    rng = np.random.default_rng(seed)
    shape = (4, 4, 3)
    b0 = rng.uniform(100, 3000, shape)
    adc = rng.uniform(0.3e-3, 3e-3, (15, *shape))
    dw = b0 * np.exp(-1000 * adc)
    means = {"geometric": np.exp(np.log(dw).mean(0)),
             "arithmetic": dw.mean(0),
             "random": rng.uniform(0, 3000, shape)}
    vols = [b0, *dw, means[extra]]
    return [v.astype(np.float32) for v in vols[:count]]


def dti_files(prefix="IXI002-Guys-0828", **kwargs) -> dict[str, bytes]:
    return {f"{prefix}-DTI-{i:02d}.nii.gz": nifti(None, v)
            for i, v in enumerate(dti_volumes(**kwargs))}


BVALS = ("0" + " 995" * 15 + "\n").encode()
BVECS = ("\n".join(" ".join(["0"] * 16) for _ in range(3)) + "\n").encode()


# experiment -> scan -> collection -> {name: bytes}
SUBJECTS = {
    ("NITRC_IR_E10452", "2_Guys"): {
        "T1": {"NIfTI": {"IXI002-Guys-0828-T1.nii.gz": nifti((4, 4, 4), 1)}},
        "DTI": {
            "NIfTI": dti_files(),
            "SNAPSHOTS": {"qc.gif": b"GIF89a"},
            "bval": {"bvals.txt": BVALS},
            "bvec": {"bvecs.txt": BVECS},
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
    from brainspresso.datasets.ixi.harvest import nitrc_downloaders
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
        *(f"IXI-DTI/IXI002-Guys-0828-DTI-{i:02d}.nii.gz" for i in range(17)),
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


def test_harvest_command_per_key_sources(xnat, tmp_path, monkeypatch):
    # images from NITRC-IR (default), metadata from the bundled IXI.xls
    # (nothing to download, the offline imperial host is not tried)
    from brainspresso.datasets.ixi import harvest as dl
    server, _ = xnat
    monkeypatch.setattr(dl.NitrcIR, "server", server)
    monkeypatch.setattr(dl.Imperial, "urls", {"meta": ["http://0.0.0.0:9/x"]})
    bx("ixi", "harvest", tmp_path, "--keys", "T2w", "meta", "--subs", 12)
    assert files(tmp_path / "IXI" / "sourcedata") == {
        "nitrc_experiments.json", "IXI-T2/IXI012-HH-1211-T2.nii.gz",
    }


def check_roast(root: Path, per_subject: bytes | None, layout: str,
                dataset: bytes | None = None):
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
    assert dwi.shape == (4, 4, 3, 16)
    vols = dti_volumes()
    assert np.allclose(dwi.get_fdata(), np.stack(vols[:16], -1))
    # canonical table at the dataset level (BIDS inheritance)
    assert {"dwi.bval", "dwi.bvec"} <= out
    if dataset is None:
        dataset = (TEMPLATES / "bvals.txt").read_bytes()
    assert (raw / "dwi.bval").read_bytes() == dataset
    # per-subject table only when it differs from the canonical one
    if per_subject is None:
        assert "sub-002/dwi/sub-002_dwi.bval" not in out
        assert "sub-002/dwi/sub-002_dwi.bvec" not in out
    else:
        assert {"sub-002/dwi/sub-002_dwi.bval",
                "sub-002/dwi/sub-002_dwi.bvec"} <= out
        bval = (raw / "sub-002/dwi/sub-002_dwi.bval").read_bytes()
        assert bval == per_subject


@pytest.mark.parametrize("layout", LAYOUTS)
def test_roast_from_nitrc_folders(xnat, tmp_path, monkeypatch, layout):
    server, _ = xnat
    root = tmp_path / "IXI"
    harvest(server, root / "sourcedata")

    # Without bundled bvals/bvecs: the global table comes from sourcedata;
    # NITRC tables identical to it are not duplicated per subject
    empty = tmp_path / "empty_templates"
    empty.mkdir()
    for sub in ("Guys", "HH", "IOP"):
        (empty / sub).symlink_to(TEMPLATES / sub)
    monkeypatch.setattr(roaster, "TEMPLATES", empty)
    (root / "sourcedata" / "bvals.txt").write_bytes(BVALS)
    (root / "sourcedata" / "bvecs.txt").write_bytes(BVECS)
    roaster.roast(root, keys={"T1w", "T2w", "dwi"}, layout=layout)
    check_roast(root, per_subject=None, layout=layout, dataset=BVALS)


@pytest.mark.parametrize("layout", LAYOUTS)
def test_roast_bundled_metadata(xnat, tmp_path, layout):
    server, _ = xnat
    root = tmp_path / "IXI"
    harvest(server, root / "sourcedata")
    roaster.roast(root, keys={"meta", "T1w", "T2w", "dwi"}, layout=layout)
    # IXI.xls comes from the package templates; the per-subject NITRC
    # gradient tables (b=995) differ from the bundled global ones (b=1000)
    check_roast(root, per_subject=BVALS, layout=layout)
    raw = Layout.detect(root).raw
    lines = (raw / "participants.tsv").read_text().splitlines()
    assert lines[0].startswith("participant_id\tsite\tage\tsex")
    ids = {line.split("\t")[0] for line in lines[1:]}
    assert ids == {"sub-002", "sub-012"}

    # dates (BIDS format "date", YYYY-MM-DD), not numbers with units
    header = lines[0].split("\t")
    for line in lines[1:]:
        row = dict(zip(header, line.split("\t")))
        for key in ("dob", "study_date"):
            assert re.fullmatch(r"\d{4}-\d{2}-\d{2}|n/a", row[key])
        # integral numbers without decimals (xlrd reads floats)
        for key in ("height", "weight"):
            assert re.fullmatch(r"\d+(\.\d*[1-9])?|n/a", row[key])
    row = dict(zip(header, lines[1].split("\t")))
    assert (row["height"], row["weight"]) == ("164", "58")


def test_participants_json():
    """Data dictionary: dates in the BIDS "date" format, age in years
    (BIDS unit "year")"""
    meta = json.loads((roaster.TEMPLATES / "participants.json").read_text())
    for key in ("dob", "study_date"):
        assert meta[key]["Format"] == "date" and "Units" not in meta[key]
    assert meta["age"]["Units"] == "year"


def write_tarballs(src: Path):
    """Write the original IXI tarballs (`IXI-<scan>.tar`) of SUBJECTS."""
    src.mkdir(parents=True, exist_ok=True)
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


@pytest.mark.parametrize("layout", LAYOUTS)
def test_roast_from_tarballs(xnat, tmp_path, layout):
    """The original tarballs are still supported"""
    root = tmp_path / "IXI"
    write_tarballs(root / "sourcedata")
    roaster.roast(root, keys={"T1w", "T2w", "dwi"}, layout=layout)
    check_roast(root, per_subject=None, layout=layout)


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
        "sub-002/anat/sub-002_T1w.nii.gz",
        "sub-002/dwi/sub-002_dwi.nii.gz",
        "dwi.bval", "dwi.bvec",
    }
    assert report.counts["dwi"]["done"] == 3
    assert not report.errors


def test_missing_images_are_errors(tmp_path):
    """A wanted modality whose images are missing is a `Fail` (it used
    to be a warning); modalities not wanted are not looked for"""
    root = tmp_path / "IXI"
    folder = root / "sourcedata" / "IXI-T1"
    folder.mkdir(parents=True)
    data = SUBJECTS[("NITRC_IR_E10452", "2_Guys")]["T1"]["NIfTI"]
    for name, blob in data.items():
        (folder / name).write_bytes(blob)
    report = roaster.roast(root, keys={"T1w"}, display=False)
    assert not report.errors
    report = roaster.roast(root, keys={"T1w", "PDw", "dwi"}, display=False)
    assert sorted(e[2] for e in report.errors) == [
        "IXI-DTI.tar (or IXI-DTI/) not found",
        "IXI-PD.tar (or IXI-PD/) not found"]
    assert report.status == "partial"


# ----------------------------------------------------------------------
#   DWI volume layout
# ----------------------------------------------------------------------

def make_dti(root: Path, content: dict[str, bytes] | list, tar=False):
    """`content`: {name: bytes}, or a list of (name, bytes) (tar
    members, possibly duplicated)"""
    src = root / "sourcedata"
    src.mkdir(parents=True, exist_ok=True)
    items = content.items() if isinstance(content, dict) else content
    if tar:
        with tarfile.open(src / "IXI-DTI.tar", "w") as f:
            for name, data in items:
                info = tarfile.TarInfo(name)
                info.size = len(data)
                f.addfile(info, io.BytesIO(data))
    else:
        (src / "IXI-DTI").mkdir(exist_ok=True)
        for name, data in items:
            (src / "IXI-DTI" / name).write_bytes(data)


def roast_dti(root, layout, keys=("dwi", "trace"), **kwargs):
    return roaster.roast(root, keys=set(keys), layout=layout,
                         display=False, **kwargs)


def outputs(root):
    found = Layout.detect(root)
    raw = {p for p in files(found.raw)
           if not p.startswith(("sourcedata/", ".brainspresso/",
                                "derivatives/"))}
    drv = found.derivative("ixi-scanner").root
    return found, raw, files(drv) if drv.is_dir() else set()


@pytest.mark.parametrize("layout", LAYOUTS)
def test_dwi_duplicated_tar_member(tmp_path, layout):
    root = tmp_path / "IXI"
    content = list(dti_files().items())
    make_dti(root, [content[0], *content], tar=True)
    report = roast_dti(root, layout)
    assert not report.errors
    found, raw, drv = outputs(root)
    assert nib.load(found.raw / "sub-002/dwi/sub-002_dwi.nii.gz").shape \
        == (4, 4, 3, 16)
    assert "sub-002/dwi/sub-002_desc-trace_dwimap.nii.gz" in drv


def test_dwi_differing_duplicates_fail(tmp_path):
    root = tmp_path / "IXI"
    content = list(dti_files().items())
    other = (content[0][0], content[1][1])
    make_dti(root, [other, *content], tar=True)
    report = roast_dti(root, "flat")
    assert any("differing duplicates" in e[2] for e in report.errors)
    assert not (root / "sub-002/dwi/sub-002_dwi.nii.gz").exists()


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("extra,desc", [("geometric", "trace"),
                                        ("arithmetic", "powderavg")])
def test_dwi_derived_volume(tmp_path, layout, extra, desc):
    root = tmp_path / "IXI"
    make_dti(root, dti_files(extra=extra))
    report = roast_dti(root, layout)
    assert not report.errors
    found, raw, drv = outputs(root)
    stem = f"sub-002/dwi/sub-002_desc-{desc}_dwimap"
    assert {f"{stem}.nii.gz", f"{stem}.json", "dataset_description.json",
            ".bidsignore"} <= drv
    droot = found.derivative("ixi-scanner").root
    img = nib.load(droot / f"{stem}.nii.gz")
    assert np.allclose(img.get_fdata(), dti_volumes(extra=extra)[16])
    side = json.loads((droot / f"{stem}.json").read_text())
    assert side["SkullStripped"] is False
    assert side["Sources"] == ["bids:raw:sub-002/dwi/sub-002_dwi.nii.gz"]
    dd = json.loads((droot / "dataset_description.json").read_text())
    assert dd["DatasetType"] == "derivative"
    assert dd["GeneratedBy"][0]["Name"] == "scanner"
    assert dd["SourceDatasets"]
    assert "*_dwimap" in (droot / ".bidsignore").read_text()
    assert "sub-002/dwi/sub-002_dwi.bval" not in raw
    assert (found.raw / "dwi.bval").read_bytes() == \
        (TEMPLATES / "bvals.txt").read_bytes()


def test_dwi_unrecognised_extra_volume(tmp_path):
    root = tmp_path / "IXI"
    make_dti(root, dti_files(extra="random"))
    report = roast_dti(root, "flat")
    assert any("unrecognised DWI volume layout" in e[2]
               for e in report.errors)
    assert not (root / "sub-002/dwi/sub-002_dwi.nii.gz").exists()


@pytest.mark.parametrize("layout", LAYOUTS)
def test_dwi_16_volumes(tmp_path, layout):
    root = tmp_path / "IXI"
    make_dti(root, dti_files(count=16))
    report = roast_dti(root, layout)
    assert not report.errors
    found, raw, drv = outputs(root)
    assert "dwi.bvec" in raw
    assert not any(p.endswith("_dwimap.nii.gz") for p in drv)


def test_dwi_15_volumes_fail(tmp_path):
    root = tmp_path / "IXI"
    make_dti(root, dti_files(count=15))
    report = roast_dti(root, "flat")
    assert any("(15," in e[2] for e in report.errors)


@pytest.mark.parametrize("layout", LAYOUTS)
def test_dwi_per_subject_table_17_entries(tmp_path, layout):
    """A 17-entry NITRC table is trimmed when volume 16 is derived"""
    root = tmp_path / "IXI"
    make_dti(root, {
        **dti_files(),
        "IXI002-Guys-0828-bvals.txt": b"0" + b" 995" * 16 + b"\n",
        "IXI002-Guys-0828-bvecs.txt": BVECS.replace(b"0\n", b"0 0\n"),
    })
    report = roast_dti(root, layout)
    assert not report.errors
    found, _, _ = outputs(root)
    dwi = found.raw / "sub-002/dwi/sub-002_dwi"
    assert Path(f"{dwi}.bval").read_bytes() == BVALS
    assert Path(f"{dwi}.bvec").read_bytes() == BVECS


def reformat(data: bytes, sign: float = 1) -> bytes:
    """Same table, different formatting (floats, trailing spaces)"""
    rows = [line.split() for line in data.decode().splitlines()]
    return "".join(" ".join(f"{sign * float(x) + 0.0:.6f}" for x in row)
                   + "  \n" for row in rows).encode()


@pytest.mark.parametrize("layout", LAYOUTS)
def test_dwi_per_subject_table_same_as_canonical(tmp_path, layout):
    """A NITRC table numerically equal to the canonical one (here with
    `1000.0` and trailing spaces) is inherited, not duplicated"""
    root = tmp_path / "IXI"
    make_dti(root, {
        **dti_files(),
        "IXI002-Guys-0828-bvals.txt":
            reformat((TEMPLATES / "bvals.txt").read_bytes()),
        "IXI002-Guys-0828-bvecs.txt":
            reformat((TEMPLATES / "bvecs.txt").read_bytes()),
    })
    report = roast_dti(root, layout)
    assert not report.errors
    _, raw, _ = outputs(root)
    assert {"dwi.bval", "dwi.bvec"} <= raw
    assert not any(p.startswith("sub-002/dwi/sub-002_dwi.bv") for p in raw)


def test_dwi_per_subject_bvecs_negated(tmp_path):
    """A negated bvec table is a different table"""
    root = tmp_path / "IXI"
    make_dti(root, {
        **dti_files(),
        "IXI002-Guys-0828-bvals.txt": (TEMPLATES / "bvals.txt").read_bytes(),
        "IXI002-Guys-0828-bvecs.txt":
            reformat((TEMPLATES / "bvecs.txt").read_bytes(), sign=-1),
    })
    report = roast_dti(root, "flat")
    assert not report.errors
    _, raw, _ = outputs(root)
    assert "sub-002/dwi/sub-002_dwi.bvec" in raw
    assert "sub-002/dwi/sub-002_dwi.bval" not in raw


def test_dwi_subs_keep_dataset_table(tmp_path):
    """`--subs` does not change the dataset-level table"""
    root = tmp_path / "IXI"
    make_dti(root, dti_files())
    roaster.roast(root, subs=[999], layout="flat", display=False)
    assert (root / "dwi.bval").read_bytes() == \
        (TEMPLATES / "bvals.txt").read_bytes()
    assert not (root / "sub-002").exists()


def test_dwi_table_mismatch_fail(tmp_path):
    root = tmp_path / "IXI"
    make_dti(root, {**dti_files(count=16),
                    "IXI002-Guys-0828-bvals.txt": b"0 1000 1000\n",
                    "IXI002-Guys-0828-bvecs.txt": BVECS})
    report = roast_dti(root, "flat")
    assert any("has 3 entries" in e[2] for e in report.errors)


@pytest.mark.parametrize("layout", LAYOUTS)
def test_dwi_exclude_trace(tmp_path, layout):
    root = tmp_path / "IXI"
    make_dti(root, dti_files())
    roaster.roast(root, exclude_keys={"trace"}, layout=layout,
                  display=False)
    found, raw, drv = outputs(root)
    assert "sub-002/dwi/sub-002_dwi.nii.gz" in raw
    assert not drv
    # included by default
    roaster.roast(root, layout=layout, display=False)
    assert any(p.endswith("_desc-trace_dwimap.nii.gz")
               for p in outputs(root)[2])


def test_dwi_subs_prune_before_loading(tmp_path, monkeypatch):
    """Subjects dropped by `--subs` are not loaded"""
    root = tmp_path / "IXI"
    make_dti(root, {**dti_files(), **dti_files("IXI012-HH-1211")})
    loaded = []
    load = roaster.load_volume
    monkeypatch.setattr(roaster, "load_volume", lambda a, m: (
        loaded.append(m.name), load(a, m))[1])
    report = roast_dti(root, "flat", subs=[2], jobs=1)
    assert not report.errors
    assert loaded and all("IXI002" in name for name in loaded)
    assert not (root / "sub-012").exists()


def test_dwi_rerun_skip_does_not_classify(tmp_path, monkeypatch):
    root = tmp_path / "IXI"
    make_dti(root, dti_files())
    roast_dti(root, "flat")
    calls = []
    classify = roaster.classify_derived
    monkeypatch.setattr(roaster, "classify_derived", lambda v: (
        calls.append(1), classify(v))[1])
    report = roast_dti(root, "flat", ifexists="skip", jobs=1)
    assert not report.errors
    assert not calls


def test_dwi_keys_trace_only(tmp_path):
    root = tmp_path / "IXI"
    make_dti(root, dti_files())
    report = roast_dti(root, "study", keys=("trace",))
    assert not report.errors
    found, raw, drv = outputs(root)
    assert not any(p.startswith(("sub-", "dwi.")) for p in raw)
    assert drv == {"dataset_description.json", ".bidsignore",
                   "sub-002/dwi/sub-002_desc-trace_dwimap.nii.gz",
                   "sub-002/dwi/sub-002_desc-trace_dwimap.json"}


def test_dwi_invalid_canonical_table(tmp_path):
    """An invalid dataset-level table is one error; subjects with their
    own NITRC table are still roasted, the others fail"""
    root = tmp_path / "IXI"
    make_dti(root, {
        **dti_files(count=16), **dti_files("IXI012-HH-1211", count=16),
        "IXI002-Guys-0828-bvals.txt": BVALS,
        "IXI002-Guys-0828-bvecs.txt": BVECS,
    })
    (root / "sourcedata/bvals.txt").write_text("0 1000\n1000\n")
    report = roast_dti(root, "flat")
    errors = sorted(e[2] for e in report.errors)
    assert len(errors) == 2
    assert "bvals.txt has rows of 2, 1 entries, expected 16" in errors
    assert any("sub-012: no valid bvals.txt" in e for e in errors)
    assert (root / "sub-002/dwi/sub-002_dwi.nii.gz").exists()
    assert (root / "sub-002/dwi/sub-002_dwi.bval").read_bytes() == BVALS
    assert not (root / "sub-012/dwi/sub-012_dwi.nii.gz").exists()
    assert not (root / "dwi.bval").exists()


def test_dwi_16_volumes_no_derivative(tmp_path):
    root = tmp_path / "IXI"
    make_dti(root, dti_files(count=16))
    report = roast_dti(root, "study")
    assert not report.errors
    assert not (root / "derivatives/ixi-scanner").exists()


def test_dwi_ragged_table_message(tmp_path):
    root = tmp_path / "IXI"
    make_dti(root, {**dti_files(count=16),
                    "IXI002-Guys-0828-bvals.txt": b"0 1000\n1000\n",
                    "IXI002-Guys-0828-bvecs.txt": BVECS})
    report = roast_dti(root, "flat")
    assert any("has rows of 2, 1 entries" in e[2] for e in report.errors)
    assert not any("None" in e[2] for e in report.errors)


# ----------------------------------------------------------------------
#   Missing tarballs: an error only for the keys that `--keys` names
# ----------------------------------------------------------------------

def test_roast_options_named():
    """`named`: the keys of `--keys` as given; none with its default"""
    assert roaster.roast_options(()) == {"named": []}
    assert roaster.roast_options(list(roaster.DATASET_KEYS)) == \
        {"named": []}
    assert roaster.roast_options(["pdw", "meta"]) == {"named": ["PDw"]}
    assert roaster.roast_options(["dwi", "trace"]) == \
        {"named": ["dwi", "trace"]}


def test_missing_tarballs_not_named(tmp_path, caplog):
    """Without `--keys`, a missing tarball (no PD, no MRA) is an info
    line: the roast is complete"""
    root = tmp_path / "IXI"
    write_tarballs(root / "sourcedata")
    with caplog.at_level("INFO"):
        report = roaster.roast(root, display=False)
    assert not report.errors, report.errors
    assert report.status == "complete"
    assert "IXI-PD.tar (or IXI-PD/) not found (skipped)" in caplog.text


@pytest.mark.parametrize("keys", [["PDw"], ["pdw", "T1w"]])
def test_missing_tarballs_named(tmp_path, keys):
    """A missing tarball of a key that `--keys` names is an error"""
    root = tmp_path / "IXI"
    write_tarballs(root / "sourcedata")
    report = roaster.roast(root, keys=keys, display=False)
    assert [e[:2] for e in report.errors] == [("PDw", "IXI-PD")]
    assert report.status == "partial"


@pytest.mark.parametrize("named", [False, True])
def test_missing_tarballs_cli(tmp_path, named):
    """The command line: its default `--keys` names nothing"""
    root = tmp_path / "IXI"
    write_tarballs(root / "sourcedata")
    keys = ["--keys", "T1w", "angio"] if named else []
    bx("ixi", "roast", "--out", root, *keys)
    layout = Layout.detect(root)
    run = json.loads((layout.state / "manifest.json").read_text())["runs"][-1]
    assert run["status"] == ("partial" if named else "complete")
    assert ("angio" in run["counts"]) is named
    assert (layout.raw / "sub-002" / "anat" / "sub-002_T1w.nii.gz").is_file()


def test_missing_dti_named_by_trace(tmp_path):
    """`--keys trace` reads the DTI tarball: its absence is an error"""
    root = tmp_path / "IXI"
    write_tarballs(root / "sourcedata")
    (root / "sourcedata" / "IXI-DTI.tar").unlink()
    report = roaster.roast(root, keys=["trace"], display=False)
    assert [e[1] for e in report.errors] == ["IXI-DTI"]
    report = roaster.roast(root, display=False)
    assert not report.errors


@pytest.mark.parametrize("subs", [
    ["sub-012"], ["012"], [12], "12", "10:13", "file"])
def test_python_subs_labels_ranges_files(tmp_path, subs):
    """`roast(subs=)` takes what `--subs` takes: numbers, labels with or
    without `sub-`, ranges, files (`roast.run.python_api`)"""
    root = tmp_path / "IXI"
    write_tarballs(root / "sourcedata")
    if subs == "file":
        subs = tmp_path / "subs.txt"
        subs.write_text("012\n")
        subs = [str(subs)]
    roaster.roast(root, keys={"T1w"}, subs=subs, json="no", display=False)
    assert outputs(root)[1] == {"sub-012/anat/sub-012_T1w.nii.gz"}
    roaster.roast(root, keys={"T1w"}, exclude_subs="10:13", json="no",
                  display=False)
    assert "sub-002/anat/sub-002_T1w.nii.gz" in outputs(root)[1]
