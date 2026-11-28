"""
Hybrid placement of datasets hosted as BIDS (`bids.hosted`,
`roast.hosted`): OpenNeuro and the INDI projects, against local fakes of
their buckets.

Images are placed directly; metadata goes to `sourcedata/<folder>/`
under its remote path and is roasted into the dataset; fill-in sidecar
templates add the keys that hosted sidecars lack.
"""
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest
from fake_s3 import bucket_app, serve

from brainspresso.actions.state import read_task_states
from brainspresso.bids.compost import compost
from brainspresso.bids.harvest import InlineEntry, sha256, update_files
from brainspresso.bids.hosted import (
    FillPlan,
    FillTemplate,
    fill_files,
    fill_in,
    is_data,
    placement,
    plan_fill,
    read_fill_templates,
    render,
    trim_rows,
)
from brainspresso.bids.layout import Layout, make_layout, read_manifest
from brainspresso.bids.select import Selection
from brainspresso.cli import app
from brainspresso.datasets.catalog import CATALOG, load
from brainspresso.datasets.indi.dataset import INDIDataset
from brainspresso.datasets.indi.engine import Project, Site
from brainspresso.datasets.register import register
from brainspresso.roast import hosted as hosted_mod
from brainspresso.roast.hosted import (
    RoastMetadata,
    _plan,
    harvest_hosted,
    roast_hosted,
)
from brainspresso.sources import fcp
from brainspresso.sources import openneuro as openneuro_mod
from brainspresso.sources.base import RemoteEntry
from brainspresso.sources.s3 import S3Bucket

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
from cli_snapshot import flags as command_flags  # noqa: E402


def bx(*args):
    try:
        app([str(a) for a in args])
    except SystemExit as e:
        assert not e.code, e.code


def read_json(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def files(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in Path(root).rglob("*")
            if p.is_file() and ".brainspresso" not in p.parts}


def tsv(*rows) -> bytes:
    return "".join("\t".join(r) + "\n" for r in rows).encode()


# ----------------------------------------------------------------------
#   Classification
# ----------------------------------------------------------------------

@pytest.mark.parametrize("path,where,data", [
    ("sub-01/anat/sub-01_T1w.nii.gz", "direct", True),
    ("sub-01/anat/sub-01_T1w.json", "roasted", False),
    ("sub-01/dwi/sub-01_dwi.bval", "roasted", False),
    ("sub-01/func/sub-01_task-x_events.tsv", "roasted", False),
    ("sub-01/func/sub-01_task-x_physio.tsv.gz", "direct", True),
    ("sub-01/sub-01_sessions.tsv", "roasted", False),
    ("participants.tsv", "roasted", False),
    ("dataset_description.json", "roasted", False),
    ("README", "roasted", False),
    ("task-rest_bold.json", "roasted", False),
    ("phenotype/x.tsv", "roasted", False),
    # recordings are one unit: their files are never roasted
    ("sub-01/micr/sub-01_sample-A_SPIM.ome.zarr/zarr.json", "direct", True),
    ("sub-01/meg/sub-01_task-x_meg.ds/x.json", "direct", True),
    # optional folders other than derivatives: not BIDS metadata
    ("sourcedata/sub-01/x.json", "direct", False),
    ("code/run.json", "direct", False),
    ("stimuli/x.tsv", "direct", False),
    ("derivatives/fs/sub-01/anat/sub-01_dseg.json", "roasted", False),
    ("derivatives/fs/sub-01/anat/sub-01_dseg.nii.gz", "direct", True),
    ("derivatives/fs/dataset_description.json", "roasted", False),
])
def test_placement(path, where, data):
    assert placement(path) == where
    assert is_data(path) is data


# ----------------------------------------------------------------------
#   Rendering
# ----------------------------------------------------------------------

def test_render_is_a_copy_when_nothing_changes():
    blob = b'{"EchoTime":   0.03}\n'
    assert render(blob, "x_T1w.json") is blob
    assert render(blob, "x_T1w.json", repair=True) is blob
    table = b"participant_id\tage\nsub-01\t30\n"
    assert render(table, "participants.tsv", repair=True) is table
    assert render(b"0 1000", "dwi.bval", repair=True) == b"0 1000"
    # not UTF-8: copied as is
    latin = b'{"Note": "Z\xfcrich", "EchoTime": "0.03"}'
    assert render(latin, "x_T1w.json", repair=True) is latin


def test_render_repairs_and_fills():
    blob = b'{"MagneticFieldStrength": "3T", "EchoTime": 0.03,}'
    plan = FillPlan((FillTemplate("T1w.json", {
        "EchoTime": 9, "Manufacturer": "GE", "FlipAngle": 8,
    }),), exclude=frozenset({"FlipAngle"}))
    out = json.loads(render(blob, "sub-1_T1w.json", repair=True, fill=plan))
    # hosted keys win; excluded keys are never added
    assert out == {"MagneticFieldStrength": 3, "EchoTime": 0.03,
                   "Manufacturer": "GE"}
    # a new fill-in sidecar
    assert json.loads(render(None, "T1w.json", fill=plan)) == {
        "EchoTime": 9, "Manufacturer": "GE"}


def test_render_phasediff_reads_its_sibling():
    out = render(b'{"EchoTime": 0.00738}', "sub-1_phasediff.json",
                 repair=True,
                 sibling=lambda name: {"EchoTime": 0.00492}
                 if name == "sub-1_magnitude1.json" else None)
    assert json.loads(out)["EchoTime1"] == 0.00492


def test_fill_in_never_overrides():
    plan = FillPlan((FillTemplate("a.json", {"A": 1, "B": 1}, "site"),
                     FillTemplate("a.json", {"B": 2, "C": 2})))
    assert fill_in({"A": 0}, plan) == {"A": 0, "B": 1, "C": 2}
    assert fill_in({"A": 0}, None) == {"A": 0}


# ----------------------------------------------------------------------
#   Fill-in templates
# ----------------------------------------------------------------------

def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def test_read_fill_templates(tmp_path):
    write(tmp_path / "README", "x")
    write(tmp_path / "T1w.json", '{"Manufacturer": "GE"}')
    write(tmp_path / "A/T1w.json", '{"EchoTime": 1}')
    write(tmp_path / "B/task-rest_bold.json", '{"RepetitionTime": 2}')
    write(tmp_path / "derivatives/x/dataset_description.json", "{}")
    write(tmp_path / "_fill.json", '{"fill": ["T1w.json", "*/T1w.json", '
          '"B/task-rest_bold.json"]}')
    found = read_fill_templates(tmp_path, ["A", "B"])
    assert [(t.site, t.name) for t in found] == [
        (None, "T1w.json"), ("A", "T1w.json"), ("B", "task-rest_bold.json"),
    ]
    assert found[1].content == {"EchoTime": 1}
    assert fill_files(tmp_path) == {"_fill.json", "T1w.json", "A/T1w.json",
                                    "B/task-rest_bold.json"}
    assert fill_files(tmp_path / "A") == set()


@pytest.mark.parametrize("index,files,message", [
    ('{"fill": ["X/T1w.json"]}', {"X/T1w.json": "{}"}, "not a site"),
    ('{"fill": ["T2w.json"]}', {}, "matches no file"),
    ('{"fill": ["participants.json"]}', {"participants.json": "{}"},
     "not the name of a sidecar"),
    ('{"fill": ["sub-1_T1w.json"]}', {"sub-1_T1w.json": "{}"},
     "not a subject"),
    ('{"fill": ["T1w.json"]}', {"T1w.json": "[1]"}, "not a JSON object"),
    ('{"fill": "T1w.json"}', {}, "must be a list"),
    ('{"files": []}', {}, "unknown keys"),
    ('["T1w.json"]', {}, "not a JSON object"),
    ('{"fill": ["A/B/T1w.json"]}', {"A/B/T1w.json": "{}"}, "templates are"),
])
def test_read_fill_templates_errors(tmp_path, index, files, message):
    for name, text in files.items():
        write(tmp_path / name, text)
    write(tmp_path / "_fill.json", index)
    with pytest.raises(ValueError, match=message):
        read_fill_templates(tmp_path, ["A"])


def template(name, site=None, **content):
    return FillTemplate(name, content, site)


def test_plan_fill_top_level():
    present = ["sub-01/anat/sub-01_T1w.nii.gz",
               "sub-01/func/sub-01_task-rest_bold.nii.gz"]
    plans = plan_fill(
        [template("T1w.json", A=1), template("T1w.json", "S", B=1),
         template("dwi.json", C=1), template("bold.json", D=1)],
        present=present, placements={"task-rest_bold.json"}, site="S",
    )
    # the site template comes first (it wins), merged with the dataset's
    assert [t.site for t in plans["T1w.json"].layers] == ["S", None]
    # no dwi data: no new file; `bold.json` goes into the more specific
    # hosted top-level sidecar (both would describe the bold run)
    assert set(plans) == {"T1w.json", "task-rest_bold.json"}


def test_plan_fill_more_general_hosted_sidecar_wins():
    plans = plan_fill(
        [template("task-rest_bold.json", A=1)],
        present=["sub-01/func/sub-01_task-rest_bold.nii.gz"],
        placements={"bold.json"},
    )
    assert plans == {}


def test_plan_fill_pushed_down_to_the_subjects_of_a_site():
    present = ["sub-A1/anat/sub-A1_T1w.nii.gz",
               "sub-A2/anat/sub-A2_T1w.nii.gz",
               "sub-B1/anat/sub-B1_T1w.nii.gz",
               "sub-A3/func/sub-A3_task-rest_bold.nii.gz"]
    plans = plan_fill(
        [template("T1w.json", "A", EchoTime=1, Manufacturer="GE")],
        present=present,
        placements={"T1w.json", "sub-A2/sub-A2_T1w.json"},
        read=lambda path: {"EchoTime": 0.5} if path == "T1w.json" else {},
        subject_sites={"A1": "A", "A2": "A", "A3": "A", "B1": "B"},
    )
    # subjects of site A with T1w data; merged into a hosted subject file
    assert set(plans) == {"sub-A1/sub-A1_T1w.json", "sub-A2/sub-A2_T1w.json"}
    # a hosted top-level key is never pushed down (it would override it)
    assert plans["sub-A1/sub-A1_T1w.json"].exclude == {"EchoTime"}


# ----------------------------------------------------------------------
#   OpenNeuro (S3 listing route)
# ----------------------------------------------------------------------

DS = "ds000001"
FMAP = {"IntendedFor": ["func/sub-01_task-rest_bold.nii.gz",
                        "dwi/sub-01_dwi.nii.gz"]}
ON = {
    "CHANGES": b"1.0.0 2024-01-01\n  - initial\n",
    "README": b"A dataset\n",
    "dataset_description.json":
        b'{"Name": "x", "BIDSVersion": "1.9.0", "DatasetType": "raw"}',
    "participants.tsv": tsv(["participant_id", "age"], ["sub-01", "30"],
                            ["sub-02", "40"], ["sub-03", "50"]),
    "task-rest_bold.json": b'{"TaskName": "rest", "RepetitionTime": 2}',
    "sub-01/anat/sub-01_T1w.nii.gz": b"\x01" * 64,
    # odd formatting: a roasted copy is byte-identical
    "sub-01/anat/sub-01_T1w.json": b'{  "EchoTime" : 0.003 }',
    "sub-01/func/sub-01_task-rest_bold.nii.gz": b"\x02" * 64,
    "sub-01/func/sub-01_task-rest_events.tsv":
        tsv(["onset", "duration"], ["0", "1"]),
    "sub-01/fmap/sub-01_dir-AP_epi.nii.gz": b"\x03" * 64,
    "sub-01/fmap/sub-01_dir-AP_epi.json": json.dumps(FMAP).encode(),
    "sub-01/sub-01_scans.tsv": tsv(
        ["filename", "acq_time"],
        ["anat/sub-01_T1w.nii.gz", "n/a"],
        ["func/sub-01_task-rest_bold.nii.gz", "n/a"]),
    "sub-02/anat/sub-02_T1w.nii.gz": b"\x04" * 64,
    "sub-02/anat/sub-02_T1w.json": b'{"EchoTime": 0.003}',
    "sub-03/anat/sub-03_T1w.nii.gz": b"\x05" * 64,
    # a defect that repair fixes
    "sub-03/anat/sub-03_T1w.json": b'{"MagneticFieldStrength": "3T"}',
}
ON_OBJECTS = {f"{DS}/{k}": v for k, v in ON.items()}
ON_REQUESTS = Counter()
ON_FAIL = set()                 # listed, but their download fails


@pytest.fixture(scope="module")
def openneuro_url():
    url, stop = serve(bucket_app("openneuro.org", ON_OBJECTS, ON_REQUESTS,
                                 ON_FAIL))
    yield url
    stop()


@pytest.fixture
def openneuro(openneuro_url, monkeypatch):
    monkeypatch.setattr(openneuro_mod, "S3Bucket",
                        lambda name: S3Bucket(name, endpoint=openneuro_url))
    ON_REQUESTS.clear()
    ON_FAIL.clear()
    original = dict(ON_OBJECTS)
    yield ON_OBJECTS
    ON_OBJECTS.clear()
    ON_OBJECTS.update(original)
    ON_FAIL.clear()


def on(command, path, *args):
    bx("openneuro", command, DS, path,
       *(("--source", "s3") if command == "harvest" else ()), *args)
    return Layout.detect(Path(path) / "OpenNeuro" / DS)


@pytest.mark.parametrize("kind", ["study", "flat"])
def test_openneuro_hybrid_placement(tmp_path, openneuro, kind):
    lay = on("harvest", tmp_path, "--layout", kind)
    assert lay.kind == kind
    raw, pristine = lay.raw, lay.source / "openneuro"
    # images directly in the BIDS dataset, never in sourcedata/
    assert (raw / "sub-01/anat/sub-01_T1w.nii.gz").read_bytes() == \
        ON["sub-01/anat/sub-01_T1w.nii.gz"]
    assert not (pristine / "sub-01/anat/sub-01_T1w.nii.gz").exists()
    # metadata: a pristine copy under its remote path, roasted
    for name in ("sub-01/anat/sub-01_T1w.json", "README",
                 "sub-01/func/sub-01_task-rest_events.tsv",
                 "dataset_description.json", "participants.tsv"):
        assert (pristine / name).read_bytes() == ON[name]
    assert (raw / "sub-01/anat/sub-01_T1w.json").read_bytes() == \
        ON["sub-01/anat/sub-01_T1w.json"]
    assert (raw / "README").read_bytes() == ON["README"]
    # field maps: targets that are not in the dataset are dropped
    assert read_json(raw / "sub-01/fmap/sub-01_dir-AP_epi.json") == {
        "IntendedFor": ["func/sub-01_task-rest_bold.nii.gz"]}
    # tables: complete copy in the state folder, visible one trimmed
    assert (raw / "participants.tsv").read_text() == \
        ON["participants.tsv"].decode()
    if kind == "study":
        assert read_json(lay.root / "dataset_description.json")[
            "DatasetType"] == "study"
    # manifest: origin of every file, source of the roasted ones
    entries = read_manifest(lay.state)["files"]
    prefix = "" if kind == "flat" else "rawbids/"
    assert entries[f"{prefix}sub-01/anat/sub-01_T1w.nii.gz"]["origin"] \
        == "direct"
    t1w = entries[f"{prefix}sub-01/anat/sub-01_T1w.json"]
    assert t1w["origin"] == "roasted"
    assert t1w["source"] == "sourcedata/openneuro/sub-01/anat/sub-01_T1w.json"
    assert t1w["repair"] is True         # (owner: repair by default)
    assert entries[t1w["source"]]["origin"] == "direct"
    stages = [r["stage"] for r in read_manifest(lay.state)["runs"]]
    assert stages[-2:] == ["harvest", "roast"]


def test_openneuro_partial_harvests(tmp_path, openneuro):
    on("harvest", tmp_path, "--sub", "02")
    lay = on("harvest", tmp_path, "--sub", "03")
    rows = (lay.raw / "participants.tsv").read_text().splitlines()
    assert rows == ["participant_id\tage", "sub-02\t40", "sub-03\t50"]
    assert not (lay.raw / "sub-01").exists()
    assert (lay.raw / "sub-02/anat/sub-02_T1w.json").exists()
    entries = read_manifest(lay.state)["files"]
    assert "rawbids/sub-02/anat/sub-02_T1w.nii.gz" in entries
    assert "rawbids/sub-03/anat/sub-03_T1w.nii.gz" in entries


@pytest.mark.parametrize("policy", ["no", "only"])
def test_openneuro_sidecars(tmp_path, openneuro, policy):
    lay = on("harvest", tmp_path, "--sub", "01", "--sidecars", policy)
    raw, pristine = lay.raw, lay.source / "openneuro"
    sidecar = "sub-01/anat/sub-01_T1w.json"
    image = "sub-01/anat/sub-01_T1w.nii.gz"
    if policy == "no":
        # sidecars of data files are not fetched at all
        assert not (raw / sidecar).exists()
        assert not (pristine / sidecar).exists()
        assert (raw / image).exists()
    else:
        # metadata only: no image
        assert (raw / sidecar).exists()
        assert not (raw / image).exists()
    # tables and other metadata always
    assert (raw / "sub-01/func/sub-01_task-rest_events.tsv").exists()
    assert (raw / "participants.tsv").exists()
    run = read_manifest(lay.state)["runs"][-2]
    assert run["stage"] == "harvest" and run["sidecars"] == policy


def test_openneuro_rerun_skips_unchanged_subjects(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    task = "roast/hosted/sub-01"
    first = read_task_states(lay.state)[task]
    assert first["status"] == "done"
    sidecar = lay.raw / "sub-01/anat/sub-01_T1w.json"
    mtime = sidecar.stat().st_mtime_ns
    lay = on("harvest", tmp_path, "--sub", "01")
    # same inputs: the shard is skipped (its record is not rewritten)
    assert read_task_states(lay.state)[task]["started"] == first["started"]
    assert sidecar.stat().st_mtime_ns == mtime
    # a new pristine copy (new snapshot) reaches the dataset
    openneuro[f"{DS}/sub-01/anat/sub-01_T1w.json"] = b'{"EchoTime": 0.004}'
    lay = on("harvest", tmp_path, "--sub", "01", "--if-exists",
             "overwrite")
    assert read_json(sidecar) == {"EchoTime": 0.004}


def test_openneuro_roast_on_its_own(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01", "02")
    raw = lay.raw
    sidecar = raw / "sub-01/anat/sub-01_T1w.json"
    other = raw / "sub-02/anat/sub-02_T1w.json"
    sidecar.unlink()
    other.write_text('{"EchoTime": 1}')
    # a roast of one subject: only its files (shards in a process pool)
    on("roast", tmp_path, "--sub", "01", "--jobs", "2")
    assert sidecar.read_bytes() == ON["sub-01/anat/sub-01_T1w.json"]
    assert read_json(other) == {"EchoTime": 1}
    # under "skip", a subject whose inputs did not change is skipped (an
    # edited output is not an input); "different" compares every file
    on("roast", tmp_path)
    assert read_json(other) == {"EchoTime": 1}
    on("roast", tmp_path, "--if-exists", "different")
    assert other.read_bytes() == ON["sub-02/anat/sub-02_T1w.json"]
    assert not ON_REQUESTS.get("GET " + f"{DS}/README", 0) > 1
    # --sidecars no: sidecars are left as they are
    sidecar.unlink()
    on("roast", tmp_path, "--sidecars", "no")
    assert not sidecar.exists()
    run = read_manifest(lay.state)["runs"][-1]
    assert run["stage"] == "roast" and run["json"] == "no"


def test_openneuro_compost_keeps_pristine_metadata(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    pristine = lay.source / "openneuro"
    copies = files(pristine)
    assert copies
    # default compost: source data, but the pristine metadata is kept
    (lay.source / "notes.txt").write_text("x")
    on("compost", tmp_path, "--yes")
    assert not (lay.source / "notes.txt").exists()
    assert files(pristine) == copies
    # explicitly: deleted, and a roast leaves the roasted files alone
    on("compost", tmp_path, "--sourcedata", "--yes")
    assert not pristine.exists()
    on("roast", tmp_path)
    assert (lay.raw / "sub-01/anat/sub-01_T1w.json").exists()
    # the next harvest fetches them again
    on("harvest", tmp_path, "--sub", "01")
    assert files(pristine) == copies


def test_openneuro_compost_re_roasts_what_is_left(tmp_path, openneuro):
    lay = on("harvest", tmp_path)
    raw = lay.raw
    fmap = raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    on("compost", tmp_path, "--exclude", "sub-01/func/*", "--yes")
    assert not (raw / "sub-01/func").exists()
    # the field map no longer points at the composted run
    assert "IntendedFor" not in read_json(fmap)
    # nothing was re-created (sub-01's events, or sub-03 sidecars)
    assert not (raw / "sub-01/func/sub-01_task-rest_events.tsv").exists()
    runs = read_manifest(lay.state)["runs"]
    # roasted between the deletion of BIDS data and that of source data
    assert runs[-2]["stage"] == "roast" and runs[-2]["trigger"] == "compost"
    assert runs[-1]["stage"] == "compost"


def test_openneuro_dry_run(tmp_path, openneuro, capsys):
    bx("openneuro", "harvest", DS, tmp_path, "--source", "s3", "--dry-run",
       "--sub", "01")
    out = capsys.readouterr().out
    assert "sub-01/anat/sub-01_T1w.json  [sourcedata]" in out
    assert "sub-01/anat/sub-01_T1w.nii.gz  [direct]" in out
    assert not (tmp_path / "OpenNeuro").exists()


# ----------------------------------------------------------------------
#   INDI: SALD (pilot, single site) and ACPI (multi-site fill-in)
# ----------------------------------------------------------------------

P = "data/Projects"
SALD = f"{P}/INDI/SALD/RawData_BIDS"
ACPI = f"{P}/ACPI"
INDI = {
    f"{SALD}/dataset_description.json":
        b'{"Name": "SALD", "BIDSVersion": "1.0.2", "Authors": "A B"}',
    f"{SALD}/participants.tsv": tsv(["participant_id", "sex", "age"],
                                    ["031274", "F", "19"],
                                    ["031275", "M", "N/A"]),
    f"{SALD}/task-rest_bold.json":
        b'{"TaskName": "rest", "RepetitionTime": "2"}',
    f"{SALD}/sub-031274/anat/sub-031274_T1w.nii.gz": b"\x01" * 32,
    f"{SALD}/sub-031274/func/sub-031274_task-rest_bold.nii.gz": b"\x02" * 32,
    f"{SALD}/sub-031275/anat/sub-031275_T1w.nii.gz": b"\x03" * 32,
    # ACPI: site-level sidecars, pushed down when sites are merged
    f"{ACPI}/RawDataBIDS/du_1/T1w.json":
        b'{"MagneticFieldStrength": "3T", "EchoTime": "Min Full"}',
    f"{ACPI}/RawDataBIDS/du_1/sub-0028631/ses-1/anat/"
    f"sub-0028631_ses-1_run-1_T1w.nii.gz": b"\x04" * 32,
    f"{ACPI}/RawDataBIDS/nyu_1/participants.tsv": tsv(
        ["participant_id", "age_at_scan_1", "sex", "handedness"],
        ["0028160", "37.77", "Male", "Right"]),
    f"{ACPI}/RawDataBIDS/nyu_1/task-rest_bold.json":
        b'{"RepetitionTime": 2}',
    f"{ACPI}/RawDataBIDS/nyu_1/sub-0028160/ses-1/func/"
    f"sub-0028160_ses-1_task-rest_run-1_bold.nii.gz": b"\x05" * 32,
    f"{ACPI}/PhenotypicData/du_1_phenotypic_data.csv": (
        b'SUB_ID,SESSION,SCANNUM,AGE_AT_SCAN_1,SEX,HANDEDNESS,WEIGHT\n'
        b'"0028631","1","1","22","2","2","61.2"\n'
    ),
}
INDI_REQUESTS = Counter()


@pytest.fixture(scope="module")
def indi_url():
    url, stop = serve(bucket_app("fcp-indi", INDI, INDI_REQUESTS))
    yield url
    stop()


@pytest.fixture
def templates(tmp_path, indi_url, monkeypatch):
    """The project templates, copied to a folder the test may change."""
    monkeypatch.setattr(
        fcp, "get_bucket", lambda: S3Bucket("fcp-indi", endpoint=indi_url))
    INDI_REQUESTS.clear()
    folder = tmp_path / "templates"
    folder.mkdir()
    # `<folder>/<name>`: the templates of each project (`indi/<project>/
    # templates/`), and of the projects of these tests
    for name in CATALOG:
        dataset = load(name)
        if isinstance(dataset, INDIDataset) and dataset.template_dir.is_dir():
            shutil.copytree(dataset.template_dir, folder / name)
    monkeypatch.setattr(Project, "template_dir",
                        property(lambda self: folder / self.name))
    return folder


def test_sald_hybrid_placement_and_fill_in(tmp_path, templates):
    sald = templates / "sald"
    # (this test's own fill-in templates instead of the shipped ones)
    for name in ("T1w.json", "task-rest_bold.json"):
        (sald / name).unlink(missing_ok=True)
    write(sald / "_fill.json", '{"fill": ["T1w.json", "bold.json", '
          '"dwi.json"]}')
    write(sald / "T1w.json", '{"Manufacturer": "Siemens", '
          '"MagneticFieldStrength": 3}')
    write(sald / "bold.json", '{"RepetitionTime": 9, "EchoTime": 0.03}')
    write(sald / "dwi.json", '{"Manufacturer": "Siemens"}')
    out = tmp_path / "data"
    bx("sald", "harvest", out)
    lay = Layout.detect(out / "SALD")
    raw = lay.raw
    pristine = lay.source / "fcp-indi" / "INDI/SALD/RawData_BIDS"
    # images directly; metadata under its bucket key
    assert (raw / "sub-031274/anat/sub-031274_T1w.nii.gz").exists()
    assert (pristine / "task-rest_bold.json").read_bytes() == \
        INDI[f"{SALD}/task-rest_bold.json"]
    # repaired, and filled in (the hosted keys win)
    assert read_json(raw / "task-rest_bold.json") == {
        "TaskName": "rest", "RepetitionTime": 2, "EchoTime": 0.03}
    # a new inherited sidecar for the T1w images; none for dwi (no data)
    assert read_json(raw / "T1w.json") == {
        "Manufacturer": "Siemens", "MagneticFieldStrength": 3}
    assert not (raw / "dwi.json").exists()
    # fill-in templates are not copied as plain templates
    assert not (raw / "_fill.json").exists()
    assert not (raw / "bold.json").exists()
    # curated templates are generated files, repaired tables are trimmed
    assert (raw / "README").exists()
    assert (raw / "participants.tsv").read_text().splitlines() == [
        "participant_id\tsex\tage", "sub-031274\tF\t19",
        "sub-031275\tM\tn/a"]
    entries = read_manifest(lay.state)["files"]
    t1w = entries["rawbids/T1w.json"]
    assert t1w["origin"] == "generated" and t1w["fill"] == ["T1w.json"]
    assert len(t1w["sha256"]) == 64
    bold = entries["rawbids/task-rest_bold.json"]
    assert bold["origin"] == "roasted" and bold["repair"] is True
    assert bold["fill"] == ["bold.json"]
    assert bold["source"] == \
        "sourcedata/fcp-indi/INDI/SALD/RawData_BIDS/task-rest_bold.json"
    assert entries["rawbids/README"]["origin"] == "generated"
    assert entries["rawbids/README"]["source"].startswith(
        ".brainspresso/generated/")
    # a new template reaches the dataset with a roast (no network),
    # under `--if-exists update` (the default `skip` leaves it alone)
    write(sald / "T1w.json", '{"Manufacturer": "Siemens", '
          '"MagneticFieldStrength": 3, "FlipAngle": 9}')
    INDI_REQUESTS.clear()
    before = (raw / "T1w.json").read_bytes()
    bx("sald", "roast", out)
    assert (raw / "T1w.json").read_bytes() == before
    bx("sald", "roast", out, "--if-exists", "update")
    assert read_json(raw / "T1w.json")["FlipAngle"] == 9
    assert not INDI_REQUESTS


def test_acpi_fill_in_by_site(tmp_path, templates):
    acpi = templates / "acpi"
    write(acpi / "_fill.json",
          '{"fill": ["du_1/T1w.json", "*/task-rest_bold.json"]}')
    write(acpi / "du_1/T1w.json", '{"MagneticFieldStrength": 1.5, '
          '"EchoTime": 0.005, "Manufacturer": "GE"}')
    write(acpi / "nyu_1/task-rest_bold.json", '{"RepetitionTime": 9, '
          '"Manufacturer": "Siemens"}')
    # sites merged: site templates go to the subjects of their site,
    # merged into the hosted site sidecars pushed down there
    out = tmp_path / "merged"
    bx("acpi", "harvest", out)
    raw = Layout.detect(out / "ACPI").raw
    assert read_json(raw / "sub-0028631/sub-0028631_T1w.json") == {
        "MagneticFieldStrength": 3, "EchoTimeText": "Min Full",
        "EchoTime": 0.005, "Manufacturer": "GE"}
    assert read_json(raw / "sub-0028160/sub-0028160_task-rest_bold.json") \
        == {"RepetitionTime": 2, "Manufacturer": "Siemens"}
    assert not (raw / "T1w.json").exists()
    assert not (raw / "du_1").exists()
    # one dataset per site: at the top level of the site dataset
    out = tmp_path / "sites"
    bx("acpi", "harvest", out, "--layout", "sites")
    sites = Layout.detect(out / "ACPI")
    du_1 = sites.children["du_1"]
    assert read_json(du_1.raw / "T1w.json") == {
        "MagneticFieldStrength": 3, "EchoTimeText": "Min Full",
        "EchoTime": 0.005, "Manufacturer": "GE"}
    assert not (du_1.raw / "task-rest_bold.json").exists()
    nyu_1 = sites.children["nyu_1"].raw
    assert read_json(nyu_1 / "task-rest_bold.json") == {
        "RepetitionTime": 2, "Manufacturer": "Siemens"}
    # a roast of one site of a sites layout
    (nyu_1 / "task-rest_bold.json").unlink()
    (du_1.raw / "T1w.json").unlink()
    bx("acpi", "roast", out, "--site", "nyu_1")
    assert (nyu_1 / "task-rest_bold.json").exists()
    assert not (du_1.raw / "T1w.json").exists()


def test_indi_pristine_copy_downloaded_once(tmp_path, templates):
    # one site sidecar pushed down to every subject: one download
    bx("acpi", "harvest", tmp_path)
    assert INDI_REQUESTS[f"GET {ACPI}/RawDataBIDS/du_1/T1w.json"] == 1


def test_compost_plan_keeps_pristine_copies(tmp_path):
    root = tmp_path / "ds"
    lay = make_layout("flat", root)
    write(lay.raw / "sub-01/anat/sub-01_T1w.json", "{}")
    write(lay.source / "openneuro/sub-01/anat/sub-01_T1w.json", "{}")
    write(lay.source / "other.tar", "x")
    write(lay.state / "manifest.json", json.dumps({"layout": "flat", "files": {
        "sub-01/anat/sub-01_T1w.json": {
            "origin": "roasted",
            "source": "sourcedata/openneuro/sub-01/anat/sub-01_T1w.json"},
    }}))
    plan = compost(root, yes=True)
    assert plan.sourcedata == ["sourcedata/other.tar"]
    assert plan.pristine == [
        "sourcedata/openneuro/sub-01/anat/sub-01_T1w.json"]
    assert (lay.source / "openneuro/sub-01/anat/sub-01_T1w.json").exists()
    compost(root, sourcedata=True, yes=True)
    assert not (lay.source / "openneuro").exists()


def test_roast_hosted_without_manifest(tmp_path):
    root = tmp_path / "ds"
    (root / "sub-01").mkdir(parents=True)
    report = roast_hosted(root, display=False)
    assert report.status == "complete"
    assert os.path.exists(root / ".brainspresso" / "manifest.json")


def test_missing_pristine_copy(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    pristine = lay.source / "openneuro/sub-01/anat/sub-01_T1w.json"
    sidecar = lay.raw / "sub-01/anat/sub-01_T1w.json"
    pristine.unlink()
    # the roasted file is left as it is
    assert roast_hosted(lay, display=False).status == "complete"
    assert sidecar.exists()
    # without it, the roast reports the missing copy (a roast that
    # checks every file: under "skip", the unchanged shard is skipped)
    sidecar.unlink()
    report = roast_hosted(lay, display=False, ifexists="different")
    assert report.status == "partial"
    assert report.summary()["hosted"]["error"] == 1


def test_hosted_commands():
    def snapshot_flags(name):
        sub = register(load(name))
        return {cmd: command_flags(sub[cmd]) for cmd in ("harvest", "roast")}

    for name in ("openneuro", "sald", "acpi"):
        found = snapshot_flags(name)
        assert found["harvest"]["--sidecars"]["default"] == "yes"
        assert found["roast"]["--sidecars"]["choices"] == ["no", "only",
                                                          "yes"]
    assert "--site" in snapshot_flags("acpi")["roast"]
    assert "--site" not in snapshot_flags("openneuro")["roast"]


def test_fill_templates_come_from_the_dataset_package(tmp_path, templates):
    # any hosted dataset: `<template_dir>/_fill.json` (no fixed path)
    write(tmp_path / "pkg/_fill.json", '{"fill": ["T1w.json"]}')
    write(tmp_path / "pkg/T1w.json", '{"A": 1}')

    class Hosted(load("openneuro").__class__):
        template_dir = tmp_path / "pkg"

    assert [t.id for t in Hosted().fill_templates()] == ["T1w.json"]
    assert load("openneuro").fill_templates() == []
    # INDI: the project's folder (`Project.template_dir`)
    sald = load("sald")
    assert sald.template_dir == templates / "sald"
    write(templates / "sald/_fill.json", '{"fill": ["T1w.json"]}')
    write(templates / "sald/T1w.json", '{"A": 1}')
    assert [t.id for t in sald.fill_templates()] == ["T1w.json"]


def test_edited_files_are_kept_under_update(tmp_path, openneuro, caplog):
    lay = on("harvest", tmp_path, "--sub", "01")
    readme, desc = lay.raw / "README", lay.raw / "dataset_description.json"
    readme.write_text("My notes\n")
    # new pristine copies (e.g. fetched by a later harvest)
    pristine = lay.source / "openneuro"
    (pristine / "README").write_text("New README\n")
    (pristine / "dataset_description.json").write_text('{"Name": "y"}')
    # skip (default): both left alone, nothing said
    old = desc.read_bytes()
    caplog.clear()
    on("roast", tmp_path)
    assert desc.read_bytes() == old and readme.read_text() == "My notes\n"
    assert "was edited" not in caplog.text
    on("roast", tmp_path, "--if-exists", "update")
    # an unedited file follows its pristine copy, an edited one is kept
    assert read_json(desc) == {"Name": "y"}
    assert readme.read_text() == "My notes\n"
    assert "was edited since brainspresso wrote it" in caplog.text
    on("roast", tmp_path, "--if-exists", "overwrite")
    assert readme.read_text() == "New README\n"



# ----------------------------------------------------------------------
#   Review of #27 (round 1)
# ----------------------------------------------------------------------

def manifest_entry(lay, path):
    return read_manifest(lay.state)["files"][
        Path(path).relative_to(lay.root).as_posix()]


def test_edited_fill_in_sidecar_is_kept(tmp_path, templates, caplog):
    sald = templates / "sald"
    for name in ("T1w.json", "task-rest_bold.json"):
        (sald / name).unlink(missing_ok=True)
    write(sald / "_fill.json", '{"fill": ["T1w.json"]}')
    write(sald / "T1w.json", '{"Manufacturer": "Siemens"}')
    out = tmp_path / "data"
    bx("sald", "harvest", out)
    lay = Layout.detect(out / "SALD")
    t1w = lay.raw / "T1w.json"
    assert manifest_entry(lay, t1w)["sha256"] == sha256(t1w.read_bytes())
    t1w.write_text('{"Manufacturer": "GE"}\n')
    caplog.clear()
    bx("sald", "roast", out)
    assert read_json(t1w) == {"Manufacturer": "GE"}
    assert "was edited" not in caplog.text          # (skip: silent)
    bx("sald", "roast", out, "--if-exists", "update")
    assert read_json(t1w) == {"Manufacturer": "GE"}
    assert "was edited since brainspresso wrote it: kept" in caplog.text


def test_datasets_without_hashes(tmp_path, openneuro, caplog):
    """A manifest without hashes: a file that equals the roasted output
    is adopted, any other is backed up, then replaced."""
    lay = on("harvest", tmp_path, "--sub", "01")
    files = read_manifest(lay.state)["files"]
    update_files(lay, {k: {"sha256": None} for k in files})
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    t1w.write_text('{"EchoTime": 1}\n')
    readme = lay.raw / "README"
    caplog.clear()
    on("roast", tmp_path, "--sub", "01")       # (skip: left alone)
    assert read_json(t1w) == {"EchoTime": 1}
    assert "--if-exists" not in caplog.text
    assert "sha256" not in manifest_entry(lay, readme)
    on("roast", tmp_path, "--sub", "01", "--if-exists", "update")
    assert read_json(t1w) != {"EchoTime": 1}
    assert (lay.state / "replaced/rawbids/sub-01/anat/sub-01_T1w.json"
            ).read_text() == '{"EchoTime": 1}\n'
    assert manifest_entry(lay, t1w)["sha256"] == sha256(t1w.read_bytes())
    # unedited: adopted (its hash recorded)
    assert manifest_entry(lay, readme)["sha256"] == \
        sha256(readme.read_bytes())


def test_later_harvest_re_trims_field_maps(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01", "--datatype", "anat",
             "fmap")
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    assert "IntendedFor" not in read_json(fmap)        # no func yet
    assert table_rows(lay.raw / "sub-01/sub-01_scans.tsv") == [
        "anat/sub-01_T1w.nii.gz"]
    on("harvest", tmp_path, "--sub", "01", "--datatype", "func")
    assert read_json(fmap)["IntendedFor"] == [
        "func/sub-01_task-rest_bold.nii.gz"]
    assert table_rows(lay.raw / "sub-01/sub-01_scans.tsv") == [
        "anat/sub-01_T1w.nii.gz", "func/sub-01_task-rest_bold.nii.gz"]


def table_rows(path):
    return [line.split("\t")[0]
            for line in Path(path).read_text().splitlines()[1:]]


def test_sidecars_only_trims_against_the_selection(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01", "--sidecars", "only")
    raw = lay.raw
    assert not (raw / "sub-01/func/sub-01_task-rest_bold.nii.gz").exists()
    fmap = raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    rows = ["anat/sub-01_T1w.nii.gz", "func/sub-01_task-rest_bold.nii.gz"]
    for _ in range(2):      # the harvest, then a roast on its own
        # targets that were selected count, those not in the dataset not
        assert read_json(fmap)["IntendedFor"] == [
            "func/sub-01_task-rest_bold.nii.gz"]
        assert table_rows(raw / "sub-01/sub-01_scans.tsv") == rows
        on("roast", tmp_path, "--if-exists", "different")
    entry = manifest_entry(lay, raw / "sub-01/anat/sub-01_T1w.nii.gz")
    assert entry["fetched"] is False
    # fetched later: an ordinary file
    on("harvest", tmp_path, "--sub", "01")
    entry = manifest_entry(lay, raw / "sub-01/anat/sub-01_T1w.nii.gz")
    assert "fetched" not in entry
    # a compost forgets the unfetched files it drops
    lay = on("harvest", tmp_path, "--sub", "02", "--sidecars", "only")
    on("compost", tmp_path, "--exclude-sub", "02", "--yes")
    files = read_manifest(lay.state)["files"]
    assert not any(k.startswith("rawbids/sub-02") for k in files)


WRITER = """
import sys
from brainspresso.bids.harvest import update_files
from brainspresso.bids.layout import Layout
layout = Layout.detect(sys.argv[1])
for i in range(40):
    update_files(layout, {f"{sys.argv[2]}/{i}": {"origin": "direct"}})
"""


def test_concurrent_manifest_updates(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    procs = [subprocess.Popen([sys.executable, "-c", WRITER, str(lay.root),
                               name], cwd=Path(__file__).parents[1])
             for name in ("a", "b")]
    # while they write: a roast, which records its files and run
    on("roast", tmp_path, "--if-exists", "different")
    assert [p.wait(60) for p in procs] == [0, 0]
    manifest = read_manifest(lay.state)
    for name in ("a", "b"):
        assert all(f"{name}/{i}" in manifest["files"] for i in range(40))
    assert manifest["runs"][-1]["stage"] == "roast"
    assert not list(Path(lay.state).glob(".manifest.json.*.part"))


def test_repaired_top_level_keys_are_never_pushed_down(tmp_path):
    lay = make_layout("flat", tmp_path / "ds")
    write(lay.source / "x/inv-1_MP2RAGE.json", '{"RepetitionTime": 5}')
    write(lay.raw / "sub-A1/anat/sub-A1_inv-1_MP2RAGE.nii.gz", "x")
    write(lay.state / "manifest.json", json.dumps({"layout": "flat",
                                                   "files": {
        "inv-1_MP2RAGE.json": {"origin": "roasted", "repair": True,
                               "source": "sourcedata/x/inv-1_MP2RAGE.json"},
        "sub-A1/anat/sub-A1_inv-1_MP2RAGE.nii.gz": {"origin": "direct",
                                                    "site": "A"},
    }}))
    fill = [FillTemplate("MP2RAGE.json",
                         {"RepetitionTimePreparation": 9, "FlipAngle": 4},
                         "A")]
    roast_hosted(lay, fill=fill, display=False)
    # repair turned RepetitionTime into RepetitionTimePreparation: the
    # hosted value must not be overridden from below
    assert read_json(lay.raw / "inv-1_MP2RAGE.json") == {
        "RepetitionTimePreparation": 5}
    assert read_json(lay.raw / "sub-A1/sub-A1_MP2RAGE.json") == {
        "FlipAngle": 4}


class Coded(Project):
    """Two sites whose labels get a site code (remapped names)."""

    name = "coded"
    folder = "CODED"
    title = "Coded"
    sites = (Site("A", "CODED/A", "A"), Site("B", "CODED/B", "B"))


CODED = f"{P}/CODED"
INDI.update({
    f"{CODED}/A/sub-01/fmap/sub-01_magnitude1.nii.gz": b"\x06" * 32,
    f"{CODED}/A/sub-01/fmap/sub-01_magnitude1.json":
        b'{"EchoTime": "0.00492"}',
    f"{CODED}/A/sub-01/fmap/sub-01_phasediff.nii.gz": b"\x07" * 32,
    f"{CODED}/A/sub-01/fmap/sub-01_phasediff.json": b'{"EchoTime": 0.00738}',
    f"{CODED}/B/sub-01/anat/sub-01_T1w.nii.gz": b"\x08" * 32,
    f"{CODED}/B/sub-02/anat/sub-02_T1w.nii.gz": b"\x0c" * 32,
    # a single site whose run folders become `run-<n>` (fcon1000-like)
    f"{P}/RUNS/raw/sub-01/func_2/sub-01_task-rest_bold.nii.gz":
        b"\x0d" * 32,
    f"{P}/RUNS/raw/sub-01/fmap/sub-01_epi.nii.gz": b"\x0e" * 32,
    f"{P}/RUNS/raw/sub-01/fmap/sub-01_epi.json":
        b'{"IntendedFor": "func_2/sub-01_task-rest_bold.nii.gz"}',
    f"{P}/RUNS/raw/sub-01/sub-01_scans.tsv": tsv(
        ["filename", "acq_time"],
        ["func_2/sub-01_task-rest_bold.nii.gz", "n/a"]),
    # paths in the hosted metadata, with the labels of the bucket
    f"{CODED}/A/sub-01/func/sub-01_task-rest_bold.nii.gz": b"\x09" * 32,
    f"{CODED}/A/sub-01/fmap/sub-01_dir-AP_epi.nii.gz": b"\x0a" * 32,
    f"{CODED}/A/sub-01/fmap/sub-01_dir-AP_epi.json": json.dumps({
        "IntendedFor": ["func/sub-01_task-rest_bold.nii.gz",
                        "bids::sub-01/func/sub-01_task-rest_bold.nii.gz",
                        "dwi/sub-01_dwi.nii.gz"],
        "B0FieldIdentifier": "pepolar"}).encode(),
    f"{CODED}/A/sub-01/sub-01_scans.tsv": tsv(
        ["filename", "acq_time"],
        ["func/sub-01_task-rest_bold.nii.gz", "n/a"],
        ["dwi/sub-01_dwi.nii.gz", "n/a"]),
    # NKI-RS (single site): a hosted top-level sidecar
    f"{P}/RocklandSample/RawDataBIDSLatest/participants.tsv": tsv(
        ["participant_id", "age"], ["sub-A0001", "30"]),
    f"{P}/RocklandSample/RawDataBIDSLatest/T1w.json":
        b'{"EchoTime": "0.00247", "RepetitionTime": 2.5}',
    f"{P}/RocklandSample/RawDataBIDSLatest/sub-A0001/ses-BAS1/anat/"
    f"sub-A0001_ses-BAS1_T1w.nii.gz": b"\x0b" * 32,
})


def test_phasediff_of_a_remapped_subject(tmp_path, templates):
    INDIDataset(Coded()).execute("harvest", path=tmp_path)
    lay = Layout.detect(tmp_path / "CODED")
    fmap = lay.raw / "sub-A01/fmap"
    assert read_json(fmap / "sub-A01_phasediff.json") == {
        "EchoTime": 0.00738, "EchoTime1": 0.00492, "EchoTime2": 0.00738}
    # the magnitude is read from its pristine copy (named `sub-01_...`),
    # not from the local file
    (fmap / "sub-A01_magnitude1.json").unlink()
    item, = (i for i in _plan(lay, {
        k: v for k, v in read_manifest(lay.state)["files"].items()
        if v.get("source")})[0] if i["key"].endswith("_phasediff.json"))
    assert Path(item["sibling"]).name == "sub-01_magnitude1.json"
    assert json.loads(RoastMetadata(item).render())["EchoTime1"] == 0.00492


@pytest.mark.parametrize("order", ["together", "sourcedata first"])
def test_compost_re_roasts_before_pristine_copies_go(tmp_path, openneuro,
                                                     order):
    lay = on("harvest", tmp_path)
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    if order == "together":
        on("compost", tmp_path, "--exclude", "sub-01/func/*",
           "--sourcedata", "--yes")
    else:
        on("compost", tmp_path, "--sourcedata", "--yes")
        on("compost", tmp_path, "--exclude", "sub-01/func/*", "--yes")
    assert not (lay.source / "openneuro").exists()
    assert not (lay.raw / "sub-01/func").exists()
    if order == "together":
        assert "IntendedFor" not in read_json(fmap)
    else:
        # no base: the field map is frozen (the accepted cost) until the
        # next harvest fetches its pristine copy again
        assert read_json(fmap) == {
            "IntendedFor": ["func/sub-01_task-rest_bold.nii.gz"]}
        on("harvest", tmp_path, "--sub", "01", "--datatype", "anat",
           "fmap")
        assert "IntendedFor" not in read_json(fmap)


def test_stale_fill_list_is_removed(tmp_path, templates):
    sald = templates / "sald"
    write(sald / "_fill.json", '{"fill": ["task-rest_bold.json"]}')
    out = tmp_path / "data"
    bx("sald", "harvest", out)
    lay = Layout.detect(out / "SALD")
    bold = lay.raw / "task-rest_bold.json"
    assert manifest_entry(lay, bold)["fill"] == ["task-rest_bold.json"]
    write(sald / "_fill.json", '{"fill": []}')
    bx("sald", "roast", out, "--if-exists", "different")
    assert "fill" not in manifest_entry(lay, bold)



# ----------------------------------------------------------------------
#   Review of #27 (round 2) and owner decisions
# ----------------------------------------------------------------------

def test_paths_in_hosted_metadata_are_relabelled(tmp_path, templates):
    """Owner (Q1): paths are remapped as the images are, then trimmed."""
    INDIDataset(Coded()).execute("harvest", path=tmp_path)
    raw = Layout.detect(tmp_path / "CODED").raw
    epi = read_json(raw / "sub-A01/fmap/sub-A01_dir-AP_epi.json")
    assert epi["IntendedFor"] == [
        "func/sub-A01_task-rest_bold.nii.gz",
        "bids::sub-A01/func/sub-A01_task-rest_bold.nii.gz"]
    assert epi["B0FieldIdentifier"] == "pepolar"   # an identifier
    assert table_rows(raw / "sub-A01/sub-A01_scans.tsv") == [
        "func/sub-A01_task-rest_bold.nii.gz"]


def test_no_trim_restores_the_full_table(tmp_path, openneuro):
    """Owner (Q5): trimming always starts from the pristine copy."""
    lay = on("harvest", tmp_path, "--sub", "01")
    table = lay.raw / "participants.tsv"
    assert table_rows(table) == ["sub-01"]
    on("harvest", tmp_path, "--sub", "01", "--no-trim")
    assert table.read_bytes() == ON["participants.tsv"]
    assert not lay.original(table).exists()     # (hosted: no `orig/`)
    on("roast", tmp_path)                       # stays untrimmed
    assert table.read_bytes() == ON["participants.tsv"]
    on("harvest", tmp_path, "--sub", "01")     # trimmed again
    assert table_rows(table) == ["sub-01"]


def test_skipped_shard_never_adopts_an_edit(tmp_path, openneuro, caplog):
    lay = on("harvest", tmp_path, "--sub", "01")
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    recorded = manifest_entry(lay, t1w)["sha256"]
    t1w.write_text('{"EchoTime": 1}\n')
    on("roast", tmp_path)                       # skipped: unchanged
    assert manifest_entry(lay, t1w)["sha256"] == recorded
    (lay.source / "openneuro/sub-01/anat/sub-01_T1w.json").write_text(
        '{"EchoTime": 0.005}')
    caplog.clear()
    on("roast", tmp_path)                       # skip: left alone
    assert read_json(t1w) == {"EchoTime": 1}
    assert "was edited" not in caplog.text
    on("roast", tmp_path, "--if-exists", "update")
    assert read_json(t1w) == {"EchoTime": 1}
    assert "was edited since brainspresso wrote it" in caplog.text
    assert manifest_entry(lay, t1w)["sha256"] == recorded


def test_edited_field_map_without_pristine_copy(tmp_path, openneuro):
    lay = on("harvest", tmp_path)
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    recorded = manifest_entry(lay, fmap)["sha256"]
    on("compost", tmp_path, "--sourcedata", "--yes")
    fmap.write_text(json.dumps({**read_json(fmap), "Note": "mine"}))
    on("compost", tmp_path, "--exclude", "sub-01/func/*", "--yes")
    # no base: frozen (the edit kept as it is, not adopted)
    assert read_json(fmap) == {
        "IntendedFor": ["func/sub-01_task-rest_bold.nii.gz"], "Note": "mine"}
    assert manifest_entry(lay, fmap)["sha256"] == recorded


@pytest.mark.parametrize("with_pristine", [True, False])
def test_edited_field_map_after_compost(tmp_path, openneuro, with_pristine):
    lay = on("harvest", tmp_path)
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    if not with_pristine:
        on("compost", tmp_path, "--sourcedata", "--yes")
    fmap.write_text(json.dumps({**read_json(fmap), "Note": "mine"}))
    on("compost", tmp_path, "--exclude", "sub-01/func/*", "--yes")
    if with_pristine:
        # the user's keys, `IntendedFor` from the render
        assert read_json(fmap) == {"Note": "mine"}
    else:                                   # (frozen)
        assert read_json(fmap)["Note"] == "mine"
        assert "IntendedFor" in read_json(fmap)


def test_dataset_without_hashes_gets_its_fill_ins(tmp_path, templates,
                                                  caplog):
    """No hashes, no fill-in: backed up, then replaced by the merge."""
    nki = templates / "nki-rs"
    aside = tmp_path / "aside"
    aside.mkdir()
    fill = json.loads((nki / "_fill.json").read_text())["fill"]
    for name in ["_fill.json", *fill]:
        shutil.move(nki / name, aside / name)
    out = tmp_path / "data"
    bx("nki-rs", "harvest", out)
    lay = Layout.detect(out / "NKI-RS")
    t1w = lay.raw / "T1w.json"
    assert read_json(t1w) == {"EchoTime": 0.00247, "RepetitionTime": 2.5}
    update_files(lay, {k: {"sha256": None} for k in
                       read_manifest(lay.state)["files"]})
    for name in ["_fill.json", *fill]:
        shutil.move(aside / name, nki / name)
    old = t1w.read_bytes()
    caplog.clear()
    bx("nki-rs", "harvest", out)
    # (skip: an existing file is left alone; a roast with `update` merges)
    assert t1w.read_bytes() == old and "WARNING" not in caplog.text
    bx("nki-rs", "roast", out, "--if-exists", "update")
    merged = read_json(t1w)
    assert merged["EchoTime"] == 0.00247 and merged["RepetitionTime"] == 2.5
    template = read_json(nki / "T1w.json")
    assert set(template) <= set(merged)
    assert manifest_entry(lay, t1w)["fill"] == ["T1w.json"]
    assert (lay.state / "replaced/rawbids/T1w.json").read_bytes() == old
    assert "replaced, the old file is in" in caplog.text


@pytest.mark.parametrize("edited", [False, True])
def test_fill_in_sidecar_whose_template_is_gone(tmp_path, templates, caplog,
                                                edited):
    sald = templates / "sald"
    for name in ("T1w.json", "task-rest_bold.json"):
        (sald / name).unlink(missing_ok=True)
    write(sald / "_fill.json", '{"fill": ["T1w.json"]}')
    write(sald / "T1w.json", '{"Manufacturer": "Siemens"}')
    out = tmp_path / "data"
    bx("sald", "harvest", out)
    lay = Layout.detect(out / "SALD")
    t1w = lay.raw / "T1w.json"
    if edited:
        t1w.write_text('{"Manufacturer": "GE"}\n')
    write(sald / "_fill.json", '{"fill": []}')
    (sald / "T1w.json").unlink()
    caplog.clear()
    bx("sald", "roast", out)                    # skip: nothing removed
    assert t1w.exists() and "no longer applies" not in caplog.text
    assert read_manifest(lay.state)["files"]["rawbids/T1w.json"]["fill"]
    bx("sald", "roast", out, "--if-exists", "update")
    files = read_manifest(lay.state)["files"]
    if edited:
        assert read_json(t1w) == {"Manufacturer": "GE"}
        assert "no longer applies" in caplog.text
        assert "fill" not in files["rawbids/T1w.json"]
    else:
        assert not t1w.exists()
        assert "rawbids/T1w.json" not in files


def test_top_level_sidecar_change_reaches_pushed_down_fill(tmp_path):
    lay = make_layout("flat", tmp_path / "ds")
    pristine = write(lay.source / "x/inv-1_MP2RAGE.json",
                     '{"RepetitionTime": 5}')
    write(lay.raw / "sub-A1/anat/sub-A1_inv-1_MP2RAGE.nii.gz", "x")
    write(lay.state / "manifest.json", json.dumps({"layout": "flat",
                                                   "files": {
        "inv-1_MP2RAGE.json": {"origin": "roasted", "repair": True,
                               "source": "sourcedata/x/inv-1_MP2RAGE.json"},
        "sub-A1/anat/sub-A1_inv-1_MP2RAGE.nii.gz": {"origin": "direct",
                                                    "site": "A"},
    }}))
    fill = [FillTemplate("MP2RAGE.json",
                         {"RepetitionTimePreparation": 9, "FlipAngle": 4},
                         "A")]
    pushed = lay.raw / "sub-A1/sub-A1_MP2RAGE.json"
    roast_hosted(lay, fill=fill, display=False)
    assert read_json(pushed) == {"FlipAngle": 4}
    # the hosted top-level sidecar now gives FlipAngle, not the TR
    pristine.write_text('{"FlipAngle": 7}')
    roast_hosted(lay, fill=fill, display=False)
    assert read_json(pushed) == {"FlipAngle": 4}        # (skip)
    roast_hosted(lay, fill=fill, ifexists="update", display=False)
    assert read_json(pushed) == {"RepetitionTimePreparation": 9}


def test_forgotten_unfetched_files_re_trim_field_maps(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01", "--sidecars", "only")
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    assert read_json(fmap)["IntendedFor"] == [
        "func/sub-01_task-rest_bold.nii.gz"]
    # nothing on disk to delete: only an unfetched file to forget
    on("compost", tmp_path, "--exclude", "sub-01/func/*_bold.nii.gz",
       "--yes")
    assert "rawbids/sub-01/func/sub-01_task-rest_bold.nii.gz" not in \
        read_manifest(lay.state)["files"]
    assert "IntendedFor" not in read_json(fmap)
    assert table_rows(lay.raw / "sub-01/sub-01_scans.tsv") == [
        "anat/sub-01_T1w.nii.gz"]


def test_directly_placed_generated_files_under_skip_and_different(
    tmp_path, caplog
):
    """Pure skip: a generated file placed directly is left alone once it
    exists; `--if-exists different` replaces it (edits included)"""
    root = tmp_path / "ds"
    selection = Selection(folders=["sourcedata"])

    def harvest(text, if_exists="skip"):
        harvest_hosted([InlineEntry("sourcedata/notes.txt", text)], root,
                       selection, folder="x", display=False,
                       if_exists=if_exists)

    harvest("v1")
    lay = Layout.detect(root)
    notes = lay.source / "notes.txt"
    assert manifest_entry(lay, notes)["sha256"] == sha256(b"v1")
    caplog.clear()
    harvest("v2")                       # skip: left alone, silently
    assert notes.read_text() == "v1"
    assert manifest_entry(lay, notes)["sha256"] == sha256(b"v1")
    assert "refreshed" not in caplog.text and "edited?" not in caplog.text
    harvest("v2", "different")          # unedited: replaced
    assert notes.read_text() == "v2"
    assert manifest_entry(lay, notes)["sha256"] == sha256(b"v2")
    notes.write_text("mine")
    caplog.clear()
    harvest("v3")                       # edited, skip: left alone
    assert notes.read_text() == "mine"
    assert "WARNING" not in caplog.text
    assert manifest_entry(lay, notes)["sha256"] == sha256(b"v2")
    harvest("v3", "different")          # edited: replaced too
    assert notes.read_text() == "v3"
    assert manifest_entry(lay, notes)["sha256"] == sha256(b"v3")


def test_read_manifest_that_is_not_utf8(tmp_path, caplog):
    state = tmp_path / ".brainspresso"
    state.mkdir()
    (state / "manifest.json").write_bytes(b'{"x": "\xe9"}')
    assert read_manifest(state) is None
    assert "Could not parse" in caplog.text



# ----------------------------------------------------------------------
#   Review of #27 (round 3)
# ----------------------------------------------------------------------

class Runs(Project):
    """One site; run folders (`func_2/`) become `run-<n>` entities."""

    name = "runs"
    folder = "RUNS"
    title = "Runs"
    sites = (Site("", "RUNS/raw"),)

    def remap(self, items):
        for path, it in super().remap(items):
            yield re.sub(r"/func_(\d+)/(sub-[^_]+)_task-rest_bold",
                         r"/func/\2_task-rest_run-\1_bold", path), it


def test_unrenamed_files_point_at_renamed_runs(tmp_path, templates):
    INDIDataset(Runs()).execute("harvest", path=tmp_path)
    raw = Layout.detect(tmp_path / "RUNS").raw
    assert (raw / "sub-01/func/sub-01_task-rest_run-2_bold.nii.gz").exists()
    # the field map and the scans table kept their names
    assert read_json(raw / "sub-01/fmap/sub-01_epi.json") == {
        "IntendedFor": ["func/sub-01_task-rest_run-2_bold.nii.gz"]}
    assert table_rows(raw / "sub-01/sub-01_scans.tsv") == [
        "func/sub-01_task-rest_run-2_bold.nii.gz"]


def test_no_trim_without_pristine_copy_keeps_every_row(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    table = lay.raw / "participants.tsv"
    on("compost", tmp_path, "--sourcedata", "--yes")
    # the pristine table cannot be fetched again
    ON_FAIL.add(f"{DS}/participants.tsv")
    bx("openneuro", "harvest", DS, tmp_path, "--source", "s3", "--sub",
       "01", "--no-trim")
    # no base: the table is frozen (nothing written from a partial copy)
    assert table_rows(table) == ["sub-01"]
    ON_FAIL.clear()
    on("harvest", tmp_path, "--sub", "01", "--no-trim")
    assert table.read_bytes() == ON["participants.tsv"]


def test_orphans_outside_the_selection_are_kept_for_later(tmp_path,
                                                          templates):
    coded = templates / "coded"
    write(coded / "_fill.json", '{"fill": ["B/T1w.json"]}')
    write(coded / "B/T1w.json", '{"Manufacturer": "GE"}')
    dataset = INDIDataset(Coded())
    dataset.execute("harvest", path=tmp_path)
    lay = Layout.detect(tmp_path / "CODED")
    one = lay.raw / "sub-B01/sub-B01_T1w.json"
    two = lay.raw / "sub-B02/sub-B02_T1w.json"
    assert read_json(one) == read_json(two) == {"Manufacturer": "GE"}
    (coded / "_fill.json").unlink()
    (coded / "B/T1w.json").unlink()
    roast_hosted(lay, display=False)            # skip: nothing removed
    assert one.exists() and two.exists()
    roast_hosted(lay, selection=Selection(sub=["B01"]), ifexists="update",
                 display=False)
    assert not one.exists() and two.exists()
    assert manifest_entry(lay, two)["fill"] == ["B/T1w.json"]
    roast_hosted(lay, ifexists="update", display=False)
    assert not two.exists()
    assert "rawbids/sub-B02/sub-B02_T1w.json" not in \
        read_manifest(lay.state)["files"]


def test_field_maps_without_hashes_with_remapped_targets(tmp_path,
                                                         templates, caplog):
    INDIDataset(Coded()).execute("harvest", path=tmp_path)
    lay = Layout.detect(tmp_path / "CODED")
    epi = lay.raw / "sub-A01/fmap/sub-A01_dir-AP_epi.json"
    relabelled = epi.read_bytes()
    # what an older harvest wrote: paths not remapped, so all trimmed
    write(epi, '{\n  "B0FieldIdentifier": "pepolar"\n}\n')
    update_files(lay, {k: {"sha256": None} for k in
                       read_manifest(lay.state)["files"]})
    caplog.clear()
    old = epi.read_bytes()
    roast_hosted(lay, selection=Selection(sub=["A01"]), display=False)
    assert "has no recorded hash" in caplog.text
    assert epi.read_bytes() == relabelled
    assert (lay.state / "replaced/rawbids/sub-A01/fmap/"
            "sub-A01_dir-AP_epi.json").read_bytes() == old


def test_old_tables_without_copies_are_left_alone(tmp_path, openneuro):
    lay = on("harvest", tmp_path)
    scans = lay.raw / "sub-01/sub-01_scans.tsv"
    # no pristine copy any more: frozen, not an error
    on("compost", tmp_path, "--sourcedata", "--yes")
    on("compost", tmp_path, "--exclude-sub", "03", "--yes")
    runs = read_manifest(lay.state)["runs"]
    assert runs[-2]["stage"] == "roast" and runs[-2]["status"] == "complete"
    assert scans.exists()



# ----------------------------------------------------------------------
#   Owner decision Q2: repair on by default, `--repair/--no-repair`
# ----------------------------------------------------------------------

def test_openneuro_metadata_repaired_by_default(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "03")
    t1w = lay.raw / "sub-03/anat/sub-03_T1w.json"
    assert read_json(t1w) == {"MagneticFieldStrength": 3}
    assert manifest_entry(lay, t1w)["repair"] is True
    # the pristine copy is never changed
    assert (lay.source / "openneuro/sub-03/anat/sub-03_T1w.json"
            ).read_bytes() == ON["sub-03/anat/sub-03_T1w.json"]


def test_no_repair_keeps_the_pristine_content(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01", "03", "--no-repair")
    t1w = lay.raw / "sub-03/anat/sub-03_T1w.json"
    assert t1w.read_bytes() == ON["sub-03/anat/sub-03_T1w.json"]
    assert "repair" not in manifest_entry(lay, t1w)
    # trimming still applies
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    assert read_json(fmap)["IntendedFor"] == [
        "func/sub-01_task-rest_bold.nii.gz"]
    # a roast repairs (the default) the files it writes: under `skip`
    # an existing file stays as it is, `update` repairs it and records
    # it; `--no-repair` restores the pristine content (unedited files)
    on("roast", tmp_path)
    assert t1w.read_bytes() == ON["sub-03/anat/sub-03_T1w.json"]
    on("roast", tmp_path, "--if-exists", "update")
    assert read_json(t1w) == {"MagneticFieldStrength": 3}
    assert manifest_entry(lay, t1w)["repair"] is True
    on("roast", tmp_path, "--no-repair", "--if-exists", "update")
    assert t1w.read_bytes() == ON["sub-03/anat/sub-03_T1w.json"]
    assert "repair" not in manifest_entry(lay, t1w)


def test_repair_from_a_config_file(tmp_path, openneuro):
    write(tmp_path / ".brainspresso/config.toml",
          "[harvest]\nrepair = false\n")
    lay = on("harvest", tmp_path, "--sub", "03")
    t1w = lay.raw / "sub-03/anat/sub-03_T1w.json"
    assert t1w.read_bytes() == ON["sub-03/anat/sub-03_T1w.json"]


# ----------------------------------------------------------------------
#   State model of #27 (Fable review): probes as tests
# ----------------------------------------------------------------------

def test_edited_tables_are_kept(tmp_path, openneuro, caplog):
    """B1: tables are ordinary roasted files, with the edit rule."""
    lay = on("harvest", tmp_path, "--sub", "01")
    table = lay.raw / "participants.tsv"
    table.write_bytes(b"participant_id\tage\tnote\nsub-01\t30\tmine\n")
    scans = lay.raw / "sub-01/sub-01_scans.tsv"
    edited = scans.read_bytes().replace(b"n/a", b"2020-01-01")
    scans.write_bytes(edited)
    caplog.clear()
    on("roast", tmp_path, "--if-exists", "skip", "--sub", "01")
    assert table.read_bytes() == \
        b"participant_id\tage\tnote\nsub-01\t30\tmine\n"
    assert scans.read_bytes() == edited
    assert caplog.text.count("was edited") == 2


def test_no_trim_tables_survive_a_compost(tmp_path, openneuro, caplog):
    lay = on("harvest", tmp_path, "--no-trim", "--sub", "01", "02")
    table = lay.raw / "participants.tsv"
    full = ["sub-01", "sub-02", "sub-03"]
    assert table_rows(table) == full
    on("compost", tmp_path, "--sub", "01", "--yes")
    assert not (lay.raw / "sub-02").exists()
    assert table_rows(table) == full            # (not trimmed)
    on("harvest", tmp_path, "--no-trim", "--sub", "01")
    assert table_rows(table) == full
    assert "was edited" not in caplog.text


def test_unrepaired_dataset_without_hashes(tmp_path, openneuro, caplog):
    lay = on("harvest", tmp_path, "--no-repair")
    t1w = lay.raw / "sub-03/anat/sub-03_T1w.json"
    pristine = ON["sub-03/anat/sub-03_T1w.json"]
    assert t1w.read_bytes() == pristine
    # an older harvest: no hashes
    update_files(lay, {k: {"sha256": None, "repair": None} for k in
                       read_manifest(lay.state)["files"]})
    caplog.clear()
    on("harvest", tmp_path)                     # skip: left alone
    assert t1w.read_bytes() == pristine
    assert "has no recorded hash" not in caplog.text
    on("roast", tmp_path, "--if-exists", "update")
    assert "has no recorded hash" in caplog.text
    assert read_json(t1w) == {"MagneticFieldStrength": 3}
    assert (lay.state / "replaced/rawbids/sub-03/anat/sub-03_T1w.json"
            ).read_bytes() == pristine


def test_field_map_without_hash_that_grows(tmp_path, openneuro, caplog):
    lay = on("harvest", tmp_path, "--sub", "01", "--datatype", "anat",
             "fmap")
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    old = fmap.read_bytes()
    update_files(lay, {k: {"sha256": None} for k in
                       read_manifest(lay.state)["files"]})
    caplog.clear()
    on("harvest", tmp_path, "--sub", "01")
    # (a file without a hash that differs: backed up, then replaced)
    assert fmap.read_bytes() != old
    assert (lay.state / "replaced/rawbids/sub-01/fmap/"
            "sub-01_dir-AP_epi.json").read_bytes() == old
    assert "has no recorded hash" in caplog.text


def test_push_down_without_the_top_level_base(tmp_path):
    """B2: keys of a top-level sidecar whose base is gone."""
    lay = make_layout("flat", tmp_path / "ds")
    pristine = write(lay.source / "x/T1w.json", '{"FlipAngle": 7}')
    write(lay.raw / "sub-A1/anat/sub-A1_T1w.nii.gz", "x")
    write(lay.state / "manifest.json", json.dumps({"layout": "flat",
                                                   "files": {
        "T1w.json": {"origin": "roasted", "source": "sourcedata/x/T1w.json"},
        "sub-A1/anat/sub-A1_T1w.nii.gz": {"origin": "direct", "site": "A"},
    }}))
    fill = [FillTemplate("T1w.json", {"FlipAngle": 4, "EchoTime": 1}, "A")]
    top, pushed = lay.raw / "T1w.json", lay.raw / "sub-A1/sub-A1_T1w.json"
    roast_hosted(lay, fill=fill, display=False)
    assert read_json(pushed) == {"EchoTime": 1}
    assert read_json(top) == {"FlipAngle": 7}
    pristine.unlink()                       # (compost --sourcedata)
    roast_hosted(lay, fill=fill, display=False, ifexists="different")
    # the keys come from the visible file: FlipAngle still not pushed
    assert read_json(pushed) == {"EchoTime": 1}
    assert read_json(top) == {"FlipAngle": 7}          # (frozen)
    # neither readable: no push-down at all (never "no keys")
    top.write_text("not JSON")
    pushed.unlink()
    roast_hosted(lay, fill=fill, display=False, ifexists="different")
    assert not pushed.exists()


def test_flat_dataset_compost_then_roast(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--layout", "flat")
    on("compost", tmp_path, "--sub", "01", "--yes")
    pristine = lay.source / "openneuro"
    assert (pristine / "sub-01/anat/sub-01_T1w.json").exists()
    on("roast", tmp_path)
    assert read_manifest(lay.state)["runs"][-1]["status"] == "complete"
    assert table_rows(lay.raw / "participants.tsv") == ["sub-01"]


def test_harvest_merges_entries_and_keeps_edits(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    entry = manifest_entry(lay, t1w)
    t1w.write_text('{"EchoTime": 1}\n')
    on("harvest", tmp_path, "--sub", "01", "02")
    assert read_json(t1w) == {"EchoTime": 1}
    # merged: the hash only the roast writes is still the one it wrote
    assert manifest_entry(lay, t1w) == entry
    (lay.raw / "README").unlink()
    on("roast", tmp_path)
    assert (lay.raw / "README").read_bytes() == ON["README"]


def test_restricted_roast_leaves_other_files_alone(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    t1w.unlink()
    on("roast", tmp_path, "--ses", "9")
    assert not t1w.exists()
    on("roast", tmp_path)
    assert t1w.exists()


def test_edited_field_map_round_trip(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    fmap.write_text(json.dumps({**read_json(fmap), "Note": "mine"}))
    on("compost", tmp_path, "--exclude", "sub-01/func/*", "--yes")
    assert read_json(fmap) == {"Note": "mine"}
    on("harvest", tmp_path, "--sub", "01")
    assert read_json(fmap) == {
        "Note": "mine", "IntendedFor": ["func/sub-01_task-rest_bold.nii.gz"]}


def test_pristine_copies_follow_the_listing(tmp_path, openneuro):
    """S4: a new snapshot of a pristine copy, under `--if-exists skip`."""
    lay = on("harvest", tmp_path, "--sub", "01")
    openneuro[f"{DS}/participants.tsv"] = \
        ON["participants.tsv"] + b"sub-04\t60\n"
    openneuro[f"{DS}/sub-04/anat/sub-04_T1w.nii.gz"] = b"\x09" * 64
    on("harvest", tmp_path, "--sub", "04")
    assert table_rows(lay.raw / "participants.tsv") == ["sub-01", "sub-04"]


def test_render_version_and_bidsignore_sign_shards(tmp_path, openneuro,
                                                   monkeypatch):
    lay = on("harvest", tmp_path, "--sub", "01")
    task = "roast/hosted/sub-01"
    first = read_task_states(lay.state)[task]["started"]
    on("roast", tmp_path)
    assert read_task_states(lay.state)[task]["started"] == first
    monkeypatch.setattr(hosted_mod, "RENDER_VERSION",
                        hosted_mod.RENDER_VERSION + 1)
    on("roast", tmp_path)
    second = read_task_states(lay.state)[task]["started"]
    assert second != first
    write(lay.raw / ".bidsignore", "*_bold.nii.gz\n")
    on("roast", tmp_path)
    assert read_task_states(lay.state)[task]["started"] != second


# ----------------------------------------------------------------------
#   Fable follow-up of #27
# ----------------------------------------------------------------------

def test_deleted_files_return(tmp_path, openneuro):
    lay = on("harvest", tmp_path)
    paths = [lay.raw / p for p in (
        "sub-01/anat/sub-01_T1w.json", "sub-01/sub-01_scans.tsv",
        "participants.tsv", "sub-01/func/sub-01_task-rest_events.tsv")]
    for path in paths:
        path.unlink()
    on("roast", tmp_path)
    assert all(path.exists() for path in paths)


def test_deleted_kept_edit_returns(tmp_path, openneuro):
    """N2: a file the last shard run left as it is, then deleted."""
    lay = on("harvest", tmp_path, "--sub", "01")
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    t1w.write_text('{"EchoTime": 1}\n')
    (lay.source / "openneuro/sub-01/anat/sub-01_T1w.json").write_text(
        '{"EchoTime": 0.005}')
    on("roast", tmp_path)                   # the shard runs: edit kept
    assert read_json(t1w) == {"EchoTime": 1}
    t1w.unlink()
    on("roast", tmp_path)
    assert read_json(t1w) == {"EchoTime": 0.005}


def test_deleted_frozen_file_is_reported(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    (lay.source / "openneuro/sub-01/anat/sub-01_T1w.json").unlink()
    roast_hosted(lay, display=False)        # frozen
    t1w.unlink()
    report = roast_hosted(lay, display=False)
    assert report.summary()["hosted"].get("error") == 1


def test_overwritten_edit_is_ours_again(tmp_path, openneuro, caplog):
    """N1: after `--if-exists overwrite`, the file follows its base."""
    lay = on("harvest", tmp_path, "--sub", "01")
    readme = lay.raw / "README"
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    readme.write_text("My notes\n")
    t1w.write_text('{"EchoTime": 1}\n')
    pristine = lay.source / "openneuro"
    write(pristine / "README", "New README\n")
    write(pristine / "sub-01/anat/sub-01_T1w.json", '{"EchoTime": 0.005}')
    on("roast", tmp_path, "--if-exists", "overwrite")
    assert readme.read_text() == "New README\n"
    assert read_json(t1w) == {"EchoTime": 0.005}
    write(pristine / "README", "Newer README\n")
    write(pristine / "sub-01/anat/sub-01_T1w.json", '{"EchoTime": 0.006}')
    caplog.clear()
    on("roast", tmp_path)                       # skip: left alone
    assert readme.read_text() == "New README\n"
    on("roast", tmp_path, "--if-exists", "update")
    assert readme.read_text() == "Newer README\n"
    assert read_json(t1w) == {"EchoTime": 0.006}
    assert "was edited" not in caplog.text


def test_old_tables_are_still_trimmed_by_compost(tmp_path, openneuro):
    """N4: tables of a dataset harvested before hybrid placement."""
    lay = on("harvest", tmp_path)
    update_files(lay, {k: {"origin": "direct", "source": None,
                           "sha256": None}
                       for k, v in read_manifest(lay.state)["files"].items()
                       if v.get("source")})
    on("compost", tmp_path, "--sub", "01", "--yes")
    assert table_rows(lay.raw / "participants.tsv") == ["sub-01"]


@pytest.mark.parametrize("failure", ["policy", "write"])
def test_replace_never_loses_the_file(tmp_path, openneuro, monkeypatch,
                                      failure):
    """N3: a file without a hash is copied, the action replaces it."""
    lay = on("harvest", tmp_path, "--no-repair")
    t1w = lay.raw / "sub-03/anat/sub-03_T1w.json"
    before = t1w.read_bytes()
    update_files(lay, {k: {"sha256": None} for k in
                       read_manifest(lay.state)["files"]})
    if failure == "policy":
        on("roast", tmp_path, "--sidecars", "no")
        assert t1w.read_bytes() == before
        assert not (lay.state / "replaced").exists()
        return

    def fail(self):
        raise OSError("disk full")
    monkeypatch.setattr(hosted_mod.RoastMetadata, "render", fail)
    roast_hosted(lay, display=False)
    assert t1w.read_bytes() == before


def test_edited_field_map_intended_for_follows_the_render(tmp_path,
                                                          openneuro,
                                                          caplog):
    lay = on("harvest", tmp_path, "--sub", "01")
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    fmap.write_text(json.dumps({"IntendedFor": [], "Note": "mine"}))
    caplog.clear()
    on("roast", tmp_path, "--sub", "01")
    assert read_json(fmap) == {
        "IntendedFor": ["func/sub-01_task-rest_bold.nii.gz"], "Note": "mine"}
    assert "IntendedFor is taken from the pristine copy" in caplog.text


def test_compost_no_trim_leaves_hosted_tables(tmp_path, openneuro):
    lay = on("harvest", tmp_path)
    table = lay.raw / "participants.tsv"
    on("compost", tmp_path, "--sub", "01", "--no-trim", "--yes")
    assert table_rows(table) == ["sub-01", "sub-02", "sub-03"]
    on("roast", tmp_path)
    assert table_rows(table) == ["sub-01"]


def test_trim_rows_labels_and_short_rows(caplog):
    here = {"sub-01"}.__contains__
    # labels without `sub-` (an unrepaired table)
    text = "participant_id\tage\n01\t30\n02\t40\n"
    out = trim_rows(text, "participants.tsv", lambda p, folder: here(p))
    assert out == "participant_id\tage\n01\t30\n"
    assert "has no" not in caplog.text
    # a row too short to have the column: kept, with a warning
    out = trim_rows("a\tparticipant_id\nx\n", "participants.tsv",
                    lambda p, folder: False)
    assert out == "a\tparticipant_id\nx\n"
    assert "a row has no participant_id, kept" in caplog.text


def test_unchanged_pristine_copies_keep_their_mtime(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    readme = lay.source / "openneuro/README"
    before = readme.stat().st_mtime_ns
    on("harvest", tmp_path, "--sub", "01")
    assert readme.stat().st_mtime_ns == before


def test_pristine_copies_without_digest_or_mtime(tmp_path, openneuro_url,
                                                 openneuro):
    """A listing without digests nor mtimes (OpenNeuro GraphQL, files
    not annexed): downloaded again, but an identical copy keeps its
    mtime (shard signatures)."""
    url = f"{openneuro_url}/openneuro.org/{DS}/README"
    entries = [RemoteEntry("README", url=url, size=len(ON["README"]))]
    root = tmp_path / "ds"
    harvest_hosted(entries, root, Selection(), folder="x", display=False)
    lay = Layout.detect(root)
    copy = lay.source / "x/README"
    before = copy.stat().st_mtime_ns
    ON_REQUESTS.clear()
    harvest_hosted(entries, root, Selection(), folder="x", display=False)
    assert ON_REQUESTS[f"GET {DS}/README"] == 1     # (downloaded again)
    assert copy.stat().st_mtime_ns == before


# ----------------------------------------------------------------------
#   Pure `--if-exists skip` (r43): `update` is the old `skip`
# ----------------------------------------------------------------------

def _visible(lay) -> dict[str, bytes]:
    """Visible metadata files of the raw dataset (no images)."""
    return {rel: (lay.raw / rel).read_bytes() for rel in files(lay.raw)
            if not rel.endswith(".nii.gz") and not rel.startswith(
                ("sourcedata/", "derivatives/"))}


def test_skip_leaves_hosted_files_alone(tmp_path, openneuro, caplog):
    lay = on("harvest", tmp_path / "a", "--sub", "01")
    before = _visible(lay)
    # new bases: a new snapshot of the README, a sidecar, a field map
    openneuro[f"{DS}/README"] = b"A new README\n"
    openneuro[f"{DS}/sub-01/anat/sub-01_T1w.json"] = b'{"EchoTime": 0.004}'
    openneuro[f"{DS}/sub-01/fmap/sub-01_dir-AP_epi.json"] = json.dumps(
        {**FMAP, "TotalReadoutTime": 0.05}).encode()
    caplog.clear()
    lay = on("harvest", tmp_path / "a", "--sub", "01", "02")
    after = _visible(lay)
    # the pristine copies follow the listing
    assert (lay.source / "openneuro/README").read_bytes() == \
        b"A new README\n"
    # existing files are left alone, silently
    for rel in ("README", "dataset_description.json",
                "sub-01/anat/sub-01_T1w.json", "task-rest_bold.json"):
        assert after[rel] == before[rel], rel
    assert not [r for r in caplog.records if r.levelname == "WARNING"]
    # new files are written; trimmed tables and field maps follow
    assert "sub-02/anat/sub-02_T1w.json" in after
    assert table_rows(lay.raw / "participants.tsv") == ["sub-01", "sub-02"]
    assert read_json(lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json")[
        "TotalReadoutTime"] == 0.05
    # `update` is the old `skip`: the same files as a fresh harvest
    on("roast", tmp_path / "a", "--if-exists", "update")
    fresh = on("harvest", tmp_path / "b", "--sub", "01", "02")
    assert _visible(lay) == _visible(fresh)
    # a hosted roast records its hashes itself: no "converted" entry
    entries = read_manifest(lay.state)["files"].values()
    assert not [e for e in entries if e.get("origin") == "converted"]


def test_deleted_visible_file_is_written_again_under_skip(tmp_path,
                                                          openneuro):
    lay = on("harvest", tmp_path, "--sub", "01")
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    content = t1w.read_bytes()
    on("roast", tmp_path)               # (the shard: skipped, unchanged)
    t1w.unlink()
    on("roast", tmp_path)
    assert t1w.read_bytes() == content


def test_compost_roast_runs_as_update(tmp_path, openneuro, monkeypatch):
    lay = on("harvest", tmp_path)
    seen = []
    real = hosted_mod.run_roast

    def spy(roaster, ctx, **kwargs):
        seen.append(ctx.ifexists)
        return real(roaster, ctx, **kwargs)

    monkeypatch.setattr(hosted_mod, "run_roast", spy)
    on("compost", tmp_path, "--exclude-sub", "03", "--yes")
    assert seen == ["update"]
    assert table_rows(lay.raw / "participants.tsv") == ["sub-01", "sub-02"]


def test_harvest_under_skip_refreshes_bases_only(tmp_path, templates,
                                                 caplog):
    """A second harvest after a template change: the generated base in
    `<state>/generated/` follows, the visible file is left alone (pure
    skip), `roast --if-exists update` renders it"""
    out = tmp_path / "data"
    bx("sald", "harvest", out)
    lay = Layout.detect(out / "SALD")
    readme = lay.raw / "README"
    base = lay.generated(readme)
    old = readme.read_bytes()
    write(templates / "sald/README", "A new README\n")
    caplog.clear()
    bx("sald", "harvest", out)
    assert base.read_text() == "A new README\n"
    assert readme.read_bytes() == old
    assert not [r for r in caplog.records if r.levelname == "WARNING"
                and "README" in r.getMessage()]
    bx("sald", "roast", out, "--if-exists", "update")
    assert readme.read_text() == "A new README\n"


def test_frozen_file_under_skip(tmp_path, openneuro, caplog):
    """A missing base: the visible file is left alone silently under
    skip; a missing visible file too is still an error"""
    lay = on("harvest", tmp_path, "--sub", "01")
    t1w = lay.raw / "sub-01/anat/sub-01_T1w.json"
    content = t1w.read_bytes()
    (lay.source / "openneuro/sub-01/anat/sub-01_T1w.json").unlink()
    caplog.clear()
    report = roast_hosted(lay, display=False)
    assert not report.errors and t1w.read_bytes() == content
    assert "left as it is" not in caplog.text
    t1w.unlink()
    report = roast_hosted(lay, display=False)
    assert len(report.errors) == 1 and "harvest again" in \
        report.errors[0][2]


def test_hosted_update_adopts_an_edit_that_holds_the_render(
        tmp_path, openneuro, caplog):
    """Compare before keep: a stale hash (a killed roast) is no edit"""
    lay = on("harvest", tmp_path, "--sub", "01")
    readme = lay.raw / "README"
    pristine = lay.source / "openneuro/README"
    pristine.write_text("New README\n")
    readme.write_bytes(pristine.read_bytes())       # (hash not recorded)
    caplog.clear()
    on("roast", tmp_path, "--if-exists", "update")
    assert "was edited" not in caplog.text
    assert manifest_entry(lay, readme)["sha256"] == \
        sha256(readme.read_bytes())
    pristine.write_text("Newer README\n")
    on("roast", tmp_path, "--if-exists", "update")
    assert readme.read_text() == "Newer README\n"


# ----------------------------------------------------------------------
#   Review round 2 of #39: hosted derivative descriptions
# ----------------------------------------------------------------------

DERIV_DESC = "derivatives/fmriprep/dataset_description.json"


def _with_derivative(openneuro):
    openneuro[f"{DS}/{DERIV_DESC}"] = \
        b'{"Name": "fmriprep", "BIDSVersion": "1.9.0"}'


def test_hosted_derivative_description_is_its_render(tmp_path, openneuro,
                                                     caplog):
    """Completed in the render (`SourceDatasets`): recorded, unedited
    files are left alone by `update`, no backup accumulates"""
    _with_derivative(openneuro)
    lay = on("harvest", tmp_path, "--sub", "01", "--folders",
             "derivatives")
    desc = lay.root / DERIV_DESC
    meta = read_json(desc)
    assert meta["DatasetType"] == "derivative"
    assert meta["SourceDatasets"][0]["URL"]
    assert manifest_entry(lay, desc)["sha256"] == sha256(desc.read_bytes())
    content = desc.read_bytes()
    for _ in range(2):
        caplog.clear()
        on("roast", tmp_path, "--if-exists", "update")
        assert desc.read_bytes() == content
        assert not [r for r in caplog.records if r.levelname == "WARNING"
                    and desc.name in r.getMessage()]
    assert not (lay.state / "replaced").exists()


def test_edited_hosted_derivative_description_survives(tmp_path, openneuro,
                                                      caplog):
    _with_derivative(openneuro)
    lay = on("harvest", tmp_path, "--sub", "01", "--folders",
             "derivatives")
    desc = lay.root / DERIV_DESC
    desc.write_bytes(b'{"Name": "mine"}')
    for _ in range(2):
        caplog.clear()
        on("roast", tmp_path, "--if-exists", "update")
        assert desc.read_bytes() == b'{"Name": "mine"}'
        assert "was edited" in caplog.text
    assert not (lay.state / "replaced").exists()


def test_interrupted_roast_leaves_a_complete_description(
        tmp_path, openneuro, monkeypatch):
    """An interrupted `finish_roast` (no hash recorded): the description
    is complete anyway (the render), and adopted by the next update"""
    from brainspresso.roast import run as run_mod  # noqa: PLC0415

    _with_derivative(openneuro)
    real = run_mod.finish_roast
    calls = []

    def once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("interrupted")
        return real(*args, **kwargs)

    monkeypatch.setattr(run_mod, "finish_roast", once)
    with pytest.raises(Exception):
        on("harvest", tmp_path, "--sub", "01", "--folders", "derivatives")
    lay = Layout.detect(tmp_path / "OpenNeuro" / DS)
    desc = lay.root / DERIV_DESC
    assert "SourceDatasets" in read_json(desc)
    on("roast", tmp_path, "--if-exists", "update")
    assert "SourceDatasets" in read_json(desc)
    assert manifest_entry(lay, desc)["sha256"] == sha256(desc.read_bytes())
    assert not (lay.state / "replaced").exists()


def test_stale_field_map_hash_is_not_warned_about(tmp_path, openneuro,
                                                  caplog):
    lay = on("harvest", tmp_path, "--sub", "01")
    fmap = lay.raw / "sub-01/fmap/sub-01_dir-AP_epi.json"
    content = fmap.read_bytes()
    update_files(lay, {_key(lay, fmap): {"sha256": "0" * 64}})   # (stale)
    caplog.clear()
    on("roast", tmp_path, "--if-exists", "update")
    assert fmap.read_bytes() == content
    assert "was edited" not in caplog.text
    # (it holds the render: adopted)
    assert manifest_entry(lay, fmap)["sha256"] == sha256(content)


def _key(lay, path):
    return Path(path).relative_to(lay.root).as_posix()


NESTED_DESC = "derivatives/fs/derivatives/qc/dataset_description.json"


def test_nested_hosted_derivative_description(tmp_path, openneuro, caplog):
    """Round 3 of #39: a derivative of a derivative is completed in its
    render too (the URL of the layout that owns it, at any depth)"""
    openneuro[f"{DS}/derivatives/fs/dataset_description.json"] = \
        b'{"Name": "fs", "BIDSVersion": "1.9.0"}'
    openneuro[f"{DS}/{NESTED_DESC}"] = \
        b'{"Name": "qc", "BIDSVersion": "1.9.0"}'
    lay = on("harvest", tmp_path, "--sub", "01", "--folders",
             "derivatives")
    desc = lay.root / NESTED_DESC
    meta = read_json(desc)
    owner = lay.derivative("fs").derivative("qc")
    assert meta["SourceDatasets"][0] == {"URL": owner.source_url}
    assert manifest_entry(lay, desc)["sha256"] == sha256(desc.read_bytes())
    content = desc.read_bytes()
    for _ in range(2):
        caplog.clear()
        on("roast", tmp_path, "--if-exists", "update")
        assert desc.read_bytes() == content
        assert not [r for r in caplog.records if r.levelname == "WARNING"
                    and "dataset_description" in r.getMessage()]
    assert not (lay.state / "replaced").exists()
    desc.write_bytes(b'{"Name": "mine"}')
    caplog.clear()
    on("roast", tmp_path, "--if-exists", "update")
    assert desc.read_bytes() == b'{"Name": "mine"}'
    assert "was edited" in caplog.text
    assert not (lay.state / "replaced").exists()
