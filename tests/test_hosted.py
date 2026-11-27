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
import shutil
import sys
from collections import Counter
from pathlib import Path

import pytest
from fake_s3 import bucket_app, serve

from brainspresso.actions.state import read_task_states
from brainspresso.bids.compost import compost
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
)
from brainspresso.bids.layout import Layout, make_layout, read_manifest
from brainspresso.cli import app
from brainspresso.datasets.catalog import load
from brainspresso.datasets.INDI import engine
from brainspresso.datasets.register import register
from brainspresso.roast.hosted import roast_hosted
from brainspresso.sources import fcp
from brainspresso.sources import openneuro as openneuro_mod
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
    write(tmp_path / "fill.toml", 'version = 1\nfiles = ["T1w.json", '
          '"*/T1w.json", "B/task-rest_bold.json"]\n')
    found = read_fill_templates(tmp_path, ["A", "B"])
    assert [(t.site, t.name) for t in found] == [
        (None, "T1w.json"), ("A", "T1w.json"), ("B", "task-rest_bold.json"),
    ]
    assert found[1].content == {"EchoTime": 1}
    assert fill_files(tmp_path) == {"fill.toml", "T1w.json", "A/T1w.json",
                                    "B/task-rest_bold.json"}
    assert fill_files(tmp_path / "A") == set()


@pytest.mark.parametrize("toml,files,message", [
    ('files = ["X/T1w.json"]', {"X/T1w.json": "{}"}, "not a site"),
    ('files = ["T2w.json"]', {}, "matches no file"),
    ('files = ["participants.json"]', {"participants.json": "{}"},
     "not the name of a sidecar"),
    ('files = ["sub-1_T1w.json"]', {"sub-1_T1w.json": "{}"},
     "not a subject"),
    ('files = ["T1w.json"]', {"T1w.json": "[1]"}, "not a JSON object"),
    ('version = 2\nfiles = []', {}, "not supported"),
    ('fill = []', {}, "unknown keys"),
    ('files = ["A/B/T1w.json"]', {"A/B/T1w.json": "{}"}, "templates are"),
])
def test_read_fill_templates_errors(tmp_path, toml, files, message):
    for name, text in files.items():
        write(tmp_path / name, text)
    write(tmp_path / "fill.toml", toml + "\n")
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
    "sub-02/anat/sub-02_T1w.nii.gz": b"\x04" * 64,
    "sub-02/anat/sub-02_T1w.json": b'{"EchoTime": 0.003}',
    "sub-03/anat/sub-03_T1w.nii.gz": b"\x05" * 64,
}
ON_OBJECTS = {f"{DS}/{k}": v for k, v in ON.items()}
ON_REQUESTS = Counter()


@pytest.fixture(scope="module")
def openneuro_url():
    url, stop = serve(bucket_app("openneuro.org", ON_OBJECTS, ON_REQUESTS))
    yield url
    stop()


@pytest.fixture
def openneuro(openneuro_url, monkeypatch):
    monkeypatch.setattr(openneuro_mod, "S3Bucket",
                        lambda name: S3Bucket(name, endpoint=openneuro_url))
    ON_REQUESTS.clear()
    original = dict(ON_OBJECTS)
    yield ON_OBJECTS
    ON_OBJECTS.clear()
    ON_OBJECTS.update(original)


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
    assert "repair" not in t1w           # OpenNeuro: no repair
    assert entries[t1w["source"]]["origin"] == "direct"
    stages = [r["stage"] for r in read_manifest(lay.state)["runs"]]
    assert stages[-2:] == ["harvest", "roast"]


def test_openneuro_partial_harvests(tmp_path, openneuro):
    on("harvest", tmp_path, "--subs", "02")
    lay = on("harvest", tmp_path, "--subs", "03")
    rows = (lay.raw / "participants.tsv").read_text().splitlines()
    assert rows == ["participant_id\tage", "sub-02\t40", "sub-03\t50"]
    assert not (lay.raw / "sub-01").exists()
    assert (lay.raw / "sub-02/anat/sub-02_T1w.json").exists()
    entries = read_manifest(lay.state)["files"]
    assert "rawbids/sub-02/anat/sub-02_T1w.nii.gz" in entries
    assert "rawbids/sub-03/anat/sub-03_T1w.nii.gz" in entries


@pytest.mark.parametrize("policy", ["no", "only"])
def test_openneuro_sidecars(tmp_path, openneuro, policy):
    lay = on("harvest", tmp_path, "--subs", "01", "--sidecars", policy)
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
    lay = on("harvest", tmp_path, "--subs", "01")
    task = "roast/hosted/sub-01"
    first = read_task_states(lay.state)[task]
    assert first["status"] == "done"
    sidecar = lay.raw / "sub-01/anat/sub-01_T1w.json"
    mtime = sidecar.stat().st_mtime_ns
    lay = on("harvest", tmp_path, "--subs", "01")
    # same inputs: the shard is skipped (its record is not rewritten)
    assert read_task_states(lay.state)[task]["started"] == first["started"]
    assert sidecar.stat().st_mtime_ns == mtime
    # a new pristine copy (new snapshot) reaches the dataset
    openneuro[f"{DS}/sub-01/anat/sub-01_T1w.json"] = b'{"EchoTime": 0.004}'
    lay = on("harvest", tmp_path, "--subs", "01", "--if-exists",
             "overwrite")
    assert read_json(sidecar) == {"EchoTime": 0.004}


def test_openneuro_roast_on_its_own(tmp_path, openneuro):
    lay = on("harvest", tmp_path, "--subs", "01", "02")
    raw = lay.raw
    sidecar = raw / "sub-01/anat/sub-01_T1w.json"
    other = raw / "sub-02/anat/sub-02_T1w.json"
    sidecar.unlink()
    other.write_text('{"EchoTime": 1}')
    # a roast of one subject: only its files (shards in a process pool)
    on("roast", tmp_path, "--subs", "01", "--jobs", "2")
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
    lay = on("harvest", tmp_path, "--subs", "01")
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
    on("harvest", tmp_path, "--subs", "01")
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
    run = read_manifest(lay.state)["runs"][-1]
    assert run["stage"] == "roast" and run["trigger"] == "compost"


def test_openneuro_dry_run(tmp_path, openneuro, capsys):
    bx("openneuro", "harvest", DS, tmp_path, "--source", "s3", "--dry-run",
       "--subs", "01")
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
    shutil.copytree(engine.TEMPLATES, folder)
    monkeypatch.setattr(engine, "TEMPLATES", folder)
    return folder


def test_sald_hybrid_placement_and_fill_in(tmp_path, templates):
    sald = templates / "sald"
    write(sald / "fill.toml", 'version = 1\nfiles = ["T1w.json", '
          '"bold.json", "dwi.json"]\n')
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
    assert not (raw / "fill.toml").exists()
    assert not (raw / "bold.json").exists()
    # curated templates are generated files, repaired tables are trimmed
    assert (raw / "README").exists()
    assert (raw / "participants.tsv").read_text().splitlines() == [
        "participant_id\tsex\tage", "sub-031274\tF\t19",
        "sub-031275\tM\tn/a"]
    entries = read_manifest(lay.state)["files"]
    assert entries["rawbids/T1w.json"] == {"origin": "generated",
                                           "fill": ["T1w.json"]}
    bold = entries["rawbids/task-rest_bold.json"]
    assert bold["origin"] == "roasted" and bold["repair"] is True
    assert bold["fill"] == ["bold.json"]
    assert bold["source"] == \
        "sourcedata/fcp-indi/INDI/SALD/RawData_BIDS/task-rest_bold.json"
    assert entries["rawbids/README"]["origin"] == "generated"
    assert entries["rawbids/README"]["source"].startswith(
        ".brainspresso/generated/")
    # a new template reaches the dataset with a roast (no network)
    write(sald / "T1w.json", '{"Manufacturer": "Siemens", '
          '"MagneticFieldStrength": 3, "FlipAngle": 9}')
    INDI_REQUESTS.clear()
    bx("sald", "roast", out)
    assert read_json(raw / "T1w.json")["FlipAngle"] == 9
    assert not INDI_REQUESTS


def test_acpi_fill_in_by_site(tmp_path, templates):
    acpi = templates / "acpi"
    write(acpi / "fill.toml",
          'version = 1\nfiles = ["du_1/T1w.json", "*/task-rest_bold.json"]\n')
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
    bx("acpi", "roast", out, "--sites", "nyu_1")
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
    lay = on("harvest", tmp_path, "--subs", "01")
    pristine = lay.source / "openneuro/sub-01/anat/sub-01_T1w.json"
    sidecar = lay.raw / "sub-01/anat/sub-01_T1w.json"
    pristine.unlink()
    # the roasted file is left as it is
    assert roast_hosted(lay, display=False).status == "complete"
    assert sidecar.exists()
    # without it, the roast reports the missing copy
    sidecar.unlink()
    report = roast_hosted(lay, display=False)
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
    assert "--sites" in snapshot_flags("acpi")["roast"]
    assert "--sites" not in snapshot_flags("openneuro")["roast"]


def test_fill_templates_come_from_the_dataset_package(tmp_path, templates):
    # any hosted dataset: `<template_dir>/fill.toml` (no fixed path)
    write(tmp_path / "pkg/fill.toml", 'files = ["T1w.json"]\n')
    write(tmp_path / "pkg/T1w.json", '{"A": 1}')

    class Hosted(load("openneuro").__class__):
        template_dir = tmp_path / "pkg"

    assert [t.id for t in Hosted().fill_templates()] == ["T1w.json"]
    assert load("openneuro").fill_templates() == []
    # INDI: the project's folder (`Project.template_dir`)
    sald = load("sald")
    assert sald.template_dir == templates / "sald"
    write(templates / "sald/fill.toml", 'files = ["T1w.json"]\n')
    write(templates / "sald/T1w.json", '{"A": 1}')
    assert [t.id for t in sald.fill_templates()] == ["T1w.json"]
