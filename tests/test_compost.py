import json
from pathlib import Path

import pytest

from brainspresso.bids.compost import compost, plan_compost
from brainspresso.bids.select import Selection

# Layout of a dataset roasted by brainspresso:
# participants.tsv at the dataset root, subjects under rawdata/
ROASTED = {
    "sourcedata/IXI-T1.tar": "x" * 100,
    "sourcedata/IXI-DTI.tar": "x" * 100,
    "sourcedata/IXI.xls": "x",
    "dataset_description.json": "{}",
    "participants.json": "{}",
    "participants.tsv": (
        "participant_id\tage\tsex\n"
        "sub-001\t25\tF\n"
        "sub-002\t70\tM\n"
        "sub-003\t65\tF\n"
    ),
    "rawdata/dwi.bval": "0 1000",
    "rawdata/T1w.json": "{}",
    "rawdata/sub-001/anat/sub-001_T1w.nii.gz": "x",
    "rawdata/sub-001/anat/sub-001_T1w.json": "{}",
    "rawdata/sub-001/anat/sub-001_T2w.nii.gz": "x",
    "rawdata/sub-001/anat/sub-001_T2w.json": "{}",
    "rawdata/sub-001/dwi/sub-001_dwi.nii.gz": "x",
    "rawdata/sub-002/anat/sub-002_T1w.nii.gz": "x",
    "rawdata/sub-002/anat/sub-002_T1w.json": "{}",
    "rawdata/sub-003/anat/sub-003_T1w.nii.gz": "x",
    "derivatives/fs/dataset_description.json": "{}",
    "derivatives/fs/sourcedata/sub-001/mri/aseg.mgz": "x",
    "derivatives/fs/sub-001/anat/sub-001_dseg.nii.gz": "x",
    "derivatives/fs/sub-002/anat/sub-002_dseg.nii.gz": "x",
}

# Layout of a BIDS dataset harvested as is (e.g. OpenNeuro)
HARVESTED = {
    "dataset_description.json": "{}",
    "participants.tsv": (
        "participant_id\tage\n"
        "sub-01\t30\n"
        "sub-02\t40\n"
    ),
    ".brainspresso/orig/participants.tsv": (
        "participant_id\tage\n"
        "sub-01\t30\n"
        "sub-02\t40\n"
        "sub-03\t50\n"
    ),
    ".brainspresso/manifest.json": json.dumps({
        "runs": [{"source": "openneuro"}],
        "files": {
            "sub-01/anat/sub-01_T1w.nii.gz": {"url": "a"},
            "sub-02/anat/sub-02_T1w.nii.gz": {"url": "b"},
        },
    }),
    "sub-01/sub-01_scans.tsv": (
        "filename\tacq_time\n"
        "anat/sub-01_T1w.nii.gz\tn/a\n"
        "func/sub-01_task-rest_bold.nii.gz\tn/a\n"
    ),
    "sub-01/anat/sub-01_T1w.nii.gz": "x",
    "sub-01/func/sub-01_task-rest_bold.nii.gz": "x",
    "sub-01/func/sub-01_task-rest_bold.json": "{}",
    "sub-02/anat/sub-02_T1w.nii.gz": "x",
}


def make(root: Path, layout: dict) -> Path:
    for path, content in layout.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(content)
    return root


def files(root: Path) -> set[str]:
    return {
        p.relative_to(root).as_posix()
        for p in root.rglob("*") if p.is_file()
    }


def participants(path: Path) -> list[str]:
    return [line.split("\t")[0] for line in path.read_text().splitlines()[1:]]


@pytest.fixture
def roasted(tmp_path):
    return make(tmp_path / "IXI", ROASTED)


@pytest.fixture
def harvested(tmp_path):
    return make(tmp_path / "ds000001", HARVESTED)


def test_sourcedata(roasted):
    compost(roasted, yes=True)
    left = files(roasted)
    assert not any(f.startswith("sourcedata/") for f in left)
    assert not (roasted / "sourcedata").exists()
    # BIDS data and nested source data untouched
    assert set(ROASTED) - left == {
        f for f in ROASTED if f.startswith("sourcedata/")
    }


def test_sourcedata_keep_and_nested(roasted):
    compost(roasted, keep=["*.xls"], nested=True, yes=True)
    left = files(roasted)
    assert "sourcedata/IXI.xls" in left
    assert "sourcedata/IXI-T1.tar" not in left
    assert not (roasted / "derivatives/fs/sourcedata").exists()
    assert "derivatives/fs/sub-001/anat/sub-001_dseg.nii.gz" in left


def test_sourcedata_not_roasted(tmp_path):
    root = make(tmp_path / "GSP", {"sourcedata/a.tar": "x"})
    with pytest.raises(ValueError, match="roasted"):
        compost(root, yes=True)
    assert files(root) == {"sourcedata/a.tar"}
    compost(root, yes=True, force=True)
    assert files(root) == set()


def test_confirmation(roasted):
    before = files(roasted)
    plan = compost(roasted, ask=lambda _: "n")
    assert files(roasted) == before
    assert plan.files == []
    compost(roasted, ask=lambda _: "y")
    assert "sourcedata/IXI.xls" not in files(roasted)


def test_non_interactive_requires_yes(roasted, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    with pytest.raises(ValueError, match="--yes"):
        compost(roasted)


def test_dry_run(roasted, capsys):
    before = files(roasted)
    plan = compost(roasted, dry_run=True)
    assert files(roasted) == before
    assert sorted(plan.sourcedata) == sorted(
        f for f in ROASTED if f.startswith("sourcedata/")
    )
    assert "sourcedata/IXI-T1.tar" in capsys.readouterr().out


def test_nothing_to_compost(roasted):
    with pytest.raises(ValueError, match="Nothing to compost"):
        compost(roasted, sourcedata=False, yes=True)


def test_exclude_subs(roasted):
    compost(roasted, selection=Selection(exclude_subs=["002"]), yes=True)
    left = files(roasted)
    # filters do not touch source data by default
    assert "sourcedata/IXI-T1.tar" in left
    assert not (roasted / "rawdata/sub-002").exists()
    assert "rawdata/sub-001/anat/sub-001_T1w.nii.gz" in left
    # derivatives untouched unless asked
    assert "derivatives/fs/sub-002/anat/sub-002_dseg.nii.gz" in left
    # participants.tsv lives next to rawdata/
    assert participants(roasted / "participants.tsv") == [
        "sub-001", "sub-003"
    ]


def test_filters_and_sourcedata(roasted):
    compost(
        roasted, selection=Selection(subs=["001"]), sourcedata=True,
        yes=True,
    )
    left = files(roasted)
    assert not any(f.startswith("sourcedata/") for f in left)
    assert participants(roasted / "participants.tsv") == ["sub-001"]


def test_suffixes(roasted):
    compost(roasted, selection=Selection(suffixes=["T1w"]), yes=True)
    left = files(roasted)
    assert "rawdata/sub-001/anat/sub-001_T2w.nii.gz" not in left
    assert "rawdata/sub-001/anat/sub-001_T2w.json" not in left
    assert not (roasted / "rawdata/sub-001/dwi").exists()
    # top-level sidecars that do not apply anymore are deleted
    assert "rawdata/dwi.bval" not in left
    assert "rawdata/T1w.json" in left
    assert "rawdata/sub-001/anat/sub-001_T1w.json" in left
    # dataset-level files are never deleted
    assert "dataset_description.json" in left
    assert "participants.json" in left


def test_where_derivatives(roasted):
    compost(roasted, where=["age>=60"], derivatives=True, yes=True)
    left = files(roasted)
    assert not (roasted / "rawdata/sub-001").exists()
    assert not (roasted / "derivatives/fs/sub-001").exists()
    assert "derivatives/fs/sub-002/anat/sub-002_dseg.nii.gz" in left
    assert "derivatives/fs/dataset_description.json" in left
    # nested source data is not a subject folder: untouched
    assert "derivatives/fs/sourcedata/sub-001/mri/aseg.mgz" in left
    assert participants(roasted / "participants.tsv") == [
        "sub-002", "sub-003"
    ]


def test_harvested(harvested):
    compost(harvested, selection=Selection(datatypes=["anat"]), yes=True)
    left = files(harvested)
    assert not (harvested / "sub-01/func").exists()
    scans = (harvested / "sub-01/sub-01_scans.tsv").read_text()
    assert "anat/sub-01_T1w.nii.gz" in scans
    assert "bold" not in scans
    assert ".brainspresso/manifest.json" in left

    compost(harvested, selection=Selection(subs=["02"]), yes=True)
    # trimmed from the untouched copy kept by harvest
    assert participants(harvested / "participants.tsv") == ["sub-02"]
    manifest = json.loads(
        (harvested / ".brainspresso/manifest.json").read_text()
    )
    assert list(manifest["files"]) == ["sub-02/anat/sub-02_T1w.nii.gz"]
    assert manifest["runs"][-1]["action"] == "compost"
    assert manifest["runs"][-1]["filters"] == {"subs": ["02"]}


def test_plan_kept_subjects(roasted):
    plan = plan_compost(
        roasted, sourcedata=False, selection=Selection(subs=["003"])
    )
    assert plan.kept_subjects == {"sub-003"}
    assert "rawdata/sub-003/anat/sub-003_T1w.nii.gz" not in plan.data


def test_cli(tmp_path, capsys):
    from brainspresso.cli import app
    import brainspresso  # noqa: F401  (registers datasets)

    make(tmp_path / "IXI", ROASTED)
    try:
        app(["ixi", "compost", str(tmp_path), "--dry-run",
             "--exclude-subs", "001", "002"])
    except SystemExit as e:
        assert not e.code
    out = capsys.readouterr().out
    assert "rawdata/sub-001/anat/sub-001_T1w.nii.gz" in out
    assert "sourcedata" not in out.split("Compost")[1]
    assert files(tmp_path / "IXI") == set(ROASTED)
