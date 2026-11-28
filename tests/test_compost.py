import json
from pathlib import Path

import pytest

from brainspresso.bids.compost import compost, plan_compost
from brainspresso.bids.layout import Layout, make_layout
from brainspresso.bids.select import Selection

# A dataset roasted by brainspresso, given relative to the raw dataset
# (`RAW`), to `sourcedata/` (`SOURCE`) and to each derivative dataset
# (`DERIVATIVES`); placed by `layout` (study or flat).
SOURCE = {
    "IXI-T1.tar": "x" * 100,
    "IXI-DTI.tar": "x" * 100,
    "IXI.xls": "x",
}
RAW = {
    "dataset_description.json": "{}",
    "participants.json": "{}",
    "participants.tsv": (
        "participant_id\tage\tsex\n"
        "sub-001\t25\tF\n"
        "sub-002\t70\tM\n"
        "sub-003\t65\tF\n"
    ),
    "dwi.bval": "0 1000",
    "T1w.json": "{}",
    "sub-001/anat/sub-001_T1w.nii.gz": "x",
    "sub-001/anat/sub-001_T1w.json": "{}",
    "sub-001/anat/sub-001_T2w.nii.gz": "x",
    "sub-001/anat/sub-001_T2w.json": "{}",
    "sub-001/dwi/sub-001_dwi.nii.gz": "x",
    "sub-002/anat/sub-002_T1w.nii.gz": "x",
    "sub-002/anat/sub-002_T1w.json": "{}",
    "sub-003/anat/sub-003_T1w.nii.gz": "x",
}
DERIVATIVES = {"fs": {
    "dataset_description.json": "{}",
    "sourcedata/sub-001/mri/aseg.mgz": "x",
    "sub-001/anat/sub-001_dseg.nii.gz": "x",
    "sub-002/anat/sub-002_dseg.nii.gz": "x",
}}


def roasted_files(layout: Layout) -> dict:
    """Files of the roasted dataset, relative to the dataset folder"""
    def rel(path):
        return path.relative_to(layout.root).as_posix()
    out = {rel(layout.source / k): v for k, v in SOURCE.items()}
    out.update({rel(layout.raw / k): v for k, v in RAW.items()})
    for name, content in DERIVATIVES.items():
        drv = layout.derivative(name).root
        out.update({rel(drv / k): v for k, v in content.items()})
    return out


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


@pytest.fixture(params=["study", "flat"])
def roasted(tmp_path, request):
    # no manifest: the layout is detected from the tree
    layout = make_layout(request.param, tmp_path / "IXI")
    make(layout.root, roasted_files(layout))
    return layout.root


def roasted_tree(root: Path) -> dict:
    return roasted_files(Layout.detect(root))


def raw(root: Path, rel: str = "") -> str:
    """Path of a file of the raw dataset, relative to the dataset folder"""
    path = Layout.detect(root).raw / rel
    return path.relative_to(root).as_posix()


@pytest.fixture
def harvested(tmp_path):
    return make(tmp_path / "ds000001", HARVESTED)


def test_sourcedata(roasted):
    compost(roasted, yes=True)
    left = files(roasted)
    assert not any(f.startswith("sourcedata/") for f in left)
    assert not (roasted / "sourcedata").exists()
    # BIDS data and nested source data untouched
    tree = roasted_tree(roasted)
    assert set(tree) - left == {
        f for f in tree if f.startswith("sourcedata/")
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
        f for f in roasted_tree(roasted) if f.startswith("sourcedata/")
    )
    assert "sourcedata/IXI-T1.tar" in capsys.readouterr().out


def test_nothing_to_compost(roasted):
    with pytest.raises(ValueError, match="Nothing to compost"):
        compost(roasted, sourcedata=False, yes=True)


def test_exclude_subs(roasted):
    compost(roasted, selection=Selection(exclude_sub=["002"]), yes=True)
    left = files(roasted)
    # filters do not touch source data by default
    assert "sourcedata/IXI-T1.tar" in left
    assert not (roasted / raw(roasted, "sub-002")).exists()
    assert raw(roasted, "sub-001/anat/sub-001_T1w.nii.gz") in left
    # derivatives untouched unless asked
    assert "derivatives/fs/sub-002/anat/sub-002_dseg.nii.gz" in left
    # participants.tsv lives in the raw dataset
    assert participants(roasted / raw(roasted, "participants.tsv")) == [
        "sub-001", "sub-003"
    ]


def test_filters_and_sourcedata(roasted):
    compost(
        roasted, selection=Selection(sub=["001"]), sourcedata=True,
        yes=True,
    )
    left = files(roasted)
    assert not any(f.startswith("sourcedata/") for f in left)
    assert participants(roasted / raw(roasted, "participants.tsv")) == [
        "sub-001"
    ]


def test_suffixes(roasted):
    compost(roasted, selection=Selection(suffix=["T1w"]), yes=True)
    left = files(roasted)
    assert raw(roasted, "sub-001/anat/sub-001_T2w.nii.gz") not in left
    assert raw(roasted, "sub-001/anat/sub-001_T2w.json") not in left
    assert not (roasted / raw(roasted, "sub-001/dwi")).exists()
    # top-level sidecars that do not apply anymore are deleted
    assert raw(roasted, "dwi.bval") not in left
    assert raw(roasted, "T1w.json") in left
    assert raw(roasted, "sub-001/anat/sub-001_T1w.json") in left
    # dataset-level files are never deleted
    assert raw(roasted, "dataset_description.json") in left
    assert raw(roasted, "participants.json") in left


def test_where_derivatives(roasted):
    compost(roasted, where=["age>=60"], derivatives=True, yes=True)
    left = files(roasted)
    assert not (roasted / raw(roasted, "sub-001")).exists()
    assert not (roasted / "derivatives/fs/sub-001").exists()
    assert "derivatives/fs/sub-002/anat/sub-002_dseg.nii.gz" in left
    assert "derivatives/fs/dataset_description.json" in left
    # nested source data is not a subject folder: untouched
    assert "derivatives/fs/sourcedata/sub-001/mri/aseg.mgz" in left
    assert participants(roasted / raw(roasted, "participants.tsv")) == [
        "sub-002", "sub-003"
    ]


def test_harvested(harvested):
    compost(harvested, selection=Selection(datatype=["anat"]), yes=True)
    left = files(harvested)
    assert not (harvested / "sub-01/func").exists()
    scans = (harvested / "sub-01/sub-01_scans.tsv").read_text()
    assert "anat/sub-01_T1w.nii.gz" in scans
    assert "bold" not in scans
    assert ".brainspresso/manifest.json" in left

    compost(harvested, selection=Selection(sub=["02"]), yes=True)
    # trimmed from the untouched copy kept by harvest
    assert participants(harvested / "participants.tsv") == ["sub-02"]
    manifest = json.loads(
        (harvested / ".brainspresso/manifest.json").read_text()
    )
    assert list(manifest["files"]) == ["sub-02/anat/sub-02_T1w.nii.gz"]
    assert manifest["runs"][-1]["stage"] == "compost"
    assert manifest["runs"][-1]["filters"] == {"sub": ["02"]}


def test_plan_kept_subjects(roasted):
    plan = plan_compost(
        roasted, sourcedata=False, selection=Selection(sub=["003"])
    )
    assert plan.kept_subjects == {"sub-003"}
    assert raw(roasted, "sub-003/anat/sub-003_T1w.nii.gz") not in plan.data


@pytest.mark.parametrize("kind", ["study", "flat"])
def test_cli(tmp_path, capsys, kind):
    import brainspresso  # noqa: F401  (registers datasets)
    from brainspresso.cli import app

    roasted = tmp_path / "IXI"
    tree = roasted_files(make_layout(kind, roasted))
    make(roasted, tree)
    try:
        app(["ixi", "compost", str(tmp_path), "--dry-run",
             "--exclude-sub", "001", "002"])
    except SystemExit as e:
        assert not e.code
    out = capsys.readouterr().out
    assert raw(roasted, "sub-001/anat/sub-001_T1w.nii.gz") in out
    assert "sourcedata" not in out.split("Compost")[1]
    assert files(tmp_path / "IXI") == set(tree)
