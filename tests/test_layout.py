"""Dataset layouts (`bids.layout`) and the state they record."""
import json
from pathlib import Path

import pytest

from brainspresso.bids import layout as layout_mod
from brainspresso.actions.state import (
    read_task_states, task_path, write_task_state,
)
from brainspresso.bids.harvest import (
    InlineEntry, harvest, read_manifest, record_run,
)
from brainspresso.bids.layout import (
    DEFAULT_LAYOUT, DerivativeLayout, FlatLayout, Layout, LayoutError,
    SitesLayout, StudyLayout, make_layout, relative_url, resolve_layout,
)
from brainspresso.bids.select import Selection


def write(path: Path, content="x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content if isinstance(content, str)
                    else json.dumps(content))
    return path


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


class PathLike:
    """A path that is not a `pathlib.Path`"""
    def __init__(self, path):
        self.path = str(path)

    def __fspath__(self):
        return self.path


# ----------------------------------------------------------------------
#   Paths
# ----------------------------------------------------------------------

def test_default_layout():
    assert DEFAULT_LAYOUT == "study"
    assert make_layout(None, "/data/DS").kind == DEFAULT_LAYOUT


def test_default_layout_is_one_constant(tmp_path, monkeypatch):
    monkeypatch.setattr(layout_mod, "DEFAULT_LAYOUT", "flat")
    assert make_layout(None, tmp_path).kind == "flat"
    assert resolve_layout(tmp_path / "new").kind == "flat"
    assert SitesLayout(tmp_path).child("A").kind == "flat"


def test_study_paths():
    lay = make_layout("study", "/data/DS")
    assert isinstance(lay, StudyLayout)
    root = Path("/data/DS")
    assert lay.root == root
    assert lay.raw == lay.bids == root / "rawbids"
    assert lay.source == root / "sourcedata"
    assert lay.state == root / ".brainspresso"
    assert lay.manifest == root / ".brainspresso/manifest.json"
    assert lay.tasks == root / ".brainspresso/tasks"
    assert lay.derivatives == root / "derivatives"
    assert lay.phenotype == root / "rawbids/phenotype"
    assert lay.bidsignore == root / "rawbids/.bidsignore"
    assert lay.description == root / "rawbids/dataset_description.json"
    assert lay.datasets() == [lay]


def test_flat_paths():
    lay = make_layout("flat", "/data/DS")
    assert isinstance(lay, FlatLayout)
    root = Path("/data/DS")
    assert lay.raw == lay.bids == root
    assert lay.source == root / "sourcedata"
    assert lay.state == root / ".brainspresso"
    assert lay.derivatives == root / "derivatives"
    assert lay.phenotype == root / "phenotype"
    assert lay.bidsignore == root / ".bidsignore"
    assert lay.description == root / "dataset_description.json"


def test_pathlike(tmp_path):
    lay = make_layout("study", PathLike(tmp_path / "DS"))
    assert lay.raw == tmp_path / "DS" / "rawbids"
    assert lay.relative(PathLike(lay.raw / "sub-01")) == (lay.raw, "sub-01")
    assert Layout.detect(PathLike(tmp_path / "nothing")) is None


def test_unknown_layout():
    with pytest.raises(ValueError, match="Unknown layout"):
        make_layout("rawdata", "/data/DS")


@pytest.mark.parametrize("kind, url", [
    ("study", "../../rawbids/"), ("flat", "../../"),
])
def test_derivative(kind, url):
    lay = make_layout(kind, "/data/DS")
    fs = lay.derivative("freesurfer")
    assert isinstance(fs, DerivativeLayout)
    assert fs.root == fs.bids == Path("/data/DS/derivatives/freesurfer")
    assert fs.raw == lay.raw
    assert fs.source == fs.root / "sourcedata"
    assert fs.bidsignore == fs.root / ".bidsignore"
    assert fs.phenotype == fs.root / "phenotype"
    assert fs.state == lay.state                 # one state per dataset
    assert fs.source_url == url
    assert (fs.root / url).resolve() == Path(lay.raw).resolve()
    # derivative of a derivative
    nested = fs.derivative("qc")
    assert nested.root == fs.root / "derivatives/qc"
    assert nested.raw == lay.raw
    with pytest.raises(ValueError):
        lay.derivative("../x")


def test_list_derivatives(tmp_path):
    lay = make_layout("study", tmp_path)
    assert lay.list_derivatives() == []
    (tmp_path / "derivatives/b").mkdir(parents=True)
    (tmp_path / "derivatives/a").mkdir()
    write(tmp_path / "derivatives/README")
    assert [d.name for d in lay.list_derivatives()] == ["a", "b"]


# ----------------------------------------------------------------------
#   Mapping
# ----------------------------------------------------------------------

def test_relative_study():
    lay = make_layout("study", "/data/DS")
    raw, root = lay.raw, lay.root
    assert lay.relative(raw / "sub-01/anat/x.nii.gz") == \
        (raw, "sub-01/anat/x.nii.gz")
    assert lay.relative(raw) == (raw, ".")
    assert lay.relative(raw / "participants.tsv") == \
        (raw, "participants.tsv")
    fs = root / "derivatives/fs"
    assert lay.relative(fs / "sub-01/anat/x.nii.gz") == \
        (fs, "sub-01/anat/x.nii.gz")
    assert lay.relative(fs / "sourcedata/sub-01/mri/aseg.mgz") == \
        (fs, "sourcedata/sub-01/mri/aseg.mgz")
    # files of the study dataset itself
    assert lay.relative(root / "sourcedata/a.tar") == \
        (root, "sourcedata/a.tar")
    assert lay.relative(root / "dataset_description.json") == \
        (root, "dataset_description.json")
    # relative paths are relative to the dataset folder
    assert lay.relative("rawbids/sub-01") == (raw, "sub-01")
    assert lay.relative("derivatives/fs/sub-01") == (fs, "sub-01")
    with pytest.raises(ValueError):
        lay.relative("/elsewhere/sub-01")
    with pytest.raises(ValueError):
        lay.relative("../sub-01")


def test_relative_flat():
    lay = make_layout("flat", "/data/DS")
    root = lay.root
    assert lay.relative(root / "sub-01/x.nii") == (root, "sub-01/x.nii")
    assert lay.relative(root / "sourcedata/a.tar") == \
        (root, "sourcedata/a.tar")
    assert lay.relative(root / "derivatives/fs/sub-01/x") == \
        (root / "derivatives/fs", "sub-01/x")


@pytest.mark.parametrize("kind", ["study", "flat"])
def test_resolve(kind):
    """Paths of a flat BIDS listing (as hosted) placed in the layout"""
    lay = make_layout(kind, "/data/DS")
    assert lay.resolve("sub-01/anat/x.nii.gz") == \
        lay.raw / "sub-01/anat/x.nii.gz"
    assert lay.resolve("participants.tsv") == lay.raw / "participants.tsv"
    assert lay.resolve(".bidsignore") == lay.bidsignore
    assert lay.resolve("phenotype/a.tsv") == lay.phenotype / "a.tsv"
    assert lay.resolve("code/run.sh") == lay.raw / "code/run.sh"
    assert lay.resolve("sourcedata/ICBM/README.txt") == \
        lay.source / "ICBM/README.txt"
    assert lay.resolve("derivatives/fs/sub-01/x.mgz") == \
        lay.derivative("fs").root / "sub-01/x.mgz"
    assert lay.resolve("derivatives/fs/sourcedata/sub-01/x.mgz") == \
        lay.derivative("fs").source / "sub-01/x.mgz"
    assert lay.resolve("derivatives/README") == lay.derivatives / "README"


def test_original():
    lay = make_layout("study", "/data/DS")
    assert lay.original(lay.raw / "participants.tsv") == \
        lay.state / "orig/rawbids/participants.tsv"
    assert lay.derivative("fs").original(
        lay.derivative("fs").root / "sub-01/sub-01_scans.tsv"
    ) == lay.state / "orig/derivatives/fs/sub-01/sub-01_scans.tsv"
    flat = make_layout("flat", "/data/DS")
    assert flat.original(flat.raw / "participants.tsv") == \
        flat.state / "orig/participants.tsv"


# ----------------------------------------------------------------------
#   Sites
# ----------------------------------------------------------------------

def test_sites(tmp_path):
    lay = make_layout("sites", tmp_path / "FCON", children={
        "ICBM": "flat", "Atlanta": "flat",
    })
    assert isinstance(lay, SitesLayout)
    assert lay.child_kind == "flat"           # that of existing sites
    icbm = lay.child("ICBM")
    assert isinstance(icbm, FlatLayout)
    assert icbm.root == tmp_path / "FCON/ICBM"
    assert icbm.state == tmp_path / "FCON/ICBM/.brainspresso"
    # parent: README and merged participants, its own state folder
    assert lay.raw == lay.root
    assert lay.state == tmp_path / "FCON/.brainspresso"
    assert lay.resolve("participants.tsv") == lay.root / "participants.tsv"
    assert lay.resolve("sourcedata/phenotype/a.csv") == \
        lay.source / "phenotype/a.csv"
    assert lay.datasets() == [lay.children["ICBM"], lay.children["Atlanta"]]
    # paths map to the dataset of their site
    path = icbm.root / "derivatives/fcon1000/sub-1/x.nii.gz"
    assert lay.relative(path) == (icbm.root / "derivatives/fcon1000",
                                  "sub-1/x.nii.gz")
    assert lay.relative(icbm.root / "sub-1/x") == (icbm.root, "sub-1/x")
    assert lay.relative(lay.root / "README") == (lay.root, "README")
    # new sites
    assert lay.child("NewYork").kind == "flat"
    mixed = SitesLayout(tmp_path / "X", {"A": "flat", "B": "study"})
    assert mixed.child_kind == DEFAULT_LAYOUT
    assert mixed.child("C", kind="flat").kind == "flat"
    assert lay.to_manifest() == {"layout": "sites", "children": {
        "Atlanta": "flat", "ICBM": "flat", "NewYork": "flat",
    }}


def test_sites_study_children(tmp_path):
    lay = make_layout("sites", tmp_path, child_kind="study")
    site = lay.child("A")
    assert site.raw == tmp_path / "A/rawbids"
    assert relative_url(lay.root, site.raw) == "../../"
    path = site.raw / "sub-1/x"
    assert lay.relative(path) == (site.raw, "sub-1/x")


# ----------------------------------------------------------------------
#   Detection
# ----------------------------------------------------------------------

@pytest.mark.parametrize("kind", ["study", "flat"])
def test_detect_from_manifest(tmp_path, kind):
    lay = make_layout(kind, tmp_path / "DS")
    record_run(lay, "harvest")
    # the tree says nothing (no rawbids/, no sub-*)
    assert Layout.detect(lay.root) == lay
    assert read_manifest(lay.state)["layout"] == kind


def test_detect_from_tree(tmp_path):
    assert Layout.detect(tmp_path / "missing") is None
    assert Layout.detect(tmp_path) is None
    (tmp_path / "sourcedata").mkdir()
    assert Layout.detect(tmp_path) is None      # harvested, not roasted

    study = tmp_path / "study"
    (study / "rawbids").mkdir(parents=True)
    assert Layout.detect(study).kind == "study"

    described = tmp_path / "described"
    write(described / "dataset_description.json", {"DatasetType": "study"})
    assert Layout.detect(described).kind == "study"

    flat = tmp_path / "flat"
    (flat / "sub-01").mkdir(parents=True)
    assert Layout.detect(flat).kind == "flat"

    raw = tmp_path / "raw"
    write(raw / "dataset_description.json", {"DatasetType": "raw"})
    assert Layout.detect(raw).kind == "flat"

    sites = tmp_path / "sites"
    write(sites / "README")
    (sites / "A/sub-01").mkdir(parents=True)
    (sites / "B/rawbids").mkdir(parents=True)
    (sites / "sourcedata/sub-01").mkdir(parents=True)
    found = Layout.detect(sites)
    assert isinstance(found, SitesLayout)
    assert {k: v.kind for k, v in found.children.items()} == {
        "A": "flat", "B": "study",
    }


def test_detect_sites_from_manifest(tmp_path):
    lay = make_layout("sites", tmp_path, child_kind="study")
    for name in ("A", "B"):
        record_run(lay.child(name), "harvest")
    record_run(lay, "harvest")
    found = Layout.detect(tmp_path)
    assert found == lay
    assert found.children["A"].raw == tmp_path / "A/rawbids"


def test_detect_ignores_unknown_manifest_layout(tmp_path):
    write(tmp_path / ".brainspresso/manifest.json", {"layout": "rawdata"})
    (tmp_path / "sub-01").mkdir()
    assert Layout.detect(tmp_path).kind == "flat"


def test_resolve_layout(tmp_path):
    new = tmp_path / "new"
    assert resolve_layout(new).kind == DEFAULT_LAYOUT
    assert resolve_layout(new, "flat").kind == "flat"
    lay = make_layout("flat", tmp_path / "DS")
    assert resolve_layout(lay) is lay
    with pytest.raises(LayoutError):
        resolve_layout(lay, "study")


@pytest.mark.parametrize("recorded", [True, False])
def test_resolve_layout_conflict(tmp_path, recorded):
    lay = make_layout("study", tmp_path / "DS")
    if recorded:
        record_run(lay, "harvest")
    else:
        lay.raw.mkdir(parents=True)
    assert resolve_layout(lay.root, "study") == lay
    assert resolve_layout(lay.root) == lay
    with pytest.raises(LayoutError, match="conflicts with --layout flat"):
        resolve_layout(lay.root, "flat")
    assert issubclass(LayoutError, ValueError)     # user-facing error


def test_resolve_layout_sites_conflict(tmp_path):
    lay = make_layout("sites", tmp_path, child_kind="flat")
    record_run(lay.child("A"), "harvest")
    record_run(lay, "harvest")
    assert resolve_layout(tmp_path, "sites", child_kind="flat") == lay
    with pytest.raises(LayoutError, match="--site-layout study"):
        resolve_layout(tmp_path, "sites", child_kind="study")
    with pytest.raises(LayoutError):
        resolve_layout(tmp_path, "study")


# ----------------------------------------------------------------------
#   Descriptions
# ----------------------------------------------------------------------

RAW_DESCRIPTION = {
    "Name": "IXI", "BIDSVersion": "1.8.0", "DatasetType": "raw",
    "License": "CC BY-SA 3.0", "Authors": ["A", "B"], "Funding": ["F"],
    "GeneratedBy": [{"Name": "x"}],
}


def test_dataset_description():
    lay = make_layout("study", "/data/DS")
    assert lay.dataset_description()["DatasetType"] == "study"
    assert lay.dataset_description("raw") == {
        "Name": "DS", "BIDSVersion": layout_mod.BIDS_VERSION,
        "DatasetType": "raw",
    }
    flat = make_layout("flat", "/data/DS")
    assert flat.dataset_description()["DatasetType"] == "raw"
    drv = lay.derivative("fs").dataset_description()
    assert drv["DatasetType"] == "derivative"
    assert drv["Name"] == "fs"
    assert drv["SourceDatasets"] == [{"URL": "../../rawbids/"}]


def test_write_descriptions_study(tmp_path):
    lay = make_layout("study", tmp_path / "IXI")
    write(lay.description, RAW_DESCRIPTION)
    fs = lay.derivative("fs")
    write(fs.description, {
        "Name": "FS", "BIDSVersion": "1.8.0",
        "SourceDatasets": [{"URL": "../../"}, {"DOI": "doi:10.1/x"},
                           {"URL": "https://example.org/ds"}],
    })
    (lay.derivative("empty").root).mkdir(parents=True)

    written = lay.write_descriptions()
    assert set(written) == {lay.root / "dataset_description.json",
                            fs.description}
    study = read_json(lay.root / "dataset_description.json")
    assert study == {
        "Name": "IXI", "BIDSVersion": layout_mod.BIDS_VERSION,
        "DatasetType": "study", "License": "CC BY-SA 3.0",
        "Authors": ["A", "B"], "Funding": ["F"],
    }
    # the raw description is left as is
    assert read_json(lay.description) == RAW_DESCRIPTION
    derived = read_json(fs.description)
    assert derived["DatasetType"] == "derivative"
    # "../../" pointed at the raw dataset of a flat layout: replaced
    assert derived["SourceDatasets"] == [
        {"URL": "../../rawbids/"}, {"DOI": "doi:10.1/x"},
        {"URL": "https://example.org/ds"},
    ]
    assert not (lay.derivative("empty").description).exists()
    # idempotent
    assert lay.write_descriptions() == []


def test_write_descriptions_flat(tmp_path):
    lay = make_layout("flat", tmp_path / "IXI")
    write(lay.description, RAW_DESCRIPTION)
    fs = lay.derivative("fs")
    write(fs.description, {"Name": "FS", "DatasetType": "derivative",
                           "SourceDatasets": [{"URL": "../../"}]})
    assert lay.write_descriptions() == []
    assert read_json(lay.description) == RAW_DESCRIPTION
    assert read_json(fs.description)["SourceDatasets"] == [{"URL": "../../"}]


def test_write_descriptions_sites(tmp_path):
    lay = make_layout("sites", tmp_path, child_kind="study")
    site = lay.child("A")
    write(site.description, {"Name": "X: A", "DatasetType": "raw"})
    assert lay.write_descriptions() == [site.root / "dataset_description.json"]
    assert read_json(site.root / "dataset_description.json")["Name"] == "X: A"
    assert not (tmp_path / "dataset_description.json").exists()


# ----------------------------------------------------------------------
#   State
# ----------------------------------------------------------------------

def test_record_run(tmp_path):
    lay = make_layout("study", tmp_path)
    assert record_run(lay, "compost", create=False) is None
    assert not lay.manifest.exists()
    record_run(lay, "harvest", {"source": "x"},
               files={"rawbids/a": {"origin": "direct"}})
    record_run(lay, "roast", {"keys": ["meta"]})
    record_run(lay, "compost", deleted=["rawbids/a"], create=False)
    manifest = read_manifest(lay.state)
    assert manifest["schema"] == 1
    assert manifest["layout"] == "study"
    assert [r["stage"] for r in manifest["runs"]] == [
        "harvest", "roast", "compost",
    ]
    assert manifest["runs"][0]["source"] == "x"
    assert "date" in manifest["runs"][0]
    assert manifest["files"] == {}


def test_task_state(tmp_path):
    lay = make_layout("study", tmp_path)
    assert read_task_states(lay.state) == {}
    path = write_task_state(lay.state, "grind/fs/sub-01/ses-1", {
        "stage": "grind", "status": "done", "unit": {"sub": "01"},
    })
    assert path == task_path(lay.state, "grind/fs/sub-01/ses-1")
    assert path.parent == lay.tasks
    assert "/" not in path.name and path.suffix == ".json"
    write_task_state(lay.state, "roast/shard-1", {"status": "failed"})
    states = read_task_states(lay.state)
    assert set(states) == {"grind/fs/sub-01/ses-1", "roast/shard-1"}
    assert states["grind/fs/sub-01/ses-1"]["schema"] == 1
    assert states["grind/fs/sub-01/ses-1"]["status"] == "done"
    assert not list(lay.tasks.glob("*.tmp"))


@pytest.mark.parametrize("kind", ["study", "flat"])
def test_harvest_places_files(tmp_path, kind):
    """A hosted BIDS listing goes into `layout.raw` (and next to it)"""
    entries = [
        InlineEntry("dataset_description.json", json.dumps(
            {"Name": "ds", "BIDSVersion": "1.9.0", "DatasetType": "raw"})),
        InlineEntry("participants.tsv",
                    "participant_id\tage\nsub-01\t30\nsub-02\t40\n"),
        InlineEntry("sub-01/anat/sub-01_T1w.json", "{}"),
        InlineEntry("sourcedata/notes.txt", "x"),
        InlineEntry("derivatives/fs/dataset_description.json", json.dumps(
            {"Name": "fs", "SourceDatasets": [{"URL": "../../"}]})),
        InlineEntry("derivatives/fs/sub-01/anat/sub-01_dseg.json", "{}"),
    ]
    root = tmp_path / "ds"
    harvest(entries, root, Selection(folders=["sourcedata", "derivatives"]),
            layout=kind)
    lay = Layout.detect(root)
    assert lay.kind == kind
    assert (lay.raw / "sub-01/anat/sub-01_T1w.json").exists()
    assert (lay.source / "notes.txt").exists()
    fs = lay.derivative("fs")
    assert (fs.root / "sub-01/anat/sub-01_dseg.json").exists()
    # trimmed (sub-02 was not downloaded), original kept in the state
    assert (lay.raw / "participants.tsv").read_text().splitlines() == [
        "participant_id\tage", "sub-01\t30",
    ]
    assert "sub-02" in lay.original(lay.raw / "participants.tsv").read_text()
    # descriptions for the layout
    assert read_json(fs.description)["SourceDatasets"][0] == \
        {"URL": fs.source_url}
    if kind == "study":
        assert read_json(root / "dataset_description.json")["DatasetType"] \
            == "study"
    # manifest: layout, paths relative to the dataset folder, origin
    manifest = read_manifest(lay.state)
    assert manifest["layout"] == kind
    raw = "" if kind == "flat" else "rawbids/"
    assert manifest["files"][f"{raw}sub-01/anat/sub-01_T1w.json"] == {
        "origin": "generated", "size": 2,
    }
    assert "sourcedata/notes.txt" in manifest["files"]
    assert "derivatives/fs/sub-01/anat/sub-01_dseg.json" in manifest["files"]
    assert manifest["runs"][-1]["stage"] == "harvest"
    # a later harvest cannot change the layout
    other = {"study": "flat", "flat": "study"}[kind]
    with pytest.raises(LayoutError):
        harvest(entries, root, Selection(), layout=other)


def test_no_rawdata_anywhere():
    """No module builds the historical `rawdata/` folder"""
    package = Path(layout_mod.__file__).parents[1]
    offenders = []
    for path in package.rglob("*.py"):
        for line in path.read_text().splitlines():
            if ("'rawdata'" in line or '"rawdata"' in line):
                offenders.append(f"{path.relative_to(package)}: {line}")
    assert offenders == []
