"""Tests of `scripts/migrate_layout.py` (old `rawdata/` layout)."""
import importlib.util
import json
from pathlib import Path

import pytest

from brainspresso.bids.harvest import read_manifest
from brainspresso.bids.layout import Layout

SCRIPT = Path(__file__).parents[1] / "scripts" / "migrate_layout.py"
_spec = importlib.util.spec_from_file_location("migrate_layout", SCRIPT)
mig = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mig)

DESC = {"Name": "X", "BIDSVersion": "1.8.0", "License": "CC0"}


def write(path: Path, content="x"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content if isinstance(content, str)
                    else json.dumps(content))


def files(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix()
            for p in root.rglob("*") if p.is_file()}


def snapshot(root: Path) -> dict:
    return {f: (root / f).read_bytes() for f in files(root)}


def common(ds: Path):
    write(ds / "dataset_description.json", DESC)
    write(ds / "README")
    write(ds / "participants.tsv", "participant_id\nsub-01\n")
    write(ds / "participants.json", "{}")
    write(ds / "sourcedata" / "archive.tar", "big")
    write(ds / "rawdata" / "sub-01" / "anat" / "sub-01_T1w.nii.gz")


def ixi(ds):
    common(ds)
    write(ds / "rawdata" / "dwi.bval")
    write(ds / "rawdata" / "dwi.bvec")
    write(ds / "rawdata" / "sub-01" / "dwi" / "sub-01_dwi.nii.gz")


def oasis2(ds):
    common(ds)
    write(ds / "sessions.json", "{}")
    write(ds / "rawdata" / "sub-01" / "sub-01_sessions.tsv")


def oasis3(ds, same=True):
    common(ds)
    write(ds / "sessions.json", "{}")
    write(ds / "rawdata" / "dataset_description.json",
          DESC if same else {"Name": "other"})
    write(ds / "rawdata" / "phenotype" / "x.tsv")
    drv = ds / "derivatives" / "freesurfer"
    write(drv / "dataset_description.json",
          {"Name": "fs", "SourceDatasets": [{"URL": "../../rawdata/"}]})
    write(drv / "sourcedata" / "fs.tar.gz")
    write(drv / "sub-01" / "stats.txt")


def abide(ds):
    common(ds)
    write(ds / "phenotype" / "ados.tsv")
    write(ds / "phenotype" / "ados.json", "{}")


SHAPES = {"ixi": ixi, "oasis2": oasis2, "oasis3": oasis3, "abide": abide}
ROOT_RAW = {"dataset_description.json", "README", "participants.tsv",
            "participants.json", "sessions.json"}


def old_raw(ds: Path) -> set[str]:
    out = {f[len("rawdata/"):] for f in files(ds) if f.startswith("rawdata/")}
    return out | {f for f in files(ds) if f in ROOT_RAW
                  or f.startswith("phenotype/")}


@pytest.mark.parametrize("kind", ["study", "flat"])
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_migrate(tmp_path, shape, kind):
    ds = tmp_path / "DS"
    SHAPES[shape](ds)
    raw_before = old_raw(ds)
    before = snapshot(ds)
    assert mig.main([str(ds), "--layout", kind]) == 0
    assert snapshot(ds) == before              # dry run moves nothing

    assert mig.main([str(ds), "--layout", kind, "--apply"]) == 0
    layout = Layout.detect(ds)
    assert layout.kind == kind
    assert read_manifest(ds / ".brainspresso")["layout"] == kind
    assert not (ds / "rawdata").exists()
    raw_after = {f for f in files(layout.raw)
                 if not f.startswith((".brainspresso/", "sourcedata/",
                                      "derivatives/"))}
    if kind == "study":
        assert raw_after == raw_before
        top = json.loads((ds / "dataset_description.json").read_text())
        assert top["DatasetType"] == "study" and top["License"] == "CC0"
    else:
        assert raw_after == raw_before
    assert (ds / "sourcedata" / "archive.tar").read_text() == "big"
    for f, data in before.items():
        if f.startswith("derivatives/") and not f.endswith(
                "dataset_description.json"):
            assert (ds / f).read_bytes() == data
    for drv in layout.list_derivatives():
        desc = json.loads(drv.description.read_text())
        url = "../../rawbids/" if kind == "study" else "../../"
        assert desc["SourceDatasets"][0] == {"URL": url}
        assert {"URL": "../../rawdata/"} not in desc["SourceDatasets"]
    log = (ds / ".brainspresso" / "migrate.log").read_text()
    assert "move rawdata/sub-01" in log

    # idempotence: second run refuses
    before = snapshot(ds)
    assert mig.main([str(ds), "--layout", kind, "--apply"]) == 1
    assert snapshot(ds) == before


@pytest.mark.parametrize("kind", ["study", "flat"])
def test_different_duplicate_saved(tmp_path, kind):
    ds = tmp_path / "DS"
    oasis3(ds, same=False)
    assert mig.main([str(ds), "--layout", kind, "--apply"]) == 0
    saved = ds / ".brainspresso" / "orig" / "rawdata_dataset_description.json"
    assert json.loads(saved.read_text()) == {"Name": "other"}
    raw = Layout.detect(ds).raw
    assert json.loads((raw / "dataset_description.json").read_text()) == DESC


def _refused(ds, kind="study"):
    before = snapshot(ds)
    with pytest.raises(mig.Refusal):
        mig.migrate(ds, kind, apply=True)
    assert snapshot(ds) == before


def test_refuse_no_rawdata(tmp_path):
    write(tmp_path / "DS" / "README")
    _refused(tmp_path / "DS")


def test_refuse_rawbids(tmp_path):
    ds = tmp_path / "DS"
    ixi(ds)
    write(ds / "rawbids" / "README")
    _refused(ds)


def test_refuse_root_sub(tmp_path):
    ds = tmp_path / "DS"
    ixi(ds)
    write(ds / "sub-02" / "anat" / "x.nii.gz")
    _refused(ds, "flat")


def test_refuse_conflict(tmp_path):
    ds = tmp_path / "DS"
    ixi(ds)
    write(ds / "rawdata" / "README", "different")
    _refused(ds, "study")
    _refused(ds, "flat")


def test_refuse_unknown_root_entry(tmp_path):
    ds = tmp_path / "DS"
    ixi(ds)
    write(ds / "notes" / "todo.txt")
    _refused(ds)


def test_refuse_recorded_layout(tmp_path):
    ds = tmp_path / "DS"
    ixi(ds)
    write(ds / ".brainspresso" / "manifest.json", {"layout": "flat"})
    _refused(ds)


@pytest.mark.parametrize("kind", ["study", "flat"])
def test_stdlib_backend_matches(tmp_path, monkeypatch, kind):
    oasis3(tmp_path / "lib")
    mig.migrate(tmp_path / "lib", kind, apply=True)
    monkeypatch.setattr(mig, "BACKEND", "stdlib")
    oasis3(tmp_path / "std")
    mig.migrate(tmp_path / "std", kind, apply=True)
    for f in files(tmp_path / "lib"):
        if f.endswith("dataset_description.json"):
            assert json.loads((tmp_path / "lib" / f).read_text()) == \
                json.loads((tmp_path / "std" / f).read_text()), f
    assert Layout.detect(tmp_path / "std").kind == kind
    man = read_manifest(tmp_path / "std" / ".brainspresso")
    assert man["schema"] == 1 and man["runs"][-1]["stage"] == "migrate"


def test_recursive(tmp_path):
    ixi(tmp_path / "IXI")
    abide(tmp_path / "ABIDE")
    write(tmp_path / "other" / "README")
    assert mig.main([str(tmp_path), "--layout", "flat", "--recursive",
                     "--apply"]) == 0
    assert Layout.detect(tmp_path / "IXI").kind == "flat"
    assert Layout.detect(tmp_path / "ABIDE").kind == "flat"
