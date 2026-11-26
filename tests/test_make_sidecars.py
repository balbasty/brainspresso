"""Tests for `scripts/make_sidecars.py` (protocol printouts -> sidecars)."""
import configparser
import importlib.util
import io
import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DATASETS = ("ABIDE1", "ABIDE2", "CoRR")


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "make_sidecars", ROOT / "scripts" / "make_sidecars.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ms = _load_script()


def _lock(dataset):
    path = ROOT / "printouts" / dataset / "sidecars.lock.json"
    if not path.exists():
        pytest.skip("sidecars not generated yet")
    return json.loads(path.read_text())


@pytest.mark.parametrize("dataset", DATASETS)
def test_table_loads_and_covers_templates(dataset):
    entries = ms.load_table(dataset)
    assert entries
    assert ms.uncovered(dataset, entries) == []
    outputs = {e["output"] for e in entries}
    tpl = ms.template_dir(dataset)
    for out in outputs:
        assert (tpl / out).is_file(), out


@pytest.mark.parametrize("dataset", DATASETS)
def test_lock_matches_table(dataset):
    entries = ms.load_table(dataset)
    lock = _lock(dataset)
    assert set(lock["sidecars"]) == {e["output"] for e in entries}
    pdir = ROOT / "printouts" / dataset
    for e in entries:
        if e.get("manual"):
            continue
        rec = lock["sidecars"][e["output"]]
        assert rec["printout"] == e["printout"]
        assert rec["sha256"] == ms.sha256(pdir / e["printout"])


def test_known_paths():
    assert any(e["output"] == "BNI/1/T1w.json"
               and e["printout"] == "BNI_1/anat.txt"
               for e in ms.load_table("ABIDE2"))
    assert any(e["output"] == "Caltech/T1w.json"
               for e in ms.load_table("ABIDE1"))
    assert any(e["output"] == "BMB/1/T1w.json"
               for e in ms.load_table("CoRR"))


def test_table_validation(tmp_path):
    pdir = tmp_path / "printouts" / "ABIDE1"
    pdir.mkdir(parents=True)
    (pdir / "sidecars.toml").write_text(
        '[[sidecar]]\noutput = "X/T1w.json"\nprintout = "missing.pdf"\n')
    with pytest.raises(ms.TableError):
        ms.load_table("ABIDE1", root=tmp_path)
    (pdir / "sidecars.toml").write_text(
        '[[sidecar]]\noutput = "X/T1w.json"\n')
    with pytest.raises(ms.TableError):
        ms.load_table("ABIDE1", root=tmp_path)
    (pdir / "sidecars.toml").write_text(
        '[[sidecar]]\noutput = "X/T1w.json"\nmanual = true\nfoo = 1\n')
    with pytest.raises(ms.TableError):
        ms.load_table("ABIDE1", root=tmp_path)


def _synthetic_tree(tmp_path):
    root = tmp_path
    pdir = root / "printouts" / "ABIDE1"
    pdir.mkdir(parents=True)
    (pdir / "S").mkdir()
    (pdir / "S" / "anat.pdf").write_bytes(b"fake printout")
    (pdir / "sidecars.toml").write_text(
        '[[sidecar]]\noutput = "S/T1w.json"\nprintout = "S/anat.pdf"\n'
        'select = "1"\n'
        'defaults = { MagneticFieldStrength = 3 }\n'
        'assigns = { Manufacturer = "Acme" }\n\n'
        '[[sidecar]]\noutput = "S/bold.json"\nmanual = true\n')
    tpl = root / ms.TEMPLATES["ABIDE1"] / "S"
    tpl.mkdir(parents=True)
    (tpl / "T1w.json").write_text(json.dumps({"EchoTime": 0.1}))
    (tpl / "bold.json").write_text("{}")
    return root


def test_synthetic_run(tmp_path, monkeypatch):
    root = _synthetic_tree(tmp_path)
    calls = []

    def fake_convert(printout, select=None, nii=None):
        calls.append((Path(printout).name, select, nii))
        return {"Manufacturer": "Siemens", "EchoTime": 0.002}

    monkeypatch.setattr(ms, "convert", fake_convert)
    monkeypatch.setattr(ms, "protocol2bids_version",
                        lambda: {"version": "9.9", "commit": "abc"})
    out = io.StringIO()
    assert ms.run("ABIDE1", check=True, root=root, out=out) == 1
    assert json.loads(
        (root / ms.TEMPLATES["ABIDE1"] / "S/T1w.json").read_text()
    ) == {"EchoTime": 0.1}

    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    assert calls[-1] == ("anat.pdf", "1", None)
    path = root / ms.TEMPLATES["ABIDE1"] / "S/T1w.json"
    text = path.read_text()
    assert json.loads(text) == {
        "MagneticFieldStrength": 3, "Manufacturer": "Acme",
        "EchoTime": 0.002}
    assert text.endswith("}\n") and '\n    "' in text
    assert (root / ms.TEMPLATES["ABIDE1"] / "S/bold.json").read_text() \
        == "{}"

    pdir = root / "printouts" / "ABIDE1"
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert lock["protocol2bids"] == {"version": "9.9", "commit": "abc"}
    rec = lock["sidecars"]["S/T1w.json"]
    assert rec["printout"] == "S/anat.pdf"
    assert rec["select"] == "1"
    assert rec["sha256"] == ms.sha256(pdir / "S/anat.pdf")
    assert lock["sidecars"]["S/bold.json"] == {"manual": True}
    report = (pdir / "report.md").read_text()
    assert "changed `EchoTime`: 0.1 -> 0.002" in report
    assert "added `Manufacturer`" in report

    # unchanged tree: --check passes
    assert ms.run("ABIDE1", check=True, root=root, out=io.StringIO()) == 0
    # changed printout: --check fails
    (pdir / "S" / "anat.pdf").write_bytes(b"other printout")
    assert ms.run("ABIDE1", check=True, root=root, out=io.StringIO()) == 1


def test_uncovered_sidecar_is_an_error(tmp_path, monkeypatch):
    root = _synthetic_tree(tmp_path)
    (root / ms.TEMPLATES["ABIDE1"] / "S/dwi.json").write_text("{}")
    with pytest.raises(ms.TableError):
        ms.run("ABIDE1", root=root, out=io.StringIO())


def test_diff():
    added, removed, changed = ms.diff({"a": 1, "b": 2}, {"b": 3, "c": 4})
    assert added == {"c": 4}
    assert removed == {"a": 1}
    assert changed == {"b": (2, 3)}


@pytest.mark.parametrize("dataset", DATASETS)
def test_check_real(dataset):
    """Committed sidecars are up to date (needs protocol2bids)."""
    pytest.importorskip("protocol2bids")
    lock = _lock(dataset)
    if ms.protocol2bids_version() != lock["protocol2bids"]:
        pytest.skip("installed protocol2bids differs from the lock")
    assert ms.run(dataset, check=True, out=io.StringIO()) == 0


def test_printouts_not_packaged():
    cfg = configparser.ConfigParser()
    cfg.read(ROOT / "setup.cfg")
    include = cfg["options.packages.find"]["include"].split()
    assert all(p.startswith("brainspresso") for p in include)
    data = cfg["options.package_data"]["*"]
    assert "printouts" not in data and "PDFs" not in data
    assert "datasets/*/templates/*/*" in data
    assert "protocol2bids" in cfg["options.extras_require"]["maint"]
    for extra in ("test", "all"):
        if extra in cfg["options.extras_require"]:
            value = cfg["options.extras_require"][extra]
            assert "protocol2bids" not in value
            assert "maint" not in value
    assert not (ROOT / "printouts" / "__init__.py").exists()
    assert not (ROOT / "MANIFEST.in").exists() or "printouts" not in (
        ROOT / "MANIFEST.in").read_text()
    for ds in ("ABIDE/II", "CoRR"):
        assert not (ROOT / "brainspresso/datasets" / ds / "templates/PDFs"
                    ).exists()


@pytest.mark.skipif(shutil.which("git") is None, reason="needs git")
def test_no_printouts_tracked_in_package():
    res = subprocess.run(
        ["git", "ls-files", "brainspresso"], cwd=ROOT,
        capture_output=True, text=True)
    if res.returncode != 0:
        pytest.skip("not a git checkout")
    files = res.stdout.splitlines()
    assert not [f for f in files if "/PDFs/" in f]
