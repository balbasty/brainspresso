"""Tests for `scripts/make_sidecars.py` (protocol printouts -> sidecars)."""
import configparser
import importlib.util
import io
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DATASETS = ("ABIDE1", "ABIDE2", "CoRR", "MPI-LEMON", "NKI-RS", "SALD",
            "SLIM")
# INDI projects: bucket-hosted sidecars, fill-in templates
INDI = ("MPI-LEMON", "NKI-RS", "SALD", "SLIM")


# make_sidecars reads TOML with tomllib (3.11+), else tomli
pytest.importorskip("tomllib" if sys.version_info >= (3, 11) else "tomli")


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
        if "common" in e:
            assert rec["common"] == e["common"]
            continue
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
    assert any(e["output"] == "task-rest_bold.json"
               and e["printout"] == "rest.pdf"
               for e in ms.load_table("SLIM"))
    # SALD uses SLIM's (byte-identical) printouts by reference
    assert any(e["printout"] == "../SLIM/anat.pdf"
               for e in ms.load_table("SALD"))
    assert ms.template_dir("NKI-RS").as_posix().endswith(
        "brainspresso/datasets/INDI/templates/nki-rs")


@pytest.mark.parametrize("dataset", INDI)
def test_indi_entries_are_fill_in(dataset):
    """INDI templates fill the bucket's sidecars in (flag in table and
    lock), and are named after BIDS files (no sub/ses/run entity)."""
    entries = ms.load_table(dataset)
    lock = _lock(dataset)
    for e in entries:
        assert e.get(ms.FILL_KEY) is True, e["output"]
        assert lock["sidecars"][e["output"]][ms.LOCK_FILL_KEY] is True
        name = e["output"].rsplit("/", 1)[-1]
        assert not any(name.startswith(x) or f"_{x}" in name
                       for x in ("sub-", "ses-", "run-")), name
        if name.endswith("_bold.json"):
            task = name.split("task-", 1)[1].split("_", 1)[0]
            tpl = json.loads((ms.template_dir(dataset) / e["output"])
                             .read_text(encoding="utf-8"))
            assert tpl["TaskName"] == task


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
    # manual entries take no conversion key
    for key, value in [("nii", '"x.nii"'), ("select", '"0"'),
                       ("defaults", "{ A = 1 }"), ("assigns", "{ A = 1 }"),
                       ("fill_from", '"x.pdf"'), ("fill_select", "1")]:
        (pdir / "sidecars.toml").write_text(
            '[[sidecar]]\noutput = "X/T1w.json"\nmanual = true\n'
            f'{key} = {value}\n')
        with pytest.raises(ms.TableError, match=key):
            ms.load_table("ABIDE1", root=tmp_path)
    (pdir / "sidecars.toml").write_text(
        '[[sidecar]]\noutput = "X/T1w.json"\nmanual = true\n'
        'note = "n"\nprevious = "Y/T1w.json"\n')
    assert ms.load_table("ABIDE1", root=tmp_path)


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


def _fake_p2b(monkeypatch, values):
    monkeypatch.setattr(ms, "convert",
                        lambda p, s=None, n=None: dict(values[Path(p).name]))
    monkeypatch.setattr(ms, "protocol2bids_version",
                        lambda: {"version": "9.9", "commit": "abc"})


def test_only_merges_report_and_lock(tmp_path, monkeypatch):
    """`--only` keeps the report sections and lock entries of the other
    sidecars."""
    root = _synthetic_tree(tmp_path)
    pdir = root / "printouts" / "ABIDE1"
    (pdir / "S" / "rest.pdf").write_bytes(b"rest printout")
    toml = pdir / "sidecars.toml"
    toml.write_text(toml.read_text() + (
        '\n[[sidecar]]\noutput = "S/dwi.json"\nprintout = "S/rest.pdf"\n'))
    tpl = root / ms.TEMPLATES["ABIDE1"] / "S"
    (tpl / "dwi.json").write_text("{}")
    values = {"anat.pdf": {"EchoTime": 0.002},
              "rest.pdf": {"EchoTime": 0.03}}
    _fake_p2b(monkeypatch, values)
    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    full_report = (pdir / "report.md").read_text()
    full_lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert "changed `EchoTime`: 0.1 -> 0.002" in full_report

    # regenerate only dwi, after its printout changed
    (pdir / "S" / "rest.pdf").write_bytes(b"new rest printout")
    values["rest.pdf"] = {"EchoTime": 0.04}
    assert ms.run("ABIDE1", root=root, out=io.StringIO(),
                  only="S/dwi*") == 0
    report = (pdir / "report.md").read_text()
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    sections = ms.report_sections(report)
    assert list(sections) == ["S/T1w.json", "S/bold.json", "S/dwi.json"]
    assert sections["S/T1w.json"] == \
        ms.report_sections(full_report)["S/T1w.json"]
    assert lock["sidecars"]["S/T1w.json"] == \
        full_lock["sidecars"]["S/T1w.json"]
    assert lock["sidecars"]["S/bold.json"] == {"manual": True}
    assert lock["sidecars"]["S/dwi.json"]["sha256"] == \
        ms.sha256(pdir / "S/rest.pdf")
    assert "changed `EchoTime`: 0.03 -> 0.04" in sections["S/dwi.json"]


def _git(root, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t",
                    *args], cwd=root, check=True, capture_output=True)


def test_baseline_errors(tmp_path, monkeypatch):
    root = _synthetic_tree(tmp_path)
    _fake_p2b(monkeypatch, {"anat.pdf": {"EchoTime": 0.002}})
    # unknown --baseline ref
    with pytest.raises(ms.TableError, match="nosuchref"):
        ms.run("ABIDE1", root=root, out=io.StringIO(), baseline="nosuchref")
    pdir = root / "printouts" / "ABIDE1"
    tpl = root / ms.TEMPLATES["ABIDE1"] / "S"
    toml = pdir / "sidecars.toml"
    # outside git, a `previous` deleted from the working copy is not fatal
    (tpl / "T1w.json").unlink()
    toml.write_text(toml.read_text().replace(
        'select = "1"\n', 'select = "1"\nprevious = "S/old.json"\n'))
    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    assert "added `EchoTime`" in (pdir / "report.md").read_text()


@pytest.mark.skipif(shutil.which("git") is None, reason="needs git")
def test_previous_missing_at_git_baseline(tmp_path, monkeypatch):
    """Checked for every entry before anything is written."""
    root = _synthetic_tree(tmp_path)
    pdir = root / "printouts" / "ABIDE1"
    tpl = root / ms.TEMPLATES["ABIDE1"] / "S"
    (pdir / "S" / "rest.pdf").write_bytes(b"rest printout")
    toml = pdir / "sidecars.toml"
    # S/T1w.json (written first) is fine; S/dwi.json has a bad previous
    toml.write_text(toml.read_text() + (
        '\n[[sidecar]]\noutput = "S/dwi.json"\nprintout = "S/rest.pdf"\n'
        'previous = "S/old.json"\n'))
    _git(root, "init", "-q")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "init")
    (tpl / "dwi.json").write_text("{}")     # not committed
    _fake_p2b(monkeypatch, {"anat.pdf": {"EchoTime": 0.002},
                            "rest.pdf": {"EchoTime": 0.03}})
    before = {p: p.read_bytes() for p in tpl.iterdir()}
    with pytest.raises(ms.TableError, match="previous S/old.json"):
        ms.run("ABIDE1", root=root, out=io.StringIO())
    assert {p: p.read_bytes() for p in tpl.iterdir()} == before
    assert not (pdir / "sidecars.lock.json").exists()
    assert not (pdir / "report.md").exists()


def test_unreadable_baseline_still_regenerates(tmp_path, monkeypatch):
    root = _synthetic_tree(tmp_path)
    _fake_p2b(monkeypatch, {"anat.pdf": {"EchoTime": 0.002}})
    pdir = root / "printouts" / "ABIDE1"
    tpl = root / ms.TEMPLATES["ABIDE1"] / "S"
    (tpl / "T1w.json").write_text("{not json")
    out = io.StringIO()
    assert ms.run("ABIDE1", root=root, out=out) == 0
    assert "diff unavailable: unreadable baseline (JSONDecodeError" \
        in out.getvalue()
    assert "## S/bold.json\n\nmanual" in out.getvalue()
    assert json.loads((tpl / "T1w.json").read_text())["EchoTime"] == 0.002
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert lock["sidecars"]["S/T1w.json"]["sha256"] == \
        ms.sha256(pdir / "S/anat.pdf")


def test_provenance_per_entry(tmp_path, monkeypatch):
    """`--only` keeps the protocol2bids version and baseline of the kept
    entries; the top-level version becomes "mixed"."""
    root = _synthetic_tree(tmp_path)
    pdir = root / "printouts" / "ABIDE1"
    (pdir / "S" / "rest.pdf").write_bytes(b"rest printout")
    toml = pdir / "sidecars.toml"
    toml.write_text(toml.read_text() + (
        '\n[[sidecar]]\noutput = "S/dwi.json"\nprintout = "S/rest.pdf"\n'))
    (root / ms.TEMPLATES["ABIDE1"] / "S/dwi.json").write_text("{}")
    v1 = {"version": "1.0", "commit": "a" * 40}
    v2 = {"version": "2.0", "commit": "b" * 40}
    monkeypatch.setattr(ms, "convert",
                        lambda p, s=None, n=None: {"EchoTime": 0.002})
    monkeypatch.setattr(ms, "protocol2bids_version", lambda: v1)
    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert lock["protocol2bids"] == v1
    assert lock["sidecars"]["S/T1w.json"]["protocol2bids"] == v1
    assert "protocol2bids" not in lock["sidecars"]["S/bold.json"]

    monkeypatch.setattr(ms, "protocol2bids_version", lambda: v2)
    assert ms.run("ABIDE1", root=root, out=io.StringIO(),
                  only="S/dwi*") == 0
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert lock["protocol2bids"] == "mixed"
    assert lock["sidecars"]["S/T1w.json"]["protocol2bids"] == v1
    assert lock["sidecars"]["S/dwi.json"]["protocol2bids"] == v2
    sections = ms.report_sections((pdir / "report.md").read_text())
    assert sections["S/T1w.json"].startswith(
        "protocol2bids 1.0 (commit aaaaaaa); baseline `HEAD`")
    assert sections["S/dwi.json"].startswith(
        "protocol2bids 2.0 (commit bbbbbbb); baseline `HEAD`")


def test_common_entry(tmp_path, monkeypatch):
    root = _synthetic_tree(tmp_path)
    pdir = root / "printouts" / "ABIDE1"
    tpl = root / ms.TEMPLATES["ABIDE1"]
    for x in ("A", "B"):
        (pdir / "S" / f"{x}.pdf").write_bytes(x.encode())
        (tpl / x).mkdir()
        (tpl / x / "bold.json").write_text("{}")
    (tpl / "S" / "bold.json").write_text("{}")
    (pdir / "sidecars.toml").write_text(
        '[[sidecar]]\noutput = "S/T1w.json"\nprintout = "S/anat.pdf"\n\n'
        '[[sidecar]]\noutput = "S/bold.json"\n'
        'common = ["A/bold.json", "B/bold.json"]\n\n'
        '[[sidecar]]\noutput = "A/bold.json"\nprintout = "S/A.pdf"\n\n'
        '[[sidecar]]\noutput = "B/bold.json"\nprintout = "S/B.pdf"\n')
    _fake_p2b(monkeypatch, {
        "anat.pdf": {"EchoTime": 0.002},
        "A.pdf": {"EchoTime": 0.03, "RepetitionTime": 2.0, "X": [1]},
        "B.pdf": {"EchoTime": 0.03, "RepetitionTime": 1.5, "X": [1]}})
    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    assert json.loads((tpl / "S/bold.json").read_text()) == {
        "EchoTime": 0.03, "X": [1]}
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert lock["sidecars"]["S/bold.json"]["common"] == [
        "A/bold.json", "B/bold.json"]
    assert ms.run("ABIDE1", root=root, out=io.StringIO(), check=True) == 0
    # regenerated when one of its sidecars is (--only)
    (tpl / "S/bold.json").write_text("{}")
    assert ms.run("ABIDE1", root=root, out=io.StringIO(),
                  only="A/*", check=True) == 1
    # validation
    toml = pdir / "sidecars.toml"
    for bad in ('common = ["A/bold.json"]', 'common = ["A/bold.json", '
                '"Z/bold.json"]', 'common = ["A/bold.json", "B/bold.json"]'
                '\nnii = "x"',
                # not strings, not distinct, with manual
                'common = ["A/bold.json", ["B/bold.json"]]',
                'common = ["A/bold.json", 1]',
                'common = "A/bold.json"',
                'common = ["A/bold.json", "A/bold.json"]',
                'common = ["A/bold.json", "B/bold.json"]\nmanual = true'):
        toml.write_text(
            '[[sidecar]]\noutput = "S/bold.json"\n' + bad + '\n\n'
            '[[sidecar]]\noutput = "A/bold.json"\nprintout = "S/A.pdf"\n\n'
            '[[sidecar]]\noutput = "B/bold.json"\nprintout = "S/B.pdf"\n')
        with pytest.raises(ms.TableError):
            ms.load_table("ABIDE1", root=root)


def _common_tree(tmp_path):
    root = _synthetic_tree(tmp_path)
    pdir = root / "printouts" / "ABIDE1"
    tpl = root / ms.TEMPLATES["ABIDE1"]
    for x in ("A", "B"):
        (pdir / "S" / f"{x}.pdf").write_bytes(x.encode())
        (tpl / x).mkdir()
        (tpl / x / "bold.json").write_text("{}")
    (tpl / "S" / "bold.json").write_text("{}")
    (pdir / "sidecars.toml").write_text(
        '[[sidecar]]\noutput = "S/T1w.json"\nprintout = "S/anat.pdf"\n\n'
        '[[sidecar]]\noutput = "S/bold.json"\n'
        'common = ["A/bold.json", "B/bold.json"]\n\n'
        '[[sidecar]]\noutput = "A/bold.json"\nprintout = "S/A.pdf"\n\n'
        '[[sidecar]]\noutput = "B/bold.json"\nprintout = "S/B.pdf"\n')
    return root, pdir, tpl


def test_common_canonical_values():
    got = ms.common_keys([
        {"a": False, "b": 90, "c": [1, 2], "d": {"x": 1, "y": 2}, "e": 1},
        {"a": 0, "b": 90.0, "c": [1, 2], "d": {"y": 2, "x": 1}, "e": 1}])
    assert got == {"c": [1, 2], "d": {"x": 1, "y": 2}, "e": 1}


def test_common_provenance_from_sources(tmp_path, monkeypatch):
    """A common entry takes the protocol2bids of its sources' records."""
    root, pdir, tpl = _common_tree(tmp_path)
    v1 = {"version": "1.0", "commit": "a" * 40}
    v2 = {"version": "2.0", "commit": "b" * 40}
    monkeypatch.setattr(ms, "convert",
                        lambda p, s=None, n=None: {"EchoTime": 0.03})
    monkeypatch.setattr(ms, "protocol2bids_version", lambda: v1)
    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    # only A regenerated, with v2: S/bold.json (common) is "mixed"
    monkeypatch.setattr(ms, "protocol2bids_version", lambda: v2)
    assert ms.run("ABIDE1", root=root, out=io.StringIO(), only="A/*") == 0
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert lock["sidecars"]["A/bold.json"]["protocol2bids"] == v2
    assert lock["sidecars"]["B/bold.json"]["protocol2bids"] == v1
    assert lock["sidecars"]["S/bold.json"]["protocol2bids"] == "mixed"
    sections = ms.report_sections((pdir / "report.md").read_text())
    assert sections["S/bold.json"].startswith(
        "protocol2bids mixed; baseline `HEAD`")
    # protocol2bids not installed: no version recorded
    monkeypatch.setattr(ms, "protocol2bids_version",
                        lambda: {"version": None, "commit": None})
    assert ms.run("ABIDE1", root=root, out=io.StringIO(), only="B/*") == 0
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert "protocol2bids" not in lock["sidecars"]["B/bold.json"]
    # A (v2) is the only source with a version left
    assert lock["sidecars"]["S/bold.json"]["protocol2bids"] == v2
    sections = ms.report_sections((pdir / "report.md").read_text())
    assert sections["B/bold.json"].startswith("baseline `HEAD`")
    assert "None" not in (pdir / "report.md").read_text()


@pytest.mark.parametrize("baseline", ["null", "[]", "3"])
def test_baseline_not_an_object(tmp_path, monkeypatch, baseline):
    root = _synthetic_tree(tmp_path)
    _fake_p2b(monkeypatch, {"anat.pdf": {"EchoTime": 0.002}})
    tpl = root / ms.TEMPLATES["ABIDE1"] / "S"
    (tpl / "T1w.json").write_text(baseline)
    out = io.StringIO()
    assert ms.run("ABIDE1", root=root, out=out) == 0
    assert "diff unavailable: unreadable baseline (ValueError: not an " \
        "object" in out.getvalue()
    assert json.loads((tpl / "T1w.json").read_text())["EchoTime"] == 0.002


def test_uncovered_sidecar_is_an_error(tmp_path, monkeypatch):
    root = _synthetic_tree(tmp_path)
    (root / ms.TEMPLATES["ABIDE1"] / "S/dwi.json").write_text("{}")
    with pytest.raises(ms.TableError):
        ms.run("ABIDE1", root=root, out=io.StringIO())


def test_fill_flag(tmp_path, monkeypatch):
    """`fill` marks fill-in templates: recorded in the lock (also for
    manual entries and for entries kept by `--only`), conversion unchanged."""
    root = _synthetic_tree(tmp_path)
    pdir = root / "printouts" / "ABIDE1"
    toml = pdir / "sidecars.toml"
    plain = toml.read_text()
    _fake_p2b(monkeypatch, {"anat.pdf": {"EchoTime": 0.002}})
    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    tpl = root / ms.TEMPLATES["ABIDE1"] / "S/T1w.json"
    before = tpl.read_text()

    toml.write_text(plain.replace(
        'select = "1"\n', 'select = "1"\nfill = true\n').replace(
        'manual = true\n', 'manual = true\nfill = true\n'))
    assert ms.run("ABIDE1", check=True, root=root, out=io.StringIO()) == 1
    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    assert tpl.read_text() == before
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert lock["sidecars"]["S/T1w.json"][ms.LOCK_FILL_KEY] is True
    assert lock["sidecars"]["S/bold.json"] == {
        "manual": True, ms.LOCK_FILL_KEY: True}
    assert ms.run("ABIDE1", check=True, root=root, out=io.StringIO()) == 0

    # the lock follows the table, even for entries outside --only
    toml.write_text(plain)
    assert ms.run("ABIDE1", root=root, out=io.StringIO(),
                  only="S/bold*") == 0
    lock = json.loads((pdir / "sidecars.lock.json").read_text())
    assert ms.LOCK_FILL_KEY not in lock["sidecars"]["S/T1w.json"]

    toml.write_text(plain.replace('select = "1"\n',
                                  'select = "1"\nfill = "yes"\n'))
    with pytest.raises(ms.TableError, match=ms.FILL_KEY):
        ms.load_table("ABIDE1", root=root)


def test_diff():
    added, removed, changed = ms.diff({"a": 1, "b": 2}, {"b": 3, "c": 4})
    assert added == {"c": 4}
    assert removed == {"a": 1}
    assert changed == {"b": (2, 3)}


def test_fill_from(tmp_path, monkeypatch):
    """`fill_from` fills missing keys only, as `p2b --fill-from`."""
    root = _synthetic_tree(tmp_path)
    pdir = root / "printouts" / "ABIDE1"
    (pdir / "S" / "table.pdf").write_bytes(b"table")
    toml = pdir / "sidecars.toml"
    toml.write_text(toml.read_text().replace(
        'select = "1"\n',
        'select = "1"\nfill_from = "S/table.pdf"\nfill_select = 2\n'))
    monkeypatch.setattr(ms, "convert",
                        lambda p, s=None, n=None: {"EchoTime": 0.002})
    seen = []

    def fake_fill(printout, select=None):
        seen.append((Path(printout).name, select))
        return {"EchoTime": 0.009, "RepetitionTime": 2.0,
                "MagneticFieldStrength": 1.5, "Manufacturer": "Other"}

    monkeypatch.setattr(ms, "fill", fake_fill)
    monkeypatch.setattr(ms, "protocol2bids_version",
                        lambda: {"version": "9.9", "commit": "abc"})
    assert ms.run("ABIDE1", root=root, out=io.StringIO()) == 0
    assert seen == [("table.pdf", 2)]
    got = json.loads(
        (root / ms.TEMPLATES["ABIDE1"] / "S/T1w.json").read_text())
    # printout > defaults > fill; assigns override all
    assert got == {"EchoTime": 0.002, "RepetitionTime": 2.0,
                   "MagneticFieldStrength": 3, "Manufacturer": "Acme"}
    rec = json.loads((pdir / "sidecars.lock.json").read_text())
    rec = rec["sidecars"]["S/T1w.json"]
    assert rec["fill_from"] == "S/table.pdf"
    assert rec["fill_select"] == "2"
    assert rec["fill_sha256"] == ms.sha256(pdir / "S/table.pdf")
    # fill_select without fill_from, missing fill_from: table errors
    toml.write_text(toml.read_text().replace(
        'fill_from = "S/table.pdf"\n', ''))
    with pytest.raises(ms.TableError):
        ms.load_table("ABIDE1", root=root)
    toml.write_text(toml.read_text().replace(
        'fill_select = 2\n', 'fill_from = "S/missing.pdf"\n'))
    with pytest.raises(ms.TableError):
        ms.load_table("ABIDE1", root=root)


@pytest.mark.parametrize("dataset", DATASETS)
def test_check_real(dataset):
    """Committed sidecars are up to date (needs protocol2bids)."""
    pytest.importorskip("protocol2bids")
    lock = _lock(dataset)
    installed = ms.protocol2bids_version()
    if any(rec.get("protocol2bids", installed) != installed
           for rec in lock["sidecars"].values()):
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
    # protocol2bids (a git URL, which PyPI rejects in package metadata)
    # is only in requirements-maint.txt, pinned to the locked commit
    assert "maint" not in cfg["options.extras_require"]
    for value in [cfg["options"].get("install_requires", ""),
                  *cfg["options.extras_require"].values()]:
        assert "protocol2bids" not in value
        assert "git+" not in value and " @ " not in value
    reqs = [line for line in
            (ROOT / "requirements-maint.txt").read_text().splitlines()
            if line.strip() and not line.startswith("#")]
    assert 'tomli; python_version < "3.11"' in reqs[1:]
    assert reqs[0].startswith(
        "protocol2bids @ git+https://github.com/balbasty/protocol2bids@")
    for ds in DATASETS:
        lock = _lock(ds)
        assert lock["protocol2bids"]["commit"]
        for rec in lock["sidecars"].values():
            if not rec.get("manual"):
                assert reqs[0].endswith("@" + rec["protocol2bids"]["commit"])
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
    assert not [f for f in files if f.endswith((".pdf", ".nii"))]
