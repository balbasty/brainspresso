"""
Roaster outputs against the golden baseline (`scripts/golden.py`).

A reduced matrix (`golden.CI_MATRIX`) of the golden harness: each
roasted fixture must match `tests/golden/<dataset>.json`. When a change
of output is intended, regenerate the baseline with
`python scripts/golden.py record` and paste `python scripts/golden.py
diff` in the pull request.
"""
import contextlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import golden  # noqa: E402


@pytest.mark.parametrize("name", list(golden.CI_MATRIX))
def test_golden(name, tmp_path):
    lines = golden.check({name: golden.CI_MATRIX[name]}, tmp_path)
    assert not lines, (
        "roasted outputs differ from the golden baseline "
        f"(regenerate with `{golden.RECORD}` if intended):\n"
        + "\n".join(lines))


@pytest.mark.parametrize("source", ["root", "cwd", "user", "env"])
def test_golden_ignores_user_config(source, tmp_path, monkeypatch):
    """No config of the user running the harness is read: a root file
    (`$BDP_PATH`, else the current folder, read even with `--out`), a
    user file or a `BRAINSPRESSO_OPT_*` variable"""
    hostile = tmp_path / "hostile"
    (hostile / ".brainspresso").mkdir(parents=True)
    (hostile / ".brainspresso" / "config.toml").write_text(
        "[roast]\nsidecars = 'no'\n")
    if source == "root":
        monkeypatch.setenv("BDP_PATH", str(hostile))
    elif source == "cwd":
        monkeypatch.delenv("BDP_PATH")
        monkeypatch.chdir(hostile)
    elif source == "user":
        monkeypatch.setenv("BRAINSPRESSO_CONFIG_HOME",
                           str(hostile / ".brainspresso"))
    else:
        monkeypatch.setenv("BRAINSPRESSO_OPT_ROAST__SIDECARS", "no")
    selection = {"cobre": [("study", "default")]}
    assert golden.check(selection, tmp_path / "work") == []
    # (the hostile config is read when the harness does not hide it)
    with golden.patched(golden, quiet_config=contextlib.nullcontext):
        lines = golden.check(selection, tmp_path / "work2")
    assert any("_T1w.json" in line for line in lines), lines


def test_work_folder_reused(tmp_path):
    """`--work DIR` twice: the folders of the first run are replaced"""
    selection = {"cobre": [("study", "keys-meta")]}
    first = golden.run(selection, tmp_path)
    assert golden.run(selection, tmp_path) == first


def test_diff_base_errors(capsys):
    """A bad revision is an error, not an empty baseline"""
    assert golden.main(["diff", "--base", "no-such-revision^^^",
                        "--datasets", "cobre"]) == 2
    assert "not a git revision" in capsys.readouterr().err
    with pytest.raises(golden.GitError):
        golden.load("cobre", "no-such-revision^^^")
    # a valid revision without the file: no baseline
    assert golden.load("no-such-dataset", "HEAD") == {}


def test_record_prune(tmp_path, monkeypatch):
    """`save` replaces the cases it is given, and drops the others with
    `prune`; `record --prune` keeps only the selection"""
    monkeypatch.setattr(golden, "GOLDEN", tmp_path)
    golden.save("x", {"study/a": 1, "flat/a": 2})
    golden.save("x", {"study/a": 3})
    assert golden.load("x") == {"flat/a": 2, "study/a": 3}
    golden.save("x", {"study/b": 4}, prune=True)
    assert golden.load("x") == {"study/b": 4}
    golden.save("cobre", {"study/stale": {}})
    assert golden.main(["record", "--datasets", "cobre", "--layouts",
                        "study", "--cases", "keys-meta", "--prune",
                        "--verbose"]) == 0
    assert list(golden.load("cobre")) == ["study/keys-meta"]
