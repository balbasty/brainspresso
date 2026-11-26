"""
The command line built from `Dataset` classes (step 4).

* Every flag of the step-3 command line still exists, with the same
  names, default and choices (or more choices), except the documented
  changes (`EXCEPTIONS`, `docs/design/architecture.md` §6).
* The root help lists the catalog without importing datasets; a dataset
  help is the full help; `python -m brainspresso[.cli]` works.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

from brainspresso.errors import UsageError

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
from cli_snapshot import snapshot  # noqa: E402

SNAPSHOT = Path(__file__).parent / "data" / "cli_flags_step3.json"
INDI = ("acpi", "fcon1000", "hbn", "mpi-lemon", "natview", "nki-rs",
        "sald", "simon", "slim")

# (dataset or "*", command or "*", flag) -> why it changes
EXCEPTIONS = {
    # free text checked by `Dataset.select_sites` (was a choice list);
    # default () = all sites (was the list of sites)
    ("abide1", "harvest", "--sites"): "choices, default",
    ("abide2", "harvest", "--sites"): "choices, default",
    ("corr", "harvest", "--sites"): "choices, default",
    # ABIDE-I download-nitrc/xnat: removed (`harvest --source`)
    ("abide1", "download-nitrc", "*"): "removed",
    ("abide1", "download-xnat", "*"): "removed",
    # deprecated alias, hidden from the help (`--layout sites:<kind>`)
    ("*", "harvest", "--site-layout"): "hidden",
}


def _exception(ds, cmd, flag):
    for key in ((ds, cmd, flag), (ds, cmd, "*"), ("*", cmd, flag),
                (ds, "*", flag)):
        if key in EXCEPTIONS:
            return EXCEPTIONS[key]
    return None


def _norm(value):
    """Defaults: None and [] are unset; a scalar is a list of one."""
    if value is None:
        return []
    if not isinstance(value, list):
        value = [value]
    return sorted(str(v) for v in value)


@pytest.fixture(scope="module")
def current():
    from brainspresso.cli import app
    return snapshot(app)


def test_flags_superset(current):
    before = json.loads(SNAPSHOT.read_text())
    problems = []
    for ds, commands in before.items():
        for cmd, flags in commands.items():
            for flag, old in flags.items():
                if _exception(ds, cmd, flag):
                    continue
                new = current.get(ds, {}).get(cmd, {}).get(flag)
                if new is None:
                    problems.append(f"{ds} {cmd} {flag}: missing")
                    continue
                if not set(old["names"]) <= set(new["names"]):
                    problems.append(f"{ds} {cmd} {flag}: names "
                                    f"{old['names']} -> {new['names']}")
                if old["choices"] is not None and (
                    new["choices"] is None
                    or not set(old["choices"]) <= set(new["choices"])
                ):
                    problems.append(f"{ds} {cmd} {flag}: choices "
                                    f"{old['choices']} -> {new['choices']}")
                if _norm(old["default"]) != _norm(new["default"]):
                    problems.append(f"{ds} {cmd} {flag}: default "
                                    f"{old['default']} -> {new['default']}")
    assert not problems, "\n".join(problems)


# ----------------------------------------------------------------------
#   Root help, catalog, `python -m`
# ----------------------------------------------------------------------

def run_python(*args, **kwargs):
    return subprocess.run([sys.executable, *args], capture_output=True,
                          text=True, cwd=Path(__file__).parents[1],
                          **kwargs)


@pytest.mark.parametrize("module", ["brainspresso.cli", "brainspresso"])
def test_python_m_lists_datasets(module):
    from brainspresso.datasets.catalog import CATALOG
    out = run_python("-m", module, "--help")
    assert out.returncode == 0, out.stderr
    for name in CATALOG:
        assert name in out.stdout


def test_root_help_imports_no_dataset():
    code = (
        "import sys\n"
        "from brainspresso.cli import main\n"
        "try:\n    main(['--help'])\n"
        "except SystemExit:\n    pass\n"
        "mods = [m for m in sys.modules if m.startswith("
        "'brainspresso.datasets.') and m != 'brainspresso.datasets.catalog']\n"
        "print('MODULES', mods)\n"
    )
    out = run_python("-c", code)
    assert out.returncode == 0, out.stderr
    assert "MODULES []" in out.stdout


def test_dataset_help_is_the_long_help(capsys):
    from brainspresso.cli import main
    try:
        main(["ixi", "--help"])
    except SystemExit:          # (cyclopts >= 4 exits after the help)
        pass
    out = capsys.readouterr().out
    assert "Information eXtraction from Images" in out
    assert "harvest" in out and "roast" in out


def test_catalog_up_to_date():
    from make_catalog import main as make_catalog
    assert make_catalog(check=True) == 0


def test_catalog_summaries():
    from brainspresso.datasets.catalog import CATALOG, load
    for name, (_, summary) in CATALOG.items():
        assert load(name).summary == summary


def test_broken_dataset(monkeypatch, capsys):
    from brainspresso import cli
    from brainspresso.datasets import catalog
    monkeypatch.setitem(catalog.CATALOG, "broken",
                        ("brainspresso.datasets.nowhere:X", "Broken"))
    with pytest.raises(SystemExit) as e:
        cli.main(["broken", "harvest", "--subs", "1"])
    assert e.value.code == 1
    assert "dataset broken is unavailable" in capsys.readouterr().err


# ----------------------------------------------------------------------
#   A toy dataset
# ----------------------------------------------------------------------

def _toy():
    from typing import Annotated
    from cyclopts import Parameter
    from brainspresso.datasets.base import Dataset, KeySpec
    from brainspresso.datasets.options import OptionGroup, option_group
    from brainspresso.sources.base import LocalSource, StaticURLs

    @option_group("Toy", 50)
    class ToyOptions(OptionGroup):
        """`--colour`."""

        colour: Annotated[str, Parameter(help="A colour")] = "red"

        def source_options(self, dataset):
            return {"colour": self.colour}

    class Toy(Dataset):
        name = "toy"
        folder = "Toy"
        help = "Toy dataset\n\nFor tests."
        keys = KeySpec(None, ("raw", "meta"), ("raw", "meta"))
        subject_width = 3
        credentials = "token"
        requires = {"roast": ("no_such_module_xyz",)}
        extra = "toy"
        option_defaults = {"harvest": {"jobs": 3}}
        roaster = staticmethod(lambda ctx: iter(()))

        def sources(self, args=None):
            return {"urls": StaticURLs({"raw": []}, name="urls"),
                    "local": LocalSource(provides={"meta"}, name="local")}

        def options(self, command):
            return {"toy": ToyOptions} if command == "harvest" else {}

        def cli(self, app):
            @app.command(name="hello")
            def hello():
                """Say hello."""
                print("hello")

    return Toy


def test_toy_commands():
    from brainspresso.datasets.register import register
    from brainspresso.utils.deps import UNAVAILABLE
    app = register(_toy())
    assert {"harvest", "roast", "compost", "info", "hello"} <= set(
        app._commands)
    assert any(u.command == "toy roast" for u in UNAVAILABLE)
    harvest = snapshot_of(app, "harvest")
    assert harvest["--keys"]["choices"] == ["meta", "raw"]
    assert harvest["--source"]["choices"] == ["local", "urls"]
    assert harvest["--jobs"]["default"] == 3
    assert harvest["--colour"]["default"] == "red"
    assert "--token" in harvest and "--user" not in harvest
    assert "--sites" not in harvest


def snapshot_of(app, command):
    from cli_snapshot import flags
    return flags(app[command])


def test_toy_args_and_labels():
    from brainspresso.datasets.base import Args
    toy = _toy()()
    args = Args.of(toy, "harvest")
    assert args.harvest.jobs == 3 and args.extras["toy"].colour == "red"
    assert toy.source_options(args)["colour"] == "red"
    assert toy.labels(["2", "sub-012", "0:3"]) == {"000", "001", "002",
                                                   "012"}
    # no width: verbatim (zero-padded labels kept)
    from brainspresso.datasets.OpenNeuro.dataset import OpenNeuroDataset
    assert OpenNeuroDataset().labels(["0005", "sub-07", "1:3"]) == {
        "0005", "07", "1:3"}


def test_toy_dry_run_creates_nothing(tmp_path):
    toy = _toy()()
    from brainspresso.datasets.options import HarvestOptions
    toy.execute("harvest", path=tmp_path,
                harvest=HarvestOptions(dry_run=True))
    assert not any(tmp_path.iterdir())


def test_toy_stub(capsys):
    from brainspresso.datasets.register import register
    app = register(_toy())
    with pytest.raises(SystemExit) as e:
        app(["roast", "--keys", "raw"])
    assert e.value.code == 1
    assert "pip install 'brainspresso[toy]'" in capsys.readouterr().err


def test_user_errors(tmp_path, capsys):
    from brainspresso.datasets.register import register
    app = register(_toy())
    log = tmp_path / "log.txt"
    with pytest.raises(SystemExit):
        app(["compost", str(tmp_path), "--log", str(log), "--yes"])
    err = capsys.readouterr().err
    assert "Dataset folder not found" in err and "Traceback" not in err
    assert "Traceback" in log.read_text()


def test_layout_values():
    from brainspresso.bids.layout import LayoutError, parse_layout
    assert parse_layout(None) == (None, None)
    assert parse_layout("study") == ("study", None)
    assert parse_layout("sites") == ("sites", None)
    assert parse_layout("sites:flat") == ("sites", "flat")
    for bad in ("sites:sites", "flat:study", "tree"):
        with pytest.raises(LayoutError):
            parse_layout(bad)


def test_sites_layout_only_for_multisite(tmp_path, capsys):
    from brainspresso.cli import main
    with pytest.raises(SystemExit) as e:
        main(["ixi", "compost", str(tmp_path), "--layout", "sites"])
    assert e.value.code


# ----------------------------------------------------------------------
#   Every dataset option reaches a source
# ----------------------------------------------------------------------

def test_source_options_are_accepted():
    from brainspresso.datasets.base import Args
    from brainspresso.datasets.catalog import CATALOG, load
    problems = []
    for name in CATALOG:
        ds = load(name)
        if type(ds).harvest is not type(ds).__mro__[-2].harvest:
            continue                # own harvest (INDI, OpenNeuro)
        args = Args.of(ds, "harvest")
        groups = ds.groups("harvest")
        options = {}
        for group in ("sites", "login", "token"):
            if group in groups:
                options.update(getattr(args, group).source_options(ds))
        for group in args.extras.values():
            options.update(group.source_options(ds))
        sources = ds.sources(args).values()
        for key in options:
            if not any(s._is_option(key) for s in sources):
                problems.append(f"{name}: {key}")
    assert not problems, problems


def test_compost_zero_padded_labels(tmp_path):
    from cli_utils import bx
    root = tmp_path / "ds"
    for sub in ("sub-0005", "sub-5"):
        (root / sub / "anat").mkdir(parents=True)
        (root / sub / "anat" / f"{sub}_T1w.nii.gz").write_bytes(b"x")
    (root / "dataset_description.json").write_text(
        '{"Name": "x", "BIDSVersion": "1.10.0"}')
    bx("openneuro", "compost", "ds000001", "--out", root,
       "--exclude-subs", "0005", "--yes")
    assert not (root / "sub-0005").exists()
    assert (root / "sub-5").exists()


def test_bad_option_creates_nothing(tmp_path, capsys):
    from brainspresso.cli import main
    with pytest.raises(SystemExit) as e:
        main(["oasis3", "roast", str(tmp_path), "--keys", "nosuchkey"])
    assert e.value.code == 1
    assert "Unknown key 'nosuchkey'" in capsys.readouterr().err
    assert not any(tmp_path.iterdir())


# ----------------------------------------------------------------------
#   Review of PR #19
# ----------------------------------------------------------------------

def _bids(root, subs=("sub-0005", "sub-5")):
    for sub in subs:
        (root / sub / "anat").mkdir(parents=True)
        (root / sub / "anat" / f"{sub}_T1w.nii.gz").write_bytes(b"x")
    (root / "dataset_description.json").write_text(
        '{"Name": "x", "BIDSVersion": "1.10.0"}')
    return root


def test_commands_return_nothing(tmp_path, capsys):
    # cyclopts >= 4 prints what a command returns; the console script of
    # cyclopts 3 (`sys.exit(main())`) would print it and exit 1
    root = _bids(tmp_path / "ds")
    out = run_python("-c", (
        "import sys\nfrom brainspresso.cli import main\n"
        f"sys.exit(main(['openneuro', 'compost', 'ds1', '--out', "
        f"{str(root)!r}, '--exclude-subs', '0005', '--dry-run']))\n"))
    assert out.returncode == 0, out.stderr
    assert "CompostPlan(" not in out.stdout + out.stderr
    from brainspresso.cli import main
    try:
        assert main(["openneuro", "compost", "ds1", "--out", str(root),
                     "--exclude-subs", "0005", "--dry-run"]) is None
    except SystemExit as e:     # (cyclopts >= 4 exits after a command)
        assert not e.code
    assert "CompostPlan(" not in capsys.readouterr().out


def test_ignore_runs_is_not_force(tmp_path, capsys):
    import json as _json
    from brainspresso.cli import main
    root = _bids(tmp_path / "ds")
    runs = root / ".brainspresso" / "runs"
    runs.mkdir(parents=True)
    (runs / "elsewhere-12.json").write_text(_json.dumps(
        {"command": "harvest", "host": "elsewhere", "pid": 12}))
    args = ["openneuro", "compost", "ds1", "--out", str(root),
            "--exclude-subs", "0005", "--yes"]
    with pytest.raises(SystemExit):
        main([*args, "--force"])
    assert "--ignore-runs" in capsys.readouterr().err
    assert (root / "sub-0005").exists()
    from cli_utils import bx
    bx(*args, "--ignore-runs")
    assert not (root / "sub-0005").exists()


def test_legacy_subjects_files_and_ranges(tmp_path):
    from brainspresso.datasets.COBRE.dataset import COBRE
    from brainspresso.datasets.base import Args
    from brainspresso.datasets.options import SelectionOptions
    subs = tmp_path / "subs.txt"
    subs.write_text("40000\nsub-40001\n")
    cobre = COBRE()
    assert cobre.numbers([str(subs), "3:5", "sub-7"]) == [3, 4, 7, 40000,
                                                          40001]
    cobre.check("roast", Args.of(cobre, "roast", select=SelectionOptions(
        subs=(str(subs), "1:3"))))
    with pytest.raises(UsageError):
        cobre.numbers(["abc"])


def test_legacy_roast_bad_options_create_nothing(tmp_path, capsys):
    from brainspresso.cli import main
    # an invalid subject
    (tmp_path / "COBRE").mkdir()
    with pytest.raises(SystemExit):
        main(["cobre", "roast", str(tmp_path), "--subs", "abc"])
    assert "Subjects are numbers" in capsys.readouterr().err
    assert not any((tmp_path / "COBRE").iterdir())
    # a missing dataset
    with pytest.raises(SystemExit):
        main(["oasis2", "roast", str(tmp_path)])
    assert "harvest it first" in capsys.readouterr().err
    assert not (tmp_path / "OASIS-2").exists()


def test_key_trees(capsys):
    from brainspresso.cli import main
    from brainspresso.datasets.Bullitt.dataset import Bullitt
    spec = Bullitt().keys_for("harvest")
    assert "phantoms" in spec.all_choices() and "all" in spec.all_choices()
    with pytest.raises(UsageError, match="Unknown keys"):
        spec.check(["nosuchkey"])
    with pytest.raises(SystemExit) as e:
        main(["bullitt", "harvest", "--keys", "nosuchkey"])
    assert e.value.code
    try:
        main(["bullitt", "harvest", "--help"])
    except SystemExit:
        pass
    out = capsys.readouterr().out
    assert "Keys (a group selects" in out and "phantoms" in out


def test_key_spec_effective():
    from brainspresso.datasets.IXI.dataset import KEYS, IXI
    from brainspresso.datasets.UNCWisconsin.dataset import UNCWisconsin
    roast = IXI().keys_for("roast")
    assert roast.effective(KEYS) == ((), ("trace",))
    assert roast.effective((), ["T2w"]) == ((), ("T2w", "trace"))
    assert roast.effective(KEYS, ["T2w"]) == ((), ("T2w", "trace"))
    assert roast.effective(["T1w", "trace"]) == (("T1w", "trace"), ())
    assert roast.effective([*KEYS, "trace"]) == ((), ())
    assert IXI().keys_for("harvest").effective(KEYS, ["dwi"]) == (
        (), ("dwi",))
    unc = UNCWisconsin().keys_for("harvest")
    assert unc.effective(("raw", "meta")) == (("raw", "meta"), ())
    assert unc.effective(("raw", "meta"), ["dwi"]) == (("raw", "meta"),
                                                       ("dwi",))
