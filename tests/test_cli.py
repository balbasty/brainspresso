"""
The command line built from `Dataset` classes (step 4).

* Every flag of the step-3 command line still exists, with the same
  names, default and choices (or more choices), except the documented
  changes (`EXCEPTIONS`, `docs/design/architecture.md` §6).
* The root help lists the catalog without importing datasets; a dataset
  help is the full help; `python -m brainspresso[.cli]` works.
"""
import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Annotated

import pytest
from cli_utils import bx
from cyclopts import CycloptsError, Parameter

from brainspresso import cli
from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import (
    Layout,
    LayoutError,
    make_layout,
    parse_layout,
)
from brainspresso.cli import app, main
from brainspresso.datasets import catalog
from brainspresso.datasets.base import Args, Dataset, KeySpec
from brainspresso.datasets.bullitt.dataset import Bullitt
from brainspresso.datasets.catalog import CATALOG, load
from brainspresso.datasets.indi.abide.i.dataset import ABIDE1
from brainspresso.datasets.indi.abide.ii.dataset import ABIDE2
from brainspresso.datasets.indi.adhd200.dataset import ADHD200
from brainspresso.datasets.indi.cobre.dataset import COBRE
from brainspresso.datasets.ixi.dataset import IXI, KEYS
from brainspresso.datasets.oasis.iii.dataset import OASIS3
from brainspresso.datasets.openneuro.dataset import OpenNeuroDataset
from brainspresso.datasets.options import (
    HarvestOptions,
    OptionGroup,
    option_group,
)
from brainspresso.datasets.register import command_names, register
from brainspresso.datasets.unc_wisconsin.dataset import UNCWisconsin
from brainspresso.errors import UsageError
from brainspresso.sources.base import LocalSource, StaticURLs
from brainspresso.utils.deps import UNAVAILABLE

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
from cli_snapshot import flags, snapshot  # noqa: E402
from make_catalog import main as make_catalog  # noqa: E402

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
    # default () = all discs (was the list of discs), so that a roast
    # tells named discs (a missing one is an error) from the default
    ("oasis1", "*", "--discs"): "default",
    ("oasis2", "*", "--parts"): "default",
    # ABIDE-I download-nitrc/xnat: removed (`harvest --source`)
    ("abide1", "download-nitrc", "*"): "removed",
    ("abide1", "download-xnat", "*"): "removed",
    # deprecated alias, hidden from the help (`--layout sites:<kind>`)
    ("*", "harvest", "--site-layout"): "hidden",
    # renamed `--bids/--no-bids` (hidden deprecated alias)
    ("adhd200", "harvest", "--bidsify"): "hidden",
    # `--sidecars yes|no|only`; `--json VALUE` (always a value) and
    # `--no-json`: hidden deprecated aliases
    ("*", "roast", "--json"): "hidden",
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
    return snapshot(app)


def test_flags_superset(current):
    before = json.loads(SNAPSHOT.read_text())
    problems = []
    for ds, commands in before.items():
        for cmd, old_flags in commands.items():
            for flag, old in old_flags.items():
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
    try:
        main(["ixi", "--help"])
    except SystemExit:          # (cyclopts >= 4 exits after the help)
        pass
    out = capsys.readouterr().out
    assert "Information eXtraction from Images" in out
    assert "harvest" in out and "roast" in out


def test_catalog_up_to_date():
    assert make_catalog(check=True) == 0


def test_catalog_summaries():
    for name, (_, summary) in CATALOG.items():
        assert load(name).summary == summary


@pytest.mark.parametrize("name", sorted(CATALOG))
def test_catalog_name_resolves_and_has_help(name, capsys):
    """Every catalog import path resolves; `bx <name> --help` works."""
    module, _, attr = CATALOG[name][0].partition(":")
    assert module.startswith("brainspresso.datasets.")
    assert module == module.lower()             # PEP 8 package names
    assert hasattr(importlib.import_module(module), attr)
    dataset = load(name)
    assert dataset.name == name
    try:
        main([name, "--help"])
    except SystemExit as e:     # (cyclopts >= 4 exits after the help)
        assert not e.code, e.code
    out = capsys.readouterr()
    assert "unavailable" not in out.err, out.err
    for command in command_names(dataset):
        assert command in out.out


# INDI sharing category (`fcon_1000.projects.nitrc.org/indi/<category>/`)
INDI_CATEGORIES = {
    "abide1": "retro", "abide2": "retro", "adhd200": "retro",
    "cobre": "retro", "corr": "retro", "mpi-lemon": "retro",
    "natview": "retro", "sald": "retro", "simon": "retro", "slim": "retro",
    "hbn": "pro", "nki-rs": "enhanced", "fcon1000": "fcp",
}


def test_indi_category(capsys):
    for name in CATALOG:
        assert load(name).indi_category == INDI_CATEGORIES.get(name), name
    try:
        main(["abide1", "info"])
    except SystemExit as e:
        assert not e.code, e.code
    assert "  INDI category: retro\n" in capsys.readouterr().out


def test_broken_dataset(monkeypatch, capsys):
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
    return flags(app[command])


def test_toy_args_and_labels():
    toy = _toy()()
    args = Args.of(toy, "harvest")
    assert args.harvest.jobs == 3 and args.extras["toy"].colour == "red"
    assert toy.source_options(args)["colour"] == "red"
    assert toy.labels(["2", "sub-012", "0:3"]) == {"000", "001", "002",
                                                   "012"}
    # no width: verbatim (zero-padded labels kept)
    assert OpenNeuroDataset().labels(["0005", "sub-07", "1:3"]) == {
        "0005", "07", "1:3"}


def test_toy_dry_run_creates_nothing(tmp_path):
    toy = _toy()()
    toy.execute("harvest", path=tmp_path,
                harvest=HarvestOptions(dry_run=True))
    assert not any(tmp_path.iterdir())


def test_toy_stub(capsys):
    app = register(_toy())
    with pytest.raises(SystemExit) as e:
        app(["roast", "--keys", "raw"])
    assert e.value.code == 1
    assert "pip install 'brainspresso[toy]'" in capsys.readouterr().err


def test_user_errors(tmp_path, capsys):
    app = register(_toy())
    log = tmp_path / "log.txt"
    with pytest.raises(SystemExit):
        app(["compost", str(tmp_path), "--log", str(log), "--yes"])
    err = capsys.readouterr().err
    assert "Dataset folder not found" in err and "Traceback" not in err
    assert "Traceback" in log.read_text()


def test_layout_values():
    assert parse_layout(None) == (None, None)
    assert parse_layout("study") == ("study", None)
    assert parse_layout("sites") == ("sites", None)
    assert parse_layout("sites:flat") == ("sites", "flat")
    for bad in ("sites:sites", "flat:study", "tree"):
        with pytest.raises(LayoutError):
            parse_layout(bad)


def test_sites_layout_only_for_multisite(tmp_path, capsys):
    with pytest.raises(SystemExit) as e:
        main(["ixi", "compost", str(tmp_path), "--layout", "sites"])
    assert e.value.code


# ----------------------------------------------------------------------
#   Every dataset option reaches a source
# ----------------------------------------------------------------------

def test_source_options_are_accepted():
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
    try:
        assert main(["openneuro", "compost", "ds1", "--out", str(root),
                     "--exclude-subs", "0005", "--dry-run"]) is None
    except SystemExit as e:     # (cyclopts >= 4 exits after a command)
        assert not e.code
    assert "CompostPlan(" not in capsys.readouterr().out


def test_ignore_runs_is_not_force(tmp_path, capsys):
    root = _bids(tmp_path / "ds")
    runs = root / ".brainspresso" / "runs"
    runs.mkdir(parents=True)
    (runs / "elsewhere-12.json").write_text(json.dumps(
        {"command": "harvest", "host": "elsewhere", "pid": 12}))
    args = ["openneuro", "compost", "ds1", "--out", str(root),
            "--exclude-subs", "0005", "--yes"]
    with pytest.raises(SystemExit):
        main([*args, "--force"])
    assert "--ignore-runs" in capsys.readouterr().err
    assert (root / "sub-0005").exists()
    bx(*args, "--ignore-runs")
    assert not (root / "sub-0005").exists()


def test_roast_bad_options_create_nothing(tmp_path, capsys):
    # an invalid subject range (no dataset takes subject numbers any
    # more: `legacy_subjects` went with the ABIDE-II port)
    (tmp_path / "ABIDE-2").mkdir()
    with pytest.raises(SystemExit):
        main(["abide2", "roast", str(tmp_path), "--subs", "a:b"])
    assert "not numbers" in capsys.readouterr().err
    assert not any((tmp_path / "ABIDE-2").iterdir())
    # a missing dataset
    with pytest.raises(SystemExit):
        main(["oasis2", "roast", str(tmp_path)])
    assert "harvest it first" in capsys.readouterr().err
    assert not (tmp_path / "OASIS-2").exists()


def test_key_trees(capsys):
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


@pytest.mark.parametrize("flags,value", [
    ([], True), (["--no-bids"], False), (["--no-bidsify"], False),
    (["--bidsify"], True),
])
def test_adhd200_bids_flag(tmp_path, monkeypatch, caplog, flags, value):
    seen = {}

    def harvest(self, layout, args):
        seen.update(self.source_options(args))

    monkeypatch.setattr(ADHD200, "harvest", harvest)
    app = register(ADHD200())
    try:
        app(["harvest", str(tmp_path), "--dry-run", *flags])
    except SystemExit as e:
        assert not e.code
    assert seen["bidsify"] is value
    if "bidsify" in "".join(flags):
        assert "--bidsify/--no-bidsify is deprecated" in caplog.text


# ----------------------------------------------------------------------
#   Review of the fix commit (round 3)
# ----------------------------------------------------------------------

def test_group_defaults_cover_all():
    # ("all",) and groups that cover every key count as "no --keys"
    abide = ABIDE1().keys_for("harvest")
    # (excluding `fs` excludes its add-on `fs-all`)
    assert abide.effective(("all",), ["fs"]) == ((), ("fs", "fs-all"))
    adhd = ADHD200().keys_for("harvest")
    assert adhd.effective(("all",), ["mriqc"]) == ((), ("mriqc",))
    # Bullitt's default leaves `phantoms` out: it stays a selection
    bullitt = Bullitt().keys_for("harvest")
    assert bullitt.effective(bullitt.default) == (bullitt.default, ())
    assert bullitt.leaves(["raw"]) == {"flash", "mprage", "T2w", "angio",
                                       "dwi"}


def test_abide1_fs_keys_are_siblings():
    # `fs` converts the files that BIDS describes, `fs-all` every file:
    # siblings in the roast keys (`KeyAliases` expands a group to its
    # leaves, so `fs` could not be told from `fs-all` as their group)
    keys = ABIDE1().keys_for("roast").resolve()
    fs = keys.selection(["fs"])
    assert fs.wants("fs") and not fs.wants("fs-all")
    assert keys.selection(["fs-all"]).wants("fs-all")
    meta = keys.selection(["meta", "anat"])
    assert {meta.wants(k) for k in ("meta", "pheno", "T1w")} == {True}
    assert not meta.wants("rest") and not meta.wants("fs")
    assert not keys.selection(["pheno"]).wants("meta")
    derivatives = keys.selection(exclude_keys=["fs-all"])
    assert derivatives.wants("fs") and not derivatives.wants("fs-all")


def test_unnamed_key_group():
    spec = OASIS3().keys_for("harvest")
    assert "" not in spec.all_choices()
    assert "fmap" in spec.all_choices() and "dwi" in spec.all_choices()
    assert "* \n" not in OASIS3().key_help("harvest") + "\n"
    assert "  * fmap" in OASIS3().key_help("harvest")


def test_bad_subject_range(tmp_path, capsys):
    with pytest.raises(UsageError, match="not numbers"):
        IXI().labels(["a:b"])
    with pytest.raises(SystemExit) as e:
        main(["ixi", "roast", str(tmp_path), "--subs", "a:b"])
    assert e.value.code == 1
    assert "not numbers" in capsys.readouterr().err


def test_abide_two_exclude_keys(tmp_path):
    # (engine) two excluded keys select the rest
    for dataset, excluded, kept in ((ABIDE1, ["T1w", "rest"], "meta"),
                                    (ABIDE2, ["dti", "rest"], "T1w")):
        selection = dataset().keys_for("roast").resolve().selection(
            exclude_keys=excluded)
        assert not any(selection.wants(k) for k in excluded)
        assert selection.wants(kept) and selection.wants("pheno")


def test_abide_roast_help_notes(capsys):
    for ds, note in (("abide1", "--source xnat"),
                     ("abide2", "is not roasted")):
        try:
            main([ds, "roast", "--help"])
        except SystemExit:
            pass
        assert note in capsys.readouterr().out


def test_lock_states_parent_first(tmp_path):
    # the parent (one dataset per site), shared, before the site (listed
    # or not); a whole parent: itself, then its sites
    root = tmp_path / "DS"
    sites = make_layout("sites", root, child_kind="study")
    sites.child("A")
    record_run(sites.child("A"), "harvest")
    record_run(sites, "harvest")
    for name in ("A", "NEW"):
        site = make_layout("study", root / name)
        assert Dataset.lock_states(site) == (
            [root / ".brainspresso"], [root / name / ".brainspresso"])
    found = Layout.detect(root)
    assert Dataset.lock_states(found) == (
        [], [root / ".brainspresso", root / "A" / ".brainspresso"])
    # not a site
    assert Dataset.lock_states(make_layout("flat", tmp_path / "X")) == (
        [], [tmp_path / "X" / ".brainspresso"])


@pytest.mark.skipif(sys.platform == "win32", reason=(
    "Windows file names cannot contain ':'"))
def test_subject_file_with_colon(tmp_path):
    subs = tmp_path / "a:b.txt"
    subs.write_text("2\n12\n")
    assert IXI().labels([str(subs)]) == {"002", "012"}


# ----------------------------------------------------------------------
#   Roast --sidecars (`--json VALUE`, `--no-json`: deprecated aliases)
# ----------------------------------------------------------------------

def _roast(tmp_path, *tokens, dataset=IXI):
    """`Args` of `bx <dataset> roast <tokens>` (parsed, nothing runs)."""
    app = register(dataset())
    _, bound, _ = app.parse_args(["roast", str(tmp_path), *tokens],
                                 exit_on_error=False, print_error=False)
    groups = {k: v for k, v in bound.arguments.items()
              if k not in ("path", "out") and v is not None}
    return Args.of(dataset(), "roast", **groups)


#: tokens -> policy of the roasters, and the old `--json` that meant it
SIDECARS = [
    ([], "yes", []),
    (["--sidecars", "yes"], "yes", ["--json", "yes"]),
    (["--sidecars", "no"], "no", ["--json", "no"]),
    (["--sidecars", "only"], "only", ["--json", "only"]),
    (["--json", "yes"], "yes", ["--json", "yes"]),
    (["--json", "no"], "no", ["--json", "no"]),
    (["--json", "only"], "only", ["--json", "only"]),
    (["--no-json"], "no", ["--no-json"]),
    (["--json", "no", "--sidecars", "no"], "no", ["--json", "no"]),
    (["--json", "only", "--sidecars", "only"], "only", ["--json", "only"]),
    (["--no-json", "--sidecars", "no"], "no", ["--no-json"]),
    (["--json", "no", "--no-json"], "no", ["--no-json"]),
]

#: the old `--json` forms -> their policy (step 3: `Literal | bool`)
OLD_JSON = {("--json", "yes"): "yes", ("--json", "no"): "no",
            ("--json", "only"): "only", ("--no-json",): "no", (): "yes"}


@pytest.mark.parametrize("tokens,policy,old", SIDECARS)
def test_sidecars(tmp_path, tokens, policy, old):
    args = _roast(tmp_path, *tokens)
    assert args.roast.sidecar_policy == policy
    # the policy of the old `--json` that meant the same
    assert OLD_JSON[tuple(old)] == policy


@pytest.mark.parametrize("tokens", [
    ["--json", "yes", "--sidecars", "no"],
    ["--json", "only", "--sidecars", "no"],
    ["--json", "no", "--sidecars", "only"],
    ["--no-json", "--sidecars", "only"],
    ["--no-json", "--sidecars", "yes"],
    ["--json", "only", "--no-json"],
    ["--json", "yes", "--no-json"],
])
def test_sidecars_conflict(tmp_path, tokens):
    args = _roast(tmp_path, *tokens)
    with pytest.raises(UsageError, match="contradict"):
        args.roast.sidecar_policy
    # checked before anything is written
    with pytest.raises(UsageError):
        IXI().check("roast", args)


@pytest.mark.parametrize("tokens", [
    ["--sidecars"], ["--sidecars", "maybe"], ["--json"],
    ["--json", "maybe"], ["--no-sidecars"], ["--sidecars-only"],
    ["--no-no-json"],
])
def test_sidecars_bad(tmp_path, tokens):
    with pytest.raises(CycloptsError):
        _roast(tmp_path, *tokens)


def test_json_aliases_are_deprecated(tmp_path, caplog):
    args = _roast(tmp_path, "--json", "only")
    assert args.roast.sidecar_policy == "only"
    assert args.roast.sidecar_policy == "only"
    assert caplog.text.count("--json is deprecated: use --sidecars") == 1
    args = _roast(tmp_path, "--no-json")
    assert args.roast.sidecar_policy == "no"
    assert caplog.text.count(
        "--no-json is deprecated: use --sidecars no") == 1
    snapshot = snapshot_of(register(IXI()), "roast")
    assert "--json" not in snapshot and "--no-json" not in snapshot
    assert snapshot["--sidecars"]["names"] == ["--sidecars"]
    assert sorted(snapshot["--sidecars"]["choices"]) == ["no", "only",
                                                         "yes"]


@pytest.mark.parametrize("name,dataset", [
    ("cobre", COBRE), ("abide1", ABIDE1), ("abide2", ABIDE2),
    ("bullitt", Bullitt), ("oasis3", OASIS3),
])
def test_sidecars_of_every_roast(tmp_path, name, dataset):
    # legacy roasters (`json=args.roast.sidecar_policy`) and the engine
    for tokens, policy in ((["--sidecars", "only"], "only"),
                           (["--no-json"], "no")):
        assert _roast(tmp_path, *tokens, dataset=dataset
                      ).roast.sidecar_policy == policy


# ----------------------------------------------------------------------
#   --lock-wait (`--wait SECONDS`: deprecated on harvest and compost)
# ----------------------------------------------------------------------

@pytest.mark.parametrize("command,group", [
    ("harvest", "harvest"), ("roast", "roast"), ("compost", "compost"),
])
def test_lock_wait(tmp_path, command, group):
    app = register(IXI())
    _, bound, _ = app.parse_args([command, str(tmp_path), "--lock-wait",
                                  "2.5"], exit_on_error=False,
                                 print_error=False)
    args = Args.of(IXI(), command, **{group: bound.arguments[group]})
    assert args.lock_wait == 2.5


@pytest.mark.parametrize("command", ["harvest", "compost"])
def test_wait_is_a_deprecated_alias(tmp_path, caplog, command):
    app = register(IXI())
    _, bound, _ = app.parse_args([command, str(tmp_path), "--wait", "3"],
                                 exit_on_error=False, print_error=False)
    args = Args.of(IXI(), command, **{command: bound.arguments[command]})
    assert args.lock_wait == 3
    assert args.lock_wait == 3
    assert caplog.text.count("--wait is deprecated: use --lock-wait") == 1
    assert "--wait" not in snapshot_of(app, command)    # hidden


def test_roast_has_no_wait(tmp_path):
    # reserved for the execution flag `--wait/--detach` (grind, §7)
    app = register(IXI())
    with pytest.raises(CycloptsError):
        app.parse_args(["roast", str(tmp_path), "--wait", "3"],
                       exit_on_error=False, print_error=False)


@pytest.mark.parametrize("command", ["harvest", "compost"])
@pytest.mark.parametrize("tokens,expected", [
    (["--wait", "3", "--lock-wait", "3"], 3),
    (["--wait", "3", "--lock-wait", "4"], None),
    (["--wait", "0", "--lock-wait", "4"], None),
])
def test_wait_and_lock_wait(tmp_path, command, tokens, expected):
    app = register(IXI())
    _, bound, _ = app.parse_args([command, str(tmp_path), *tokens],
                                 exit_on_error=False, print_error=False)
    args = Args.of(IXI(), command, **{command: bound.arguments[command]})
    if expected is None:
        with pytest.raises(UsageError, match="contradict"):
            IXI().check(command, args)
    else:
        assert args.lock_wait == expected


def test_roast_wait_suggests_lock_wait(tmp_path):
    with pytest.raises(CycloptsError, match="--lock-wait"):
        register(IXI()).parse_args(["roast", str(tmp_path), "--wait", "3"],
                                   exit_on_error=False, print_error=False)
