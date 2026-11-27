"""
Config files (`brainspresso.config`): layers, sections, keys, `bx config`.

Options are checked on the parsed command (`App.parse_args`, through the
config callable of the root app), so nothing runs.
"""
import json
import logging
import subprocess
import sys
from pathlib import Path

import pytest
from cyclopts import CycloptsError

from brainspresso import cli, config
from brainspresso.actions import locks
from brainspresso.actions.locks import (
    LockError,
    find_dataset,
    is_state_folder,
    lock_file,
)
from brainspresso.actions.runlock import run_lock
from brainspresso.bids.layout import Layout, is_sites_parent, make_layout
from brainspresso.cli import build_app, global_options, main
from brainspresso.config import ConfigError, Loader, dataset_names, tree
from brainspresso.datasets import base
from brainspresso.datasets.ADHD200.dataset import ADHD200
from brainspresso.datasets.base import Dataset
from brainspresso.datasets.catalog import ARGUMENTS, CATALOG, KEYS, load
from brainspresso.datasets.IXI.dataset import IXI
from brainspresso.errors import UsageError

# ----------------------------------------------------------------------
#   Helpers
# ----------------------------------------------------------------------


@pytest.fixture
def home(tmp_path, monkeypatch):
    """The folder of the user file."""
    folder = tmp_path / "home"
    folder.mkdir()
    monkeypatch.setenv(config.HOME_ENV, str(folder))
    return folder


def write(path: Path, text: str) -> Path:
    """Write a file (and its folders)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def user(home, text):
    return write(home / "config.toml", text)


def located(folder, text):
    return write(config.located_file(folder), text)


def parse(*tokens, files=(), environ=None):
    """Parse a command: (bound arguments, report of the loader)."""
    loader = Loader(files, environ or {})
    app = build_app(tokens[0], loader=loader)
    _, bound, _ = app.parse_args([str(t) for t in tokens],
                                 exit_on_error=False, print_error=False)
    return bound.arguments, loader.report


def value(arguments, group, field):
    """A field of a parsed group (None: the group's default)."""
    group = arguments.get(group)
    return None if group is None else getattr(group, field)


def origin(report, key):
    return report["origins"][key]


# ----------------------------------------------------------------------
#   Names of the dataset sections
# ----------------------------------------------------------------------

def test_section_names():
    # sections are named after the command names (one more part per
    # argument): the one rule, `config.dataset_names`
    for name in CATALOG:
        expected = "openneuro.{dataset}" if name == "openneuro" else name
        assert dataset_names(name) == (expected, ())
    assert ARGUMENTS == {"openneuro": ("dataset",)}


@pytest.mark.parametrize("written,name", [
    ("abide1", "abide1"), ("ABIDE1", "abide1"), ("Nki-Rs", "nki-rs"),
    ("unc-wisconsin", "unc-wisconsin"), ("IXI", "ixi"),
    ("OpenNeuro", "openneuro"), ("fcon1000", "fcon1000"),
])
def test_section_names_are_case_insensitive(written, name):
    _, index = tree()
    assert index[written.lower()].dataset == name


@pytest.mark.parametrize("written", [
    "ABIDE.I", "ABIDE-1", "INDI.NKIRS", "NKI-RS-x", "nki_rs", "UNCWisconsin",
])
def test_no_other_names(home, written):
    user(home, f"[{written}.harvest]\njobs = 1\n")
    with pytest.raises(ConfigError, match="unknown section"):
        parse("ixi", "harvest")


def test_section_names_are_unambiguous(monkeypatch):
    tree()          # builds (no name names two datasets)
    tree.cache_clear()
    monkeypatch.setitem(CATALOG, "IXI", ("x:y", "Another IXI"))
    try:
        with pytest.raises(ValueError, match="names"):
            tree()
    finally:
        tree.cache_clear()


def test_keys_have_no_forbidden_key():
    for keys in KEYS.values():
        assert not set(keys) & set(config.FORBIDDEN)


# ----------------------------------------------------------------------
#   Layers
# ----------------------------------------------------------------------

def test_each_layer(tmp_path, home):
    root, ds = tmp_path / "root", tmp_path / "root" / "IXI"
    a, b = tmp_path / "a.toml", tmp_path / "b.toml"
    steps = [
        ("user", lambda: user(home, "[defaults]\njobs = 1\n")),
        ("root", lambda: located(root, "[defaults]\njobs = 2\n")),
        ("dataset", lambda: located(ds, "[defaults]\njobs = 3\n")),
        ("file", lambda: write(a, "[defaults]\njobs = 4\n")),
        ("file", lambda: write(b, "[defaults]\njobs = 5\n")),
    ]
    for n, (kind, make) in enumerate(steps, 1):
        make()
        args, report = parse("ixi", "harvest", root,
                             files=[f for f in (a, b) if f.exists()])
        assert value(args, "harvest", "jobs") == n
        assert origin(report, "jobs").kind == kind
    env = {"BRAINSPRESSO_OPT_JOBS": "6"}
    args, report = parse("ixi", "harvest", root, files=[a, b], environ=env)
    assert value(args, "harvest", "jobs") == 6
    assert str(origin(report, "jobs")) == "env $BRAINSPRESSO_OPT_JOBS"
    args, report = parse("ixi", "harvest", root, "--jobs", "7",
                         files=[a, b], environ=env)
    assert value(args, "harvest", "jobs") == 7
    assert origin(report, "jobs").kind == "CLI"


def test_later_layer_beats_specific_section(tmp_path, home):
    user(home, "[ixi.harvest]\njobs = 2\n")
    extra = write(tmp_path / "x.toml", "[defaults]\njobs = 3\n")
    args, _ = parse("ixi", "harvest", tmp_path, files=[extra])
    assert value(args, "harvest", "jobs") == 3


def test_most_specific_section_wins(tmp_path, home):
    text = """
[defaults]
jobs = 1
lock-wait = 1
level = "debug"
log = "a.log"
[harvest]
jobs = 2
lock_wait = 2
level = "warning"
[ixi]
jobs = 3
lock-wait = 3
[ixi.harvest]
jobs = 4
"""
    user(home, text)
    args, report = parse("ixi", "harvest", tmp_path)
    assert value(args, "harvest", "jobs") == 4
    assert value(args, "harvest", "lock_wait") == 3
    assert value(args, "log", "level") == "warning"
    assert value(args, "log", "log") == "a.log"
    assert origin(report, "lock-wait").section == "ixi"
    assert origin(report, "level").section == "harvest"
    # another dataset: the generic sections only
    args, _ = parse("cobre", "harvest", tmp_path)
    assert value(args, "harvest", "jobs") == 2


def test_dataset_file_from_parsed_path(tmp_path, home):
    root = tmp_path / "root"
    located(root, "[ixi.harvest]\nkeys = ['T1w']\n")
    located(root / "IXI", "[harvest]\njobs = 3\n")
    other = tmp_path / "elsewhere"
    located(other, "[harvest]\njobs = 5\n")
    args, report = parse("ixi", "harvest", root)
    assert value(args, "keys", "keys") == ("T1w",)
    assert value(args, "harvest", "jobs") == 3
    assert origin(report, "jobs").kind == "dataset"
    # --out: that dataset folder (the root is still `path`)
    args, report = parse("ixi", "harvest", root, "--out", other)
    assert value(args, "harvest", "jobs") == 5
    assert value(args, "keys", "keys") == ("T1w",)
    # a root from the user file
    user(home, f"[defaults]\npath = '{root}'\n")
    args, report = parse("ixi", "harvest")
    assert value(args, "harvest", "jobs") == 3
    assert origin(report, "path").kind == "user"
    # out from the user file, in the section of the dataset
    user(home, f"[ixi]\nout = '{other}'\n")
    args, report = parse("ixi", "harvest")
    assert value(args, "harvest", "jobs") == 5
    assert args["out"] == str(other)


def test_path_and_out_are_one_location(tmp_path, home):
    # a `path` (or `--out`) on the command line: neither `path` nor `out`
    # comes from config, for the values and to find the files
    configured, given = tmp_path / "configured", tmp_path / "given"
    located(configured, "[harvest]\njobs = 2\n")
    located(given / "IXI", "[harvest]\njobs = 3\n")
    located(tmp_path / "out", "[harvest]\njobs = 4\n")
    user(home, f"[defaults]\npath = '{tmp_path / 'nowhere'}'\n"
               f"[ixi]\nout = '{configured}'\n")
    args, report = parse("ixi", "harvest", given)
    assert args["path"] == str(given) and "out" not in args
    assert value(args, "harvest", "jobs") == 3
    assert origin(report, "out").kind == "built-in"
    args, report = parse("ixi", "harvest", "--out", tmp_path / "out")
    assert "path" not in args
    assert value(args, "harvest", "jobs") == 4
    assert origin(report, "path").kind == "built-in"
    # neither given: both from config
    args, report = parse("ixi", "harvest")
    assert args["out"] == str(configured)
    assert value(args, "harvest", "jobs") == 2


def test_user_file_is_not_a_root_file(tmp_path, home, monkeypatch):
    # running from `~`: `~/.brainspresso/config.toml` is the user file,
    # read once (its `out` is not refused as that of a root file)
    monkeypatch.setenv(config.HOME_ENV, str(tmp_path / ".brainspresso"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BDP_PATH", raising=False)
    located(tmp_path, f"[harvest]\njobs = 2\n"
                      f"[ixi]\nout = '{tmp_path / 'IXI'}'\n")
    args, report = parse("ixi", "harvest")
    assert value(args, "harvest", "jobs") == 2
    assert [la.kind for la in report["layers"]] == ["user", "dataset",
                                                   "env"]
    # nor a `--config` file a dataset file
    ds = located(tmp_path / "DS", "[harvest]\njobs = 3\n")
    args, report = parse("ixi", "harvest", "--out", tmp_path / "DS",
                         files=[ds])
    assert origin(report, "jobs").kind == "file"
    assert [la.kind for la in report["layers"]] == ["user", "file", "env"]


def test_root_from_bdp_path(tmp_path, home, monkeypatch):
    located(tmp_path, "[harvest]\njobs = 3\n")
    monkeypatch.setenv("BDP_PATH", str(tmp_path))
    args, report = parse("ixi", "harvest")
    assert value(args, "harvest", "jobs") == 3
    assert origin(report, "jobs").kind == "root"


def test_location_not_in_located_files(tmp_path, home):
    located(tmp_path, "[defaults]\npath = 'x'\n")
    with pytest.raises(ConfigError, match="'path' cannot be set in a root"):
        parse("ixi", "harvest", tmp_path)
    config.located_file(tmp_path).unlink()
    located(tmp_path / "IXI", "[ixi]\nout = 'x'\n")
    with pytest.raises(ConfigError, match="dataset file"):
        parse("ixi", "harvest", tmp_path)


# ----------------------------------------------------------------------
#   Sections
# ----------------------------------------------------------------------

def test_dataset_sections(tmp_path, home):
    user(home, """
[abide1.roast]
exclude-keys = ["rest"]
[ABIDE2.roast]
exclude-keys = ["dti"]
[abide2]
jobs = 7
[nki-rs.harvest]
jobs = 4
[fcon1000]
jobs = 3
""")
    args, report = parse("abide1", "roast", tmp_path)
    assert value(args, "keys", "exclude_keys") == ("rest",)
    assert origin(report, "exclude-keys").section == "abide1.roast"
    args, report = parse("abide2", "roast", tmp_path)
    assert value(args, "keys", "exclude_keys") == ("dti",)
    assert origin(report, "exclude-keys").section == "ABIDE2.roast"
    assert report["parts"] == ("abide2",)
    args, _ = parse("abide2", "harvest", tmp_path)
    assert value(args, "harvest", "jobs") == 7
    args, _ = parse("nki-rs", "harvest", tmp_path)
    assert value(args, "harvest", "jobs") == 4
    args, _ = parse("fcon1000", "harvest", tmp_path)
    assert value(args, "harvest", "jobs") == 3
    args, _ = parse("hbn", "harvest", tmp_path)
    assert value(args, "harvest", "jobs") is None     # built-in


def test_openneuro_accession(tmp_path, home):
    user(home, """
[openneuro.harvest]
tag = "1.0.0"
source = "s3"
[openneuro.ds000224.harvest]
tag = "2.0.0"
[openneuro.ds000224]
out = "here"
""")
    args, report = parse("openneuro", "harvest", "ds000224")
    assert value(args, "openneuro", "tag") == "2.0.0"
    assert value(args, "openneuro", "route") == "s3"
    assert args["out"] == "here"
    assert report["parts"] == ("openneuro", "ds000224")
    args, _ = parse("openneuro", "harvest", "ds000001", tmp_path)
    assert value(args, "openneuro", "tag") == "1.0.0"
    assert "out" not in args
    # accessions compare case-insensitively, as dataset names do
    args, _ = parse("openneuro", "harvest", "DS000224", tmp_path)
    assert value(args, "openneuro", "tag") == "2.0.0"
    user(home, "[OpenNeuro.DS000224.harvest]\ntag = '3.0.0'\n")
    args, _ = parse("openneuro", "harvest", "ds000224", tmp_path)
    assert value(args, "openneuro", "tag") == "3.0.0"


def test_openneuro_dataset_file(tmp_path, home):
    located(tmp_path / "OpenNeuro" / "ds000224",
            "[harvest]\ntag = '3.0.0'\n")
    args, report = parse("openneuro", "harvest", "ds000224", tmp_path)
    assert value(args, "openneuro", "tag") == "3.0.0"
    assert origin(report, "tag").kind == "dataset"


def test_out_names_one_dataset(home):
    for text in ("[openneuro]\nout = 'x'\n", "[OpenNeuro]\nout = 'x'\n",
                 "[harvest]\nout = 'x'\n"):
        user(home, text)
        with pytest.raises(ConfigError, match="'out'"):
            parse("ixi", "harvest")


@pytest.mark.parametrize("section", [
    "abide1.roast", "ABIDE1.roast", "Abide1.roast",
])
def test_case_insensitive_sections(tmp_path, home, section):
    user(home, f"[{section}]\nexclude-keys = ['rest']\n")
    args, report = parse("abide1", "roast", tmp_path)
    assert value(args, "keys", "exclude_keys") == ("rest",)
    assert origin(report, "exclude-keys").section == section


def test_same_section_twice(tmp_path, home):
    user(home, "[ixi.harvest]\njobs = 1\n[IXI.harvest]\njobs = 2\n")
    with pytest.raises(ConfigError, match="also set"):
        parse("ixi", "harvest", tmp_path)


def test_reserved_sections(tmp_path, home):
    user(home, """
[profiles.slurm]
partition = "x"
[grind.fs]
x = 1
[ixi.grind.freesurfer]
threads = 2
[defaults]
jobs = 2
""")
    args, _ = parse("ixi", "harvest", tmp_path)
    assert value(args, "harvest", "jobs") == 2


# ----------------------------------------------------------------------
#   Keys
# ----------------------------------------------------------------------

@pytest.mark.parametrize("key,section", [
    ("yes", "compost"), ("y", "compost"), ("force", "compost"),
    ("ignore_runs", "compost"), ("ignore-runs", "defaults"),
    ("password", "harvest"), ("token", "openneuro"), ("config", "defaults"),
])
def test_forbidden_keys(home, key, section):
    path = user(home, f"[{section}]\n{key} = true\n")
    with pytest.raises(ConfigError) as e:
        parse("ixi", "harvest")
    assert str(path) in str(e.value) and repr(key) in str(e.value)
    assert "cannot be set in a config file" in str(e.value)


@pytest.mark.parametrize("text,match", [
    ("[harvest]\njbos = 2\n", r"\[harvest\]: unknown key 'jbos' "
     r"\(did you mean 'jobs'\?\)"),
    ("[ixi.harvst]\njobs = 1\n", r"unknown section \[ixi.harvst\] "
     r"\(did you mean 'harvest'\?\)"),
    ("[ixy]\njobs = 1\n", r"unknown section \[ixy\] \(did you mean 'ixi'"),
    ("[abide3]\njobs = 1\n", r"unknown section \[abide3\] \(did you "
     r"mean 'abide[12]'"),
    ("jobs = 1\n", "outside a section"),
    ("[ixi.compost]\njobs = 1\n", r"\[ixi.compost\]: unknown key 'jobs'"),
    ("[ixi.harvest]\ntag = 'x'\n", "not an option of harvest"),
    ("[ixi]\ndiscs = [1]\n", "not an option of this dataset"),
    ("[ixi.harvest]\njobs = [1, 2]\n", "takes one value, not a list"),
    ("[ixi.harvest]\nno-jobs = true\n", "no negative flag"),
    ("[ixi.harvest]\nno-dry-run = 1\n", "takes true or false"),
    ("[ixi.harvest]\njobs = 1\njobs_ = 2\n", "unknown key"),
    ("[ixi.harvest]\nexclude_keys = []\nexclude-keys = []\n",
     "set twice"),
])
def test_unknown_or_bad_keys(home, text, match):
    path = user(home, text)
    with pytest.raises(ConfigError, match=match) as e:
        parse("ixi", "harvest")
    assert str(path) in str(e.value)


def test_keys_of_other_datasets_are_ignored(tmp_path, home):
    # OASIS-I's `discs`, ADHD-200's `bids`: not options of IXI harvest
    user(home, "[harvest]\ndiscs = [1]\nbids = false\n"
               "[oasis1.harvest]\ndiscs = [2]\n")
    args, report = parse("ixi", "harvest", tmp_path)
    assert "discs" not in report["origins"]
    args, _ = parse("oasis1", "harvest", tmp_path)
    assert [int(d) for d in value(args, "discs", "discs")] == [2]


def test_type_error_names_file_and_section(home):
    path = user(home, "[ixi.harvest]\njobs = 'many'\n")
    with pytest.raises(CycloptsError) as e:
        parse("ixi", "harvest")
    message = str(e.value)
    assert str(path) in message and "[ixi.harvest]" in message
    assert "many" in message


def test_choice_error(home):
    user(home, "[ixi.harvest]\nkeys = ['nope']\n")
    with pytest.raises(CycloptsError, match="nope"):
        parse("ixi", "harvest")
    user(home, "[ixi.harvest]\nif-exists = 'sometimes'\n")
    with pytest.raises(CycloptsError, match="sometimes"):
        parse("ixi", "harvest")
    user(home, "[cobre.harvest]\nkeys = ['PDw']\n")
    with pytest.raises(CycloptsError, match="PDw"):
        parse("cobre", "harvest")


def test_shared_value_outside_choices(tmp_path, home, caplog):
    # a value of a shared section that is not a choice of a dataset is
    # ignored there (logged); the section of a dataset still fails
    user(home, "[harvest]\nkeys = ['PDw']\nlayout = 'sites'\njobs = 2\n"
               "[defaults]\nlayout = 'flat'\n")
    caplog.set_level(logging.DEBUG, logger="brainspresso.config")
    args, report = parse("ixi", "harvest", tmp_path)
    assert value(args, "keys", "keys") == ("PDw",)
    assert value(args, "layout", "layout") == "flat"     # from [defaults]
    assert origin(report, "layout").section == "defaults"
    assert "layout = 'sites' is not a choice of ixi" in caplog.text
    args, report = parse("cobre", "harvest", tmp_path)
    assert origin(report, "keys").kind == "built-in"
    assert value(args, "harvest", "jobs") == 2
    args, _ = parse("nki-rs", "harvest", tmp_path)
    assert value(args, "layout", "layout") == "sites"
    user(home, "[ixi.harvest]\nlayout = 'sites'\n")
    with pytest.raises(CycloptsError, match="sites"):
        parse("ixi", "harvest", tmp_path)


def test_lists(tmp_path, home):
    user(home, """
[ixi.harvest]
keys = "T1w"
exclude_keys = ["dwi", "angio"]
subs = [2, "sub-012", "3:5"]
[unc-wisconsin.harvest]
keys = []
""")
    args, _ = parse("ixi", "harvest", tmp_path)
    assert value(args, "keys", "keys") == ("T1w",)
    assert value(args, "keys", "exclude_keys") == ("dwi", "angio")
    assert value(args, "select", "subs") == ("2", "sub-012", "3:5")
    # an empty list replaces a non-empty default
    args, _ = parse("unc-wisconsin", "harvest", tmp_path)
    assert value(args, "keys", "keys") == ()
    # the command line replaces the list (it does not add to it)
    args, _ = parse("ixi", "harvest", tmp_path, "--keys", "T2w")
    assert value(args, "keys", "keys") == ("T2w",)
    assert value(args, "keys", "exclude_keys") == ("dwi", "angio")


@pytest.mark.parametrize("text,expected", [
    ("bids = false", False), ("no-bids = true", False),
    ("no_bids = false", True), ("bids = true", True),
])
def test_bools(tmp_path, home, text, expected):
    user(home, f"[adhd200.harvest]\n{text}\n")
    args, report = parse("adhd200", "harvest", tmp_path)
    assert value(args, "bids", "bids") is expected
    assert origin(report, "bids").kind == "user"
    # the command line wins
    args, _ = parse("adhd200", "harvest", tmp_path,
                    "--bids" if not expected else "--no-bids")
    assert value(args, "bids", "bids") is (not expected)


def test_bool_or_none_and_literals(tmp_path, home):
    user(home, "[openneuro.harvest]\ninsecure = true\ntrim = false\n"
               "if_exists = 'overwrite'\nsource = 'graphql'\n")
    args, _ = parse("openneuro", "harvest", "ds1", tmp_path)
    assert value(args, "harvest", "insecure") is True
    assert value(args, "hosted", "trim") is False
    assert value(args, "harvest", "if_exists") == "overwrite"
    assert value(args, "openneuro", "route") == "graphql"


def _policy(args):
    roast = args.get("roast")
    return roast.sidecar_policy if roast is not None else "yes"


@pytest.mark.parametrize("text,policy", [
    ("sidecars = 'yes'", "yes"), ("sidecars = 'no'", "no"),
    ("sidecars = 'only'", "only"),
])
def test_sidecars_key(tmp_path, home, text, policy):
    user(home, f"[roast]\n{text}\n")
    args, report = parse("ixi", "roast", tmp_path)
    assert _policy(args) == policy
    assert origin(report, "sidecars").kind == "user"


@pytest.mark.parametrize("text", ["sidecars = true", "sidecars = 'maybe'"])
def test_sidecars_key_bad(tmp_path, home, text):
    user(home, f"[ixi.roast]\n{text}\n")
    with pytest.raises(CycloptsError):
        parse("ixi", "roast", tmp_path)


@pytest.mark.parametrize("text", ["sidecars-only = true",
                                  "no-sidecars = true"])
def test_old_sidecar_keys_are_unknown(home, text):
    user(home, f"[roast]\n{text}\n")
    with pytest.raises(ConfigError):
        parse("ixi", "roast")


@pytest.mark.parametrize("cli,policy", [
    (["--sidecars", "no"], "no"), (["--json", "yes"], "yes"),
    (["--no-json"], "no"),
])
def test_sidecars_from_the_command_line(tmp_path, home, cli, policy):
    # one option: `--sidecars` or an alias on the command line keeps
    # `sidecars` out of config
    user(home, "[roast]\nsidecars = 'only'\n")
    args, report = parse("ixi", "roast", tmp_path, *cli)
    assert _policy(args) == policy


@pytest.mark.parametrize("text", [
    "json = 'only'", "json = false", "no-json = true", "no_json = true",
])
def test_json_key_is_refused(home, text):
    user(home, f"[roast]\n{text}\n")
    with pytest.raises(ConfigError, match=r"use sidecars \(--sidecars"):
        parse("ixi", "roast")


@pytest.mark.parametrize("text,use", [
    ("[nki-rs.harvest]\nsite_layout = 'flat'\n", "--layout sites:<layout>"),
    ("[defaults]\nsite-layout = 'flat'\n", "--layout sites:<layout>"),
    ("[adhd200.harvest]\nbidsify = true\n", "--bids/--no-bids"),
    ("[harvest]\nno-bidsify = true\n", "--bids/--no-bids"),
])
def test_deprecated_aliases_are_refused(home, text, use):
    path = user(home, text)
    with pytest.raises(ConfigError, match="deprecated alias") as e:
        parse("adhd200", "harvest")
    assert use in str(e.value) and str(path) in str(e.value)


def test_alias_pair_from_the_command_line(tmp_path, home, caplog):
    # an alias on the command line: its option is not read from config
    user(home, "[adhd200.harvest]\nbids = true\n"
               "[nki-rs.harvest]\nlayout = 'flat'\n")
    caplog.set_level(logging.WARNING)
    args, report = parse("adhd200", "harvest", tmp_path, "--no-bidsify")
    assert args["bids"].source_options(ADHD200()) == {"bidsify": False}
    assert "--bidsify/--no-bidsify is deprecated" in caplog.text
    assert origin(report, "bids").kind == "built-in"
    args, _ = parse("adhd200", "harvest", tmp_path, "--no-bids")
    assert args["bids"].source_options(ADHD200()) == {"bidsify": False}
    args, _ = parse("nki-rs", "harvest", tmp_path, "--site-layout", "flat")
    assert value(args, "layout", "layout") is None
    args, _ = parse("nki-rs", "harvest", tmp_path, "--layout", "sites",
                    "--site-layout", "flat")
    assert args["layout"].value(load("nki-rs")) == "sites:flat"


# ----------------------------------------------------------------------
#   --config and environment variables
# ----------------------------------------------------------------------

def test_global_options():
    assert global_options(["--config", "a.toml", "--config=b.toml", "ixi",
                           "harvest", "--config", "c"]) == (
        ["a.toml", "b.toml"], ["ixi", "harvest", "--config", "c"])
    with pytest.raises(ConfigError, match="needs a file"):
        global_options(["--config"])
    with pytest.raises(ConfigError, match="needs a file"):
        global_options(["--config="])


def test_config_repeated(tmp_path, home):
    a = write(tmp_path / "a.toml", "[defaults]\njobs = 1\nlock-wait = 1\n")
    b = write(tmp_path / "b.toml", "[defaults]\njobs = 2\n")
    args, report = parse("ixi", "harvest", tmp_path, files=[a, b])
    assert value(args, "harvest", "jobs") == 2
    assert value(args, "harvest", "lock_wait") == 1
    assert origin(report, "jobs").source == str(b)
    args, _ = parse("ixi", "harvest", tmp_path, files=[b, a])
    assert value(args, "harvest", "jobs") == 1


def test_config_file_errors(tmp_path, capsys):
    with pytest.raises(SystemExit) as e:
        main(["--config", str(tmp_path / "missing.toml"), "ixi", "harvest",
              str(tmp_path), "--dry-run"])
    assert e.value.code == 1
    assert "Config file not found" in capsys.readouterr().err
    bad = write(tmp_path / "bad.toml", "[defaults\n")
    with pytest.raises(SystemExit):
        main(["--config", str(bad), "ixi", "harvest", str(tmp_path)])
    assert f"Config {bad}: TOMLDecodeError" in capsys.readouterr().err
    other = write(tmp_path / "x.ini", "")
    with pytest.raises(SystemExit):
        main(["--config", str(other), "ixi", "harvest", str(tmp_path)])
    assert "not a .toml" in capsys.readouterr().err


def test_yaml_config(tmp_path):
    pytest.importorskip("yaml")
    path = write(tmp_path / "c.yaml", "ixi:\n  harvest:\n    jobs: 3\n"
                                      "    keys: [T1w]\n")
    args, report = parse("ixi", "harvest", tmp_path, files=[path])
    assert value(args, "harvest", "jobs") == 3
    assert value(args, "keys", "keys") == ("T1w",)


def test_env_vars(tmp_path):
    env = {
        "BRAINSPRESSO_OPT_LEVEL": "warning",
        "BRAINSPRESSO_OPT_HARVEST__JOBS": "3",
        "BRAINSPRESSO_OPT_ABIDE1__ROAST__EXCLUDE_KEYS": "rest anat",
        "BRAINSPRESSO_OPT_ADHD200__HARVEST__NO_BIDS": "1",
        "BRAINSPRESSO_OPT_OPENNEURO__DS000224__HARVEST__TAG": "2.0.0",
        "BRAINSPRESSO_OPT_NKI_RS__HARVEST__JOBS": "5",
        # other variables of brainspresso are not options
        "BRAINSPRESSO_LOCKS": "0", "BRAINSPRESSO_LOCK_TIMEOUT": "3",
    }
    args, report = parse("abide1", "roast", tmp_path, environ=env)
    assert value(args, "keys", "exclude_keys") == ("rest", "anat")
    assert value(args, "log", "level") == "warning"
    assert str(origin(report, "exclude-keys")) == \
        "env $BRAINSPRESSO_OPT_ABIDE1__ROAST__EXCLUDE_KEYS"
    args, _ = parse("adhd200", "harvest", tmp_path, environ=env)
    assert value(args, "bids", "bids") is False
    assert value(args, "harvest", "jobs") == 3
    args, _ = parse("openneuro", "harvest", "ds000224", tmp_path,
                    environ=env)
    assert value(args, "openneuro", "tag") == "2.0.0"
    args, _ = parse("nki-rs", "harvest", tmp_path, environ=env)
    assert value(args, "harvest", "jobs") == 5


@pytest.mark.parametrize("var,match", [
    ("BRAINSPRESSO_OPT_NOPE__JOBS", "unknown section 'NOPE'"),
    ("BRAINSPRESSO_OPT_JBOS", "unknown key 'jbos'"),
    ("BRAINSPRESSO_OPT_PASSWORD", "cannot be set"),
    ("BRAINSPRESSO_OPT_IXI____JOBS", "not BRAINSPRESSO_OPT_"),
])
def test_bad_env_vars(tmp_path, var, match):
    with pytest.raises(ConfigError, match=match):
        parse("ixi", "harvest", tmp_path, environ={var: "1"})


def test_real_run(tmp_path, home):
    # a dry run from the dataset's section: nothing is deleted
    root = tmp_path / "ds"
    for sub in ("sub-01", "sub-02"):
        write(root / sub / "anat" / f"{sub}_T1w.nii.gz", "x")
    write(root / "dataset_description.json",
          '{"Name": "x", "BIDSVersion": "1.10.0"}')
    user(home, f"[openneuro.ds1]\nout = '{root}'\n"
               "[openneuro.compost]\ndry-run = true\nexclude-subs = ['01']\n")
    try:
        main(["openneuro", "compost", "ds1"])
    except SystemExit as e:
        assert not e.code
    assert (root / "sub-01").exists()
    try:
        main(["openneuro", "compost", "ds1", "--no-dry-run", "--yes"])
    except SystemExit as e:
        assert not e.code
    assert not (root / "sub-01").exists() and (root / "sub-02").exists()


# ----------------------------------------------------------------------
#   bx config
# ----------------------------------------------------------------------

def test_config_show(tmp_path, home, capsys, monkeypatch):
    path = user(home, "[ixi.harvest]\nkeys = ['T1w']\n"
                      "[defaults]\nlock-wait = 2\n")
    monkeypatch.setenv("BRAINSPRESSO_OPT_HARVEST__LEVEL", "debug")
    try:
        main(["config", "show", "ixi", "harvest", str(tmp_path), "--jobs",
              "3"])
    except SystemExit as e:
        assert not e.code
    out = capsys.readouterr().out
    assert "[ixi.harvest]" in out
    assert "jobs = 3  # CLI" in out
    assert f'keys = ["T1w"]  # {path} [ixi.harvest]' in out
    assert f"lock-wait = 2.0  # {path} [defaults]" in out
    assert "if-exists = \"skip\"  # built-in" in out
    assert 'level = "debug"  # env $BRAINSPRESSO_OPT_HARVEST__LEVEL' in out
    assert "yes" not in out             # (not an option of harvest)


def test_config_show_every_command(tmp_path, home, capsys):
    user(home, "[openneuro.ds1.compost]\nkeep = ['*.tsv']\n")
    try:
        main(["config", "show", "openneuro", "ds1"])
    except SystemExit as e:
        assert not e.code
    out = capsys.readouterr().out
    for command in ("harvest", "compost", "info"):
        assert f"[openneuro.ds1.{command}]" in out
    assert 'keep = ["*.tsv"]' in out
    assert "yes = false  # built-in" in out


def test_config_show_sections(tmp_path, home, capsys):
    path = user(home, "[ABIDE1.roast]\njobs = 2\n[defaults]\nlock-wait = 1\n")
    extra = write(tmp_path / "x.toml", "[defaults]\nlock-wait = 3\n")
    try:
        main(["--config", str(extra), "config", "show"])
    except SystemExit as e:
        assert not e.code
    out = capsys.readouterr().out
    assert f"[defaults]\nlock-wait = 3  # {extra} [defaults]" in out
    assert f"[abide1.roast]\njobs = 2  # {path} [ABIDE1.roast]" in out


def test_config_show_errors(capsys):
    for tokens in (["nope"], ["openneuro", "roast"]):
        with pytest.raises(SystemExit) as e:
            main(["config", "show", *tokens])
        assert e.value.code == 1
    assert "has no command 'roast'" in capsys.readouterr().err


def test_config_paths(tmp_path, home, capsys, monkeypatch):
    located(tmp_path, "")
    extra = write(tmp_path / "x.toml", "")
    monkeypatch.setenv("BRAINSPRESSO_OPT_JOBS", "2")
    try:
        main(["--config", str(extra), "config", "paths", "openneuro",
              "ds1", "--path", str(tmp_path)])
    except SystemExit as e:
        assert not e.code
    lines = capsys.readouterr().out.splitlines()
    assert lines.pop(0).split()[:2] == ["command", "harvest"]
    assert lines[0].split() == ["user", "missing",
                                str(home / "config.toml")]
    assert lines[1].split() == ["root", "found",
                                str(config.located_file(tmp_path))]
    assert lines[2].split() == ["dataset", "missing", str(
        config.located_file(tmp_path / "OpenNeuro" / "ds1"))]
    assert lines[3].split() == ["--config", "found", str(extra)]
    assert lines[4].split() == ["env", "set", "$BRAINSPRESSO_OPT_JOBS"]


# ----------------------------------------------------------------------
#   Start-up: no dataset imported
# ----------------------------------------------------------------------

def run_python(code):
    return subprocess.run([sys.executable, "-c", code], capture_output=True,
                          text=True, cwd=Path(__file__).parents[1])


IMPORTED = (
    "import sys\n"
    "from brainspresso.cli import main\n"
    "try:\n    main({tokens!r})\n"
    "except SystemExit:\n    pass\n"
    "mods = sorted({{m.split('.')[2] for m in sys.modules if m.startswith("
    "'brainspresso.datasets.') and m.count('.') >= 3}})\n"
    "print('DATASETS', mods)\n"
)


@pytest.mark.parametrize("tokens,datasets", [
    (["--help"], []),
    (["config", "--help"], []),
    (["config", "show"], []),
    (["config", "paths"], []),
    (["config", "show", "ixi", "info"], ["IXI"]),
])
def test_config_commands_import_no_dataset(tokens, datasets):
    out = run_python(IMPORTED.format(tokens=tokens))
    assert out.returncode == 0, out.stderr
    assert f"DATASETS {datasets}" in out.stdout, out.stdout


def test_module_app_has_a_loader():
    assert isinstance(cli.app.config[0] if isinstance(
        cli.app.config, tuple) else cli.app.config, Loader)


# ----------------------------------------------------------------------
#   Review of PR #24
# ----------------------------------------------------------------------

def test_show_env_negative_flag(capsys, monkeypatch):
    monkeypatch.setenv("BRAINSPRESSO_OPT_ADHD200__HARVEST__NO_BIDS", "1")
    try:
        main(["config", "show"])
    except SystemExit as e:
        assert not e.code
    out = capsys.readouterr().out
    assert "[adhd200.harvest]\nbids = false  # env " in out


@pytest.mark.parametrize("env", [
    {"BRAINSPRESSO_OPT_JOBS": "1", "BRAINSPRESSO_OPT_DEFAULTS__JOBS": "2"},
    {"BRAINSPRESSO_OPT_JOBS": "1", "BRAINSPRESSO_OPT_jobs": "2"},
    {"BRAINSPRESSO_OPT_IXI__HARVEST__JOBS": "1",
     "BRAINSPRESSO_OPT_ixi__harvest__jobs": "2"},
    {"BRAINSPRESSO_OPT_ADHD200__HARVEST__BIDS": "1",
     "BRAINSPRESSO_OPT_ADHD200__HARVEST__NO_BIDS": "1"},
])
def test_duplicate_env_vars(tmp_path, env):
    with pytest.raises(ConfigError, match="set the same option") as e:
        parse("ixi", "harvest", tmp_path, environ=env)
    for var in env:
        assert f"${var}" in str(e.value)


def test_env_lists_split_as_a_shell(tmp_path):
    env = {"BRAINSPRESSO_OPT_COMPOST__WHERE": '"age >= 60" sex==F'}
    args, _ = parse("ixi", "compost", tmp_path, environ=env)
    assert value(args, "filters", "where") == ("age >= 60", "sex==F")
    with pytest.raises(ConfigError, match="quotation"):
        parse("ixi", "compost", tmp_path,
              environ={"BRAINSPRESSO_OPT_COMPOST__WHERE": '"age'})


def test_show_without_arguments(home, capsys):
    user(home, "[openneuro.harvest]\ntag = '1.0.0'\n"
               "[openneuro.ds1.harvest]\ntag = '2.0.0'\n")
    try:
        main(["config", "show", "openneuro"])
    except SystemExit as e:
        assert not e.code
    out = capsys.readouterr().out
    for command in ("harvest", "compost", "info"):
        assert f"[openneuro.{command}]\n# without DATASET: " in out
    assert 'tag = "1.0.0"  #' in out and "2.0.0" not in out
    assert "jobs = 8  # built-in" in out


def test_paths_of_a_command(tmp_path, home, capsys):
    user(home, f"[compost]\npath = '{tmp_path}'\n")
    try:
        main(["config", "paths", "ixi", "compost"])
    except SystemExit as e:
        assert not e.code
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split()[:2] == ["command", "compost"]
    assert lines[2].split() == ["root", "missing",
                                str(config.located_file(tmp_path))]
    assert lines[3].split()[-1] == str(config.located_file(
        tmp_path / "IXI"))


@pytest.mark.parametrize("section", ["defaults", "harvest", "roast",
                                     "ixi.compost"])
def test_wait_key_is_refused(home, section):
    user(home, f"[{section}]\nwait = 3\n")
    with pytest.raises(ConfigError, match=r"use lock-wait \(--lock-wait\)"):
        parse("ixi", "harvest")


def test_lock_wait_alias_from_the_command_line(tmp_path, home):
    # `--wait` given: `lock-wait` is not read from config
    user(home, "[harvest]\nlock-wait = 7\n")
    args, report = parse("ixi", "harvest", tmp_path, "--wait", "2")
    assert value(args, "harvest", "lock_wait") is None
    assert value(args, "harvest", "wait") == 2
    assert origin(report, "lock-wait").kind == "built-in"


@pytest.mark.parametrize("text,warns", [
    ("[harvest]\nif-exists = 'sometimes'\n", True),
    ("[defaults]\nlayout = 'tree'\n", True),
    ("[harvest]\nkeys = ['nosuchkey']\n", True),
    ("[harvest]\nkeys = ['T1w', 'nosuchkey']\n", True),
    ("[harvest]\nlayout = 'sites'\n", False),        # fits INDI
    ("[harvest]\nkeys = ['PDw']\n", False),          # fits IXI
    ("[roast]\nkeys = ['nosuchkey']\n", False),      # free text somewhere
    ("[ixi.harvest]\njobs = 3\n", False),
])
def test_shared_value_of_no_dataset(tmp_path, home, caplog, text, warns):
    # a shared value that fits no dataset at all: a warning (still
    # ignored); one that fits some datasets: a debug log where it does not
    path = user(home, text)
    caplog.set_level(logging.DEBUG, logger="brainspresso.config")
    parse("cobre", "harvest", tmp_path)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING
                and "not a choice of any dataset" in r.getMessage()]
    assert bool(warnings) is warns
    if warns:
        message = warnings[0].getMessage()
        assert str(path) in message and "not a choice of any dataset" \
            in message
        assert text.split("]")[0] + "]" in message      # the section
        assert text.split("\n")[1].split(" =")[0] in message   # the key


# ----------------------------------------------------------------------
#   A `.brainspresso/` folder with only a config file is not a dataset
# ----------------------------------------------------------------------

def test_root_with_only_a_config_file(tmp_path, monkeypatch, capsys):
    root = tmp_path / "root"
    located(root, "[harvest]\njobs = 2\n")
    for ds in ("IXI", "COBRE"):
        (root / ds / "sub-002" / "anat").mkdir(parents=True)
    state = root / ".brainspresso"
    # neither a dataset (locks of files under it) nor a sites parent
    assert not is_state_folder(root)
    assert not is_sites_parent(root)
    assert find_dataset(root / "elsewhere" / "file.txt") is None
    assert Dataset.lock_states(make_layout("flat", root / "IXI")) == (
        [], [root / "IXI" / ".brainspresso"])
    # a harvest under it takes no lock of the root
    states = []
    original = base.run_lock

    def spy(paths, command, **kwargs):
        states.extend(paths)
        return original(paths, command, **kwargs)

    monkeypatch.setattr(base, "run_lock", spy)
    monkeypatch.setattr(IXI, "harvest", lambda self, layout, args: None)
    try:
        main(["ixi", "harvest", str(root)])
    except SystemExit as e:
        assert not e.code
    assert states == [root / "IXI" / ".brainspresso"]
    # `bx config` writes nothing
    for tokens in (["show"], ["show", "ixi", "harvest", str(root)],
                   ["paths", "ixi", "--path", str(root)]):
        try:
            main(["config", *tokens])
        except SystemExit as e:
            assert not e.code
    assert [p.name for p in state.iterdir()] == ["config.toml"]
    assert not is_state_folder(root)


def test_dataset_with_only_a_config_file(tmp_path):
    # its first command makes it a dataset before any lock is taken
    ds = tmp_path / "IXI"
    located(ds, "[harvest]\njobs = 2\n")
    assert not is_state_folder(ds)
    layout = make_layout("flat", ds)
    layout.ensure_state()
    assert is_state_folder(ds)
    assert find_dataset(ds / "sub-002" / "x.nii.gz") == ds
    assert (ds / ".brainspresso" / "config.toml").exists()


def test_dataset_file_values_are_not_shared(tmp_path, home):
    # the sections of a dataset's own file are that dataset's: a value
    # that is not one of its choices is an error, never ignored
    located(tmp_path / "IXI", "[harvest]\nkeys = ['bold']\n")
    with pytest.raises(CycloptsError, match="bold"):
        parse("ixi", "harvest", tmp_path)
    located(tmp_path / "IXI", "[defaults]\nlayout = 'sites'\n")
    with pytest.raises(CycloptsError, match="sites"):
        parse("ixi", "harvest", tmp_path)
    located(tmp_path / "IXI", "[harvest]\nkeys = ['T1w']\n")
    args, _ = parse("ixi", "harvest", tmp_path)
    assert value(args, "keys", "keys") == ("T1w",)


@pytest.mark.parametrize("kind", ["user", "root", "file", "env"])
def test_shared_list_keeps_what_fits(tmp_path, home, caplog, kind):
    # `[harvest] keys = ["T1w", "bold"]`: IXI takes T1w (not every key)
    text = "[harvest]\nkeys = ['T1w', 'bold']\n"
    files, environ = [], {}
    if kind == "user":
        path = str(user(home, text))
    elif kind == "root":
        path = str(located(tmp_path, text))
    elif kind == "file":
        files = [write(tmp_path / "x.toml", text)]
        path = str(files[0])
    else:
        environ = {"BRAINSPRESSO_OPT_HARVEST__KEYS": "T1w bold"}
        path = "$BRAINSPRESSO_OPT_HARVEST__KEYS"
    caplog.set_level(logging.WARNING, logger="brainspresso.config")
    args, _ = parse("ixi", "harvest", tmp_path, files=files,
                    environ=environ)
    assert value(args, "keys", "keys") == ("T1w",)
    message = [r.getMessage() for r in caplog.records
               if "not choices of ixi" in r.getMessage()]
    assert message and path in message[0] and "'bold'" in message[0]
    assert "[harvest]" in message[0] and "'T1w'" in message[0]
    # none fits: not set from it (a warning too)
    caplog.clear()
    user(home, "[harvest]\nkeys = ['bold']\n")
    args, report = parse("ixi", "harvest", tmp_path / "elsewhere")
    assert origin(report, "keys").kind == "built-in"
    assert "not choices of ixi" in caplog.text


@pytest.mark.parametrize("raw,values", [
    (r"sub-0\d* C:\data\x", (r"sub-0\d*", r"C:\data\x")),
    ("O'Brien sex==F", ("O'Brien", "sex==F")),
    ('"age >= 60" sex==F', ("age >= 60", "sex==F")),
    ("'age >= 60'", ("age >= 60",)),
    (r'"C:\My data"', (r"C:\My data",)),
])
def test_env_list_quoting(tmp_path, raw, values):
    env = {"BRAINSPRESSO_OPT_COMPOST__KEEP": raw}
    args, _ = parse("ixi", "compost", tmp_path, environ=env)
    assert value(args, "compost", "keep") == values


def test_show_unbound_with_location(tmp_path, home, capsys):
    # `--path`/`--out` without the accession: the root file of `--path`,
    # the dataset file of `--out`
    root, out = tmp_path / "root", tmp_path / "out"
    located(root, "[openneuro.harvest]\ntag = '1.0.0'\n")
    located(out, "[harvest]\njobs = 3\n")
    for tokens, expected in (
        (["--path", str(root)],
         ['tag = "1.0.0"', f'path = {json.dumps(str(root))}  # CLI']),
        ([f"--out={out}"],
         ["jobs = 3", f'out = {json.dumps(str(out))}  # CLI']),
    ):
        try:
            main(["config", "show", "openneuro", "harvest", *tokens])
        except SystemExit as e:
            assert not e.code
        text = capsys.readouterr().out
        assert "# without DATASET" in text
        for line in expected:
            assert line in text


@pytest.mark.parametrize("tokens", [
    ["ixi", "harvest", "--jobs"],
    ["openneuro", "harvest", "ds1", "--tag"],
    ["ixi", "harvest", "--nope"],
])
def test_show_errors_are_cyclopts_errors(capsys, tokens):
    with pytest.raises(SystemExit) as e:
        main(["config", "show", *tokens])
    assert e.value.code
    captured = capsys.readouterr()
    assert "Traceback" not in captured.out + captured.err
    assert "without DATASET" not in captured.out


def test_show_hides_deprecated_aliases(capsys):
    for tokens in (["ixi", "roast"], ["adhd200", "harvest"],
                   ["nki-rs", "harvest"]):
        try:
            main(["config", "show", *tokens])
        except SystemExit as e:
            assert not e.code
    out = capsys.readouterr().out
    for key in ("json", "no-json", "wait", "site-layout", "bidsify"):
        assert f"\n{key} = " not in out
    assert "\nsidecars = " in out and "\nlock-wait = " in out


@pytest.mark.parametrize("names", [
    ["config.toml~"], [".config.toml.swp"], [".DS_Store"],
    ["config.toml.bak", "notes.txt"],
])
def test_config_folder_with_other_files(tmp_path, names):
    # only state entries make a state folder (not backups, swap files)
    located(tmp_path, "[harvest]\njobs = 2\n")
    for name in names:
        (tmp_path / ".brainspresso" / name).write_text("x")
    assert not is_state_folder(tmp_path)
    (tmp_path / "IXI" / "sub-002").mkdir(parents=True)
    assert not is_sites_parent(tmp_path)


@pytest.mark.parametrize("entry", ["locks", "runs", "tasks", "orig"])
def test_state_entries(tmp_path, entry):
    (tmp_path / ".brainspresso" / entry).mkdir(parents=True)
    assert is_state_folder(tmp_path)


def test_state_file_entries(tmp_path):
    for entry in ("manifest.json", "run"):
        folder = tmp_path / entry
        (folder / ".brainspresso").mkdir(parents=True)
        (folder / ".brainspresso" / entry).write_text("{}")
        assert is_state_folder(folder)
    # an empty one too (created by an older version, or by hand)
    (tmp_path / "empty" / ".brainspresso").mkdir(parents=True)
    assert is_state_folder(tmp_path / "empty")


def test_state_folder_of_another_process_is_seen_at_once(tmp_path):
    # B (this process) looks up a file of a dataset whose `.brainspresso/`
    # only holds `config.toml`; A (another process) makes it a state
    # folder: B sees the dataset at once (one lock file for both)
    ds = tmp_path / "IXI"
    located(ds, "[harvest]\njobs = 2\n")
    target = ds / "sub-002" / "anat" / "x.nii.gz"
    locks.forget_datasets()
    assert find_dataset(target) is None
    before = lock_file(target)[0]
    code = ("import sys\nfrom brainspresso.bids.layout import make_layout\n"
            "make_layout('flat', sys.argv[1]).ensure_state()\n")
    out = subprocess.run([sys.executable, "-c", code, str(ds)],
                         capture_output=True, text=True,
                         cwd=Path(__file__).parents[1])
    assert out.returncode == 0, out.stderr
    assert find_dataset(target) == ds
    after = lock_file(target)[0]
    assert after != before and after.is_relative_to(ds / ".brainspresso")


def test_user_config_home_is_never_a_dataset(tmp_path, monkeypatch):
    home = tmp_path / "home" / ".brainspresso"
    home.mkdir(parents=True)
    monkeypatch.setenv(config.HOME_ENV, str(home))
    # empty, or with anything in it: still the user config folder
    assert not is_state_folder(home.parent)
    (home / "locks").mkdir()
    assert not is_state_folder(home.parent)
    assert find_dataset(home.parent / "Downloads" / "x.zip") is None
    # through a symbolic link too
    (tmp_path / "link").symlink_to(home.parent)
    assert not is_state_folder(tmp_path / "link")


def test_sites_with_only_a_config_file_are_not_locked(tmp_path):
    # a site whose `.brainspresso/` only holds `config.toml` is not a run
    # lock target (its run records would make it a state folder mid-run)
    root = tmp_path / "DS"
    sites = make_layout("sites", root, child_kind="flat")
    for name in ("A", "B"):
        (root / name / "sub-01").mkdir(parents=True)
    sites.ensure_state()
    located(root / "C", "[harvest]\njobs = 2\n")
    (root / "C" / "sub-01").mkdir()
    found = Layout.detect(root)
    _, targets = Dataset.lock_states(found)
    assert root / "C" / ".brainspresso" not in targets
    # and `run_lock` refuses it
    with pytest.raises(LockError, match="No state folder"):
        with run_lock(root / "C" / ".brainspresso", "harvest"):
            pass
    assert [p.name for p in (root / "C" / ".brainspresso").iterdir()] == [
        "config.toml"]


def test_show_effective_defaults(capsys):
    # no `null` (not TOML): the effective value, or a comment
    for tokens in (["ixi", "roast"], ["ixi", "roast", "--no-json"],
                   ["ixi", "harvest", "--wait", "3"]):
        try:
            main(["config", "show", *tokens])
        except SystemExit as e:
            assert not e.code
    out = capsys.readouterr().out
    assert "null" not in out
    blocks = out.split("\n\n")
    assert 'sidecars = "yes"  # built-in' in blocks[0]
    assert 'sidecars = "no"' in blocks[1]
    assert "lock-wait = 0  # built-in" in blocks[0]
    assert "lock-wait = 3.0" in blocks[2]
    assert "# path: not set  # built-in" in blocks[0]


def test_show_unbound_one_location(tmp_path, home, capsys):
    # a configured `out` is not used with `--path` on the command line
    user(home, f"[openneuro.ds1]\nout = '{tmp_path / 'x'}'\n"
               f"[defaults]\npath = '{tmp_path / 'y'}'\n")
    try:
        main(["config", "show", "openneuro", "harvest", "--out",
              str(tmp_path / "z")])
    except SystemExit as e:
        assert not e.code
    out = capsys.readouterr().out
    assert "# path: not set  # built-in" in out
    assert f'out = {json.dumps(str(tmp_path / "z"))}  # CLI' in out


@pytest.mark.parametrize("command", ["harvest", "compost"])
def test_wait_with_lock_wait_zero(tmp_path, command):
    args, _ = parse("ixi", command, tmp_path, "--wait", "3",
                    "--lock-wait", "0")
    groups = {k: v for k, v in args.items() if k not in ("path", "out")}
    with pytest.raises(UsageError, match="contradict"):
        IXI().check(command, base.Args.of(IXI(), command, **groups))


def test_shared_list_warns_once(tmp_path, home, caplog):
    user(home, "[harvest]\nkeys = ['T1w', 'nosuchkey', 'bold']\n")
    caplog.set_level(logging.DEBUG, logger="brainspresso.config")
    args, _ = parse("ixi", "harvest", tmp_path)
    assert value(args, "keys", "keys") == ("T1w",)
    warnings = [r.getMessage() for r in caplog.records
                if r.levelno == logging.WARNING]
    # once for the key of no dataset, once for the key of other datasets
    assert len(warnings) == 2
    assert "'nosuchkey' not a choice of any dataset" in warnings[0]
    assert "['bold'] are not choices of ixi" in warnings[1]
    assert "nosuchkey" not in warnings[1]
