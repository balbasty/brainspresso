"""
Golden outputs of the roasters: record, check and diff.

Each ported roaster's test fixture (the synthetic sources that the
`make_source` helpers of its roast tests write, or its fake server) is
roasted with `bx <dataset> roast` in both layouts and with a few fixed
option sets (`CASES`: the default, `--sidecars only`, `--keys meta`,
`--keys` of a few image keys, `--keys` of every key, and a second roast
of the default). For each, the harness records:

* `files`: every file under the dataset folder, with the SHA-256 of its
  content (the harvested inputs, the lock files and the state files
  below are left out);
* `tasks`: the task records (`.brainspresso/tasks/`), with their input
  signature recomputed from normalised inputs (`signature`);
* `manifest`: `.brainspresso/manifest.json`;
* `runs`: per roast, its exit status, its report (`status`, `counts`,
  `errors`) and its warnings (log records of `brainspresso` at WARNING
  and above).

The baseline is one JSON per dataset in `tests/golden/`;
`tests/test_golden.py` checks a reduced matrix (`CI_MATRIX`) in the
main test run.

Normalised, so that the outputs are the same on every run and platform:

* paths: the dataset folder is `<root>`, the installed package
  `<pkg>`, separators are `/`; tracebacks are cut;
* files that start with the gzip magic (`.nii.gz`, `.mgz`...): the
  SHA-256 of their decompressed content (`gz:` prefix), since the
  compressed bytes depend on the platform's zlib;
* task records: `started`, `finished` and `run` (a random id) are
  dropped; `config_hash`, the input signature, is replaced by the
  signature of the same shard with its inputs relative to the dataset
  and a fixed size and mtime (and its `config` normalised as above):
  it changes when what a shard is signed with changes (the inputs it
  lists, its config, its scope, the signature version), which would
  make every existing dataset re-read its archives;
* manifest: the `date` of each run is dropped;
* warnings: the warn-once caches of the modules (`_warned`) are cleared
  before each roast, as each `bx` command is a process of its own;
* left out: the harvested inputs (files that the roast did not write
  or change), lock files (`.brainspresso/locks/`, named after absolute
  paths) and log records of other packages.

The fixtures are made deterministic (gzip mtime 0 while they are
built, every input dated `MTIME`); xlsx files written by openpyxl keep
their own timestamps (they are inputs, only read).

Usage::

    python scripts/golden.py check [--datasets ...] [--layouts ...]
        [--cases ...] [--ci] [--work DIR] [--verbose]
    python scripts/golden.py record [...]
    python scripts/golden.py diff [--base REV]

`check` roasts and prints what differs from the baseline (exit status
1 if anything does); `record` regenerates the baseline (only the
entries of the selection are replaced): run it when a change of output
is intended, and paste the output of `diff` (the committed baseline at
`--base`, default `HEAD`, against the working tree) in the pull
request. `--work DIR` keeps the roasted datasets (`DIR/<dataset>/
<layout>/<case>/`), to compare file contents between two checkouts
(`diff -r`). `--ci` selects the matrix of `tests/test_golden.py`;
`--verbose` shows what the commands print.
"""
import argparse
import contextlib
import gzip
import hashlib
import io
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import types
import typing
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
TESTS = REPO / "tests"
GOLDEN = TESTS / "golden"
sys.path[:0] = [str(TESTS)]
if __name__ == "__main__":
    # (the checkout, as `python -m pytest` from its root; imported by
    # `tests/test_golden.py`, the package that the tests import)
    sys.path[:0] = [str(REPO)]

# (the test modules, once their folder is on the path)
import roast_utils  # noqa: E402
import test_girder  # noqa: E402
import test_ixi  # noqa: E402
import test_roast_abide1_nitrc  # noqa: E402
import test_roast_abide1_xnat  # noqa: E402
import test_roast_abide2  # noqa: E402
import test_roast_bullitt  # noqa: E402
import test_roast_cobre  # noqa: E402
import test_roast_oasis1  # noqa: E402
import test_roast_oasis2  # noqa: E402
import test_roast_oasis3  # noqa: E402
from validate_bids import patched  # noqa: E402

import brainspresso  # noqa: E402
from brainspresso.actions import engine  # noqa: E402
from brainspresso.actions.state import TASKS_DIR  # noqa: E402
from brainspresso.bids.layout import STATE_DIR, Layout  # noqa: E402
from brainspresso.cli import main as bx_main  # noqa: E402
from brainspresso.datasets import base  # noqa: E402
from brainspresso.datasets.bullitt import roaster as bullitt  # noqa: E402
from brainspresso.datasets.indi.abide.i import roaster as abide1  # noqa: E402
from brainspresso.datasets.indi.abide.ii import (  # noqa: E402
    roaster as abide2,
)
from brainspresso.datasets.indi.cobre import roaster as cobre  # noqa: E402
from brainspresso.datasets.ixi import roaster as ixi  # noqa: E402
from brainspresso.datasets.oasis.i import roaster as oasis1  # noqa: E402
from brainspresso.datasets.oasis.ii import roaster as oasis2  # noqa: E402
from brainspresso.datasets.oasis.iii import roaster as oasis3  # noqa: E402
from brainspresso.datasets.unc_wisconsin import harvest as unc  # noqa: E402
from brainspresso.datasets.unc_wisconsin import (  # noqa: E402
    roaster as uncwisc,
)

LAYOUTS = roast_utils.LAYOUTS
PKG = Path(brainspresso.__file__).resolve().parent
#: mtime of every input of a fixture (2001-09-09)
MTIME = 1_000_000_000
#: Command that regenerates the baseline
RECORD = "python scripts/golden.py record"


# ----------------------------------------------------------------------
#   Fixtures: name -> (command, build(parent, layout) -> root, extra
#   roast arguments, the image keys of the `keys-images` case)
# ----------------------------------------------------------------------

def source(folder, *builders):
    """A fixture whose sources `builders[i](root)` write"""
    def build(parent: Path, layout: str) -> Path:
        root = parent / folder
        for builder in builders:
            builder(root)
        return root
    return build


def unc_wisconsin(parent, layout):
    """UNC-Wisconsin, harvested from the fake Girder server"""
    with test_girder.serve(test_girder.TREE) as fake, \
            patched(unc, KITWARE=fake.api, SCAN_DATA=fake.root):
        bx("unc-wisconsin", "harvest", parent, "--layout", layout,
           "--keys", "all")
    return parent / "UNC-Wisconsin"


class Fixture(typing.NamedTuple):
    """A roasted fixture"""

    #: `bx <command> roast`
    command: str
    #: build(parent, layout) -> dataset folder
    build: typing.Callable
    #: Its roaster module (`KEYS`: the keys of the `keys-all` case)
    roaster: types.ModuleType
    #: Image keys of the `keys-images` case
    images: tuple = ()
    #: Arguments of every roast
    extra: tuple = ()


FIXTURES = {
    "abide1-nitrc": Fixture("abide1", source(
        "ABIDE-1", test_roast_abide1_nitrc.make_source), abide1,
        ("rest",)),
    "abide1-xnat": Fixture("abide1", source(
        "ABIDE-1", test_roast_abide1_xnat.make_source), abide1,
        ("fs",), ("--source", "xnat")),
    "abide2": Fixture("abide2", source(
        "ABIDE-2", test_roast_abide2.make_source), abide2, ("dti",)),
    "bullitt": Fixture("bullitt", source(
        "Bullitt", test_roast_bullitt.make_source), bullitt,
        ("mprage", "vessels")),
    "cobre": Fixture("cobre", source(
        "COBRE", test_roast_cobre.make_source), cobre, ("T1w",)),
    "ixi": Fixture("ixi", source(
        "IXI", lambda root: test_ixi.write_tarballs(root / "sourcedata")),
        ixi, ("dwi",)),
    "oasis1": Fixture("oasis1", source(
        "OASIS-1", test_roast_oasis1.make_source,
        lambda root: test_roast_oasis1.freesurfer_disc(root / "sourcedata")),
        oasis1, ("fs",)),
    "oasis2": Fixture("oasis2", source(
        "OASIS-2", test_roast_oasis2.make_source), oasis2, ("raw",)),
    "oasis3": Fixture("oasis3", source(
        "OASIS-3", test_roast_oasis3.make_source,
        test_roast_oasis3.add_freesurfer), oasis3, ("T1w", "fs")),
    "unc-wisconsin": Fixture("unc-wisconsin", unc_wisconsin, uncwisc,
                             ("dwi",)),
}


def cases(name: str) -> dict[str, list[list[str]]]:
    """Option sets of a fixture: case -> arguments of each roast"""
    fixture = FIXTURES[name]
    return {
        "default": [[]],
        "sidecars-only": [["--sidecars", "only"]],
        "keys-meta": [["--keys", "meta"]],
        "keys-images": [["--keys", *fixture.images]],
        # (every leaf: `all` is not a choice of every dataset)
        "keys-all": [["--keys", *sorted(fixture.roaster.KEYS.leaves)]],
        "rerun": [[], []],
    }


#: Case names (the same for every fixture)
CASES = list(cases("cobre"))
#: What `tests/test_golden.py` checks: dataset -> [(layout, case)];
#: every dataset: a roast and its re-run and every key in one layout
#: (alternating), `--sidecars only` in the other
CI_MATRIX = {
    name: [(LAYOUTS[i % 2], "rerun"), (LAYOUTS[i % 2], "keys-all"),
           (LAYOUTS[1 - i % 2], "sidecars-only")]
    for i, name in enumerate(FIXTURES)
}


# ----------------------------------------------------------------------
#   Determinism
# ----------------------------------------------------------------------

class _FixedTime:
    """The `time` module of `gzip`, at the epoch (gzip mtime 0)"""

    def __getattr__(self, name):
        return getattr(time, name)

    @staticmethod
    def time():
        return 0.0


@contextlib.contextmanager
def quiet_config():
    """No user config file, no `BRAINSPRESSO_OPT_*` variable"""
    saved = dict(os.environ)
    with tempfile.TemporaryDirectory() as home:
        os.environ["BRAINSPRESSO_CONFIG_HOME"] = home
        for var in list(os.environ):
            if var.startswith("BRAINSPRESSO_OPT_"):
                del os.environ[var]
        try:
            yield
        finally:
            os.environ.clear()
            os.environ.update(saved)


def build_fixture(name: str, parent: Path, layout: str) -> Path:
    """Write (or harvest) the sources of a fixture; date them `MTIME`"""
    with patched(gzip, time=_FixedTime()):
        root = FIXTURES[name].build(parent, layout)
    for path in root.rglob("*"):
        # (no fixture has symbolic links; `follow_symlinks=False` is not
        # available on Windows)
        if not path.is_symlink():
            os.utime(path, (MTIME, MTIME))
    return root


# ----------------------------------------------------------------------
#   Normalisation
# ----------------------------------------------------------------------

class Normaliser:
    """Replace the machine-specific parts of strings and values"""

    def __init__(self, root: Path):
        names = {}
        for path, name in ((root, "<root>"), (PKG, "<pkg>")):
            for form in (str(path), path.as_posix(), repr(str(path))[1:-1],
                         str(path.resolve()), path.resolve().as_posix()):
                names[form] = name
        # (longest first: the package may be below the dataset)
        self.names = sorted(names.items(), key=lambda x: -len(x[0]))
        self.path = re.compile(r"(<root>|<pkg>)([^\s'\"`,;:)\]}]*)")

    def text(self, value: str) -> str:
        """Normalise a string"""
        value = value.split("Traceback (most recent call last)")[0]
        for form, name in self.names:
            value = value.replace(form, name)
        return self.path.sub(
            lambda m: m[1] + re.sub(r"\\+", "/", m[2]), value).rstrip()

    def value(self, value):
        """Normalise a JSON-like value"""
        if isinstance(value, dict):
            return {self.text(str(k)): self.value(v)
                    for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self.value(v) for v in value]
        if isinstance(value, os.PathLike):
            value = os.fspath(value)
        if isinstance(value, str):
            return self.text(value)
        return value


class _Stat:
    """`os` of the engine, whose `stat` is fixed (normalised signature)"""

    def __getattr__(self, name):
        return getattr(os, name)

    @staticmethod
    def stat(path, *args, **kwargs):
        return os.stat_result((0,) * 10)


class _Shard:
    """What `engine._signature` reads of a shard"""

    def __init__(self, inputs, config):
        self.inputs = inputs
        self.config = config


@contextlib.contextmanager
def signatures(norm: Normaliser, table: dict):
    """Record the normalised signature of every signed shard"""
    real = engine._signature

    def signed(ctx, policy, shard, scope=None):
        signature = real(ctx, policy, shard, scope)
        if signature is not None:
            fake = _Shard(norm.value(list(shard.inputs)),
                          norm.value(getattr(shard, "config", None)))
            with patched(engine, os=_Stat()):
                table[signature] = real(ctx, policy, fake, scope)
        return signature

    with patched(engine, _signature=signed):
        yield


class _Warnings(logging.Handler):
    """Collect the log records at WARNING and above"""

    def __init__(self):
        super().__init__(logging.WARNING)
        self.records = []

    def emit(self, record):
        self.records.append(
            f"{record.levelname}: {record.name}: {record.getMessage()}")


def forget_warnings() -> None:
    """
    Clear the warn-once caches (`_warned`) of the loaded modules.

    Each `bx` command is a process of its own: a roast warns as it
    would on the command line, whatever ran before it.
    """
    for name, module in list(sys.modules.items()):
        if name.startswith("brainspresso."):
            cache = getattr(module, "_warned", None)
            if isinstance(cache, set):
                cache.clear()


def digest(path: Path) -> str:
    """SHA-256 of a file (`gz:` and of its content if it is gzipped)"""
    data = path.read_bytes()
    if data[:2] == b"\x1f\x8b":
        try:
            data = gzip.decompress(data)
        except (OSError, EOFError, zlib.error):
            pass
        else:
            return "gz:" + hashlib.sha256(data).hexdigest()
    return hashlib.sha256(data).hexdigest()


def tree(root: Path) -> dict[str, str]:
    """Relative path -> digest of every file under a folder"""
    return {p.relative_to(root).as_posix(): digest(p)
            for p in sorted(root.rglob("*")) if p.is_file()}


#: Fields of a task record that change on every run
VOLATILE = ("started", "finished", "run")


def snapshot(root: Path, inputs: dict, norm: Normaliser,
             table: dict) -> dict:
    """Files, task records and manifest of a roasted dataset"""
    layout = Layout.detect(root)
    state = (layout.state if layout else root / STATE_DIR).relative_to(root)
    skip = (state / "locks", state / TASKS_DIR, state / "manifest.json")
    files = {}
    for path, sha in tree(root).items():
        if inputs.get(path) == sha:
            continue
        if any(Path(path).is_relative_to(s) for s in skip):
            continue
        files[path] = sha
    for path in inputs:
        if not (root / path).exists():
            files[path] = None              # (an input removed)
    tasks = {}
    for path in sorted((root / state / TASKS_DIR).glob("*.json")):
        record = json.loads(path.read_text())
        for key in VOLATILE:
            record.pop(key, None)
        if "config_hash" in record:
            sig = record.pop("config_hash")
            record["signature"] = table.get(sig, sig and "<unknown>")
        tasks[record.get("task", path.stem)] = norm.value(record)
    manifest = root / state / "manifest.json"
    manifest = json.loads(manifest.read_text()) if manifest.exists() else {}
    for run in manifest.get("runs", []):
        run.pop("date", None)
    return {"files": files, "tasks": tasks, "manifest": norm.value(manifest)}


# ----------------------------------------------------------------------
#   Roasts
# ----------------------------------------------------------------------

#: Show what the commands print (`--verbose`)
VERBOSE = False


def bx(*args) -> int:
    """Run `bx *args`; return its exit status"""
    with contextlib.ExitStack() as stack:
        if not VERBOSE:
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
        try:
            status = bx_main([str(a) for a in args])
        except SystemExit as e:
            status = e.code
    return int(status or 0)


def roast_case(name: str, layout: str, case: str, parent: Path) -> dict:
    """Build a fixture, roast it as a case says; return the record"""
    command, extra = FIXTURES[name].command, list(FIXTURES[name].extra)
    root = build_fixture(name, parent, layout)
    inputs = tree(root)
    norm = Normaliser(root)
    table, runs = {}, []
    reports = []

    def capture(roaster, ctx, **kwargs):
        report = real(roaster, ctx, **{**kwargs, "display": False})
        reports.append(report)
        return report

    real = base.run_roast
    logger = logging.getLogger("brainspresso")
    for args in cases(name)[case]:
        forget_warnings()
        handler = _Warnings()
        logger.addHandler(handler)
        del reports[:]
        try:
            with signatures(norm, table), \
                    patched(base, run_roast=capture):
                status = bx(command, "roast", "--out", root, "--layout",
                            layout, *extra, *args)
        finally:
            logger.removeHandler(handler)
        run = {"args": [*extra, *args], "exit": status}
        for report in reports:
            run["report"] = norm.value({
                "status": report.status,
                "counts": report.summary(),
                "errors": [list(e) for e in report.errors],
            })
        run["warnings"] = [norm.text(r) for r in handler.records]
        runs.append(run)
    return {"runs": runs, **snapshot(root, inputs, norm, table)}


def run(selection: dict, work: Path | None = None) -> dict:
    """Roast a selection; return `{dataset: {"<layout>/<case>": record}}`"""
    out = {}
    with quiet_config(), contextlib.ExitStack() as stack:
        if work is None:
            work = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        for name, pairs in selection.items():
            for layout, case in pairs:
                parent = work / name / layout / case
                parent.mkdir(parents=True)
                out.setdefault(name, {})[f"{layout}/{case}"] = \
                    roast_case(name, layout, case, parent)
    return out


# ----------------------------------------------------------------------
#   Baseline and differences
# ----------------------------------------------------------------------

def baseline_path(name: str, folder: Path = GOLDEN) -> Path:
    """Baseline file of a dataset"""
    return folder / f"{name}.json"


def load(name: str, rev: str | None = None) -> dict:
    """Baseline of a dataset (in the working tree, or at a git revision)"""
    if rev is None:
        path = baseline_path(name)
        return json.loads(path.read_text()) if path.exists() else {}
    rel = baseline_path(name).relative_to(REPO).as_posix()
    out = subprocess.run(["git", "show", f"{rev}:{rel}"], cwd=REPO,
                         capture_output=True, text=True)
    return json.loads(out.stdout) if out.returncode == 0 else {}


def save(name: str, records: dict) -> None:
    """Merge records into the baseline of a dataset"""
    data = load(name)
    data.update(records)
    data = {k: data[k] for k in sorted(data)}
    GOLDEN.mkdir(parents=True, exist_ok=True)
    baseline_path(name).write_text(
        json.dumps(data, indent=1, sort_keys=True) + "\n")


def _short(value) -> str:
    """Compact JSON, digests shortened"""
    text = json.dumps(value, sort_keys=True)
    return re.sub(r"\b([0-9a-f]{12})[0-9a-f]{52}\b", r"\1", text)


def _flat(value, prefix=()):
    """(path, value) of the leaves of a JSON value (lists are leaves)"""
    if isinstance(value, dict) and value:
        for key, item in value.items():
            yield from _flat(item, (*prefix, key))
    else:
        yield prefix, value


def _list_diff(old: list, new: list) -> list[str]:
    """Items removed (-) and added (+), as multisets"""
    old_s = [_short(x) for x in old]
    new_s = [_short(x) for x in new]
    out = []
    rest = list(new_s)
    for item in old_s:
        if item in rest:
            rest.remove(item)
        else:
            out.append(f"- {item}")
    out += [f"+ {item}" for item in rest]
    if not out and old_s != new_s:
        out.append("(order changed)")
    return out


def _value_diff(path: str, old, new) -> list[str]:
    """Lines of the difference of two leaves"""
    if isinstance(old, list) and isinstance(new, list):
        return [f"{path}: {line}" for line in _list_diff(old, new)]
    return [f"{path}: {_short(old)} -> {_short(new)}"]


def diff_record(old: dict, new: dict) -> list[str]:
    """Readable differences between two records of a case"""
    lines = []
    if not old:
        return ["(new case)"]
    if not new:
        return ["(case removed)"]
    old_f, new_f = old.get("files", {}), new.get("files", {})
    for path in sorted(set(old_f) | set(new_f)):
        if path not in new_f:
            lines.append(f"files: - {path}")
        elif path not in old_f:
            lines.append(f"files: + {path}")
        elif old_f[path] != new_f[path]:
            lines.append(f"files: ~ {path} ({_short(old_f[path])} -> "
                         f"{_short(new_f[path])})")
    old_r, new_r = old.get("runs", []), new.get("runs", [])
    for i in range(max(len(old_r), len(new_r))):
        a = old_r[i] if i < len(old_r) else {}
        b = new_r[i] if i < len(new_r) else {}
        lines += [f"run {i + 1}: {line}" for line in _leaf_diff(a, b)]
    for key in ("tasks", "manifest"):
        a, b = old.get(key, {}), new.get(key, {})
        if key == "tasks":
            for task in sorted(set(a) | set(b)):
                if task not in b:
                    lines.append(f"tasks: - {task}")
                elif task not in a:
                    lines.append(f"tasks: + {task}")
                else:
                    lines += [f"tasks: {task}: {line}"
                              for line in _leaf_diff(a[task], b[task])]
        else:
            lines += [f"manifest: {line}" for line in _leaf_diff(a, b)]
    return lines


def _leaf_diff(old: dict, new: dict) -> list[str]:
    """Differences of the leaves of two JSON values"""
    a, b = dict(_flat(old)), dict(_flat(new))
    lines = []
    for key in sorted(set(a) | set(b), key=lambda k: [str(x) for x in k]):
        path = ".".join(map(str, key))
        if key not in b:
            lines.append(f"{path}: - {_short(a[key])}")
        elif key not in a:
            lines.append(f"{path}: + {_short(b[key])}")
        elif a[key] != b[key]:
            lines += _value_diff(path, a[key], b[key])
    return lines


def diff(old: dict, new: dict, keys=None) -> list[str]:
    """Differences of the baselines of several datasets (Markdown)"""
    lines = []
    for name in sorted(set(old) | set(new)):
        a, b = old.get(name, {}), new.get(name, {})
        for key in sorted(keys.get(name, ()) if keys else set(a) | set(b)):
            got = diff_record(a.get(key, {}), b.get(key, {}))
            if got:
                lines += [f"* `{name}` `{key}`"]
                lines += [f"  * {line}" for line in got]
    return lines


def check(selection: dict, work: Path | None = None) -> list[str]:
    """Roast a selection; return its differences from the baseline"""
    new = run(selection, work)
    old = {name: load(name) for name in new}
    return diff(old, new, {name: [f"{lay}/{case}" for lay, case in pairs]
                           for name, pairs in selection.items()})


def select(datasets, layouts, case_names) -> dict:
    """The selection of the command line: dataset -> [(layout, case)]"""
    return {name: [(lay, case) for lay in layouts for case in case_names]
            for name in datasets}


def main(argv=None):
    """Command-line entry point"""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("mode", choices=("check", "record", "diff"))
    parser.add_argument("--datasets", nargs="+", choices=list(FIXTURES),
                        default=list(FIXTURES))
    parser.add_argument("--layouts", nargs="+", choices=LAYOUTS,
                        default=list(LAYOUTS))
    parser.add_argument("--cases", nargs="+", choices=CASES, default=CASES)
    parser.add_argument("--ci", action="store_true",
                        help="the reduced matrix of tests/test_golden.py")
    parser.add_argument("--work", type=Path,
                        help="keep the roasted datasets in this folder")
    parser.add_argument("--base", default="HEAD",
                        help="diff: the git revision of the old baseline")
    parser.add_argument("--verbose", action="store_true",
                        help="show the output and the log of the commands")
    args = parser.parse_args(argv)
    global VERBOSE
    VERBOSE = args.verbose
    if not VERBOSE:
        # (the console log of `brainspresso`, `logging.basicConfig`)
        for handler in logging.getLogger().handlers:
            if type(handler) is logging.StreamHandler:
                handler.setLevel(logging.CRITICAL + 1)
    selection = CI_MATRIX if args.ci else \
        select(args.datasets, args.layouts, args.cases)
    if args.mode == "diff":
        old = {name: load(name, args.base) for name in args.datasets}
        new = {name: load(name) for name in args.datasets}
        lines = diff(old, new)
        print("\n".join(lines) if lines else "No change.")
        return 0
    start = time.monotonic()
    if args.mode == "record":
        for name, records in run(selection, args.work).items():
            save(name, records)
        print(f"Recorded in {time.monotonic() - start:.1f} s")
        return 0
    lines = check(selection, args.work)
    print("\n".join(lines) if lines else "No change.")
    print(f"({time.monotonic() - start:.1f} s)", file=sys.stderr)
    return 1 if lines else 0


if __name__ == "__main__":
    sys.exit(main())
