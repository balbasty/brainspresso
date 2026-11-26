"""
Roast runner: run the actions that roasters yield.

A **roaster** is a plain callable ``roaster(ctx) -> Iterator[Action |
Roaster]``. It reads source data (`utils.archive`, from `ctx.source`) and
yields actions (`actions.Action`) that write the BIDS dataset, or
sub-roasters (**shards**: one per archive, per subject...). It knows
nothing about selection, `--json`, `IfExists`, counters or display: the
runner applies them to the outputs that each action declares.

```python
def roast_ixi(ctx):
    yield from copy_templates(ctx, ["README", "dataset_description.json"])
    for key in ("T1w", "T2w"):
        yield Shard(roast_anat, key, tag=key)     # picklable sub-roaster

run_roast(roast_ixi, ctx, jobs=4)
```

Rules
-----
* Each action is run (to completion) **before the roaster is asked for
  the next one**: actions may close over open archive handles.
* An action runs if any of its outputs may be selected
  (`ctx.keeps(path)`: `ctx.selection.keeps` on the path relative to
  the raw dataset, the subject/session filters only outside of it;
  outputs in the folder of a category that is not wanted, e.g.
  `derivatives/<pipeline>/`, are dropped), and if its `kind` passes the
  JSON policy (`--json no` drops sidecars, `--json only` drops data;
  tables and `meta` files (bval/bvec) always pass; so do `staging`
  actions, whose outputs are inputs of later actions). Outputs of
  several default kinds (`[nii, json]`) need an explicit `kind`, else
  the action is an error. An action without outputs (`Fail`) always
  runs. A claim action runs unless none of its claimed paths can be
  selected (`keeps(path, partial=True) is False`); the sub-actions it
  yields are selected one by one; a claim that yields no action is
  counted as "empty". All outputs of an action that runs are written;
  `IfExists` applies to the set.
* `IfExists`: the action's own, else the runner's (`ctx.ifexists`),
  except that under `skip` tables (kind "table", e.g. the pristine
  copies in `<state>/orig/`) use `different`: they are regenerated and
  replaced if their content changed (e.g. after a new harvest).
* Outputs in `<state>/orig/` (pristine tables) are selected as the
  table they are the pristine copy of.
* An action that fails is reported and counted; the roast goes on. A
  roaster that raises is reported, and the actions it would have yielded
  next are lost (a generator cannot be resumed).
* After all actions and shards: the `finalize(ctx)` hook of every
  roaster that has one (optional attribute; `Shard` forwards that of
  its function), children first, the top-level roaster last. It sees
  the finished tree and may yield more actions and shards. Then, even
  if the roast failed, the common steps: visible tables trimmed from
  their pristine copies (`trim_originals`), layout descriptions and
  manifest (`finish_roast`, with a `status`: "complete", "partial" if
  anything failed, "failed" if the runner itself stopped).

Shards, parallelism and state
-----------------------------
With `jobs > 1`, every shard runs in a local process pool ("spawn"
start method), through a thin `submit(task, resources=None)` protocol
(`LocalExecutor`): shards yielded by roasters running in the main
process are submitted; shards yielded inside a worker are handed back
to the main process when that shard finishes, and submitted then. So
a shard runs **asynchronously**: a roaster must not rely on the outputs
of the shards it yields (use `finalize`), and shards must not write the
same files as each other or as the top-level roaster. **Shards must be
picklable** (module-level functions, `functools.partial` of them, or
`Shard`), as must `ctx`; a shard that cannot be pickled runs in the
main process (or in its worker), and an unpicklable `ctx` makes the
whole roast run in the main process, both with a warning. If a worker
dies (`BrokenProcessPool`), the shards that had started are counted as
errors and the others, as well as all later ones, run in the main
process. With `jobs=1` (default), everything runs sequentially, in
order. ("spawn" re-imports the main module in each worker: a script
that calls `run_roast` with `jobs > 1` needs an `if __name__ ==
"__main__":` guard; the `bx` command does.)

Each shard with an `id` (and every shard run in a worker) writes its
state to `<state>/tasks/<id>.json` as it goes (status, counts,
progress, outputs, start and end; `bids.harvest.write_task_state`); the
main process reads progress from these while shards run. A shard that
declares its `inputs` (source archives) and finished without errors is
skipped by a later roast under `IfExists` "skip" if its inputs (paths,
sizes, mtimes), the selection and the JSON policy are unchanged and
its outputs exist: its archives are not reopened. A shard that yielded
shards is never skipped this way.
"""
from __future__ import annotations

import copy
import hashlib
import json
import multiprocessing
import os
import pickle
import time
import traceback
import uuid
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, CancelledError, Future
from concurrent.futures import ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Literal, Union

from brainspresso.actions.action import Action, IfExists, Kind
from brainspresso.bids.keys import KeyAliases, KeySelection
from brainspresso.bids.layout import ORIG_DIR, Layout, resolve_layout

lg = getLogger(__name__)

Roaster = Callable[["RoastContext"], Union[Iterator[Union[Action, "Roaster"]],
                                          None]]
JsonPolicy = Literal["yes", "no", "only"]
OUTCOMES = ("done", "skipped", "error", "empty")
# Seconds between reads of the state files of running shards
POLL_INTERVAL = 0.2
# Seconds between writes of a task state on outcomes (progress: always)
WRITE_INTERVAL = 0.5
_SIGNATURE_VERSION = 1


class Shard:
    """
    A picklable sub-roaster: `fn(ctx, *args, **kwargs)`.

    Parameters
    ----------
    fn : callable(ctx, *args, **kwargs) -> iterator
    tag : str
        Default progress group of its actions
    id : str
        Task id: its state is written to `<state>/tasks/<id>.json`
        (e.g. "roast/subjects/sub-01"); unique in the dataset
    inputs : list[path]
        Source files it reads (archives): with an `id`, an unchanged
        signature lets a later roast under "skip" skip it
    advance : str
        Progress group to advance by one when it is done
    unit : dict
        Entities of the unit (task state), e.g. `{"sub": "01"}`
    source : path
        `ctx.source` while it runs (default: that of its parent)

    These names cannot be keyword arguments of `fn`.
    """

    def __init__(self, fn, *args, tag=None, id=None, inputs=None,
                 advance=None, unit=None, source=None, **kwargs):
        self.fn, self.args, self.kwargs = fn, args, kwargs
        self.tag, self.id, self.advance = tag, id, advance
        self.inputs = list(inputs) if inputs is not None else None
        self.unit = dict(unit or {})
        self.source = source

    def __call__(self, ctx):
        return self.fn(ctx, *self.args, **self.kwargs)

    @property
    def finalize(self) -> "Shard | None":
        """`fn.finalize`, with the same arguments"""
        fin = getattr(self.fn, "finalize", None)
        if fin is None:
            return None
        return Shard(fin, *self.args, tag=self.tag, source=self.source,
                     **self.kwargs)

    def __repr__(self):
        args = ", ".join(map(repr, self.args))
        return f"{getattr(self.fn, '__name__', self.fn)}({args})"


@dataclass
class RoastContext:
    """
    What a roaster needs to know about the roast.

    Attributes
    ----------
    layout : Layout
        Paths of the dataset
    selection : KeySelection
        Selection on the raw dataset (keys, subjects, sessions), and
        the categories wanted (`wants`)
    aliases : KeyAliases | None
        Keys of the dataset (predicates, categories and their folders)
    json : {"yes", "no", "only"}
        Write sidecars only (`only`), not at all (`no`), or with data
    ifexists : {"skip", "overwrite", "different", "refresh", "error"}
        Default `IfExists` of actions that do not set their own
    options : dict
        Dataset-specific options
    templates : Path | None
        Folder of templates shipped with the dataset (`resource`)
    tag : str
        Progress group of the roaster that is running (default of
        `progress` and of the actions it yields)
    source : Path | None
        Folder of the source data the running roaster reads (default:
        `layout.source`; a roaster or `Shard` with a `source` attribute
        sets it while it runs)
    """
    layout: Layout
    selection: KeySelection = field(default_factory=KeySelection)
    aliases: KeyAliases | None = None
    json: JsonPolicy = "yes"
    ifexists: IfExists.Choice = "skip"
    options: dict = field(default_factory=dict)
    templates: Path | None = None
    tag: str = "roast"
    source: Path | None = None

    def __post_init__(self):
        self._reporter: _Reporter | None = None
        self._folders: list[tuple[Path, frozenset]] | None = None
        if isinstance(self.json, bool):
            self.json = "yes" if self.json else "no"
        self.json = str(self.json).lower()
        if self.json not in ("yes", "no", "only"):
            raise ValueError(f"json must be yes, no or only: {self.json}")
        self.source = Path(self.source) if self.source is not None \
            else Path(self.layout.source)

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_reporter"] = None
        return state

    # ------------------------------------------------------------------
    def wants(self, key: str) -> bool:
        """Whether a key (a category: metadata, a pipeline...; or a BIDS
        key) is wanted"""
        return self.selection.wants(key)

    def resource(self, name: str | os.PathLike) -> Path | None:
        """A metadata file: from `ctx.source` if there, else from the
        templates shipped with the dataset, else None (copying None is
        an error; `actions.helpers.copy_templates` reports it)"""
        for folder in (self.source, self.templates):
            if folder is not None and (Path(folder) / name).exists():
                return Path(folder) / name
        return None

    def progress(
        self,
        total: int | None = None,
        *,
        advance: int = 0,
        tag: str | None = None,
    ) -> None:
        """Declare the number of units of a progress group (`total`),
        or that some are finished (`advance`). Units are what the
        roaster says they are (subjects, archives...)."""
        if self._reporter is not None:
            self._reporter.progress(tag or self.tag, total, advance)

    def accepts(self, kind: Kind) -> bool:
        """JSON policy on a kind of output (`staging` actions are not
        subject to it: see the runner)"""
        if self.json == "no":
            return kind != "sidecar"
        if self.json == "only":
            return kind != "data"
        return True

    def _unorig(self, path) -> Path:
        """A pristine table (`<state>/orig/...`) as the table it copies"""
        path = Path(path)
        try:
            return self.layout.root / path.relative_to(
                Path(self.layout.state) / ORIG_DIR)
        except ValueError:
            return path

    def _relative(self, path) -> tuple[Path, str] | None:
        """(BIDS dataset, relative path) of an output"""
        try:
            dataset, rel = self.layout.relative(self._unorig(path))
        except ValueError:
            return None
        return Path(dataset), "" if rel == "." else rel

    def _blocked(self, path) -> bool:
        """In the folder of categories that are all unwanted"""
        if self._folders is None:
            folders = defaultdict(set)
            for category, prefixes in getattr(
                    self.aliases, "folders", {}).items():
                for prefix in prefixes:
                    folders[self.layout.resolve(prefix)].add(category)
            self._folders = [(p, frozenset(c)) for p, c in folders.items()]
        if not self._folders:
            return False
        path = self._unorig(path)
        for prefix, categories in self._folders:
            if path == prefix or prefix in path.parents:
                if not any(map(self.selection.wants, categories)):
                    return True
        return False

    def keeps(
        self, path: str | os.PathLike, partial: bool = False
    ) -> bool | None:
        """
        Whether an output (absolute path, or a claim with
        `partial=True`) is selected: `selection.keeps` on its path in
        the raw dataset, the subject/session filters elsewhere
        (derivatives, `sourcedata/`); False in the folder of a category
        that is not wanted. Pristine tables (`<state>/orig/`) are
        judged as the table they are the copy of.
        """
        if self._blocked(path):
            return False
        found = self._relative(path)
        if found is None:
            return True             # not in the dataset: not ours to filter
        dataset, rel = found
        if dataset == Path(self.layout.raw):
            return self.selection.keeps(rel, partial)
        return self.selection.base.keeps(rel, partial)


def make_context(
    root: str | os.PathLike | Layout,
    aliases: KeyAliases | None = None,
    *,
    layout: str | None = None,
    select_keys: Iterable[str] = (),
    exclude_keys: Iterable[str] = (),
    json: JsonPolicy | bool = "yes",
    ifexists: IfExists.Choice = "skip",
    templates: str | os.PathLike | None = None,
    options: dict | None = None,
    source: str | os.PathLike | None = None,
    **filters,
) -> RoastContext:
    """
    Context of a roast from command-line values.

    Parameters
    ----------
    root : path | Layout
        Dataset folder (`<root>/<DS>`)
    aliases : KeyAliases
        Keys of the dataset
    layout : {"study", "flat"}
        Layout of a new dataset (must match an existing one)
    select_keys, exclude_keys : list[str]
        `--keys`, `--exclude-keys`
    source : path
        Source folder (default: `layout.source`)
    **filters
        `Selection` filters (`subs`, `exclude_subs`, `sessions`...)
    """
    aliases = aliases or KeyAliases(set())
    return RoastContext(
        layout=resolve_layout(root, layout),
        selection=aliases.selection(select_keys, exclude_keys, **filters),
        aliases=aliases,
        json=json,
        ifexists=ifexists,
        options=dict(options or {}),
        templates=Path(templates) if templates is not None else None,
        source=source,
    )


def roast_dataset(
    roaster: Roaster,
    root: str | os.PathLike | Layout,
    aliases: KeyAliases,
    *,
    keys: Iterable[str] = (),
    exclude_keys: Iterable[str] = (),
    templates: str | os.PathLike | None = None,
    jobs: int = 1,
    display: bool = True,
    info: dict | None = None,
    **options,
) -> "RoastReport":
    """
    `make_context` + `run_roast`, with the names of the command line:
    the `roast()` function of a dataset.

    Parameters
    ----------
    roaster : callable
    root : path
        Dataset folder
    aliases : KeyAliases
    keys, exclude_keys : list[str]
        `--keys`, `--exclude-keys`
    templates : path
        Templates shipped with the dataset
    jobs, display, info
        See `run_roast`
    **options
        `make_context` options (`layout`, `json`, `ifexists`, `subs`,
        `exclude_subs`, `sessions`...)
    """
    ctx = make_context(root, aliases, select_keys=as_list(keys),
                       exclude_keys=as_list(exclude_keys),
                       templates=templates, **options)
    return run_roast(roaster, ctx, jobs=jobs, display=display, info=info)


def as_list(value, fmt: str | None = None) -> list:
    """A command-line value (one value, a list, or None) as a list;
    `fmt` formats each value (e.g. "{:03d}" for subject numbers)"""
    if value is None:
        return []
    if isinstance(value, (str, int)):
        value = [value]
    value = list(value)
    if fmt is not None:
        value = [fmt.format(int(v) if "d" in fmt else v) for v in value]
    return value


# ----------------------------------------------------------------------
#   Report
# ----------------------------------------------------------------------

@dataclass
class RoastReport:
    """What a roast did"""
    counts: dict[str, Counter] = field(
        default_factory=lambda: defaultdict(Counter)
    )
    errors: list[tuple[str, str, str]] = field(default_factory=list)
    outputs: list[Path] = field(default_factory=list)
    progress: dict[str, tuple[int, int | None]] = field(default_factory=dict)
    status: str = "complete"

    def total(self, outcome: str) -> int:
        return sum(c[outcome] for c in self.counts.values())

    def summary(self) -> dict:
        return {tag: dict(c) for tag, c in sorted(self.counts.items())}


class _Reporter:
    """Records what happens (worker processes send these events back;
    intermediate statuses are not kept), and the state of the running
    task, if any (`tasks`: innermost last)"""

    def __init__(self):
        self.events: list[tuple] = []
        self.tasks: list[_TaskState] = []

    def status(self, tag: str, path: str, status: dict) -> None:
        pass

    def outcome(self, tag, path, outcome, message="") -> None:
        if self.tasks:
            self.tasks[-1].outcome(tag, outcome, message)
        self._outcome(tag, path, outcome, message)

    def progress(self, tag, total=None, advance=0) -> None:
        if self.tasks:
            self.tasks[-1].progress(tag, total, advance)
        self._progress(tag, total, advance)

    def outputs(self, paths: list[Path]) -> None:
        if self.tasks:
            self.tasks[-1].outputs(paths)
        self._outputs(paths)

    def _outcome(self, tag, path, outcome, message="") -> None:
        self.events.append(("outcome", tag, path, outcome, message))

    def _progress(self, tag, total=None, advance=0) -> None:
        self.events.append(("progress", tag, total, advance))

    def _outputs(self, paths) -> None:
        self.events.append(("outputs", list(paths)))


class _MainReporter(_Reporter):
    """Counts, collects and displays events"""

    def __init__(self, out: Callable[[dict], Any] | None = None):
        super().__init__()
        self.out = out
        self.report = RoastReport()
        self.totals: dict[str, int] = {}
        self.finished: Counter = Counter()
        # progress of running shards, from their state files
        self.live: dict[str, dict[str, list]] = {}

    def _show(self, row: dict) -> None:
        if self.out is not None:
            self.out(row)

    def replay(self, events: Iterable[tuple]) -> None:
        """Events of a shard run in a worker"""
        for event in events:
            getattr(self, "_" + event[0])(*event[1:])

    def status(self, tag, path, status):
        self._show({"modality": tag, "path": path, **status})

    def _outcome(self, tag, path, outcome, message=""):
        counts = self.report.counts[tag]
        counts[outcome] += 1
        row = {"modality": tag, "path": path, "status": outcome}
        if outcome == "error":
            self.report.errors.append((tag, path, message))
            lg.error(f"{path}: {message}")
            row.update({"message": message, "errors": counts["error"]})
        elif outcome == "skipped":
            row["skipped"] = counts["skipped"]
        self._show(row)

    def _progress(self, tag, total=None, advance=0):
        if total is not None:
            self.totals[tag] = total
        self.finished[tag] += advance
        self.report.progress[tag] = (self.finished[tag], self.totals.get(tag))
        self._show_progress(tag)

    def _show_progress(self, tag):
        total = self.totals.get(tag)
        done = self.finished[tag]
        for progress in self.live.values():
            live_total, live_done = progress.get(tag, (None, 0))
            total = total or live_total
            done += live_done
        if total:
            pct = min(100, 100 * done / total)
            self._show({"modality": tag, "progress": pct})

    def _outputs(self, paths):
        self.report.outputs.extend(paths)

    def live_progress(self, task: str, progress: dict | None) -> None:
        """Progress of a running shard (None: it is finished)"""
        old = self.live.pop(task, {})
        if progress:
            self.live[task] = progress
        for tag in set(old) | set(progress or {}):
            if (progress or {}).get(tag) != old.get(tag):
                self._show_progress(tag)

    def skipped_task(self, record: dict, tag: str, name: str) -> None:
        """A shard skipped as unchanged: what it did last time"""
        for group, counts in (record.get("counts") or {}).items():
            done = sum(n for outcome, n in counts.items()
                       if outcome in ("done", "skipped"))
            self.report.counts[group]["skipped"] += done
        for group, (total, advance) in (record.get("progress") or {}).items():
            self._progress(group, total, advance)
        self._outputs(self.root / p for p in record.get("outputs") or [])
        self._show({"modality": tag, "path": name, "status": "skipped",
                    "message": "unchanged since the last roast"})

    def close(self, tags: Iterable[str]) -> None:
        for tag in tags:
            row = {"modality": tag, "status": "done", "message": ""}
            if self.totals.get(tag):
                row["progress"] = 100
            self._show(row)


# ----------------------------------------------------------------------
#   Task state (<state>/tasks/<id>.json)
# ----------------------------------------------------------------------

class _TaskState:
    """State file of a shard, written as it runs"""

    def __init__(self, layout: Layout, task: str, run: str, *,
                 signature: str | None = None, unit: dict | None = None):
        from brainspresso.bids.harvest import now
        self.layout, self.task = layout, task
        self.record = {
            "stage": "roast", "pipeline": None, "unit": dict(unit or {}),
            "status": "running", "config_hash": signature,
            "started": now(), "finished": None, "error": None,
            "outputs": [], "run": run, "counts": {}, "progress": {},
            "children": 0,
        }
        self.last = 0.0
        self.write()

    def write(self) -> None:
        from brainspresso.bids.harvest import write_task_state
        self.last = time.monotonic()
        try:
            write_task_state(self.layout.state, self.task, self.record)
        except OSError as e:          # progress is a convenience
            lg.warning(f"Could not write the state of {self.task}: {e}")

    def outcome(self, tag, outcome, message="") -> None:
        counts = self.record["counts"].setdefault(tag, {})
        counts[outcome] = counts.get(outcome, 0) + 1
        if outcome == "error" and not self.record["error"]:
            self.record["error"] = message
        if time.monotonic() - self.last > WRITE_INTERVAL:
            self.write()

    def progress(self, tag, total=None, advance=0) -> None:
        progress = self.record["progress"].setdefault(tag, [None, 0])
        if total is not None:
            progress[0] = total
        progress[1] += advance
        self.write()

    def outputs(self, paths) -> None:
        root = Path(self.layout.root)
        for path in paths:
            try:
                path = Path(path).relative_to(root).as_posix()
            except ValueError:
                path = str(path)
            self.record["outputs"].append(path)

    def finish(self, status: str | None = None) -> None:
        from brainspresso.bids.harvest import now
        failed = any(c.get("error") for c in self.record["counts"].values())
        self.record["status"] = status or ("failed" if failed else "done")
        self.record["finished"] = now()
        self.write()


def _read_task(layout: Layout, task: str) -> dict | None:
    from brainspresso.bids.harvest import task_path
    path = task_path(layout.state, task)
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def _signature(ctx: RoastContext, shard: Shard) -> str | None:
    """Input signature of a shard: its inputs (path, size, mtime), the
    selection and the JSON policy"""
    if not shard.inputs:
        return None
    inputs = []
    for path in shard.inputs:
        try:
            stat = os.stat(path)
            inputs.append([str(path), stat.st_size, stat.st_mtime_ns])
        except OSError:
            inputs.append([str(path), None, None])
    selection = getattr(ctx.selection, "fingerprint", None)
    blob = json.dumps({
        "version": _SIGNATURE_VERSION, "inputs": inputs, "json": ctx.json,
        "selection": selection() if selection else repr(ctx.selection),
    }, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()


# ----------------------------------------------------------------------
#   Execution
# ----------------------------------------------------------------------

class LocalExecutor:
    """
    The local backend of the submit protocol: `submit(task,
    resources=None) -> Future` runs `task()` (a picklable callable) in a
    "spawn" process pool. `resources` (CPUs, memory... for the grind
    backends) are accepted and ignored.
    """

    def __init__(self, jobs: int):
        self.pool = ProcessPoolExecutor(
            jobs, mp_context=multiprocessing.get_context("spawn")
        )

    def submit(self, task: Callable[[], Any],
               resources: dict | None = None) -> Future:
        return self.pool.submit(task)

    def shutdown(self) -> None:
        self.pool.shutdown(wait=True, cancel_futures=True)


@dataclass
class _Pending:
    shard: Shard
    tag: str
    task: str
    auto: bool


@dataclass
class _ShardTask:
    """A shard run in a worker process"""
    shard: Shard
    ctx: RoastContext
    tag: str
    task: str
    run: str
    signature: str | None = None

    def __call__(self) -> tuple[list, list, list]:
        ctx = copy.copy(self.ctx)
        reporter = _Reporter()
        ctx._reporter = reporter
        engine = _Engine(ctx, reporter, run=self.run, worker=True)
        reporter.tasks.append(_TaskState(
            ctx.layout, self.task, self.run, signature=self.signature,
            unit=self.shard.unit,
        ))
        engine.run_shard(self.shard, self.tag)
        state = reporter.tasks.pop()
        # finalize hooks go back to the main process if they can
        finalizers = []
        for fin, tag in engine.finalizers:
            if _picklable(fin, warn=False):
                finalizers.append((fin, tag))
            else:
                engine.drive(fin, tag)
        state.record["children"] += len(engine.deferred)
        state.finish()
        return reporter.events, engine.deferred, finalizers


def _name(obj) -> str:
    return getattr(obj, "__name__", None) or repr(obj)


def _picklable(obj, warn: bool = True) -> bool:
    try:
        pickle.dumps(obj)
    except Exception as e:
        if warn:
            lg.warning(f"{_name(obj)} is not picklable ({e}): "
                       f"run in the current process")
        return False
    return True


class _Engine:
    """Drives roasters, runs actions, dispatches shards"""

    def __init__(self, ctx: RoastContext, reporter: _Reporter, *,
                 run: str, executor=None, worker: bool = False):
        self.ctx, self.reporter = ctx, reporter
        self.run, self.executor, self.worker = run, executor, worker
        self.pending: dict[Future, _Pending] = {}
        self.deferred: list[tuple[Shard, str]] = []     # worker only
        self.finalizers: list[tuple[Roaster, str]] = []
        self.count = 0
        self.mtimes: dict[str, int] = {}
        self.polled = 0.0

    # ------------------------------------------------------------------
    #   Roasters
    # ------------------------------------------------------------------
    def drive(self, roaster, tag: str) -> None:
        """Consume a roaster: run its actions, dispatch its shards"""
        ctx = self.ctx
        tag = getattr(roaster, "tag", None) or tag
        source = getattr(roaster, "source", None)
        finalize = getattr(roaster, "finalize", None)
        if finalize is not None:
            self.finalizers.append((finalize, tag))
        outer_source = ctx.source
        if source is not None:
            # for the roaster, its actions and the shards it yields
            ctx.source = Path(source)
        try:
            self._drive(roaster, tag)
        finally:
            ctx.source = outer_source

    def _drive(self, roaster, tag: str) -> None:
        ctx = self.ctx
        outer = ctx.tag

        def enter():
            # `ctx.tag`: that of the roaster whose code runs
            ctx.tag = tag

        def leave():
            ctx.tag = outer

        try:
            enter()
            items = roaster(ctx)
            items = iter(items) if items is not None else iter(())
        except Exception as e:
            self.roaster_error(tag, roaster, e)
            return
        finally:
            leave()
        while True:
            try:
                enter()
                item = next(items)
            except StopIteration:
                break
            except Exception as e:
                self.roaster_error(tag, roaster, e)
                break
            finally:
                leave()
            if isinstance(item, Action):
                self.run_action(item, tag)
            elif callable(item):
                self.dispatch(item, tag)
            else:
                self.reporter.outcome(
                    tag, _name(roaster), "error",
                    f"roaster yielded {type(item).__name__}, "
                    f"not an Action or a roaster")
            if self.pending:
                self.poll()

    def roaster_error(self, tag, roaster, e) -> None:
        lg.error(f"{_name(roaster)}: {e}\n{traceback.format_exc()}")
        self.reporter.outcome(tag, _name(roaster), "error",
                              f"{type(e).__name__}: {e}")

    # ------------------------------------------------------------------
    #   Shards
    # ------------------------------------------------------------------
    def dispatch(self, item, tag: str) -> None:
        """A shard yielded by a roaster: skip, submit, defer or run"""
        tag = getattr(item, "tag", None) or tag
        if self.reporter.tasks:
            self.reporter.tasks[-1].record["children"] += 1
        if self.worker:
            if _picklable(item, warn=False):
                self.deferred.append((item, tag))       # to the main process
                return
        elif self.skip(item, tag):
            return
        elif self.executor is not None and _picklable(item):
            self.submit(item, tag)
            return
        self.run_shard(item, tag)

    def run_shard(self, shard, tag: str) -> None:
        """Run a shard here (with its state file if it has an id)"""
        task_id = getattr(shard, "id", None)
        if task_id and not self.worker:
            self.reporter.tasks.append(_TaskState(
                self.ctx.layout, task_id, self.run,
                signature=_signature(self.ctx, shard), unit=shard.unit,
            ))
        try:
            self.drive(shard, tag)
            advance = getattr(shard, "advance", None)
            if advance:
                self.ctx.progress(advance=1, tag=advance)
        finally:
            if task_id and not self.worker:
                self.reporter.tasks.pop().finish()

    def skip(self, shard, tag: str) -> bool:
        """Skip a shard whose inputs did not change (under "skip")"""
        if not isinstance(shard, Shard) or not shard.id or not shard.inputs:
            return False
        if IfExists.from_any(self.ctx.ifexists) is not IfExists.SKIP:
            return False
        record = _read_task(self.ctx.layout, shard.id)
        if not record or record.get("status") != "done" \
                or record.get("children"):
            return False
        if record.get("config_hash") != _signature(self.ctx, shard):
            return False
        root = Path(self.ctx.layout.root)
        if not all((root / p).exists() for p in record.get("outputs", [])):
            return False
        self.reporter.skipped_task(record, tag, _name(shard))
        return True

    def submit(self, shard, tag: str) -> None:
        if not isinstance(shard, Shard):
            shard = Shard(shard, tag=tag,
                          source=getattr(shard, "source", None))
        self.count += 1
        task_id = shard.id or f"roast/.auto/{self.run}-{self.count}"
        task = _ShardTask(shard, self.ctx, tag, task_id, self.run,
                          _signature(self.ctx, shard) if shard.id else None)
        try:
            future = self.executor.submit(task)
        except (BrokenProcessPool, RuntimeError) as e:
            # the pool broke (a worker died) before we collected it
            self.stop_pool(e)
            self.run_shard(shard, tag)
            return
        self.pending[future] = _Pending(shard, tag, task_id, not shard.id)

    def poll(self, block: bool = False) -> None:
        """Collect the shards that finished (all of them if `block`),
        and the progress of the others"""
        while self.pending:
            done, _ = wait(list(self.pending),
                           timeout=POLL_INTERVAL if block else 0,
                           return_when=FIRST_COMPLETED)
            for future in done:
                if future in self.pending:      # (not collected by a
                    self.collect(future)        # shard run meanwhile)
            self.read_progress()
            if not block:
                return

    def read_progress(self) -> None:
        """Progress of running shards, from their state files"""
        if time.monotonic() - self.polled < POLL_INTERVAL:
            return
        self.polled = time.monotonic()
        from brainspresso.bids.harvest import task_path
        for pending in list(self.pending.values()):
            path = task_path(self.ctx.layout.state, pending.task)
            try:
                mtime = path.stat().st_mtime_ns
            except OSError:
                continue
            if self.mtimes.get(pending.task) == mtime:
                continue
            self.mtimes[pending.task] = mtime
            record = _read_task(self.ctx.layout, pending.task)
            if record and record.get("run") == self.run:
                self.reporter.live_progress(pending.task,
                                            record.get("progress"))

    def collect(self, future: Future) -> None:
        pending = self.pending.pop(future)
        self.reporter.live_progress(pending.task, None)
        self.mtimes.pop(pending.task, None)
        try:
            events, deferred, finalizers = future.result()
        except (BrokenProcessPool, CancelledError):
            self.broken(pending)
        except Exception as e:
            self.reporter.outcome(pending.tag, _name(pending.shard), "error",
                                  f"{type(e).__name__}: {e}")
        else:
            self.reporter.replay(events)
            self.finalizers.extend(finalizers)
            for shard, tag in deferred:
                self.dispatch(shard, tag)
        if pending.auto:
            from brainspresso.bids.harvest import task_path
            task_path(self.ctx.layout.state, pending.task).unlink(
                missing_ok=True)

    def broken(self, pending: _Pending) -> None:
        """A worker died: the shards that had started are errors, the
        others (and all later ones) run in the main process"""
        self.stop_pool()
        record = _read_task(self.ctx.layout, pending.task)
        if record and record.get("run") == self.run:
            self.reporter.outcome(
                pending.tag, _name(pending.shard), "error",
                "the worker process died while this shard ran (it, or "
                "a shard running at the same time, crashed it)")
            record["status"], record["error"] = "failed", "worker died"
            from brainspresso.bids.harvest import write_task_state
            write_task_state(self.ctx.layout.state, pending.task, record)
        else:
            self.run_shard(pending.shard, pending.tag)

    def stop_pool(self, error: Exception | None = None) -> None:
        if self.executor is None:
            return
        lg.error(f"A worker process died{f' ({error})' if error else ''}"
                 f": the remaining shards run in the main process")
        executor, self.executor = self.executor, None
        try:
            executor.shutdown()
        except Exception as e:      # already broken
            lg.debug(f"shutdown: {e}")

    # ------------------------------------------------------------------
    #   Actions
    # ------------------------------------------------------------------
    def run_action(self, action: Action, tag: str,
                   staging: bool = False) -> list[Path]:
        """Select and run one action (and the sub-actions it yields)"""
        ctx, reporter = self.ctx, self.reporter
        tag = action.tag or tag
        action.tag = tag
        staging = staging or action.staging
        declared = action.declared
        if action.claim is not None:
            paths = action.claim.paths
            if all(ctx.keeps(p, partial=True) is False for p in paths):
                return []
            name = paths[0].name or str(paths[0])
        elif declared:
            name = getattr(action, "name", "") or declared[0].name
            kinds = action.kinds
            if not action.kind_given and len(kinds) > 1:
                reporter.outcome(
                    tag, name, "error",
                    f"outputs of several kinds ({', '.join(sorted(kinds))})"
                    f": the action needs an explicit `kind`")
                return []
            if not staging and not ctx.accepts(action.kind):
                return []
            if all(ctx.keeps(o) is False for o in declared):
                return []
        else:
            # no output (`Fail`): nothing can hide it
            name = getattr(action, "name", "") or _name(action)
        if not action.ifexists_given:
            policy = IfExists.from_any(ctx.ifexists)
            if policy is IfExists.SKIP and action.claim is None and \
                    action.kind == "table":
                policy = IfExists.DIFFERENT
            action.ifexists = policy

        outcome, message, nb_sub, outputs = None, "", 0, []
        for item in action:
            if isinstance(item, Action):
                nb_sub += 1
                outputs += self.run_action(item, tag, staging)
            elif item.get("status") in OUTCOMES:
                outcome, message = item["status"], item.get("message", "")
            else:
                reporter.status(tag, name, item)

        own = action.outputs or []
        if action.claim is None:
            reporter.outcome(tag, name, outcome or "done", message)
        elif outcome == "error":
            reporter.outcome(tag, name, "error", message)
        elif not nb_sub:
            reporter.outcome(tag, name, "empty", "the claim yielded nothing")
        reporter.outputs(own)
        if action.claim is not None:
            action.outputs = outputs + own
        return outputs + own


# ----------------------------------------------------------------------
#   Entry point
# ----------------------------------------------------------------------

def run_roast(
    roaster: Roaster,
    ctx: RoastContext,
    *,
    jobs: int | None = 1,
    display: bool | Callable[[dict], Any] = True,
    info: dict | None = None,
    executor=None,
) -> RoastReport:
    """
    Run a roaster and the common steps that follow every roast.

    Parameters
    ----------
    roaster : callable(ctx) -> iterator of Action | roaster
    ctx : RoastContext
    jobs : int
        Number of processes for shards (1: sequential)
    display : bool | callable(row)
        Show the progress table (or send its rows to a callable)
    info : dict
        Extra fields of the run, in the manifest
    executor : object with `submit(task, resources=None) -> Future`
        Backend for shards (default: `LocalExecutor(jobs)` if
        `jobs > 1`)

    Returns
    -------
    RoastReport
    """
    from brainspresso.bids.harvest import finish_roast, trim_originals
    from brainspresso.utils.tabular import bidsify_tab

    report = RoastReport()

    def run(out):
        nonlocal report
        reporter = _MainReporter(out)
        reporter.root = Path(ctx.layout.root)
        report = reporter.report
        ctx._reporter = reporter
        try:
            _run_all(roaster, ctx, reporter, jobs or 1, executor)
        finally:
            ctx._reporter = None
            reporter.close(set(report.counts) | set(reporter.totals))

    status = "failed"
    try:
        if callable(display):
            run(display)
        elif display:
            with bidsify_tab() as out:
                run(out)
        else:
            run(None)
        status = "partial" if report.errors else "complete"
    finally:
        report.status = status
        try:
            trim_originals(ctx.layout)
        except Exception as e:
            lg.error(f"Could not trim tables: {e}\n{traceback.format_exc()}")
            report.status = status = "failed" if status == "failed" \
                else "partial"
        keys = ctx.selection.keys
        finish_roast(ctx.layout, **{
            **({"keys": sorted(keys)} if keys is not None else {}),
            "json": ctx.json,
            "status": status,
            "counts": report.summary(),
            **(info or {}),
        })
    return report


def _run_all(roaster, ctx, reporter: _MainReporter, jobs: int,
             executor=None) -> None:
    if executor is None and jobs > 1:
        if _picklable(ctx, warn=False):
            executor = LocalExecutor(jobs)
        else:
            try:
                pickle.dumps(ctx)
            except Exception as e:
                lg.warning(f"The roast context cannot be pickled ({e}): "
                           f"everything runs in the main process")
    engine = _Engine(ctx, reporter, run=uuid.uuid4().hex, executor=executor)
    try:
        engine.drive(roaster, ctx.tag)
        while True:
            engine.poll(block=True)
            if not engine.finalizers:
                break
            finalize, tag = engine.finalizers.pop()     # children first
            engine.drive(finalize, tag)
    finally:
        if engine.executor is not None:
            engine.executor.shutdown()
