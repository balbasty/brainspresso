"""
Engine: run the actions that producers yield, and dispatch their shards.

This module is stage-agnostic: roast (`brainspresso.roast`) and grind
build on it, each with its own context (a `Context`) and its own
acceptance policy (a `Policy`).

A **producer** is a plain callable ``producer(ctx) -> Iterator[Action |
Producer]``. It yields actions (`actions.Action`), or sub-producers
(**shards**: one per archive, per subject...). It knows nothing about
acceptance (selection, output policies), `IfExists`, counters or
display: the engine applies them to the outputs that each action
declares, through the policy it is given.

```python
def produce(ctx):
    yield WriteJSON({"Name": "x"}, ctx.root / "x.json")
    for label in ("01", "02"):
        yield Shard(produce_subject, label, id=f"run/sub-{label}")

report = run(produce, Context(root), policy=Policy(), jobs=4)
```

Rules
-----
* Each action is run (to completion) **before the producer is asked for
  the next one**: actions may close over open archive handles.
* An action runs if the policy accepts it (`Policy.accept`); the policy
  then sets its defaults (`Policy.prepare`: `IfExists`...). A policy
  that raises `Rejected` makes the action an error, which is counted
  but not run. The sub-actions that a claim action yields are accepted
  one by one (they inherit its `staging` flag); a claim that yields no
  action is counted as "empty". All outputs of an action that runs are
  written; `IfExists` applies to the set.
* An action that fails is reported and counted; the run goes on. A
  producer that raises is reported, and the actions it would have
  yielded next are lost (a generator cannot be resumed).
* After all actions and shards: the `finalize(ctx)` hook of every
  producer that has one (optional attribute; `Shard` forwards that of
  its function), children first, the top-level producer last, once per
  run (also that of a shard skipped as unchanged, see below). It sees
  the finished tree and may yield more actions and shards.

Shards, parallelism and state
-----------------------------
With `jobs > 1`, every shard runs in a local process pool ("spawn"
start method), through a thin `submit(task, resources=None)` protocol
(`LocalExecutor`): shards yielded by producers running in the main
process are submitted; shards yielded inside a worker are handed back
to the main process when that shard finishes, and submitted then. So
a shard runs **asynchronously**: a producer must not rely on the outputs
of the shards it yields (use `finalize`), and shards must not write the
same files as each other or as the top-level producer (files are
locked, `actions.file`, so they would wait for each other, but their
outputs would be in an arbitrary order). **Shards must be picklable**
(module-level functions, `functools.partial` of them, or `Shard`), as
must `ctx` and the policy; a shard that cannot be pickled runs in the
main process (or in its worker), and an unpicklable `ctx` or policy
makes the whole run happen in the main process, both with a warning. If
a worker dies (`BrokenProcessPool`), the shards that had started (their
state file says so, or a marker file that a worker of the local pool
writes when it starts a shard: the state folder may be missing or not
writable) are counted as errors, so that a shard that crashes its
worker is not run again; the others, as well as all later ones, run in
the main process. (Markers live in `<tmp>/bx-shards-<host>-<pid>-*`,
removed at the end of the run, or by a later run on the same host if
the process was killed.
Another executor, whose workers may run on other machines, gets no
markers: only state files tell which shards had started.) With
`jobs=1` (default), everything runs sequentially, in order. ("spawn"
re-imports the main module in each worker: a script that calls `run`
with `jobs > 1` needs an `if __name__ == "__main__":` guard; the `bx`
command does.)

When the context has a state folder (`ctx.state`), each shard with an
`id` (and every shard run in a worker) writes its state to
`<state>/tasks/<id>.json` as it goes (status, counts, progress, outputs,
start and end; `actions.state`); the main process reads progress from
these while shards run. A shard that declares its `inputs` (source
files) and finished without errors is skipped by a later run under
`IfExists` "skip" if its inputs (paths, sizes, mtimes), its `config`
(JSON-able options of what it writes) and the policy's `fingerprint`
are unchanged and its outputs exist: its inputs are not
reopened (its `finalize` still runs). A shard that yielded shards is
never skipped this way.
"""
from __future__ import annotations

import copy
import hashlib
import json
import multiprocessing
import os
import pickle
import re
import shutil
import socket
import tempfile
import time
import traceback
import uuid
from collections import Counter, defaultdict
from concurrent.futures import (
    FIRST_COMPLETED,
    CancelledError,
    Future,
    ProcessPoolExecutor,
    wait,
)
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Union

from brainspresso.actions.action import Action, IfExists
from brainspresso.actions.runlock import held_states, hold_shared, pid_alive
from brainspresso.actions.state import (
    TASKS_DIR,
    now,
    read_task,
    task_path,
    write_task_state,
)
from brainspresso.utils.tabular import bidsify_tab

lg = getLogger(__name__)

__all__ = [
    "Context", "Engine", "LocalExecutor", "OUTCOMES", "Policy",
    "Producer", "Rejected", "Report", "Shard", "run",
]

Producer = Callable[["Context"], Union[Iterator[Union[Action, "Producer"]],
                                       None]]
#: Final statuses of an action
OUTCOMES = ("done", "skipped", "error", "empty")
#: Seconds between reads of the state files of running shards
POLL_INTERVAL = 0.2
#: Seconds between writes of a task state on outcomes (progress: always)
WRITE_INTERVAL = 0.5
_SIGNATURE_VERSION = 1


# ----------------------------------------------------------------------
#   Context and policy
# ----------------------------------------------------------------------

class Context:
    """
    What the engine needs to know about a run.

    Subclasses add what their producers need (e.g.
    `roast.RoastContext`, a dataclass that sets these attributes).

    Parameters
    ----------
    root : str | PathLike
        Folder that outputs are recorded relative to (task states)
    state : str | PathLike | None
        State folder (`<state>/tasks/` holds the task states); None: no
        task state is written, and shards are never skipped
    ifexists : {"skip", "overwrite", "different", "refresh", "error"}
        Default `IfExists` of actions that do not set their own
    tag : str
        Progress group of the producer that is running (default of
        `progress` and of the actions it yields)
    source : str | PathLike | None
        Folder of the source data of the running producer (a producer or
        `Shard` with a `source` attribute sets it while it runs)
    stage : str
        Name of the stage (task states, ids of anonymous tasks)
    """

    #: Name of the stage (task states, ids of anonymous tasks)
    stage: str = "run"
    _reporter: "_Reporter | None" = None

    def __init__(
        self,
        root: str | os.PathLike,
        *,
        state: str | os.PathLike | None = None,
        ifexists: IfExists.Choice = "skip",
        tag: str = "run",
        source: str | os.PathLike | None = None,
        stage: str | None = None,
    ):
        self.root = Path(root)
        self.state = Path(state) if state is not None else None
        self.ifexists = ifexists
        self.tag = tag
        self.source = Path(source) if source is not None else None
        if stage is not None:
            self.stage = stage
        self._reporter = None

    def __getstate__(self) -> dict:
        """Pickle without the reporter (local to a process)."""
        state = dict(self.__dict__)
        state["_reporter"] = None
        return state

    def progress(
        self,
        total: int | None = None,
        *,
        advance: int = 0,
        tag: str | None = None,
    ) -> None:
        """
        Declare the size of a progress group, or that units are done.

        Units are what the producer says they are (subjects,
        archives...).

        Parameters
        ----------
        total : int | None
            Number of units of the group
        advance : int
            Number of units just finished
        tag : str | None
            Progress group (default: `ctx.tag`)
        """
        if self._reporter is not None:
            self._reporter.progress(tag or self.tag, total, advance)


class Rejected(Exception):
    """Raised by `Policy.accept`: the action is an error (not run)."""


class Policy:
    """
    Acceptance policy: which actions run, and with what defaults.

    The base policy accepts every action, and gives those without an
    explicit `IfExists` the default of the context. A layer subclasses it
    (e.g. `roast.RoastPolicy`: selection, JSON policy, category
    folders, kinds). A policy is pickled to worker processes with the
    context: it should hold no open resource.
    """

    def accept(self, action: Action, ctx: Context, *,
               staging: bool = False) -> bool:
        """
        Decide whether an action runs.

        Parameters
        ----------
        action : Action
            The action (with `dst` or a `claim`, or neither: `Fail`)
        ctx : Context
            Context of the run
        staging : bool
            Whether the action is staging (`action.staging`, or a
            sub-action of a staging claim)

        Returns
        -------
        bool
            True: run it; False: drop it silently

        Raises
        ------
        Rejected
            To count the action as an error without running it
        """
        return True

    def prepare(self, action: Action, ctx: Context) -> None:
        """
        Set the defaults of an accepted action, before it runs.

        Parameters
        ----------
        action : Action
            The action (accepted)
        ctx : Context
            Context of the run
        """
        if not action.ifexists_given:
            action.ifexists = IfExists.from_any(ctx.ifexists)

    def fingerprint(self, ctx: Context) -> dict:
        """
        Describe what else decides the outputs of a shard.

        It is part of the input signature of shards (skip on unchanged
        inputs), with the inputs themselves.

        Parameters
        ----------
        ctx : Context
            Context of the run

        Returns
        -------
        dict
            JSON-serializable values (e.g. a selection)
        """
        return {}


class Shard:
    """
    A picklable sub-producer: `fn(ctx, *args, **kwargs)`.

    Parameters
    ----------
    fn : callable(ctx, *args, **kwargs) -> iterator
        The producer
    *args
        Positional arguments of `fn`, after `ctx`
    tag : str
        Default progress group of its actions
    id : str
        Task id: its state is written to `<state>/tasks/<id>.json`
        (e.g. "roast/subjects/sub-01"); unique in the dataset
    inputs : list[path]
        Source files it reads (archives): with an `id`, an unchanged
        signature lets a later run under "skip" skip it
    advance : str
        Progress group to advance by one when it is done
    unit : dict
        Entities of the unit (task state), e.g. `{"sub": "01"}`
    source : path
        `ctx.source` while it runs (default: that of its parent)
    config : JSON-able, optional
        What else decides its outputs (e.g. the options of each file it
        writes): part of its input signature, with `inputs`
    **kwargs
        Keyword arguments of `fn` (none of the names above)
    """

    def __init__(self, fn, *args, tag=None, id=None, inputs=None,
                 advance=None, unit=None, source=None, config=None,
                 **kwargs):
        self.fn, self.args, self.kwargs = fn, args, kwargs
        self.tag, self.id, self.advance = tag, id, advance
        self.inputs = list(inputs) if inputs is not None else None
        self.unit = dict(unit or {})
        self.source = source
        self.config = config

    def __call__(self, ctx):
        """Run the producer: `fn(ctx, *args, **kwargs)`."""
        return self.fn(ctx, *self.args, **self.kwargs)

    @property
    def finalize(self) -> "Shard | None":
        """`fn.finalize`, with the same arguments (None if no hook)."""
        fin = getattr(self.fn, "finalize", None)
        if fin is None:
            return None
        return Shard(fin, *self.args, tag=self.tag, source=self.source,
                     **self.kwargs)

    def __repr__(self):
        """Name of the function and arguments."""
        args = ", ".join(map(repr, self.args))
        return f"{getattr(self.fn, '__name__', self.fn)}({args})"


# ----------------------------------------------------------------------
#   Report
# ----------------------------------------------------------------------

@dataclass
class Report:
    """
    What a run did.

    Attributes
    ----------
    counts : dict[str, Counter]
        `{tag: {outcome: n}}`
    errors : list[tuple[str, str, str]]
        `(tag, name, message)` of every error
    outputs : list[Path]
        Outputs written or found already there
    progress : dict[str, tuple[int, int | None]]
        `{tag: (done, total)}`
    status : {"complete", "partial", "failed"}
        "partial" if anything failed, "failed" if the run itself stopped
    """

    counts: dict[str, Counter] = field(
        default_factory=lambda: defaultdict(Counter)
    )
    errors: list[tuple[str, str, str]] = field(default_factory=list)
    outputs: list[Path] = field(default_factory=list)
    progress: dict[str, tuple[int, int | None]] = field(default_factory=dict)
    status: str = "complete"

    def total(self, outcome: str) -> int:
        """
        Count an outcome over all tags.

        Parameters
        ----------
        outcome : {"done", "skipped", "error", "empty"}
            The outcome

        Returns
        -------
        int
            Number of actions with that outcome
        """
        return sum(c[outcome] for c in self.counts.values())

    def summary(self) -> dict:
        """Return the counts as `{tag: {outcome: n}}`, sorted by tag."""
        return {tag: dict(c) for tag, c in sorted(self.counts.items())}


class _Reporter:
    """
    Records what happens, and the state of the running task.

    Worker processes send these events back (intermediate statuses are
    not kept). `tasks`: states of the running tasks, innermost last.
    """

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
    """Counts, collects and displays events (main process)."""

    def __init__(self, out: Callable[[dict], Any] | None = None,
                 report: Report | None = None,
                 root: Path | None = None):
        super().__init__()
        self.out = out
        self.report = report if report is not None else Report()
        self.root = Path(root) if root is not None else Path(".")
        self.totals: dict[str, int] = {}
        self.finished: Counter = Counter()
        # progress of running shards, from their state files
        self.live: dict[str, dict[str, list]] = {}

    def _show(self, row: dict) -> None:
        if self.out is not None:
            self.out(row)

    def replay(self, events: Iterable[tuple]) -> None:
        """Replay the events of a shard run in a worker."""
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
        """Show the progress of a running shard (None: it is finished)."""
        old = self.live.pop(task, {})
        if progress:
            self.live[task] = progress
        for tag in set(old) | set(progress or {}):
            if (progress or {}).get(tag) != old.get(tag):
                self._show_progress(tag)

    def skipped_task(self, record: dict, tag: str, name: str) -> None:
        """Count a shard skipped as unchanged: what it did last time."""
        for group, counts in (record.get("counts") or {}).items():
            done = sum(n for outcome, n in counts.items()
                       if outcome in ("done", "skipped"))
            self.report.counts[group]["skipped"] += done
        for group, (total, advance) in (record.get("progress") or {}).items():
            self._progress(group, total, advance)
        self._outputs(self.root / p for p in record.get("outputs") or [])
        self._show({"modality": tag, "path": name, "status": "skipped",
                    "message": "unchanged since the last run"})

    def close(self, tags: Iterable[str]) -> None:
        """Show the final row of every progress group."""
        for tag in tags:
            row = {"modality": tag, "status": "done", "message": ""}
            if self.totals.get(tag):
                row["progress"] = 100
            self._show(row)


# ----------------------------------------------------------------------
#   Task state (<state>/tasks/<id>.json)
# ----------------------------------------------------------------------

class _TaskState:
    """State file of a shard, written as it runs (no-op without state)."""

    def __init__(self, ctx: Context, task: str, run: str, *,
                 signature: str | None = None, unit: dict | None = None):
        self.state, self.root, self.task = ctx.state, ctx.root, task
        self.record = {
            "stage": ctx.stage, "pipeline": getattr(ctx, "pipeline", None),
            "unit": dict(unit or {}),
            "status": "running", "config_hash": signature,
            "started": now(), "finished": None, "error": None,
            "outputs": [], "run": run, "counts": {}, "progress": {},
            "children": 0,
        }
        self.last = 0.0
        self.write()

    def write(self) -> None:
        if self.state is None:
            return
        self.last = time.monotonic()
        try:
            write_task_state(self.state, self.task, self.record)
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
        root = Path(self.root)
        for path in paths:
            try:
                path = Path(path).relative_to(root).as_posix()
            except ValueError:
                path = str(path)
            self.record["outputs"].append(path)

    def finish(self, status: str | None = None) -> None:
        failed = any(c.get("error") for c in self.record["counts"].values())
        self.record["status"] = status or ("failed" if failed else "done")
        self.record["finished"] = now()
        self.write()


def _signature(ctx: Context, policy: Policy, shard: Shard) -> str | None:
    """Input signature of a shard: inputs and policy fingerprint."""
    if not shard.inputs:
        return None
    inputs = []
    for path in shard.inputs:
        try:
            stat = os.stat(path)
            inputs.append([str(path), stat.st_size, stat.st_mtime_ns])
        except OSError:
            inputs.append([str(path), None, None])
    config = getattr(shard, "config", None)
    blob = json.dumps({
        "version": _SIGNATURE_VERSION, "inputs": inputs,
        **policy.fingerprint(ctx),
        **({"config": config} if config is not None else {}),
    }, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()


# ----------------------------------------------------------------------
#   Execution
# ----------------------------------------------------------------------

class LocalExecutor:
    """
    The local backend of the submit protocol.

    `submit(task, resources=None) -> Future` runs `task()` (a picklable
    callable) in a "spawn" process pool. `resources` (CPUs, memory...
    for the grind backends) are accepted and ignored.

    Workers take the shared run locks that the main process holds
    (`actions.runlock`), so that a worker that outlives it still
    protects the dataset from compost.

    Parameters
    ----------
    jobs : int
        Number of worker processes
    """

    def __init__(self, jobs: int):
        self.pool = ProcessPoolExecutor(
            jobs, mp_context=multiprocessing.get_context("spawn"),
            initializer=hold_shared, initargs=(held_states(),),
        )

    def submit(self, task: Callable[[], Any],
               resources: dict | None = None) -> Future:
        """
        Submit a task.

        Parameters
        ----------
        task : callable() -> Any
            Picklable callable
        resources : dict | None
            Resources it needs (ignored locally)

        Returns
        -------
        Future
            Its result
        """
        return self.pool.submit(task)

    def shutdown(self) -> None:
        """Wait for the running tasks and cancel the others."""
        self.pool.shutdown(wait=True, cancel_futures=True)


@dataclass
class _Pending:
    shard: Shard
    tag: str
    task: str
    auto: bool
    started: Path | None = None     # start marker (local pool)


@dataclass
class _ShardTask:
    """A shard run in a worker process."""

    shard: Shard
    ctx: Context
    policy: Policy
    tag: str
    task: str
    run: str
    signature: str | None = None
    #: File created when the shard starts (local pool): if the worker
    #: dies, the main process tells whether it had started, even
    #: without a (writable) state folder
    started: str | None = None

    def __call__(self) -> tuple[list, list, list]:
        if self.started is not None:
            try:
                Path(self.started).touch()
            except OSError as e:          # (loud without state files)
                log = lg.debug if _state_usable(self.ctx.state) \
                    else lg.warning
                log(f"Could not mark {self.task} as started: {e}")
        ctx = copy.copy(self.ctx)
        reporter = _Reporter()
        ctx._reporter = reporter
        engine = Engine(ctx, reporter, self.policy, run=self.run,
                        worker=True)
        reporter.tasks.append(_TaskState(
            ctx, self.task, self.run, signature=self.signature,
            unit=self.shard.unit,
        ))
        engine.run_shard(self.shard, self.tag)
        # finalize hooks go back to the main process if they can; those
        # run here run before the task state is popped, so that `_drive`
        # counts the shards they yield on it, as those of the shard
        finalizers = []
        for fin, tag in engine.finalizers:
            if _picklable(fin, warn=False):
                finalizers.append((fin, tag))
            else:
                engine.drive(fin, tag)
        state = reporter.tasks.pop()
        state.finish()
        return reporter.events, engine.deferred, finalizers


#: Prefix of the folders of start markers, in the temporary folder:
#: `bx-shards-<host>-<pid>-<random>`
MARKERS_PREFIX = "bx-shards-"


def _host() -> str:
    """Name of this host, as it is in the names of marker folders."""
    return re.sub(r"[^A-Za-z0-9]", "_", socket.gethostname()) or "_"


def _state_usable(state) -> bool:
    """Whether task states can be written (state folder writable)."""
    if state is None:
        return False
    folder = Path(state) / TASKS_DIR
    while not folder.exists() and folder.parent != folder:
        folder = folder.parent          # (created on the first write)
    return folder.is_dir() and os.access(folder, os.W_OK)


def _marker_folder(state=None) -> Path | None:
    """
    Create the folder of the start markers of a run.

    It is removed when the run ends (`Engine.close`). Those that a
    killed process of this host left behind (their pid is not alive)
    are removed first: only folders whose name is exactly
    `bx-shards-<this host>-<pid>-<random>` (the tmp folder may be shared
    by machines, or by containers with their own pids).

    Parameters
    ----------
    state : path | None
        State folder of the run: if it is usable, a failure is not
        worth a warning (state files tell which shards had started)

    Returns
    -------
    Path | None
        The folder (None if it cannot be created)
    """
    me, host = os.getpid(), _host()
    tmp = Path(tempfile.gettempdir())
    # (`mkdtemp` suffixes: lowercase letters, digits and "_")
    pattern = re.compile(rf"^{re.escape(MARKERS_PREFIX + host)}-(\d+)"
                         rf"-[a-z0-9_]+$")
    try:
        for folder in tmp.glob(f"{MARKERS_PREFIX}{host}-*"):
            match = pattern.match(folder.name)
            if match and int(match[1]) != me \
                    and not pid_alive(int(match[1])):
                shutil.rmtree(folder, ignore_errors=True)
        return Path(tempfile.mkdtemp(
            prefix=f"{MARKERS_PREFIX}{host}-{me}-"))
    except OSError as e:
        log = lg.debug if _state_usable(state) else lg.warning
        log(f"Could not create the start markers of shards in {tmp} "
            f"({e}): if a worker dies, only state files tell which "
            f"shards had started")
        return None


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


class Engine:
    """
    Drive producers, run actions, dispatch shards.

    Parameters
    ----------
    ctx : Context
        Context of the run
    reporter : _Reporter
        Where events go (`_MainReporter` in the main process)
    policy : Policy
        Acceptance policy
    run : str
        Id of the run (task states)
    executor : object with `submit(task, resources=None) -> Future`
        Backend for shards (None: run them here)
    worker : bool
        Whether this engine runs a shard in a worker process (shards it
        yields are handed back to the main process)
    """

    def __init__(self, ctx: Context, reporter: _Reporter,
                 policy: Policy | None = None, *, run: str,
                 executor=None, worker: bool = False):
        self.ctx, self.reporter = ctx, reporter
        self.policy = policy if policy is not None else Policy()
        self.run, self.executor, self.worker = run, executor, worker
        self.pending: dict[Future, _Pending] = {}
        self.deferred: list[tuple[Shard, str]] = []     # worker only
        self.finalizers: list[tuple[Producer, str]] = []
        self.count = 0
        self.mtimes: dict[str, int] = {}
        self.polled = 0.0
        # folder of the start markers of submitted shards (local pool)
        self.markers: Path | None = None
        self.markers_tried = False

    # ------------------------------------------------------------------
    #   Producers
    # ------------------------------------------------------------------
    def drive(self, producer, tag: str) -> None:
        """
        Consume a producer: run its actions, dispatch its shards.

        Parameters
        ----------
        producer : Producer
            The producer (its `tag`, `source` and `finalize`
            attributes, if any, are used)
        tag : str
            Its progress group, unless it has its own
        """
        ctx = self.ctx
        tag = getattr(producer, "tag", None) or tag
        source = getattr(producer, "source", None)
        finalize = getattr(producer, "finalize", None)
        if finalize is not None:
            self.finalizers.append((finalize, tag))
        outer_source = ctx.source
        if source is not None:
            # for the producer, its actions and the shards it yields
            ctx.source = Path(source)
        try:
            self._drive(producer, tag)
        finally:
            ctx.source = outer_source

    def _drive(self, producer, tag: str) -> None:
        ctx = self.ctx
        outer = ctx.tag

        def enter():
            # `ctx.tag`: that of the producer whose code runs
            ctx.tag = tag

        def leave():
            ctx.tag = outer

        try:
            enter()
            items = producer(ctx)
            items = iter(items) if items is not None else iter(())
        except Exception as e:
            self.producer_error(tag, producer, e)
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
                self.producer_error(tag, producer, e)
                break
            finally:
                leave()
            if isinstance(item, Action):
                self.run_action(item, tag)
            elif callable(item):
                # a child of the innermost task that runs here (not in
                # `dispatch`: children that a worker hands back were
                # counted on their parent, which does not run here)
                if self.reporter.tasks:
                    self.reporter.tasks[-1].record["children"] += 1
                self.dispatch(item, tag)
            else:
                self.reporter.outcome(
                    tag, _name(producer), "error",
                    f"{_name(producer)} yielded {type(item).__name__}, "
                    f"not an Action or a producer")
            if self.pending:
                self.poll()

    def producer_error(self, tag: str, producer, e: Exception) -> None:
        """
        Count an exception raised by a producer as an error.

        Parameters
        ----------
        tag : str
            Its progress group
        producer : Producer
            The producer
        e : Exception
            The exception
        """
        lg.error(f"{_name(producer)}: {e}\n{traceback.format_exc()}")
        self.reporter.outcome(tag, _name(producer), "error",
                              f"{type(e).__name__}: {e}")

    # ------------------------------------------------------------------
    #   Shards
    # ------------------------------------------------------------------
    def dispatch(self, item, tag: str) -> None:
        """
        Skip, submit, defer or run a shard yielded by a producer.

        Parameters
        ----------
        item : Producer
            The shard
        tag : str
            Progress group of the producer that yielded it
        """
        tag = getattr(item, "tag", None) or tag
        if self.worker:
            if _picklable(item, warn=False):
                self.deferred.append((item, tag))       # to the main process
                return
        elif self.skip(item, tag):
            # its `finalize` runs all the same (once per run)
            finalize = getattr(item, "finalize", None)
            if finalize is not None:
                self.finalizers.append((finalize, tag))
            return
        elif self.executor is not None and _picklable(item):
            self.submit(item, tag)
            return
        self.run_shard(item, tag)

    def run_shard(self, shard, tag: str) -> None:
        """
        Run a shard here (with its state file if it has an id).

        Parameters
        ----------
        shard : Producer
            The shard
        tag : str
            Its progress group
        """
        task_id = getattr(shard, "id", None)
        if task_id and not self.worker:
            self.reporter.tasks.append(_TaskState(
                self.ctx, task_id, self.run,
                signature=_signature(self.ctx, self.policy, shard),
                unit=shard.unit,
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
        """
        Skip a shard whose inputs did not change (under "skip").

        Parameters
        ----------
        shard : Producer
            The shard (only a `Shard` with `id` and `inputs` can be
            skipped)
        tag : str
            Its progress group

        Returns
        -------
        bool
            Whether it was skipped
        """
        if not isinstance(shard, Shard) or not shard.id or not shard.inputs:
            return False
        if self.ctx.state is None:
            return False
        if IfExists.from_any(self.ctx.ifexists) is not IfExists.SKIP:
            return False
        record = read_task(self.ctx.state, shard.id)
        if not record or record.get("status") != "done" \
                or record.get("children"):
            return False
        if record.get("config_hash") != _signature(self.ctx, self.policy,
                                                   shard):
            return False
        root = Path(self.ctx.root)
        if not all((root / p).exists() for p in record.get("outputs", [])):
            return False
        self.reporter.skipped_task(record, tag, _name(shard))
        return True

    def submit(self, shard, tag: str) -> None:
        """
        Submit a shard to the executor.

        Parameters
        ----------
        shard : Producer
            The shard (picklable)
        tag : str
            Its progress group
        """
        if not isinstance(shard, Shard):
            shard = Shard(shard, tag=tag,
                          source=getattr(shard, "source", None))
        self.count += 1
        task_id = shard.id or \
            f"{self.ctx.stage}/.auto/{self.run}-{self.count}"
        started = None
        if isinstance(self.executor, LocalExecutor):
            # whether it started, if its worker dies (its state file
            # may be missing: no state folder, or one that cannot be
            # written); local workers only see this machine's tmp
            if not self.markers_tried:
                self.markers_tried = True
                self.markers = _marker_folder(self.ctx.state)
            if self.markers is not None:
                started = self.markers / f"{self.count}.started"
        task = _ShardTask(
            shard, self.ctx, self.policy, tag, task_id, self.run,
            _signature(self.ctx, self.policy, shard) if shard.id else None,
            str(started) if started is not None else None,
        )
        try:
            future = self.executor.submit(task)
        except (BrokenProcessPool, RuntimeError) as e:
            # the pool broke (a worker died) before we collected it
            self.stop_pool(e)
            self.run_shard(shard, tag)
            return
        self.pending[future] = _Pending(shard, tag, task_id, not shard.id,
                                        started)

    def poll(self, block: bool = False) -> None:
        """
        Collect the shards that finished, and the progress of the others.

        Parameters
        ----------
        block : bool
            Wait until all shards are collected
        """
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
        """Read the progress of running shards from their state files."""
        if self.ctx.state is None:
            return
        if time.monotonic() - self.polled < POLL_INTERVAL:
            return
        self.polled = time.monotonic()
        for pending in list(self.pending.values()):
            path = task_path(self.ctx.state, pending.task)
            try:
                mtime = path.stat().st_mtime_ns
            except OSError:
                continue
            if self.mtimes.get(pending.task) == mtime:
                continue
            self.mtimes[pending.task] = mtime
            record = read_task(self.ctx.state, pending.task)
            if record and record.get("run") == self.run:
                self.reporter.live_progress(pending.task,
                                            record.get("progress"))

    def collect(self, future: Future) -> None:
        """
        Collect a finished shard: its events, shards and finalizers.

        Parameters
        ----------
        future : Future
            Its future
        """
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
        if pending.auto and self.ctx.state is not None:
            try:
                task_path(self.ctx.state, pending.task).unlink(
                    missing_ok=True)
            except OSError as e:          # (a state folder in a bad way)
                lg.debug(f"Could not remove the state of "
                         f"{pending.task}: {e}")

    def broken(self, pending: _Pending) -> None:
        """
        Handle a worker that died.

        The shards that had started (their state file, or their start
        marker, says so) are errors, and are not
        run again: one of them crashed the worker, and would crash the
        main process. The others (and all later ones) run in the main
        process.

        Parameters
        ----------
        pending : _Pending
            A shard that was submitted to the broken pool
        """
        self.stop_pool()
        record = None
        if self.ctx.state is not None:
            record = read_task(self.ctx.state, pending.task)
            if not record or record.get("run") != self.run:
                record = None                   # (or from another run)
        started = record is not None or (
            pending.started is not None and pending.started.exists())
        if not started:
            self.run_shard(pending.shard, pending.tag)
            return
        self.reporter.outcome(
            pending.tag, _name(pending.shard), "error",
            "the worker process died while this shard ran (it, or "
            "a shard running at the same time, crashed it)")
        if record is not None:
            record["status"], record["error"] = "failed", "worker died"
            try:
                write_task_state(self.ctx.state, pending.task, record)
            except OSError as e:      # the error is counted all the same
                lg.warning(f"Could not write the state of "
                           f"{pending.task}: {e}")

    def stop_pool(self, error: Exception | None = None) -> None:
        """
        Stop using the executor: later shards run in this process.

        Parameters
        ----------
        error : Exception | None
            Why (for the log)
        """
        if self.executor is None:
            return
        lg.error(f"A worker process died{f' ({error})' if error else ''}"
                 f": the remaining shards run in the main process")
        executor, self.executor = self.executor, None
        try:
            executor.shutdown()
        except Exception as e:      # already broken
            lg.debug(f"shutdown: {e}")

    def close(self) -> None:
        """Shut the executor down and remove the start markers."""
        try:
            if self.executor is not None:
                self.executor.shutdown()
        finally:
            if self.markers is not None:
                shutil.rmtree(self.markers, ignore_errors=True)
                self.markers = None

    # ------------------------------------------------------------------
    #   Actions
    # ------------------------------------------------------------------
    def run_action(self, action: Action, tag: str,
                   staging: bool = False) -> list[Path]:
        """
        Accept and run one action (and the sub-actions it yields).

        Parameters
        ----------
        action : Action
            The action
        tag : str
            Progress group of the producer (unless the action has one)
        staging : bool
            Whether it is a sub-action of a staging claim

        Returns
        -------
        list[Path]
            Its outputs (written or already there)
        """
        ctx, reporter = self.ctx, self.reporter
        tag = action.tag or tag
        action.tag = tag
        staging = staging or action.staging
        declared = action.declared
        if action.claim is not None:
            paths = action.claim.paths
            name = paths[0].name or str(paths[0])
        elif declared:
            name = getattr(action, "name", "") or declared[0].name
        else:
            name = getattr(action, "name", "") or _name(action)
        try:
            if not self.policy.accept(action, ctx, staging=staging):
                return []
        except Rejected as e:
            reporter.outcome(tag, name, "error", str(e))
            return []
        self.policy.prepare(action, ctx)

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

def run(
    producer: Producer,
    ctx: Context,
    *,
    policy: Policy | None = None,
    jobs: int | None = 1,
    display: bool | Callable[[dict], Any] = True,
    executor=None,
    report: Report | None = None,
) -> Report:
    """
    Run a producer: its actions, shards and `finalize` hooks.

    Parameters
    ----------
    producer : callable(ctx) -> iterator of Action | Producer
        The top-level producer
    ctx : Context
        Context of the run
    policy : Policy | None
        Acceptance policy (default: `Policy()`, which accepts all)
    jobs : int
        Number of processes for shards (1: sequential)
    display : bool | callable(row)
        Show the progress table (or send its rows to a callable)
    executor : object with `submit(task, resources=None) -> Future`
        Backend for shards (default: `LocalExecutor(jobs)` if
        `jobs > 1`)
    report : Report | None
        Report to fill (default: a new one), so that a caller still has
        it if the run raises (its `status` is then "failed")

    Returns
    -------
    Report
        What the run did
    """
    report = report if report is not None else Report()
    policy = policy if policy is not None else Policy()

    def go(out):
        reporter = _MainReporter(out, report, root=ctx.root)
        ctx._reporter = reporter
        try:
            _run_all(producer, ctx, policy, reporter, jobs or 1, executor)
        finally:
            ctx._reporter = None
            reporter.close(set(report.counts) | set(reporter.totals))

    status = "failed"
    try:
        if callable(display):
            go(display)
        elif display:
            with bidsify_tab() as out:
                go(out)
        else:
            go(None)
        status = "partial" if report.errors else "complete"
    finally:
        report.status = status
    return report


def _run_all(producer, ctx: Context, policy: Policy,
             reporter: _MainReporter, jobs: int, executor=None) -> None:
    if executor is None and jobs > 1:
        if _picklable((ctx, policy), warn=False):
            executor = LocalExecutor(jobs)
        else:
            try:
                pickle.dumps((ctx, policy))
            except Exception as e:
                lg.warning(f"The context or the policy cannot be pickled "
                           f"({e}): everything runs in the main process")
    engine = Engine(ctx, reporter, policy, run=uuid.uuid4().hex,
                    executor=executor)
    try:
        engine.drive(producer, ctx.tag)
        while True:
            engine.poll(block=True)
            if not engine.finalizers:
                break
            finalize, tag = engine.finalizers.pop()     # children first
            engine.drive(finalize, tag)
    finally:
        engine.close()
