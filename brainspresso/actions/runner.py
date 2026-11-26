"""
Roast runner: run the actions that roasters yield.

A **roaster** is a plain callable ``roaster(ctx) -> Iterator[Action |
Roaster]``. It reads source data (`utils.archive`) and yields actions
(`actions.Action`) that write the BIDS dataset, or sub-roasters
(**shards**: one per archive, per subject...). It knows nothing about
selection, `--json`, `IfExists`, counters or display: the runner applies
them to the outputs that each action declares.

```python
def roast_ixi(ctx):
    yield CopyJSON(ctx.resource("dataset_description.json"),
                   ctx.layout.raw / "dataset_description.json", tag="meta")
    for key in ("T1w", "T2w"):
        yield Shard(roast_anat, key, tag=key)     # picklable sub-roaster

run_roast(roast_ixi, ctx, jobs=4)
```

Rules
-----
* Each action is run (to completion) **before the roaster is asked for
  the next one**: actions may close over open archive handles.
* An action runs if any of its outputs may be selected
  (`ctx.keeps(path)`, i.e. `ctx.selection.keeps` on the path relative
  to the BIDS dataset that contains it, the subject/session filters only
  outside the raw dataset), and if its `kind` passes the JSON policy
  (`--json no` drops sidecars, `--json only` drops data). A claim
  action runs unless its claim cannot be selected
  (`keeps(claim, partial=True) is False`); the sub-actions it yields are
  selected one by one. All outputs of an action that runs are written;
  `IfExists` applies to the set.
* Outputs in `<state>/orig/` (pristine tables) are selected as the
  table they are the pristine copy of.
* An action that fails is reported and counted; the roast goes on. A
  roaster that raises is reported, and the actions it would have yielded
  next are lost (a generator cannot be resumed).
* After all actions: `roaster.finalize(ctx)` (optional attribute; it
  sees the finished tree and may yield more actions), then the common
  steps: visible tables trimmed from their pristine copies
  (`trim_originals`), layout descriptions and manifest (`finish_roast`).

Shards and parallelism
----------------------
With `jobs > 1`, the shards that the top-level roaster yields run in a
local process pool (`ProcessPoolExecutor`, "spawn" start method), while
the top-level roaster goes on in the main process. **Shards must be
picklable** (module-level functions, `functools.partial` of them, or
`Shard`), as must `ctx` (layout, selection, options...). A shard that
cannot be pickled runs in the main process, with a warning. Shards must
not write the same files as each other or as the top-level roaster.
Their statuses are reported when they finish. With `jobs=1` (default),
everything runs sequentially, in order.
"""
from __future__ import annotations

import copy
import multiprocessing
import os
import pickle
import traceback
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, Future
from concurrent.futures import ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Literal, Union

from brainspresso.actions.action import Action, IfExists, Kind
from brainspresso.actions.action import default_kind
from brainspresso.bids.keys import KeyAliases, KeySelection
from brainspresso.bids.layout import ORIG_DIR, Layout, resolve_layout

lg = getLogger(__name__)

Roaster = Callable[["RoastContext"], Union[Iterator[Union[Action, "Roaster"]],
                                          None]]
JsonPolicy = Literal["yes", "no", "only"]
OUTCOMES = ("done", "skipped", "error")


@dataclass
class Shard:
    """
    A picklable sub-roaster: `fn(ctx, *args, **kwargs)`, with a default
    progress tag for its actions.
    """
    fn: Callable
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    tag: str | None = None

    def __init__(self, fn, *args, tag=None, **kwargs):
        self.fn, self.args, self.kwargs, self.tag = fn, args, kwargs, tag

    def __call__(self, ctx):
        return self.fn(ctx, *self.args, **self.kwargs)

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
        Selection on the raw dataset (`keys`, subjects, sessions), and
        non-BIDS categories (`wants`)
    keys : KeyAliases | None
        Keys of the dataset
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
    """
    layout: Layout
    selection: KeySelection = field(default_factory=KeySelection)
    keys: KeyAliases | None = None
    json: JsonPolicy = "yes"
    ifexists: IfExists.Choice = "skip"
    options: dict = field(default_factory=dict)
    templates: Path | None = None
    tag: str = "roast"

    def __post_init__(self):
        self._reporter: _Reporter | None = None
        if isinstance(self.json, bool):
            self.json = "yes" if self.json else "no"
        self.json = str(self.json).lower()
        if self.json not in ("yes", "no", "only"):
            raise ValueError(f"json must be yes, no or only: {self.json}")

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_reporter"] = None
        return state

    # ------------------------------------------------------------------
    def wants(self, category: str) -> bool:
        """Whether a non-BIDS category (metadata, a pipeline...) is wanted"""
        return self.selection.wants(category)

    def resource(self, name: str | os.PathLike) -> Path | None:
        """A metadata file: from `sourcedata/` if there, else from the
        templates shipped with the dataset, else None"""
        for folder in (self.layout.source, self.templates):
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
        """JSON policy"""
        if self.json == "no":
            return kind != "sidecar"
        if self.json == "only":
            return kind != "data"
        return True

    def keeps(
        self, path: str | os.PathLike, partial: bool = False
    ) -> bool | None:
        """
        Whether an output (absolute path, or a claim with
        `partial=True`) is selected: `selection.keeps` on its path in
        the raw dataset, the subject/session filters elsewhere
        (derivatives, `sourcedata/`). Pristine tables (`<state>/orig/`)
        are judged as the table they are the copy of.
        """
        layout = self.layout
        path = Path(path)
        try:
            path = layout.root / path.relative_to(layout.state / ORIG_DIR)
        except ValueError:
            pass
        try:
            dataset, rel = layout.relative(path)
        except ValueError:
            return True             # not in the dataset: not ours to filter
        rel = "" if rel == "." else rel
        if Path(dataset) == Path(layout.raw):
            return self.selection.keeps(rel, partial)
        return self.selection.base.keeps(rel, partial)


def make_context(
    root: str | os.PathLike | Layout,
    keys: KeyAliases | None = None,
    *,
    layout: str | None = None,
    select_keys: Iterable[str] = (),
    exclude_keys: Iterable[str] = (),
    json: JsonPolicy | bool = "yes",
    ifexists: IfExists.Choice = "skip",
    templates: str | os.PathLike | None = None,
    options: dict | None = None,
    **filters,
) -> RoastContext:
    """
    Context of a roast from command-line values.

    Parameters
    ----------
    root : path | Layout
        Dataset folder (`<root>/<DS>`)
    keys : KeyAliases
        Keys of the dataset
    layout : {"study", "flat"}
        Layout of a new dataset (must match an existing one)
    select_keys, exclude_keys : list[str]
        `--keys`, `--exclude-keys`
    **filters
        `Selection` filters (`subs`, `exclude_subs`, `sessions`...)
    """
    keys = keys or KeyAliases(set())
    return RoastContext(
        layout=resolve_layout(root, layout),
        selection=keys.selection(select_keys, exclude_keys, **filters),
        keys=keys,
        json=json,
        ifexists=ifexists,
        options=dict(options or {}),
        templates=Path(templates) if templates is not None else None,
    )


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

    def total(self, outcome: str) -> int:
        return sum(c[outcome] for c in self.counts.values())

    def summary(self) -> dict:
        return {tag: dict(c) for tag, c in sorted(self.counts.items())}


class _Reporter:
    """Records what happens (worker processes send these events back;
    intermediate statuses are not kept)"""

    def __init__(self):
        self.events: list[tuple] = []

    def status(self, tag: str, path: str, status: dict) -> None:
        pass

    def outcome(self, tag, path, outcome, message="") -> None:
        self.events.append(("outcome", tag, path, outcome, message))

    def progress(self, tag, total=None, advance=0) -> None:
        self.events.append(("progress", tag, total, advance))

    def outputs(self, paths: list[Path]) -> None:
        self.events.append(("outputs", list(paths)))


class _MainReporter(_Reporter):
    """Counts, collects and displays events"""

    def __init__(self, out: Callable[[dict], Any] | None = None):
        super().__init__()
        self.out = out
        self.report = RoastReport()
        self.totals: dict[str, int] = {}
        self.finished: Counter = Counter()

    def _show(self, row: dict) -> None:
        if self.out is not None:
            self.out(row)

    def replay(self, events: Iterable[tuple]) -> None:
        for event in events:
            getattr(self, event[0])(*event[1:])

    def status(self, tag, path, status):
        self._show({"modality": tag, "path": path, **status})

    def outcome(self, tag, path, outcome, message=""):
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

    def progress(self, tag, total=None, advance=0):
        if total is not None:
            self.totals[tag] = total
        self.finished[tag] += advance
        self.report.progress[tag] = (self.finished[tag], self.totals.get(tag))
        if self.totals.get(tag):
            pct = min(100, 100 * self.finished[tag] / self.totals[tag])
            self._show({"modality": tag, "progress": pct})

    def outputs(self, paths):
        self.report.outputs.extend(paths)

    def close(self, tags: Iterable[str]) -> None:
        for tag in tags:
            row = {"modality": tag, "status": "done", "message": ""}
            if self.totals.get(tag):
                row["progress"] = 100
            self._show(row)


# ----------------------------------------------------------------------
#   Running
# ----------------------------------------------------------------------

def run_roast(
    roaster: Roaster,
    ctx: RoastContext,
    *,
    jobs: int | None = 1,
    display: bool = True,
    info: dict | None = None,
) -> RoastReport:
    """
    Run a roaster and the common steps that follow every roast.

    Parameters
    ----------
    roaster : callable(ctx) -> iterator of Action | roaster
    ctx : RoastContext
    jobs : int
        Number of processes for shards (1: sequential)
    display : bool
        Show the progress table
    info : dict
        Extra fields of the run, in the manifest

    Returns
    -------
    RoastReport
    """
    from brainspresso.bids.harvest import finish_roast, trim_originals
    from brainspresso.utils.tabular import bidsify_tab

    def run(out):
        reporter = _MainReporter(out)
        ctx._reporter = reporter
        try:
            _run_all(roaster, ctx, reporter, jobs or 1)
        finally:
            ctx._reporter = None
        reporter.close(set(reporter.report.counts) | set(reporter.totals))
        return reporter.report

    if display:
        with bidsify_tab() as out:
            report = run(out)
    else:
        report = run(None)

    trim_originals(ctx.layout)
    keys = ctx.selection.keys
    finish_roast(ctx.layout, **{
        **({"keys": sorted(keys)} if keys is not None else {}),
        "json": ctx.json,
        "counts": report.summary(),
        **(info or {}),
    })
    return report


def _run_all(roaster, ctx, reporter: _MainReporter, jobs: int) -> None:
    pool = futures = None
    if jobs > 1:
        pool = ProcessPoolExecutor(
            jobs, mp_context=multiprocessing.get_context("spawn")
        )
        futures = {}
    try:
        _drive(roaster, ctx, reporter, ctx.tag, pool, futures)
        if pool is not None:
            _collect(futures, reporter, wait_all=True)
        finalize = getattr(roaster, "finalize", None)
        if finalize is not None:
            _drive(finalize, ctx, reporter, getattr(roaster, "tag", None)
                   or ctx.tag)
    finally:
        if pool is not None:
            pool.shutdown(cancel_futures=True)


def _name(obj) -> str:
    return getattr(obj, "__name__", None) or repr(obj)


def _drive(roaster, ctx, reporter, tag, pool=None, futures=None) -> None:
    """Consume a roaster: run its actions, recurse into its shards"""
    tag = getattr(roaster, "tag", None) or tag
    outer = ctx.tag
    try:
        # `ctx.tag` is the tag of the roaster whose code is running
        ctx.tag = tag
        items = roaster(ctx)
        items = iter(items) if items is not None else iter(())
    except Exception as e:
        _roaster_error(reporter, tag, roaster, e)
        return
    finally:
        ctx.tag = outer
    while True:
        try:
            ctx.tag = tag
            item = next(items)
        except StopIteration:
            break
        except Exception as e:
            _roaster_error(reporter, tag, roaster, e)
            break
        finally:
            ctx.tag = outer
        if isinstance(item, Action):
            _run_action(item, ctx, reporter, tag)
        elif callable(item):
            shard_tag = getattr(item, "tag", None) or tag
            if pool is not None and _picklable(item):
                futures[pool.submit(_run_shard, item, ctx, shard_tag)] = (
                    shard_tag, item
                )
            else:
                _drive(item, ctx, reporter, shard_tag)
        else:
            reporter.outcome(tag, _name(roaster), "error",
                             f"roaster yielded {type(item).__name__}, "
                             f"not an Action or a roaster")
        if futures:
            _collect(futures, reporter)


def _roaster_error(reporter, tag, roaster, e) -> None:
    lg.error(f"{_name(roaster)}: {e}\n{traceback.format_exc()}")
    reporter.outcome(tag, _name(roaster), "error", f"{type(e).__name__}: {e}")


def _picklable(shard) -> bool:
    try:
        pickle.dumps(shard)
    except Exception as e:
        lg.warning(f"Shard {_name(shard)} is not picklable ({e}): "
                   f"run in the main process")
        return False
    return True


def _collect(futures: dict[Future, tuple], reporter, wait_all=False):
    """Report the shards that finished (all of them if `wait_all`)"""
    while futures:
        done, _ = wait(list(futures), timeout=None if wait_all else 0,
                       return_when=FIRST_COMPLETED)
        if not done:
            return
        for future in done:
            tag, shard = futures.pop(future)
            try:
                reporter.replay(future.result())
            except Exception as e:
                reporter.outcome(tag, _name(shard), "error",
                                 f"{type(e).__name__}: {e}")


def _run_shard(shard, ctx: RoastContext, tag: str) -> list[tuple]:
    """Run a shard in a worker process; return its events"""
    reporter = _Reporter()
    ctx = copy.copy(ctx)
    ctx._reporter = reporter
    _drive(shard, ctx, reporter, tag)
    return reporter.events


def _run_action(action: Action, ctx, reporter, tag) -> list[Path]:
    """Select and run one action (and the sub-actions it yields)"""
    tag = action.tag or tag
    action.tag = tag
    if action.claim is not None:
        if ctx.keeps(action.claim.path, partial=True) is False:
            return []
        name = str(action.claim.path.name) or str(action.claim.path)
    else:
        outputs = action.declared
        if not ctx.accepts(action.kind):
            return []
        if outputs and all(ctx.keeps(o) is False for o in outputs):
            return []
        name = getattr(action, "name", "") or (
            outputs[0].name if outputs else _name(action))
    if not action.ifexists_given:
        action.ifexists = IfExists.from_any(ctx.ifexists)
    action.keeps = lambda path: (
        ctx.keeps(path) is not False and ctx.accepts(default_kind(path))
    )

    outcome, message, nb_sub, outputs = None, "", 0, []
    for item in action:
        if isinstance(item, Action):
            nb_sub += 1
            outputs += _run_action(item, ctx, reporter, tag)
        elif item.get("status") in OUTCOMES:
            outcome, message = item["status"], item.get("message", "")
        else:
            reporter.status(tag, name, item)

    own = action.outputs or []
    if action.claim is None or outcome == "error" or (own and not nb_sub):
        # sub-actions of a claim are counted on their own
        reporter.outcome(tag, name, outcome or "done", message)
    reporter.outputs(own)
    if action.claim is not None:
        action.outputs = outputs + own
    return outputs + own
