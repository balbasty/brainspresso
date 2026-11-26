"""
Run a grind: one dataset, or several datasets as one task graph.

Interfaces only (see `brainspresso.grind`).

* `grind_dataset`: `bx <ds> grind <pipeline>`. A dataset with one
  dataset per site is ground site by site; group outputs go to the
  parent.
* `grind_collection`: `bx grind <pipeline> --datasets ... --where ...`.
  Tasks of each dataset keep their state in that dataset; group tasks
  and outputs live in `<out>/derivatives/<pipeline>/`.

Locks: grind never takes the run lock of harvest and roast
(`<state>/run`). The process that plans and submits, and every task
while it runs, hold `<state>/<GRIND_LOCK>` shared: the same mechanism
(`actions.runlock.run_lock(..., name=GRIND_LOCK)`, step 7) with run
records of its own (`<state>/runs-<GRIND_LOCK>/`), which the run lock
does not read. `bx <ds> grind` takes it through `Dataset.run`
(`LOCKS["grind"] = (GRIND_LOCK, "shared")`, over
`Dataset.lock_states`: parents before sites); `grind_collection` takes
it in every dataset, and a task in every dataset it reads
(`Task.locks`). Compost takes it exclusively when it deletes BIDS data
or derivatives. A running task also holds an exclusive lock on
`<state>/tasks/<id>.run`, so a unit never runs twice at once.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from brainspresso.actions.engine import Report
from brainspresso.bids.layout import Layout
from brainspresso.bids.select import Selection
from brainspresso.grind.execution import Executor, Profile
from brainspresso.grind.graph import Graph, IfStale
from brainspresso.grind.index import DatasetRef, Where
from brainspresso.grind.model import Pipeline
from brainspresso.grind.overrides import GrindOverrides

__all__ = ["GRIND_LOCK", "GrindReport", "grind_collection",
           "grind_dataset", "grind_status"]

#: Name of the grind lock file, in the state folder of a dataset
GRIND_LOCK = "grind"


@dataclass
class GrindReport:
    """
    What a grind did (or, with `dry_run` or `detach`, what it planned).

    Attributes
    ----------
    graph : Graph | None
        The planned graph
    status : {"complete", "partial", "failed", "submitted", "planned"}
        "partial": some tasks failed or were skipped; "submitted": a
        detached run; "planned": a dry run
    counts : dict[str, dict[str, int]]
        `{stage: {task status: n}}`
    failures : dict[str, str]
        `{task id: error}` of failed and skipped tasks
    jobs : dict[str, str]
        `{task id: job id}` of submitted tasks
    engine : Report | None
        Report of the action engine (attached runs)
    """

    graph: Graph | None = None
    status: str = "planned"
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)
    jobs: dict[str, str] = field(default_factory=dict)
    engine: Report | None = None


def grind_dataset(
    pipeline: Pipeline | str,
    layout: Layout,
    *,
    dataset: str | None = None,
    selection: Selection | None = None,
    overrides: GrindOverrides | None = None,
    config: Mapping[str, Any] | str | Path | None = None,
    profile: Profile | str | None = None,
    jobs: int | None = None,
    redo: Sequence[str] = (),
    if_stale: IfStale = "error",
    variant: str | None = None,
    variants: Mapping[str, str | None] | None = None,
    detach: bool | None = None,
    dry_run: bool = False,
    display: bool = True,
    executor: Executor | None = None,
) -> GrindReport:
    """
    Grind one dataset: the body of `bx <ds> grind <pipeline>`.

    Plans the graph, writes the description of the derivative and the
    pending task records, submits the tasks to the backend of the
    profile and, unless detached, waits and reports.

    Parameters
    ----------
    pipeline : Pipeline | str
        The pipeline, or its registered name
    layout : Layout
        Layout of the dataset (a sites layout: each site, with group
        outputs in the parent)
    dataset : str | None
        Reference of the dataset (task records, index)
    selection : Selection | None
        Subjects and sessions to grind (default: all)
    overrides : GrindOverrides | None
        What the dataset declares (`Dataset.grind`)
    config : Mapping[str, Any] | str | Path | None
        Configuration fields, or a file of them
    profile : Profile | str | None
        Execution profile, or its name (default: a local pool)
    jobs : int | None
        Overrides `Profile.jobs`
    redo : Sequence[str]
        Stages to run again, with what depends on them
    if_stale : {"error", "redo", "keep"}
        What to do with tasks done under another signature
    variant : str | None
        Write `derivatives/<pipeline>-<variant>/` instead
    variants : Mapping[str, str | None] | None
        Variant of each required pipeline (default: its unnamed one)
    detach : bool | None
        Overrides `Profile.detach`
    dry_run : bool
        Plan only: nothing is written, locked, pulled or submitted
    display : bool
        Show the progress table
    executor : Executor | None
        Backend to use instead of that of the profile

    Returns
    -------
    GrindReport
        What was done

    Raises
    ------
    DatasetNotFound
        If the dataset has no raw BIDS data
    RunLocked
        If a compost holds the grind lock
    """
    raise NotImplementedError


def grind_collection(
    pipeline: Pipeline | str,
    datasets: Sequence[str | DatasetRef],
    *,
    where: Sequence[str | Where] = (),
    root: str | Path | None = None,
    out: str | Path | None = None,
    selection: Selection | None = None,
    config: Mapping[str, Any] | str | Path | None = None,
    profile: Profile | str | None = None,
    jobs: int | None = None,
    redo: Sequence[str] = (),
    if_stale: IfStale = "error",
    variant: str | None = None,
    variants: Mapping[str, str | None] | None = None,
    detach: bool | None = None,
    dry_run: bool = False,
    display: bool = True,
    executor: Executor | None = None,
) -> GrindReport:
    """
    Grind several datasets as one task graph: the body of `bx grind`.

    Each dataset is resolved with its own overrides; the index and the
    `where` conditions select subjects; one graph is planned and
    submitted. Group outputs go to `<out>/derivatives/<pipeline>/`,
    whose `SourceDatasets` lists every dataset.

    Parameters
    ----------
    pipeline : Pipeline | str
        The pipeline, or its registered name
    datasets : Sequence[str | DatasetRef]
        Dataset references (`--datasets`)
    where : Sequence[str | Where]
        Conditions on the index (`--where`)
    root : str | Path | None
        Folder of all datasets
    out : str | Path | None
        Home of group outputs and of their state (default: `root`)
    selection : Selection | None
        Subjects and sessions, applied to every dataset
    config : Mapping[str, Any] | str | Path | None
        Configuration fields, or a file of them
    profile : Profile | str | None
        Execution profile, or its name
    jobs : int | None
        Overrides `Profile.jobs`
    redo : Sequence[str]
        Stages to run again, with what depends on them
    if_stale : {"error", "redo", "keep"}
        What to do with tasks done under another signature
    variant : str | None
        Write `derivatives/<pipeline>-<variant>/` instead
    variants : Mapping[str, str | None] | None
        Variant of each required pipeline (default: its unnamed one)
    detach : bool | None
        Overrides `Profile.detach`
    dry_run : bool
        Plan only
    display : bool
        Show the progress table
    executor : Executor | None
        Backend to use instead of that of the profile

    Returns
    -------
    GrindReport
        What was done
    """
    raise NotImplementedError


def grind_status(
    pipeline: Pipeline | str,
    layout: Layout,
    *,
    variant: str | None = None,
) -> GrindReport:
    """
    Report the state of a pipeline in a dataset, from its task records.

    The body of `--status`: nothing is planned, locked or submitted.

    Parameters
    ----------
    pipeline : Pipeline | str
        The pipeline, or its registered name
    layout : Layout
        Layout of the dataset
    variant : str | None
        Variant of the derivative

    Returns
    -------
    GrindReport
        Counts per stage and status, failures and job ids
    """
    raise NotImplementedError
