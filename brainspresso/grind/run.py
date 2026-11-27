"""
Run a grind: one dataset, or several datasets as one plan.

Interfaces only (see `brainspresso.grind`).

* `grind_dataset`: `bx <ds> grind <pipeline>`. A dataset with one
  dataset per site is ground site by site; group outputs go to the
  parent.
* `grind_collection`: `bx grind <pipeline> --datasets ... --where ...`.
  Tasks of each dataset keep their state in that dataset; group tasks
  and outputs live in `<out>/derivatives/<pipeline>/`.
* `planner`: what `bx percolate` calls to run one task of a grind plan
  (`percolate.run.PLANNERS`).

Both commands plan (`grind.graph.plan`), then: by default run the plan
from memory on the local pool, writing no plan file; with `plan_path`
(`--plan PATH`) write the plan and run nothing; when detached, or with
a scheduler backend, save the plan under `<state>/plans/` and hand it
to `percolate.run.percolate`, which waits or detaches
(`ExecutionOptions.detach`: wait locally, detach on schedulers).

Locks: grind never takes the run lock of harvest and roast
(`<state>/run`). The process that plans and runs, and every task while
it runs, hold `<state>/<GRIND_LOCK>` shared: the same mechanism
(`actions.runlock.run_lock(..., name=GRIND_LOCK)`, step 7) with run
records of its own (`<state>/runs-<GRIND_LOCK>/`), which the run lock
does not read. `bx <ds> grind` takes it through `Dataset.run`
(`LOCKS["grind"] = (GRIND_LOCK, "shared")`, over
`Dataset.lock_states`: parents before sites); `grind_collection` takes
it in every dataset, and a task in every dataset it reads
(`GrindPlanner.locks`). Compost takes it exclusively when it deletes
BIDS data or derivatives. A running task also holds an exclusive lock
on `<state>/tasks/<id>.run`, so a unit never runs twice at once.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from brainspresso.bids.layout import Layout
from brainspresso.bids.select import Selection
from brainspresso.grind.graph import IfStale
from brainspresso.grind.index import DatasetRef, Where
from brainspresso.grind.model import Pipeline
from brainspresso.grind.overrides import GrindOverrides
from brainspresso.percolate.options import ExecutionOptions, Profile
from brainspresso.percolate.plan import Plan, PlanTask
from brainspresso.percolate.run import PlanStatus

__all__ = ["GRIND_LOCK", "GrindPlanner", "GrindReport",
           "grind_collection", "grind_dataset", "grind_status", "planner"]

#: Name of the grind lock file, in the state folder of a dataset
GRIND_LOCK = "grind"


@dataclass
class GrindReport:
    """
    What a grind did, or planned.

    Attributes
    ----------
    plan : Plan | None
        The plan (its `path` is set when it was saved)
    status : {"complete", "partial", "failed", "submitted", "planned"}
        "partial": some tasks failed or were skipped; "submitted": a
        detached run (`bx percolate <plan> --status` follows it);
        "planned": `--plan` or a dry run
    counts : dict[str, dict[str, int]]
        `{stage: {task status: n}}`
    failures : dict[str, str]
        `{task id: error}` of failed and skipped tasks
    progress : PlanStatus | None
        State of the plan when the command returned
    """

    plan: Plan | None = None
    status: str = "planned"
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)
    progress: PlanStatus | None = None


class GrindPlanner:
    """
    The grind side of `bx percolate` (a `percolate.run.StagePlanner`).

    Attributes
    ----------
    stage : str
        "grind"
    """

    stage = "grind"

    def run(self, plan: Plan, task: PlanTask, profile: Profile) -> str:
        """
        Run one stage on one unit, from a plan.

        The pipeline is found by name (`get_pipeline`) and must have
        the version of the plan; the context is built from the plan
        (dataset, effective configuration, variants). The task takes
        its locks, resolves its inputs, checks its requirements from
        the upstream records and its signature against the plan, then
        runs `Stage.run` as one shard of the action engine.

        Parameters
        ----------
        plan : Plan
            The plan
        task : PlanTask
            The task
        profile : Profile
            Container runtime, scratch, `lock_wait`

        Returns
        -------
        str
            "done", "failed", "skipped" (requirements can no longer be
            met) or "waiting" (an upstream task is not over)

        Raises
        ------
        PlanError
            If the pipeline has another version, the image another
            registry digest than the plan records (a digest that the
            plan does not hold is written in the task record, never in
            the plan), or the task another signature than the plan
            says
        """
        raise NotImplementedError

    def locks(self, plan: Plan, task: PlanTask) -> list[tuple[Path, str]]:
        """
        Return the grind locks that a task holds shared while it runs.

        Parameters
        ----------
        plan : Plan
            The plan
        task : PlanTask
            The task

        Returns
        -------
        list[tuple[Path, str]]
            `(state folder, GRIND_LOCK)` of every dataset whose data or
            derivatives it reads, parents before sites: its dataset
            (and sites parent); for a group task the home and each site
            or dataset of the plan
        """
        raise NotImplementedError


#: The planner that `percolate.run.PLANNERS` names for grind
planner = GrindPlanner()


def grind_dataset(
    pipeline: Pipeline | str,
    layout: Layout,
    *,
    dataset: str | None = None,
    selection: Selection | None = None,
    overrides: GrindOverrides | None = None,
    config: Mapping[str, Any] | None = None,
    execution: ExecutionOptions | None = None,
    redo: Sequence[str] = (),
    if_stale: IfStale = "error",
    variant: str | None = None,
    variants: Mapping[str, str | None] | None = None,
    plan_path: str | Path | None = None,
    dry_run: bool = False,
    display: bool = True,
) -> GrindReport:
    """
    Grind one dataset: the body of `bx <ds> grind <pipeline>`.

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
    config : Mapping[str, Any] | None
        Configuration fields of the pipeline (generated options), over
        those of the config file (separate PR, which owns `--config`)
    execution : ExecutionOptions | None
        Backend, profile, limits, resources, `--wait`/`--detach`
        (default: the local pool, waiting)
    redo : Sequence[str]
        Stages to run again, with what depends on them
    if_stale : {"error", "redo", "keep"}
        What to do with tasks done under another signature
    variant : str | None
        Write `derivatives/<pipeline>-<variant>/` instead
    variants : Mapping[str, str | None] | None
        Variant of each required pipeline (default: its unnamed one)
    plan_path : str | Path | None
        `--plan PATH`: write the plan there and run nothing
    dry_run : bool
        Print the plan: nothing is written, locked, pulled or run
    display : bool
        Show the progress table

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
    config: Mapping[str, Any] | None = None,
    execution: ExecutionOptions | None = None,
    redo: Sequence[str] = (),
    if_stale: IfStale = "error",
    variant: str | None = None,
    variants: Mapping[str, str | None] | None = None,
    plan_path: str | Path | None = None,
    dry_run: bool = False,
    display: bool = True,
) -> GrindReport:
    """
    Grind several datasets as one plan: the body of `bx grind`.

    Each dataset is resolved with its own overrides; the index and the
    `where` conditions select subjects; one plan is made. Group outputs
    go to `<out>/derivatives/<pipeline>/`, whose `SourceDatasets` lists
    every dataset; a saved plan goes to `<out>/.brainspresso/plans/`.

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
    config : Mapping[str, Any] | None
        Configuration fields of the pipeline (generated options), over
        those of the config file (separate PR, which owns `--config`)
    execution : ExecutionOptions | None
        Backend, profile, limits, resources, `--wait`/`--detach`
        (default: the local pool, waiting)
    redo : Sequence[str]
        Stages to run again, with what depends on them
    if_stale : {"error", "redo", "keep"}
        What to do with tasks done under another signature
    variant : str | None
        Write `derivatives/<pipeline>-<variant>/` instead
    variants : Mapping[str, str | None] | None
        Variant of each required pipeline (default: its unnamed one)
    plan_path : str | Path | None
        `--plan PATH`: write the plan there and run nothing
    dry_run : bool
        Print the plan only
    display : bool
        Show the progress table

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

    Whatever plan ran them (`bx percolate PLAN --status` reports one
    plan). Nothing is planned, locked or submitted.

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
        Counts per stage and status, and failures
    """
    raise NotImplementedError
