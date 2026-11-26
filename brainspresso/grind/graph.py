"""
Plan a grind: units, tasks, signatures and the task graph.

Interfaces only (see `brainspresso.grind`).

One **task** per (stage, unit). Its id is
`grind/<pipeline>/<stage>/<unit label>` and its state file
`<state>/tasks/<id>.json` (`actions.state`), in the state folder of the
dataset of the unit (group units: that of the home of the run). The
record is the done marker; grind adds the `GRIND_FIELDS` to the schema
of `actions.state`. The graph is static: it is planned before anything
is submitted, and each task checks its requirements again when it
starts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Mapping, Sequence

from brainspresso.bids.layout import Layout
from brainspresso.bids.select import Selection
from brainspresso.grind.model import (
    Config,
    Pipeline,
    Resources,
    Stage,
    Unit,
)
from brainspresso.grind.overrides import GrindOverrides

__all__ = [
    "GRIND_FIELDS", "VERDICTS", "DatasetScope", "Graph", "IfStale",
    "Scope", "Task", "Verdict", "enumerate_units", "plan", "signature",
    "task_id",
]

#: Fields that grind adds to task records (`actions.state`):
#: the stage of the pipeline, the dataset reference, the upstream tasks
#: with their signatures, the pipeline version, the image, the job
GRIND_FIELDS = ("pipeline_stage", "dataset", "requires", "version",
                "container", "job")
#: What the plan says of a task
Verdict = Literal["todo", "done", "queued", "stale", "blocked"]
#: The verdicts: "todo" (no record, failed or skipped before), "done"
#: (same signature, outputs exist), "queued" (pending or running in a
#: live job), "stale" (done with another signature), "blocked" (a
#: requirement cannot be met)
VERDICTS = ("todo", "done", "queued", "stale", "blocked")
#: What to do with stale tasks: stop, recompute, or leave them (their
#: dependants are then blocked)
IfStale = Literal["error", "redo", "keep"]


@dataclass(frozen=True)
class DatasetScope:
    """
    One dataset of a run.

    Attributes
    ----------
    ref : str
        Its reference (`grind.index.DatasetRef`), the `dataset` of its
        units
    layout : Layout
        Its layout (never a sites layout: one entry per site)
    selection : Selection
        Subjects and sessions to grind
    overrides : GrindOverrides
        What the dataset declares
    """

    ref: str
    layout: Layout
    selection: Selection = field(default_factory=Selection)
    overrides: GrindOverrides = GrindOverrides()


@dataclass(frozen=True)
class Scope:
    """
    The datasets of a run, and where its group outputs go.

    Attributes
    ----------
    datasets : tuple[DatasetScope, ...]
        The datasets (one; each site of a sites layout; those of
        `--datasets`)
    home : Layout
        Layout that holds group outputs (`home.derivative(<pipeline>)`)
        and the state of group tasks: the dataset itself, the parent of
        a sites layout, or the collection of a cross-dataset run
    variant : str | None
        `--variant`: outputs go to `derivatives/<pipeline>-<variant>/`
    """

    datasets: tuple[DatasetScope, ...]
    home: Layout
    variant: str | None = None


@dataclass(frozen=True)
class Task:
    """
    One (stage, unit) of the graph.

    Attributes
    ----------
    id : str
        Task id (`task_id`)
    pipeline : str
        Name of the pipeline
    stage : str
        Name of the stage
    unit : Unit
        The unit
    state : Path
        State folder that holds its record
    root : Path
        Derivative dataset it writes into
    after : tuple[str, ...]
        Ids of tasks that must succeed first
    after_any : tuple[str, ...]
        Ids of tasks that must be over first (fan-in on what succeeded)
    resources : Resources
        Resources of the task
    signature : str | None
        Its signature, when it can be known at plan time (no upstream
        task still to run)
    verdict : {"todo", "done", "queued", "stale", "blocked"}
        What the plan says of it
    reason : str
        Why it is blocked or stale
    """

    id: str
    pipeline: str
    stage: str
    unit: Unit
    state: Path
    root: Path
    after: tuple[str, ...] = ()
    after_any: tuple[str, ...] = ()
    resources: Resources = Resources()
    signature: str | None = None
    verdict: Verdict = "todo"
    reason: str = ""


@dataclass(frozen=True)
class Graph:
    """
    The tasks of a run, each after those it depends on.

    Attributes
    ----------
    tasks : tuple[Task, ...]
        Every task, whatever its verdict, in dependency order
    """

    tasks: tuple[Task, ...] = ()

    def task(self, id: str) -> Task:
        """
        Return a task by id.

        Parameters
        ----------
        id : str
            Task id

        Returns
        -------
        Task
            The task

        Raises
        ------
        KeyError
            If the graph has no such task
        """
        raise NotImplementedError

    def todo(self) -> tuple[Task, ...]:
        """
        Return the tasks to submit.

        Returns
        -------
        tuple[Task, ...]
            Tasks whose verdict is "todo", in dependency order
        """
        raise NotImplementedError

    def waves(self) -> list[list[Task]]:
        """
        Group the tasks to submit by depth in the graph.

        Returns
        -------
        list[list[Task]]
            Wave `n` only depends on earlier waves
        """
        raise NotImplementedError

    def arrays(self) -> list[list[Task]]:
        """
        Group the tasks to submit into job arrays.

        Returns
        -------
        list[list[Task]]
            Tasks of one stage with equal resources and no dependency
            on each other, in dependency order
        """
        raise NotImplementedError


def task_id(pipeline: str, stage: str, unit: Unit) -> str:
    """
    Return the id of a task.

    Parameters
    ----------
    pipeline : str
        Name of the pipeline
    stage : str
        Name of the stage
    unit : Unit
        The unit

    Returns
    -------
    str
        `grind/<pipeline>/<stage>/<unit.label>`, e.g.
        "grind/fs-long/cross/sub-01/ses-1"; unique in the state folder
        of the dataset of the unit
    """
    raise NotImplementedError


def enumerate_units(
    layout: Layout,
    level: str,
    selection: Selection | None = None,
    overrides: GrindOverrides | None = None,
    *,
    dataset: str | None = None,
) -> list[Unit]:
    """
    List the units of a level in one dataset.

    From the `sub-*/[ses-*]` folders of `layout.raw` (what is on disk,
    not the participants table), filtered by the selection and by the
    exclusions of the dataset. A dataset without sessions has one
    "session" unit per subject. "dataset" and "group" give one unit.

    Parameters
    ----------
    layout : Layout
        Layout of the dataset (not a sites layout)
    level : str
        A key of `grind.model.LEVELS`
    selection : Selection | None
        Subjects and sessions to keep
    overrides : GrindOverrides | None
        Exclusions of the dataset
    dataset : str | None
        Reference of the dataset, for `Unit.dataset`

    Returns
    -------
    list[Unit]
        Units, sorted by label
    """
    raise NotImplementedError


def signature(
    pipeline: Pipeline,
    stage: Stage,
    config: Config | None,
    *,
    inputs: Sequence[Path] = (),
    root: Path | None = None,
    upstream: Mapping[str, str | None] | None = None,
    container: str | None = None,
) -> str:
    """
    Compute the signature of a task (the `config_hash` of its record).

    SHA-256 of: the name and version of the pipeline, the name of the
    stage, `stage.fingerprint(config)`, the image digest, the raw
    inputs (path relative to `root`, size, modification time) and the
    signature of each upstream task. A change therefore propagates to
    everything downstream, and the member list of a fan-in is part of
    it.

    Parameters
    ----------
    pipeline : Pipeline
        The pipeline
    stage : Stage
        The stage
    config : Config | None
        Effective configuration
    inputs : Sequence[Path]
        Raw input files
    root : Path | None
        Dataset folder that input paths are made relative to
    upstream : Mapping[str, str | None] | None
        `{upstream task id: its signature}`
    container : str | None
        Image and digest

    Returns
    -------
    str
        Hexadecimal digest
    """
    raise NotImplementedError


def plan(
    pipeline: Pipeline,
    scope: Scope,
    configs: Mapping[str, Config | None] | None = None,
    *,
    redo: Sequence[str] = (),
    if_stale: IfStale = "error",
) -> Graph:
    """
    Build the task graph of a run, without running or writing anything.

    For each dataset: the effective configuration, the units of each
    stage, their raw inputs (`Requirement.min`/`max`/`pick` and the
    overrides of the dataset), dependencies from the levels of the
    stages (unit to unit, fan-in, broadcast), then the verdict of each
    task from its record. Tasks of required pipelines are part of the
    graph. Each dataset ends with a `finalize` task (descriptions,
    manifest, failure list) after all of its tasks.

    Parameters
    ----------
    pipeline : Pipeline
        The pipeline to run
    scope : Scope
        Datasets, and the home of group outputs
    configs : Mapping[str, Config | None] | None
        Configuration per pipeline name, before dataset overrides
        (default: the defaults of each model)
    redo : Sequence[str]
        Stages to run again, with everything that depends on them
        ("all": every stage)
    if_stale : {"error", "redo", "keep"}
        What to do with tasks done under another signature

    Returns
    -------
    Graph
        The graph

    Raises
    ------
    ValueError
        If the pipeline is inconsistent, or if tasks are stale under
        "error"
    """
    raise NotImplementedError
