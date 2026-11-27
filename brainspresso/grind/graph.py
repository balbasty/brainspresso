"""
Plan a grind: units, tasks, signatures and the task graph.

Interfaces only (see `brainspresso.grind`).

One **task** per (stage, unit, variant). Its id is
`grind/<pipeline>[@<variant>]/<stage>/<unit label>` and its state file
`<state>/tasks/<id>.json` (`actions.state`), in the state folder of the
dataset of the unit (group units: that of the home of the run). The
record is the done marker; grind adds the `GRIND_FIELDS` to the schema
of `actions.state`. The graph is static: `plan` returns a
`percolate.plan.Plan` before anything runs, and each task checks its
requirements and its signature again when it starts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from brainspresso.bids.layout import Layout
from brainspresso.bids.select import Selection
from brainspresso.grind.containers import ContainerRuntime
from brainspresso.grind.model import Config, Pipeline, Stage, Unit
from brainspresso.grind.overrides import GrindOverrides
from brainspresso.percolate.plan import Plan

__all__ = [
    "GRIND_FIELDS", "VERDICTS", "DatasetScope", "IfStale",
    "Scope", "Verdict", "derivative_name", "enumerate_units", "plan",
    "signature", "task_id",
]

#: Fields that grind adds to task records (`actions.state`):
#: the stage of the pipeline, the variant, the dataset reference, the
#: upstream tasks with their signatures, the pipeline version, the
#: image with its digest, the plan that ran it, the job (backend, id,
#: host, pid), whether it was staged and on what scratch (the option as
#: given, e.g. "$SLURM_TMPDIR": never a full path). They are passed to
#: the engine as the extra record fields of the shard, and merged into
#: the record
GRIND_FIELDS = ("pipeline_stage", "variant", "dataset", "requires",
                "version", "container", "plan", "job", "staged",
                "scratch")
#: What the plan says of a task
Verdict = Literal["todo", "done", "queued", "stale", "blocked"]
#: The verdicts: "todo" (no record, failed or skipped before), "done"
#: (same signature, outputs exist), "queued" (recorded pending or
#: running, by another plan or an earlier run of this one), "stale"
#: (done with another signature), "blocked" (a
#: requirement cannot be met)
VERDICTS = ("todo", "done", "queued", "stale", "blocked")
#: What to do with stale tasks: stop, recompute, or leave them (their
#: dependents are then blocked)
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
        and task ids to `grind/<pipeline>@<variant>/`
    variants : Mapping[str, str | None]
        Variant in which each required pipeline is read and, if need
        be, run (`--with <pipeline>=<variant>`); a pipeline that is not
        named uses its default variant, never `variant`
    """

    datasets: tuple[DatasetScope, ...]
    home: Layout
    variant: str | None = None
    variants: Mapping[str, str | None] = field(default_factory=dict)


def derivative_name(pipeline: str, variant: str | None = None) -> str:
    """
    Return the name of the derivative folder of a pipeline.

    Parameters
    ----------
    pipeline : str
        Name of the pipeline
    variant : str | None
        Variant (letters and digits only)

    Returns
    -------
    str
        `<pipeline>`, or `<pipeline>-<variant>`

    Raises
    ------
    ValueError
        If the variant is not made of letters and digits, or if
        `<pipeline>-<variant>` is the name of a registered pipeline
    """
    raise NotImplementedError


def task_id(pipeline: str, stage: str, unit: Unit,
            variant: str | None = None) -> str:
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
    variant : str | None
        Variant of the pipeline

    Returns
    -------
    str
        `grind/<pipeline>[@<variant>]/<stage>/<unit.label>`, e.g.
        "grind/fs-long/cross/sub-01/ses-1" or
        "grind/fs-long@hires/cross/sub-01/ses-1"; unique in the state
        folder of the dataset of the unit, so variants never share a
        record
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
    stage, `stage.fingerprint(config)`, the image reference, the raw
    inputs (path relative to `root`, size, modification time) and the
    id and signature of each upstream task. A change therefore
    propagates to everything downstream, and the member list of a
    fan-in and the variants of required pipelines are part of it.

    The image digest is not: it is only known once the image is local.
    It is recorded and compared apart (`plan`).

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
        Dataset folder that input paths are made relative to: a
        signature never holds an absolute path, so it is the same on
        translated roots
    upstream : Mapping[str, str | None] | None
        `{upstream task key: its signature}` (`PlanTask.key`: an
        upstream task may be in another dataset)
    container : str | None
        Image reference (`Container.image`), without digest

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
    runtime: ContainerRuntime | None = None,
    command: Sequence[str] = (),
    options: Mapping[str, Any] | None = None,
) -> Plan:
    """
    Build the plan of a run, without running or writing anything.

    For each dataset: the effective configuration, the units of each
    stage, their raw inputs (`Requirement.min`/`max`/`pick` and the
    overrides of the dataset), dependencies from the levels of the
    stages (unit to unit, fan-in, broadcast), then the verdict of each
    task from its record. Tasks of required pipelines are part of the
    graph, in the variant that `scope.variants` gives them. Each
    dataset ends with a `finalize` task (descriptions, manifest,
    failure list) after all of its tasks.

    Images are never pulled here. A done task is stale because of its
    image only if the recorded digest and the one that
    `runtime.inspect` finds are both known and differ; an unknown
    digest leaves it done (the plan records a null digest: unchecked).
    Images are pulled when the plan is run; a task then refuses a
    digest that differs from the plan's.

    A task recorded "pending" or "running" is "queued": it is not run
    by this plan, whose tasks that depend on it check its record when
    they start (`bx percolate PLAN --status` of the plan named in the
    record tells whether its job is still alive, and `--cancel` frees
    it).

    Every choice made here is recorded in the plan: the effective
    configurations, `if_stale`, the variants, image references and
    digests, the options of the command.

    Parameters
    ----------
    pipeline : Pipeline
        The pipeline to run
    scope : Scope
        Datasets, the home of group outputs, the variants
    configs : Mapping[str, Config | None] | None
        Configuration per pipeline name, before dataset overrides
        (default: the defaults of each model)
    redo : Sequence[str]
        Stages to run again, with everything that depends on them
        ("all": every stage)
    if_stale : {"error", "redo", "keep"}
        What to do with tasks done under another signature
    runtime : ContainerRuntime | None
        Runtime that inspects images (None: digests are not compared)
    command : Sequence[str]
        The command line, recorded in the plan
    options : Mapping[str, Any] | None
        Snapshot of the command options, recorded in the plan

    Returns
    -------
    Plan
        The plan (in memory: `percolate.plan.write_plan` saves it)

    Raises
    ------
    ValueError
        If the pipeline is inconsistent, if tasks are stale under
        "error", or if a derivative folder has content but no record
        of this pipeline (foreign outputs are never adopted)
    """
    raise NotImplementedError
