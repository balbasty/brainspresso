"""
Plans: the tasks of one roast or grind command, as plain data.

Interfaces only (see `brainspresso.percolate`).

A plan is a YAML file, `<state>/plans/<timestamp>-<hash>.yaml` (the
state folder of the dataset; `<root>/.brainspresso/plans/` for a
cross-dataset run), or the path given to `--plan`. It is written once
and never changed: the state of its tasks is in their task records
(JSON, `actions.state`), which name the plan.

```yaml
schema: 1
stage: grind                    # or roast
created: '2026-11-27T10:15:00+00:00'
brainspresso: '0.4.0'
command: [bx, ixi, grind, fs-long, --subs, '01']   # as typed
pipeline: {name: fs-long, version: '7.4.1', variant: null}
variants: {}                    # variant of each required pipeline
if_stale: error
images: {fs-long: {image: 'freesurfer/freesurfer:7.4.1', digest: null}}
options: {subs: ['sub-01']}     # snapshot of the command options
home: /data/bx/IXI              # where group outputs and the plan live
datasets:
- ref: IXI
  root: /data/bx/IXI            # as the planning machine sees it
  uid: 7b0c5c1e-...             # id of the dataset, in its manifest
  layout: study
  config: {fs-long: {hires: false}}   # effective, per pipeline
tasks:
- id: grind/fs-long/cross/sub-01/ses-1
  dataset: IXI
  stage: cross
  scope: {level: session, sub: sub-01, ses: ses-1}
  after: []
  after_any: []
  resources: {cpus: 4, memory_gb: 16.0}
  overrides: {}
  signature: null
  verdict: todo
```

Paths. A plan holds no path but the roots (`home`, each dataset
`root`): tasks are ids and scopes, and everything else is relative to a
root. On another machine (compute nodes) the roots are translated by
the path map of the profile (`percolate.options.translate`), and
`resolve_roots` checks that each translated root exists and holds the
dataset the plan names (`uid`).

YAML and strings. YAML 1.1 reads an unquoted `01` as a number and `no`
or `on` as booleans, and YAML 1.2 readers read `1e3` as a number
although PyYAML writes it unquoted (`YAML_PITFALLS`). So:

* plans are read with `yaml.safe_load`, nothing else, and written with
  a `yaml.SafeDumper` subclass that quotes every string scalar
  (`plan_dumper`): such values round-trip as strings for any reader,
  and nothing is refused when writing (the example above shows the
  content, not the quoting);

* BIDS labels are written with their prefix (`sub-01`, `ses-no`), which
  no reader takes for anything but a string;
* `read_plan` checks that every field that must be a string is one
  (`check_strings`) and names the field to quote otherwise: it never
  converts a number or a boolean back into a label.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

__all__ = [
    "PLANS_DIR", "PLAN_SCHEMA", "YAML_PITFALLS", "Plan", "PlanDataset",
    "PlanError", "PlanTask", "Resources", "TaskScope", "check_strings",
    "plan_dumper", "plan_path", "read_plan", "resolve_roots",
    "write_plan",
]

#: Folder of the plans, in the state folder
PLANS_DIR = "plans"
#: Version of the format of plan files
PLAN_SCHEMA = 1
#: Strings that YAML readers take for something else when unquoted
#: (number, boolean, null, date): why the plan writer quotes every
#: string, and what the contract tests round-trip
YAML_PITFALLS = ("01", "1", "1.0", "1e3", "0x1F", "0o17", "no", "on",
                 "yes", "off", "y", "n", "true", "False", "null", "~",
                 "", "2026-11-27", ".inf", ".nan")


class PlanError(ValueError):
    """
    A plan that cannot be read, written or followed.

    Unknown schema, a field of the wrong type (a label read as a
    number: "quote it"), a root that is missing, or a stale plan: a
    pipeline version or image digest that differs from the plan's.
    """


@dataclass(frozen=True)
class Resources:
    """
    Resources of one task.

    Attributes
    ----------
    cpus : int
        CPU cores (`--cpus`)
    memory_gb : float | None
        Memory, in GiB (`--mem`; None: the default of the backend)
    gpus : int
        GPUs (`--gpus`)
    walltime_min : int | None
        Time limit, in minutes (`--time`; None: the default of the
        backend)
    scratch_gb : float | None
        Local scratch space, in GiB
    """

    cpus: int = 1
    memory_gb: float | None = None
    gpus: int = 0
    walltime_min: int | None = None
    scratch_gb: float | None = None

    def merged(self, other: "Resources | Mapping[str, Any] | None"
               ) -> "Resources":
        """
        Override with the fields that another set of resources gives.

        Parameters
        ----------
        other : Resources | Mapping[str, Any] | None
            Overrides: a mapping only overrides the fields it names

        Returns
        -------
        Resources
            The merged resources
        """
        raise NotImplementedError


@dataclass(frozen=True)
class TaskScope:
    """
    What one task is restricted to.

    Attributes
    ----------
    level : str
        "session", "subject", "dataset" or "group" (`grind.model.LEVELS`;
        roast uses "subject", and "dataset" for its head and finalize
        tasks)
    sub : str | None
        Subject, with its prefix (`sub-01`)
    ses : str | None
        Session, with its prefix (`ses-1`)
    extra : Mapping[str, str]
        Other entities, values with their prefix
    """

    level: str
    sub: str | None = None
    ses: str | None = None
    extra: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PlanTask:
    """
    One task of a plan: a scoped re-invocation of its stage.

    Attributes
    ----------
    id : str
        Task id, also that of its record (`roast/...`, `grind/...`)
    dataset : str | None
        Reference of its dataset (None: the home of a cross-dataset
        run)
    stage : str | None
        Stage of the pipeline (grind); None for roast
    scope : TaskScope
        What it is restricted to
    after : tuple[str, ...]
        Ids of tasks that must succeed first
    after_any : tuple[str, ...]
        Ids of tasks that must be over first
    resources : Resources
        What the stage declares: a default, see `resolve_resources`
    overrides : Mapping[str, Any]
        Resource fields that the dataset overrides
    signature : str | None
        Signature expected when it starts, if known at plan time
    verdict : str
        What the plan said of it ("todo", "done", "queued", "stale",
        "blocked"); only "todo" tasks are run
    reason : str
        Why it is blocked, stale or queued
    """

    id: str
    dataset: str | None = None
    stage: str | None = None
    scope: TaskScope = TaskScope("dataset")
    after: tuple[str, ...] = ()
    after_any: tuple[str, ...] = ()
    resources: Resources = Resources()
    overrides: Mapping[str, Any] = field(default_factory=dict)
    signature: str | None = None
    verdict: str = "todo"
    reason: str = ""


@dataclass(frozen=True)
class PlanDataset:
    """
    One dataset of a plan.

    Attributes
    ----------
    ref : str
        Its reference (`grind.index.DatasetRef`)
    root : str
        Dataset folder, as the machine that planned sees it; the only
        absolute path of the dataset in the plan (translated by the
        path map of the profile on other machines)
    uid : str | None
        Identifier of the dataset: a UUID that `Layout.ensure_state`
        writes once in `<state>/manifest.json` (step 7); what a job
        checks before it trusts a translated root. None: a dataset
        made before, accepted with a warning
    layout : str
        Its layout kind
    config : Mapping[str, Mapping[str, Any]]
        Effective configuration per pipeline, for this dataset
    """

    ref: str
    root: str
    uid: str | None = None
    layout: str = "study"
    config: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class Plan:
    """
    The tasks of one roast or grind command, and what was chosen for it.

    Everything decided when planning is recorded here and is not asked
    again when the plan is run: options, effective configurations, the
    policy for stale tasks, the variants of required pipelines, image
    references and digests. What is chosen when it is run: the backend,
    the profile and resource overrides (`percolate.options`).

    Attributes
    ----------
    stage : {"roast", "grind"}
        Stage whose planner runs the tasks (`percolate.run.PLANNERS`)
    datasets : tuple[PlanDataset, ...]
        The datasets
    home : str
        Root of the dataset, of the sites parent or of the collection
        that holds group outputs and the saved plan
    tasks : tuple[PlanTask, ...]
        Every task, whatever its verdict, each after those it depends
        on
    pipeline : Mapping[str, str | None]
        Grind: `{"name": ..., "version": ..., "variant": ...}`
    variants : Mapping[str, str | None]
        Grind: variant of each required pipeline
    if_stale : {"error", "redo", "keep"}
        Grind: what was decided for stale tasks
    images : Mapping[str, Mapping[str, str | None]]
        `{pipeline: {"image": ..., "digest": ...}}`; a digest is None
        when the image was not local at plan time
    options : Mapping[str, Any]
        Snapshot of the command options (selection, layout...)
    command : tuple[str, ...]
        The command line, for information
    created : str
        UTC time, ISO 8601
    brainspresso : str
        Version that wrote the plan
    schema : int
        `PLAN_SCHEMA`
    path : Path | None
        File it was read from or written to (None: in memory)
    """

    stage: Literal["roast", "grind"]
    datasets: tuple[PlanDataset, ...] = ()
    home: str = ""
    tasks: tuple[PlanTask, ...] = ()
    pipeline: Mapping[str, str | None] = field(default_factory=dict)
    variants: Mapping[str, str | None] = field(default_factory=dict)
    if_stale: str = "error"
    images: Mapping[str, Mapping[str, str | None]] = field(
        default_factory=dict)
    options: Mapping[str, Any] = field(default_factory=dict)
    command: tuple[str, ...] = ()
    created: str = ""
    brainspresso: str = ""
    schema: int = PLAN_SCHEMA
    path: Path | None = None

    @property
    def id(self) -> str:
        """
        Identifier of the plan: `<timestamp>-<hash>`.

        The hash is that of its content; task records name it (`plan`).
        """
        raise NotImplementedError

    def task(self, id: str) -> PlanTask:
        """
        Return a task by id.

        Parameters
        ----------
        id : str
            Task id

        Returns
        -------
        PlanTask
            The task

        Raises
        ------
        KeyError
            If the plan has no such task
        """
        raise NotImplementedError

    def todo(self) -> tuple[PlanTask, ...]:
        """
        Return the tasks to run.

        Returns
        -------
        tuple[PlanTask, ...]
            Tasks whose verdict is "todo", in dependency order
        """
        raise NotImplementedError


def resolve_roots(plan: Plan,
                  paths: Sequence[tuple[str, str]] = ()) -> dict:
    """
    Return the roots of a plan on this machine, checked.

    Each root is translated by the path map (first prefix that
    matches), must exist, and must hold the dataset that the plan
    names: the `uid` of its manifest.

    Parameters
    ----------
    plan : Plan
        The plan
    paths : Sequence[tuple[str, str]]
        Ordered `(from, to)` prefixes (`Profile.paths`)

    Returns
    -------
    dict[str | None, Path]
        `{dataset reference: root}`; None: the home

    Raises
    ------
    PlanError
        Naming the root that is missing here, or that holds another
        dataset, and the path map that was applied (a dataset without
        an id is accepted with a warning)
    """
    raise NotImplementedError


def plan_dumper() -> type:
    """
    Return the YAML dumper of plans.

    A subclass of `yaml.SafeDumper` (it writes plain data only) that
    represents every `str` as a quoted scalar, keys included; numbers,
    booleans and nulls stay plain. So `"1e3"`, `"01"`, `"no"` and
    `"on"` are read back as strings by YAML 1.1 and 1.2 readers alike.
    Built on first use: PyYAML is not imported with this module.

    Returns
    -------
    type
        The dumper class, for `yaml.dump(..., Dumper=...)`
    """
    raise NotImplementedError


def check_strings(document: Mapping[str, Any]) -> None:
    """
    Check the strings of a plan document.

    Every field that must be a string (ids, dataset references, labels,
    pipeline, stage, variant and version names, dependencies) is one,
    and labels carry their prefix. Nothing is converted: `sub: 01`,
    read as the number 1 from a plan edited by hand, is an error. It is
    the check of the reader; the writer refuses nothing.

    Parameters
    ----------
    document : Mapping[str, Any]
        What `yaml.safe_load` returned

    Raises
    ------
    PlanError
        Naming the first offending field and its value
    """
    raise NotImplementedError


def plan_path(state: str | Path, plan: Plan) -> Path:
    """
    Return the default path of a plan.

    Parameters
    ----------
    state : str | Path
        State folder of the dataset (of the home of a cross-dataset
        run)
    plan : Plan
        The plan

    Returns
    -------
    Path
        `<state>/plans/<plan.id>.yaml`
    """
    raise NotImplementedError


def write_plan(plan: Plan, path: str | Path | None = None) -> Path:
    """
    Write a plan (YAML, every string quoted; atomically).

    `yaml.dump(document, Dumper=plan_dumper())`.

    Parameters
    ----------
    plan : Plan
        The plan
    path : str | Path | None
        Where (default: `plan_path` of its home)

    Returns
    -------
    Path
        The file

    Raises
    ------
    OSError
        If it cannot be written
    """
    raise NotImplementedError


def read_plan(path: str | Path) -> Plan:
    """
    Read a plan (`yaml.safe_load`).

    Parameters
    ----------
    path : str | Path
        Plan file, or a plan id in the dataset of the current folder

    Returns
    -------
    Plan
        The plan, with its `path`

    Raises
    ------
    PlanError
        If the schema is unknown, or if `check_strings` refuses it
    """
    raise NotImplementedError
