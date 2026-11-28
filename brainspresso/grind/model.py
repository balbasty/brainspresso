"""
Model of a grind pipeline: pipelines, stages, units and requirements.

Interfaces only (see `brainspresso.grind`). Everything here is
declarative: a pipeline lists stages, a stage lists what it needs and
what it writes, and the order of the work follows from that
(`grind.graph`). There is no wiring syntax.

A task of a plan names its pipeline and stage; the job that runs it
finds them again by name (`get_pipeline`, same version, else it
refuses). So a pipeline that runs from a plan is registered, and the
callables of its stages (`FunctionStage.fn`, `CommandStage.command`)
are importable as `module:qualname`: module-level functions, never
lambdas or closures.

```python
fs_long = Pipeline(
    "fs-long", "7.4.1", config=FsConfig, stages=(
        FunctionStage("cross", "session", fn=cross, inputs=(
            Requirement("t1w", Query(suffix=("T1w",))),)),
        FunctionStage("base", "subject", fn=base, inputs=(
            Requirement("cross", stage="cross", max=None, need="all"),)),
        FunctionStage("long", "session", fn=long, inputs=(
            Requirement("cross", stage="cross"),
            Requirement("base", stage="base"))),
    ))
```
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    Union,
    runtime_checkable,
)

from brainspresso.actions.action import Action
from brainspresso.actions.engine import Producer
from brainspresso.bids.select import Selection
from brainspresso.grind.containers import Container
from brainspresso.percolate.plan import Resources

if TYPE_CHECKING:
    from brainspresso.grind.context import GrindContext

__all__ = [
    "LEVELS", "PIPELINES", "CommandStage", "Config", "FunctionStage",
    "Level", "Need", "Output", "Pick", "Pipeline", "Query", "Requirement",
    "Resources", "Stage", "StageFunction", "Unit", "Work", "format_output",
    "get_pipeline", "register_pipeline",
]

#: Name of a unit level (a key of `LEVELS`)
Level = str
#: Unit levels: name -> entities that key a unit of that level. A
#: dataset without sessions has one "session" unit per subject (no
#: `ses`). "dataset": one unit per dataset; "group": one unit per run
#: (the dataset, the parent of a sites layout, or the collection of a
#: cross-dataset run). Levels can be added (e.g. "run").
LEVELS: Mapping[str, tuple[str, ...]] = MappingProxyType({
    "session": ("sub", "ses"),
    "subject": ("sub",),
    "dataset": (),
    "group": (),
})
#: Fan-in rule of a requirement: every upstream unit ("all"), at least
#: one ("any"), or at least this many
Need = Union[Literal["all", "any"], int]
#: What to do when a requirement matches more files than its `max`
Pick = Literal["error", "first", "last"]


@runtime_checkable
class Config(Protocol):
    """
    Configuration of a pipeline: what the contract needs of it.

    Any pydantic v2 model satisfies this protocol; pipelines declare a
    model with documented fields (the command line options, `--config`
    files and hashes derive from it). The contract itself does not
    import pydantic.
    """

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        """
        Return the fields as a dict.

        Parameters
        ----------
        mode : {"python", "json"}
            "json": JSON-serializable values only (hashes, files)

        Returns
        -------
        dict[str, Any]
            `{field: value}`
        """
        ...

    @classmethod
    def model_validate(cls, obj: Any) -> "Config":
        """
        Build a validated configuration from a mapping.

        Parameters
        ----------
        obj : Any
            `{field: value}` (a config file, command line options)

        Returns
        -------
        Config
            The configuration
        """
        ...

    @classmethod
    def model_json_schema(cls) -> dict[str, Any]:
        """
        Return the JSON schema of the fields (names, types, help).

        Returns
        -------
        dict[str, Any]
            JSON schema
        """
        ...


@dataclass(frozen=True)
class Unit:
    """
    What one task of a stage works on.

    Attributes
    ----------
    level : str
        Its level (a key of `LEVELS`)
    dataset : str | None
        Reference of its dataset (`grind.index.DatasetRef`, as a
        string); None for the group unit of a cross-dataset run
    sub : str | None
        Subject label, without `sub-`; labels are strings, never numbers
    ses : str | None
        Session label, without `ses-`; None if the dataset has no
        sessions, or above the session level
    extra : tuple[tuple[str, str], ...]
        Other entities, for levels beyond those of `LEVELS`
    """

    level: Level
    dataset: str | None = None
    sub: str | None = None
    ses: str | None = None
    extra: tuple[tuple[str, str], ...] = ()

    @property
    def entities(self) -> dict[str, str]:
        """Entities of the unit, e.g. `{"sub": "01", "ses": "1"}`."""
        raise NotImplementedError

    @property
    def label(self) -> str:
        """
        Label of the unit in task ids.

        `sub-01/ses-1`, `sub-01`, `dataset` or `group`.
        """
        raise NotImplementedError

    def contains(self, other: "Unit") -> bool:
        """
        Tell whether another unit is inside this one.

        A unit contains the units of the same dataset whose entities
        include its own (a subject contains its sessions); a group unit
        contains every unit of the run.

        Parameters
        ----------
        other : Unit
            A unit of the same or of a finer level

        Returns
        -------
        bool
            Whether `other` fans into (or equals) this unit
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Query:
    """
    Files of a BIDS dataset that a requirement matches.

    Empty filters match everything. Only data files match (sidecars and
    inherited metadata follow them). Named after the BIDS entities, as
    the fields of `bids.select.Selection`.

    Attributes
    ----------
    datatype : tuple[str, ...]
        Datatype folders (`anat`, `func`...)
    suffix : tuple[str, ...]
        Suffixes (`T1w`, `bold`...)
    extension : tuple[str, ...]
        Extensions, with the leading dot (`.nii.gz`)
    entities : Mapping[str, tuple[str, ...]]
        Values to keep per entity (`{"acq": ("mprage",)}`), as in
        `bids.select.Selection.entities`
    include, exclude : tuple[str, ...]
        Glob patterns on the path relative to the dataset
    metadata : Mapping[str, Any]
        Conditions on sidecar fields (`{"MagneticFieldStrength": 3}`);
        reserved, format decided in step 7
    """

    datatype: tuple[str, ...] = ()
    suffix: tuple[str, ...] = ()
    extension: tuple[str, ...] = ()
    entities: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def selection(self, unit: Unit | None = None) -> Selection:
        """
        Return the query as a `Selection`, restricted to a unit.

        Parameters
        ----------
        unit : Unit | None
            Unit whose subject and session are added to the filters

        Returns
        -------
        Selection
            Filters of `bids.select` (without `metadata`)
        """
        raise NotImplementedError

    def narrow(self, other: "Query") -> "Query":
        """
        Combine with another query: files that match both.

        Used for dataset overrides ("which T1w",
        `GrindOverrides.inputs`).

        Parameters
        ----------
        other : Query
            Query to intersect with

        Returns
        -------
        Query
            The intersection
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Requirement:
    """
    A named input of a stage.

    Where it comes from: the raw dataset (`pipeline` and `stage` both
    None), an earlier stage of the same pipeline (`stage`), or another
    pipeline, which must be listed in `Pipeline.requires` (`pipeline`,
    and optionally one of its stages). An upstream stage becomes a
    dependency of the task; a `pipeline` without `stage` depends on
    every terminal stage of that pipeline (`Pipeline.terminal`), in the
    variant that the run chose for it (`grind.graph.Scope.variants`).
    The levels give the mapping: same level,
    unit to unit; a finer upstream level fans in (every upstream unit
    that the unit `contains`); a coarser one is broadcast.

    Attributes
    ----------
    name : str
        Key of the files in `Work.inputs`
    query : Query | None
        Files to match (None with a `stage`: all of its outputs, or
        those named in `outputs`)
    pipeline : str | None
        Pipeline whose derivative is read
    stage : str | None
        Stage (of `pipeline`, else of this pipeline) that must have
        run; None with a `pipeline`: each of its terminal stages
    outputs : tuple[str, ...]
        Names of `Output`s of the upstream stage to take
    min : int
        Fewest matching files per unit (0: optional input)
    max : int | None
        Most matching files per unit (None: no limit); more is an error
        unless `pick` or a dataset override decides
    pick : {"error", "first", "last"}
        What to do above `max` (files sorted by path)
    need : {"all", "any"} | int
        Fan-in: how many upstream units must have succeeded for the
        task to run ("all": every planned one). Default "any": the task
        runs on those that succeeded
    """

    name: str
    query: Query | None = None
    pipeline: str | None = None
    stage: str | None = None
    outputs: tuple[str, ...] = ()
    min: int = 1
    max: int | None = 1
    pick: Pick = "error"
    need: Need = "any"

    @property
    def raw(self) -> bool:
        """Whether it reads the raw dataset (no upstream task)."""
        raise NotImplementedError


@dataclass(frozen=True)
class Output:
    """
    A declared output of a stage.

    Attributes
    ----------
    name : str
        Key of the path in `Work.outputs`
    template : str
        Path relative to the derivative dataset (`format_output`), e.g.
        `sub-{sub}[/ses-{ses}]/anat/{base}_desc-preproc_{suffix}.nii.gz`;
        fixed by the pipeline (the configuration does not override it)
    source : str | None
        Name of the requirement that gives `{base}` and `{suffix}`
    claim : bool
        The template names a folder (or the start of file names) whose
        content is only known once the stage has run (`actions.Claim`)
    optional : bool
        The stage may not write it (not checked when a unit is skipped)
    """

    name: str
    template: str
    source: str | None = None
    claim: bool = False
    optional: bool = False


@dataclass(frozen=True)
class Work:
    """
    What `Stage.run` receives: one unit, resolved.

    Attributes
    ----------
    unit : Unit
        The unit
    config : Config | None
        Effective configuration (defaults, dataset overrides, config
        file, command line)
    inputs : Mapping[str, tuple[Path, ...]]
        Files of each requirement, by name: the paths to read, i.e.
        the copies on scratch when inputs are staged
    outputs : Mapping[str, Path]
        Path of each declared output, by name, in `root`
    root : Path
        Derivative dataset that the stage writes into
    stage : {"none", "inputs", "all"}
        Staging of the task (`grind.staging`). "all": commands write
        under `workdir` and their outputs are committed to `root`
        afterwards; actions that write through `File` always write to
        `root`
    workdir : Path
        Scratch folder of the task (removed when it succeeds)
    resources : Resources
        Resources granted to the task
    members : tuple[Unit, ...]
        Fan-in: upstream units that succeeded (their files are in
        `inputs`)
    failed : tuple[Unit, ...]
        Fan-in: upstream units that failed or were skipped
    """

    unit: Unit
    config: Config | None = None
    inputs: Mapping[str, tuple[Path, ...]] = field(default_factory=dict)
    outputs: Mapping[str, Path] = field(default_factory=dict)
    root: Path = Path(".")
    stage: str = "none"
    workdir: Path = Path(".")
    resources: Resources = Resources()
    members: tuple[Unit, ...] = ()
    failed: tuple[Unit, ...] = ()


@dataclass(frozen=True)
class Stage(ABC):
    """
    One step of a pipeline, run once per unit of its level.

    Subclass it for another kind of stage (a MATLAB batch, a report, a
    table built from all units): only `run` is required.

    Attributes
    ----------
    name : str
        Name, unique in the pipeline (task ids, `--redo`)
    level : str
        Level of its units (a key of `LEVELS`)
    inputs : tuple[Requirement, ...]
        What it needs
    outputs : tuple[Output, ...]
        What it writes
    resources : Resources
        Resources that one task needs: the default of the plan, below
        the profile, the dataset overrides and the command line
        (`percolate.options.resolve_resources`)
    container : Container | None
        Container of its commands (None: that of the pipeline)
    """

    name: str
    level: Level
    inputs: tuple[Requirement, ...] = ()
    outputs: tuple[Output, ...] = ()
    resources: Resources = Resources()
    container: Container | None = None

    @abstractmethod
    def run(self, ctx: "GrindContext", work: Work
            ) -> Iterator[Action | Producer]:
        """
        Yield the actions that process one unit.

        It is a producer of the action engine (`actions.engine`), run
        as one shard: actions are run one by one, their outputs are
        written through `actions.file.File` and recorded in the task
        state. A tool that writes by itself is wrapped in one action
        that claims its output folder.

        Parameters
        ----------
        ctx : GrindContext
            Context of the run
        work : Work
            The unit, its inputs and its output paths

        Yields
        ------
        Action | Producer
            Actions, or shards of its own
        """
        raise NotImplementedError

    def fingerprint(self, config: Config | None) -> dict[str, Any]:
        """
        Describe what of the configuration decides its outputs.

        Part of the task signature (`grind.graph.signature`). Default:
        every field (`config.model_dump(mode="json")`).

        Parameters
        ----------
        config : Config | None
            Effective configuration

        Returns
        -------
        dict[str, Any]
            JSON-serializable values
        """
        raise NotImplementedError


#: Body of a `FunctionStage`: `fn(ctx, work)` yields actions
StageFunction = Callable[["GrindContext", Work], Iterator]


@dataclass(frozen=True)
class FunctionStage(Stage):
    """
    A stage whose body is a Python function.

    Attributes
    ----------
    fn : callable(ctx, work) -> iterator of Action | Producer | None
        Function that yields the actions; importable as
        `module:qualname` (`validate` refuses a lambda or a closure)
    """

    fn: StageFunction | None = None

    def run(self, ctx: "GrindContext", work: Work
            ) -> Iterator[Action | Producer]:
        """
        Yield the actions of `fn(ctx, work)`.

        Parameters
        ----------
        ctx : GrindContext
            Context of the run
        work : Work
            The unit, its inputs and its output paths

        Yields
        ------
        Action | Producer
            What `fn` yields
        """
        raise NotImplementedError


@dataclass(frozen=True)
class CommandStage(Stage):
    """
    A stage that runs one command line, in its container if it has one.

    The command is run through the container runtime of the profile
    (`grind.containers`), inputs mounted read-only and the derivative
    and scratch folders read-write. Outputs written by the command are
    those declared by the stage (claims for those it cannot list).

    Attributes
    ----------
    command : callable(work) -> list[str] | None
        Function that returns the argv (paths as the command sees
        them); importable as `module:qualname`
    env : Mapping[str, str]
        Environment variables of the command
    """

    command: Callable[[Work], Sequence[str]] | None = None
    env: Mapping[str, str] = field(default_factory=dict)

    def run(self, ctx: "GrindContext", work: Work
            ) -> Iterator[Action | Producer]:
        """
        Yield one action that runs the command.

        Parameters
        ----------
        ctx : GrindContext
            Context of the run
        work : Work
            The unit, its inputs and its output paths

        Yields
        ------
        Action
            The command, with its declared outputs or claims
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Pipeline:
    """
    A dataset-agnostic pipeline: stages, a configuration, requirements.

    It reads a raw BIDS dataset (and the derivatives of the pipelines
    it requires) and writes `derivatives/<name>/`.

    Attributes
    ----------
    name : str
        Name: command line, task ids, derivative folder
    version : str
        Version (signatures, `GeneratedBy`)
    stages : tuple[Stage, ...]
        Its stages, in any order (requirements order them)
    config : type[Config] | None
        Configuration model (None: no option)
    requires : tuple[str, ...]
        Names of pipelines whose outputs it reads; their tasks join the
        same graph
    container : Container | None
        Default container of its stages
    description : str
        One line, for the help and `GeneratedBy`
    code_url : str | None
        `GeneratedBy.CodeURL`
    """

    name: str
    version: str
    stages: tuple[Stage, ...] = ()
    config: type[Config] | None = None
    requires: tuple[str, ...] = ()
    container: Container | None = None
    description: str = ""
    code_url: str | None = None

    def stage(self, name: str) -> Stage:
        """
        Return a stage by name.

        Parameters
        ----------
        name : str
            Name of the stage

        Returns
        -------
        Stage
            The stage

        Raises
        ------
        KeyError
            If the pipeline has no such stage
        """
        raise NotImplementedError

    def ordered(self) -> tuple[Stage, ...]:
        """
        Return the stages in dependency order.

        Returns
        -------
        tuple[Stage, ...]
            Each stage after those it requires
        """
        raise NotImplementedError

    def terminal(self) -> tuple[Stage, ...]:
        """
        Return the stages that no other stage of the pipeline requires.

        What a `Requirement` on this pipeline without a `stage` waits
        for.

        Returns
        -------
        tuple[Stage, ...]
            Terminal stages, in dependency order
        """
        raise NotImplementedError

    def validate(self) -> None:
        """
        Check the declaration.

        Stage names are unique, levels are known, requirements name
        existing stages, outputs and required pipelines, output
        templates only use known fields, stage callables are importable
        (`module:qualname`), and stages do not depend on
        each other in a cycle. An output with `claim=True` must use
        every entity of the level of its stage (`sub-{sub}/` is refused
        for a session stage: its tasks would overwrite each other).

        Raises
        ------
        ValueError
            If the declaration is inconsistent
        """
        raise NotImplementedError

    def default_config(self) -> Config | None:
        """
        Return the configuration with every default.

        Returns
        -------
        Config | None
            `config()` (None without a model)
        """
        raise NotImplementedError


#: Pipelines by name: a `Pipeline`, or "module:attribute" loaded on
#: first use (as the dataset catalog)
PIPELINES: dict[str, "Pipeline | str"] = {}


def register_pipeline(pipeline: "Pipeline | str",
                      name: str | None = None) -> None:
    """
    Add a pipeline to the registry.

    Parameters
    ----------
    pipeline : Pipeline | str
        The pipeline, or "module:attribute" to import lazily
    name : str | None
        Its name (required with a string)

    Raises
    ------
    ValueError
        If the name is taken by another pipeline
    """
    raise NotImplementedError


def get_pipeline(name: str) -> Pipeline:
    """
    Find a registered pipeline.

    Parameters
    ----------
    name : str
        Name of the pipeline (case-insensitive)

    Returns
    -------
    Pipeline
        The pipeline, validated

    Raises
    ------
    KeyError
        If no pipeline has this name
    """
    raise NotImplementedError


def format_output(template: str, unit: Unit, **fields: Any) -> PurePosixPath:
    """
    Format the template of an output path.

    A `str.format` template over the entities of the unit (`{sub}`,
    `{ses}`), `{base}` (file name of the source input without suffix
    and extension) and `{suffix}`, and extra fields (configuration
    values). A `[...]` segment is dropped when one of its fields is
    missing or None, so one template serves datasets with and without
    sessions. Labels are strings: no numeric format.

    Parameters
    ----------
    template : str
        E.g. `sub-{sub}[/ses-{ses}]/anat/{base}_probseg.nii.gz`
    unit : Unit
        Unit that gives the entities
    **fields
        Other fields

    Returns
    -------
    PurePosixPath
        Path relative to the derivative dataset

    Raises
    ------
    KeyError
        If a field outside brackets is missing
    ValueError
        If the path leaves the dataset (`..`, absolute)
    """
    raise NotImplementedError
