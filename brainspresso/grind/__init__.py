"""
Grind: preprocess BIDS datasets into `derivatives/<pipeline>/`.

**Interfaces only** (step 4g): this package is the contract of
`docs/design/architecture.md` (§7) written as stubs. Nothing runs yet:
every function raises `NotImplementedError`, and step 7 fills the
bodies without changing the names, the arguments or the meaning that the
docstrings give them.

* `model`: `Pipeline`, `Stage` (`FunctionStage`, `CommandStage`),
  `Requirement`, `Query`, `Output`, `Unit`, `LEVELS`, `Resources`,
  `Config`, `Work`, `format_output`, the pipeline registry;
* `overrides`: `GrindOverrides`, what a dataset declares for grind;
* `containers`: `Container`, `Mount`, `ContainerRuntime`;
* `execution`: `Executor`, `Handle`, `After` (the submit protocol),
  `Profile`;
* `graph`: `Task`, `Graph`, `Scope`, `enumerate_units`, `signature`,
  `plan`;
* `context`: `GrindContext`, `GrindPolicy` (the grind layer of the
  action engine);
* `bidsapp`: `BidsAppPipeline`, `BidsAppStage`, `BidsAppConfig`;
* `index`: `DatasetRef`, `resolve_datasets`, `build_index`, `Where`;
* `provenance`: `generated_by`, `source_datasets`,
  `describe_derivative`;
* `run`: `grind_dataset`, `grind_collection`, `grind_status`,
  `GrindReport`.

Layering: `grind/` builds on `actions/` and `bids/`; `actions/` never
imports it. No module of this package imports an optional dependency
(pydantic, submitit, Dask, Ray).
"""
from brainspresso.grind.bidsapp import (
    BidsAppConfig,
    BidsAppPipeline,
    BidsAppStage,
)
from brainspresso.grind.containers import (
    RUNTIMES,
    Container,
    ContainerRuntime,
    Mount,
    get_runtime,
)
from brainspresso.grind.context import GrindContext, GrindPolicy
from brainspresso.grind.execution import (
    BACKENDS,
    After,
    Executor,
    Handle,
    Profile,
    load_profile,
    make_executor,
)
from brainspresso.grind.graph import (
    GRIND_FIELDS,
    VERDICTS,
    DatasetScope,
    Graph,
    Scope,
    Task,
    enumerate_units,
    plan,
    signature,
    task_id,
)
from brainspresso.grind.index import (
    WHERE_OPERATORS,
    DatasetRef,
    Where,
    build_index,
    parse_where,
    resolve_datasets,
)
from brainspresso.grind.model import (
    LEVELS,
    PIPELINES,
    CommandStage,
    Config,
    FunctionStage,
    Output,
    Pipeline,
    Query,
    Requirement,
    Resources,
    Stage,
    Unit,
    Work,
    format_output,
    get_pipeline,
    register_pipeline,
)
from brainspresso.grind.overrides import GrindOverrides
from brainspresso.grind.provenance import (
    describe_derivative,
    generated_by,
    source_datasets,
)
from brainspresso.grind.run import (
    GRIND_LOCK,
    GrindReport,
    grind_collection,
    grind_dataset,
    grind_status,
)

__all__ = [
    "BACKENDS", "GRIND_FIELDS", "GRIND_LOCK", "LEVELS", "PIPELINES",
    "RUNTIMES", "VERDICTS", "WHERE_OPERATORS", "After", "BidsAppConfig",
    "BidsAppPipeline", "BidsAppStage", "CommandStage", "Config",
    "Container", "ContainerRuntime", "DatasetRef", "DatasetScope",
    "Executor", "FunctionStage", "Graph", "GrindContext",
    "GrindOverrides", "GrindPolicy", "GrindReport", "Handle", "Mount",
    "Output", "Pipeline", "Profile", "Query", "Requirement", "Resources",
    "Scope", "Stage", "Task", "Unit", "Where", "Work", "build_index",
    "describe_derivative", "enumerate_units", "format_output",
    "generated_by", "get_pipeline", "get_runtime", "grind_collection",
    "grind_dataset", "grind_status", "load_profile", "make_executor",
    "parse_where", "plan", "register_pipeline", "resolve_datasets",
    "signature", "source_datasets", "task_id",
]
