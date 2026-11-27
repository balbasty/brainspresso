"""
Grind: preprocess BIDS datasets into `derivatives/<pipeline>/`.

**Interfaces only** (step 4g): this package is the contract of
`docs/design/architecture.md` (§7) written as stubs. Nothing runs yet:
every function raises `NotImplementedError`, and step 7 fills the
bodies without changing the names, the arguments or the meaning that the
docstrings give them.

* `model`: `Pipeline`, `Stage` (`FunctionStage`, `CommandStage`),
  `Requirement`, `Query`, `Output`, `Unit`, `LEVELS`, `Config`, `Work`,
  `format_output`, the pipeline registry;
* `overrides`: `GrindOverrides`, what a dataset declares for grind;
* `containers`: `Container`, `Mount`, `ContainerRuntime`;
* `graph`: `Scope`, `task_id`, `canonical_key`, `derivative_name`,
  `enumerate_units`,
  `signature`, `plan` (which returns a `percolate.plan.Plan`);
* `context`: `GrindContext`, `GrindPolicy` (the grind layer of the
  action engine);
* `bidsapp`: `BidsAppPipeline`, `BidsAppStage`, `BidsAppConfig`;
* `index`: `DatasetRef`, `resolve_datasets`, `build_index`, `Where`;
* `provenance`: `generated_by`, `source_datasets`,
  `describe_derivative`;
* `staging`: `stage_in`, `stage_out` (node-local scratch);
* `run`: `grind_dataset`, `grind_collection`, `grind_status`,
  `GrindReport`, `GrindPlanner`.

Plans, execution options, scheduler jobs and `bx percolate` are
stage-agnostic and live in `brainspresso.percolate` (`Resources` is
re-exported here).

Layering: `grind/` builds on `actions/`, `bids/` and `percolate/`;
none of them imports it. No module of this package imports an optional
dependency (pydantic, PyYAML).
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
from brainspresso.grind.graph import (
    GRIND_FIELDS,
    VERDICTS,
    DatasetScope,
    Scope,
    canonical_key,
    derivative_name,
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
    GrindPlanner,
    GrindReport,
    grind_collection,
    grind_dataset,
    grind_status,
)
from brainspresso.grind.staging import stage_in, stage_out

__all__ = [
    "GRIND_FIELDS", "GRIND_LOCK", "LEVELS", "PIPELINES", "RUNTIMES",
    "VERDICTS", "WHERE_OPERATORS", "BidsAppConfig", "BidsAppPipeline",
    "BidsAppStage", "CommandStage", "Config", "Container",
    "ContainerRuntime", "DatasetRef", "DatasetScope", "FunctionStage",
    "GrindContext", "GrindOverrides", "GrindPlanner", "GrindPolicy",
    "GrindReport", "Mount", "Output", "Pipeline", "Query",
    "Requirement", "Resources", "Scope", "Stage", "Unit", "Where",
    "Work", "build_index", "canonical_key", "derivative_name",
    "describe_derivative", "enumerate_units", "format_output",
    "generated_by", "get_pipeline", "get_runtime", "grind_collection",
    "grind_dataset", "grind_status", "parse_where", "plan",
    "register_pipeline", "resolve_datasets", "signature",
    "source_datasets", "stage_in", "stage_out", "task_id",
]
