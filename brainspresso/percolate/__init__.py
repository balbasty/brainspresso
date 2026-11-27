"""
Percolate: saved plans of roast and grind, and how their tasks run.

**Interfaces only** (step 4g), as `brainspresso.grind`: the contract of
`docs/design/architecture.md` (§7) written as stubs; every function
raises `NotImplementedError`.

A **plan** lists the tasks of one roast or grind command. A task is a
scoped re-invocation of its stage, described by plain data, never a
serialised callable: running it calls the planner of the stage again,
restricted to the scope of the task. `bx <ds> roast|grind` plan in
memory and run; `--plan PATH` writes the plan and runs nothing;
`bx percolate PLAN` runs or tracks a saved plan, locally or as
scheduler jobs whose payload is `bx percolate PLAN --task <id>`.

* `plan`: `Plan`, `PlanTask`, `PlanDataset`, `TaskScope`, `Resources`,
  `read_plan`, `write_plan`, `plan_dumper`, `resolve_roots`,
  `check_strings`;
* `options`: `ExecutionOptions` (the option group shared by roast,
  grind and percolate), `Profile`, `load_profile`, `resolve_resources`,
  `translate` (path maps);
* `jobs`: `Job`, `Scheduler`, `make_jobs`, `write_jobs`, `submit_jobs`;
* `run`: `StagePlanner`, `PLANNERS`, `percolate`, `run_task`,
  `plan_status`, `watch_plan`, `cancel_plan`, `list_plans`,
  `PlanStatus`.

Layering: `percolate/` builds on `actions/` and `bids/` and is
stage-agnostic: it imports neither `roast/` nor `grind/`, which
register their planners by name (`PLANNERS`). It imports no optional
dependency at module level (PyYAML is imported when a plan is read or
written).
"""
from brainspresso.percolate.jobs import (
    JOB_STATES,
    SCHEDULERS,
    Job,
    JobState,
    Scheduler,
    get_scheduler,
    make_jobs,
    submit_jobs,
    write_jobs,
)
from brainspresso.percolate.options import (
    BACKENDS,
    PROFILES_SECTION,
    SCRATCH_VARIABLES,
    STAGING,
    ExecutionOptions,
    Profile,
    load_profile,
    resolve_resources,
    translate,
)
from brainspresso.percolate.plan import (
    PLAN_SCHEMA,
    PLANS_DIR,
    YAML_PITFALLS,
    Plan,
    PlanDataset,
    PlanError,
    PlanTask,
    Resources,
    TaskScope,
    check_strings,
    plan_dumper,
    plan_path,
    read_plan,
    resolve_roots,
    write_plan,
)
from brainspresso.percolate.run import (
    PLANNERS,
    PlanStatus,
    StagePlanner,
    cancel_plan,
    get_planner,
    list_plans,
    percolate,
    plan_status,
    run_task,
    watch_plan,
)

__all__ = [
    "BACKENDS", "JOB_STATES", "PLANNERS", "PLANS_DIR", "PLAN_SCHEMA",
    "PROFILES_SECTION", "SCHEDULERS", "SCRATCH_VARIABLES", "STAGING",
    "YAML_PITFALLS", "ExecutionOptions", "Job", "JobState", "Plan",
    "PlanDataset", "PlanError", "PlanStatus", "PlanTask", "Profile",
    "Resources", "Scheduler", "StagePlanner", "TaskScope",
    "cancel_plan", "check_strings", "get_planner",
    "get_scheduler", "list_plans", "load_profile", "make_jobs",
    "percolate", "plan_dumper", "plan_path", "plan_status", "read_plan",
    "resolve_resources", "resolve_roots", "run_task", "submit_jobs",
    "translate", "watch_plan", "write_jobs", "write_plan",
]
