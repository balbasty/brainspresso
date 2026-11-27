"""
Run and track plans: `bx percolate`.

Interfaces only (see `brainspresso.percolate`).

* `bx percolate PLAN [execution options]`: run the tasks that are not
  done, on the local pool or as scheduler jobs (`--write-jobs DIR`:
  write the jobs only);
* `--retry failed`: also run again the tasks recorded "failed" or
  "skipped", and what depends on them;
* `--status`: report the tasks from their records, reconciled with the
  scheduler (a record "pending" or "running" whose job is gone becomes
  "failed");
* `--cancel`: cancel its jobs and record their tasks "failed";
* `--task ID`, `--array JOB --index N`: run one task, in this process
  (what jobs call);
* no argument: list the plans of the dataset and their state.

One lock per plan: every form but `--task`, `--array` and a read-only
`--status` holds an exclusive lock on the plan file, so one process at
a time submits, retries or cancels a plan.

Running a task calls the planner of its stage (`StagePlanner.run`),
restricted to its scope. The task takes the locks of its stage and its
own unit lock, checks its inputs and its signature again, and refuses a
plan that no longer matches (`PlanError`: reported, never followed
blindly). A task whose upstream task is still "pending" or "running"
is not run and stays "pending": the next `bx percolate PLAN` runs it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, Sequence, runtime_checkable

from brainspresso.percolate.options import ExecutionOptions, Profile
from brainspresso.percolate.plan import Plan, PlanTask

__all__ = ["PLANNERS", "PlanStatus", "StagePlanner", "cancel_plan",
           "get_planner", "list_plans", "percolate", "plan_status",
           "run_task"]

#: Planner of each stage, "module:attribute", loaded on first use
PLANNERS: dict[str, str] = {
    "roast": "brainspresso.roast.plan:planner",
    "grind": "brainspresso.grind.run:planner",
}


@runtime_checkable
class StagePlanner(Protocol):
    """
    What a stage gives percolate: how to plan, and how to run one task.

    Roast: one task per top-level shard that has an id (a subject;
    nested shards are found when it runs, as today), a head task for
    the actions of the top-level roaster itself, and a finalize task
    (hooks, trimmed tables, manifest). Grind: one task per (stage,
    unit), the stage found by pipeline name and version.

    Attributes
    ----------
    stage : str
        "roast" or "grind"
    """

    stage: str

    def run(self, plan: Plan, task: PlanTask, profile: Profile) -> str:
        """
        Run one task of a plan, in this process.

        The stage is built again from the plan (dataset, options,
        configuration) and restricted to the scope of the task, on the
        roots that `percolate.plan.resolve_roots` gives for this
        machine.

        Parameters
        ----------
        plan : Plan
            The plan
        task : PlanTask
            The task
        profile : Profile
            Container runtime, scratch, lock wait

        Returns
        -------
        str
            Status written in its record: "done", "failed", "skipped"
            or "pending" (an upstream task is not over)

        Raises
        ------
        PlanError
            If a root is missing here or holds another dataset, or if
            the plan no longer matches: a pipeline of another
            version, another image digest, a signature that differs
            from the plan's
        """
        ...

    def locks(self, plan: Plan, task: PlanTask) -> list[tuple[Path, str]]:
        """
        Return the run locks that a task holds shared while it runs.

        Parameters
        ----------
        plan : Plan
            The plan
        task : PlanTask
            The task

        Returns
        -------
        list[tuple[Path, str]]
            `(state folder, lock name)` in the order they are taken
            (parents before sites): `run` for roast, `grind` for grind
        """
        ...


@dataclass
class PlanStatus:
    """
    State of a plan, from the records of its tasks.

    Attributes
    ----------
    plan : Plan | None
        The plan
    status : {"planned", "running", "complete", "partial", "stale"}
        "partial": some tasks failed or were skipped and none is
        running; "stale": the plan no longer matches the dataset
    counts : dict[str, int]
        `{task status: n}` ("todo" for tasks without a record)
    failures : dict[str, str]
        `{task id: error}` of failed and skipped tasks
    jobs : dict[str, str]
        `{task id: scheduler job id}` of tasks that have one
    unknown : dict[str, str]
        Tasks recorded "pending" or "running" that cannot be checked
        (a local run on another host): `{task id: host}`
    """

    plan: Plan | None = None
    status: str = "planned"
    counts: dict[str, int] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)
    jobs: dict[str, str] = field(default_factory=dict)
    unknown: dict[str, str] = field(default_factory=dict)


def get_planner(stage: str) -> StagePlanner:
    """
    Return the planner of a stage.

    Parameters
    ----------
    stage : str
        A key of `PLANNERS`

    Returns
    -------
    StagePlanner
        Its planner

    Raises
    ------
    KeyError
        If the stage has no planner
    """
    raise NotImplementedError


def percolate(
    plan: Plan | str | Path,
    options: ExecutionOptions | None = None,
    *,
    retry: Sequence[str] = (),
    write_jobs: str | Path | None = None,
    detach: bool = False,
    display: bool = True,
) -> PlanStatus:
    """
    Run a plan: the body of `bx percolate PLAN`.

    Under the lock of the plan. Tasks already done (same signature) are
    left; the others run in dependency order, on the local pool (each
    worker runs `run_task`) or as scheduler jobs (`make_jobs`,
    `submit_jobs`). A plan in memory can only run locally: a scheduler
    backend saves it first.

    Parameters
    ----------
    plan : Plan | str | Path
        The plan, or its file
    options : ExecutionOptions | None
        Backend, profile, limits and resource overrides
    retry : Sequence[str]
        Record statuses to run again ("failed" covers "skipped")
    write_jobs : str | Path | None
        Write the jobs in this folder and submit nothing
    detach : bool
        Scheduler backends: return once the jobs are submitted instead
        of waiting for them
    display : bool
        Show the progress table

    Returns
    -------
    PlanStatus
        State of the plan when it returns

    Raises
    ------
    LockError
        If another process holds the lock of the plan
    PlanError
        If the plan is stale
    """
    raise NotImplementedError


def run_task(plan: Plan | str | Path, task: str, *,
             options: ExecutionOptions | None = None) -> str:
    """
    Run one task: the body of `bx percolate PLAN --task ID`.

    Also of `--array JOB --index N`, once the task id is read from the
    job manifest. Takes no plan lock.

    Parameters
    ----------
    plan : Plan | str | Path
        The plan, or its file
    task : str
        Task id
    options : ExecutionOptions | None
        Profile and container runtime (as given when it was submitted)

    Returns
    -------
    str
        Status written in the record of the task
    """
    raise NotImplementedError


def plan_status(plan: Plan | str | Path, *,
                reconcile: bool = True) -> PlanStatus:
    """
    Report the state of a plan: the body of `--status`.

    Parameters
    ----------
    plan : Plan | str | Path
        The plan, or its file
    reconcile : bool
        Ask the scheduler about recorded jobs and record as "failed"
        the tasks whose job ended without finishing them (needs the
        lock of the plan; skipped, read-only, when it is held)

    Returns
    -------
    PlanStatus
        State of the plan
    """
    raise NotImplementedError


def cancel_plan(plan: Plan | str | Path) -> PlanStatus:
    """
    Cancel the jobs of a plan: the body of `--cancel`.

    Tasks recorded "pending" or "running" become "failed" ("cancelled"),
    so that `--retry failed` runs them again; also those that cannot be
    checked.

    Parameters
    ----------
    plan : Plan | str | Path
        The plan, or its file

    Returns
    -------
    PlanStatus
        State of the plan afterwards
    """
    raise NotImplementedError


def list_plans(state: str | Path) -> list[PlanStatus]:
    """
    List the plans of a dataset: the body of `bx percolate` alone.

    Parameters
    ----------
    state : str | Path
        State folder of the dataset

    Returns
    -------
    list[PlanStatus]
        One entry per plan in `<state>/plans/`, newest first, not
        reconciled
    """
    raise NotImplementedError


