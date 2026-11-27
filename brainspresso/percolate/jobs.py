"""
Scheduler jobs: batch scripts that run tasks of a plan.

Interfaces only (see `brainspresso.percolate`).

The payload of a job is plain data: a batch script whose command is
`bx percolate PLAN --task <key>`, or, for a job array, `bx percolate
PLAN --array <job> --index <n>` with the array index of the scheduler.
No callable is pickled, so scripts can be read, checked and submitted
by hand. Backends are small native templates over the scheduler's own
commands (`sbatch --parsable`, `qsub`); there is no submitit backend:
what submitit adds is pickling callables and fetching their results,
which this design does not use, and it does not cover SGE and PBS.

Dependencies. A job waits for the **end** of the jobs it depends on,
not for their success: tasks check their requirements when they start,
and a must-succeed dependency on an array would leave every dependent
queued forever after one failed element. A must-succeed dependency
(`Job.after`) is only used in two cases: on a whole job, when each task
of this job requires every task of that job; element-wise on an aligned
array (`Job.pairwise`), when element `n` requires element `n`. The four
kinds reach `Scheduler.submit_command` apart:

* "any" (wait for the end): SLURM `afterany`, SGE `-hold_jid`, PBS
  `depend=afterany`;
* "ok" (must succeed): SLURM `afterok` with
  `--kill-on-invalid-dep=yes`; SGE `-hold_jid` (it has no must-succeed
  form: the task checks); PBS `depend=afterok` between single jobs,
  `afterany` for arrays;
* "pairwise_any" (element `n` waits for the end of element `n`): SGE
  `-hold_jid_ad`; SLURM and PBS have no such form: whole array,
  `afterany`;
* "pairwise_ok" (element `n` needs element `n` to succeed): SLURM
  `aftercorr` with `--kill-on-invalid-dep=yes`; SGE `-hold_jid_ad`
  (the task checks); PBS whole array, `afterany`.

`write_jobs` writes, in one folder: one script per job, `manifest.json`
(jobs, their tasks and dependencies) and `submit.sh`, which submits
them in order, feeds the captured job ids into the dependencies and
appends them to `submitted.tsv`. `submit_jobs` does the same from
Python and writes the job ids into the task records. Tasks write their
own record when they run, so a plan submitted by hand is tracked too.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping, Protocol, Sequence, runtime_checkable

from brainspresso.percolate.options import ExecutionOptions, Profile
from brainspresso.percolate.plan import Plan, Resources

__all__ = ["JOB_STATES", "SCHEDULERS", "Job", "JobState", "Scheduler",
           "get_scheduler", "make_jobs", "submit_jobs", "write_jobs"]

#: Backends that submit to a scheduler
SCHEDULERS = ("slurm", "sge", "pbs")
#: State of a job, as its scheduler reports it
JobState = Literal["queued", "running", "done", "failed", "cancelled",
                   "unknown"]
#: The job states ("unknown": the scheduler no longer knows the job)
JOB_STATES = ("queued", "running", "done", "failed", "cancelled",
              "unknown")


@dataclass(frozen=True)
class Job:
    """
    One submission: a task, or an array of tasks.

    Attributes
    ----------
    name : str
        Name in the plan (`j0003`): script `<name>.sh`, `--array` value
    tasks : tuple[str, ...]
        Task keys (`PlanTask.key`); several: a job array, element `n`
        runs `tasks[n]`
    resources : Resources
        Resources of each task (`resolve_resources`)
    after : tuple[str, ...]
        Names of jobs that must succeed first: jobs of which every
        task is required by every task of this one, or aligned arrays
        (in `pairwise`) whose element `n` element `n` requires
    after_any : tuple[str, ...]
        Names of jobs that must be over first: the default for a
        dependency between jobs
    pairwise : tuple[str, ...]
        Names of jobs in `after` or `after_any` that this array is
        aligned with: element `n` only waits for their element `n`.
        In `after`: "pairwise_ok"; in `after_any`: "pairwise_any"
    """

    name: str
    tasks: tuple[str, ...]
    resources: Resources = Resources()
    after: tuple[str, ...] = ()
    after_any: tuple[str, ...] = ()
    pairwise: tuple[str, ...] = ()


@runtime_checkable
class Scheduler(Protocol):
    """
    A scheduler backend: templates and command lines, no state.

    Attributes
    ----------
    name : str
        One of `SCHEDULERS`
    array_index : str
        Shell expression of the array index in a job
        (`$SLURM_ARRAY_TASK_ID`)
    pairwise : bool
        Whether the scheduler has element-wise array dependencies
        (SLURM `aftercorr`, SGE `-hold_jid_ad`); otherwise they become
        whole-array ones
    must_succeed : bool
        Whether it has a must-succeed dependency (not SGE); otherwise
        "ok" dependencies become "any" ones
    """

    name: str
    array_index: str
    pairwise: bool
    must_succeed: bool

    def script(self, job: Job, command: Sequence[str],
               profile: Profile) -> str:
        """
        Render the batch script of a job.

        Parameters
        ----------
        job : Job
            The job (resources, array size)
        command : Sequence[str]
            The `bx percolate ...` argv
        profile : Profile
            Partition, account, directives, setup lines, throttle,
            path map (the plan file and the options are named as the
            nodes see them)

        Returns
        -------
        str
            The script: directives, `profile.setup`, then the command
        """
        ...

    def submit_command(self, script: str,
                       after: Mapping[str, Sequence[str]] | None = None
                       ) -> list[str]:
        """
        Build the command line that submits a script.

        It prints the job id alone (`sbatch --parsable`).

        Parameters
        ----------
        script : str
            Path of the script
        after : Mapping[str, Sequence[str]] | None
            Scheduler job ids per kind: "any" (wait for the end), "ok"
            (must succeed), "pairwise_any", "pairwise_ok". A kind the
            scheduler lacks is mapped to the nearest that never waits
            for success wrongly: "pairwise_any" to a whole-array "any"
            (never to an element-wise must-succeed form), "ok" and
            "pairwise_ok" to "any" where there is no must-succeed
            form

        Returns
        -------
        list[str]
            argv
        """
        ...

    def query(self, ids: Sequence[str]) -> dict[str, JobState]:
        """
        Ask the scheduler for the state of jobs.

        Parameters
        ----------
        ids : Sequence[str]
            Scheduler job ids (array elements as the scheduler names
            them)

        Returns
        -------
        dict[str, JobState]
            State of each ("unknown" when it has forgotten the job)
        """
        ...

    def cancel_command(self, ids: Sequence[str]) -> list[str]:
        """
        Build the command line that cancels jobs.

        Parameters
        ----------
        ids : Sequence[str]
            Scheduler job ids

        Returns
        -------
        list[str]
            argv
        """
        ...


def get_scheduler(name: str) -> Scheduler:
    """
    Return a scheduler backend by name.

    Parameters
    ----------
    name : str
        One of `SCHEDULERS`

    Returns
    -------
    Scheduler
        The backend

    Raises
    ------
    ValueError
        If the name is unknown
    """
    raise NotImplementedError


def make_jobs(plan: Plan, profile: Profile,
              options: ExecutionOptions | None = None,
              tasks: Sequence[str] | None = None) -> list[Job]:
    """
    Group the tasks to run into jobs.

    Tasks of one stage with equal resources and no dependency on each
    other form one array, split at `max_array`. Two arrays are aligned
    (`Job.pairwise`) only if they are built here together from the same
    ordered list of scopes and split at the same places, so that
    element `n` depends on element `n` and on no other; equal lengths
    are not enough. Any other dependency is on the whole job, and waits
    for its end (`Job.after_any`) unless every task of the job requires
    every task of the other, or element `n` requires element `n` of
    an aligned array (`Job.after`).

    Parameters
    ----------
    plan : Plan
        The plan
    profile : Profile
        Limits and resources
    options : ExecutionOptions | None
        Options of the command line
    tasks : Sequence[str] | None
        Keys of the tasks to run (default: those of `plan.todo()`)

    Returns
    -------
    list[Job]
        Jobs, each after those it depends on
    """
    raise NotImplementedError


def write_jobs(plan: Plan, jobs: Sequence[Job], profile: Profile,
               folder: str | Path) -> Path:
    """
    Write the batch scripts of jobs, without submitting them.

    The body of `bx percolate PLAN --write-jobs DIR`.

    Parameters
    ----------
    plan : Plan
        The plan (saved: scripts name its file)
    jobs : Sequence[Job]
        The jobs
    profile : Profile
        Scheduler, directives, setup lines
    folder : str | Path
        Where to write

    Returns
    -------
    Path
        `<folder>/submit.sh`; next to it, `<job>.sh` and
        `manifest.json`

    Raises
    ------
    ValueError
        If the profile has no scheduler backend, or the plan no file
    """
    raise NotImplementedError


def submit_jobs(plan: Plan, jobs: Sequence[Job], profile: Profile,
                folder: str | Path | None = None) -> dict[str, str]:
    """
    Write the scripts of jobs and submit them.

    Each task gets a "pending" record with its job id (merged into the
    record that exists).

    Parameters
    ----------
    plan : Plan
        The plan (saved)
    jobs : Sequence[Job]
        The jobs
    profile : Profile
        Scheduler, directives, setup lines
    folder : str | Path | None
        Where scripts go (default: a new folder next to the plan)

    Returns
    -------
    dict[str, str]
        `{job name: scheduler job id}`
    """
    raise NotImplementedError
