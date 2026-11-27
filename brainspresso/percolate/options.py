"""
Execution options: where and how tasks run.

Interfaces only (see `brainspresso.percolate`).

`ExecutionOptions` is one option group, shared by `bx <ds> roast`,
`bx <ds> grind` and `bx percolate`, so that a cluster run is one
command. Named profiles are the `[profiles.<name>]` sections
(`PROFILES_SECTION`) of the config file (separate PR:
`~/.brainspresso/config.toml`, then the root and dataset files,
`--config FILE`, environment and the command line, later ones
winning); this module only says which fields a profile has.

Path translation: the login node, the compute nodes and containers
may see the same data under different mount points. A profile carries
an ordered path map (`[[profiles.<name>.paths]]` with `from` and `to`;
`--path-map SRC=DST`), applied to the roots of the plan, to the plan
file and to the files that options name, when job scripts are written
and when a job reads the plan (`translate`).

Staging (`--stage`): "none" (default) runs tasks in place; "inputs"
copies the inputs of a task to node-local scratch first; "all" also
has tools write there, their outputs being committed to the dataset
afterwards through the action layer. Grind tasks only
(`grind.staging`).

Fixed in the plan: the tasks, their dependencies, the resources that
stages declare and those that datasets override. Chosen when the plan
is run: the backend, the profile, the container runtime, limits and
resource overrides. Resources of a task: stage declaration < profile <
dataset overrides < command line (`resolve_resources`).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from brainspresso.percolate.plan import PlanTask, Resources

__all__ = ["BACKENDS", "PROFILES_SECTION", "SCRATCH_VARIABLES", "STAGING",
           "ExecutionOptions", "Profile", "load_profile",
           "resolve_resources", "translate"]

#: Names of the execution backends (`--backend`)
BACKENDS = ("local", "slurm", "sge", "pbs")
#: Values of `--stage`
STAGING = ("none", "inputs", "all")
#: Environment variables tried for the default scratch folder of a
#: job, in order (then the temporary folder of the node)
SCRATCH_VARIABLES = ("SLURM_TMPDIR", "TMPDIR", "TMP")
#: Section of the config file that holds the profiles, by name
PROFILES_SECTION = "profiles"


@dataclass(frozen=True)
class ExecutionOptions:
    """
    The execution options of the command line.

    None means "not given": the profile, then the defaults, decide.

    Attributes
    ----------
    profile : str | None
        `--profile NAME`
    backend : str | None
        `--backend`, one of `BACKENDS`
    jobs : int | None
        `--jobs`: workers of the local pool
    partition : str | None
        `--partition` (alias `--queue`)
    account : str | None
        `--account`
    time : str | None
        `--time`: time limit of each task (`[D-]HH:MM[:SS]` or minutes)
    mem : str | None
        `--mem`: memory of each task (`16G`)
    cpus : int | None
        `--cpus`
    gpus : int | None
        `--gpus`
    max_array : int | None
        `--max-array`: largest job array
    max_running : int | None
        `--max-running`: most tasks running at once (array throttle)
    directives : tuple[str, ...]
        `--directive` (repeatable): raw scheduler lines, added to every
        batch script as they are
    container_runtime : str | None
        `--container-runtime`: "docker", "apptainer" or "none"
    path_map : tuple[str, ...]
        `--path-map SRC=DST` (repeatable), before those of the profile
    stage : str | None
        `--stage`, one of `STAGING`
    scratch : str | None
        `--scratch PATH`; may name a variable of the node
        (`$SLURM_TMPDIR`), expanded in the job
    keep_scratch_on_failure : bool
        `--keep-scratch-on-failure`
    detach : bool | None
        `--detach` (True) or `--wait` (False): one option, two
        spellings. None: the profile, else the default of the backend
    """

    profile: str | None = None
    backend: str | None = None
    jobs: int | None = None
    partition: str | None = None
    account: str | None = None
    time: str | None = None
    mem: str | None = None
    cpus: int | None = None
    gpus: int | None = None
    max_array: int | None = None
    max_running: int | None = None
    directives: tuple[str, ...] = ()
    container_runtime: str | None = None
    path_map: tuple[str, ...] = ()
    stage: str | None = None
    scratch: str | None = None
    keep_scratch_on_failure: bool = False
    detach: bool | None = None


@dataclass(frozen=True)
class Profile:
    """
    Where and how tasks run: a named profile, with the options applied.

    Attributes
    ----------
    name : str
        Name of the profile
    backend : str
        One of `BACKENDS`
    container_runtime : str
        "docker", "apptainer" or "none"
    jobs : int | None
        Workers of the local pool (None: the CPUs of the machine)
    partition, account : str | None
        Scheduler partition (or queue) and account
    resources : Resources | None
        Defaults for every task (time, memory, CPUs, GPUs)
    stages : Mapping[str, Mapping[str, Any]]
        Resource fields per task id pattern
        (`{"grind/fs-long/cross/*": {"memory_gb": 32}}`)
    max_array, max_running : int | None
        Largest job array; most tasks running at once
    directives : tuple[str, ...]
        Raw scheduler lines of every batch script
    setup : tuple[str, ...]
        Shell lines run before the task in a batch script (modules,
        environment activation)
    executable : str
        Command that runs brainspresso on the nodes (`bx`)
    detach : bool | None
        `detach = true|false`: return once tasks are started instead
        of waiting for them. None: wait on the local backend, detach
        on schedulers. The command line wins over it. Waiting is
        monitoring only: stopping it never affects the jobs
    lock_wait : float
        Seconds a task waits for the locks of its datasets
    paths : tuple[tuple[str, str], ...]
        Ordered path map: `(from, to)` prefixes, the first that matches
        a path is applied
    stage : str
        One of `STAGING`
    keep_scratch_on_failure : bool
        Leave the scratch folder of a failed task
    scratch : str | None
        Parent of the scratch folders of tasks, expanded on the node
        (None: the first of `SCRATCH_VARIABLES` that is set)
    image_cache : Path | None
        Where Apptainer images are kept
    """

    name: str = "local"
    backend: str = "local"
    container_runtime: str = "docker"
    jobs: int | None = None
    partition: str | None = None
    account: str | None = None
    resources: Resources | None = None
    stages: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    max_array: int | None = None
    max_running: int | None = None
    directives: tuple[str, ...] = ()
    setup: tuple[str, ...] = ()
    executable: str = "bx"
    detach: bool | None = None
    lock_wait: float = 0
    paths: tuple[tuple[str, str], ...] = ()
    stage: str = "none"
    keep_scratch_on_failure: bool = False
    scratch: str | None = None
    image_cache: Path | None = None


def load_profile(options: ExecutionOptions | None = None,
                 config: Mapping[str, Any] | None = None) -> Profile:
    """
    Build the profile of a run: the named profile, then the options.

    Parameters
    ----------
    options : ExecutionOptions | None
        Options of the command line (None: the local default)
    config : Mapping[str, Any] | None
        The merged config (separate PR), whose `profiles` section is
        read (None: no named profile exists)

    Returns
    -------
    Profile
        The profile, options applied (`time`, `mem`, `cpus` and `gpus`
        are not: they override per task, see `resolve_resources`)

    Raises
    ------
    KeyError
        If the config has no profile of this name
    ValueError
        If the backend or the runtime is unknown
    """
    raise NotImplementedError


def resolve_resources(task: PlanTask, profile: Profile,
                      options: ExecutionOptions | None = None) -> Resources:
    """
    Return the resources that a task runs with.

    Precedence, lowest first: what the stage declares
    (`task.resources`), the profile (`resources`, then the `stages`
    patterns that match), what the dataset overrides
    (`task.overrides`), the command line (`time`, `mem`, `cpus`,
    `gpus`).

    Parameters
    ----------
    task : PlanTask
        The task
    profile : Profile
        The profile
    options : ExecutionOptions | None
        Options of the command line

    Returns
    -------
    Resources
        The resources
    """
    raise NotImplementedError


def translate(path: str | Path,
              paths: Sequence[tuple[str, str]]) -> Path:
    """
    Translate a path of the planning machine into one of this machine.

    Parameters
    ----------
    path : str | Path
        Absolute path, as the machine that planned sees it
    paths : Sequence[tuple[str, str]]
        Ordered `(from, to)` prefixes; the first whose `from` is a
        parent of the path (whole components) is applied

    Returns
    -------
    Path
        The translated path (unchanged if no prefix matches)
    """
    raise NotImplementedError
