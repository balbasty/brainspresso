"""
Execution backends: the submit protocol and user profiles.

Interfaces only (see `brainspresso.grind`); no backend lives here.
`Executor` extends the seam that roast already uses
(`actions.engine.LocalExecutor.submit(task, resources=None)`) with
dependencies and arrays:

* local pool: the engine holds a task until its `after` handles are
  done;
* submitit (SLURM, SGE, PBS): `afterok` for "ok" dependencies,
  `afterany` for "any"; tasks of one stage with equal resources form
  one job array, with pairwise dependencies between arrays where the
  scheduler has them (SLURM `aftercorr`);
* Dask and Ray (optional): handles passed as dependencies.

Dependencies are scheduling hints: every task checks its requirements
from the task states when it starts (`grind.graph`).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    runtime_checkable,
)

from brainspresso.grind.model import Resources

__all__ = ["BACKENDS", "After", "Executor", "Handle", "Profile",
           "load_profile", "make_executor"]

#: Names of the execution backends
BACKENDS = ("local", "slurm", "sge", "pbs", "dask", "ray")


@runtime_checkable
class Handle(Protocol):
    """
    A submitted task.

    `concurrent.futures.Future` and submitit jobs satisfy it.
    """

    def done(self) -> bool:
        """
        Tell whether the task has finished (or failed, or was cancelled).

        Returns
        -------
        bool
            Whether it is over
        """
        ...

    def result(self) -> Any:
        """
        Wait for the task and return its result.

        Returns
        -------
        Any
            What the task returned (its exception is raised)
        """
        ...

    def exception(self) -> BaseException | None:
        """
        Wait for the task and return its exception.

        Returns
        -------
        BaseException | None
            None if it succeeded
        """
        ...

    def cancel(self) -> bool:
        """
        Cancel the task if it has not started.

        Returns
        -------
        bool
            Whether it was cancelled
        """
        ...


@dataclass(frozen=True)
class After:
    """
    Dependencies of a submission on earlier ones.

    Attributes
    ----------
    handles : tuple[Handle, ...]
        Tasks to wait for
    condition : {"ok", "any"}
        "ok": they must succeed (SLURM `afterok`), else the task is not
        started; "any": they must be over (`afterany`)
    pairwise : bool
        Between two arrays of the same length: element `i` waits for
        element `i` only (SLURM `aftercorr`; other backends wait for
        all)
    """

    handles: tuple[Handle, ...]
    condition: Literal["ok", "any"] = "ok"
    pairwise: bool = False


@runtime_checkable
class Executor(Protocol):
    """
    The submit protocol of the engine, with dependencies and arrays.

    `actions.engine.LocalExecutor` is its local backend (step 7 adds
    `after` and `submit_array` to it).
    """

    def submit(
        self,
        task: Callable[[], Any],
        resources: dict | None = None,
        after: Sequence[After] = (),
    ) -> Handle:
        """
        Submit a task.

        Parameters
        ----------
        task : callable() -> Any
            Picklable callable
        resources : dict | None
            `Resources.as_dict()` (a backend ignores what it cannot
            honour)
        after : Sequence[After]
            Dependencies

        Returns
        -------
        Handle
            The submitted task
        """
        ...

    def submit_array(
        self,
        tasks: Sequence[Callable[[], Any]],
        resources: dict | None = None,
        after: Sequence[After] = (),
    ) -> list[Handle]:
        """
        Submit tasks that share resources, as one job array if possible.

        Parameters
        ----------
        tasks : Sequence[callable() -> Any]
            Picklable callables
        resources : dict | None
            Resources of each task
        after : Sequence[After]
            Dependencies of every task (`pairwise`: of each on its
            counterpart)

        Returns
        -------
        list[Handle]
            One handle per task, in order
        """
        ...

    def shutdown(self) -> None:
        """Release the backend (wait for or detach from its tasks)."""
        ...


@dataclass(frozen=True)
class Profile:
    """
    Where and how tasks run: one entry of the user configuration file.

    Attributes
    ----------
    name : str
        Name (`--profile`)
    backend : str
        One of `BACKENDS`
    runtime : str
        Container runtime, one of `grind.containers.RUNTIMES`
    jobs : int | None
        Local pool: workers; schedulers: most tasks running at once
        (array throttle)
    options : Mapping[str, Any]
        Backend options (partition or queue, account, QOS, modules)
    resources : Mapping[str, Mapping[str, Any]]
        Resource overrides per `"<pipeline>/<stage>"` (glob patterns)
    max_array : int | None
        Largest job array (larger ones are split)
    detach : bool
        Submit and exit instead of waiting for the tasks
    wait : float
        Seconds a task waits for the grind lock of its dataset
    scratch : Path | None
        Parent of the scratch folders of tasks (default: the node's
        temporary folder)
    image_cache : Path | None
        Where Apptainer images are kept
    """

    name: str = "local"
    backend: str = "local"
    runtime: str = "docker"
    jobs: int | None = None
    options: Mapping[str, Any] = field(default_factory=dict)
    resources: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    max_array: int | None = None
    detach: bool = False
    wait: float = 0
    scratch: Path | None = None
    image_cache: Path | None = None

    def resources_for(self, pipeline: str, stage: str,
                      default: Resources) -> Resources:
        """
        Resources of the tasks of a stage under this profile.

        Parameters
        ----------
        pipeline : str
            Name of the pipeline
        stage : str
            Name of the stage
        default : Resources
            Resources that the stage (and the dataset) ask for

        Returns
        -------
        Resources
            `default` with the overrides of the profile
        """
        raise NotImplementedError


def load_profile(name: str | None = None,
                 path: str | Path | None = None) -> Profile:
    """
    Read a profile from the user configuration file.

    Parameters
    ----------
    name : str | None
        Name of the profile (None: the default one, else a local pool
        with Docker)
    path : str | Path | None
        Configuration file (default: the user's)

    Returns
    -------
    Profile
        The profile

    Raises
    ------
    KeyError
        If the file has no profile of this name
    """
    raise NotImplementedError


def make_executor(profile: Profile) -> Executor:
    """
    Build the backend of a profile.

    Parameters
    ----------
    profile : Profile
        The profile

    Returns
    -------
    Executor
        Its backend

    Raises
    ------
    ModuleNotFoundError
        If the backend needs a package that is not installed
    """
    raise NotImplementedError
