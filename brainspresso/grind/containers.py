"""
Containers: images, mounts and the runtimes that run them.

Interfaces only (see `brainspresso.grind`). A stage names a `Container`;
the profile names the runtime: "docker" (local default), "apptainer"
(clusters; images are converted once into a cache) or "none" (the tools
are installed on the host). Raw data is mounted read-only, the
derivative and scratch folders read-write, and the container runs as
the calling user.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Mapping, Protocol, Sequence, runtime_checkable

__all__ = ["RUNTIMES", "Container", "ContainerRuntime", "Mount",
           "get_runtime"]

#: Names of the container runtimes
RUNTIMES = ("docker", "apptainer", "none")


@dataclass(frozen=True)
class Container:
    """
    A container image.

    Attributes
    ----------
    image : str
        Image reference (`nipreps/fmriprep:23.2.1`), or the path of an
        Apptainer image file
    digest : str | None
        Content digest (`sha256:...`), part of task signatures and of
        `GeneratedBy`; resolved by `ContainerRuntime.pull` when None
    env : Mapping[str, str]
        Environment variables set in the container
    gpu : bool
        Whether GPUs are passed to it
    """

    image: str
    digest: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    gpu: bool = False


@dataclass(frozen=True)
class Mount:
    """
    A host folder made visible in a container.

    Attributes
    ----------
    host : Path
        Folder (or file) on the host
    guest : str
        Its path in the container
    mode : {"ro", "rw"}
        Read-only (inputs) or read-write (outputs, scratch)
    """

    host: Path
    guest: str
    mode: Literal["ro", "rw"] = "ro"


@runtime_checkable
class ContainerRuntime(Protocol):
    """
    Something that runs a command in a container.

    Attributes
    ----------
    name : str
        One of `RUNTIMES`
    """

    name: str

    def available(self) -> bool:
        """
        Tell whether the runtime can be used on this machine.

        Returns
        -------
        bool
            Whether its executable is found
        """
        ...

    def pull(self, container: Container) -> Container:
        """
        Make an image available locally.

        Docker pulls it; Apptainer builds an image file in the cache.
        Done once, before tasks are submitted.

        Parameters
        ----------
        container : Container
            The image

        Returns
        -------
        Container
            The same, with its `digest` (and local `image`) resolved
        """
        ...

    def command(
        self,
        container: Container | None,
        argv: Sequence[str],
        *,
        mounts: Sequence[Mount] = (),
        env: Mapping[str, str] | None = None,
        workdir: str | None = None,
    ) -> list[str]:
        """
        Build the host command line that runs a command in a container.

        Parameters
        ----------
        container : Container | None
            The image (None, or the "none" runtime: `argv` unchanged,
            with host paths)
        argv : Sequence[str]
            Command, with paths as seen in the container
        mounts : Sequence[Mount]
            Folders to mount
        env : Mapping[str, str] | None
            Environment variables, over those of the container
        workdir : str | None
            Working directory in the container

        Returns
        -------
        list[str]
            argv to run on the host
        """
        ...


def get_runtime(name: str) -> ContainerRuntime:
    """
    Return a container runtime by name.

    Parameters
    ----------
    name : str
        One of `RUNTIMES`

    Returns
    -------
    ContainerRuntime
        The runtime

    Raises
    ------
    ValueError
        If the name is unknown
    """
    raise NotImplementedError
