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
        Apptainer image file. A reference enters task signatures as
        it is declared; a file enters them as `file:<SHA-256 of the
        file>`, never as its path (which the path map may translate)
    digest : str | None
        Registry manifest digest of the image (`sha256:...`), the same
        whatever runtime runs it (for a local image file: the SHA-256
        of the file). It is recorded in task records and `GeneratedBy`
        and compared when known, apart from the task signature; found
        by
        `ContainerRuntime.inspect` or `pull` when None
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

    def inspect(self, container: Container) -> str | None:
        """
        Return the registry digest of an image without pulling it.

        From `container.digest`, the local image store (Docker), or
        the digest that the runtime kept next to the image file it
        converted (Apptainer); never a digest of the local copy. Used
        when planning (dry runs included): nothing is
        downloaded or written.

        Parameters
        ----------
        container : Container
            The image

        Returns
        -------
        str | None
            Its digest; None when it is not known locally (a task is
            then never stale because of its image)
        """
        ...

    def pull(self, container: Container) -> Container:
        """
        Make an image available locally.

        Docker pulls it; Apptainer builds an image file in the cache.
        Done once, when tasks are about to run, never by a dry run or
        when planning. The plan is not changed: percolate records the
        digests once next to it (`percolate.plan.write_images`), and
        every task runs that image or refuses.

        Parameters
        ----------
        container : Container
            The image

        Returns
        -------
        Container
            The same, with its registry `digest`; `image` is unchanged
            (where the local copy is kept is the runtime's business)
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
