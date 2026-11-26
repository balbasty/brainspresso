"""
A common read-only interface for tarballs and folders of files.

Datasets that used to be distributed as tarballs (e.g. `IXI-T1.tar`) are
now often served file by file. Harvesting them into a folder named like
the tarball (e.g. `IXI-T1/`) and reading it through `FolderArchive`
lets the same conversion code handle both.
"""
from __future__ import annotations

import tarfile
from pathlib import Path
from typing import IO


class FolderArchive:
    """Minimal `tarfile.TarFile`-like view of a flat folder of files"""

    class Member:
        def __init__(self, name: str) -> None:
            self.name = name

    def __init__(self, path: str | Path, suffixes=(".nii.gz", ".nii")):
        self.path = Path(path)
        self.name = str(self.path)
        self.suffixes = tuple(suffixes)

    def getnames(self) -> list[str]:
        return sorted(
            p.name for p in self.path.iterdir()
            if p.is_file() and p.name.endswith(self.suffixes)
        )

    def getmembers(self) -> list["FolderArchive.Member"]:
        return [self.Member(name) for name in self.getnames()]

    def __iter__(self):
        return iter(self.getmembers())

    def getmember(self, name: str) -> "FolderArchive.Member":
        if not (self.path / name).is_file():
            raise KeyError(name)
        return self.Member(name)

    def extractfile(self, member) -> IO[bytes]:
        return open(self.path / getattr(member, "name", member), "rb")

    def sources(self, *names: str) -> list[Path]:
        """Files on disk that hold these members"""
        return [self.path / name for name in names]

    def __enter__(self) -> "FolderArchive":
        return self

    def __exit__(self, *args) -> None:
        pass


class TarArchive:
    """`tarfile.TarFile` with the `sources` method of `FolderArchive`"""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.tar = tarfile.open(self.path)
        self.name = self.tar.name

    def getnames(self) -> list[str]:
        return self.tar.getnames()

    def getmembers(self) -> list[tarfile.TarInfo]:
        return self.tar.getmembers()

    def __iter__(self):
        """Members in archive order, read lazily: extracting each member
        while iterating reads a compressed tarball only once"""
        return iter(self.tar)

    def getmember(self, name: str) -> tarfile.TarInfo:
        return self.tar.getmember(name)

    def extractfile(self, member) -> IO[bytes]:
        return self.tar.extractfile(member)

    def sources(self, *names: str) -> list[Path]:
        return [self.path]

    def __enter__(self) -> "TarArchive":
        return self

    def __exit__(self, *args) -> None:
        self.tar.close()


def open_archive(
    tar: str | Path, folder: str | Path
) -> TarArchive | FolderArchive | None:
    """Open the tarball if it exists, else the folder, else None"""
    if Path(tar).exists():
        return TarArchive(tar)
    if Path(folder).is_dir():
        return FolderArchive(folder)
    return None
