import os
from collections.abc import Iterable
from pathlib import Path, PurePosixPath


def get_tree_path(path: str | Path | None = None) -> Path:
    """
    If path not set, return the default path stored in the environment
    variable `BDP_PATH`. If unset, return current folder.
    """
    return Path(path or os.environ.get('BDP_PATH', '.'))


def posix_key(path: str | os.PathLike) -> PurePosixPath:
    """
    Sort key that orders paths the same way on every platform.

    `sorted(paths)` compares Windows paths case-insensitively (`anat1`
    before `OAS3...`, the reverse of POSIX); this key compares their
    POSIX parts case-sensitively, as `sorted` does on POSIX.
    """
    return PurePosixPath(Path(path).as_posix())


def sorted_paths(paths: Iterable[str | os.PathLike]) -> list:
    """
    Sort paths in the same order on every platform (`posix_key`).

    Listings (`glob`, `iterdir`) that a roaster turns into inputs,
    actions or records go through this, so that shard signatures and
    task records do not depend on the platform.
    """
    return sorted(paths, key=posix_key)
