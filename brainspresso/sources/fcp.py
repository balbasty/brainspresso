"""
The INDI / Functional Connectomes Project bucket: `s3://fcp-indi`.

Hosts ABIDE I & II, ADHD-200, CoRR and many other INDI projects, under
`data/Projects/<PROJECT>/`. The bucket is public: it is listed and read
anonymously, without credentials.

```python
from brainspresso.sources import fcp

for downloader in fcp.mirror("ABIDE/RawData/Caltech", dst):
    ...
```
"""
from __future__ import annotations

from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator

from brainspresso.download import CHUNK_SIZE, Downloader, IfExists
from brainspresso.sources.s3 import S3Bucket, S3Object

lg = getLogger(__name__)

BUCKET = "fcp-indi"
PROJECTS = "data/Projects"


def get_bucket() -> S3Bucket:
    """The fcp-indi bucket (a function, so that tests can patch it)"""
    return S3Bucket(BUCKET)


def list_objects(prefix: str) -> list[S3Object]:
    """
    List all objects under `data/Projects/<prefix>` (synchronous).

    Parameters
    ----------
    prefix : str
        Path relative to `data/Projects`, e.g. "ABIDE/RawData".
    """
    prefix = f"{PROJECTS}/{prefix.strip('/')}/"
    return get_bucket().list_all(prefix)


def mirror(
    prefix: str,
    dst: str | Path,
    keep: Callable[[PurePosixPath], bool] | None = None,
    *,
    rename: Callable[[PurePosixPath], PurePosixPath | None] | None = None,
    ifexists: IfExists.Choice = "skip",
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[Downloader]:
    """
    Yield one downloader per object under `data/Projects/<prefix>`,
    saved under `dst` with the same relative path.

    Size, mtime and MD5 (from single-part ETags) come from the listing,
    so downloads need no extra request and are verified.

    Parameters
    ----------
    prefix : str
        Path relative to `data/Projects`, e.g. "ABIDE/RawData".
    dst : Path
        Local folder that mirrors the prefix.
    keep : callable(PurePosixPath) -> bool
        Filter on the path relative to the prefix.
    rename : callable(PurePosixPath) -> PurePosixPath | None
        Map the relative path to a different local path (relative to
        `dst`). Return None to skip the object.
    ifexists : {"skip", "overwrite", "different", "refresh", "error"}
        Behaviour if a local file already exists.
    chunk_size : int
        Download chunk size.
    """
    bucket = get_bucket()
    root = f"{PROJECTS}/{prefix.strip('/')}/"
    dst = Path(dst)
    for obj in bucket.list_all(root):
        rel = PurePosixPath(obj.key[len(root):])
        if not obj.key[len(root):] or obj.key.endswith("/"):
            continue
        if rel.name.startswith("."):        # .DS_Store and the like
            continue
        if keep is not None and not keep(rel):
            continue
        local = rename(rel) if rename else rel
        if local is None:
            continue
        yield Downloader(
            bucket.url(obj.key),
            dst / local,
            size=obj.size,
            mtime=obj.mtime,
            digests={"md5": obj.md5} if obj.md5 else None,
            ifexists=ifexists,
            chunk_size=chunk_size,
        )
