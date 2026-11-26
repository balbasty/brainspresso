"""
The INDI / Functional Connectomes Project bucket: `s3://fcp-indi`.

Hosts ABIDE I & II, ADHD-200, CoRR and many other INDI projects, under
`data/Projects/<PROJECT>/`. The bucket is public: it is listed and read
anonymously, without credentials.

```python
from brainspresso.sources.fcp import FCP

source = FCP("ABIDE/RawData", provides={"raw"})
source.harvest(layout, selection)
```
"""
from __future__ import annotations

from logging import getLogger

from brainspresso.sources.base import RemoteEntry
from brainspresso.sources.s3 import S3Bucket, S3Prefix

lg = getLogger(__name__)

BUCKET = "fcp-indi"
PROJECTS = "data/Projects"


def get_bucket() -> S3Bucket:
    """Return the fcp-indi bucket (a function, so that tests can patch it)."""
    return S3Bucket(BUCKET)


def _bucket() -> S3Bucket:
    return get_bucket()


class FCP(S3Prefix):
    """
    Source: a folder of `s3://fcp-indi/data/Projects/`.

    Parameters
    ----------
    prefix : str
        Path relative to `data/Projects`, e.g. "ABIDE/RawData".
    """

    name = "s3"

    def __init__(self, prefix: str = "", **kwargs):
        super().__init__(_bucket, f"{PROJECTS}/{prefix.strip('/')}",
                         **kwargs)


def list_entries(prefix: str) -> list[RemoteEntry]:
    """
    List all objects under `data/Projects/<prefix>` (synchronous).

    Parameters
    ----------
    prefix : str
        Path relative to `data/Projects`.

    Returns
    -------
    list[RemoteEntry]
        Entries, with paths relative to the prefix.
    """
    return FCP(prefix).entries()
