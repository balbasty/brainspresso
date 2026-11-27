"""
Kinds of the files of a BIDS dataset, from their names.

The action core (`actions.Action`) carries an opaque `kind`; the roast
layer (`roast.policy`) computes it with `default_kind` when an action does
not set one, and the JSON policy (`--sidecars yes|no|only`) is defined
on these kinds.

```python
default_kind("sub-01/anat/sub-01_T1w.nii.gz")    # "data"
default_kind("sub-01/anat/sub-01_T1w.json")      # "sidecar"
default_kind("participants.tsv")                 # "table"
default_kind("sub-01/dwi/sub-01_dwi.bval")       # "meta"
```
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Literal

__all__ = ["KINDS", "Kind", "default_kind", "kinds_of"]

Kind = Literal["data", "sidecar", "meta", "table"]
#: Every kind
KINDS: tuple[str, ...] = ("data", "sidecar", "meta", "table")

# Dataset-level JSON files that describe tables or the dataset, not data
# files (kind "table", so that `--sidecars no` does not drop them)
_TABLE_JSON_SUFFIXES = ("participants", "sessions", "scans", "samples")
_DATASET_JSON = ("dataset_description.json", "genetic_info.json")
_DATASET_FILES = (
    "README", "README.md", "README.txt", "README.rst", "CHANGES",
    "LICENSE", "LICENSE.md", "LICENSE.txt", "CITATION.cff", ".bidsignore",
)


def default_kind(path: str | os.PathLike) -> Kind:
    """
    Kind of a file of a BIDS dataset, from its name.

    * `*.tsv`, dataset files (`README`, `.bidsignore`...) and the JSON
      files that describe tables or the dataset (`participants.json`,
      `sub-*_sessions.json`, `phenotype/*.json`,
      `dataset_description.json`): "table";
    * other `*.json`: "sidecar";
    * `*.bval`, `*.bvec`: "meta" (metadata that is not JSON: written
      under every sidecar policy, as `bids.select` treats them as
      metadata of the DWI series);
    * anything else (images, surfaces...): "data".

    Parameters
    ----------
    path : str | PathLike
        Path of the file (only its name and folder name are used)

    Returns
    -------
    {"data", "sidecar", "meta", "table"}
        Its kind
    """
    path = Path(path)
    name = path.name
    if name.endswith((".bval", ".bvec")):
        return "meta"
    if name.endswith(".tsv") or name in _DATASET_FILES:
        return "table"
    if name.endswith(".json"):
        stem = name[:-len(".json")]
        if (
            name in _DATASET_JSON or
            stem.split("_")[-1] in _TABLE_JSON_SUFFIXES or
            path.parent.name == "phenotype"
        ):
            return "table"
        return "sidecar"
    return "data"


def kinds_of(paths: Iterable[str | os.PathLike]) -> set[Kind]:
    """
    Default kinds of a set of files.

    Parameters
    ----------
    paths : iterable of str | PathLike
        Files

    Returns
    -------
    set of {"data", "sidecar", "meta", "table"}
        Their kinds (`default_kind`)
    """
    return set(map(default_kind, paths))
