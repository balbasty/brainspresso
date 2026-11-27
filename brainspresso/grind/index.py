"""
Datasets of a cross-dataset run: references, index and conditions.

Interfaces only (see `brainspresso.grind`).

A dataset **reference** is `<Module.Path>[/<argument>...][/<site>]`:
the name of the dataset, then its positional arguments
(`Dataset.arguments`), then at most one site: `IXI`,
`OpenNeuro/ds000224`, `ABIDE.I/NYU`. The name is the module path of
the dataset under `brainspresso.datasets` (`ABIDE.I`, not `abide-1`),
as in the sections of the config file (separate PR,
`claude/feat/config-file`, which adds it to the catalog); it is
case-insensitive. A `*`
in an argument or a site matches what is on disk (`OpenNeuro/*`). A
path (absolute, or starting with `./` or `../`) names a BIDS dataset
outside the catalog. The canonical form is the `dataset` of units, task
records and index rows.

The **index** has one row per subject (and session) on disk, with the
columns of each raw dataset's `participants.tsv` and `sessions.tsv`.
`--where` conditions select rows.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from brainspresso.grind.graph import DatasetScope

__all__ = ["WHERE_OPERATORS", "DatasetRef", "Where", "build_index",
           "parse_where", "resolve_datasets"]

#: Operators of `--where` conditions (`in`: one of a comma-separated
#: list; `~`: regular expression)
WHERE_OPERATORS = ("==", "!=", "<=", ">=", "<", ">", "in", "~")


@dataclass(frozen=True)
class DatasetRef:
    """
    Reference of a dataset: `<Module.Path>[/<argument>...][/<site>]`.

    Attributes
    ----------
    name : str
        Module path of the dataset, in its canonical case (`ABIDE.I`),
        or "" for a path
    arguments : tuple[str, ...]
        Values of the positional arguments of the dataset
    site : str | None
        Site of a dataset with one dataset per site
    path : Path | None
        Folder of a dataset outside the catalog
    """

    name: str
    arguments: tuple[str, ...] = ()
    site: str | None = None
    path: Path | None = None

    @classmethod
    def parse(cls, text: str) -> "DatasetRef":
        """
        Parse a reference.

        The parts after the name are arguments up to the number that
        the dataset declares; one more is a site.

        Parameters
        ----------
        text : str
            E.g. "OpenNeuro/ds000224"

        Returns
        -------
        DatasetRef
            The reference (patterns kept as they are)

        Raises
        ------
        KeyError
            If no dataset has this name
        ValueError
            If it has too many or too few parts
        """
        raise NotImplementedError

    def __str__(self) -> str:
        """Return the canonical form (`parse` reads it back)."""
        raise NotImplementedError


def resolve_datasets(
    refs: Sequence[str | DatasetRef],
    root: str | Path | None = None,
    **selection: Any,
) -> list[DatasetScope]:
    """
    Resolve references into the datasets of a run.

    Patterns are expanded against what is on disk; a dataset with one
    dataset per site gives one entry per site; each entry gets the
    layout, the overrides (`Dataset.grind`) and the selection of its
    dataset (labels formatted by the dataset, e.g. zero-padded).

    Parameters
    ----------
    refs : Sequence[str | DatasetRef]
        References (`--datasets`)
    root : str | Path | None
        Folder of all datasets (default: the configured one)
    **selection
        `subs`, `exclude_subs`, `sessions`... of the command line

    Returns
    -------
    list[DatasetScope]
        One entry per dataset, sorted by reference, without duplicates

    Raises
    ------
    DatasetNotFound
        If a reference without pattern is not on disk
    """
    raise NotImplementedError


@dataclass(frozen=True)
class Where:
    """
    One `--where` condition: `<column> <operator> <value>`.

    Attributes
    ----------
    column : str
        Column of the index
    op : str
        One of `WHERE_OPERATORS`
    value : str
        Value to compare with (numbers compare as numbers)
    """

    column: str
    op: str
    value: str

    def matches(self, row: dict[str, str]) -> bool:
        """
        Tell whether a row of the index fulfils the condition.

        Parameters
        ----------
        row : dict[str, str]
            `{column: value}`

        Returns
        -------
        bool
            False when the column is missing or "n/a"
        """
        raise NotImplementedError


def parse_where(text: str) -> Where:
    """
    Parse a `--where` condition.

    Parameters
    ----------
    text : str
        E.g. "age >= 60", "sex == F", "dataset in IXI,OASIS.III"

    Returns
    -------
    Where
        The condition

    Raises
    ------
    ValueError
        If it is not `<column> <operator> <value>`
    """
    raise NotImplementedError


def build_index(
    datasets: Sequence[DatasetScope],
    where: Sequence[Where] = (),
) -> list[dict[str, str]]:
    """
    Build the cross-dataset index.

    One row per subject (per session when the dataset has sessions)
    present in the raw dataset and kept by its selection, with
    `dataset`, `participant_id`, `session_id` and the union of the
    columns of the participants and sessions tables ("n/a" where
    missing). Several conditions are combined with "and".

    Parameters
    ----------
    datasets : Sequence[DatasetScope]
        The datasets
    where : Sequence[Where]
        Conditions that rows must fulfil

    Returns
    -------
    list[dict[str, str]]
        Rows, sorted by dataset, subject and session
    """
    raise NotImplementedError
