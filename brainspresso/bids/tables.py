"""
Helpers for BIDS tables: filter participants with simple queries, and trim
tables so that they only reference what has been downloaded (so that
participant/session/scan tables describe the local subset).
"""
from __future__ import annotations

import csv
import io
import operator
import re
from pathlib import Path
from typing import Iterable

_OPS = {
    "==": operator.eq, "!=": operator.ne,
    ">=": operator.ge, "<=": operator.le,
    ">": operator.gt, "<": operator.lt,
}
_QUERY = re.compile(r"^\s*([^\s=!<>]+)\s*(==|!=|>=|<=|>|<|=)\s*(.*?)\s*$")


def read_tsv(text: str) -> tuple[list[str], list[dict]]:
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader.fieldnames or []), list(reader)


def write_tsv(path: Path, header: list[str], rows: Iterable[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, header, delimiter="\t", lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_query(query: str):
    """
    Parse a query of the form `column OP value`, with OP in
    `==, !=, >=, <=, >, <` (`=` is an alias for `==`).
    Values are compared as numbers when both sides are numeric,
    and as (case-insensitive) strings otherwise. Rows where the value
    is missing (`n/a`) never match.
    """
    match = _QUERY.match(query)
    if not match:
        raise ValueError(
            f"Invalid query {query!r}. Expected `column OP value` "
            f"with OP in {list(_OPS)}"
        )
    column, op, value = match.groups()
    op = _OPS["==" if op == "=" else op]
    value = value.strip("'\"")

    def predicate(row: dict) -> bool:
        if column not in row:
            raise KeyError(
                f"Column {column!r} not in participants.tsv. "
                f"Available columns: {', '.join(row)}"
            )
        cell = (row[column] or "").strip()
        if cell in ("", "n/a"):
            return False
        try:
            return op(float(cell), float(value))
        except ValueError:
            return op(cell.lower(), value.lower())

    return predicate


def query_participants(text: str, queries: Iterable[str]) -> set[str]:
    """
    Return the labels (without `sub-`) of participants that match all
    queries, given the content of a `participants.tsv` file.
    """
    _, rows = read_tsv(text)
    predicates = [parse_query(q) for q in queries]
    out = set()
    for row in rows:
        if all(p(row) for p in predicates):
            label = row.get("participant_id", "")
            out.add(label[4:] if label.startswith("sub-") else label)
    return out


def trim_tables(
    root: Path,
    originals: dict[str, Path],
    subjects: Path | None = None,
) -> list[str]:
    """
    Write trimmed copies of BIDS tables in `root`, keeping only rows that
    refer to something present on disk.

    Parameters
    ----------
    root : Path
        BIDS root
    originals : dict[str, Path]
        Mapping from table path (relative to root) to the untouched
        downloaded copy (which may be the table itself).
    subjects : Path, default=root
        Folder that contains the `sub-*` folders listed in
        `participants.tsv` (e.g. `rawdata/`, if the table lives
        next to it).

    Returns
    -------
    written : list[str]
        Relative paths of the tables written.
    """
    subjects = Path(subjects if subjects is not None else root)
    written = []
    for relpath, original in sorted(originals.items()):
        if not original.exists():
            continue
        header, rows = read_tsv(original.read_text())
        name = Path(relpath).name
        folder = (root / relpath).parent
        if name == "participants.tsv":
            rows = [r for r in rows
                    if (subjects / r.get("participant_id", "")).is_dir()]
        elif name.endswith("_sessions.tsv"):
            rows = [r for r in rows
                    if (folder / r.get("session_id", "")).is_dir()]
        elif name.endswith("_scans.tsv"):
            rows = [r for r in rows
                    if (folder / r.get("filename", "")).exists()]
        else:
            continue
        (root / relpath).parent.mkdir(parents=True, exist_ok=True)
        write_tsv(root / relpath, header, rows)
        written.append(relpath)
    return written


def is_trimmable(relpath: str) -> bool:
    name = Path(relpath).name
    return (
        relpath == "participants.tsv" or
        name.endswith("_sessions.tsv") or
        name.endswith("_scans.tsv")
    )
