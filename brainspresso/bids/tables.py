"""
Helpers for BIDS tables.

Filter participants with simple queries, and trim tables so that they
only reference what has been downloaded (so that participant/session/scan
tables describe the local subset).
"""
from __future__ import annotations

import csv
import io
import operator
import re
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterable

lg = getLogger(__name__)

_OPS = {
    "==": operator.eq, "!=": operator.ne,
    ">=": operator.ge, "<=": operator.le,
    ">": operator.gt, "<": operator.lt,
}
_QUERY = re.compile(r"^\s*([^\s=!<>]+)\s*(==|!=|>=|<=|>|<|=)\s*(.*?)\s*$")


def read_tsv(text: str) -> tuple[list[str], list[dict]]:
    """Parse a TSV table: header, rows."""
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader.fieldnames or []), list(reader)


def write_tsv(path: Path, header: list[str], rows: Iterable[dict]) -> None:
    """Write rows (dictionaries) as a TSV table."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, header, delimiter="\t", lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_query(query: str):
    """
    Parse a query of the form `column OP value`.

    OP is one of `==, !=, >=, <=, >, <` (`=` is an alias for `==`).
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
    Return the labels of the participants that match all queries.

    Labels are without `sub-`; `text` is the content of a
    `participants.tsv` file.
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
    sessions: bool = False,
) -> list[str]:
    """
    Write trimmed copies of BIDS tables in `root`.

    Only the rows that refer to something present on disk are kept
    (and, with `sessions`, the `session_id` of a phenotype table that
    names no session on disk is `n/a`, `known_sessions`).

    Parameters
    ----------
    root : Path
        Dataset folder (`layout.root`) or BIDS root
    originals : dict[str, Path]
        Mapping from table path (relative to root) to the untouched
        downloaded copy (which may be the table itself).
    subjects : Path, default=folder of the participants table
        Folder that contains the `sub-*` folders listed in
        `participants.tsv`.
    sessions : bool
        Set the `session_id` of phenotype rows that name no session on
        disk to `n/a` (the pristine copies of roasters: `trim_originals`,
        compost's `retrim_tables`; harvested tables are left as they are)

    Returns
    -------
    written : list[str]
        Relative paths of the tables written.
    """
    written = []
    for relpath, original in sorted(originals.items()):
        if not original.exists():
            continue
        header, rows = read_tsv(original.read_text(encoding="utf-8"))
        name = Path(relpath).name
        folder = (root / relpath).parent
        if name == "participants.tsv":
            base = Path(subjects) if subjects is not None else folder
            rows = [r for r in rows
                    if (base / r.get("participant_id", "")).is_dir()]
        elif is_phenotype(relpath):
            base = Path(subjects) if subjects is not None else folder.parent
            rows = [r for r in rows
                    if (base / r.get("participant_id", "")).is_dir()]
            if sessions and "session_id" in header:
                rows = known_sessions(rows, base, relpath)
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


def known_sessions(rows: list[dict], subjects: Path,
                   relpath: str = "") -> list[dict]:
    """
    Set the `session_id` of phenotype rows to n/a if not on disk.

    A `session_id` names a session of the dataset: one that the
    participant does not have in `subjects` (`sub-<label>/ses-<label>/`)
    is `n/a` (e.g. OASIS-III visits on a day without a session, or a
    session not roasted). The pristine copy keeps it, so that a later
    trim brings it back once the session is on disk.
    """
    out, unknown = [], 0
    for row in rows:
        ses = row.get("session_id") or "n/a"
        if ses != "n/a" and not (
                subjects / row.get("participant_id", "") / ses).is_dir():
            row = {**row, "session_id": "n/a"}
            unknown += 1
        out.append(row)
    if unknown:
        lg.info(f"{relpath}: session_id n/a in {unknown} of {len(rows)} "
                f"rows (no such session on disk)")
    return out


def is_phenotype(relpath: str) -> bool:
    """
    Whether a table is a phenotype table (`[<raw>/]phenotype/<name>.tsv`).

    `trim_tables` trims its rows to the subjects on disk, by
    `participant_id`, as those of the participants table. Which tables
    reach it: those a roaster writes a pristine copy of
    (`.brainspresso/orig/`: `trim_originals`, compost's
    `retrim_tables`), and those a hosted harvest trims
    (`is_trimmable`): a harvested `phenotype/<name>_sessions.tsv` is
    trimmed by `participant_id` too (it was trimmed by `session_id`
    against `phenotype/ses-*`, which dropped every row). Other
    phenotype tables of hosted harvests are not trimmed; the metadata
    roast of hosted datasets does not use this function
    (`bids.hosted.trim_rows`, by file name, unchanged).

    Parameters
    ----------
    relpath : str
        Path of the table (POSIX), relative to a dataset

    Returns
    -------
    bool
    """
    path = PurePosixPath(relpath)
    return path.suffix == ".tsv" and path.parent.name == "phenotype"


def is_trimmable(relpath: str) -> bool:
    """Whether a table is trimmed (participants, sessions, scans)."""
    name = Path(relpath).name
    return (
        relpath == "participants.tsv" or
        name.endswith("_sessions.tsv") or
        name.endswith("_scans.tsv")
    )
