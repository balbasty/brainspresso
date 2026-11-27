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
from pathlib import Path
from typing import Iterable

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
    present: Iterable[str] = (),
) -> list[str]:
    """
    Write trimmed copies of BIDS tables in `root`.

    Only the rows that refer to something present on disk are kept.

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
    present : iterable of str
        Files (relative to `root`, POSIX) that count as present although
        they are not on disk (selected, not fetched); their folders
        count as present too.

    Returns
    -------
    written : list[str]
        Relative paths of the tables written.
    """
    root = Path(root)
    files = {str(p) for p in present}
    dirs = {"/".join(p.split("/")[:i]) for p in files
            for i in range(1, p.count("/") + 1)}

    def here(path: Path, folder: bool) -> bool:
        if (path.is_dir() if folder else path.exists()):
            return True
        try:
            rel = path.relative_to(root).as_posix()
        except ValueError:
            return False
        return rel in (dirs if folder else files)

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
                    if here(base / r.get("participant_id", ""), True)]
        elif name.endswith("_sessions.tsv"):
            rows = [r for r in rows
                    if here(folder / r.get("session_id", ""), True)]
        elif name.endswith("_scans.tsv"):
            rows = [r for r in rows
                    if here(folder / r.get("filename", ""), False)]
        else:
            continue
        (root / relpath).parent.mkdir(parents=True, exist_ok=True)
        write_tsv(root / relpath, header, rows)
        written.append(relpath)
    return written


def is_trimmable(relpath: str) -> bool:
    """Whether a table is trimmed (participants, sessions, scans)."""
    name = Path(relpath).name
    return (
        relpath == "participants.tsv" or
        name.endswith("_sessions.tsv") or
        name.endswith("_scans.tsv")
    )
