"""
Tables of the ABIDE phenotypic CSV files (shared by ABIDE-I and II).

Each table (`participants.tsv`, `phenotype/<name>.tsv`) is one mapping
of columns (`Table.columns`: BIDS name -> CSV column, or alternative
CSV columns) and of values (`Table.values`). Cells are read by the name
of their column in each file, so files may order (or lack) columns
differently; a column that a file lacks is `n/a` in its rows. Column
names are matched without case and whitespace (`ADI_R_A3_Total` is
`ADI_R_A3_TOTAL`, `RBSR_5SUBSCALE_RITUAL   ISTIC` is
`RBSR_5SUBSCALE_RITUALISTIC`).

```python
table = Table({"participant_id": "SUB_ID", "sex": "SEX"},
              {"sex": {"1": "M", "2": "F"}})
write_table(table, ["phenotypic_CALTECH.csv"], "participants.tsv")
```
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import Iterable, Iterator, Mapping

from brainspresso.utils.io import write_tsv

lg = getLogger(__name__)

__all__ = [
    "NANS",
    "WIDTH",
    "Table",
    "column_key",
    "participant_label",
    "read_csv",
    "write_table",
]

WIDTH = 5               # subject labels: `sub-51456`

#: Values that mean "missing" (written `n/a`)
NANS = frozenset({'', 'n/a', 'N/A', 'nan', 'NaN', 'NAN', 'na', 'NA',
                  '-9999', '-9999.0', '-9999.00'})

#: Encodings of the CSV files, tried in order (some are not UTF-8)
ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")

_warned: set = set()


def _warn(path: Path, line: int, message: str) -> None:
    """Log a warning about a row once (every table reads the files)."""
    key = (str(path), path.stat().st_mtime_ns, line, message)
    if key not in _warned:
        _warned.add(key)
        lg.warning(f"{path.name}, line {line}: {message}")


def participant_label(value: str) -> str:
    """
    Label of a subject (numbers zero-padded to 5 digits).

    Parameters
    ----------
    value : str
        Subject identifier of a CSV file, an archive or a folder

    Returns
    -------
    str
        `"51456"`, `"0051456"` -> `"51456"`; other values stripped, as
        they are
    """
    value = str(value).strip()
    if value.startswith("sub-"):
        value = value[4:]
    return f"{int(value):0{WIDTH}d}" if value.isdigit() else value


def column_key(name: str) -> str:
    """Key of a CSV column name (upper case, without whitespace)."""
    return re.sub(r"\s+", "", name).upper()


def read_csv(path: Path) -> list[list[str]]:
    """
    Read the rows of a CSV file (header first).

    Parameters
    ----------
    path : path
        CSV file, in UTF-8 (with or without BOM), else Windows-1252

    Returns
    -------
    list[list[str]]
    """
    data = Path(path).read_bytes()
    for encoding in ENCODINGS:
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if encoding != ENCODINGS[0]:
        lg.debug(f"{Path(path).name}: read as {encoding}")
    return list(csv.reader(io.StringIO(text, newline="")))


def _cell(value: str) -> str:
    """Collapse the whitespace (line breaks, tabs) of a cell."""
    return re.sub(r"\s*[\r\n\t]+\s*", " ", value).strip()


@dataclass(frozen=True)
class Table:
    """
    A table derived from the phenotypic CSV files.

    Parameters
    ----------
    columns : dict[str, str | tuple[str]]
        BIDS column -> CSV column (`participant_id` first); a tuple
        lists alternative names, the first one a file has is read
    values : dict[str, dict | callable]
        BIDS column -> value mapping (values not in a dict are kept)
    """

    columns: Mapping[str, str | tuple[str, ...]]
    values: Mapping[str, object] = field(default_factory=dict)

    def names(self, column: str) -> tuple[str, ...]:
        """
        CSV names of a BIDS column.

        Parameters
        ----------
        column : str
            BIDS column

        Returns
        -------
        tuple[str]
        """
        names = self.columns[column]
        return (names,) if isinstance(names, str) else tuple(names)

    def convert(self, column: str, value: str) -> str:
        """
        Convert one cell.

        Parameters
        ----------
        column : str
            BIDS column
        value : str
            CSV cell

        Returns
        -------
        str
            `n/a` for missing values
        """
        value = _cell(value)
        if value in NANS:
            return "n/a"
        mapping = self.values.get(column)
        if callable(mapping):
            return mapping(value)
        if mapping is not None:
            return mapping.get(value, value)
        return value

    def rows(self, paths: Iterable[Path]) -> Iterator[list[str]]:
        """
        Rows of the table (header first) from CSV files.

        Blank rows are skipped; rows without a subject, or that do not
        reach the subject column, are skipped with a warning, as are the
        later rows of a subject listed twice.

        Parameters
        ----------
        paths : list[path]
            Phenotypic CSV files

        Yields
        ------
        list[str]
            The header, then one row per subject
        """
        yield list(self.columns)
        seen = set()
        for path in paths:
            path = Path(path)
            rows = read_csv(path)
            header = rows[0] if rows else []
            index = {}
            for i, name in enumerate(header):
                index.setdefault(column_key(name), i)

            def find(column, index=index):
                return next((index[column_key(n)] for n in self.names(column)
                             if column_key(n) in index), None)

            where = {column: find(column) for column in self.columns}
            sub = where["participant_id"]
            if sub is None:
                lg.warning(f"{path.name}: no column "
                           f"{' or '.join(self.names('participant_id'))}: "
                           f"skipped")
                continue
            missing = [c for c, i in where.items() if i is None]
            if missing:
                lg.debug(f"{path.name}: no column(s) for {missing} (n/a)")
            for line, row in enumerate(rows[1:], start=2):
                if not any(cell.strip() for cell in row):
                    continue
                if len(row) <= sub or not row[sub].strip():
                    _warn(path, line, "row skipped (no subject)")
                    continue
                label = participant_label(row[sub])
                if not label.isalnum():
                    _warn(path, line, f"row skipped (subject {label!r} is "
                                      f"not a label)")
                    continue
                if label in seen:
                    _warn(path, line, f"row skipped (subject {label} "
                                      f"listed twice)")
                    continue
                seen.add(label)
                out = [f"sub-{label}"]
                for column in list(self.columns)[1:]:
                    i = where[column]
                    cell = row[i] if i is not None and i < len(row) else ""
                    out.append(self.convert(column, cell))
                yield out


def write_table(table: Table, paths: list[Path], path_tsv) -> None:
    """
    Write a table (every subject of the CSV files).

    Parameters
    ----------
    table : Table
        The table
    paths : list[path]
        Phenotypic CSV files
    path_tsv : path
        Output table
    """
    write_tsv(table.rows(paths), path_tsv)
