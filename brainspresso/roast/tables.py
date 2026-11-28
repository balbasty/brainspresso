"""
Tables of the roasters: source tables read, cells cleaned, BIDS tables written.

Layer 1, used by every roaster:

* `read_csv`, `read_xlsx`, `read_xls` read a source table into a
  `Sheet`: its header, its rows (blank rows dropped, each with its line
  or row number), columns found by name (`Sheet.find`, `require`,
  `locate`: without case or whitespace, aliases allowed, first match
  wins). A table that cannot be read, or lacks a required column, is a
  `TableError` (its message starts with the file name); so is a missing
  optional dependency (with the `pip install brainspresso[<extra>]`
  hint).
* `clean` makes one cell safe for an unquoted TSV table: `None` and NaN
  are `n/a` (and so are the `nans` spellings), runs of tabs and line
  breaks are one space, `"` is `'`, integral floats lose their decimals
  (`30.0` is `30`, `0.0` is `0`, `0.5` stays `0.5`).
* `table_action` is the action that writes a roasted table, by default
  its pristine copy in `.brainspresso/orig/` (trimmed to what is on disk
  after every roast, `bids.harvest.trim_originals`): every cell goes
  through `clean`, with one warning per table when a cell had to be
  changed.
* `warn_once`, `row_warning`: warnings that a roast gives once, however
  many tables read the same file.

Layer 2, for tables that are a pure mapping of columns (ABIDE): `Table`.

```python
sheet = read_xlsx(path, extra="oasis2")
sub = sheet.require(("Subject ID", "ID"))
rows = [["participant_id"], *([f"sub-{sheet.cell(row, sub)}"]
                              for _, row in sheet.rows)]
yield table_action(ctx, ctx.layout.raw / "participants.tsv", rows, path)
```

Two TSV writers, two roles: roasted tables are written unquoted by
`utils.io.write_tsv`, and never need quoting since their cells went
through `clean`; `bids.tables.write_tsv` is quote-aware, for trimming
tables that came quoted from elsewhere (a `"` written unquoted would be
quoted by the trim, and the visible table would differ from its
pristine copy: the reason `clean` replaces quotes).

Not migrated here: the tables of hosted datasets (`datasets/indi/engine.py`
`read_table`/`format_tsv`, `bids/hosted.py`), which have byte-exact tests
of their own.
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
from collections import Counter
from dataclasses import dataclass, field
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import IO, Callable, Iterable, Iterator, Mapping, Sequence

from brainspresso.actions.action import Action
from brainspresso.utils.io import write_tsv

lg = getLogger(__name__)

__all__ = [
    "ENCODINGS",
    "NANS",
    "Sheet",
    "Table",
    "TableError",
    "clean",
    "column_key",
    "integer",
    "numeric_columns",
    "read_csv",
    "read_xls",
    "read_xlsx",
    "row_warning",
    "sidecar_name",
    "table_action",
    "warn_once",
    "xls_date",
]

#: Values that mean "missing" (written `n/a`)
NANS = frozenset({'', 'n/a', 'N/A', 'nan', 'NaN', 'NAN', 'na', 'NA'})

#: Encodings of text tables, tried in order (some are not UTF-8)
ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")

#: Keys of the warnings already given (`warn_once`; cleared by
#: `scripts/golden.py` before each roast, as a process would start)
_warned: set = set()

_BREAKS = re.compile(r"\s*[\r\n\t]+\s*")


class TableError(ValueError):
    """A source table that cannot be read, or lacks a required column."""


# ----------------------------------------------------------------------
#   Warnings
# ----------------------------------------------------------------------

def warn_once(key, message: str) -> None:
    """
    Log a warning once per process (per `key` and message).

    Parameters
    ----------
    key : hashable
        What the warning is about (e.g. a file and its mtime, so that a
        new version of the file warns again)
    message : str
        The warning
    """
    key = (key, message)
    if key not in _warned:
        _warned.add(key)
        lg.warning(message)


def _file_key(path) -> tuple:
    """Version of a source file (path and mtime), for `warn_once`."""
    try:
        return (str(path), Path(path).stat().st_mtime_ns)
    except (OSError, TypeError, ValueError):
        return (str(path),)


def row_warning(sheet: Sheet, line: int, message: str, *,
                unit: str = "line") -> None:
    """
    Warn about a row of a source table, once.

    Parameters
    ----------
    sheet : Sheet
        The table
    line : int
        Line (CSV) or row (spreadsheet) number; 0: about the file
    message : str
        What is wrong (`"row skipped (no subject)"`)
    unit : str
        Word before the number (`"line"`, `"row"`)
    """
    where = f", {unit} {line}" if line else ""
    warn_once((sheet.key, line), f"{sheet.name}{where}: {message}")


# ----------------------------------------------------------------------
#   Cells
# ----------------------------------------------------------------------

def column_key(name) -> str:
    """Key of a column name (upper case, without whitespace)."""
    return re.sub(r"\s+", "", "" if name is None else str(name)).upper()


def _shortest(value: float) -> str:
    """Write a float in its shortest form (`30.0`: `30`; `1e+16` stays)."""
    if value.is_integer() and abs(value) < 1e16:
        return str(int(value))
    return repr(value)


def clean(value, *, nans: Iterable[str] = NANS, quotes: str = "'",
          counts: Counter | None = None, numeric: bool = False) -> str:
    """
    Make one cell safe for an unquoted TSV table.

    Parameters
    ----------
    value : object
        The cell (a string, a number, None...)
    nans : set[str]
        Spellings of a missing value, written `n/a` (after cleaning)
    quotes : str
        Replacement of `"`
    counts : Counter, optional
        Incremented: `"quotes"` if a `"` was replaced, `"breaks"` if
        tabs or line breaks were (for the warning of the table)
    numeric : bool
        The column is numeric (`numeric_columns`): text that reads as a
        finite number is written as a float would be (`"70.0"` is
        `70`, `"0.50"` is `0.5`); other text is kept

    Returns
    -------
    str
        `n/a` for None, NaN and `nans`; integral floats without
        decimals (`30.0` is `30`), other floats in their shortest form;
        text with each run of tabs and line breaks (and the spaces
        around it) made one space, `"` replaced by `quotes`, stripped
    """
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if math.isnan(value):
            return "n/a"
        value = _shortest(value)
    elif not isinstance(value, str):
        value = str(value)
    text = _BREAKS.sub(" ", value)
    if counts is not None and text != value:
        counts["breaks"] += 1
    if '"' in text:
        text = text.replace('"', quotes)
        if counts is not None:
            counts["quotes"] += 1
    text = text.strip()
    if numeric:
        try:
            number = float(text)
        except ValueError:
            number = None
        if number is not None and math.isfinite(number):
            text = _shortest(number)
    return "n/a" if text in nans else text


#: `Units` of a data dictionary that are a date or time format
#: (`YYYY-MM-DD`, `hh:mm:ss`, ISO 8601), not a unit of a number
_DATE_UNITS = re.compile(r"YYYY|hh:|:mm|ISO\s*8601|^(date|datetime|time|"
                         r"timestamp)$", re.IGNORECASE)
#: `Format` of a numeric column (BIDS data dictionaries)
NUMERIC_FORMATS = ("number", "integer")


def numeric_columns(sidecar: Mapping | None) -> set[str]:
    """
    Find the numeric columns of a table from its data dictionary.

    A column is numeric when it has no `Levels` and either a numeric
    `Format` (`number`, `integer`) or `Units` that are not a date or
    time format (and no other `Format`).

    Parameters
    ----------
    sidecar : dict | None
        The JSON sidecar of the table (`participants.json`...)

    Returns
    -------
    set[str]
        Names of its numeric columns
    """
    out = set()
    for column, entry in (sidecar or {}).items():
        if not isinstance(entry, Mapping) or entry.get("Levels"):
            continue
        form, units = entry.get("Format"), entry.get("Units")
        if form is not None:
            if str(form).lower() in NUMERIC_FORMATS:
                out.add(column)
        elif isinstance(units, str) and units.strip() and \
                not _DATE_UNITS.search(units.strip()):
            out.add(column)
    return out


def integer(value) -> int | None:
    """
    Read an integer cell (`1`, `1.0`, `"1"`, `" 1.0 "`).

    Parameters
    ----------
    value : object
        The cell

    Returns
    -------
    int | None
        The integer, or None if the cell is not an integral number
    """
    try:
        value = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return int(value) if value.is_integer() else None


def cleaned_message(name: str, counts: Mapping[str, int]) -> str | None:
    """
    Describe the cells of a table that `clean` changed.

    Parameters
    ----------
    name : str
        Name of the table
    counts : Counter
        Of `clean`

    Returns
    -------
    str | None
        The warning, None if nothing was changed
    """
    parts = []
    if counts.get("quotes"):
        parts.append(f"'\"' replaced by \"'\" in {counts['quotes']} cells")
    if counts.get("breaks"):
        parts.append(f"tabs or line breaks replaced by a space in "
                     f"{counts['breaks']} cells")
    if not parts:
        return None
    return f"{name}: {', '.join(parts)} (BIDS tables are not quoted)"


# ----------------------------------------------------------------------
#   Reading
# ----------------------------------------------------------------------

@dataclass
class Sheet:
    """
    A source table, read.

    Parameters
    ----------
    name : str
        Its name in messages (the file name)
    header : list[str]
        Its first row (as text; empty cells are `""`)
    rows : list[tuple[int, list]]
        `(line or row number, cells)` of the other rows; blank rows (only
        empty cells or whitespace) are dropped. CSV cells are text,
        spreadsheet cells what the library reads (numbers, None...)
    path : Path, optional
        Its file (None: read from a file object)
    datemode : int, optional
        Date mode of an xls workbook (`xlrd.xldate_as_tuple`)
    """

    name: str
    header: list[str]
    rows: list[tuple[int, list]]
    path: Path | None = None
    datemode: int | None = None
    index: dict[str, int] = field(init=False, repr=False)
    key: tuple = field(init=False, repr=False)

    def __post_init__(self):
        """Index the header (`find`) and version the file (warnings)."""
        self.index = {}
        for i, column in enumerate(self.header):
            self.index.setdefault(column_key(column), i)
        self.key = _file_key(self.path) if self.path else (self.name,)

    def find(self, names: str | Sequence[str],
             default: int | None = None) -> int | None:
        """
        Find a column by name.

        Parameters
        ----------
        names : str | tuple[str]
            Its name, or alternative names (the first one found wins);
            compared without case and whitespace
        default : int, optional
            Returned if none is found

        Returns
        -------
        int | None
            Its index
        """
        names = (names,) if isinstance(names, str) else names
        return next((self.index[column_key(n)] for n in names
                     if column_key(n) in self.index), default)

    def require(self, names: str | Sequence[str]) -> int:
        """
        Find a column that the table must have.

        Parameters
        ----------
        names : str | tuple[str]
            Its name or alternative names (`find`)

        Returns
        -------
        int
            Its index

        Raises
        ------
        TableError
            If the table has no such column
        """
        index = self.find(names)
        if index is None:
            names = (names,) if isinstance(names, str) else names
            raise TableError(f"{self.name}: no column "
                             f"{' or '.join(map(repr, names))}")
        return index

    def locate(self, columns: Sequence[str | Sequence[str]]) -> list[int]:
        """
        Find columns by name, else at their expected position.

        Parameters
        ----------
        columns : list[str | tuple[str]]
            Expected header (`find`: a name or alternative names per
            column), in its expected order

        Returns
        -------
        list[int]
            Index of each column: found by name, else its position in
            `columns` (one warning lists those)
        """
        index, missing = [], []
        for i, names in enumerate(columns):
            found = self.find(names)
            if found is None:
                found = i
                name = names if isinstance(names, str) else names[0]
                given = self.header[i] if i < len(self.header) else None
                missing.append(f"{name!r} (column {i + 1}: {given!r})")
            index.append(found)
        if missing:
            lg.warning(f"{self.name}: columns not found by name, read by "
                       f"position: {', '.join(missing)}")
        return index

    @staticmethod
    def cell(row: Sequence, index: int | None, default=""):
        """
        Read a cell of a row.

        Parameters
        ----------
        row : list
            Cells of the row
        index : int | None
            Column (`find`); None: the column is absent
        default : object
            Value past the end of the row, or of an absent column

        Returns
        -------
        object
            The cell
        """
        if index is None or index >= len(row):
            return default
        return row[index]


def _blank(row: Sequence) -> bool:
    """Whether a row has only empty cells."""
    return not any(c is not None and str(c).strip() for c in row)


def _sheet(name: str, rows: list[tuple[int, list]], path=None,
           **kwargs) -> Sheet:
    """Make a `Sheet` of numbered rows (header first)."""
    header = ["" if c is None else str(c) for c in rows[0][1]] \
        if rows else []
    body = [(n, row) for n, row in rows[1:] if not _blank(row)]
    return Sheet(name, header, body, path=Path(path) if path else None,
                 **kwargs)


def read_csv(src: str | Path | IO[bytes], *, delimiter: str = ",",
             encodings: Sequence[str] = ENCODINGS, name: str | None = None,
             **csv_options) -> Sheet:
    r"""
    Read a CSV (or TSV) table.

    Parameters
    ----------
    src : path | file object
        The file, or a binary file object (a member of an archive)
    delimiter : str
        Separator of cells (`"\t"`: a TSV table)
    encodings : list[str]
        Encodings tried in order; reading with another than the first
        one is logged (info)
    name : str, optional
        Name of the table in messages (default: the file name)
    **csv_options
        Other options of `csv.reader` (`quoting=csv.QUOTE_NONE`...)

    Returns
    -------
    Sheet
        Rows numbered by the physical line they start on (a quoted cell
        with a line break spans several lines). Cells are as stored: a
        line break inside a quoted cell stays `\r\n`, `\r` or `\n` (no
        normalisation here: `clean` makes any run of them one space)

    Raises
    ------
    TableError
        If the file cannot be read, decoded or parsed (read errors of a
        file object are not converted: they are its owner's)
    ValueError
        If `encodings` is empty
    """
    path = None if hasattr(src, "read") else Path(src)
    name = name or (path.name if path else
                    Path(getattr(src, "name", "table")).name)
    if path is None:
        data = src.read()
    else:
        try:
            data = path.read_bytes()
        except OSError as e:
            raise TableError(f"{name}: cannot be read ({type(e).__name__}: "
                             f"{e})") from e
    if not encodings:
        raise ValueError("read_csv: no encoding to try")
    text = error = None
    for encoding in encodings:
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError as e:
            error = e
    if text is None:
        raise TableError(f"{name}: cannot be read (UnicodeDecodeError: "
                         f"{error})") from error
    if encoding != encodings[0]:
        first = "UTF-8" if encodings[0].startswith("utf-8") else encodings[0]
        lg.info(f"{name}: not {first}, read as {encoding}")
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter,
                        **csv_options)
    rows, line = [], 0
    try:
        for row in reader:
            rows.append((line + 1, row))      # (first line of the record)
            line = reader.line_num
    except csv.Error as e:
        raise TableError(f"{name}: cannot be read (csv.Error: "
                         f"{e})") from e
    return _sheet(name, rows, path)


def _missing(name: str, module: str, extra: str | None,
             error: ImportError) -> TableError:
    """Describe a missing optional dependency (`TableError`)."""
    hint = f": pip install brainspresso[{extra}]" if extra else ""
    return TableError(f"{name}: {module} is needed to read it{hint}")


def read_xlsx(path: str | Path, *, sheet: int = 0,
              extra: str | None = None) -> Sheet:
    """
    Read a sheet of an Excel workbook (`.xlsx`, with openpyxl).

    The workbook is read whole (not `read_only`, which trusts the
    dimension stored in the file: some files store a wrong one), with
    the values of formulas, and closed on every path.

    Parameters
    ----------
    path : path
        The workbook
    sheet : int
        Index of the sheet
    extra : str, optional
        Extra of brainspresso that installs openpyxl (the hint of the
        error if it is missing: `oasis2`)

    Returns
    -------
    Sheet
        Rows numbered as in the sheet; cells as openpyxl reads them

    Raises
    ------
    TableError
        If openpyxl is missing, or the workbook cannot be read
    """
    path = Path(path)
    try:
        import openpyxl  # noqa: PLC0415  (lazy: optional dependency)
    except ImportError as e:
        raise _missing(path.name, "openpyxl", extra, e) from e
    try:
        book = openpyxl.load_workbook(path, data_only=True)
    except Exception as e:      # (openpyxl raises many kinds of errors)
        raise TableError(f"{path.name}: cannot be read "
                         f"({type(e).__name__}: {e})") from e
    try:
        rows = [list(row) for row in
                book.worksheets[sheet].iter_rows(values_only=True)]
    except Exception as e:      # (idem)
        raise TableError(f"{path.name}: cannot be read "
                         f"({type(e).__name__}: {e})") from e
    finally:
        book.close()
    return _sheet(path.name, list(enumerate(rows, start=1)), path)


def read_xls(path: str | Path, *, sheet: int = 0,
             extra: str | None = None) -> Sheet:
    """
    Read a sheet of a legacy Excel workbook (`.xls`, with xlrd).

    Parameters
    ----------
    path : path
        The workbook
    sheet : int
        Index of the sheet
    extra : str, optional
        Extra of brainspresso that installs xlrd (`ixi`)

    Returns
    -------
    Sheet
        Rows numbered as in the sheet; cells as xlrd reads them (numbers
        are floats, dates numbers: `Sheet.datemode`)

    Raises
    ------
    TableError
        If xlrd is missing, or the workbook cannot be read
    """
    path = Path(path)
    try:
        import xlrd  # noqa: PLC0415  (lazy: optional dependency)
    except ImportError as e:
        raise _missing(path.name, "xlrd", extra, e) from e
    try:
        book = xlrd.open_workbook(path)
        try:
            data = book.sheet_by_index(sheet)
            rows = [[c.value for c in data.row(n)]
                    for n in range(data.nrows)]
        finally:
            book.release_resources()
    except Exception as e:      # (xlrd raises many kinds of errors)
        raise TableError(f"{path.name}: cannot be read "
                         f"({type(e).__name__}: {e})") from e
    return _sheet(path.name, list(enumerate(rows, start=1)), path,
                  datemode=book.datemode)


def xls_date(value: float, datemode: int) -> str:
    """
    Format a date cell of an xls workbook (`read_xls`).

    Parameters
    ----------
    value : float
        The cell (days since the epoch of the workbook)
    datemode : int
        `Sheet.datemode`

    Returns
    -------
    str
        `YYYY-MM-DD`
    """
    import xlrd  # noqa: PLC0415  (lazy: optional dependency, read_xls)

    date = xlrd.xldate.xldate_as_tuple(value, datemode)
    return f"{date[0]:04d}-{date[1]:02d}-{date[2]:02d}"


# ----------------------------------------------------------------------
#   Writing
# ----------------------------------------------------------------------

def clean_rows(rows: Iterable[Iterable], name: str, key=(),
               numeric: Iterable[str] = ()) -> Iterator[list[str]]:
    """
    Clean every cell of a table (`clean`), warned once.

    Parameters
    ----------
    rows : iterable of rows
        The table (header first)
    name : str
        Its name (in the warning)
    key : hashable
        Version of its sources (`warn_once`)
    numeric : set[str]
        Its numeric columns (`numeric_columns`)

    Yields
    ------
    list[str]
        The rows, cleaned (no value is read as missing but None and NaN)
    """
    counts = Counter()
    numeric = set(numeric)
    columns = None
    for row in rows:
        if columns is None:             # (the header)
            row = [clean(value, nans=(), counts=counts) for value in row]
            columns = [column in numeric for column in row]
            yield row
            continue
        yield [clean(value, nans=(), counts=counts,
                     numeric=i < len(columns) and columns[i])
               for i, value in enumerate(row)]
    message = cleaned_message(name, counts)
    if message:
        warn_once(key, message)


def _sidecar(sidecar, name: str) -> Mapping | None:
    """Read the data dictionary of a table (warned if it cannot be)."""
    if sidecar is None or isinstance(sidecar, Mapping):
        return sidecar
    try:
        with open(sidecar, encoding="utf-8") as f:
            value = json.load(f)
    except (OSError, ValueError) as e:
        warn_once(_file_key(sidecar), f"{name}: data dictionary "
                                      f"{Path(sidecar).name} cannot be read "
                                      f"({type(e).__name__}: {e}): no "
                                      f"column read as numeric")
        return None
    return value if isinstance(value, Mapping) else None


def _write(rows, name: str, src, sidecar, path) -> None:
    """Write a roasted table (the actor of `table_action`)."""
    rows = rows() if callable(rows) else rows
    sources = [src] if isinstance(src, (str, Path)) else list(src or ())
    key = (name, *map(_file_key, sources))
    numeric = numeric_columns(_sidecar(sidecar, name))
    write_tsv(clean_rows(rows, name, key, numeric), path)


def sidecar_name(ctx, dst: str | Path) -> str:
    """
    Name of the data dictionary of a table, as a resource.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    dst : path
        The table, in the raw dataset

    Returns
    -------
    str
        `participants.json`, `phenotype/<name>.json`; `sessions.json`
        and `scans.json` (inherited, at the top) for the tables of a
        subject
    """
    dst = Path(dst)
    try:
        rel = dst.relative_to(ctx.layout.raw)
    except ValueError:
        rel = Path(dst.name)
    for suffix in ("sessions", "scans"):
        if rel.stem.endswith(f"_{suffix}") and rel.parts[0].startswith(
                "sub-"):
            return f"{suffix}.json"
    return rel.with_suffix(".json").as_posix()


def table_action(ctx, dst: str | Path,
                 rows: Iterable[Iterable] | Callable[[], Iterable],
                 src, *, tag: str = "meta", pristine: bool = True,
                 name: str | None = None,
                 sidecar: Mapping | str | Path | bool | None = True
                 ) -> Action:
    """
    Write a roasted table (participants, sessions, phenotypes).

    Every cell goes through `clean` when the action runs (one warning per
    table if a cell is changed), so that the table needs no quoting; in
    the columns that its data dictionary declares numeric
    (`numeric_columns`), text that reads as a number is written in its
    shortest form (`"70.0"` is `70`), as spreadsheet floats are.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    dst : path
        The table (in the raw dataset)
    rows : list[list] | callable() -> iterable of rows
        Its rows (header first), or a function that reads them when the
        action runs (picklable): then an error reading the sources (a
        `TableError`) is an error of this table only, under `tag`
    src : path | list[path]
        Its sources (they date it)
    tag : str
        Progress group
    pristine : bool
        Write it to `.brainspresso/orig/`, from which the visible table
        is trimmed after every roast; else to `dst`
    name : str, optional
        Name of the table in warnings (default: its file name)
    sidecar : dict | path | bool | None
        Its data dictionary (the JSON sidecar); True: the resource of
        its name (`sidecar_name`, with `ctx.resource`: what roast
        copies); None or False: none (no column is numeric)

    Returns
    -------
    Action
        Of kind "table" (a pristine table for `RoastPolicy` when
        `pristine`)
    """
    out = ctx.layout.original(dst) if pristine else Path(dst)
    name = name or Path(dst).name
    if not callable(rows):
        rows = [list(row) for row in rows]
    if sidecar is True:
        sidecar = ctx.resource(sidecar_name(ctx, dst))
    elif sidecar is False:
        sidecar = None
    return Action(src, out, partial(_write, rows, name, src, sidecar),
                  mode="t", input="path", tag=tag)


# ----------------------------------------------------------------------
#   Mapping of columns (ABIDE)
# ----------------------------------------------------------------------

def _label(value) -> str:
    """Default label of a subject: its identifier, stripped."""
    value = str(value).strip()
    return value[4:] if value.startswith("sub-") else value


@dataclass(frozen=True)
class Table:
    """
    A table that maps columns of source tables (CSV files).

    Cells are read by the name of their column in each file, so files
    may order (or lack) columns differently; a column that a file lacks
    is `n/a` in its rows. Column names are matched without case and
    whitespace (`column_key`).

    Parameters
    ----------
    columns : dict[str, str | tuple[str]]
        BIDS column -> source column (`participant_id` first); a tuple
        lists alternative names, the first one a file has is read
    values : dict[str, dict | callable]
        BIDS column -> value mapping (values not in a dict are kept)
    baseline : str, optional
        Source column (`AGE_AT_SCAN`) that tells, among the rows of a
        subject listed more than once in a file (baseline and
        follow-up), which one is kept: the lowest value. Without it,
        the first row is kept
    nans : set[str]
        Values that mean "missing" (written `n/a`)
    label : callable(str) -> str
        Label of a subject from its identifier (default: stripped,
        without `sub-`); a row whose label is empty or not alphanumeric
        is skipped
    prefix : str
        Written before the label (`sub-`)
    """

    columns: Mapping[str, str | tuple[str, ...]]
    values: Mapping[str, object] = field(default_factory=dict)
    baseline: str | None = None
    nans: frozenset = NANS
    label: Callable[[str], str] = _label
    prefix: str = "sub-"

    def names(self, column: str) -> tuple[str, ...]:
        """
        Return the source names of a BIDS column.

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

    def convert(self, column: str, value, counts: Counter | None = None
                ) -> str:
        """
        Convert one cell.

        Parameters
        ----------
        column : str
            BIDS column
        value : object
            Source cell
        counts : Counter, optional
            Of `clean`

        Returns
        -------
        str
            `n/a` for missing values
        """
        value = clean(value, nans=self.nans, counts=counts)
        if value == "n/a":
            return value
        mapping = self.values.get(column)
        if callable(mapping):
            return mapping(value)
        if mapping is not None:
            return mapping.get(value, value)
        return value

    def _order(self, row: list, index: int | None) -> float:
        """Sort key of a row among those of one subject (`baseline`)."""
        try:
            value = str(row[index]).strip()
            order = float(value)
        except (TypeError, IndexError, ValueError):
            return float("inf")
        if value in self.nans or not math.isfinite(order):
            return float("inf")             # (missing: `-9999`, `nan`)
        return order

    def rows(self, sources: Iterable[Path | Sheet],
             name: str = "table") -> Iterator[list[str]]:
        """
        Rows of the table (header first) from source tables.

        Cells are cleaned (`clean`: tabs and line breaks collapsed, `"`
        replaced by `'`, warned once). Blank rows are skipped; rows
        without a subject, or that do not reach the subject column, are
        skipped with a warning. Of the rows of a subject listed more
        than once, one is kept (that of the first file, and in it the
        first row or the `baseline` one), with one warning per file. A
        mapped column that no file has is warned about once.

        Parameters
        ----------
        sources : list[path | Sheet]
            Source tables (CSV files, read with `read_csv`)
        name : str
            Name of the table (in warnings)

        Yields
        ------
        list[str]
            The header, then one row per subject
        """
        yield list(self.columns)
        seen = set()
        found = set()
        counts = Counter()
        sheets = [s if isinstance(s, Sheet) else read_csv(s)
                  for s in sources]
        for sheet in sheets:
            where = {column: sheet.find(self.names(column))
                     for column in self.columns}
            found.update(c for c, i in where.items() if i is not None)
            sub = where["participant_id"]
            if sub is None:
                lg.warning(f"{sheet.name}: no column "
                           f"{' or '.join(self.names('participant_id'))}: "
                           f"skipped")
                continue
            order = sheet.find(self.baseline) if self.baseline else None
            kept: dict[str, list] = {}
            twice: list[tuple[int, str]] = []
            for line, row in sheet.rows:
                if len(row) <= sub or not str(row[sub] or "").strip():
                    row_warning(sheet, line, "row skipped (no subject)")
                    continue
                label = self.label(str(row[sub]))
                if not label.isalnum():
                    row_warning(sheet, line, f"row skipped (subject "
                                             f"{label!r} is not a label)")
                    continue
                if label in seen or label in kept:
                    twice.append((line, label))
                    if label in kept and order is not None and \
                            self._order(row, order) < \
                            self._order(kept[label], order):
                        kept[label] = row
                    continue
                kept[label] = row
            seen.update(kept)
            if len(twice) == 1:
                row_warning(sheet, twice[0][0], f"row skipped (subject "
                                                f"{twice[0][1]} listed "
                                                f"twice)")
            elif twice:
                labels = sorted({label for _, label in twice})
                which = (f"the row of the lowest {self.baseline} kept"
                         if order is not None else "the first row kept")
                row_warning(sheet, 0, f"{len(twice)} rows skipped (subjects "
                                      f"listed more than once: "
                                      f"{', '.join(labels[:5])}"
                                      f"{'...' if len(labels) > 5 else ''}; "
                                      f"{which})")
            for label, row in kept.items():
                out = [f"{self.prefix}{label}"]
                for column in list(self.columns)[1:]:
                    out.append(self.convert(
                        column, sheet.cell(row, where[column]), counts))
                yield out
        key = tuple(sheet.key for sheet in sheets)
        message = cleaned_message(name, counts)
        if message:
            warn_once(key, message)
        absent = [c for c in self.columns if c not in found]
        if sheets and absent and len(absent) < len(self.columns):
            warn_once(key, f"{name}: no CSV file has a column for "
                           f"{', '.join(absent)} (n/a): "
                           f"{', '.join(s.name for s in sheets[:3])}"
                           f"{'...' if len(sheets) > 3 else ''}")
