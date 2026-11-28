import csv
import json
import logging
import os
from pathlib import Path

lg = logging.getLogger(__name__)


def read_json(src, **kwargs):
    """
    Read a JSON file

    Parameters
    ----------
    src : str or Path or file-like
        Input path

    Returns
    -------
    obj : dict
        Nested structure
    """
    if isinstance(src, (str, Path)):
        with open(src, 'rt', encoding='utf-8') as fsrc:
            return read_json(fsrc, **kwargs)
    return json.load(src, **kwargs)


def write_json(src, dst, makedirs=True, **kwargs):
    """
    Write a BIDS json (indent = 2)

    Parameters
    ----------
    src : dict
        Serializable nested structure
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wt', encoding='utf-8', newline='\n') as fdst:
            return write_json(src, fdst, **kwargs)
    kwargs.setdefault('indent', 2)
    json.dump(src, dst, **kwargs)


def copy_json(src, dst, makedirs=True, **kwargs):
    """
    Copy a JSON file, while ensuring that the output file follows our
    formatting convention (i.e., `indent=2`)

    Parameters
    ----------
    src : str or Path or file
        Input path
    dst : str or Path or file
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wt', encoding='utf-8', newline='\n') as fdst:
            return copy_json(src, fdst, **kwargs, makedirs=False)
    if isinstance(src, (str, Path)):
        with open(src, 'rt', encoding='utf-8') as fsrc:
            return copy_json(fsrc, dst, **kwargs, makedirs=False)
    kwargs.setdefault('indent', 2)
    json.dump(json.load(src), dst, **kwargs)


def write_tsv(src, dst, makedirs=True, **kwargs):
    r"""
    Write a BIDS tsv (delimiter = '\t', quoting=QUOTE_NONE)

    Rows end with "\n". A field that contains a line break ("\n" or
    "\r") raises `ValueError` before anything is written: with
    `QUOTE_NONE` it cannot be written, and `csv` itself does not reject
    a "\r" on every Python version (3.10 and 3.11 write it raw).

    Parameters
    ----------
    src : list[list]
        A list of rows (the first one is the header)
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file

    Raises
    ------
    ValueError
        If a field contains a line break (the message names its row and
        column)
    """
    rows = _tsv_rows(src, dst)             # (checked once)
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wt', encoding='utf-8', newline='') as fdst:
            return _write_tsv_rows(rows, fdst, **kwargs)
    return _write_tsv_rows(rows, dst, **kwargs)


def _write_tsv_rows(rows: list[list], dst, **kwargs) -> None:
    """Write checked rows to an open text file (`write_tsv`)."""
    kwargs.setdefault('delimiter', '\t')
    kwargs.setdefault('quoting', csv.QUOTE_NONE)
    # (`csv` ends rows with "\r\n" by default; BIDS tables use "\n")
    kwargs.setdefault('lineterminator', '\n')
    writer = csv.writer(dst, **kwargs)
    writer.writerows(rows)


def _tsv_rows(src, dst) -> list[list]:
    """Rows of a TSV table, checked for line breaks in fields."""
    rows = [list(row) for row in src]
    header = rows[0] if rows else []
    for n, row in enumerate(rows):
        for i, value in enumerate(row):
            text = "" if value is None else str(value)
            if "\n" in text or "\r" in text:
                column = header[i] if i < len(header) else f"#{i + 1}"
                name = getattr(dst, "name", dst)
                raise ValueError(
                    f"{name}: row {n + 1}, column {column!r}: the field "
                    f"{text!r} contains a line break, which a TSV table "
                    f"cannot hold"
                )
    return rows


def write_from_buffer(src, dst, makedirs=True):
    """
    Write from an open buffer

    Parameters
    ----------
    src : io.BufferedReader or bytes
        An object with the `read()` method
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wb') as fdst:
            return write_from_buffer(src, fdst, makedirs=False)
    if isinstance(src, bytes):
        dst.write(src)
    else:
        dst.write(src.read())


def write_text(src, dst, makedirs=True):
    """
    Write a text file

    Parameters
    ----------
    src : str
        Some text
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wt', encoding='utf-8', newline='\n') as fdst:
            return write_text(src, fdst, makedirs=False)
    dst.write(src)


def copy_from_buffer(src, dst, makedirs=True):
    """
    Write from a file or open buffer

    Parameters
    ----------
    src : str or Path or file-like
        Input path
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        lg.info(f'write {os.path.basename(dst)}')
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, 'wb') as fdst:
            return copy_from_buffer(src, fdst, makedirs=False)
    if isinstance(src, (str, Path)):
        with open(src, 'rb') as fsrc:
            return copy_from_buffer(fsrc, dst, makedirs=False)
    if isinstance(src, bytes):
        dst.write(src)
    else:
        dst.write(src.read())
