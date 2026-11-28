"""
The error contract of the converters.

A converter raises `ConvertError` for an input that is corrupt,
truncated or of a variant it does not support, with the name of the
file in its message (`<file name>: <reason>`) and the original error
chained. What it raises while writing its output (a full disk, a
folder that cannot be written) passes through unchanged: such an error
is not the input's. A missing optional package stays an `ImportError`.
"""
from __future__ import annotations

import math
import os
import struct
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from nibabel.filebasedimages import ImageFileError
from nibabel.spatialimages import HeaderDataError
from nibabel.wrapstruct import WrapStructError

__all__ = ["MAX_BYTES", "READ_ERRORS", "ConvertError", "check_size",
           "input_errors", "name_of"]

#: Largest plausible image (bytes): a header that declares more is
#: corrupt (sizes are computed with Python integers, which do not
#: overflow as `np.prod` does)
MAX_BYTES = 2 ** 40

#: What reading a corrupt input raises (`input_errors` converts them)
READ_ERRORS: tuple[type[BaseException], ...] = (
    OSError, EOFError, ValueError, KeyError, IndexError, struct.error,
    zlib.error, ImageFileError, HeaderDataError, WrapStructError,
)


class ConvertError(ValueError):
    """An input that a converter cannot read (`<file name>: <reason>`)."""


def name_of(path: str | os.PathLike | object) -> str:
    """
    Name an input in a message.

    Parameters
    ----------
    path : path | file
        A path, or a file object (its `name`, if it has one)

    Returns
    -------
    str
        Its file name (`"<input>"` for a file object without a name)
    """
    if isinstance(path, (str, os.PathLike)):
        return Path(path).name
    name = getattr(path, "name", None)
    return Path(name).name if isinstance(name, str) else "<input>"


@contextmanager
def input_errors(name: str, what: str = "cannot be read", *,
                 errors: tuple[type[BaseException], ...] = READ_ERRORS,
                 ) -> Iterator[None]:
    """
    Convert what reading an input raises to a `ConvertError`.

    Wrap the reading of an input only, never the writing of an output.

    Parameters
    ----------
    name : str
        Name of the input (the start of the message)
    what : str
        What went wrong (default: "cannot be read")
    errors : tuple[type]
        Errors converted (default: `READ_ERRORS`; add e.g.
        `tarfile.TarError` for the members of an archive)

    Raises
    ------
    ConvertError
        `"<name>: <what> (<Type>: <message>)"`, from the error (one of
        `errors`; a `ConvertError` passes through)
    """
    try:
        yield
    except ConvertError:
        raise
    except errors as e:
        raise ConvertError(f"{name}: {what} ({type(e).__name__}: {e})") \
            from e


def check_size(name: str, shape, itemsize: int, available: int | None = None,
               *, field: str = "dimensions") -> int:
    """
    Check the size of the data that a header declares.

    Parameters
    ----------
    name : str
        Name of the input (the start of the message)
    shape : sequence of int
        Declared shape (with channels, if any)
    itemsize : int
        Bytes per element
    available : int, optional
        Bytes of data actually there (None: not known yet)
    field : str
        Name of the header field, in the message

    Returns
    -------
    int
        The declared size, in bytes (a Python `int`)

    Raises
    ------
    ConvertError
        If a size is not positive, the total is implausible
        (`MAX_BYTES`), or more than the data `available`
    """
    shape = [int(n) for n in shape]
    nbytes = math.prod(shape) * int(itemsize)
    if min(shape, default=1) < 1 or nbytes > MAX_BYTES:
        raise ConvertError(f"{name}: implausible {field} {shape} "
                           f"({nbytes} bytes; corrupt header?)")
    if available is not None and available < nbytes:
        raise ConvertError(f"{name}: truncated data: {available} bytes for "
                           f"{nbytes} expected")
    return nbytes
