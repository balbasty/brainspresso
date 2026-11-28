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

import os
import struct
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from nibabel.filebasedimages import ImageFileError
from nibabel.spatialimages import HeaderDataError
from nibabel.wrapstruct import WrapStructError

__all__ = ["READ_ERRORS", "ConvertError", "input_errors", "name_of"]

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
def input_errors(name: str, what: str = "cannot be read") -> Iterator[None]:
    """
    Convert what reading an input raises to a `ConvertError`.

    Wrap the reading of an input only, never the writing of an output.

    Parameters
    ----------
    name : str
        Name of the input (the start of the message)
    what : str
        What went wrong (default: "cannot be read")

    Raises
    ------
    ConvertError
        `"<name>: <what> (<Type>: <message>)"`, from the error (one of
        `READ_ERRORS`; a `ConvertError` passes through)
    """
    try:
        yield
    except ConvertError:
        raise
    except READ_ERRORS as e:
        raise ConvertError(f"{name}: {what} ({type(e).__name__}: {e})") \
            from e
