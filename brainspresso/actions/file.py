"""
Files read and written by actions, protected by reader/writer locks.

A `File` context holds a lock on a file for its whole duration
(`actions.locks`): a *shared* lock for a reader, an *exclusive* lock for
a writer. A writer writes a temporary file (`<name>.tmp/<name>`, next to
the destination, so that the rename stays on one filesystem) and renames
it into place on success, while it still holds its lock. Hence:

* any number of readers may read a file at once, and every (re)open of
  the file inside one reader context sees the same content: no writer
  can replace it until the last reader leaves;
* a writer waits until no reader holds the file, and two writers of one
  file are serialised;
* a reader never sees a partial file, and nothing is created next to a
  file that is only read (lock files live in the `.brainspresso/locks/`
  folder of the dataset, or in a local per-user folder outside
  datasets; see `actions.locks` for the exact rule).

Waiting for a lock times out (`timeout`, default `locks.lock_timeout()`)
with a `LockTimeout` that names the file and what holds it.
"""
import time
from logging import getLogger
from pathlib import Path
from shutil import copy2, rmtree
from typing import IO, Iterable, Iterator, Tuple

from brainspresso.actions.locks import FileLock, LockError, LockTimeout

lg = getLogger(__name__)

__all__ = [
    "File", "Files", "FileObj", "FileObjMixin", "OpenedFile",
    "LockError", "LockTimeout",
]


def _folder_in_the_way(src: Path, dst: Path) -> bool:
    """Whether a rename of file `src` failed because `dst` is a folder."""
    return src.is_file() and dst.is_dir() and not dst.is_symlink()


def _is_writer(mode: str) -> bool:
    """Whether a mode writes (`w`, `a`, `x` or `+`)."""
    return any(c in mode for c in "wax+")


def _is_reader(mode: str) -> bool:
    """Whether a mode reads (`r` or `+`, or neither `w`, `a` nor `x`)."""
    return "r" in mode or "+" in mode or not _is_writer(mode)


class File:
    """
    A file of the tree, protected while it is read or written.

    A reader (`File(path, "r")`) holds a shared lock for the whole
    context; a writer (`"w"`, `"a"`, `"r+"`...) holds an exclusive lock,
    writes a temporary file (`safename`) and renames it to `filename`
    when the context exits without error (unless `discard` is set).
    Read-modify-write modes (`"a"`, `"r+"`) copy the existing file to
    the temporary one first, under the exclusive lock only.

    ```python
    # Lock for reading, open a file object
    with File(filename, "rb") as file_ref:
        with file_ref.open() as f:
            dat = f.read()

    # Lock for reading only when a file object is opened (then until
    # the context exits)
    with File(filename) as file_ref:
        with file_ref.open("rb") as f:
            dat = f.read()

    # Lock for writing
    with File(filename, "wb") as file_ref:
        with file_ref.open() as f:
            f.write(dat)

    # Lock, but give a path to a subroutine
    with File(filename, "r") as file_ref:
        subroutine_read(file_ref.safename)      # the file itself
    with File(filename, "w") as file_ref:
        subroutine_write(file_ref.safename)     # the temporary file
    ```

    Modes:

    | `mode` | creates | truncates | opens at | `read`  | `write` |
    | ------ | ------- | --------- | -------- | ------- | ------- |
    | `'r'`  |    ✗    |     ✗     | start    |    ✓    |    ✗    |
    | `'w'`  |    ✓    |     ✓     | start    |    ✗    |    ✓    |
    | `'a'`  |    ✓    |     ✗     | end      |    ✗    |    ✓    |
    | `'r+'` |    ✗    |     ✗     | start    |    ✓    |    ✓    |
    | `'w+'` |    ✓    |     ✓     | start    |    ✓    |    ✓    |
    | `'a+'` |    ✓    |     ✗     | end      |    ✓    |    ✓    |

    plus `'b'` (binary) or `'t'` (text).

    A thread that reads a file cannot write it in the same context (a
    lock upgrade, which would deadlock): `LockError`.

    Parameters
    ----------
    filename : str | Path
        The file
    mode : str | None
        Protection (and default opening) mode. None: no protection
        until a file object is opened (`open(mode)`), which then lasts
        until the context exits.
    timeout : float | None
        Seconds to wait for the lock (default: `locks.lock_timeout()`)

    Attributes
    ----------
    safename : Path | None
        Path to read or write inside the context: `filename` for a
        reader, the temporary file for a writer
    discard : bool
        Set inside a writer context to keep the file already there (the
        temporary file is not renamed)
    """

    # Derived from `dandi.download.DownloadDirectory`
    # https://github.com/dandi/dandi-cli/blob/master/dandi/download.py
    # Apache License Version 2.0

    def __init__(
            self,
            filename: str | Path,
            mode: str | None = None,
            *,
            timeout: float | None = None,
    ) -> None:
        self.mode = mode
        self.timeout = timeout
        self.filename: Path = Path(filename)
        self.tempdir: Path = self.filename.with_name(
            self.filename.name + '.tmp'
        )
        self.tempname: Path = self.tempdir / self.filename.name
        self.safename = None
        self.lock: FileLock | None = None
        self.writable = None
        self.readable = None
        self.discard = False
        self._entered = False
        self._writing = False

    def open(self, mode: str | None = None, **kwargs) -> "OpenedFile":
        r"""
        Open the file and return a file-like context.

        Parameters
        ----------
        mode : {'r', 'w', 'x', 'a', 'b', 't', '+'} | None
            Opening mode. By default: same as `self.mode`, or 'rb' if
            `self.mode=None`.

        Other Parameters
        ----------------
        buffering: int, default=-1
            * `0`    : switch buffering off (only allowed in binary mode)
            * `1`    : line buffering (only usable when writing in text mode)
            *  `> 1` : size in bytes of a fixed-size chunk buffer.
        encoding: str, optional
            Name of (text-mode) encoding/decoding coded.
            Can be any codec supported by python.
        errors : {'strict', 'ignore', 'replace', 'backslashreplace',
                  'surrogateescape', 'xmlcharrefreplace', 'namereplace'}
            How to handle errors
        newline : {None, '', '\n', '\r', '\r\n'}
            How to parse newline characters from the stream (text mode)

        Returns
        -------
        OpenedFile
            A context that opens the file object when entered
        """
        if not self._entered:
            raise ValueError('File.open() called outside of context manager')
        mode = mode or self.mode or 'rb'
        return OpenedFile(self, mode, **kwargs)

    # ------------------------------------------------------------------
    #   Protection
    # ------------------------------------------------------------------
    def protect(self, mode: str) -> None:
        """
        Lock the file for a mode, until the context exits.

        Called on entry when the context has a mode, else by the first
        file object opened. A writer also prepares its temporary file.

        Parameters
        ----------
        mode : str
            Opening mode (`'r'`, `'w'`, `'a'`, `'r+'`...)
        """
        writer = _is_writer(mode)
        if self.lock is not None:
            if writer and not self.lock.exclusive:
                raise LockError(
                    f'Cannot write {self.filename}: it is read in this '
                    f'context (lock upgrades are not supported)'
                )
            return
        lock = FileLock(self.filename, exclusive=writer,
                        timeout=self.timeout)
        lock.acquire()
        self.lock = lock
        if not writer:
            self.safename = self.filename
            return
        try:
            # Clear what a previous (interrupted) writer may have left:
            # only under the lock, so never the file of a running writer
            self.tempdir.mkdir(parents=True, exist_ok=True)
            if self.tempname.is_dir():
                rmtree(self.tempname)
            else:
                self.tempname.unlink(missing_ok=True)
            # Read-modify-write: start from the current file
            if 'a' in mode or ('r' in mode and '+' in mode):
                if self.filename.exists():
                    copy2(self.filename, self.tempname)
        except BaseException:
            self._cleanup()
            raise
        self.safename = self.tempname
        self._writing = True

    def __enter__(self) -> "File":
        """Enter the context, locking the file if a mode was given."""
        mode = self.mode or ''
        self.writable = _is_writer(mode) if mode else None
        self.readable = _is_reader(mode) if mode else None
        self.discard = False
        self._entered = True
        if mode:
            try:
                self.protect(mode)
            except BaseException:
                self._entered = False
                raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Rename the temporary file into place (writers), then unlock."""
        try:
            # Rename only if the context was not interrupted by an
            # exception, and if the output was not discarded (identical
            # to the file already there); while the lock is held.
            if (
                self._writing and exc_type is None and not self.discard
                and self.tempname.exists()
            ):
                try:
                    self.tempname.replace(self.filename)
                except OSError:
                    # a folder in the way of a file: IsADirectoryError
                    # (POSIX), PermissionError (Windows)
                    if not _folder_in_the_way(self.tempname, self.filename):
                        raise
                    rmtree(self.filename)
                    self.tempname.replace(self.filename)
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Remove the temporary folder (still locked), then unlock."""
        try:
            if self._writing or (self.lock is not None and
                                 self.lock.exclusive):
                if self.tempdir.exists():
                    rmtree(self.tempdir, ignore_errors=True)
        finally:
            if self.lock is not None:
                self.lock.release()
            self.lock = None
            self._entered = self._writing = False
            self.safename = None
            self.writable = None
            self.readable = None


class Files:
    """
    A collection of files, entered and exited together.

    Parameters
    ----------
    *files : File
        A series of `File`
    """

    def __init__(self, *files: File):
        self.files = files
        self._unopened = list(files)
        self._opened = []

    def __len__(self) -> int:
        """Return the number of files."""
        return len(self.files)

    def __getitem__(self, index: int) -> File:
        """Return the file at an index."""
        if self._opened:
            return self._opened[index]
        else:
            return self.files[index]

    def __iter__(self) -> Iterator[File]:
        """Iterate over the files."""
        if self._opened:
            return iter(self._opened)
        else:
            return iter(self.files)

    def __enter__(self) -> "Files":
        """Enter every file, in order (all or none)."""
        try:
            while self._unopened:
                file = self._unopened[0]
                file.__enter__()
                self._unopened.pop(0)
                self._opened.append(file)
        except BaseException as e:
            self.__exit__(type(e), e, e.__traceback__)
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit every file, in reverse order.

        Every file is exited even if exiting one fails. The first error
        of a clean exit is raised; when exiting because of an exception,
        errors are only logged, so that they do not mask it.
        """
        errors = []
        while self._opened:
            file = self._opened.pop(-1)
            try:
                file.__exit__(exc_type, exc_val, exc_tb)
            except Exception as e:
                errors.append(e)
            self._unopened.insert(0, file)
        if not errors:
            return
        if exc_type is None:
            for e in errors[1:]:
                lg.error(f"Error while exiting a file: {e!r}")
            raise errors[0]
        for e in errors:
            lg.error(f"Error while exiting a file (after {exc_type.__name__}"
                     f": {exc_val}): {e!r}")

    def open(self, *a, **k) -> "OpenedFile" | Tuple["OpenedFile"]:
        r"""
        Open the file(s) and return file-like context(s).

        Parameters
        ----------
        mode : {'r', 'w', 'x', 'a', 'b', 't', '+'} | None
            Opening mode (see `File.open`).

        Other Parameters
        ----------------
        buffering, encoding, errors, newline
            See `File.open`

        Returns
        -------
        *files : OpenedFile
            A series of file objects.
        """
        filobjs = tuple(file.open(*a, **k) for file in self.files)
        if len(filobjs) == 0:
            return None
        if len(filobjs) == 1:
            return filobjs[0]
        else:
            return filobjs


class FileObjMixin:
    """
    Logic common to all file objects (read and write speeds).

    Assumes the class has attributes:
    * fileobj: IO[bytes]
    * total_read: int
    * last_read_speed: float
    * mean_read_speed: float
    * total_write: int
    * last_write_speed: float
    * mean_write_speed: float
    """

    def error_if_notincontext(self, name: str) -> None:
        """
        Raise if the file object is not open.

        Parameters
        ----------
        name : str
            Name of the method called (for the message)
        """
        if self.fileobj is None:
            raise ValueError(
                f'FileObj.{name}() called outside of context manager'
            )

    def tell(self) -> int:
        """Return the current position."""
        self.error_if_notincontext('tell')
        return self.fileobj.tell()

    def seek(self, *a, **k):
        """Move to a position (as `IO.seek`)."""
        self.error_if_notincontext('seek')
        return self.fileobj.seek(*a, **k)

    def write(self, blob: bytes | str) -> "FileObjMixin":
        """
        Write data.

        Parameters
        ----------
        blob : bytes | str
            Data to write

        Returns
        -------
        FileObjMixin
            self
        """
        self.error_if_notincontext('write')
        tic = time.time()
        self.fileobj.write(blob)
        toc = time.time()
        self._update_write_speed(len(blob), toc-tic)
        return self

    def read(self, nbytes: int | None = None) -> bytes | str:
        """
        Read data.

        Parameters
        ----------
        nbytes : int | None
            Number of bytes (or characters) to read (None: all)

        Returns
        -------
        bytes | str
            Data read
        """
        self.error_if_notincontext('read')
        tic = time.time()
        blob = self.fileobj.read(nbytes)
        toc = time.time()
        self._update_read_speed(len(blob), toc-tic)
        return blob

    def readline(self) -> str:
        """Read one line."""
        self.error_if_notincontext('readline')
        tic = time.time()
        line = self.fileobj.readline()
        toc = time.time()
        self._update_read_speed(len(line), toc-tic)
        return line

    def readlines(self, nlines: int) -> Iterable[str]:
        """
        Read lines.

        Parameters
        ----------
        nlines : int
            Size hint (as `IO.readlines`)

        Returns
        -------
        list[str]
            Lines read
        """
        self.error_if_notincontext('readlines')
        tic = time.time()
        line = self.fileobj.readlines(nlines)
        toc = time.time()
        self._update_read_speed(len(line), toc-tic)
        return line

    def __iter__(self):
        """Iterate over lines."""
        self.error_if_notincontext('__iter__')
        lines = iter(self.fileobj)
        while True:
            try:
                tic = time.time()
                line = next(lines)
                toc = time.time()
                self._update_read_speed(len(line), toc-tic)
                yield line
            except StopIteration:
                return

    def __next__(self) -> str:
        """Read the next line."""
        self.error_if_notincontext('__next__')
        tic = time.time()
        line = self.fileobj.__next__()
        toc = time.time()
        self._update_read_speed(len(line), toc-tic)
        return line

    def append(self, blob: bytes | str) -> "FileObjMixin":
        """Write data (`write` alias)."""
        return self.write(blob)

    def __add__(self, blob: bytes | str) -> "FileObjMixin":
        """Write data (`write` alias)."""
        return self.append(blob)

    def _update_read_speed(self, nbytes: int, time: float) -> None:
        if time == 0:
            # too fast for proper timing
            return
        self.last_read_speed = nbytes / time
        if self.mean_read_speed:
            self.mean_read_speed = self.total_read / self.mean_read_speed
            self.mean_read_speed += time
            self.total_read += nbytes
            self.mean_read_speed = self.total_read / self.mean_read_speed
        else:
            self.mean_read_speed = self.last_read_speed
            self.total_read += nbytes

    def _update_write_speed(self, nbytes: int, time: float) -> None:
        if time == 0:
            # too fast for proper timing
            return
        self.last_write_speed = nbytes / time
        if self.mean_write_speed:
            self.mean_write_speed = self.total_write / self.mean_write_speed
            self.mean_write_speed += time
            self.total_write += nbytes
            self.mean_write_speed = self.total_write / self.mean_write_speed
        else:
            self.mean_write_speed = self.last_write_speed
            self.total_write += nbytes


class FileObj(FileObjMixin, File):
    """
    An opened file object, protected like a `File`.

    ```python
    with FileObj(filename, "rb") as f:      # shared lock
        dat = f.read()
    with FileObj(filename, "wb") as f:      # exclusive lock, temporary
        f.write(dat)                        # file renamed on exit
    ```

    Parameters
    ----------
    filename : str | Path
        The file
    mode : str
        Opening mode (see `File`)
    timeout : float | None
        Seconds to wait for the lock
    """

    def __init__(
            self,
            filename: str | Path,
            mode: str | None = 'rb',
            *,
            timeout: float | None = None,
    ):
        if mode is None:
            raise ValueError('mode must be provided')
        super().__init__(filename, mode, timeout=timeout)
        self.fileobj = None
        self.total_read = 0
        self.last_read_speed = 0
        self.mean_read_speed = 0
        self.total_write = 0
        self.last_write_speed = 0
        self.mean_write_speed = 0

    def __enter__(self) -> "FileObj":
        """Lock the file and open the file object."""
        super().__enter__()
        try:
            self.fileobj = self.safename.open(self.mode)
        except BaseException as e:
            super().__exit__(type(e), e, e.__traceback__)
            raise
        self.total_read = 0
        self.total_write = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the file object, then exit the `File` context."""
        try:
            self.fileobj.close()
        finally:
            self.fileobj = None
            super().__exit__(exc_type, exc_val, exc_tb)


class OpenedFile(FileObjMixin):
    """
    File object returned by `File.open()`.

    The lock (taken by the `File`, or by the first file object opened
    in a `File` without mode) is released, and the temporary file
    renamed, when the `File` context exits, not when this one does.

    !!! warning
        This object **should not** be created by a user.
        It should **only** be created inside `File.open()`.

    Parameters
    ----------
    file : File
        The (entered) `File`
    mode : str
        Opening mode
    **kwargs
        Options of `open` (`buffering`, `encoding`, `errors`, `newline`)
    """

    def __init__(self, file: File, mode: str | None, **kwargs) -> None:
        if mode is None:
            mode = file.mode
        if mode is None:
            raise ValueError('A mode must be provided')
        self.file = file
        self.mode = mode
        self.kwargs = kwargs
        self.fileobj = None
        self.writable = None
        self.readable = None
        self.total_read = 0
        self.last_read_speed = 0
        self.mean_read_speed = 0
        self.total_write = 0
        self.last_write_speed = 0
        self.mean_write_speed = 0

    def __enter__(self) -> "OpenedFile":
        """Open the file object (locking the file if not yet locked)."""
        mode = self.mode
        if mode == 'b' or mode == 't':
            mode = 'r' + mode

        self.writable = _is_writer(mode)
        self.readable = _is_reader(mode)

        if self.writable and self.file.writable is False:
            raise ValueError(
                'File was not opened in write mode, '
                'so file object cannot be opened in write mode.')
        if self.readable and self.file.readable is False:
            raise ValueError(
                'File was not opened in read mode, '
                'so file object cannot be opened in read mode.')

        # protection deferred to here (`File(path)` without mode)
        self.file.protect(mode)
        self.fileobj: IO = self.file.safename.open(mode, **self.kwargs)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the file object (the `File` keeps its lock)."""
        assert self.file is not None
        self.fileobj.close()
        self.fileobj = None
