"""
Partial files of downloads that can be resumed.

A download writes `<name>.download` under an exclusive lock of its own,
taken without waiting (one downloader per file), and renames it to
`<name>` under the exclusive reader/writer lock of `<name>`
(`actions.locks`), so that it never replaces a file while an action
reads it.

The download lock is the `actions.locks` lock of the partial file
`<name>.download` (its lock file is in the dataset's lock folder, or in
the per-user local one outside datasets). Earlier versions locked
`<name>.lock` next to the destination with `fasteners`: the two do not
exclude each other (do not download one file with both at once), and
outside datasets this lock protects one user's processes on one machine
only. A `<name>.lock` left by an earlier version is never deleted: that
version may still hold it.
"""
__all__ = ['IncompleteFile']
# stdlib
import asyncio
import hashlib
import os
import threading
import time
from functools import partial
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from typing import IO, Literal

# externals
import aiofiles
import aiofiles.os as aos

# internals
from brainspresso.actions.file import folder_in_the_way
from brainspresso.actions.locks import FileLock, LockError
from brainspresso.utils.digests import get_digester

lg = getLogger(__name__)
aop = aos.path


async def run_async(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, **kwargs), *args)


async def run_to_completion(func, *args):
    """
    Run a blocking function in the executor, and wait until it is done.

    A cancellation of the task does not stop the function, nor this wait
    (the call is shielded, and still queued or running): once the
    function is done, the cancellation propagates (`CancelledError`).
    For steps that must finish once started, or must run at all (the
    rename of a download into place, the release of its lock).
    """
    future = asyncio.get_running_loop().run_in_executor(None, func, *args)
    cancelled = False
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            if future.cancelled():          # pragma: no cover
                raise
            cancelled = True
        except BaseException:               # (the function raised)
            break
    if cancelled:
        raise asyncio.CancelledError()
    return future.result()


async def release_lock(lock: FileLock, quiet: bool = False) -> None:
    """
    Release a lock, always (one helper for every release path).

    The release runs in the executor (it may block: an unlock or a close
    on a network filesystem), shielded: it runs even if the task is
    cancelled, and this waits for it (`run_to_completion`). An `OSError`
    of the release is logged, never raised. With `quiet` (an exception
    is already propagating: the caller re-raises it), a cancellation
    during the wait is not raised either, so that the original
    exception is not replaced.
    """
    def release():
        try:
            lock.release()
        except OSError as e:
            lg.warning(f"Could not release the lock of {lock.path}: {e}")

    try:
        await run_to_completion(release)
    except asyncio.CancelledError:
        if not quiet:
            raise


class IncompleteFile:
    """
    A file being downloaded, in its incomplete state.

    It is used as a context manager and checks whether
    an unfinished download can be continued, or should be completely
    restarted.

    ```python
    with IncompleteFile(filename, checksum=sha1) as obj:
        for chunk in chunk_server:
            obj += chunk
    ```
    """

    # Derived from `dandi.download.DownloadDirectory`
    # https://github.com/dandi/dandi-cli/blob/master/dandi/download.py
    # Apache License Version 2.0

    def __init__(
        self,
        filename: str | Path,
        checksum: str | None = None,
        checkalgo: str | None = None,
        ifnochecksum: Literal['restart', 'continue'] = 'restart',
    ):
        """
        Prepare a partial file.

        Parameters
        ----------
        filename : str | Path
            Output filename
        checksum : str | None
            Expected checksum (hex) of the file
        checkalgo : str | None
            Algorithm to use to compute the checksum of downloaded file
        ifnochecksum : {'restart', 'continue'}
            Behaviour if incomplete file exists but no checksum is provided
        """
        # checks
        if checkalgo and checkalgo not in hashlib.algorithms_available:
            raise ValueError(f'Unknown hashing algorithm: {checkalgo}')
        # assign
        self.filename: Path = Path(filename)
        self.tempname: Path = self.filename.with_name(
            self.filename.name + '.download'
        )
        self.lockname: Path = self.filename.with_name(
            self.filename.name + '.lock'
        )
        self.checkname: Path = self.filename.with_name(
            self.filename.name + '.checksum'
        )
        self.validname: Path = self.filename.with_name(
            self.filename.name + '.validator'
        )
        # ETag or Last-Modified of the remote file when the partial
        # download was started (sent as `If-Range` when resuming)
        self.validator: str | None = None
        self._discard: bool = False
        self.lock: FileLock | None = None
        self.file: IO[bytes] | None = None
        self.offset: int | None = None
        self.checksum: str = checksum
        self.checkalgo: str = checkalgo
        self.ifnochecksum: Literal['r', 'c'] = ifnochecksum.lower()[0]
        self.digester = None
        self._digest: str | None = None
        self.last_speed: float = 0
        self.mean_speed: float = 0

    @property
    def digest(self) -> str:
        """Hex digest of the bytes written so far (or of the whole file)."""
        if self._digest is not None:
            return self._digest
        elif self.digester:
            return self.digester.hexdigest()
        else:
            return None

    async def __aenter__(self) -> "IncompleteFile":
        """Lock the file and open it for writing (resuming if possible)."""
        self.filename.parent.mkdir(parents=True, exist_ok=True)

        # Acquire lock
        lock = FileLock(self.tempname, exclusive=True, timeout=0)
        lg.debug(f"acquiring lock... {self.tempname}")
        await self._acquire(lock)
        self.lock = lock
        lg.debug(f"acquired lock: {self.tempname}")
        try:
            return await self._enter()
        except BaseException:
            # (an error or a cancellation: `__aexit__` will not run)
            await self._close_and_release()
            raise

    async def _acquire(self, lock: FileLock) -> None:
        """
        Take the download lock in an executor thread.

        If this task is cancelled while it waits, the acquisition may
        still complete in the executor: whichever side comes last (the
        thread, or this task) releases the lock, so that it never leaks.
        """
        guard = threading.Lock()
        state = {"cancelled": False, "acquired": False}

        def acquire():
            lock.acquire()
            with guard:
                if state["cancelled"]:
                    lock.release()
                else:
                    state["acquired"] = True

        try:
            await run_async(acquire)
        except LockError as e:
            # (another download of the file runs: in another process, or
            # in a thread of this one, maybe the same executor thread)
            raise RuntimeError(
                f'Could not acquire download lock for {self.filename} ({e})'
            ) from e
        except BaseException:
            with guard:
                state["cancelled"] = True
                acquired = state["acquired"]
            if acquired:
                await release_lock(lock, quiet=True)
            raise

    async def _close_and_release(self) -> None:
        """Close the partial file (if open) and release the lock.

        On an error path (the caller re-raises): errors of the close are
        logged, the lock is always released, and the original exception
        is never replaced.
        """
        file, self.file = self.file, None
        lock, self.lock = self.lock, None
        try:
            if file is not None:
                await file.close()
        except Exception as e:      # (not a cancellation: see below)
            lg.warning(f"Could not close {self.tempname}: {e}")
        except asyncio.CancelledError:
            pass                    # (the original exception propagates)
        finally:
            if lock is not None:
                await release_lock(lock, quiet=True)

    async def _enter(self) -> "IncompleteFile":
        """Open the partial file (resuming if possible), lock held."""
        # Check if a file was already being downloaded, and if we should
        # continue from where we left off
        try:
            async with aiofiles.open(self.checkname, 'rt',
                                     encoding='utf-8') as f:
                checksum = await f.read()
        except (FileNotFoundError, ValueError):
            checksum = None

        # Compute checksum on the fly
        self._digest = None
        if self.checkalgo:
            self.digester = await run_async(hashlib.new, self.checkalgo)

        # Check whether we should keep the existing partial file
        cont = await aop.exists(self.tempname)
        cont = cont and ((self.checksum and self.checksum == checksum) or
                         (not self.checksum and self.ifnochecksum == 'c'))
        if cont:
            mode = 'ab'
            if self.checksum:
                lg.debug(
                    'Download file exists and has matching checksum; '
                    'resuming download'
                )
            else:
                lg.debug(
                    'Download file exists; resuming download'
                )
            if self.checkalgo:
                self.digester = await run_async(
                    get_digester, self.tempname, self.checkalgo
                )
            try:
                async with aiofiles.open(self.validname, 'rt',
                                         encoding='utf-8') as f:
                    self.validator = (await f.read()).strip() or None
            except (FileNotFoundError, ValueError):
                self.validator = None
        else:
            mode = 'wb'
            if await aop.exists(self.tempname):
                if self.checksum:
                    lg.debug(
                        'Download file found, but checksum does not match; '
                        'starting new download'
                    )
                else:
                    lg.debug(
                        'Download file exists; starting new download'
                    )
            else:
                lg.debug('Starting new download')
            # Remove existing file
            await run_async(self.tempname.unlink, missing_ok=True)
            await run_async(self.validname.unlink, missing_ok=True)
            self.validator = None

        # Open file
        lg.debug(f"opening file ({mode}) ... {self.tempname}")
        self.file = await aiofiles.open(self.tempname, mode)
        self.offset = await self.file.tell()
        lg.debug(f"opened file ({mode}): {self.tempname}")

        # Write expected checksum
        if self.checksum:
            async with aiofiles.open(self.checkname, "w", encoding="utf-8",
                                     newline="\n") as f:
                await f.write(self.checksum)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close the file, release the lock and finalise (or keep) it."""
        # Rename temporary filename to output filename
        # Note that we only rename the file to its final name and
        # remove temporary file if the download was successful (i.e.
        # the context was not interrupted by an exception)
        success = exc_type is None and not self._discard
        installed = False
        lock, self.lock = self.lock, None
        try:
            # Close file
            lg.debug(f"closing file...  {self.tempname}")
            assert self.file is not None
            file, self.file = self.file, None
            await file.close()
            lg.debug(f"closed file: {self.tempname}")
            if success:
                lg.debug(
                    f"renaming file...  {self.tempname} -> {self.filename}"
                )
                # (to completion, also if the task is cancelled: the lock
                # is released only once the rename is done)
                await run_to_completion(self._install)
                installed = True
                lg.debug(f"renamed file:  {self.tempname} -> {self.filename}")
                if self.digester:
                    lg.debug(f"saving digest...  {self.filename}")
                    self._digest = await run_async(self.digester.hexdigest)
                    lg.debug(f"saved digest:  {self.filename}")
        except BaseException:
            await self._finish(lock, installed, error=True)
            raise
        await self._finish(lock, installed, error=False)

    async def _finish(self, lock, installed: bool, error: bool) -> None:
        """
        Delete the partial files, then release the lock.

        The partial files (a complete file that could not be renamed is
        kept, to be resumed) are deleted while the lock is held. A
        `<name>.lock` of an earlier version is left alone (module
        docstring).
        """
        try:
            if installed or self._discard:
                def delete():
                    for path in (self.tempname, self.checkname,
                                 self.validname):
                        lg.debug(f"deleting file...  {path}")
                        path.unlink(missing_ok=True)
                await run_to_completion(delete)
        except BaseException:
            error = True
            raise
        finally:
            self.file = None
            self.offset = None
            self._discard = False
            if lock is not None:
                lg.debug(f"releasing lock...  {self.tempname}")
                await release_lock(lock, quiet=error)
                lg.debug(f"released lock:  {self.tempname}")

    def _install(self) -> None:
        """
        Rename the complete file into place.

        Under the exclusive lock of the destination (`actions.locks`,
        default timeout): readers of the file (e.g. a roast that reads
        a source) and other writers exclude this replacement, as they
        exclude each other. `LockTimeout` (a `TimeoutError`) if they
        hold it too long.
        """
        with FileLock(self.filename, exclusive=True):
            # `os.replace`: `os.rename` onto an existing file fails on
            # Windows
            try:
                os.replace(self.tempname, self.filename)
            except OSError:
                # a folder in the way: IsADirectoryError (POSIX),
                # PermissionError (Windows)
                if not folder_in_the_way(self.tempname, self.filename):
                    raise
                rmtree(self.filename)
                os.replace(self.tempname, self.filename)

    async def append(self, blob: bytes) -> "IncompleteFile":
        """Append bytes to the file and update its digest.

        Parameters
        ----------
        blob : bytes
            Bytes to write.

        Returns
        -------
        IncompleteFile
            This object.
        """
        if self.file is None:
            raise ValueError(
                'IncompleteFile.append() called outside of context manager'
            )
        if self.digester:
            await run_async(self.digester.update, blob)
        tic = time.time()
        await self.file.write(blob)
        toc = time.time()

        # timing
        new = len(blob)
        old = await self.file.tell() - new
        self._update_speed(old, new, toc-tic)
        return self

    async def truncate(self) -> "IncompleteFile":
        """Discard all bytes written so far (restart from 0)."""
        if self.file is None:
            raise ValueError(
                'IncompleteFile.truncate() called outside of context manager'
            )
        await self.file.seek(0)
        await self.file.truncate()
        self.offset = 0
        if self.checkalgo:
            self.digester = await run_async(hashlib.new, self.checkalgo)
        await self.set_validator(None)
        return self

    async def set_validator(self, validator: str | None) -> None:
        """Save the remote validator (ETag/Last-Modified) of this file."""
        self.validator = validator
        if validator:
            async with aiofiles.open(self.validname, 'wt', encoding='utf-8',
                                     newline='\n') as f:
                await f.write(validator)
        else:
            await run_async(self.validname.unlink, missing_ok=True)

    def discard(self) -> None:
        """Delete the partial file on exit instead of renaming it."""
        self._discard = True

    async def write(self, blob: bytes) -> "IncompleteFile":
        """Alias of `append`."""
        return await self.append(blob)

    async def __add__(self, blob: bytes) -> "IncompleteFile":
        """Alias of `append`."""
        return await self.append(blob)

    def _update_speed(self, total, nbytes, time):
        if not time:
            return
        self.last_speed = nbytes / time
        if self.mean_speed:
            self.mean_speed = total / self.mean_speed + time
            self.mean_speed = (total + nbytes) / self.mean_speed
        else:
            self.mean_speed = self.last_speed
