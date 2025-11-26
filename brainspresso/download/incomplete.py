__all__ = ['IncompleteFile']
# stdlib
import asyncio
import hashlib
import time
from pathlib import Path
from shutil import rmtree
from typing import IO, Literal
from logging import getLogger
from functools import partial

# externals
import aiofiles
import aiofiles.os as aos
from fasteners import InterProcessLock

# internals
from brainspresso.utils.digests import get_digester

lg = getLogger(__name__)
aop = aos.path


async def run_async(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, **kwargs), *args)


class IncompleteFile:
    """
    An object that represents a file being downloaded, in its
    incomplete state. It is used as a context manager and checks whether
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
        if checkalgo and not hasattr(checkalgo, hashlib):
            raise ValueError('Unknown hashing algorithm')
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
        self.lock: InterProcessLock | None = None
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
        if self._digest is not None:
            return self._digest
        elif self.digester:
            return self.digester.hexdigest()
        else:
            return None

    async def __aenter__(self) -> "IncompleteFile":
        self.filename.parent.mkdir(parents=True, exist_ok=True)

        # Acquire lock
        self.lock = await run_async(InterProcessLock, str(self.lockname))
        lg.debug(f"acquiring lock... {self.lockname}")
        if not await run_async(self.lock.acquire, blocking=False):
            raise RuntimeError(
                f'Could not acquire download lock for {self.filename}'
            )
        lg.debug(f"acquired lock: {self.lockname}")

        # Check if a file was already being downloaded, and if we should
        # continue from where we left off
        try:
            async with aiofiles.open(self.checkname, 'rt') as f:
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
            if self.digest:
                self.digester = await run_async(
                    get_digester, self.tempname, self.checkalgo
                )
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

        # Open file
        lg.debug(f"opening file ({mode}) ... {self.tempname}")
        self.file = await aiofiles.open(self.tempname, mode)
        self.offset = await self.file.tell()
        lg.debug(f"opened file ({mode}): {self.tempname}")

        # Write expected checksum
        if self.checksum:
            async with aiofiles.open(self.checkname, "w") as f:
                await f.write(self.checksum)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Close file
        lg.debug(f"closing file...  {self.tempname}")
        assert self.file is not None
        await self.file.close()
        lg.debug(f"closed file: {self.tempname}")

        # Rename temporary filename to output filename
        # Note that we only rename the file to its final name and
        # remove temporary file if the download was succesful (i.e.
        # the context was not interrupted by an exception)
        try:
            if exc_type is None:
                lg.debug(
                    f"renaming file...  {self.tempname} -> {self.filename}"
                )
                try:
                    await aos.rename(self.tempname, self.filename)
                    # await run_async(self.tempname.replace, self.filename)
                except IsADirectoryError:
                    await run_async(rmtree, self.filename)
                    await aos.rename(self.tempname, self.filename)
                    # await run_async(self.tempname.replace, self.filename)
                lg.debug(f"renamed file:  {self.tempname} -> {self.filename}")
                if self.digester:
                    lg.debug(f"saving digest...  {self.filename}")
                    self._digest = await run_async(self.digester.hexdigest)
                    lg.debug(f"saved digest:  {self.filename}")
        finally:
            # Release lock and delete existing files
            assert self.lock is not None
            lg.debug(f"releasing lock...  {self.lockname}")
            await run_async(self.lock.release)
            lg.debug(f"released lock:  {self.lockname}")
            if exc_type is None:
                lg.debug(f"deleting file...  {self.tempname}")
                await run_async(self.tempname.unlink, missing_ok=True)
                lg.debug(f"deleted file:  {self.tempname}")
                lg.debug(f"deleting file...  {self.lockname}")
                await run_async(self.lockname.unlink, missing_ok=True)
                lg.debug(f"deleted file:  {self.lockname}")
                lg.debug(f"deleting file...  {self.checkname}")
                await run_async(self.checkname.unlink, missing_ok=True)
                lg.debug(f"deleted file:  {self.checkname}")
            self.lock = None
            self.file = None
            self.offset = None

    async def append(self, blob: bytes) -> "IncompleteFile":
        if self.file is None:
            raise ValueError(
                'IncompleteFile.append() called outside of context manager'
            )
        if self.digest:
            await run_async(self.digest.update, blob)
        tic = time.time()
        await self.file.write(blob)
        toc = time.time()

        # timing
        new = len(blob)
        old = await self.file.tell() - new
        self._update_speed(old, new, toc-tic)
        return self

    async def write(self, blob: bytes) -> "IncompleteFile":
        return await self.append(blob)

    async def __add__(self, blob: bytes) -> "IncompleteFile":
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
