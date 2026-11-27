"""Download of a single remote file into a local file."""
import asyncio
import datetime
import os
import os.path as op
import random
import time
from enum import Enum as _Enum
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import AsyncIterator, Callable, Literal
from urllib.parse import ParseResult, urlparse

import aiofiles.os as aos
import aiohttp
from humanize import naturaldate, naturalsize

from brainspresso.download.constants import CHUNK_SIZE, RETRY_STATUSES
from brainspresso.download.incomplete import IncompleteFile
from brainspresso.download.remote import RemoteFile, check_request_options
from brainspresso.utils.digests import get_digest, sort_digests

lg = getLogger(__name__)
aop = aos.path


async def run_async(func, *args, **kwargs):
    """Run a blocking function in the default executor.

    Parameters
    ----------
    func : callable
        Blocking function.
    *args, **kwargs
        Arguments of `func`.

    Returns
    -------
    object
        The value returned by `func`.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, **kwargs), *args)


async def lexists(path: str) -> bool:
    """Whether `path` exists (broken symlinks included), asynchronously."""
    return await run_async(op.lexists, path)


class IfExists:
    """
    Behaviour of a download when its destination file exists.

    This class both:
    - holds the set of singleton values (as an Enum)
    - defines constant (such as the default value)
    - serves as a context manager to override whichever value was set.

    ```python
    action = Action(..., ifexists='overwrite')
    with IfExists('refresh'):
        yield from action
    ```
    """

    Choice = Literal['skip', 'overwrite', 'different', 'refresh', 'error']

    class Enum(_Enum):
        """Singleton values of the `ifexists` option."""

        SKIP = S = 1
        OVERWRITE = O = 2       # noqa: E741
        DIFFERENT = D = 3
        REFRESH = R = 4
        ERROR = E = 5

    # Expose values
    SKIP = Enum.SKIP
    OVERWRITE = Enum.OVERWRITE
    DIFFERENT = Enum.DIFFERENT
    REFRESH = Enum.REFRESH
    ERROR = Enum.ERROR

    # Set (class attribute) default
    default: Enum = DIFFERENT
    current: Enum | None = None

    @classmethod
    def from_any(cls, x: str | int | Enum | None) -> Enum:
        """Return the singleton representation of a value."""
        if x is None:
            return cls.default
        elif isinstance(x, str):
            return getattr(cls.Enum, x[0].upper())
        else:
            return cls.Enum(x)

    def __init__(self, value: Choice | Enum) -> None:
        self.value = self.from_any(value)
        self._prev = None

    def __enter__(self) -> None:
        """Set this value as the current one."""
        self._prev = type(self).current
        type(self).current = self.value

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore the previous value."""
        type(self).current = self._prev
        self._prev = None


class Downloader:
    r"""
    An object that knows how to download a file.

    While downloading, the downloader yields regular status messages.
    Possible status are

    * intermediate status:
        {
            'done': int,        # total number of bytes downloaded
            'done%': float,     # percentage of the file downloaded
        }
    * error status:
        {
            'status': 'error',  # An error happened
            'message': str,     # Description of the error
        }
    * file finished downloading but checksum differs:
        {
            'checksum': 'differs',  # Checksum if not the same as expected
            'status': 'error',      # An error happened
            'message': msg,         # Description of the error
        }
    * file finished downloading and checksum matches:
        {
            'checksum': 'ok',
            'status': 'done',
        }
    * file finished downloading and checksum cannot be checked:
        {
            'checksum': '-',        # Checksum was not provided
            'status': 'done',
        }

    ```python
    downloader = Downloader(url, filename)
    for status in downloader:
        if 'done' in status:
            print('downloaded: {status["done"]}B', end='')
            if 'done%' in status:
                print('({status["done%"]}%)', end='')
            print(end='\r')
        else:
            print('\n')
            if status['status'] == 'error':
                print('error: {status["message"]}')
            if 'checksum' in status:
                print('checksum: {status["checksum"]}')
            if status['status'] == 'done':
                print('done.')
    ```
    """

    # Derived from `dandi.download`
    # https://github.com/dandi/dandi-cli/blob/master/dandi/download.py
    # Apache License Version 2.0

    RETRY_STATUSES = RETRY_STATUSES

    def __init__(
        self,
        src: str | ParseResult,
        dst: str | Path | None = None,
        *,
        ifexists: IfExists.Enum | IfExists.Choice = 'different',
        chunk_size: int = CHUNK_SIZE,
        session: aiohttp.ClientSession | None = None,
        auth: Callable[[aiohttp.ClientSession], None] = None,
        size: int | None = None,
        mtime: datetime.datetime | None = None,
        digests: dict[str, str] | None = None,
        ifnodigest: Literal['restart', 'continue'] = 'continue',
        max_attempts: int = 6,
        get_opt: dict = {},
        insecure: bool = False,
        **kwargs,
    ):
        """
        Prepare the download of a remote file.

        Parameters
        ----------
        src : str | ParseResult
            Remote URL
        dst : str | Path | None
            Output filename

        Other Parameters
        ----------------
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
            Behaviour if destination file already exists
        chunk_size : int
            Number of bytes to read at once
        session : Session
            Opened session
        size : int | None
            Expected size of the file
        mtime : datetime
            Expected last-modified time
        digests : dict | None
            Expected digest(s) of the file.
            Keys are algorithm names (e.g. "sha256") and values are the
            digests.
        ifnodigest : {'restart', 'continue'}
            Behaviour if incomplete file exists but no digest is provided.
            With 'continue', the download resumes with a `Range` request.
            The remote ETag/Last-Modified is sent as `If-Range`, so the
            server sends the full file if it changed in between.
        max_attempts : int
            Maximum number of attempts. Retries resume from where the
            previous attempt stopped.
        get_opt : dict
            Options passed to `aiohttp.ClientSession.request`
            (e.g. `headers`).
        insecure : bool
            Do not verify the TLS certificate of the server (and of the
            servers it redirects to).
        """
        # backward compatibility (typo)
        max_attempts = kwargs.pop('max_attemps', max_attempts)
        if kwargs:
            raise TypeError(f'Unexpected arguments: {list(kwargs)}')
        if not isinstance(src, ParseResult):
            src = urlparse(src)
        self.src = src
        dst = Path(dst or '.')
        if dst.is_dir():
            dst = dst / PurePosixPath(src.path).name
        self.dst = dst
        self.session = session
        self.auth = auth
        self.size = size
        self.mtime = mtime
        self.chunk_size = chunk_size
        if digests:
            digests = sort_digests(digests)
        self.digests = digests
        self.ifnodigest = ifnodigest
        self.ifexists = IfExists.from_any(ifexists)
        self.max_attempts = max_attempts
        self.get_opt = check_request_options(get_opt)
        self.insecure = insecure
        self._stat_done = False

    @property
    async def size(self) -> int:
        """Size of the remote file, in bytes (fetched on first use)."""
        if self._size is None:
            await self._get_size_mtime()
        return self._size

    @size.setter
    def size(self, value: int | None) -> None:
        self._size = value

    @property
    async def mtime(self) -> int:
        """Last-modified time of the remote file (fetched on first use)."""
        if self._mtime is None:
            await self._get_size_mtime()
        return self._mtime

    @mtime.setter
    def mtime(self, value: int | None) -> None:
        self._mtime = value

    async def _get_size_mtime(self) -> None:
        # A single HEAD request, done at most once
        if self._stat_done:
            return
        self._stat_done = True
        if self._size is None or self._mtime is None:
            remote = RemoteFile(
                self.src,
                session=self.session,
                auth=self.auth,
                get_opt=self.get_opt,
                insecure=self.insecure,
            )
            if self._size is None:
                self.size = await remote.size
            if self._mtime is None:
                self.mtime = await remote.mtime

    async def _should_overwrite(self) -> AsyncIterator[dict | bool]:
        if not (await lexists(self.dst)):
            lg.info(f'File {self.dst!s} does not exits: download')
            yield True
            return

        ifexists = IfExists.current or self.ifexists

        if ifexists is IfExists.ERROR:
            lg.error(f'File {self.dst!s} already exists')
            yield {'status': 'error', 'message': 'already exists'}
            yield False
            return

        if ifexists is IfExists.SKIP:
            lg.info(f'File {self.dst!s} already exists: skip')
            yield {'status': 'skipped', 'message': 'already exists'}
            yield False
            return

        if ifexists is IfExists.OVERWRITE:
            lg.info(f'File {self.dst!s} already exists: overwrite')
            yield True
            return

        if ifexists is IfExists.DIFFERENT:
            size = await self.size
            dst = await run_async(self.dst.resolve)
            stat = await aos.stat(dst)
            if (size is not None and size != stat.st_size):
                lg.info(
                    f'Size of {self.dst!s} does not match size on server '
                    f'({naturalsize(size)} != {naturalsize(stat.st_size)})'
                    f': redownloading'
                )
                yield True
                return

            if self.digests:
                checkalgo, checksum = next(iter(self.digests.items()))
                local_checksum = await run_async(
                    get_digest, self.dst, checkalgo
                )
                if checksum == local_checksum:
                    lg.info(f'File {self.dst!s} is same as remote: skip')
                    yield {'status': 'skipped', 'message': 'already exists'}
                    yield False
                    return
                else:
                    lg.info(
                        f'Checksum of {self.dst!s} does not match '
                        f'checksum on server ({checksum} != {local_checksum})'
                        f': redownloading'
                    )

            yield True
            return

        if ifexists is IfExists.REFRESH:
            mtime = await self.mtime
            if mtime is None:
                lg.warning(
                    f'{self.dst!s} - no mtime in the record: '
                    f'redownloading'
                )
                yield True
                return
            size = await self.size
            if size is None:
                lg.warning(
                    f'{self.dst!s} - no size in the record: '
                    f'redownloading'
                )
                yield True
                return
            local_stat = await aos.stat(await run_async(self.dst.resolve))
            local_size = local_stat.st_size
            local_mtime = datetime.datetime.fromtimestamp(local_stat.st_mtime)
            local_mtime = local_mtime.astimezone(datetime.timezone.utc)
            if local_mtime == mtime and local_size == size:
                lg.info(f'File {self.dst!s} is fresh enough: skip')
                yield {'status': 'skipped', 'message': 'already exists'}
                yield False
                return
            else:
                lg.info(
                    f'File {self.dst!s} is not fresh '
                    f'({naturalsize(size)} != {naturalsize(local_size)} ||'
                    f'{naturaldate(mtime)} != {naturaldate(local_mtime)})'
                    f': redownload'
                )
                yield True
                return

        lg.info(f'File {self.dst!s} (uncaught case): redownload')
        yield True
        return

    def __aiter__(self) -> AsyncIterator[dict]:
        """Iterate over progress reports of the download (see `iter`)."""
        return self.iter()

    async def iter(self) -> AsyncIterator[dict]:
        """Download the file."""
        # --------------------------------------------------------------
        # If file exists, select replacement strategy
        # --------------------------------------------------------------
        should_overwrite = False
        async for status in self._should_overwrite():
            if isinstance(status, bool):
                should_overwrite = status
            else:
                yield status
        if not should_overwrite:
            return

        if self.digests:
            checkalgo, checksum = next(iter(self.digests.items()))
        else:
            checksum = checkalgo = None

        # --------------------------------------------------------------
        # Download (each attempt resumes from the partial file)
        # --------------------------------------------------------------
        for attempt in range(self.max_attempts):
            try:
                async for status in self._attempt(checksum, checkalgo):
                    yield status
                return

            # When a ValueError is raised, it's because the caller
            # provided invalid parameters (e.g., an invalid URL), and so
            # retrying won't change anything.
            except ValueError:
                raise

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                msg = _format_exc(exc)
                status = getattr(exc, "status", None)
                if status is not None and status not in RETRY_STATUSES:
                    lg.info(f'Download failed: {self.dst!s} | {msg}')
                    yield {'status': 'error', 'message': msg}
                    return
                if 1 + attempt >= self.max_attempts:
                    lg.info(
                        f'Download failed after {self.max_attempts} '
                        f'attempts: {self.dst!s} | {msg}'
                    )
                    yield {'status': 'error', 'message': msg}
                    return
                delay = _retry_delay(exc, attempt)
                lg.info(
                    f'Failed to download {self.dst!s} on attempt '
                    f'#{attempt+1:d}: {msg}. Retrying in {delay:.1f}s.'
                )
                yield {
                    'status': f'retry {attempt+1}',
                    'message': msg,
                }
                await asyncio.sleep(delay)

    async def _attempt(self, checksum, checkalgo) -> AsyncIterator[dict]:
        """Make a single download attempt. Raises on network errors."""
        async with IncompleteFile(
            self.dst,
            checksum=checksum,
            checkalgo=checkalgo,
            ifnochecksum=self.ifnodigest,
        ) as local_file:

            assert local_file.offset is not None
            if self._size is not None and local_file.offset == self._size:
                # Exit early when downloaded == size, as making
                # a Range request in such a case results in a
                # 416 error from S3.
                downloaded = local_file.offset
            elif self._size is not None and local_file.offset > self._size:
                await local_file.truncate()
                downloaded = None
            else:
                downloaded = None

            if downloaded is None:
                async with RemoteFile(
                    self.src,
                    session=self.session,
                    auth=self.auth,
                    chunk_size=self.chunk_size,
                    offset=local_file.offset,
                    get_opt=self.get_opt,
                    insecure=self.insecure,
                    if_range=local_file.validator,
                ) as remote_file:

                    if remote_file.offset != local_file.offset:
                        # Server sent the full file: restart from 0
                        await local_file.truncate()
                    downloaded = local_file.offset

                    # Size/mtime from the response itself (no HEAD)
                    size = await remote_file.size
                    if self._size is None:
                        self._size = size
                    elif size is not None and size != self._size:
                        lg.warning(
                            f'{self.dst!s}: expected size {self._size} '
                            f'but server announces {size}'
                        )
                    size = self._size
                    if self._mtime is None:
                        self._mtime = await remote_file.mtime
                    self._stat_done = True
                    if remote_file.validator and not downloaded:
                        await local_file.set_validator(
                            remote_file.validator
                        )
                    yield {'size': size}

                    warned = False
                    mean_speed = 0
                    tic = time.time()
                    async for chunk in remote_file:
                        nbytes = len(chunk)
                        downloaded += nbytes
                        out = {'done': downloaded}
                        if size:
                            if downloaded > size and not warned:
                                warned = True
                                lg.warning(
                                    'Downloaded %d bytes although size '
                                    'was told to be just %d.',
                                    downloaded, size,
                                )
                            out['done%'] = 100 * downloaded / size
                        await local_file.append(chunk)

                        # Update total speed
                        toc = time.time()
                        mean_speed = _update_speed(
                            mean_speed, downloaded - nbytes,
                            nbytes, toc-tic
                        )
                        tic = toc
                        out['dspeed'] = remote_file.mean_speed
                        out['wspeed'] = local_file.mean_speed
                        out['tspeed'] = mean_speed
                        yield out

            # ----------------------------------------------------------
            # Checks (inside the context: on failure, the partial file
            # is kept and the next attempt resumes from it)
            # ----------------------------------------------------------
            if self._size is not None and downloaded < self._size:
                raise aiohttp.ClientPayloadError(
                    f'Connection closed after {downloaded} of '
                    f'{self._size} bytes'
                )

            if checksum:
                dlchecksum = local_file.digest
                if dlchecksum != checksum:
                    # Corrupted: delete partial file, do not retry
                    local_file.discard()
                    msg = (
                        f'{checkalgo}: '
                        f'downloaded {dlchecksum} != {checksum}'
                    )
                    lg.info(f'{self.dst!s} is different: {msg}.')
                    yield {
                        'checksum': 'differs',
                        'status': 'error',
                        'message': msg,
                    }
                    return
                lg.info(
                    'Verified that %s has correct %s %s',
                    self.dst, checkalgo, dlchecksum
                )
                yield {'checksum': 'ok'}
            else:
                yield {'checksum': '-'}

        # File has been renamed to its final name
        mtime = self._mtime
        if mtime is not None:
            yield {'status': 'setting mtime'}
            times = (time.time(), mtime.timestamp())
            dst = await run_async(self.dst.resolve)
            await run_async(os.utime, dst, times)

        yield {'status': 'done'}


def _format_exc(exc: BaseException) -> str:
    msg = str(exc)
    name = exc.__class__.__name__
    return f'[{name}] {msg}' if msg else f'[{name}]'


def _retry_delay(exc: BaseException, attempt: int) -> float:
    """Exponential backoff with jitter, honoring `Retry-After`."""
    headers = getattr(exc, "headers", None) or {}
    retry_after = headers.get("Retry-After", None)
    if retry_after:
        try:
            return min(300.0, float(retry_after))
        except ValueError:
            pass
    return min(60.0, 2.0 ** attempt) * (0.5 + random.random())


def _update_speed(old_speed, prev_bytes, nbytes, time):
    if not time:
        return old_speed
    MOM = 0.9
    new_speed = nbytes / time
    if old_speed:
        mean_speed = (1-MOM) * prev_bytes / old_speed + MOM * time
        mean_speed = ((1-MOM) * prev_bytes + MOM * nbytes) / mean_speed
    else:
        mean_speed = new_speed
    return mean_speed
