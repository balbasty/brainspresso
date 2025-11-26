import asyncio
import aiofiles.os as aos
import time
import random
import os
import os.path as op
import datetime
from enum import Enum as _Enum
from logging import getLogger
from pathlib import Path, PosixPath
from typing import Literal, Callable, AsyncIterator
from urllib.parse import urlparse, ParseResult
from functools import partial

import aiohttp
import aiohttp.web
from humanize import naturalsize, naturaldate

from braindataprep.utils.digests import get_digest
from braindataprep.utils.digests import sort_digests
from braindataprep.download.remote import RemoteFile
from braindataprep.download.incomplete import IncompleteFile
from braindataprep.download.constants import CHUNK_SIZE

lg = getLogger(__name__)
aop = aos.path


async def run_async(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, **kwargs), *args)


async def lexists(path: str) -> bool:
    return await run_async(op.lexists, path)


class IfExists:
    """
    This class both:
    - holds the set of singleton values (as an Enum)
    - defines constant (such as the default value)
    - serves as a context manager to override whichever value was set

    ```python
    action = Action(..., ifexists='overwrite')
    with IfExists('refresh'):
        yield from action
    ```
    """

    Choice = Literal['skip', 'overwrite', 'different', 'refresh', 'error']

    class Enum(_Enum):
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
        """Return the singleton representation of a value"""
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
        self._prev = type(self).current
        type(self).current = self.value

    def __exit__(self, exc_type, exc_val, exc_tb):
        type(self).current = self._prev
        self._prev = None


class Downloader:
    """
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
            'status': 'error',  # An error happended
            'message': str,     # Description of the error
        }
    * file finished downloading but checksum differs:
        {
            'checksum': 'differs',  # Checksum if not the same as expected
            'status': 'error',      # An error happended
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

    RETRY_STATUSES: list[int] = [
        400,    # Bad Request - https://github.com/dandi/dandi-cli/issues/87
        500,    # Internal Server Error
        502,    # Bad Gateway
        503,    # Service Unavailable
        504,    # Gateway Timeout
    ]

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
        ifnodigest: Literal['restart', 'continue'] = 'restart',
        max_attemps: int = 3,
        get_opt: dict = {},
    ):
        """
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
            Behaviour if incomplete file exists but no digest is provided
        max_attempts : int
            Maximum number of attempts
        """
        if not isinstance(src, ParseResult):
            src = urlparse(src)
        self.src = src
        dst = Path(dst or '.')
        if dst.is_dir():
            dst = dst / PosixPath(src.path).name
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
        self.max_attemps = max_attemps
        self.get_opt = get_opt

    @property
    async def size(self) -> int:
        if self._size is None:
            await self._get_size_mtime()
        return self._size

    @size.setter
    def size(self, value: int | None) -> None:
        self._size = value

    @property
    async def mtime(self) -> int:
        if self._mtime is None:
            await self._get_size_mtime()
        return self._mtime

    @mtime.setter
    def mtime(self, value: int | None) -> None:
        self._mtime = value

    async def _get_size_mtime(self) -> None:
        if self._size is None or self._mtime is None:
            remote = RemoteFile(
                self.src,
                session=self.session,
                auth=self.auth,
                get_opt=self.get_opt,
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
                local_checksum = get_digest(self.dst, checkalgo)
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
        return self.iter()

    async def iter(self) -> AsyncIterator[dict]:
        """
        Download the file
        """
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

        # --------------------------------------------------------------
        # Read size and mtime from remote
        # --------------------------------------------------------------
        size = await self.size
        yield {'size': size}

        # --------------------------------------------------------------
        # Download
        # --------------------------------------------------------------
        for attempt in range(self.max_attemps):
            try:
                warned = False
                if self.digests:
                    checksum, checkalgo = next(iter(self.digests.items()))
                else:
                    checksum = checkalgo = None
                async with IncompleteFile(
                    self.dst,
                    checksum=checksum,
                    checkalgo=checkalgo,
                    ifnochecksum=self.ifnodigest,
                ) as local_file:

                    assert local_file.offset is not None
                    downloaded = local_file.offset
                    if size is not None and downloaded == size:
                        # Exit early when downloaded == size, as making
                        # a Range request in such a case results in a
                        # 416 error from S3. Problems will result if
                        # `size` is None but we've already downloaded
                        # everything.
                        break

                    async with RemoteFile(
                        self.src,
                        session=self.session,
                        auth=self.auth,
                        chunk_size=self.chunk_size,
                        offset=local_file.offset,
                        get_opt=self.get_opt,
                    ) as remote_file:

                        mean_speed = 0
                        tic = time.time()
                        async for chunk in remote_file:
                            nbytes = len(chunk)
                            downloaded += nbytes
                            out = {'done': downloaded}
                            if size:
                                if downloaded > size and not warned:
                                    warned = True
                                    # Yield ERROR?
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

                    dlchecksum = local_file.digest

                # ------------------------------------------------------
                # success! -> a few checks then break out of trials loop
                # ------------------------------------------------------

                if checksum and dlchecksum:

                    if dlchecksum != checksum:
                        msg = (
                            f'{checkalgo}: '
                            f'downloaded {dlchecksum} != {checksum}'
                        )
                        lg.info('{self.dst!s} is different: {msg}.')
                        yield {
                            'checksum': 'differs',
                            'status': 'error',
                            'message': msg,
                        }
                        return
                    else:
                        lg.info(
                            'Verified that %s has correct %s %s',
                            self.dst, checkalgo, dlchecksum
                        )
                        yield {'checksum': 'ok'}

                else:
                    yield {'checksum': '-'}

                mtime = await self.mtime
                if mtime is not None:
                    yield {'status': 'setting mtime'}
                    times = (time.time(), mtime.timestamp())
                    dst = await run_async(self.dst.resolve)
                    await run_async(os.utime, dst, times)

                yield {'status': 'done'}
                return

            # ----------------------------------------------------------
            # An exception was raised
            # ----------------------------------------------------------

            # When `requests` raises a ValueError, it's because the caller
            # provided invalid parameters (e.g., an invalid URL), and so
            # retrying won't change anything.
            except ValueError:
                raise

            # Catching RequestException lets us retry on timeout & connection
            # errors (among others) in addition to HTTP status errors.
            except (
                aiohttp.web.HTTPException,
                asyncio.TimeoutError
            ) as exc:
                # TODO: actually we should probably retry only on
                # selected codes, and also respect Retry-After
                defst = self.RETRY_STATUSES[0]
                if (
                    1 + attempt >= self.max_attemps or
                    getattr(exc, "status", defst) not in self.RETRY_STATUSES
                ):
                    lg.info(f'Download failed: {str(exc)}')
                    yield {'status': 'error', 'message': str(exc)}
                    return
                # if is_access_denied(exc) or attempt >= 2:
                #     raise
                # sleep a little and retry
                lg.info(
                    f'Failed to download on attempt #{attempt:d}: {str(exc)}, '
                    f'will sleep a bit and retry'
                )
                asyncio.sleep(random.random() * 5)


def _update_speed(old_speed, prev_bytes, nbytes, time):
    if not time:
        return
    MOM = 0.9
    new_speed = nbytes / time
    if old_speed:
        mean_speed = (1-MOM) * prev_bytes / old_speed + MOM * time
        mean_speed = ((1-MOM) * prev_bytes + MOM * nbytes) / mean_speed
    else:
        mean_speed = new_speed
    return mean_speed
