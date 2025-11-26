# std
import time
from inspect import iscoroutinefunction
from typing import Callable
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, ParseResult

# externals
import requests
import aiohttp

# internals
from brainspresso.download.constants import CHUNK_SIZE


class RemoteFile:
    """
    This object represents a remote file, whose bytes are downloaded.
    It is used as a context manager.

    ```python
    with RemoteFile(url) as f:
        obj = b''
        for chunk in f:
            obj += chunk
    ```
    """

    Session = aiohttp.ClientSession | requests.Session
    REDIRECTION = (300, 301, 302, 303, 307, 308)

    def __init__(
            self,
            url: str | ParseResult,
            session: Session | Callable[[], Session] | dict | None = None,
            auth: Callable[[Session], None] | None = None,
            chunk_size: int = CHUNK_SIZE,
            offset: int = 0,
            get_opt: dict = {},
    ):
        """
        Parameters
        ----------
        url : str | ParseResult
            Remote URL
        session : Session | callable[[], Session] | dict | None
            Opened session, or function that returns a session, or
            dictionary of arguments passed to `aiohttp.ClientSession`.
        auth : callable[[Session], None]
            Authentification function
        chunk_size : int
            Number of bytes to read at once
        offset : int
            Number of bytes to skip
        get_opt : dict
            Options passed to `get`
        """
        if not isinstance(url, ParseResult):
            url = urlparse(url)
        self.url = url
        self._session = session or self._default_session
        self.session = None
        self.session_is_mine = session is None
        self._auth = auth or (lambda x: None)
        self.chunk_size = chunk_size
        self.get_opt = get_opt or {}
        self._has_range = None
        self.offset = offset
        self.response = None
        self.iterator = None
        self.buffer = None
        self.last_speed = None
        self.mean_speed = 0

    async def auth(self, *args, **kwargs):
        if iscoroutinefunction(self._auth.__call__):
            return await self._auth(*args, **kwargs)
        else:
            return self._auth(*args, **kwargs)

    async def _default_session(self, *args, **kwargs) -> Session:
        return aiohttp.ClientSession(*args, **kwargs)

    async def _get_or_make_session(self) -> Session:
        session = self._session
        if isinstance(session, dict):
            session = await self._default_session(**session)
        elif iscoroutinefunction(session):
            session = await session()
        elif callable(session):
            session = session()
        elif session is None:
            session = await self._default_session()
        if not isinstance(session, aiohttp.ClientSession):
            raise TypeError(
                f"Expected a `aiohttp.ClientSession` but got: {session}"
            )
        # await self.auth(session)
        return session

    @property
    async def has_range(self) -> bool:
        if self._has_range is None:
            self._has_range = await self._check_has_range()
        return self._has_range

    async def _check_has_range(self):
        answer = False
        try:
            h = {'Range': 'bytes=0-0'}
            r = await self._try_head(self.url.geturl(), headers=h)
            answer = (r.status == 206)
        finally:
            return answer

    @property
    async def size(self):
        """Try to guess the file size from remote"""
        if self.response:
            if 'Content-Range' in self.response.headers:
                return int(
                    self.response.headers['Content-Range'].split('/')[-1]
                )
            elif 'Content-Length' in self.response.headers:
                return int(self.response.headers['Content-Length'])
            else:
                return None
        else:
            size = None
            try:
                r = await self._try_head(self.url.geturl())
                if r.status == 200 and 'Content-Length' in r.headers:
                    size = int(r.headers['Content-Length'])
            finally:
                return size

    @property
    async def mtime(self):
        """Try to guess the "last-modified" time from remote"""
        if self.response:
            if 'Last-Modified' in self.response.headers:
                return parsedate_to_datetime(
                    self.response.headers['Last-Modified']
                )
            else:
                return None
        else:
            mtime = None
            try:
                r = await self._try_head(self.url.geturl())
                if r.status == 200 and 'Last-Modified' in r.headers:
                    mtime = parsedate_to_datetime(r.headers['Last-Modified'])
            finally:
                return mtime

    async def _try_get(self, url, *args, **kwargs):
        kwargs.update(self.get_opt)
        if self.session is None:
            self.session = await self._get_or_make_session()
        r = await self.session.head(url, *args, **kwargs)
        if r.status in self.REDIRECTION:
            url = urlparse(r.headers['Location'])
            r = self.session.head(url, *args, **kwargs)
        if r.status not in (200, 206) and self.auth:
            await self.auth(self.session)
            r = await self.session.head(url, *args, **kwargs)
        r = await self.session.get(url, *args, **kwargs)
        return r

    async def _try_head(self, url, *args, **kwargs):
        kwargs.update(self.get_opt)
        if self.session is None:
            self.session = await self._get_or_make_session()
        r = await self.session.head(url, *args, **kwargs)
        if r.status in self.REDIRECTION:
            url = urlparse(r.headers['Location'])
            r = self.session.head(url, *args, **kwargs)
        if r.status not in (200, 206) and self.auth:
            await self.auth(self.session)
            r = await self.session.head(url, *args, **kwargs)
        return r

    async def __aenter__(self):
        # open session
        if self.session is None:
            self.session = await self._get_or_make_session()
        # open content streamer
        h = {}
        if self.offset and (await self.has_range):
            h['Range'] = f'bytes={self.offset}-'
        self.response = await self._try_get(self.url.geturl(), headers=h)
        # get content chunk iterator
        self.iterator = self._timed_iterator(
            self.response.content.iter_chunked(self.chunk_size)
        )
        # skip offset if range not available
        if self.offset and not (await self.has_range):
            await self._skip(self.offset)
        else:
            self.total = 0
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.iterator = None
        self.response = None
        self.buffer = None
        if self.session_is_mine:
            await self.session.close()
            self.session = None

    async def _timed_iterator(self, iterator):
        tic = time.time()
        async for chunk in iterator:
            toc = time.time()
            self._update_speed(len(chunk), toc - tic)
            # total must be updated after update speed
            self.total += len(chunk)
            yield chunk
            tic = toc

    async def _skip(self, nbytes):
        self.buffer = None
        self.total = 0
        try:
            while nbytes > 0:
                chunk = await anext(self.iterator)
                nbytes -= len(chunk)
        except StopIteration:
            pass
        if nbytes < 0:
            self.buffer = chunk[nbytes:]

    def __aiter__(self):
        return self.iter()

    async def iter(self):
        if self.buffer is not None:
            yield self.buffer
        async for chunk in self.iterator:
            yield chunk

    def _update_speed(self, nbytes, time):
        if not time:
            return
        MOM = 0.9
        new_speed = nbytes / time
        old_speed = self.mean_speed
        if old_speed:
            mean_speed = (1-MOM) * self.total / old_speed + MOM * time
            mean_speed = ((1-MOM) * self.total + MOM * nbytes) / mean_speed
        else:
            mean_speed = new_speed
        self.mean_speed = mean_speed
        self.last_speed = new_speed
