"""Remote files, read as streams of bytes."""
# std
import inspect
import time
from email.utils import parsedate_to_datetime
from logging import getLogger
from typing import Callable
from urllib.parse import ParseResult, urlparse

import aiohttp

# externals
import requests

# internals
from brainspresso.download.constants import CHUNK_SIZE
from brainspresso.download.session import new_session

lg = getLogger(__name__)


def check_request_options(opt: dict | None) -> dict:
    """
    Check the options passed to `aiohttp.ClientSession.request`.

    TLS verification
    is not one of them: it is the `insecure` option of downloads.
    """
    opt = dict(opt or {})
    tls = {"ssl", "verify", "verify_ssl"} & set(opt)
    if tls:
        raise TypeError(
            f"Request options {sorted(tls)} are not supported: use "
            f"`insecure=True` to skip TLS certificate verification"
        )
    return opt


class RemoteFile:
    """
    A remote file, whose bytes are downloaded.

    It is used as an async context manager.

    ```python
    async with RemoteFile(url) as f:
        obj = b''
        async for chunk in f:
            obj += chunk
    ```

    When `offset > 0`, a `Range` request is sent. If the server does not
    honor it (status 200 instead of 206) -- or if `if_range` is provided
    and the remote file has changed -- the full file is sent and
    `self.offset` is reset to 0. The caller must then restart writing from
    the beginning. If the requested range starts at the end of the file
    (status 416), `self.complete` is True and no bytes are yielded.
    """

    Session = aiohttp.ClientSession | requests.Session
    AUTH_STATUSES = (401, 403)

    def __init__(
            self,
            url: str | ParseResult,
            session: Session | Callable[[], Session] | dict | None = None,
            auth: Callable[[Session], None] | None = None,
            chunk_size: int = CHUNK_SIZE,
            offset: int = 0,
            get_opt: dict = {},
            if_range: str | None = None,
            insecure: bool = False,
    ):
        """
        Prepare the reading of a remote file.

        Parameters
        ----------
        url : str | ParseResult
            Remote URL
        session : Session | callable[[], Session] | dict | None
            Opened session, or function that returns a session, or
            dictionary of arguments passed to `aiohttp.ClientSession`.
        auth : callable[[Session], None]
            Authentication function. May be sync or async.
            It is called if the server answers 401 or 403.
        chunk_size : int
            Number of bytes to read at once
        offset : int
            Number of bytes to skip
        get_opt : dict
            Options passed to `get`
        if_range : str
            Validator (ETag or Last-Modified) of the partial file.
            If the remote file does not match it anymore, the server
            sends the full file.
        insecure : bool
            Do not verify TLS certificates
        """
        if not isinstance(url, ParseResult):
            url = urlparse(url)
        self.url = url
        self._session = session
        self.session = None
        self.session_is_mine = False
        self._auth = auth
        self.chunk_size = chunk_size
        self.get_opt = check_request_options(get_opt)
        self.insecure = insecure
        self.offset = offset
        self.if_range = if_range
        self.complete = False
        self.response = None
        self.iterator = None
        self.last_speed = None
        self.mean_speed = 0
        self.total = 0
        self._head = None

    # ------------------------------------------------------------------
    #   Session / auth
    # ------------------------------------------------------------------

    async def auth(self, session) -> None:
        """Run the authentication hook (if any) on `session`."""
        if self._auth is None:
            return
        result = self._auth(session)
        if inspect.isawaitable(result):
            await result

    async def _get_or_make_session(self) -> aiohttp.ClientSession:
        session = self._session
        if isinstance(session, aiohttp.ClientSession) and not session.closed:
            return session
        if isinstance(session, dict):
            session = new_session(**session)
        elif callable(session):
            session = session()
            if inspect.isawaitable(session):
                session = await session
        else:
            # None, a closed session, or a `requests.Session` (which
            # cannot be used in an async context)
            session = new_session()
            self.session_is_mine = True
        if not isinstance(session, aiohttp.ClientSession):
            raise TypeError(
                f"Expected a `aiohttp.ClientSession` but got: {session}"
            )
        return session

    async def _request(
        self, method: str, headers: dict | None = None
    ) -> aiohttp.ClientResponse:
        """
        Send a request, (re)authenticate if needed, and raise on error.

        The returned response must be released by the caller.
        """
        if self.session is None:
            self.session = await self._get_or_make_session()
        opt = dict(self.get_opt)
        headers = {**opt.pop("headers", {}), **(headers or {})}
        opt.setdefault("allow_redirects", True)
        if self.insecure:
            opt["ssl"] = False
        url = self.url.geturl()

        r = await self.session.request(method, url, headers=headers, **opt)
        if r.status in self.AUTH_STATUSES and self._auth is not None:
            r.release()
            lg.debug(f"{r.status} on {url}: authenticating")
            await self.auth(self.session)
            r = await self.session.request(method, url, headers=headers, **opt)
        if r.status == 416:
            # Requested range not satisfiable: let the caller decide
            return r
        if r.status >= 400:
            r.release()
            r.raise_for_status()
        return r

    # ------------------------------------------------------------------
    #   Metadata
    # ------------------------------------------------------------------

    async def _head_once(self) -> aiohttp.ClientResponse | None:
        """Single HEAD request, cached. Returns None on failure."""
        if self._head is None:
            try:
                r = await self._request("HEAD")
                r.release()
                self._head = r
            except Exception as e:
                lg.debug(f"HEAD failed on {self.url.geturl()}: {e}")
                self._head = False
            finally:
                if self.response is None and self.session_is_mine:
                    await self.session.close()
                    self.session = None
                    self.session_is_mine = False
        return self._head or None

    @staticmethod
    def _size_from(r: aiohttp.ClientResponse, offset: int = 0) -> int | None:
        if "Content-Range" in r.headers:
            total = r.headers["Content-Range"].rpartition("/")[2]
            if total.isdigit():
                return int(total)
        if r.status in (200, 206) and "Content-Length" in r.headers:
            length = int(r.headers["Content-Length"])
            return length + (offset if r.status == 206 else 0)
        return None

    @property
    async def size(self) -> int | None:
        """Size of the remote file (from the GET response if available)."""
        if self.response is not None:
            return self._size_from(self.response, self.offset)
        r = await self._head_once()
        return self._size_from(r) if r else None

    @property
    async def mtime(self):
        """Last-modified time of the remote file."""
        r = self.response if self.response is not None else (
            await self._head_once()
        )
        if r and "Last-Modified" in r.headers:
            try:
                return parsedate_to_datetime(r.headers["Last-Modified"])
            except (TypeError, ValueError):
                return None
        return None

    @property
    def validator(self) -> str | None:
        """Strong ETag, or Last-Modified, usable in an `If-Range` header."""
        if self.response is None:
            return None
        etag = self.response.headers.get("ETag", "")
        if etag and not etag.startswith("W/"):
            return etag
        return self.response.headers.get("Last-Modified", None)

    # ------------------------------------------------------------------
    #   Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self):
        """Send the request and check the response."""
        h = {}
        if self.offset:
            h["Range"] = f"bytes={self.offset}-"
            if self.if_range:
                h["If-Range"] = self.if_range
        self.response = await self._request("GET", headers=h)

        if self.response.status == 416:
            # The partial file is (at least) as large as the remote file.
            size = self._size_from(self.response)
            self.complete = (size is not None and size == self.offset)
            if not self.complete:
                self.response.release()
                raise aiohttp.ClientResponseError(
                    self.response.request_info, (),
                    status=416, message="Range Not Satisfiable",
                    headers=self.response.headers,
                )
            self.iterator = _empty()
        else:
            if self.offset and self.response.status != 206:
                # Range ignored, or remote file changed: restart
                lg.info(
                    f"{self.url.geturl()}: server sent full file, "
                    f"restarting from 0"
                )
                self.offset = 0
            self.iterator = self._timed_iterator(
                self.response.content.iter_chunked(self.chunk_size)
            )
        self.total = 0
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release (or close) the response."""
        if self.response is not None:
            if exc_type is None:
                self.response.release()
            else:
                # Body not fully read: do not put the connection back
                # in the pool.
                self.response.close()
        self.iterator = None
        self.response = None
        if self.session_is_mine and self.session is not None:
            await self.session.close()
            self.session = None
            self.session_is_mine = False

    async def _timed_iterator(self, iterator):
        tic = time.time()
        async for chunk in iterator:
            toc = time.time()
            self._update_speed(len(chunk), toc - tic)
            # total must be updated after update speed
            self.total += len(chunk)
            yield chunk
            tic = toc

    def __aiter__(self):
        """Iterate over chunks of bytes of the response."""
        return self.iterator

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


async def _empty():
    return
    yield
