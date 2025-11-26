import os
import getpass

import requests
import aiohttp

from brainspresso.download import Downloader


Session = requests.Session | aiohttp.ClientSession

servers = {
    "default": "https://demo.dataverse.org",
    "harvard": "https://dataverse.harvard.edu",
}


def get_credentials(token=None):
    methods = ['DATAVERSE_TOKEN', getpass.getpass, ValueError]

    if not token:
        for method in methods:
            if isinstance(method, str):
                token = os.environ.get(method, None)
            elif callable(method):
                token = methods('Dataverse token: ')
            elif isinstance(method, type) and issubclass(method, Exception):
                raise ValueError(
                    'Could not get Dataverse token. '
                    'Set environment variable `DATAVERSE_TOKEN`'
                )

    return token


class Dataverse:

    TOKEN_HEADER = "X-Dataverse-key"

    def __init__(
        self,
        token: str | None = None,
        server: str | None = None,
        open: bool = False,
        keep_open: bool = True,
    ):
        self.token = get_credentials(token)
        self.server = server or servers["default"]
        if "http" not in self.server:
            self.server = servers[self.server]
        self.server = self.server.rstrip("/")
        self.session = None
        self._keep_open = None
        self._keep_open_default = keep_open
        if open:
            self.open()

    @property
    def headers(self) -> dict[str, str]:
        return {self.TOKEN_HEADER: self.token}

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            raise RuntimeError(
                'Session not open. Call `dataverse.open()` or use as context '
                '`with dataverse: ...`')
        return self._session

    @property
    async def asession(self) -> requests.Session:
        if self._asession is None:
            raise RuntimeError(
                'Session not open. Call `dataverse.open_async()` or use '
                'as context `with dataverse: ...`')
        return self._asession

    @session.setter
    def session(self, value: Session):
        if isinstance(value, requests.Session):
            self._session = value
        elif isinstance(value, aiohttp.ClientSession):
            self._asession = value
        elif value is None:
            self._session = self._asession = None
        raise TypeError(type(value))

    @property
    def keep_open(self) -> bool:
        if self._keep_open is None:
            return self._keep_open_default
        else:
            return self._keep_open

    @keep_open.setter
    def keep_open(self, value):
        self._keep_open = value

    @property
    def is_open(self) -> bool:
        return self._session is not None or self._session_async is not None

    @property
    def is_closed(self) -> bool:
        return not self.is_open

    def get(self, *args, **kwargs) -> requests.Response:
        kwargs.setdefault("headers", {})
        kwargs["headers"].update(self.headers)
        return self.session.get(*args, **kwargs)

    def head(self, *args, **kwargs) -> requests.Response:
        kwargs.setdefault("headers", {})
        kwargs["headers"].update(self.headers)
        return self.session.head(*args, **kwargs)

    def open(self, keep_open: bool | None = None):
        if keep_open is not None:
            self.keep_open = keep_open
        if self.is_open:
            return self
        self.session = requests.Session()
        return self

    def close(self):
        if self.is_closed:
            return self
        self.session.close()
        self.session = None
        self.keep_open = None
        return self

    def __enter__(self):
        self._was_open = self.is_open
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        if not self._was_open:
            self.close()
        delattr(self, '_was_open')
        return self

    async def aget(self, *args, **kwargs) -> aiohttp.ClientResponse:
        kwargs.setdefault("headers", {})
        kwargs["headers"].update(self.headers)
        session = await self.asession
        return await session.get(*args, **kwargs)

    async def ahead(self, *args, **kwargs) -> aiohttp.ClientResponse:
        kwargs.setdefault("headers", {})
        kwargs["headers"].update(self.headers)
        session = await self.asession
        return await session.head(*args, **kwargs)

    async def aopen(self, keep_open: bool | None = None):
        if keep_open is not None:
            self.keep_open = keep_open
        if self.is_open:
            return self
        self.session = await aiohttp.ClientSession()
        return self

    async def aclose(self):
        if self.is_closed:
            return self
        await self.session.close()
        self.session = None
        self.keep_open = None
        return self

    async def __aenter__(self):
        self._was_open = self.is_open
        await self.aopen()
        return self

    async def __aexit__(self, type, value, traceback):
        if not self._was_open:
            await self.aclose()
        delattr(self, '_was_open')
        return self

    def get_dataset_downloader(
        self,
        id: str,
        version: str | None = None,
    ) -> Downloader:
        src = f"${self.server}/api/access/dataset/:persistentId/"
        if version:
            src += f"versions/{version}"
        src += f"?persistentId={id}"
        dst = id.replace(":", "-").replace("/", "-")
        if version:
            dst += f"_{version}"
        dst += ".zip"
        return Downloader(
            src=src,
            dst=dst,
            session=self.session,
            get_opt=self.headers,
        )

    def get_file_downloader(
        self,
        id: str,
        version: str | None = None,
    ) -> Downloader:
        src = f"${self.server}/api/access/datafile/:persistentId/"
        if version:
            src += f"versions/{version}"
        src += f"?persistentId={id}"
        dst = id.replace(":", "-").replace("/", "-")
        if version:
            dst += f"_{version}"
        dst += ".zip"
        return Downloader(
            src=src,
            dst=dst,
            session=self.session,
            get_opt=self.headers,
        )
