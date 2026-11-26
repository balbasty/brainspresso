"""
Credentials of the sources: one helper, used by every source.

A secret comes from (in this order) the value given on the command
line, environment variables, an interactive prompt (only if the standard
input is a terminal), else a `CredentialsError` that says how to give it.

```python
user, password = get_credentials("NITRC", user, password,
                                 env_user=["NITRC_USER"],
                                 env_password=["NITRC_PASS"])
token = get_secret("Dataverse token", token, env=["DATAVERSE_TOKEN"],
                   hidden=True)
```

Authentication hooks of the downloads (`Downloader(auth=...)`) are
asynchronous and picklable: `NitrcAuth` (NITRC website login) and
`XnatAuth` (XNAT `JSESSION`, e.g. NITRC-IR).
"""
from __future__ import annotations

import getpass
import os
import sys
from logging import getLogger
from typing import Sequence

import aiohttp

lg = getLogger(__name__)

__all__ = [
    "CredentialsError", "get_secret", "get_credentials", "nitrc_credentials",
    "NitrcAuth", "XnatAuth", "NITRC_LOGIN",
]

NITRC_LOGIN = "https://www.nitrc.org/account/login.php"
NITRC_USER = ("NITRC_USER", "XNAT_USER")
NITRC_PASS = ("NITRC_PASS", "XNAT_PASS")


class CredentialsError(ValueError):
    """Credentials are needed but none were given"""


def interactive() -> bool:
    """Whether the user can be prompted (standard input is a terminal)"""
    try:
        return sys.stdin is not None and sys.stdin.isatty()
    except (AttributeError, ValueError):
        return False


def prompt(message: str, hidden: bool = False) -> str:
    """Ask the user (a function, so that tests can replace it)"""
    return getpass.getpass(message) if hidden else input(message)


def get_secret(
    what: str,
    value: str | None = None,
    env: Sequence[str] = (),
    *,
    hidden: bool = False,
    option: str | None = None,
) -> str:
    """
    A credential: `value` if given, else the first environment variable
    of `env` that is set, else a prompt, else a `CredentialsError`.

    Parameters
    ----------
    what : str
        What is asked (e.g. "NITRC password"), shown in the prompt and
        in the error
    value : str, optional
        Value given by the user (e.g. `--password`)
    env : list[str]
        Environment variables, in order of preference
    hidden : bool
        Do not echo the answer of the prompt
    option : str, optional
        Command-line option that gives it (for the error message)
    """
    if value:
        return value
    if isinstance(env, str):
        env = [env]
    for name in env:
        value = os.environ.get(name)
        if value:
            return value
    if interactive():
        try:
            value = prompt(f"{what}: ", hidden)
        except (EOFError, KeyboardInterrupt):
            value = None
        if value:
            return value
    how = []
    if option:
        how.append(f"use `{option}`")
    if env:
        how.append("set " + " or ".join(f"`{e}`" for e in env))
    raise CredentialsError(
        f"{what} is required" + (f": {', or '.join(how)}" if how else "")
        + (" (no terminal to prompt for it)." if not interactive() else ".")
    )


def get_credentials(
    service: str,
    user: str | None = None,
    password: str | None = None,
    *,
    env_user: Sequence[str] = (),
    env_password: Sequence[str] = (),
) -> tuple[str, str]:
    """`(user, password)` of a service (see `get_secret`)"""
    user = get_secret(f"{service} user", user, env_user, option="--user")
    password = get_secret(f"{service} password", password, env_password,
                          hidden=True, option="--password")
    return user, password


def nitrc_credentials(
    user: str | None = None, password: str | None = None
) -> tuple[str, str]:
    """NITRC (and NITRC-IR) credentials: `NITRC_USER`/`NITRC_PASS`
    (or `XNAT_USER`/`XNAT_PASS`)"""
    return get_credentials("NITRC", user, password,
                           env_user=NITRC_USER, env_password=NITRC_PASS)


def basic_auth(user: str, password: str) -> str:
    """Value of a basic `Authorization` header"""
    encode = getattr(aiohttp, "encode_basic_auth", None)
    if encode is not None:
        return encode(user, password)
    return aiohttp.BasicAuth(user, password).encode()


class NitrcAuth:
    """
    Asynchronous authentication hook for the NITRC website (project
    downloads, `*.projects.nitrc.org`): logs in with the account form,
    the session then holds the login cookie.

    Credentials are resolved when the hook is created (before downloads
    start). It is a class, so that it can be pickled.
    """

    def __init__(
        self,
        user: str | None = None,
        password: str | None = None,
        *,
        url: str = NITRC_LOGIN,
        insecure: bool = True,
    ):
        self.user, self.password = nitrc_credentials(user, password)
        self.url = url
        self.insecure = insecure

    def __repr__(self):
        return f"NitrcAuth({self.user!r}, url={self.url!r})"

    async def __call__(self, session: aiohttp.ClientSession):
        if not isinstance(session, aiohttp.ClientSession):
            raise TypeError(
                f"NitrcAuth expects an aiohttp session, got {session!r}"
            )
        data = {"form_loginname": self.user, "form_pw": self.password}
        opt = {"ssl": False} if self.insecure else {}
        async with session.post(self.url, data=data, **opt) as r:
            await r.read()
            if r.status >= 400:
                lg.warning(f"NITRC login failed ({r.status})")
        return session


class XnatAuth:
    """
    Asynchronous authentication hook for an XNAT server: opens a session
    (`POST <server>/data/JSESSION` with basic credentials); its cookie
    is then sent with every request of the aiohttp session.
    """

    def __init__(
        self,
        server: str,
        user: str | None = None,
        password: str | None = None,
        *,
        insecure: bool = False,
        service: str = "NITRC",
        env_user: Sequence[str] = NITRC_USER,
        env_password: Sequence[str] = NITRC_PASS,
    ):
        self.server = server.rstrip("/")
        self.user, self.password = get_credentials(
            service, user, password,
            env_user=env_user, env_password=env_password,
        )
        self.insecure = insecure

    def __repr__(self):
        return f"XnatAuth({self.server!r}, {self.user!r})"

    @property
    def headers(self) -> dict:
        """Basic authentication header"""
        return {"Authorization": basic_auth(self.user, self.password)}

    async def __call__(self, session: aiohttp.ClientSession):
        opt = {"ssl": False} if self.insecure else {}
        async with session.post(f"{self.server}/data/JSESSION",
                                headers=self.headers, **opt) as r:
            await r.read()
            if r.status >= 400:
                lg.warning(f"XNAT login on {self.server} failed "
                           f"({r.status})")
        return session
