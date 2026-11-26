"""
NITRC project downloads (`*.projects.nitrc.org`).

They are static URLs behind the NITRC website login.

NITRC's certificate chain is often incomplete, so these sources skip TLS
verification by default (`insecure = True`), and download one file at a
time (`jobs = 1`): the host throttles concurrent downloads.

```python
source = NITRC({"raw": [url1, url2], "meta": [url3]}).with_options(
    user=user, password=password)
source.harvest(layout, keys=["meta"])
```
"""
from __future__ import annotations

from brainspresso.sources.base import StaticURLs
from brainspresso.sources.credentials import (
    NITRC_LOGIN,
    NitrcAuth,
    get_credentials,
    nitrc_credentials,
)

__all__ = ["NITRC", "NitrcAuth", "nitrc_credentials", "get_credentials"]


class NITRC(StaticURLs):
    """
    Source: files of NITRC projects, with the NITRC login.

    Per-run options (`with_options`): `user`, `password` (else
    `NITRC_USER`/`NITRC_PASS`, else a prompt). `login = False` for files
    that need no login.

    Parameters
    ----------
    urls : dict[str, list[str | RemoteEntry]]
        URLs per key (see `StaticURLs`).
    **kwargs
        Options of `StaticURLs` (and `user`, `password`).
    """

    name = "nitrc"
    insecure = True
    jobs = 1
    login = True
    login_url = NITRC_LOGIN
    user: str | None = None
    password: str | None = None

    def auth(self):
        """Return the NITRC login hook (None if `login` is false)."""
        if not self.login:
            return None
        auth = self.__dict__.get("_auth")
        if auth is None or (self.user and auth.user != self.user):
            auth = NitrcAuth(self.user, self.password, url=self.login_url,
                             insecure=self.insecure)
            self._auth = auth
        return auth
