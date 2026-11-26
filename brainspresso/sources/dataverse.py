"""
Dataverse repositories (e.g. https://dataverse.harvard.edu).

Files are downloaded by persistent identifier
(`<server>/api/access/datafile/:persistentId/?persistentId=doi:...`),
with an API token in the `X-Dataverse-key` header (datasets under terms
of use need one).

```python
source = Dataverse({"raw": {"GSP_part1.tar": "doi:10.7910/DVN/25833/D9DWAI"}},
                   server="harvard").configure(token=token)
source.harvest(layout)
```
"""
from __future__ import annotations

from typing import Mapping
from urllib.parse import quote

from brainspresso.sources.base import RemoteEntry, StaticURLs
from brainspresso.sources.credentials import get_secret

servers = {
    "default": "https://demo.dataverse.org",
    "harvard": "https://dataverse.harvard.edu",
}

TOKEN_HEADER = "X-Dataverse-key"
TOKEN_ENV = ("DATAVERSE_TOKEN",)


def get_token(token: str | None = None) -> str:
    """Dataverse API token: given, else `DATAVERSE_TOKEN`, else a prompt."""
    return get_secret("Dataverse token", token, TOKEN_ENV, hidden=True,
                      option="--token")


def server_url(server: str | None) -> str:
    """URL of a Dataverse server, from its URL or a name of `servers`."""
    server = server or servers["default"]
    if "://" not in server:
        server = servers[server]
    return server.rstrip("/")


def file_url(server: str, persistent_id: str,
             version: str | None = None) -> str:
    """Download URL of a file, by persistent identifier."""
    url = f"{server_url(server)}/api/access/datafile/:persistentId/"
    if version:
        url += f"versions/{version}"
    return url + f"?persistentId={quote(persistent_id, safe=':/')}"


class Dataverse(StaticURLs):
    """
    Source: files of a Dataverse repository, by persistent identifier.

    Parameters
    ----------
    files : dict[str, dict[str, str]]
        `{key: {local name: persistent id}}`
    server : str
        Server URL or a name of `servers`
    token_required : bool
        Ask for a token (else, use one only if given or in the
        environment)

    Per-run option: `token`.
    """

    name = "dataverse"
    insecure = True
    jobs = 1
    token: str | None = None
    token_required: bool = True

    def __init__(self, files: Mapping[str, Mapping[str, str]],
                 server: str | None = "harvard", *,
                 token_required: bool | None = None, **kwargs):
        self.server = server_url(server)
        urls = {
            key: [RemoteEntry(name, file_url(self.server, pid),
                              meta={"persistent_id": pid})
                  for name, pid in names.items()]
            for key, names in files.items()
        }
        super().__init__(urls, **kwargs)
        if token_required is not None:
            self.token_required = token_required

    def download_options(self, options) -> dict:
        """Add the API token (if any) to the options of `Source`."""
        opts = super().download_options(options)
        if options.dry_run:
            return opts
        token = get_token(self.token) if self.token_required else \
            (self.token or None)
        if token:
            opts["get_opt"] = {"headers": {TOKEN_HEADER: token}}
        return opts
