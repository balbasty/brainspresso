"""
XNAT servers (NITRC-IR, formerly XNAT Central).

This module holds an asynchronous client of the archive REST API, and a source
that downloads the server-built archives of scans and assessors
(`.../scans/<id>/files?format=tar.gz`).

```python
class OASIS3(XNAT):
    project = "OASIS3"
    def keep_scan(self, subject, experiment, scan, wants): ...

OASIS3().with_options(user=..., password=...).harvest(layout, selection)
```

Each scan (or assessor) is one entry, saved as
`<experiment>/<scan id>.tar.gz` (`local_path`), with `meta` keys
`project`, `subject`, `experiment`, `id`, `type` ("scan"/"assessor"),
`record` (the XNAT record: `ID`, `type`, `label`...).
"""
from __future__ import annotations

import asyncio
import fnmatch
from logging import getLogger
from typing import Iterable

import aiohttp

from brainspresso.download.constants import RETRY_STATUSES
from brainspresso.download.session import new_session
from brainspresso.sources.base import (
    RemoteEntry, Source, SourceUnavailable, as_wants,
)
from brainspresso.sources.credentials import XnatAuth

lg = getLogger(__name__)

# XNAT Central was decommissioned in May 2024; its projects (OASIS) moved
# to NITRC-IR, which uses NITRC credentials.
DEFAULT_SERVER = "https://www.nitrc.org/ir"


class XNATClient:
    """
    Minimal asynchronous client of the XNAT archive API (read-only).

    It is used as an async context manager:

    ```python
    async with XNATClient(server, auth) as xnat:
        subjects = await xnat.subjects("OASIS3")
    ```

    With `auth` (an `XnatAuth`), a session is opened first (JSESSION
    cookie), and reopened once if the server answers 401/403.

    Parameters
    ----------
    server : str
        URL of the server.
    auth : XnatAuth, optional
        Authentication hook.
    jobs : int
        Maximum number of concurrent requests.
    insecure : bool
        Do not verify the TLS certificate.
    max_attempts : int
        Maximum number of attempts of a request.
    """

    def __init__(self, server: str = DEFAULT_SERVER,
                 auth: XnatAuth | None = None, *, jobs: int = 8,
                 insecure: bool = False, max_attempts: int = 5):
        self.server = server.rstrip("/")
        self.auth = auth
        self.jobs = jobs
        self.insecure = insecure
        self.max_attempts = max_attempts
        self.session = None
        self._semaphore = None

    async def __aenter__(self):
        """Open the HTTP session (and the XNAT session)."""
        self.session = new_session()
        self._semaphore = asyncio.Semaphore(self.jobs)
        if self.auth is not None:
            await self.auth(self.session)
        return self

    async def __aexit__(self, *exc):
        """Close the XNAT session (if any) and the HTTP session."""
        if self.auth is not None:
            try:
                async with self.session.delete(
                    f"{self.server}/data/JSESSION", **self._opt
                ) as r:
                    await r.read()
            except Exception:       # logout is best effort
                pass
        await self.session.close()
        self.session = None

    @property
    def _opt(self) -> dict:
        return {"ssl": False} if self.insecure else {}

    async def get_json(self, path: str) -> dict:
        """GET `<server>/data/<path>?format=json` (with retries)."""
        url = f"{self.server}/data/{path.lstrip('/')}"
        authenticated = False
        async with self._semaphore:
            # a re-authentication is not counted as an attempt
            attempt = 0
            while attempt < self.max_attempts:
                try:
                    async with self.session.get(
                        url, params={"format": "json"}, **self._opt
                    ) as r:
                        if r.status in (401, 403) and self.auth and \
                                not authenticated:
                            authenticated = True
                            await r.read()
                            await self.auth(self.session)
                            continue
                        if r.status in RETRY_STATUSES and \
                                attempt + 1 < self.max_attempts:
                            await asyncio.sleep(2 ** attempt)
                            attempt += 1
                            continue
                        if r.status in (401, 403):
                            raise PermissionError(
                                f"{url}: access denied ({r.status})"
                            )
                        r.raise_for_status()
                        return await r.json(content_type=None)
                except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
                    if attempt + 1 >= self.max_attempts:
                        raise
                    await asyncio.sleep(2 ** attempt)
                attempt += 1
        raise RuntimeError(f"Could not get {url}")

    async def results(self, path: str) -> list[dict]:
        """Return the records (`ResultSet.Result`) of a JSON listing."""
        data = await self.get_json(path)
        try:
            return data["ResultSet"]["Result"]
        except (KeyError, TypeError):
            return []

    def _base(self, project, subject=None, experiment=None) -> str:
        path = f"archive/projects/{project}"
        if subject:
            path += f"/subjects/{subject}"
        if experiment:
            path += f"/experiments/{experiment}"
        return path

    async def subjects(self, project: str) -> list[dict]:
        """List the subjects of a project."""
        return await self.results(f"{self._base(project)}/subjects")

    async def experiments(self, project: str,
                          subject: str | None = None) -> list[dict]:
        """List the experiments of a project (or of a subject)."""
        return await self.results(
            f"{self._base(project, subject)}/experiments"
        )

    async def scans(self, project, subject, experiment) -> list[dict]:
        """List the scans of an experiment."""
        return await self.results(
            f"{self._base(project, subject, experiment)}/scans"
        )

    async def assessors(self, project, subject, experiment) -> list[dict]:
        """List the assessors of an experiment."""
        return await self.results(
            f"{self._base(project, subject, experiment)}/assessors"
        )

    def archive_url(self, project, subject, experiment, id,
                    type: str = "scan") -> str:
        """URL of the tar.gz archive of a scan or assessor."""
        base = self._base(project, subject, experiment)
        return f"{self.server}/data/{base}/{type}s/{id}/files?format=tar.gz"


class XNAT(Source):
    """
    Source: scans and assessors of an XNAT project.

    They are downloaded as server-built archives
    (`<experiment>/<id>.tar.gz`).

    Subclasses refine the listing with hooks (all return True by
    default, except `keep_assessor`):

    * `keep_experiment(subject, experiment, wants)`: records of the
      subject and experiment (`label`, `ID`...)
    * `keep_scan(subject, experiment, scan, wants)`: scan record (`ID`,
      `type`...)
    * `keep_assessor(subject, experiment, assessor, wants)`

    Subjects are pruned with the label mapping (`labels`, from the
    subject label). Per-run options: `user`, `password`.

    Parameters
    ----------
    project : str, optional
        XNAT project.
    server : str, optional
        URL of the server.
    **kwargs
        Options of `Source` (and `user`, `password`, `login`...).
    """

    name = "xnat"
    server: str = DEFAULT_SERVER
    project: str = ""
    login: bool = True
    jobs = 1
    user: str | None = None
    password: str | None = None
    list_jobs: int = 8

    def __init__(self, project: str | None = None, *,
                 server: str | None = None, **kwargs):
        super().__init__(**kwargs)
        if project is not None:
            self.project = project
        if server is not None:
            self.server = server.rstrip("/")

    # ------------------------------------------------------------------
    def auth(self) -> XnatAuth | None:
        """Return the XNAT login hook (None if `login` is false)."""
        if not self.login:
            return None
        auth = self.__dict__.get("_auth")
        if auth is None or (self.user and auth.user != self.user):
            auth = XnatAuth(self.server, self.user, self.password,
                            insecure=self.insecure)
            self._auth = auth
        return auth

    def client(self) -> XNATClient:
        """Return a client of the server (with the login hook)."""
        return XNATClient(self.server, self.auth(), jobs=self.list_jobs,
                          insecure=self.insecure)

    def local_path(self, entry: RemoteEntry) -> str | None:
        """Path of an entry: `<experiment>/<id>.tar.gz`."""
        return entry.path

    # ------------------------------------------------------------------
    #   Hooks
    # ------------------------------------------------------------------

    def keep_experiment(self, subject: dict, experiment: dict,
                        wants) -> bool:
        """Whether to list an experiment (hook; default: True)."""
        return True

    def keep_scan(self, subject: dict, experiment: dict, scan: dict,
                  wants) -> bool:
        """Whether to fetch a scan (hook; default: True)."""
        return True

    def keep_assessor(self, subject: dict, experiment: dict,
                      assessor: dict, wants) -> bool:
        """Whether to fetch an assessor (hook; default: False)."""
        return False

    def subject_ids(self, subjects: list[dict], selection) -> list[dict]:
        """Subjects to list (pruned with the label mapping)."""
        return [s for s in subjects
                if self.keeps_subject(selection, s.get("label", ""))]

    def extra(self, client: XNATClient, selection, wants) -> list:
        """Entries outside the subject tree (coroutine allowed)."""
        return []

    # ------------------------------------------------------------------
    def entry(self, client: XNATClient, subject: dict, experiment: dict,
              record: dict, type: str = "scan") -> RemoteEntry:
        """Build the entry of a scan or assessor record.

        Parameters
        ----------
        client : XNATClient
            Client (for URLs).
        subject, experiment, record : dict
            XNAT records.
        type : {"scan", "assessor"}
            Type of the record.

        Returns
        -------
        RemoteEntry
        """
        sub, exp = subject["label"], experiment["label"]
        id = record["ID"] if type == "scan" else record.get(
            "label", record["ID"])
        return RemoteEntry(
            path=f"{exp}/{id}.tar.gz",
            url=client.archive_url(self.project, sub, exp, id, type),
            meta={"project": self.project, "subject": sub,
                  "experiment": exp, "id": id, "type": type,
                  "record": record},
        )

    async def list_async(self, selection=None, keys=None
                         ) -> list[RemoteEntry]:
        """List scans and assessors (async version of `list`).

        Parameters
        ----------
        selection : Selection | None
            Subject selection.
        keys : Wants | list[str] | None
            Wanted keys.

        Returns
        -------
        list[RemoteEntry]
        """
        wants = as_wants(keys)
        async with self.client() as client:
            out = []
            extra = self.extra(client, selection, wants)
            if asyncio.iscoroutine(extra):
                extra = await extra
            out += list(extra)
            subjects = self.subject_ids(
                await client.subjects(self.project), selection
            )

            async def subject_entries(subject):
                entries = []
                experiments = await client.experiments(
                    self.project, subject["label"])
                for exp in experiments:
                    if not self.keep_experiment(subject, exp, wants):
                        continue
                    scans, assessors = await asyncio.gather(
                        client.scans(self.project, subject["label"],
                                     exp["label"]),
                        client.assessors(self.project, subject["label"],
                                         exp["label"]),
                    )
                    entries += [
                        self.entry(client, subject, exp, s, "scan")
                        for s in scans
                        if self.keep_scan(subject, exp, s, wants)
                    ]
                    entries += [
                        self.entry(client, subject, exp, a, "assessor")
                        for a in assessors
                        if self.keep_assessor(subject, exp, a, wants)
                    ]
                return entries

            for entries in await asyncio.gather(
                *map(subject_entries, subjects)
            ):
                out += entries
        return out

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        """List scans and assessors.

        Parameters
        ----------
        selection : Selection | None
            Subject selection.
        keys : Wants | list[str] | None
            Wanted keys.

        Returns
        -------
        list[RemoteEntry]

        Raises
        ------
        SourceUnavailable
            If the server cannot be reached.
        """
        try:
            return asyncio.run(self.list_async(selection, keys))
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
            raise SourceUnavailable(
                f"{self.server}: [{type(e).__name__}] {e}"
            ) from e


def filter_list(full_list: Iterable[str],
                patterns: str | Iterable[str] | None) -> list[str]:
    """Elements of a list that match glob patterns (all if None)."""
    full_list = list(full_list)
    if not patterns:
        return full_list
    if isinstance(patterns, str):
        patterns = [patterns]
    elems = []
    for pattern in patterns:
        elems.extend(fnmatch.filter(full_list, pattern))
    return elems
