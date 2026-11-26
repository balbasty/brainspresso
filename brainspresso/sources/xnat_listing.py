"""
Asynchronous listing of the files of an XNAT project (e.g. NITRC-IR).

XNAT's REST API needs one request per experiment to list files; they
are sent concurrently.

```python
experiments, files = list_project_files("https://www.nitrc.org/ir", "ixi")
for f in files:
    m = f.meta
    print(m["experiment"], m["scan"], m["collection"], f.name, f.size, f.url)
```

Each file is a `RemoteEntry` with path
`<experiment>/<scan>/<collection>/<name>` and `meta` keys `experiment`
(label, e.g. "2_Guys"), `experiment_id` (e.g. "NITRC_IR_E10452"),
`scan` (e.g. "T1"), `collection` (resource label, e.g. "NIfTI", "bval")
and `name`.
"""
from __future__ import annotations

import asyncio
from logging import getLogger
from typing import Callable

import aiohttp

from brainspresso.download.constants import RETRY_STATUSES
from brainspresso.download.session import new_session
from brainspresso.sources.base import RemoteEntry

lg = getLogger(__name__)


def xnat_entry(experiment, experiment_id, scan, collection, name, size, url):
    """A file of an XNAT scan, as a `RemoteEntry`"""
    return RemoteEntry(
        path=f"{experiment}/{scan}/{collection}/{name}",
        url=url,
        size=size,
        meta={"experiment": experiment, "experiment_id": experiment_id,
              "scan": scan, "collection": collection, "name": name},
    )


async def _get_json(
    session: aiohttp.ClientSession, url: str, attempts: int = 5, **opt
) -> dict:
    for attempt in range(attempts):
        try:
            async with session.get(url, params={"format": "json"},
                                   **opt) as r:
                if r.status in RETRY_STATUSES and attempt + 1 < attempts:
                    await asyncio.sleep(2 ** attempt)
                    continue
                r.raise_for_status()
                return await r.json(content_type=None)
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
            if attempt + 1 >= attempts:
                raise
            await asyncio.sleep(2 ** attempt)
    raise RuntimeError(f"Could not get {url}")


def _scan_from_uri(uri: str) -> str:
    # /data/experiments/<E>/scans/<scan>/resources/<id>/files/<name>
    parts = uri.split("/")
    try:
        return parts[parts.index("scans") + 1]
    except (ValueError, IndexError):
        return ""


async def list_project_files_async(
    server: str,
    project: str,
    keep_experiment: Callable[[dict], bool] | None = None,
    auth: aiohttp.BasicAuth | None = None,
    jobs: int = 8,
) -> tuple[list[dict], list[RemoteEntry]]:
    """
    List the experiments of a project and all their scan files.

    Parameters
    ----------
    server : str
        XNAT root URL (e.g. "https://www.nitrc.org/ir")
    project : str
        Project ID (e.g. "ixi")
    keep_experiment : callable(dict) -> bool
        Filter on experiment records (keys: ID, label, date, ...)
    auth : aiohttp.BasicAuth
        Credentials, if the project is not public
    jobs : int
        Number of concurrent requests

    Returns
    -------
    experiments : list[dict]
        Experiment records (after filtering)
    files : list[RemoteEntry]
    """
    server = server.rstrip("/")
    async with new_session(auth=auth) as session:
        data = await _get_json(
            session, f"{server}/data/projects/{project}/experiments"
        )
        experiments = data["ResultSet"]["Result"]
        if keep_experiment:
            experiments = [e for e in experiments if keep_experiment(e)]
        semaphore = asyncio.Semaphore(jobs)

        async def files_of(exp: dict) -> list[RemoteEntry]:
            async with semaphore:
                data = await _get_json(
                    session,
                    f"{server}/data/experiments/{exp['ID']}/scans/ALL/files",
                )
            out = []
            for f in data["ResultSet"]["Result"]:
                size = f.get("Size")
                out.append(xnat_entry(
                    experiment=exp.get("label", exp["ID"]),
                    experiment_id=exp["ID"],
                    scan=_scan_from_uri(f["URI"]),
                    collection=f.get("collection", ""),
                    name=f["Name"],
                    size=int(size) if size not in (None, "") else None,
                    url=f"{server}{f['URI']}",
                ))
            return out

        nested = await asyncio.gather(*map(files_of, experiments))
    return experiments, [f for files in nested for f in files]


def list_project_files(*args, **kwargs):
    """Synchronous version of `list_project_files_async`"""
    return asyncio.run(list_project_files_async(*args, **kwargs))
