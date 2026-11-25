"""
Asynchronous listing of the files of an XNAT project (e.g. NITRC-IR).

XNAT's REST API needs one request per experiment to list files; they
are sent concurrently.

```python
files = list_project_files("https://www.nitrc.org/ir", "ixi")
for f in files:
    print(f.experiment, f.scan, f.collection, f.name, f.size, f.url)
```
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from logging import getLogger
from typing import Callable

import aiohttp

from brainspresso.download.constants import RETRY_STATUSES, TIMEOUT

lg = getLogger(__name__)


@dataclass(frozen=True)
class XNATFile:
    experiment: str         # experiment label (e.g. "2_Guys")
    experiment_id: str      # experiment ID (e.g. "NITRC_IR_E10452")
    scan: str               # scan ID (e.g. "T1")
    collection: str         # resource label (e.g. "NIfTI", "bval")
    name: str               # file name
    size: int | None
    url: str


async def _get_json(
    session: aiohttp.ClientSession, url: str, attempts: int = 5
) -> dict:
    for attempt in range(attempts):
        try:
            async with session.get(url, params={"format": "json"}) as r:
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
) -> tuple[list[dict], list[XNATFile]]:
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
    files : list[XNATFile]
    """
    server = server.rstrip("/")
    async with aiohttp.ClientSession(timeout=TIMEOUT, auth=auth) as session:
        data = await _get_json(
            session, f"{server}/data/projects/{project}/experiments"
        )
        experiments = data["ResultSet"]["Result"]
        if keep_experiment:
            experiments = [e for e in experiments if keep_experiment(e)]
        semaphore = asyncio.Semaphore(jobs)

        async def files_of(exp: dict) -> list[XNATFile]:
            async with semaphore:
                data = await _get_json(
                    session,
                    f"{server}/data/experiments/{exp['ID']}/scans/ALL/files",
                )
            out = []
            for f in data["ResultSet"]["Result"]:
                size = f.get("Size")
                out.append(XNATFile(
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
