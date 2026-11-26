"""
Girder data management platform (e.g. https://data.kitware.com).

Girder organises data as collections → folders (nested) → items → files.
Its REST API (`/api/v1`) is public for public collections; private ones
need an API token (`Girder-Token` header).

```python
girder = Girder("https://data.kitware.com/api/v1")
for f in girder.walk("collection", "54b582c38d777f4362aa9cb3"):
    print(f.path, f.size, f.sha512)
```

Paths are built from folder and item names. An item that holds a
single file with the same name as the item maps to `<folders>/<item>`;
other items map to `<folders>/<item>/<file>`.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator, Literal
from urllib.parse import urlparse

import aiohttp

from brainspresso.download import CHUNK_SIZE, Downloader, IfExists
from brainspresso.download.constants import RETRY_STATUSES
from brainspresso.download.session import new_session

lg = getLogger(__name__)

KITWARE = "https://data.kitware.com/api/v1"
PAGE = 500

ResourceType = Literal["collection", "folder"]


@dataclass(frozen=True)
class GirderFile:
    path: PurePosixPath         # relative to the root of the walk
    file_id: str
    size: int | None
    sha512: str | None
    url: str
    item_id: str = ""
    meta: dict = field(default_factory=dict, compare=False, hash=False)


@dataclass(frozen=True)
class GirderFolder:
    path: PurePosixPath         # relative to the root of the walk
    folder_id: str
    meta: dict = field(default_factory=dict, compare=False, hash=False)
    description: str = ""


def parse_girder_url(url: str) -> tuple[str, ResourceType, str]:
    """
    Parse a Girder web URL into `(api, type, id)`.

    `https://data.kitware.com/#collection/<id>` →
    `("https://data.kitware.com/api/v1", "collection", "<id>")`.
    Also accepts `#folder/<id>` and `#collection/<id>/folder/<id>`
    (the deepest resource wins).
    """
    parsed = urlparse(url)
    api = f"{parsed.scheme}://{parsed.netloc}/api/v1"
    parts = [p for p in parsed.fragment.split("/") if p]
    rtype, rid = None, None
    for kind, ident in zip(parts[::2], parts[1::2]):
        if kind in ("collection", "folder"):
            rtype, rid = kind, ident
    if rtype is None:
        raise ValueError(f"Not a Girder collection/folder URL: {url}")
    return api, rtype, rid


class Girder:
    """Minimal async client for the Girder REST API (read-only)"""

    def __init__(
        self,
        api: str = KITWARE,
        token: str | None = None,
        jobs: int = 8,
        max_attempts: int = 5,
    ):
        self.api = api.rstrip("/")
        self.token = token or os.environ.get("GIRDER_TOKEN") or None
        self.jobs = jobs
        self.max_attempts = max_attempts

    @property
    def headers(self) -> dict:
        return {"Girder-Token": self.token} if self.token else {}

    def download_url(self, file_id: str) -> str:
        return f"{self.api}/file/{file_id}/download"

    # ------------------------------------------------------------------
    async def _get(self, session, path: str, **params):
        url = f"{self.api}/{path.lstrip('/')}"
        for attempt in range(self.max_attempts):
            try:
                async with session.get(url, params=params) as r:
                    if (
                        r.status in RETRY_STATUSES and
                        attempt + 1 < self.max_attempts
                    ):
                        await asyncio.sleep(2 ** attempt)
                        continue
                    if r.status in (401, 403):
                        raise PermissionError(
                            f"{url}: access denied ({r.status}). "
                            f"Private data needs a Girder API token "
                            f"(--token or GIRDER_TOKEN)."
                        )
                    r.raise_for_status()
                    return await r.json(content_type=None)
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
                if attempt + 1 >= self.max_attempts:
                    raise
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"Could not get {url}")

    async def _get_all(self, session, path: str, **params) -> list[dict]:
        """GET a paginated list endpoint"""
        out, offset = [], 0
        while True:
            page = await self._get(
                session, path, limit=PAGE, offset=offset, **params
            )
            out.extend(page)
            if len(page) < PAGE:
                return out
            offset += PAGE

    async def info(self, rtype: ResourceType, rid: str) -> dict:
        """Metadata of a collection or folder (name, size, description)"""
        async with new_session(headers=self.headers) as session:
            return await self._get(session, f"{rtype}/{rid}")

    async def walk_async(
        self,
        rtype: ResourceType,
        rid: str,
        keep_folder: Callable[[PurePosixPath], bool] | None = None,
        folders: list[GirderFolder] | None = None,
    ) -> list[GirderFile]:
        """
        List all files under a collection or folder.

        Parameters
        ----------
        rtype : {"collection", "folder"}
        rid : str
            Collection or folder ID
        keep_folder : callable(PurePosixPath) -> bool
            Called on each sub-folder path (relative to the root);
            sub-folders for which it returns False are not explored.
        folders : list, optional
            If provided, the folders that are explored (with their
            metadata) are appended to it.
        """
        semaphore = asyncio.Semaphore(self.jobs)
        files: list[GirderFile] = []

        async with new_session(headers=self.headers) as session:

            async def get_all(path, **params):
                async with semaphore:
                    return await self._get_all(session, path, **params)

            async def item_files(item: dict, prefix: PurePosixPath):
                entries = await get_all(f"item/{item['_id']}/files")
                single = (
                    len(entries) == 1 and entries[0]["name"] == item["name"]
                )
                for f in entries:
                    path = (
                        prefix / item["name"] if single
                        else prefix / item["name"] / f["name"]
                    )
                    files.append(GirderFile(
                        path=path,
                        file_id=f["_id"],
                        size=f.get("size"),
                        sha512=f.get("sha512") or None,
                        url=self.download_url(f["_id"]),
                        item_id=item["_id"],
                        meta=item.get("meta") or {},
                    ))

            async def folder(ptype: str, pid: str, prefix: PurePosixPath):
                subfolders = await get_all(
                    "folder", parentType=ptype, parentId=pid
                )
                items = (
                    await get_all("item", folderId=pid)
                    if ptype == "folder" else []
                )
                tasks = []
                for sub in subfolders:
                    path = prefix / sub["name"]
                    if keep_folder and not keep_folder(path):
                        continue
                    if folders is not None:
                        folders.append(GirderFolder(
                            path=path,
                            folder_id=sub["_id"],
                            meta=sub.get("meta") or {},
                            description=sub.get("description") or "",
                        ))
                    tasks.append(folder("folder", sub["_id"], path))
                tasks += [item_files(item, prefix) for item in items]
                await asyncio.gather(*tasks)

            await folder(rtype, rid, PurePosixPath())

        if folders is not None:
            folders.sort(key=lambda f: str(f.path))
        return sorted(files, key=lambda f: str(f.path))

    def walk(self, *args, **kwargs) -> list[GirderFile]:
        """Synchronous version of `walk_async`"""
        return asyncio.run(self.walk_async(*args, **kwargs))

    # ------------------------------------------------------------------
    def mirror(
        self,
        rtype: ResourceType,
        rid: str,
        dst: str | Path,
        keep: Callable[[GirderFile], bool] | None = None,
        *,
        rename: Callable[[GirderFile], PurePosixPath | None] | None = None,
        keep_folder: Callable[[PurePosixPath], bool] | None = None,
        ifexists: IfExists.Choice = "skip",
        chunk_size: int = CHUNK_SIZE,
        folders: list[GirderFolder] | None = None,
    ) -> Iterator[Downloader]:
        """
        Yield one downloader per file under a collection or folder,
        saved under `dst` with the same relative path (or `rename(f)`).

        Sizes and SHA-512 checksums (when Girder computed them) come from
        the listing, so downloads are verified.
        """
        dst = Path(dst)
        headers = self.headers
        listing = self.walk(
            rtype, rid, keep_folder=keep_folder, folders=folders
        )
        for f in listing:
            if keep is not None and not keep(f):
                continue
            local = rename(f) if rename else f.path
            if local is None:
                continue
            yield Downloader(
                f.url,
                dst / local,
                size=f.size,
                digests={"sha512": f.sha512} if f.sha512 else None,
                ifexists=ifexists,
                chunk_size=chunk_size,
                get_opt={"headers": headers} if headers else {},
            )
