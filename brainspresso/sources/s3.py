"""
Anonymous listing of public S3 buckets.

It uses the `ListObjectsV2` REST API over aiohttp (no boto/fsspec
dependency).

```python
bucket = S3Bucket("openneuro.org")
async for obj in bucket.list("ds000224/"):
    print(obj.key, obj.size, bucket.url(obj.key))
```

`S3Prefix` is the source of the files under a prefix (see
`sources.base`); `fcp.FCP` is the one of the INDI bucket.

Notes
-----
* Path-style URLs (`https://s3.<region>.amazonaws.com/<bucket>/<key>`)
  are used, because bucket names that contain dots (`openneuro.org`)
  break TLS with virtual-host-style URLs.
* Buckets outside us-east-1 answer `301 PermanentRedirect` on the
  global endpoint; the region is read from the `x-amz-bucket-region`
  header and remembered.
* The ETag of an object is its MD5 only if it was uploaded in a single
  part (multipart ETags end with `-<nparts>`); see `S3Object.md5`.
"""
from __future__ import annotations

import asyncio
import datetime
import re
from dataclasses import dataclass
from logging import getLogger
from typing import AsyncIterator, Iterator
from urllib.parse import quote, urlparse
from xml.etree import ElementTree

import aiohttp

from brainspresso.download.constants import RETRY_STATUSES
from brainspresso.download.session import new_session
from brainspresso.sources.base import (
    RemoteEntry,
    Source,
    SourceUnavailable,
    as_wants,
)

lg = getLogger(__name__)

_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
_MD5 = re.compile(r"^[0-9a-f]{32}$")


@dataclass(frozen=True)
class S3Object:
    """An object in a bucket, as returned by `ListObjectsV2`."""

    key: str
    size: int
    mtime: datetime.datetime | None = None
    etag: str | None = None

    @property
    def md5(self) -> str | None:
        """MD5 of the object, if it can be inferred from its ETag."""
        etag = (self.etag or "").strip('"').lower()
        return etag if _MD5.match(etag) else None

    def entry(self, url: str, path: str | None = None) -> RemoteEntry:
        """Return the object as a `RemoteEntry` (`path` defaults to key)."""
        return RemoteEntry(
            path=self.key if path is None else path,
            url=url,
            size=self.size,
            mtime=self.mtime,
            digests={"md5": self.md5} if self.md5 else {},
            meta={"key": self.key},
        )


def parse_s3_url(url: str) -> tuple[str, str]:
    """
    Split an S3 URL into `(bucket, prefix)`.

    Parameters
    ----------
    url : str
        `s3://bucket/prefix`, or a path-style https URL.

    Returns
    -------
    bucket, prefix : str
    """
    parsed = urlparse(url)
    if parsed.scheme == "s3":
        return parsed.netloc, parsed.path.lstrip("/")
    if parsed.scheme in ("http", "https"):
        bucket, _, prefix = parsed.path.lstrip("/").partition("/")
        return bucket, prefix
    raise ValueError(f"Not an S3 URL: {url}")


class S3Bucket:
    """A public (anonymously readable) S3 bucket."""

    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        endpoint: str | None = None,
        session: aiohttp.ClientSession | None = None,
        max_attempts: int = 5,
    ):
        """
        Prepare the listing of a bucket.

        Parameters
        ----------
        bucket : str
            Bucket name (e.g. "openneuro.org", "fcp-indi")
        region : str, optional
            AWS region. Discovered automatically if not provided.
        endpoint : str, optional
            Custom endpoint (S3-compatible storage). If provided,
            `region` is ignored.
        session : aiohttp.ClientSession, optional
            Session to use. A new one is created for each listing
            otherwise.
        max_attempts : int
            Number of attempts for each listing request.
        """
        self.bucket = bucket
        self.region = region
        self.endpoint = endpoint.rstrip("/") if endpoint else None
        self.session = session
        self.max_attempts = max_attempts

    @property
    def base_url(self) -> str:
        """URL of the bucket (custom endpoint, regional or global)."""
        if self.endpoint:
            return f"{self.endpoint}/{self.bucket}"
        if self.region and self.region != "us-east-1":
            return f"https://s3.{self.region}.amazonaws.com/{self.bucket}"
        return f"https://s3.amazonaws.com/{self.bucket}"

    def url(self, key: str, version_id: str | None = None) -> str:
        """Public (path-style) URL of an object."""
        url = f"{self.base_url}/{quote(key)}"
        if version_id:
            url += f"?versionId={quote(version_id)}"
        return url

    # ------------------------------------------------------------------
    #   Listing
    # ------------------------------------------------------------------

    async def _get_page(self, session, params) -> ElementTree.Element:
        for attempt in range(self.max_attempts):
            try:
                async with session.get(self.base_url, params=params) as r:
                    if r.status in (301, 307, 400) and (
                        "x-amz-bucket-region" in r.headers
                    ):
                        region = r.headers["x-amz-bucket-region"]
                        if region != self.region and not self.endpoint:
                            lg.debug(f"s3://{self.bucket} is in {region}")
                            self.region = region
                            continue
                    if r.status in RETRY_STATUSES and (
                        attempt + 1 < self.max_attempts
                    ):
                        await asyncio.sleep(2 ** attempt)
                        continue
                    if r.status == 403:
                        raise PermissionError(
                            f"s3://{self.bucket} cannot be listed "
                            f"anonymously (403)"
                        )
                    r.raise_for_status()
                    body = await r.read()
                    return ElementTree.fromstring(body)
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
                if attempt + 1 >= self.max_attempts:
                    raise
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"Could not list s3://{self.bucket}")

    async def _iter_pages(
        self, prefix: str, delimiter: str | None
    ) -> AsyncIterator[ElementTree.Element]:
        session = self.session
        own = session is None or session.closed
        if own:
            session = new_session()
        try:
            params = {"list-type": "2", "prefix": prefix}
            if delimiter:
                params["delimiter"] = delimiter
            while True:
                page = await self._get_page(session, params)
                yield page
                token = page.findtext(f"{_NS}NextContinuationToken")
                truncated = page.findtext(f"{_NS}IsTruncated") == "true"
                if not (truncated and token):
                    break
                params["continuation-token"] = token
        finally:
            if own:
                await session.close()

    async def list(
        self, prefix: str = "", recursive: bool = True
    ) -> AsyncIterator[S3Object]:
        """
        List objects under a prefix.

        If `recursive=False`, only objects directly under `prefix`
        (interpreted as a directory) are returned.
        """
        delimiter = None if recursive else "/"
        async for page in self._iter_pages(prefix, delimiter):
            for elem in page.iter(f"{_NS}Contents"):
                yield _parse_object(elem)

    async def ls(self, prefix: str = "") -> tuple[list[S3Object], list[str]]:
        """
        List a "directory" in one pass.

        It returns the objects directly under
        `prefix` and its subdirectories (common prefixes, with a
        trailing slash).
        """
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        objects, dirs = [], []
        async for page in self._iter_pages(prefix, "/"):
            for elem in page.iter(f"{_NS}Contents"):
                objects.append(_parse_object(elem))
            for elem in page.iter(f"{_NS}CommonPrefixes"):
                dirs.append(elem.findtext(f"{_NS}Prefix"))
        return objects, dirs

    async def dirs(self, prefix: str = "") -> AsyncIterator[str]:
        """List "subdirectories" (common prefixes) under a prefix."""
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        async for page in self._iter_pages(prefix, "/"):
            for elem in page.iter(f"{_NS}CommonPrefixes"):
                yield elem.findtext(f"{_NS}Prefix")

    # ------------------------------------------------------------------
    #   Synchronous helpers
    # ------------------------------------------------------------------

    def list_all(self, prefix: str = "", recursive: bool = True) -> list:
        """List objects (synchronous version of `list`, returns a list)."""
        async def run():
            return [obj async for obj in self.list(prefix, recursive)]
        return asyncio.run(run())

    def iter_all(self, prefix: str = "") -> Iterator[S3Object]:
        """Iterate over all objects under `prefix` (synchronous)."""
        yield from self.list_all(prefix)


def _parse_object(elem: ElementTree.Element) -> S3Object:
    mtime = elem.findtext(f"{_NS}LastModified")
    if mtime:
        mtime = datetime.datetime.fromisoformat(mtime.replace("Z", "+00:00"))
    return S3Object(
        key=elem.findtext(f"{_NS}Key"),
        size=int(elem.findtext(f"{_NS}Size") or 0),
        mtime=mtime or None,
        etag=elem.findtext(f"{_NS}ETag"),
    )


class S3Prefix(Source):
    """
    Source: the objects under a prefix of a public bucket.

    They are mirrored under `destination(layout)` with their path relative to
    the prefix (or `local_path(entry)`).

    Subclasses (or the `keep`/`rename` arguments) filter and map entries:
    `keep(entry, selection, wants) -> bool` and `local_path(entry)`.
    Hidden files (`.DS_Store`...) and "folder" objects are skipped.

    Parameters
    ----------
    bucket : S3Bucket | str | callable() -> S3Bucket
        Bucket (a callable is called at each listing, so that tests can
        patch it)
    prefix : str
        Prefix (a folder: a trailing "/" is added)
    keep : callable(RemoteEntry, selection, wants) -> bool
        Filter
    rename : callable(RemoteEntry) -> str | None
        Local path, relative to `destination(layout)` (None: skip)
    """

    jobs = 4

    def __init__(self, bucket, prefix: str = "", *, keep=None, rename=None,
                 **kwargs):
        super().__init__(**kwargs)
        self._bucket = bucket
        self.prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        if keep is not None:
            self.keep = keep
        if rename is not None:
            self.local_path = rename

    @property
    def bucket(self) -> S3Bucket:
        """The bucket (a callable is called at each access)."""
        bucket = self._bucket
        if isinstance(bucket, str):
            return S3Bucket(bucket)
        if callable(bucket) and not isinstance(bucket, S3Bucket):
            return bucket()
        return bucket

    def keep(self, entry: RemoteEntry, selection, wants) -> bool:
        """Whether to fetch an entry (filter that subclasses override)."""
        return True

    def entries(self, prefix: str = "") -> list[RemoteEntry]:
        """
        List all objects under `self.prefix + prefix`.

        Parameters
        ----------
        prefix : str
            Sub-prefix.

        Returns
        -------
        list[RemoteEntry]
            Entries, with paths relative to `self.prefix`.
        """
        bucket = self.bucket
        root = self.prefix
        out = []
        try:
            objects = bucket.list_all(root + prefix)
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError,
                PermissionError) as e:
            raise SourceUnavailable(
                f"s3://{bucket.bucket}/{root + prefix}: "
                f"[{type(e).__name__}] {e}"
            ) from e
        for obj in objects:
            rel = obj.key[len(root):]
            if not rel or rel.endswith("/"):
                continue
            if any(p.startswith(".") for p in rel.split("/")):
                continue
            out.append(obj.entry(bucket.url(obj.key), rel))
        return out

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        """List the objects under the prefix, filtered by `keep`.

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
        return [e for e in self.entries() if self.keep(e, selection, wants)]
