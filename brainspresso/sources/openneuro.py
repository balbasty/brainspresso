"""
OpenNeuro listing.

Two routes are supported:

* **GraphQL** (`https://openneuro.org/crn/graphql`): lists the files of a
  given snapshot (tag), with sizes and download URLs that point to the
  exact S3 object version of that snapshot. For annexed files, the file
  `id` is the git-annex key (`SHA256E-s<size>--<sha256>.ext` or `MD5E-...`),
  from which the checksum is recovered.
* **S3** (`s3://openneuro.org/<dsid>/`): anonymous listing of the bucket,
  which mirrors the **latest** snapshot. Checksums come from single-part
  ETags (MD5). The snapshot tag is read from the first line of `CHANGES`
  (best effort).

`list_snapshot(..., source="auto")` tries GraphQL first and falls back to
S3 when the API cannot be reached and no specific tag was requested.

`OpenNeuro` is the source of a dataset (see `sources.base`): it lists a
snapshot and places the selected files in the BIDS dataset
(`bids.harvest.harvest`: selection with inheritance, `--where`, trimmed
tables, manifest).
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from logging import getLogger
from urllib.parse import quote

import aiohttp

from brainspresso.download.session import new_session
from brainspresso.sources.base import (
    DownloadOptions, RemoteEntry, Source, SourceUnavailable,
)
from brainspresso.sources.s3 import S3Bucket

lg = getLogger(__name__)

GRAPHQL_URL = "https://openneuro.org/crn/graphql"
BUCKET = "openneuro.org"
DATASET_ID = re.compile(r"^ds\d{6}$")
ANNEX_KEY = re.compile(
    r"^(?P<backend>SHA256E?|SHA512E?|SHA1E?|MD5E?)"
    r"-s(?P<size>\d+)(?:-[^-]+)*--(?P<hash>[0-9a-fA-F]+)"
)
_ANNEX_ALGO = {"SHA256": "sha256", "SHA512": "sha512",
               "SHA1": "sha1", "MD5": "md5"}
_VERSION = re.compile(r"^\s*(\d+\.\d+\.\d+)\b")


class OpenNeuroError(RuntimeError):
    """An error of the OpenNeuro API."""


class APIUnavailable(OpenNeuroError, SourceUnavailable):
    """The OpenNeuro API cannot be reached."""


@dataclass
class Snapshot:
    """A listed snapshot: dataset, tag, listing route and files."""

    dataset: str
    tag: str | None
    source: str                             # "graphql" | "s3"
    files: list[RemoteEntry]

    @property
    def size(self) -> int:
        """Total size of the files, in bytes."""
        return sum(f.size or 0 for f in self.files)


def check_dataset_id(dataset: str) -> str:
    """Return `dataset` if it is an accession number (else `ValueError`)."""
    if not DATASET_ID.match(dataset):
        raise ValueError(
            f"Invalid OpenNeuro accession {dataset!r} (expected dsXXXXXX)"
        )
    return dataset


def digests_from_annex_key(key: str | None) -> dict[str, str]:
    """`SHA256E-s123--abcd.nii.gz` -> {"sha256": "abcd"}."""
    match = ANNEX_KEY.match(key or "")
    if not match:
        return {}
    algo = _ANNEX_ALGO[match["backend"].rstrip("E")]
    return {algo: match["hash"].lower()}


# ----------------------------------------------------------------------
#   GraphQL
# ----------------------------------------------------------------------

class OpenNeuroAPI:
    """Minimal async client for the OpenNeuro GraphQL API."""

    def __init__(
        self,
        url: str = GRAPHQL_URL,
        token: str | None = None,
        session: aiohttp.ClientSession | None = None,
        max_attempts: int = 4,
    ):
        self.url = url
        self.token = token
        self.session = session
        self.max_attempts = max_attempts

    async def query(self, query: str) -> dict:
        """Run a GraphQL query.

        Parameters
        ----------
        query : str
            GraphQL query.

        Returns
        -------
        dict
            The `data` field of the answer.
        """
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        payload = json.dumps({"query": query})
        session = self.session
        own = session is None or session.closed
        if own:
            session = new_session(timeout=aiohttp.ClientTimeout(
                total=None, sock_connect=20, sock_read=180,
            ))
        try:
            for attempt in range(self.max_attempts):
                try:
                    async with session.post(
                        self.url, data=payload, headers=headers
                    ) as r:
                        if r.status >= 500 and (
                            attempt + 1 < self.max_attempts
                        ):
                            await asyncio.sleep(2 ** attempt)
                            continue
                        r.raise_for_status()
                        body = await r.json(content_type=None)
                    break
                except (aiohttp.ClientConnectionError,
                        asyncio.TimeoutError) as e:
                    if attempt + 1 >= self.max_attempts:
                        raise APIUnavailable(
                            f"Cannot reach {self.url}: "
                            f"[{type(e).__name__}] {e}"
                        ) from e
                    await asyncio.sleep(2 ** attempt)
        except aiohttp.ClientResponseError as e:
            raise APIUnavailable(f"{self.url}: {e.status} {e.message}")
        finally:
            if own:
                await session.close()
        if body.get("errors"):
            messages = "; ".join(
                str(e.get("message", e)) for e in body["errors"]
            )
            raise OpenNeuroError(messages)
        return body.get("data") or {}

    async def snapshots(self, dataset: str) -> list[dict]:
        """List snapshots of a dataset: [{tag, created}, ...]."""
        check_dataset_id(dataset)
        data = await self.query(
            f'query {{ dataset(id: "{dataset}") {{ '
            f'latestSnapshot {{ tag }} snapshots {{ tag created }} }} }}'
        )
        if not data.get("dataset"):
            raise OpenNeuroError(f"Dataset {dataset} not found")
        return data["dataset"]["snapshots"] or []

    async def latest_tag(self, dataset: str) -> str:
        """Return the tag of the latest snapshot of a dataset."""
        check_dataset_id(dataset)
        data = await self.query(
            f'query {{ dataset(id: "{dataset}") '
            f'{{ latestSnapshot {{ tag }} }} }}'
        )
        if not data.get("dataset"):
            raise OpenNeuroError(f"Dataset {dataset} not found")
        return data["dataset"]["latestSnapshot"]["tag"]

    async def files(self, dataset: str, tag: str) -> list[RemoteEntry]:
        """List all files of a snapshot."""
        check_dataset_id(dataset)
        tag = json.dumps(str(tag))      # quoted and escaped
        data = await self.query(
            f'query {{ snapshot(datasetId: "{dataset}", tag: {tag}) {{ '
            f'id tag files(recursive: true) {{ '
            f'id filename size directory annexed urls }} }} }}'
        )
        snapshot = data.get("snapshot")
        if not snapshot:
            raise OpenNeuroError(f"Snapshot {dataset}:{tag} not found")
        out = []
        for f in snapshot["files"] or []:
            if f.get("directory"):
                continue
            path = f["filename"]
            urls = [u for u in (f.get("urls") or []) if u]
            if urls:
                url = urls[0]
            else:
                url = (
                    f"https://openneuro.org/crn/datasets/{dataset}/"
                    f"snapshots/{snapshot['tag']}/files/"
                    f"{quote(path.replace('/', ':'))}"
                )
            digests = (
                digests_from_annex_key(f.get("id"))
                if f.get("annexed") else {}
            )
            size = f.get("size")
            out.append(RemoteEntry(
                path=path,
                url=url,
                size=int(size) if size is not None else None,
                digests=digests,
            ))
        return out


# ----------------------------------------------------------------------
#   S3
# ----------------------------------------------------------------------

async def list_s3(
    dataset: str,
    bucket: S3Bucket | None = None,
) -> Snapshot:
    """List the latest snapshot of a dataset from the public bucket."""
    check_dataset_id(dataset)
    bucket = bucket or S3Bucket(BUCKET)
    prefix = f"{dataset}/"
    files = []
    async for obj in bucket.list(prefix):
        if obj.key.endswith("/"):
            continue
        files.append(obj.entry(bucket.url(obj.key), obj.key[len(prefix):]))
    if not files:
        raise OpenNeuroError(
            f"{dataset} not found in s3://{bucket.bucket} "
            f"(private, deleted, or not yet published?)"
        )
    tag = None
    changes = next((f for f in files if f.path == "CHANGES"), None)
    if changes is not None:
        tag = await _read_tag(changes.url)
    return Snapshot(dataset, tag, "s3", files)


async def _read_tag(url: str) -> str | None:
    try:
        async with new_session() as session:
            headers = {"Range": "bytes=0-1023"}
            async with session.get(url, headers=headers) as r:
                r.raise_for_status()
                head = (await r.read()).decode("utf-8", "replace")
    except Exception as e:      # best effort
        lg.debug(f"Could not read snapshot tag from CHANGES: {e}")
        return None
    for line in head.splitlines():
        match = _VERSION.match(line)
        if match:
            return match[1]
    return None


# ----------------------------------------------------------------------
#   Entry point
# ----------------------------------------------------------------------

async def list_snapshot(
    dataset: str,
    tag: str | None = None,
    source: str = "auto",
    api: OpenNeuroAPI | None = None,
    bucket: S3Bucket | None = None,
) -> Snapshot:
    """
    List the files of an OpenNeuro snapshot.

    Parameters
    ----------
    dataset : str
        Accession number (e.g. "ds000224")
    tag : str, optional
        Snapshot tag (e.g. "1.0.4"). Default: latest.
    source : {"auto", "graphql", "s3"}
        Listing route. "auto" tries GraphQL, then falls back to S3 if the
        API is unreachable (only when `tag` is not specified, since the
        bucket only mirrors the latest snapshot).
    """
    check_dataset_id(dataset)
    if source not in ("auto", "graphql", "s3"):
        raise ValueError(f"Unknown source {source!r}")

    if source in ("auto", "graphql"):
        api = api or OpenNeuroAPI()
        try:
            resolved = tag or await api.latest_tag(dataset)
            files = await api.files(dataset, resolved)
            return Snapshot(dataset, resolved, "graphql", files)
        except APIUnavailable as e:
            if source == "graphql":
                raise
            if tag:
                raise OpenNeuroError(
                    f"{e}. A specific snapshot ({tag}) can only be listed "
                    f"through the OpenNeuro API. Retry later, or drop "
                    f"--tag (with --source s3) to get the latest snapshot "
                    f"from S3."
                ) from e
            lg.warning(
                f"{e}. Falling back to the S3 bucket (latest snapshot)."
            )

    snapshot = await list_s3(dataset, bucket)
    if tag and snapshot.tag and snapshot.tag != tag:
        raise OpenNeuroError(
            f"S3 holds snapshot {snapshot.tag}, not {tag}. Use the API "
            f"(--source graphql) for older snapshots."
        )
    return snapshot


# ----------------------------------------------------------------------
#   Source
# ----------------------------------------------------------------------

class OpenNeuro(Source):
    """
    Source: an OpenNeuro dataset, hosted as BIDS.

    `list()` returns the whole snapshot (selection needs the full
    listing, for inheritance); `fetch(entries, layout, options,
    selection=, where=, trim=, listing=, provenance=)` selects and
    places files with `bids.harvest.harvest`.

    Parameters
    ----------
    dataset : str
        Accession number (e.g. "ds000224")
    tag : str, optional
        Snapshot tag (default: latest)
    route : {"auto", "graphql", "s3"}
        Listing route (see `list_snapshot`)
    token : str, optional
        API key (private datasets)
    """

    name = "openneuro"
    provides = frozenset({"bids"})
    jobs = 8

    def __init__(self, dataset: str, tag: str | None = None,
                 route: str = "auto", token: str | None = None, *,
                 api: OpenNeuroAPI | None = None,
                 bucket: S3Bucket | None = None, **kwargs):
        super().__init__(**kwargs)
        self.dataset = check_dataset_id(dataset)
        self.tag = tag
        self.route = route
        self.token = token
        self.api = api
        self.bucket = bucket
        self._snapshot = None

    def snapshot(self) -> Snapshot:
        """Return the listed snapshot (listed once)."""
        if self._snapshot is None:
            self._snapshot = asyncio.run(list_snapshot(
                self.dataset, self.tag, self.route,
                api=self.api or OpenNeuroAPI(token=self.token),
                bucket=self.bucket,
            ))
        return self._snapshot

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        """List the whole snapshot (see class docstring).

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
        return self.snapshot().files

    def provenance(self) -> dict:
        """Provenance of the harvest (dataset, tag, listing route)."""
        snapshot = self.snapshot()
        return {"source": "openneuro", "dataset": self.dataset,
                "tag": snapshot.tag, "listing": snapshot.source}

    def fetch(self, entries, layout, options=None, *, selection=None,
              where=(), trim: bool = True, listing: bool = False,
              repair: bool = False, provenance: dict | None = None):
        """Select files and place them with `bids.harvest.harvest`.

        Parameters
        ----------
        entries : iterable[RemoteEntry]
            Files of the snapshot.
        layout : Layout
            Layout of the dataset.
        options : DownloadOptions | Mapping | None
            Options of the harvest.
        selection : Selection, optional
            Subject selection.
        where : list
            Other filters of the selection.
        trim : bool
            Trim the generated files to the selection.
        listing : bool
            Only list the selected files.
        repair : bool
            Repair the existing files.
        provenance : dict, optional
            Provenance to record (default: `provenance()`).
        """
        from brainspresso.bids.harvest import harvest
        from brainspresso.bids.select import Selection
        options = DownloadOptions.from_any(options)
        return harvest(
            list(entries), layout, selection or Selection(),
            where=where, trim=trim, if_exists=options.ifexists,
            jobs=options.jobs or self.jobs, dry_run=options.dry_run,
            listing=listing, repair=repair, insecure=bool(options.insecure),
            provenance=provenance or self.provenance(),
        )

    def harvest(self, layout, selection=None, keys=None, options=None,
                **kwargs):
        """List and fetch (keyword arguments are passed to `fetch`)."""
        return self.fetch(self.list(selection, keys), layout, options,
                          selection=selection, **kwargs)
