__all__ = ["DownloadManager", "DownloadTable"]
import asyncio
import time
import warnings
from pathlib import Path
from collections import Counter
from logging import getLogger
from typing import Literal, Iterable

import aiohttp

from brainspresso.utils.tabular import LogSafeTabular, get_style
from brainspresso.utils.log import HideLoggingStream
from brainspresso.download.constants import TIMEOUT
from brainspresso.download.downloader import IfExists
from brainspresso.download.downloader import Downloader

lg = getLogger(__name__)

# Keys of "progress" statuses, which are rate-limited before display
_PROGRESS_KEYS = frozenset({"done", "done%", "dspeed", "wspeed", "tspeed"})


class DownloadTable(LogSafeTabular):

    def __init__(self, *args, **kwargs):
        rec_fields = (
            "path",
            "size",
            "done",
            "done%",
            "checksum",
            "dspeed",
            "wspeed",
            "tspeed",
            "status",
            "message",
        )
        kwargs.setdefault('style', get_style(hide_if_missing=False))
        kwargs.setdefault('columns', rec_fields),
        super().__init__(*args, **kwargs)


class DownloadManager:
    """
    A class that manages a list of downloads.

    It runs up to `jobs` downloads concurrently in a single event loop,
    sharing one HTTP session, and displays their status in a table.

    ```python
    manager = DownloadManager([
        Downloader(url1, fname1),
        Downloader(url2, fname2),
        Downloader(url3, fname3),
    ], jobs=2)
    manager.run()
    ```
    """

    # Minimum delay between two progress updates of the same file
    REFRESH_INTERVAL: float = 0.25

    def __init__(
            self,
            downloaders: Iterable[Downloader],
            ifexists: IfExists.Choice | None = None,
            on_error: Literal["yield", "raise"] = "yield",
            path: Literal["name", "full", "abs", "short"] = "name",
            jobs: int | None = None,
            tabular_jobs: int | None = None,
            jobs_per_host: int | None = None,
    ):
        """
        Parameters
        ----------
        downloaders : iterable[Downloader]
            An iterable of downloaders. It may be a (slow) generator:
            it is consumed in a background thread while downloads run.

        Other Parameters
        ----------------
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh', None}
            Behaviour if destination file already exists
        on_error : {"yield", "raise"}
            Whether to raise an error a yield a status when an exception
            is encountered in a download
        path : {"name", "full", "abs", "short"}
            Which version of the path to display
            * "name"  : file name only
            * "full"  : full path (as stored in downloader)
            * "abs"   : absolute path
            * "short" : hide common prefix
        jobs : int
            Number of concurrent downloads. Default: 4
        tabular_jobs : int
            Number of parallel printing jobs. Default: jobs
        jobs_per_host : int
            Maximum number of concurrent connections to a single host.
            Default: jobs
        """
        if isinstance(downloaders, Downloader):
            downloaders = [downloaders]
        self.downloaders = downloaders

        self.ifexists = IfExists.from_any(ifexists)
        self.on_error = on_error
        self.path = path
        self.jobs = max(1, jobs or 4)
        self.jobs_per_host = max(1, jobs_per_host or self.jobs)
        self.out = DownloadTable(max_workers=tabular_jobs or self.jobs)

    def run(self, mode: Literal["async", "threaded"] = "async"):
        """Run all downloads"""
        if mode != "async":
            warnings.warn(
                "DownloadManager.run: the 'threaded' mode has been removed. "
                "Downloads now run concurrently in a single event loop.",
                DeprecationWarning, stacklevel=2,
            )
        return asyncio.run(self.run_async())

    def _iter_jobs(self):
        """Yield (display path, downloader) pairs"""
        if self.path[0] == 's':
            # Shorten path, but we need to access all downloaders which
            # might be slow is the input is a looooong generator
            self.downloaders = list(self.downloaders)
            paths = self.shortpath([dl.dst for dl in self.downloaders])
            yield from zip(map(str, paths), self.downloaders)
        else:
            for downloader in self.downloaders:
                yield str(self.repath(downloader.dst)), downloader

    async def run_async(self):
        guard = {'yield': _Guard, 'raise': lambda x: x}[self.on_error]
        queue: asyncio.Queue = asyncio.Queue(maxsize=4 * self.jobs)
        last_emit: dict[str, float] = {}
        pending: dict[str, dict] = {}

        def emit(path, status):
            # Progress statuses are rate-limited: rendering the table
            # at every chunk would slow down the downloads.
            now = time.monotonic()
            if _PROGRESS_KEYS.issuperset(status):
                if now - last_emit.get(path, 0) < self.REFRESH_INTERVAL:
                    pending[path] = status
                    return
            else:
                status = {**pending.pop(path, {}), **status}
            pending.pop(path, None)
            last_emit[path] = now
            self.out({"path": path, **status})

        async def producer():
            # The iterable may perform blocking network calls (listing),
            # so it is consumed in a thread.
            iterator = self._iter_jobs()
            sentinel = object()
            try:
                while True:
                    item = await asyncio.to_thread(next, iterator, sentinel)
                    if item is sentinel:
                        break
                    await queue.put(item)
            finally:
                for _ in range(self.jobs):
                    await queue.put(None)

        async def worker(session):
            while (item := await queue.get()) is not None:
                path, downloader = item
                if not (
                    isinstance(downloader.session, aiohttp.ClientSession)
                    and not downloader.session.closed
                ):
                    downloader.session = session
                try:
                    async for status in guard(downloader):
                        emit(path, status)
                finally:
                    if path in pending:
                        self.out({"path": path, **pending.pop(path)})
                    last_emit.pop(path, None)

        connector = aiohttp.TCPConnector(
            limit=max(2 * self.jobs, 10),
            limit_per_host=self.jobs_per_host,
        )
        async with aiohttp.ClientSession(
            connector=connector, timeout=TIMEOUT
        ) as session:
            with self.out, IfExists(self.ifexists), HideLoggingStream():
                tasks = [asyncio.create_task(producer())]
                tasks += [
                    asyncio.create_task(worker(session))
                    for _ in range(self.jobs)
                ]
                try:
                    done, _ = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_EXCEPTION
                    )
                    for task in done:
                        if task.exception() is not None:
                            raise task.exception()
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

    def shortpath(self, paths):
        if len(paths) == 1:
            # fallback to mode "name"
            return [path.name for path in paths]
        common = self.commonprefix(*paths)
        if common is None:
            # fallback to mode "full"
            return paths
        return [path.relative_to(Path(common)) for path in paths]

    def repath(self, path):
        mode = self.path[0].lower()
        if mode == "a":  # abs
            return path.absolute()
        if mode == "n":  # name
            return path.name
        if mode == "f":  # full
            return path
        assert False

    def commonprefix(self, *paths):
        """Common prefix of given paths"""
        # https://gist.github.com/chrono-meter/7e47528a3f902c9ade7e0cc442394d08
        counter = Counter()

        for path in paths:
            counter.update([path])
            counter.update(path.parents)

        try:
            return sorted(
                (x for x, count in counter.items() if count >= len(paths)),
                key=lambda x: len(str(x))
            )[-1]
        except LookupError:
            return None


class _Guard:
    # Convert exception to status

    def __init__(self, downloader):
        self.downloader = downloader

    def __getattr__(self, name):
        return getattr(self.downloader, name)

    def __aiter__(self):
        return self.iter()

    async def iter(self):
        try:
            async for status in self.downloader:
                yield status
        except Exception as exc:
            lg.error(
                f"Caught while downloading {self.downloader.dst!s}: "
                f"[{str(exc.__class__.__name__)}] {exc}"
            )
            yield {
                "status": "error",
                "message": f"[{str(exc.__class__.__name__)}] {exc}",
            }
