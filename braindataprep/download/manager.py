__all__ = ["DownloadManager", "DownloadTable"]
import asyncio
from pathlib import Path
from collections import Counter
from logging import getLogger
from typing import Literal, Iterable
from queue import Queue, Empty
from multiprocessing import Manager, Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor

import aiohttp

from braindataprep.utils.tabular import LogSafeTabular, get_style
from braindataprep.utils.log import HideLoggingStream
from braindataprep.download.downloader import IfExists
from braindataprep.download.downloader import Downloader

lg = getLogger(__name__)


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
    A class that manages is list of downloads.

    It runs them one at a time and display their status in a table.

    ```python
    manager = DownloadManager(
        Downloader(url1, fname1),
        Downloader(url2, fname2),
        Downloader(url3, fname3),
    )
    manager.run()
    ```
    """

    def __init__(
            self,
            downloaders: Iterable[Downloader],
            ifexists: IfExists.Choice | None = None,
            on_error: Literal["yield", "raise"] = "yield",
            path: Literal["name", "full", "abs", "short"] = "name",
            jobs: int | None = 1,
            tabular_jobs: int | None = None,
    ):
        """
        Parameters
        ----------
        *downloaders : Downloader
            A list of downloaders

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
            Number of parallel downloaders. Default: cpu_count/2
        tabular_jobs : int
            Number of parallel printing jobs. Default: jobs
        """
        self.downloaders = downloaders

        self.ifexists = IfExists.from_any(ifexists)
        self.on_error = on_error
        self.path = path
        self.jobs = max(1, jobs or (cpu_count()//2))
        self.out = DownloadTable(max_workers=tabular_jobs or self.jobs)

    def run(self, mode="async"):
        if mode == "async":
            return asyncio.run(self.run_async())
        else:
            return self.run_threaded()

    def run_threaded(self):
        """Run all downloads"""
        guard = {'yield': _Guard, 'raise': lambda x: x}[self.on_error]
        jobs = Queue()
        manager = Manager()
        statuses = manager.Queue()

        def unpack_jobs():
            while True:
                try:
                    job = jobs.get_nowait()
                    if not job.ready():
                        jobs.put_nowait(job)
                    unpack_statuses(forever=True)
                except Empty:
                    break

        def unpack_statuses(forever: bool = False):
            while forever or (jobs.qsize() > self.jobs):
                # unpack top of the queue
                try:
                    status = statuses.get_nowait()
                    self.out(status)
                except Exception:
                    break
                # check if a job has finished
                if not forever:
                    try:
                        job = jobs.get_nowait()
                        if not job.ready():
                            jobs.put_nowait(job)
                    except Empty:
                        break

        with (
            self.out,
            IfExists(self.ifexists),
            Pool(self.jobs) as pool
        ):

            if self.path[0] == 's':
                # Shorten path, but we need to access all downloaders which
                # might be slow is the input is a looooong generator
                self.downloaders = list(self.downloaders)
                paths = self.shortpath([dl.dst for dl in self.downloaders])

                for path, downloader in zip(paths, self.downloaders):
                    jobs.put_nowait(pool.apply_async(
                        _run, (guard(downloader), path, statuses)
                    ))
                    unpack_statuses()
                unpack_jobs()

            else:
                # Just yield from the generator
                for downloader in self.downloaders:
                    path = str(self.repath(downloader.dst))
                    jobs.put_nowait(pool.apply_async(
                        _run, (guard(downloader), path, statuses)
                    ))
                    unpack_statuses()
                unpack_jobs()

    async def run_async(self):
        guard = {'yield': _Guard, 'raise': lambda x: x}[self.on_error]
        loop = asyncio.get_running_loop()

        async with aiohttp.ClientSession() as session:

            with (
                self.out,
                IfExists(self.ifexists),
                ThreadPoolExecutor() as pool
            ):
                if self.path[0] == 's':
                    # Shorten path, but we need to access all downloaders which
                    # might be slow is the input is a looooong generator
                    self.downloaders = list(self.downloaders)
                    paths = self.shortpath([dl.dst for dl in self.downloaders])

                    for path, downloader in zip(paths, self.downloaders):
                        downloader.session = session
                        downloader = guard(downloader)
                        async for status in _run_async(downloader, path):
                            await loop.run_in_executor(pool, self.out, status)

                else:
                    # Just yield from the generator
                    for downloader in self.downloaders:
                        downloader.session = session
                        downloader = guard(downloader)
                        path = str(self.repath(downloader.dst))
                        async for status in _run_async(downloader, path):
                            await loop.run_in_executor(pool, self.out, status)

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


reusable_session = None


async def _make_session():
    return aiohttp.ClientSession()


async def _make_reusable_session():
    global reusable_session
    reusable_session = await _make_session()
    yield reusable_session
    reusable_session.close()


async def _get_reusable_session():
    global reusable_session
    if reusable_session is None:
        _make_reusable_session()
    return reusable_session


async def _run_async(downloader, path):
    if not isinstance(downloader.session, aiohttp.ClientSession):
        async with aiohttp.ClientSession() as session:
            downloader = session
            await _run_async(downloader, path)
        return
    with HideLoggingStream():
        async for status in downloader:
            status = {"path": path, **status}
            yield status


async def _run_async_queue(downloader, path, statuses):
    # if downloader.session is None:
    #     downloader.session = await _get_reusable_session()
    downloader.session = await _get_reusable_session()
    with HideLoggingStream():
        async for status in downloader:
            status = {"path": path, **status}
            statuses.put_nowait(status)


def _run(downloader, path, statuses):
    asyncio.run(_run_async_queue(downloader, path, statuses))


class _Guard:
    # Convert exception to status
    # Must be pickable since it is passed to multiprocessing

    def __init__(self, downloader):
        self.downloader = downloader

    def __getattr__(self, name):
        if name in ("session", "dst"):
            return getattr(self.downloader, name)
        return super().__getattr__(name)
        # if name == "downloader":
        #     return super().__getattr__(name)
        # return getattr(self.downloader, name)

    def __setattr__(self, name, value):
        if name in ("session", "dst"):
            return setattr(self.downloader, name, value)
        return super().__setattr__(name, value)
        # if name == "downloader":
        #     return super().__setattr__(name, value)
        # return setattr(self.downloader, name, value)

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
