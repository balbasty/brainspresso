from pathlib import Path
from typing import Iterable
from humanize import naturalsize

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.utils.ui import human2bytes
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.sources.base import DownloadOptions, LocalSource, StaticURLs
from brainspresso.datasets.ADNI.I.command import adni1


def sources(urls=()) -> dict:
    """
    Sources of ADNI-1: the data are under a data use agreement. They are
    either downloaded from URLs that the user got from the LONI portal
    (`urls`), or put in `sourcedata/` by the user (`local`).
    """
    return {
        "urls": StaticURLs({"raw": list(urls)}, name="urls"),
        "local": LocalSource(provides={"raw", "meta"}),
    }


@adni1.command(name="harvest")
def download(
    path: str | None = None,
    *,
    urls: Iterable[str],
    if_exists: IfExists.Choice = "skip",
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    layout: LayoutChoice | None = None,
    log: str | None = None,
):
    """
    Download source data for the ADNI-1 dataset.

    Parameters
    ----------
    path
        Path to root of all datasets. A `ADNI-1` folder will be created.
    urls
        URL(s) to download. Can be a path to a file.
    if_exists
        Behaviour if a file already exists
    packet
        Packet size to download, in bytes
    jobs
        Number of parallel downloaders
    layout
        Layout of the dataset (where source data does not move, but
        later commands reuse it): "study" (raw dataset in
        `rawbids/`) or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log
        Path to log file

    """
    setup_filelog(log)
    path = Path(get_tree_path(path))
    dataset = resolve_layout(path / 'ADNI-1', layout)
    record_run(dataset, "harvest", {"dataset": "ADNI-1"})

    # load all URLs
    tmp, urls = [urls] if isinstance(urls, str) else urls, []
    for url in tmp:
        if Path(url).exists():
            with Path(url).open('rt') as f:
                for line in f.readlines():
                    line = line.strip()
                    if line:
                        urls.append(line)
        else:
            urls.append(url)

    sources(urls)["urls"].harvest(
        dataset, options=DownloadOptions(ifexists=if_exists, jobs=jobs,
                                         chunk_size=human2bytes(packet)),
    )
