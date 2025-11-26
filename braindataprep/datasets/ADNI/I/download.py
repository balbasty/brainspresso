from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from humanize import naturalsize

from braindataprep.utils.ui import human2bytes
from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.download import DownloadManager
from braindataprep.download import Downloader
from braindataprep.download import IfExists
from braindataprep.download import CHUNK_SIZE
from braindataprep.datasets.ADNI.I.command import adni1


@adni1.command
def download(
    path: str | None = None,
    *,
    urls: Iterable[str],
    if_exists: IfExists.Choice = "skip",
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
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
    log
        Path to log file

    """
    setup_filelog(log)
    path = Path(get_tree_path(path))
    src = path / 'ADNI-1' / 'sourcedata'

    # load all URLs
    tmp, urls = urls, []
    for url in tmp:
        if Path(url).exists():
            with Path(url).open('rt') as f:
                for line in f.readlines():
                    line = line.strip()
                    if line:
                        urls.append(line)
        else:
            urls.append(url)

    def downloaders():
        for url in urls:
            yield Downloader(
                url, src / Path(urlparse(url).path).name,
                chunk_size=human2bytes(packet),
            )

    DownloadManager(
        downloaders(),
        ifexists=if_exists,
        jobs=jobs,
    ).run()
