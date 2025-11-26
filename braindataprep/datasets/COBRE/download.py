from pathlib import Path
from urllib.parse import urlparse
from humanize import naturalsize
from typing import Literal, Iterable
from warnings import filterwarnings
from urllib3.exceptions import InsecureRequestWarning

from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.utils.ui import human2bytes
from braindataprep.download import Downloader
from braindataprep.download import DownloadManager
from braindataprep.download import CHUNK_SIZE
from braindataprep.download import IfExists
from braindataprep.sources.nitrc import nitrc_authentifier
from braindataprep.datasets.COBRE.command import cobre


URLBASE = 'https://fcon_1000.projects.nitrc.org/indi/retro/COBRE'
URLS = {
    'meta': [
        f'{URLBASE}/COBRE_parameters_rest.csv',
        f'{URLBASE}/COBRE_parameters_mprage.csv',
        f'{URLBASE}/COBRE_phenotypic_data.csv',
    ],
    'raw': [
        f'{URLBASE}/COBRE_scan_data.tgz'
    ],
}

KeyChoice = Literal["raw", "meta"]


@cobre.command
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = tuple(URLS.keys()),
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    log: str | None = None,
) -> None:
    """
    Download source data for the IXI dataset.

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `IXI` folder will be created.
    keys : [list of] {"raw", "meta"}
        Modalities to download
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    user : str
        NITRC username
    password : str
        NITRC password
    packet : int
        Packet size to download, in bytes
    log : str
        Path to log file
    """
    setup_filelog(log)
    auth = nitrc_authentifier(user, password)
    keys = keys or URLS.keys()
    keys = list({key: None for key in keys}.keys())  # remove duplicates
    path: Path = Path(get_tree_path(path))
    src = path / 'COBRE' / 'sourcedata'
    src.mkdir(parents=True, exist_ok=True)

    filterwarnings('ignore', category=InsecureRequestWarning)
    DownloadManager((
        Downloader(
            url,  src / Path(urlparse(url).path).name,
            ifexists=if_exists,
            chunk_size=human2bytes(packet),
            auth=auth,
            get_opt=dict(verify=False),
        )
        for key in keys
        for url in URLS[key]),
        on_error="raise"
    ).run()
