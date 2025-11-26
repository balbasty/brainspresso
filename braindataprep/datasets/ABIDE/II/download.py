from enum import Enum
from humanize import naturalsize
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from urllib3.exceptions import InsecureRequestWarning
from warnings import filterwarnings

from braindataprep.utils.ui import human2bytes
from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.sources.nitrc import nitrc_authentifier
from braindataprep.download import DownloadManager
from braindataprep.download import Downloader
from braindataprep.download import IfExists
from braindataprep.download import CHUNK_SIZE
from braindataprep.datasets.ABIDE.II.command import abide2

SITES = (
    'BNI', 'EMC', 'ETH', 'GU', 'IU', 'IP', 'KUL', 'KKI', 'NYU', 'ONRC',
    'OHSU', 'TCD', 'SDSU', 'SU', 'UCD', 'UCLA', 'U_MIA', 'USM', 'UPSM'
)
KEYS = ("raw", "meta")

SiteChoice = Enum("SiteChoice", [(site, site) for site in SITES], type=str)
KeyChoice = Enum("KeyChoice", [("raw", "raw"), ("meta", "meta")], type=str)

URLBASE = 'https://fcon_1000.projects.nitrc.org/indi/abide2/release'
IMGBASE = f'{URLBASE}/imaging_data'
PHNBASE = f'{URLBASE}/phenotypic_data'
SAMPS = {'KUL': [3], 'NYU': [1, 2], 'UCLA': [1, 'Long'], 'UPSM': ['Long'],
         'ONRC': [2], 'SU': [2]}
PARTS = {
    'KKI': ['_29273_29322', '_29323_29372', '_29373_29423', '_29424_29485'],
    'ONRC': ['_part1', '_part2', '_part3', '_part4'],
}
URLS = {
    SITE: {
        'raw': [
            f'{IMGBASE}/ABIDEII-{SITE}_{SAMP}{SUFFIX}.tar.gz'
            for SUFFIX in PARTS.get(SITE, [''])
            for SAMP in SAMPS.get(SITE, [1])
        ],
        'meta': [
            f'{PHNBASE}/ABIDEII-{SITE}_{SAMP}.csv'
            for SAMP in SAMPS.get(SITE, [1])
        ],
    }
    for SITE in SITES
}


@abide2.command
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KEYS,
    sites: Iterable[SiteChoice] = SITES,
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    log: str | None = None,
):
    """
    Download source data for the ABIDE-II dataset.

    **Possible keys:**
    * **raw**          All the raw imaging data
    * **meta**         Metadata

    Parameters
    ----------
    path
        Path to root of all datasets. An `ABIDE-2` folder will be created.
    keys
        Data categories to download
    sites
        Sites to download
    parts
        Parts to download
    if_exists
        Behaviour if a file already exists
    user
        NITRC username
    password
        NITRC password
    packet
        Packet size to download, in bytes
    jobs
        Number of parallel downloaders
    log
        Path to log file

    """
    setup_filelog(log)
    path = Path(get_tree_path(path))
    keys = set(keys or KEYS)
    sites = set(sites or SITES)
    src = path / 'ABIDE-2' / 'sourcedata'
    auth = nitrc_authentifier(user, password)

    def downloaders():
        for site in sites:
            for key in keys:
                for url in URLS[site][key]:
                    yield Downloader(
                        url, src / Path(urlparse(url).path).name,
                        chunk_size=human2bytes(packet),
                        auth=auth,
                        get_opt=dict(verify=False),
                    )

    filterwarnings('ignore', category=InsecureRequestWarning)
    DownloadManager(
        downloaders(),
        ifexists=if_exists,
        jobs=jobs,
    ).run()
