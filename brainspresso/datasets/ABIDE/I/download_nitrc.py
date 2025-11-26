from enum import Enum
from humanize import naturalsize
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from urllib3.exceptions import InsecureRequestWarning
from warnings import filterwarnings

from brainspresso.utils.ui import human2bytes
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.sources.nitrc import nitrc_authentifier_async
from brainspresso.download import DownloadManager
from brainspresso.download import Downloader
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.datasets.ABIDE.I.command import abide1

SITES = (
    'Caltech',
    'CMU',
    'KKI',
    'Leuven',
    'MaxMun',
    'NYU',
    'OHSU',
    'Olin',
    'Pitt',
    'SBL',
    'SDSU',
    'Stanford',
    'Trinity',
    'UCLA',
    'UM',
    'USM',
    'Yale',
)
KEYS = ("raw", "meta")

SiteChoice = Enum("SiteChoice", [(site, site) for site in SITES], type=str)
KeyChoice = Enum("KeyChoice", [("raw", "raw"), ("meta", "meta")], type=str)

URLBASE = 'https://fcp_private.projects.nitrc.org/downloads'
URLBASE += '/abide_mrdata_r01_release'
IMGBASE = f'{URLBASE}/ImagingData'
PHNBASE = f'{URLBASE}/PhenotypicData'
SAMPS = {'Leuven': ['_1', '_2'], 'UCLA': ['_1', '_2'], 'UM': ['_1', '_2']}
PARTS = {
    'CMU': ['_a', '_b'],
    'MaxMun': ['_a', '_b', '_c', '_d'],
    'NYU': ['_a', '_b', '_c', '_d', '_e'],
}
URLS = {
    SITE: {
        'raw': [
            f'{IMGBASE}/{SITE}{SAMP}{SUFFIX}.tgz'
            for SUFFIX in PARTS.get(SITE, [''])
            for SAMP in SAMPS.get(SITE, [''])
        ],
        'meta': [
            f'{PHNBASE}/phenotypic_{FIXED_SITE}{SAMP}.csv'
            for FIXED_SITE in [{'MaxMun': 'Max_Mun'}.get(SITE, SITE).upper()]
            for SAMP in SAMPS.get(SITE, [''])
        ],
    }
    for SITE in SITES
}


@abide1.command
def download_nitrc(
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
    level: str = "info",
):
    """
    Download source data for the ABIDE-I dataset.

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
    level
        Loggin level
    """
    setup_filelog(log, level=level)
    path = Path(get_tree_path(path))
    keys = set(keys or KEYS)
    sites = set(sites or SITES)
    src = path / 'ABIDE-1' / 'sourcedata'
    auth = nitrc_authentifier_async(user, password)

    def downloaders():
        for site in sites:
            for key in keys:
                for url in URLS[site][key]:
                    yield Downloader(
                        url, src / Path(urlparse(url).path).name,
                        chunk_size=human2bytes(packet),
                        auth=auth,
                        get_opt=dict(verify_ssl=False),
                        ifnodigest="continue",
                    )

    filterwarnings('ignore', category=InsecureRequestWarning)
    DownloadManager(
        downloaders(),
        ifexists=if_exists,
        jobs=jobs,
    ).run("async")
