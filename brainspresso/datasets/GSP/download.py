from pathlib import Path
from enum import Enum
from typing import Iterable
from humanize import naturalsize
from warnings import filterwarnings
from urllib3.exceptions import InsecureRequestWarning

from brainspresso.utils.ui import human2bytes
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.sources.dataverse import Dataverse, get_credentials
from brainspresso.download import DownloadManager
from brainspresso.download import Downloader
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.datasets.GSP.command import gsp


KEYS = ("raw", "meta")
KeyChoice = Enum("KeyChoice", [("raw", "raw"), ("meta", "meta")], type=str)

URLBASE = 'https://dataverse.harvard.edu/api/access/datafile/:persistentId/'
URLBASE += "?persistentId="
FILE_IDS = {
    "meta": {
        "GSP_list_140630.csv": "doi:10.7910/DVN/25833/1RIO8A",
        "GSP_retest_140630.csv": "doi:10.7910/DVN/25833/J3MVZX",
    },
    "raw": {
        "GSP_part1_140630.tar": "doi:10.7910/DVN/25833/D9DWAI",
        "GSP_part2_140630.tar": "doi:10.7910/DVN/25833/FLDSGW",
        "GSP_part3_140630.tar": "doi:10.7910/DVN/25833/MZ4PKQ",
        "GSP_part4_140630.tar": "doi:10.7910/DVN/25833/WYMY3W",
        "GSP_part5_140630.tar": "doi:10.7910/DVN/25833/QT8HGQ",
        "GSP_part6_140630.tar": "doi:10.7910/DVN/25833/HMLVBE",
        "GSP_part7_140630.tar": "doi:10.7910/DVN/25833/LV9K01",
        "GSP_part8_140630.tar": "doi:10.7910/DVN/25833/5RBER6",
        "GSP_part9_140630.tar": "doi:10.7910/DVN/25833/7AIUVE",
        "GSP_part10_140630.tar": "doi:10.7910/DVN/25833/RV7AJA",
        "GSP_retest_140630.tar": "doi:10.7910/DVN/25833/ZBV7LM",
    }
}


@gsp.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KEYS,
    if_exists: IfExists.Choice = "skip",
    token: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    log: str | None = None,
    level: str = "info",
):
    """
    Download source data for the CoRR dataset.

    **Possible keys:**
    * **raw**          All the raw imaging data
    * **meta**         Metadata

    Parameters
    ----------
    path
        Path to root of all datasets. A `CoRR` folder will be created.
    keys
        Data categories to download
    parts
        Parts to download
    if_exists
        Behaviour if a file already exists
    token
        Dataverse token
    packet
        Packet size to download, in bytes
    jobs
        Number of parallel downloaders
    log
        Path to log file

    """
    setup_filelog(log, level=level)
    path = Path(get_tree_path(path))
    keys = set(keys or KEYS)
    src = path / 'GSP' / 'sourcedata'
    auth = {Dataverse.TOKEN_HEADER: get_credentials(token)}

    def downloaders():
        for key in keys:
            for fname, id in FILE_IDS[key].items():
                yield Downloader(
                    URLBASE + id,
                    src / fname,
                    chunk_size=human2bytes(packet),
                    get_opt=dict(verify_ssl=False, headers=auth),
                    ifnodigest="continue",
                )

    filterwarnings('ignore', category=InsecureRequestWarning)
    DownloadManager(
        downloaders(),
        ifexists=if_exists,
        jobs=jobs,
    ).run("async")
