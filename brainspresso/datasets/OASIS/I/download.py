from pathlib import Path
from typing import Optional, Literal, Iterable, Union
from humanize import naturalsize

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.ui import human2bytes
from brainspresso.download import CHUNK_SIZE
from brainspresso.download import IfExists
from brainspresso.sources.base import DownloadOptions, StaticURLs, Wants
from brainspresso.datasets.OASIS.I.command import oasis1


URLDATA = 'https://download.nrg.wustl.edu/data'
URLMETA = 'https://sites.wustl.edu/oasisbrains/files'
UID_META = '5708aa0a98d82080'
UID_RELI = '063c8642b909ee76'
UID_FACT = 'bcc7a002dfb104f4'
URLS = {
    'meta':
        f'{URLMETA}/2024/04/oasis_cross-sectional-{UID_META}.xlsx',
    'reliability':
        f'{URLMETA}/2024/04/oasis_cross-sectional-reliability-{UID_RELI}.xlsx',
    'facts':
        f'{URLMETA}/2024/03/oasis_cross-sectional_facts-{UID_FACT}.pdf',
    'raw': [
        URLDATA + '/oasis_cross-sectional_disc{:d}.tar.gz'.format(d)
        for d in range(1, 13)
    ],
    'fs': [
        URLDATA + '/oasis_cs_freesurfer_disc{:d}.tar.gz'.format(d)
        for d in range(1, 12)  # !!! no freesurfer disk 12
    ],
}

KeyChoice = Literal["raw", "fs", "meta", "reliability", "facts"]

# local names of the spreadsheets (without their upload id)
NAMES = {
    'meta': 'oasis_cross-sectional.xlsx',
    'reliability': 'oasis_cross-sectional-reliability.xlsx',
    'facts': 'oasis_cross-sectional_facts.pdf',
}


class WUSTL(StaticURLs):
    """OASIS-I discs (download.nrg.wustl.edu) and spreadsheets
    (sites.wustl.edu), no login. Per-run option: `discs`."""

    name = "wustl"
    discs: frozenset = frozenset(range(1, 13))
    urls = {
        key: (
            [StaticURLs.entry(url, disc=disc)
             for disc, url in enumerate(value, 1)]
            if isinstance(value, list) else
            [StaticURLs.entry(value, NAMES[key])]
        )
        for key, value in URLS.items()
    }

    def keep(self, entry, selection, wants) -> bool:
        disc = entry.meta.get("disc")
        return disc is None or disc in self.discs


@oasis1.command(name="harvest")
def download(
    path: Optional[str] = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
    discs: Iterable[int] = tuple(range(1, 13)),
    if_exists: IfExists.Choice = "skip",
    packet: Union[int, str] = naturalsize(CHUNK_SIZE),
    layout: LayoutChoice | None = None,
    log: Optional[str] = None,
):
    """
    Download source data for the OASIS-I dataset.

    **Possible keys:**
    * **raw**          All the raw imaging data
    * **fs**           Data processed with FreeSurfer
    * **meta**         Metadata
    * **reliability**  Repeatability measures data sheet
    * **facts**        Fact sheet

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-1` folder will be created.
    keys : [list of] {"raw", "fs", "meta", "reliability", "facts"}
        Data categories to download
    discs : [list of] {1..12}
        Discs to download
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    packet : int
        Packet size to download, in bytes
    layout : {"study", "flat"}
        Layout of the dataset (where source data does not move, but
        later commands reuse it): "study" (raw dataset in
        `rawbids/`) or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log : str
        Path to log file

    """
    setup_filelog(log)
    path = Path(get_tree_path(path))
    if isinstance(keys, str):
        keys = [keys]
    keys = set(keys or URLS.keys())
    dataset = resolve_layout(path / 'OASIS-1', layout)
    record_run(dataset, "harvest", {"dataset": "OASIS-1"})
    WUSTL().configure(discs=frozenset(discs or range(1, 13))).harvest(
        dataset, keys=Wants(keys),
        options=DownloadOptions(ifexists=if_exists,
                                chunk_size=human2bytes(packet)),
    )
