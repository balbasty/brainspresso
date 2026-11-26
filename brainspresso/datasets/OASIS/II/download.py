from pathlib import Path
from typing import Literal, Iterable
from humanize import naturalsize

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.utils.ui import human2bytes
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.sources.base import DownloadOptions, StaticURLs, Wants
from brainspresso.datasets.OASIS.II.command import oasis2


URLBASE = 'https://download.nrg.wustl.edu/data'
OASISBASE = 'https://sites.wustl.edu/oasisbrains/files/2024/03/'
URLS = {
    'raw': [
        f'{URLBASE}/OAS2_RAW_PART1.tar.gz',
        f'{URLBASE}/OAS2_RAW_PART2.tar.gz',
    ],
    'meta': [
        f'{OASISBASE}/oasis_longitudinal_demographics-8d83e569fa2e2d30.xlsx',
    ],
}

KeyChoice = Literal["raw", "meta"]


class WUSTL(StaticURLs):
    """OASIS-II parts (download.nrg.wustl.edu) and demographics
    (sites.wustl.edu), no login. Per-run option: `parts`."""

    name = "wustl"
    parts: frozenset = frozenset({1, 2})
    urls = {
        "raw": [StaticURLs.entry(url, part=part)
                for part, url in enumerate(URLS["raw"], 1)],
        "meta": [StaticURLs.entry(URLS["meta"][0],
                                  "oasis_longitudinal_demographics.xlsx")],
    }

    def keep(self, entry, selection, wants) -> bool:
        part = entry.meta.get("part")
        return part is None or part in self.parts


@oasis2.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
    parts: Iterable[int] = (1, 2),
    if_exists: IfExists.Choice = "skip",
    packet: int | str = naturalsize(CHUNK_SIZE),
    layout: LayoutChoice | None = None,
    log: str | None = None,
):
    """
    Download source data for the OASIS-II dataset.

    **Possible keys:**
    * **raw**          All the raw imaging data
    * **fs**           Data processed with FreeSurfer
    * **meta**         Metadata
    * **reliability**  Repeatability measures data sheet
    * **facts**        Fact sheet

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-2` folder will be created.
    keys : [list of] {"raw", "meta"}
        Data categories to download
    parts : [list of] {1, 2}
        Parts to download
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
    dataset = resolve_layout(path / 'OASIS-2', layout)
    record_run(dataset, "harvest", {"dataset": "OASIS-2"})
    WUSTL().configure(parts=frozenset(parts or (1, 2))).harvest(
        dataset, keys=Wants(keys),
        options=DownloadOptions(ifexists=if_exists,
                                chunk_size=human2bytes(packet)),
    )
