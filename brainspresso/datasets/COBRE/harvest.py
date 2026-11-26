from pathlib import Path
from humanize import naturalsize
from typing import Literal, Iterable

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.ui import human2bytes
from brainspresso.download import CHUNK_SIZE
from brainspresso.download import IfExists
from brainspresso.sources.base import DownloadOptions, Wants
from brainspresso.sources.nitrc import NITRC
from brainspresso.datasets.COBRE.command import cobre


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


def sources() -> dict:
    """Sources of COBRE: NITRC files (login), four at a time"""
    return {"nitrc": NITRC(URLS, jobs=4)}


@cobre.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = tuple(URLS.keys()),
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    layout: LayoutChoice | None = None,
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
    layout : {"study", "flat"}
        Layout of the dataset (where source data does not move, but
        later commands reuse it): "study" (raw dataset in
        `rawbids/`) or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log : str
        Path to log file
    """
    setup_filelog(log)
    if isinstance(keys, str):
        keys = [keys]
    keys = list({key: None for key in keys or URLS}.keys())
    path: Path = Path(get_tree_path(path))
    dataset = resolve_layout(path / 'COBRE', layout)
    record_run(dataset, "harvest", {"dataset": "COBRE"})
    sources()["nitrc"].configure(user=user, password=password).harvest(
        dataset, keys=Wants(keys),
        options=DownloadOptions(ifexists=if_exists, on_error="raise",
                                chunk_size=human2bytes(packet)),
    )
