from pathlib import Path
from typing import Iterable
from logging import getLogger
from humanize import naturalsize

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.keys import flatten_keys
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.ui import human2bytes
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.sources.base import (
    DownloadOptions, Wants, configure_sources, harvest_sources,
    subject_selection,
)
from brainspresso.datasets.ABIDE.I.keys import allkeys
from brainspresso.datasets.ABIDE.I.sources import (
    DEFAULTS, SITES, sources as abide1_sources,
)


lg = getLogger(__name__)

KEYS = flatten_keys(allkeys)


# Options that only apply to some sources


def _values(x) -> list[str]:
    """A single value (including a str or str-Enum) or a list -> [str]"""
    if x is None:
        return []
    if isinstance(x, (str, int)):
        x = [x]
    return [str(getattr(v, "value", v)) for v in x]


def harvest_abide1(
    path=None, *, keys=("all",), sites=(), subs=(), exclude_subs=(),
    if_exists="skip", source=None, user=None, password=None,
    packet=naturalsize(CHUNK_SIZE), jobs=None, layout=None,
) -> dict:
    """Harvest ABIDE-I from its sources (per-key defaults, or `source`)"""
    dataset = resolve_layout(Path(get_tree_path(path)) / "ABIDE-1", layout)
    record_run(dataset, "harvest", {"dataset": "ABIDE-1"})
    sites = set(_values(sites))
    if sites == set(SITES):
        sites = set()
    available = configure_sources(
        abide1_sources(), sites=frozenset(sites), user=user,
        password=password,
    )
    return harvest_sources(
        available, DEFAULTS, dataset,
        selection=subject_selection(subs, exclude_subs, width=5),
        keys=Wants(_values(keys) or ["all"], allkeys),
        options=DownloadOptions(ifexists=if_exists, jobs=jobs,
                                chunk_size=human2bytes(packet)),
        override=_values(source) or None,
    )




def download_s3(
    path: str | None = None,
    *,
    keys: Iterable[str] = ("all",),
    sites: Iterable[str] = (),
    subs: Iterable[int] = (),
    exclude_subs: Iterable[int] = (),
    if_exists: IfExists.Choice = "skip",
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = None,
    layout: LayoutChoice | None = None,
    log: str | None = None,
    **kwargs,
) -> None:
    """
    Download ABIDE-I raw data and phenotypes from s3://fcp-indi.

    No login is needed. Only the "raw" (T1w, rest) and "meta" keys are
    downloaded; derivatives are not supported by this source yet.
    """
    setup_filelog(log)
    harvest_abide1(
        path, keys=keys, sites=sites, subs=subs, exclude_subs=exclude_subs,
        if_exists=if_exists, source="s3", packet=packet, jobs=jobs,
        layout=layout,
    )




