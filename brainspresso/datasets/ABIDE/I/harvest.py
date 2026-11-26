from enum import Enum
from pathlib import Path
from typing import Iterable, Annotated
from logging import getLogger
from humanize import naturalsize
from cyclopts import Parameter

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
from brainspresso.datasets.ABIDE.I.command import abide1
from brainspresso.datasets.ABIDE.I.keys import allkeys
from brainspresso.datasets.ABIDE.I.sources import (
    DEFAULTS, SITES, sources as abide1_sources,
)


lg = getLogger(__name__)

KEYS = flatten_keys(allkeys)

SiteChoice = Enum("SiteChoice", [(site, site) for site in SITES], type=str)
KeyChoice = Enum("KeyChoice", [(site, site) for site in KEYS], type=str)
SourceChoice = Enum(
    "SourceChoice",
    [("s3", "s3"), ("nitrc", "nitrc"), ("xnat", "xnat")],
    type=str,
)

# Options that only apply to some sources
GroupNITRC = Parameter(group="nitrc/xnat (login required)")


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


@abide1.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.all,
    sites: Iterable[SiteChoice] = SITES,
    subs: Iterable[int | str] | None = tuple(),
    exclude_subs: Iterable[int | str] | None = tuple(),
    if_exists: IfExists.Choice = "skip",
    source: SourceChoice | None = None,
    user: Annotated[str | None, GroupNITRC] = None,
    password: Annotated[str | None, GroupNITRC] = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = None,
    layout: LayoutChoice | None = None,
    log: str | None = None,
):
    """
    Download source data for the ABIDE-I dataset.

    **Key hierarchy:**

    * all
      * meta                Metadata
      * raw                 All raw data
        * anat             Anatomical T1w scans
        * rest             Resting-state fMRI
      * derivatives         All derivatives
        * proc             All processed data
          * proc-min      Minimally processed data
          * ants          ANTs
          * ccs           Connectome Computation System
          * civet         Civet
          * cpac          Configurable Pipeline for the Analysis of Connectomes
          * dparsf        Data Processing Assistant for Resting-State fMRI
          * fs            FreeSurfer
          * niak          NeuroImaging Analysis Kit
        * qa               All quality assessment
          * qa-man        Manual QA
          * qa-pcp        Preprocessed Connectomes Project

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `ABIDE-1` folder will be created.
    keys : [list of] str
        Data categories to download
    sites : [list of] str
        Only download these sites (s3, nitrc).
    subs : [list of] int
        Only download these subjects (s3, xnat; all if empty).
    exclude_subs : [list of] int
        Do not download these subjects (s3, xnat)
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    source : {"s3", "nitrc", "xnat"}
        Source to download from (default: s3, and nitrc if s3 is
        unavailable):

        * s3: public INDI bucket (s3://fcp-indi), no login. Raw data
          and phenotypes only.
        * nitrc: NITRC tarballs (login required).
        * xnat: NITRC-IR (login required), including derivatives.
    user : str
        NITRC username
    password : str
        NITRC password
    packet : int
        Packet size to download, in bytes
    jobs : int
        Number of parallel downloads (default: 4 with s3, 1 otherwise)
    layout : {"study", "flat"}
        Layout of the dataset (where source data does not move, but
        later commands reuse it): "study" (raw dataset in
        `rawbids/`) or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log : str
        Path to log file

    """  # noqa: E501
    setup_filelog(log)
    harvest_abide1(
        path, keys=keys, sites=sites, subs=subs, exclude_subs=exclude_subs,
        if_exists=if_exists, source=source, user=user, password=password,
        packet=packet, jobs=jobs, layout=layout,
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


@abide1.command
def download_nitrc(
    path: str | None = None,
    *,
    keys: Iterable[str] = ("raw", "meta"),
    sites: Iterable[SiteChoice] = SITES,
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    layout: LayoutChoice | None = None,
    log: str | None = None,
    level: str = "info",
):
    """
    Download source data for the ABIDE-I dataset (NITRC tarballs).

    Same as `harvest --source nitrc`.

    **Possible keys:**
    * **raw**          All the raw imaging data
    * **meta**         Metadata

    Parameters
    ----------
    path
        Path to root of all datasets. An `ABIDE-1` folder will be created.
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
    layout
        Layout of the dataset (where source data does not move, but
        later commands reuse it): "study" (raw dataset in
        `rawbids/`) or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log
        Path to log file
    level
        Logging level
    """
    setup_filelog(log, level=level)
    harvest_abide1(
        path, keys=keys, sites=sites, if_exists=if_exists, source="nitrc",
        user=user, password=password, packet=packet, jobs=jobs,
        layout=layout,
    )


@abide1.command
def download_xnat(
    path: str | None = None,
    *,
    keys: Iterable[str] = "all",
    subs: Iterable[int | str] | None = tuple(),
    exclude_subs: Iterable[int | str] | None = tuple(),
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    layout: LayoutChoice | None = None,
    log: str | None = None,
):
    """
    Download source data for the ABIDE-I dataset (NITRC-IR).

    Same as `harvest --source xnat`. See `harvest` for the keys.

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `ABIDE-1` folder will be created.
    keys : [list of] str
        Data categories to download
    subs : [list of] int
        Only download these subjects (all if empty; ranges `start:stop`
        and files with one subject per line are accepted)
    exclude_subs : [list of] int
        Do not download these subjects
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
    harvest_abide1(
        path, keys=keys, subs=subs, exclude_subs=exclude_subs,
        if_exists=if_exists, source="xnat", user=user, password=password,
        packet=packet, jobs=jobs, layout=layout,
    )
