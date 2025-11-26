from enum import Enum
from typing import Iterable, Annotated
from logging import getLogger
from humanize import naturalsize
from cyclopts import Parameter

from braindataprep.utils.log import setup_filelog
from braindataprep.utils.keys import flatten_keys
from braindataprep.download import IfExists
from braindataprep.download import CHUNK_SIZE
from braindataprep.datasets.ABIDE.I.command import abide1
from braindataprep.datasets.ABIDE.I.keys import allkeys
from braindataprep.datasets.ABIDE.I.download_nitrc import download_nitrc
from braindataprep.datasets.ABIDE.I.download_xnat import download_xnat


lg = getLogger(__name__)


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
KEYS = flatten_keys(allkeys)

SiteChoice = Enum("SiteChoice", [(site, site) for site in SITES], type=str)
KeyChoice = Enum("KeyChoice", [(site, site) for site in KEYS], type=str)
SourceChoice = Enum(
    "SourceChoice", [("nitrc", "nitrc"), ("xnat", "xnat")], type=str
)

GroupNITRC = Parameter(group="nitrc")
GroupXNAT = Parameter(group="xnat")


@abide1.command
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.all,
    sites: Annotated[Iterable[SiteChoice], GroupNITRC] = SITES,
    subs:  Annotated[Iterable[int | str] | None, GroupXNAT] = tuple(),
    exclude_subs: Annotated[Iterable[int | str] | None, GroupXNAT] = tuple(),
    if_exists: IfExists.Choice = "skip",
    source: SourceChoice = SourceChoice.nitrc,
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
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
        * qa               All quality assesment
          * qa-man        Manual QA
          * qa-pcp        Preprocessed Connectomes Project

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `ABIDE-1` folder will be created.
    keys : [list of] str
        Data categories to download
    sites : [list of] str
        Only download these sites.
    subs : [list of] int
        Only download these subjects (all if empty).
    exclude_subs : [list of] int
        Do not download these subjects
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    source : {"nitrc", "xnat"}
        Source to download from.
    user : str
        NITRC username
    password : str
        NITRC password
    packet : int
        Packet size to download, in bytes
    log : str
        Path to log file

    """  # noqa: E501
    setup_filelog(log)

    if source == "nitrc":
        return download_nitrc(
            path,
            keys=keys,
            sites=sites,
            if_exists=if_exists,
            user=user,
            password=password,
            packet=packet,
            jobs=jobs,
            log=log,
        )
    elif source == "xnat":
        return download_xnat(
            path,
            keys=keys,
            subs=subs,
            exclude_subs=exclude_subs,
            if_exists=if_exists,
            user=user,
            password=password,
            packet=packet,
            jobs=jobs,
            log=log,
        )
