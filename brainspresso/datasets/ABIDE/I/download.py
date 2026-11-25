from enum import Enum
from typing import Iterable, Annotated
from logging import getLogger
from humanize import naturalsize
from cyclopts import Parameter

from brainspresso.utils.log import setup_filelog
from brainspresso.utils.keys import flatten_keys
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.datasets.ABIDE.I.command import abide1
from brainspresso.datasets.ABIDE.I.keys import allkeys
from brainspresso.datasets.ABIDE.I.download_nitrc import download_nitrc
from brainspresso.datasets.ABIDE.I.download_xnat import download_xnat
from brainspresso.datasets.ABIDE.I.download_s3 import download_s3


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
    "SourceChoice",
    [("s3", "s3"), ("nitrc", "nitrc"), ("xnat", "xnat")],
    type=str,
)

# Options that only apply to some sources
GroupNITRC = Parameter(group="nitrc/xnat (login required)")


@abide1.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.all,
    sites: Iterable[SiteChoice] = SITES,
    subs: Iterable[int | str] | None = tuple(),
    exclude_subs: Iterable[int | str] | None = tuple(),
    if_exists: IfExists.Choice = "skip",
    source: SourceChoice = SourceChoice.s3,
    user: Annotated[str | None, GroupNITRC] = None,
    password: Annotated[str | None, GroupNITRC] = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = None,
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
        Source to download from:

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
    log : str
        Path to log file

    """  # noqa: E501
    setup_filelog(log)

    if source == "s3":
        return download_s3(
            path,
            keys=keys,
            sites=sites if set(sites) != set(SITES) else (),
            subs=subs,
            exclude_subs=exclude_subs,
            if_exists=if_exists,
            packet=packet,
            jobs=jobs,
            log=log,
        )
    jobs = jobs or 1
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
