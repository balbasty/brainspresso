"""
Harvest ABIDE-II.

Sources:

* **nitrc** (default): NITRC tarballs
  (`ABIDEII-<SITE>_<sample>[_<part>].tar.gz`) and phenotypes
  (`ABIDEII-<SITE>_<sample>.csv`), login required. This is what
  `roast` reads.
* **s3** (`--source s3`, raw data only): the public INDI bucket
  (`s3://fcp-indi/data/Projects/ABIDE2/RawData/`), no login, mirrored in
  `sourcedata/RawData/`. The bucket holds a re-curated, BIDS-like copy
  of the images (one dataset per site, runs and acquisitions, sidecars
  in `RawData/sidecards/`), not the tarballs: `roast` does not read it
  yet. Sites that the bucket lacks (SU, U_MIA) come from NITRC.
"""
import re
from enum import Enum
from humanize import naturalsize
from pathlib import Path
from typing import Iterable

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.utils.ui import human2bytes
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.sources.base import (
    CompositeSource, DownloadOptions, Wants, configure_sources,
    harvest_sources, numeric_labels,
)
from brainspresso.sources.fcp import FCP
from brainspresso.sources.nitrc import NITRC
from brainspresso.datasets.ABIDE.II.command import abide2

SITES = (
    'BNI', 'EMC', 'ETH', 'GU', 'IU', 'IP', 'KUL', 'KKI', 'NYU', 'ONRC',
    'OHSU', 'TCD', 'SDSU', 'SU', 'UCD', 'UCLA', 'U_MIA', 'USM', 'UPSM'
)
KEYS = ("raw", "meta")

SiteChoice = Enum("SiteChoice", [(site, site) for site in SITES], type=str)
KeyChoice = Enum("KeyChoice", [("raw", "raw"), ("meta", "meta")], type=str)
SourceChoice = Enum("SourceChoice", [("nitrc", "nitrc"), ("s3", "s3")],
                    type=str)

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

# Site folder names of the bucket (`ABIDEII-<name>_<sample>`) -> site
S3_SITES = {'ETHZ': 'ETH'}
# Sites that the bucket lacks
NOT_ON_S3 = frozenset({'SU', 'U_MIA'})


class SitesOption:
    sites: frozenset = frozenset()

    def keeps_site(self, site: str) -> bool:
        return not self.sites or site in self.sites


class NITRCTarballs(SitesOption, NITRC):
    """ABIDE-II tarballs and phenotypes from NITRC (login)"""

    provides = frozenset({"raw", "meta"})
    urls = {
        key: [NITRC.entry(url, site=site)
              for site, per_key in URLS.items() for url in per_key[key]]
        for key in KEYS
    }

    def keep(self, entry, selection, wants) -> bool:
        return self.keeps_site(entry.meta["site"])


class NITRCMissingFromS3(NITRCTarballs):
    """Tarballs of the sites that the bucket lacks"""

    provides = frozenset({"raw"})

    def keep(self, entry, selection, wants) -> bool:
        return entry.meta["site"] in NOT_ON_S3 and \
            super().keep(entry, selection, wants)


def s3_site(folder: str) -> str:
    """'ABIDEII-ETHZ_1' -> 'ETH', 'ABIDEII-UCLA_Long' -> 'UCLA'"""
    name = folder[len("ABIDEII-"):].rsplit("_", 1)[0]
    return S3_SITES.get(name, name)


def sidecar_site(parts) -> str | None:
    """Site of a sidecar path below `sidecards/` (None if unknown)"""
    for part in parts:
        m = re.match(r"^(ABIDEII-[^.]*?)(?:_[a-z]+-.*|\.[^/]*)?$", part)
        if m:
            return s3_site(m.group(1))
    return None


class S3(SitesOption, FCP):
    """ABIDE-II images from s3://fcp-indi (BIDS-like copy, no login)"""

    name = "s3"
    provides = frozenset({"raw"})
    labels = numeric_labels(5)

    def __init__(self, **kwargs):
        super().__init__("ABIDE2/RawData", **kwargs)

    def keep(self, entry, selection, wants) -> bool:
        parts = entry.relpath.parts
        if len(parts) < 2 or "derivatives" in parts:
            return False
        if parts[0] == "sidecards":
            # Sidecars: the site is in the first folder or file name that
            # starts with "ABIDEII-<SITE>_<n>" (BIDS entities and extension
            # stripped). If no such name, the site is unknown: keep it.
            site = sidecar_site(parts[1:])
            if site is not None and not self.keeps_site(site):
                return False
        elif not self.keeps_site(s3_site(parts[0])):
            return False
        sub = next((p[4:] for p in parts[1:] if p.startswith("sub-")), None)
        return sub is None or self.keeps_subject(selection, sub)

    def local_path(self, entry) -> str:
        return f"RawData/{entry.path}"


def sources() -> dict:
    """Sources of ABIDE-II"""
    return {
        "nitrc": NITRCTarballs(),
        "s3": CompositeSource(S3(), NITRCMissingFromS3(), name="s3"),
    }


DEFAULTS = {"raw": ["nitrc"], "meta": ["nitrc"]}


@abide2.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KEYS,
    sites: Iterable[SiteChoice] = SITES,
    if_exists: IfExists.Choice = "skip",
    source: SourceChoice | None = None,
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    layout: LayoutChoice | None = None,
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
    if_exists
        Behaviour if a file already exists
    source
        * nitrc (default): NITRC tarballs and phenotypes (login
          required), read by `roast`.
        * s3: raw images from the public INDI bucket (a BIDS-like copy,
          not read by `roast` yet), and NITRC tarballs for the sites
          that the bucket lacks.
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

    """
    setup_filelog(log)
    path = Path(get_tree_path(path))
    if isinstance(keys, str):
        keys = [keys]
    if isinstance(sites, str):
        sites = [sites]
    keys = {str(getattr(k, "value", k)) for k in keys or KEYS}
    sites = {str(getattr(s, "value", s)) for s in sites or SITES}
    dataset = resolve_layout(path / 'ABIDE-2', layout)
    record_run(dataset, "harvest", {"dataset": "ABIDE-2"})
    harvest_sources(
        configure_sources(sources(), sites=frozenset(sites), user=user,
                          password=password),
        DEFAULTS, dataset, keys=Wants(keys),
        options=DownloadOptions(ifexists=if_exists, jobs=jobs,
                                chunk_size=human2bytes(packet)),
        override=str(getattr(source, "value", source)) if source else None,
    )
