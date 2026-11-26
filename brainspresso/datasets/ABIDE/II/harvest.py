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

from brainspresso.sources.base import (
    CompositeSource, numeric_labels,
)
from brainspresso.sources.fcp import FCP
from brainspresso.sources.nitrc import NITRC

SITES = (
    'BNI', 'EMC', 'ETH', 'GU', 'IU', 'IP', 'KUL', 'KKI', 'NYU', 'ONRC',
    'OHSU', 'TCD', 'SDSU', 'SU', 'UCD', 'UCLA', 'U_MIA', 'USM', 'UPSM'
)
KEYS = ("raw", "meta")


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


