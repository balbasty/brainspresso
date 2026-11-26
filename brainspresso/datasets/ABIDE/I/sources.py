"""
Sources of ABIDE-I.

* **s3**: public INDI bucket (`s3://fcp-indi/data/Projects/ABIDE`), no
  login. Raw data (`RawData/<Site>/<id>/session_<n>/<mod>_<n>/*.nii.gz`)
  and phenotypes (`phenotypic_<SITE>.csv`), same layout as the NITRC
  tarballs once unpacked.
* **nitrc**: NITRC tarballs (`<Site>[_<sample>][_<part>].tgz`) and
  phenotypes (login required).
* **xnat**: NITRC-IR (login required): scans and derivatives (assessors)
  as `<subject>/<scan>.tar.gz`, phenotypes (`Phenotypic_V1_0b.csv`)
  and scan parameters (`<site>/{anat,rest}.pdf`).

The default is s3, then nitrc, for raw data and phenotypes; derivatives
are only on xnat (`--source xnat`).
"""
from __future__ import annotations

from brainspresso.sources.base import (
    CompositeSource,
    RemoteEntry,
    as_wants,
    numeric_labels,
)
from brainspresso.sources.fcp import FCP
from brainspresso.sources.nitrc import NITRC
from brainspresso.sources.xnat import XNAT

PROJECT = "ABIDE"

SITES = (
    'Caltech', 'CMU', 'KKI', 'Leuven', 'MaxMun', 'NYU', 'OHSU', 'Olin',
    'Pitt', 'SBL', 'SDSU', 'Stanford', 'Trinity', 'UCLA', 'UM', 'USM',
    'Yale',
)

# Subject labels: `0051456` (host) <-> `51456` (BIDS)
LABELS = numeric_labels(5)

# file name in RawData -> key
RAW_FILES = {"mprage.nii.gz": "T1w", "rest.nii.gz": "rest"}


def _site(name: str) -> str:
    """'MaxMun_a' -> 'MaxMun', 'UCLA_1' -> 'UCLA'"""
    return name.split("_")[0]


def _pheno_site(name: str) -> str:
    """'phenotypic_MAX_MUN.csv' -> 'MAXMUN'; 'phenotypic_UCLA_1.csv' -> 'UCLA'
    (site names upper-cased, sample suffix dropped)"""
    stem = name[len("phenotypic_"):].rsplit(".", 1)[0]
    stem = stem.upper().replace("MAX_MUN", "MAXMUN")
    return stem.split("_")[0]


class SitesOption:
    """Per-run option `sites` (upper-cased site names; empty: all)"""

    sites: frozenset = frozenset()

    def keeps_site(self, site: str) -> bool:
        sites = {s.upper() for s in self.sites}
        return not sites or site.upper() in sites


# ----------------------------------------------------------------------
#   s3://fcp-indi
# ----------------------------------------------------------------------

class S3(SitesOption, FCP):
    """ABIDE-I raw data and phenotypes from s3://fcp-indi"""

    name = "s3"
    provides = frozenset({"raw", "meta"})
    labels = LABELS

    def __init__(self, **kwargs):
        super().__init__(PROJECT, **kwargs)

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        wants = as_wants(keys)
        out = []
        if wants("meta"):
            out += [e for e in self.entries("PhenotypicData/")
                    if self.keep_pheno(e)]
        if wants("raw"):
            out += [e for e in self.entries("RawData/")
                    if self.keep_raw(e, selection, wants)]
        return out

    def keep_pheno(self, entry: RemoteEntry) -> bool:
        rel = entry.relpath
        if not (rel.name.startswith("phenotypic_") and rel.suffix == ".csv"):
            return False
        return self.keeps_site(_pheno_site(rel.name))

    def keep_raw(self, entry: RemoteEntry, selection, wants) -> bool:
        # RawData/<Site>/<id>/session_<n>/<mod>_<n>/<file>
        parts = entry.relpath.parts[1:]
        if len(parts) != 5:
            return False
        site, sub, *_, fname = parts
        if fname not in RAW_FILES or not wants(RAW_FILES[fname]):
            return False
        if not self.keeps_site(_site(site)):
            return False
        return self.keeps_subject(selection, sub)

    def local_path(self, entry: RemoteEntry) -> str:
        if entry.path.startswith("PhenotypicData/"):
            return entry.name
        return entry.path


# ----------------------------------------------------------------------
#   NITRC tarballs
# ----------------------------------------------------------------------

URLBASE = 'https://fcp_private.projects.nitrc.org/downloads'
URLBASE += '/abide_mrdata_r01_release'
IMGBASE = f'{URLBASE}/ImagingData'
PHNBASE = f'{URLBASE}/PhenotypicData'
SAMPS = {'Leuven': ['_1', '_2'], 'UCLA': ['_1', '_2'], 'UM': ['_1', '_2']}
PARTS = {
    'CMU': ['_a', '_b'],
    'MaxMun': ['_a', '_b', '_c', '_d'],
    'NYU': ['_a', '_b', '_c', '_d', '_e'],
}
URLS = {
    SITE: {
        'raw': [
            f'{IMGBASE}/{SITE}{SAMP}{SUFFIX}.tgz'
            for SUFFIX in PARTS.get(SITE, [''])
            for SAMP in SAMPS.get(SITE, [''])
        ],
        'meta': [
            f'{PHNBASE}/phenotypic_{FIXED_SITE}{SAMP}.csv'
            for FIXED_SITE in [{'MaxMun': 'Max_Mun'}.get(SITE, SITE).upper()]
            for SAMP in SAMPS.get(SITE, [''])
        ],
    }
    for SITE in SITES
}


class NITRCTarballs(SitesOption, NITRC):
    """ABIDE-I tarballs and phenotypes from NITRC (login)"""

    provides = frozenset({"raw", "meta"})
    urls = {
        key: [NITRC.entry(url, site=site)
              for site, per_key in URLS.items() for url in per_key[key]]
        for key in ("raw", "meta")
    }

    def keep(self, entry, selection, wants) -> bool:
        return self.keeps_site(entry.meta["site"])


# ----------------------------------------------------------------------
#   NITRC-IR (XNAT)
# ----------------------------------------------------------------------

XNAT_SITES = [
    'Caltech', 'CMU', 'KKI', 'Leuven-1', 'Leuven-2', 'MaxMun', 'NYU',
    'OHSU', 'Olin', 'Pitt', 'SBL', 'SDSU', 'Stanford', 'Trinity', 'UCLA1',
    'UCLA2', 'UM1', 'UM2', 'USM', 'Yale',
]
URL_META = f'{PHNBASE}/Phenotypic_V1_0b.csv'
URL_PARAM = 'https://fcon_1000.projects.nitrc.org/indi/abide/scan_params'

# assessor label suffix -> key
ASSESSOR_KEYS = {'man_qa': 'qa-man', 'pcp_qa': 'qa-pcp',
                 'min_proc': 'proc-min'}


class XNATImages(XNAT):
    """ABIDE-I scans and assessors from NITRC-IR (login)"""

    project = "ABIDE"
    provides = frozenset({"raw", "derivatives"})

    # subject (and experiment) labels end with `_<id>`
    labels = numeric_labels(5)

    def keeps_subject(self, selection, host_id) -> bool:
        return self.labels.keeps(selection, str(host_id).split("_")[-1])

    def keep_experiment(self, subject, experiment, wants) -> bool:
        return experiment["label"] == subject["label"]

    def keep_scan(self, subject, experiment, scan, wants) -> bool:
        return wants(scan["ID"])

    def keep_assessor(self, subject, experiment, assessor, wants) -> bool:
        suffix = assessor["label"].split("-")[-1]
        return wants(ASSESSOR_KEYS.get(suffix, suffix))

    def local_path(self, entry: RemoteEntry) -> str:
        # <subject>/<scan or assessor>.tar.gz (experiment = subject)
        return f"{entry.meta['subject']}/{entry.meta['id']}.tar.gz"


class XNATMeta(NITRC):
    """ABIDE-I phenotypes (NITRC login) and scan parameters (public)"""

    provides = frozenset({"meta"})
    urls = {"meta": [NITRC.entry(URL_META)] + [
        NITRC.entry(f'{URL_PARAM}/{site}/{mod}.pdf', f'{site}/{mod}.pdf')
        for site in XNAT_SITES for mod in ("anat", "rest")
    ]}


def xnat_source() -> CompositeSource:
    return CompositeSource(XNATImages(), XNATMeta(), name="xnat")


def sources() -> dict:
    """Sources of ABIDE-I"""
    return {"s3": S3(), "nitrc": NITRCTarballs(), "xnat": xnat_source()}


DEFAULTS = {"raw": ["s3", "nitrc"], "meta": ["s3", "nitrc"]}
