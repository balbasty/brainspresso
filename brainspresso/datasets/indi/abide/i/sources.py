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
are only on xnat (`--source xnat`). s3 and nitrc are roasted by
`roaster.roast_nitrc`, xnat by `roaster.roast_xnat`.
"""
from __future__ import annotations

from brainspresso.datasets.indi.abide.i.roaster import (
    SCAN_ID,
    is_freesurfer,
    roast_nitrc,
    roast_xnat,
)
from brainspresso.datasets.indi.abide.i.sites import SITES
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

# Subject labels: `0051456` (host) <-> `51456` (BIDS)
LABELS = numeric_labels(5)

# file name in RawData -> key
RAW_FILES = {"mprage.nii.gz": "T1w", "rest.nii.gz": "rest"}


def _site(name: str) -> str:
    """Site of a folder of `RawData/` (`MaxMun_a` -> `MaxMun`)."""
    return name.split("_")[0]


def _pheno_site(name: str) -> str:
    """
    Site of a phenotype file, upper-cased, without its sample.

    `phenotypic_MAX_MUN.csv` -> `MAXMUN`, `phenotypic_UCLA_1.csv` ->
    `UCLA`.
    """
    stem = name[len("phenotypic_"):].rsplit(".", 1)[0]
    stem = stem.upper().replace("MAX_MUN", "MAXMUN")
    return stem.split("_")[0]


class SitesOption:
    """Per-run option `sites` (upper-cased site names; empty: all)."""

    sites: frozenset = frozenset()

    def keeps_site(self, site: str) -> bool:
        """Whether a site is selected (case-insensitive)."""
        sites = {s.upper() for s in self.sites}
        return not sites or site.upper() in sites


# ----------------------------------------------------------------------
#   s3://fcp-indi
# ----------------------------------------------------------------------

class S3(SitesOption, FCP):
    """ABIDE-I raw data and phenotypes from s3://fcp-indi."""

    name = "s3"
    provides = frozenset({"raw", "meta"})
    labels = LABELS

    def __init__(self, **kwargs):
        super().__init__(PROJECT, **kwargs)

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        """List the phenotypes and images (see `Source.list`)."""
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
        """Whether to fetch a file of `PhenotypicData/`."""
        rel = entry.relpath
        if not (rel.name.startswith("phenotypic_") and rel.suffix == ".csv"):
            return False
        return self.keeps_site(_pheno_site(rel.name))

    def keep_raw(self, entry: RemoteEntry, selection, wants) -> bool:
        """Whether to fetch a file of `RawData/`."""
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
        """Phenotypes at the top of `sourcedata/`, images as listed."""
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
    """ABIDE-I tarballs and phenotypes from NITRC (login)."""

    provides = frozenset({"raw", "meta"})
    urls = {
        key: [NITRC.entry(url, site=site)
              for site, per_key in URLS.items() for url in per_key[key]]
        for key in ("raw", "meta")
    }

    def keep(self, entry, selection, wants) -> bool:
        """Whether to fetch a file (its site is selected)."""
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


def scan_key(scan: dict) -> str:
    """
    Key of an XNAT scan record.

    Parameters
    ----------
    scan : dict
        XNAT record (`ID`: `anat`, `mprage`, `anat_2`, `rest`...)

    Returns
    -------
    str
        `rest` or `anat` for the scans the roast converts (the names it
        accepts, `roaster.SCAN_ID`), else the scan id itself
    """
    match = SCAN_ID.fullmatch(str(scan.get("ID", "")))
    if match is None:
        return str(scan.get("ID", ""))
    return "rest" if match["mod"].lower() == "rest" else "anat"


def assessor_keys(assessor: dict) -> tuple[str, ...]:
    """
    Keys of an XNAT assessor record.

    FreeSurfer outputs are `fs` and `fs-all` (`is_freesurfer`: label
    `...freesurfer...`, `..._fs`, `...-fs`; or type `fs:*`; the exact
    label on NITRC-IR is not confirmed), the others are named by the
    suffix of their label (`ASSESSOR_KEYS`).

    Parameters
    ----------
    assessor : dict
        XNAT record (`label`, `xsiType`...)

    Returns
    -------
    tuple[str]
    """
    if is_freesurfer_assessor(assessor):
        return ("fs", "fs-all")
    suffix = str(assessor.get("label", "")).split("-")[-1]
    return (ASSESSOR_KEYS.get(suffix, suffix),)


def is_freesurfer_assessor(assessor: dict) -> bool:
    """Whether an XNAT assessor record holds FreeSurfer outputs."""
    kind = str(assessor.get("xsiType", assessor.get("type", "")))
    return is_freesurfer(str(assessor.get("label", ""))) \
        or kind.lower().startswith("fs:")


class XNATImages(XNAT):
    """ABIDE-I scans and assessors from NITRC-IR (login)."""

    project = "ABIDE"
    provides = frozenset({"raw", "derivatives"})

    # subject (and experiment) labels end with `_<id>`
    labels = numeric_labels(5)

    def keeps_subject(self, selection, host_id) -> bool:
        """Whether a subject is selected (label `<site>_<id>`)."""
        return self.labels.keeps(selection, str(host_id).split("_")[-1])

    def keep_experiment(self, subject, experiment, wants) -> bool:
        """Keep the experiment named as its subject."""
        return experiment["label"] == subject["label"]

    def keep_scan(self, subject, experiment, scan, wants) -> bool:
        """Whether a scan is wanted (`anat`, `rest`, `anat_2`...)."""
        return wants(scan_key(scan))

    def keep_assessor(self, subject, experiment, assessor, wants) -> bool:
        """Whether an assessor is wanted (`assessor_keys`)."""
        return any(wants(key) for key in assessor_keys(assessor))

    def local_path(self, entry: RemoteEntry) -> str:
        """
        Path `<subject>/<scan or assessor>.tar.gz` (experiment = subject).

        A FreeSurfer assessor whose label the roast would not recognise
        (`is_freesurfer`, e.g. one known by its type only) is saved as
        `<subject>/<label>-freesurfer.tar.gz`.
        """
        name = str(entry.meta["id"])
        record = entry.meta.get("record") or {}
        if entry.meta.get("type") == "assessor" and \
                is_freesurfer_assessor(record) and not is_freesurfer(name):
            name += "-freesurfer"
        return f"{entry.meta['subject']}/{name}.tar.gz"


class XNATMeta(NITRC):
    """ABIDE-I phenotypes (NITRC login) and scan parameters (public)."""

    provides = frozenset({"meta"})
    urls = {"meta": [NITRC.entry(URL_META)] + [
        NITRC.entry(f'{URL_PARAM}/{site}/{mod}.pdf', f'{site}/{mod}.pdf')
        for site in XNAT_SITES for mod in ("anat", "rest")
    ]}


def xnat_source() -> CompositeSource:
    """Return the xnat source: images (NITRC-IR) and phenotypes (NITRC)."""
    return CompositeSource(XNATImages(), XNATMeta(), name="xnat",
                           roaster=roast_xnat)


def sources() -> dict:
    """Return the sources of ABIDE-I (with their roasters)."""
    return {"s3": S3(roaster=roast_nitrc),
            "nitrc": NITRCTarballs(roaster=roast_nitrc),
            "xnat": xnat_source()}


DEFAULTS = {"raw": ["s3", "nitrc"], "meta": ["s3", "nitrc"]}
