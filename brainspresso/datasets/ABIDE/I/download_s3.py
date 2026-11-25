"""
ABIDE-I from the public INDI bucket: s3://fcp-indi/data/Projects/ABIDE

Downloaded layout (identical to the content of the NITRC tarballs):

    ABIDE-1/sourcedata/
        phenotypic_<SITE>.csv
        RawData/<Site>[_<sample>]/<id>/session_<n>/anat_<n>/mprage.nii.gz
        RawData/<Site>[_<sample>]/<id>/session_<n>/rest_<n>/rest.nii.gz
"""
from pathlib import Path, PurePosixPath
from typing import Iterable

from humanize import naturalsize

from brainspresso.download import CHUNK_SIZE, DownloadManager, IfExists
from brainspresso.sources import fcp
from brainspresso.utils.keys import compat_keys
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.ui import human2bytes
from brainspresso.datasets.ABIDE.I.keys import allkeys

PROJECT = "ABIDE"

# file name in RawData -> key
RAW_FILES = {"mprage.nii.gz": "T1w", "rest.nii.gz": "rest"}


def _value(x):
    """Enum member -> value (str() of a str-Enum is 'Enum.member')"""
    return str(getattr(x, "value", x))


def _aslist(x) -> list:
    """A single value (including a str or str-Enum) -> [value]"""
    if x is None:
        return []
    if isinstance(x, (str, int)):
        return [x]
    return list(x)


def _site(name: str) -> str:
    """'MaxMun_a' -> 'MaxMun', 'UCLA_1' -> 'UCLA'"""
    return name.split("_")[0]


def _pheno_site(name: str) -> str:
    """'phenotypic_MAX_MUN.csv' -> 'MAXMUN'; 'phenotypic_UCLA_1.csv' -> 'UCLA'
    (site names upper-cased, sample suffix dropped)"""
    stem = name[len("phenotypic_"):].rsplit(".", 1)[0]
    stem = stem.upper().replace("MAX_MUN", "MAXMUN")
    return stem.split("_")[0]


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
    log: str | None = None,
    **kwargs,
) -> None:
    """
    Download ABIDE-I raw data and phenotypes from s3://fcp-indi.

    No login is needed. Only the "raw" (T1w, rest) and "meta" keys are
    downloaded; derivatives are not supported by this source yet.
    """
    setup_filelog(log)
    keys = {_value(k) for k in _aslist(keys or ("all",))}
    sites = {_value(s).upper() for s in _aslist(sites)}
    subs = {int(_value(s)) for s in _aslist(subs)}
    exclude_subs = {int(_value(s)) for s in _aslist(exclude_subs)}
    src = Path(get_tree_path(path)) / "ABIDE-1" / "sourcedata"
    opt = dict(ifexists=if_exists, chunk_size=human2bytes(packet))

    def want(key: str) -> bool:
        return bool(keys & compat_keys(key, allkeys))

    def keep_raw(rel: PurePosixPath) -> bool:
        # <Site>/<id>/session_<n>/<mod>_<n>/<file>
        if len(rel.parts) != 5:
            return False
        site, sub, *_, fname = rel.parts
        if fname not in RAW_FILES or not want(RAW_FILES[fname]):
            return False
        if sites and _site(site).upper() not in sites:
            return False
        try:
            sub = int(sub)
        except ValueError:
            return False
        if subs and sub not in subs:
            return False
        return sub not in exclude_subs

    def keep_pheno(rel: PurePosixPath) -> bool:
        if not (rel.name.startswith("phenotypic_") and rel.suffix == ".csv"):
            return False
        return not sites or _pheno_site(rel.name) in sites

    def downloaders():
        if want("meta"):
            yield from fcp.mirror(
                f"{PROJECT}/PhenotypicData", src, keep_pheno, **opt
            )
        if want("raw"):
            yield from fcp.mirror(
                f"{PROJECT}/RawData", src / "RawData", keep_raw, **opt
            )

    DownloadManager(
        downloaders(), ifexists=if_exists, jobs=jobs, path="short",
    ).run()
