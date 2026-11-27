"""
ABIDE-II: convert the harvested files to BIDS.

One roaster, `roast_nitrc`, of the sources `nitrc` and `s3`: both put
the NITRC tarballs in `sourcedata/` (`s3` those of the sites that the
bucket lacks, SU and U_MIA). The bucket copy (`sourcedata/RawData/`,
`--source s3`) is BIDS-like (runs, acquisitions, sessions of its own)
and is not roasted: that needs a hosted-BIDS roaster (architecture §9).

Expected input
--------------
ABIDE-2/sourcedata/
    ABIDEII-<SITE>_<sample>.csv                 (phenotypes, every column)
    ABIDEII-<SITE>_<sample>[_<part>].tar.gz
        ABIDEII-<SITE>_<sample>/<id>/session_<s>/
            anat_<n>/anat.nii.gz
            rest_<n>/rest.nii.gz
            dti_<n>/dti.{nii.gz,bval,bvec}

Output (study layout)
---------------------
ABIDE-2/
    dataset_description.json        (DatasetType "study")
    rawbids/
        README, dataset_description.json, participants.{tsv,json}
        phenotype/<name>.{tsv,json}             (adir, ados_2, srs...)
        sub-<id>/ses-<ss>/
            anat/sub-<id>_ses-<ss>[_run-<n>]_T1w.{nii.gz,json}
            func/sub-<id>_ses-<ss>_task-rest[_run-<n>]_bold.{nii.gz,json}
            dwi/sub-<id>_ses-<ss>[_run-<n>]_dwi.{nii.gz,json,bval,bvec}

`<id>` has 5 digits, `<ss>` 2 (`session_1` is `ses-01`); the first scan
of a kind (`anat_1`) has no run entity, the others are `run-<n>`. With
`--layout flat`, the content of `rawbids/` is in `ABIDE-2/` itself. The
participants and phenotype tables are written complete (every subject
of the CSV files) to `.brainspresso/orig/` and trimmed to the subjects
on disk.

Each tarball is one shard, decompressed once: its members are read
lazily, in archive order, and each selected one is extracted when it is
reached. The sidecar templates that a shard may use (paths, hashes) are
its `Shard.config`. A corrupt or truncated tarball fails its shard only.
"""
from __future__ import annotations

import re
from collections import Counter
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterator, NamedTuple

from brainspresso.actions import Action, Fail
from brainspresso.actions.archive import ExtractMember
from brainspresso.bids.keys import KeyAliases
from brainspresso.datasets.base import KeySpec
from brainspresso.datasets.indi.abide import scans
from brainspresso.datasets.indi.abide.ii.keys import allkeys
from brainspresso.datasets.indi.abide.ii.sites import (
    TEMPLATES,
    parse_site,
    site_template,
)
from brainspresso.datasets.indi.abide.ii.tables import (
    COMPOSITE,
    CSV_GLOB,
    PARTICIPANTS,
    PHENOTYPES,
)
from brainspresso.datasets.indi.abide.scans import (
    ARCHIVE_ERRORS,
    TemplateError,
    image_ext,
    member_parts,
)
from brainspresso.datasets.indi.abide.tables import WIDTH, participant_label
from brainspresso.roast import (
    RoastContext,
    RoastReport,
    Shard,
    as_list,
    roast_dataset,
)
from brainspresso.sources.base import subject_labels
from brainspresso.utils.archive import TarArchive

lg = getLogger(__name__)

__all__ = ["KEYS", "ROAST_SPEC", "roast", "roast_nitrc"]

RAWDATA = "RawData"

# Keys: those of the harvest (`keys.py`), where "meta" is also the
# category of the dataset files and participants table (`--keys pheno`:
# phenotypes only).
KEYS = KeyAliases(
    {"all": {**allkeys["all"], "meta": {"meta", "pheno"}}},
    bids={
        "T1w": {"datatypes": ["anat"], "suffixes": ["T1w"]},
        "rest": {"datatypes": ["func"], "suffixes": ["bold"]},
        "dti": {"datatypes": ["dwi"], "suffixes": ["dwi"]},
    },
    folders={"pheno": ["phenotype"]},
)

#: Keys of the roast (`--keys`)
ROAST_SPEC = KeySpec(KEYS)

#: Sidecar templates of a site and sample (`templates/<SITE>/<sample>/`)
TEMPLATE_NAMES = ("T1w.json", "bold.json", "dwi.json")

#: Sites whose `anat_<n>` (n > 1) may be a high-resolution scan of
#: another kind (the bucket's `acq-hires`), which has no template: not
#: written as runs of the T1w
HIRES_SITES = ("UCLA",)

#: Gradient tables of a DWI folder: member suffix -> BIDS extension
GRADIENTS = {".bval": ".bval", ".bvals": ".bval",
             ".bvec": ".bvec", ".bvecs": ".bvec"}


class Kind(NamedTuple):
    """
    A kind of scan of the tarballs (`<folder>_<n>/`).

    Parameters
    ----------
    key : str
        Its key (`--keys`)
    datatype : str
        BIDS datatype
    entities : str
        Entities before the run (`task-rest_`)
    suffix : str
        BIDS suffix
    template : str
        Name of its sidecar template
    """

    key: str
    datatype: str
    entities: str
    suffix: str
    template: str

    def name(self, sub: str, ses: str, run: int) -> str:
        """`sub-<id>_ses-<ss>_<entities>[run-<n>_]<suffix>` (run > 1)."""
        run_ = f"run-{run}_" if run > 1 else ""
        return f"sub-{sub}_ses-{ses}_{self.entities}{run_}{self.suffix}"


#: Scan folders (`anat_1`, `rest_2`, `dti_1`) -> kind
KINDS = {
    "anat": Kind("T1w", "anat", "", "T1w", "T1w.json"),
    "rest": Kind("rest", "func", "task-rest_", "bold", "bold.json"),
    "dti": Kind("dti", "dwi", "", "dwi", "dwi.json"),
    "dwi": Kind("dti", "dwi", "", "dwi", "dwi.json"),
}

SCAN_FOLDER = re.compile(r"(?P<kind>[A-Za-z]+)_(?P<n>\d+)")
SESSION_FOLDER = re.compile(r"session_(?P<n>\d+)")


# ----------------------------------------------------------------------
#   Python API
# ----------------------------------------------------------------------

def roast(root, *, keys=(), exclude_keys=(), subs=(), exclude_subs=(),
          **options) -> RoastReport:
    """
    Roast `<root>` (the `ABIDE-2` folder).

    Parameters
    ----------
    root : path
        Dataset folder
    subs, exclude_subs : list[int | str]
        Subjects: numbers (`29001` is `sub-29001`), labels with or
        without `sub-`, ranges, files (as `--subs`)
    keys, exclude_keys : str | list[str]
        `--keys`, `--exclude-keys` (a name or a list)
    **options
        Those of the command line (`json`, `ifexists`, `layout`), `jobs`
        and `display`

    Returns
    -------
    RoastReport
    """
    select, exclude = ROAST_SPEC.effective(as_list(keys),
                                           as_list(exclude_keys))
    return roast_dataset(
        roast_nitrc, root, KEYS, templates=TEMPLATES,
        keys=select, exclude_keys=exclude,
        subs=sorted(subject_labels(as_list(subs), WIDTH)),
        exclude_subs=sorted(subject_labels(as_list(exclude_subs), WIDTH)),
        **options,
    )


# ----------------------------------------------------------------------
#   Roaster (sources `nitrc`, `s3`)
# ----------------------------------------------------------------------

def phenotype_files(source: Path) -> list[Path]:
    """
    Phenotypic CSV files of a source folder.

    Parameters
    ----------
    source : Path
        Source folder

    Returns
    -------
    list[Path]
        The site files (`ABIDEII-<SITE>_<sample>.csv`), else the
        composite file, else nothing
    """
    if not source.is_dir():
        return []
    csvs = sorted(p for p in source.glob(CSV_GLOB) if p.is_file())
    if not csvs and (source / COMPOSITE).is_file():
        csvs = [source / COMPOSITE]
    return csvs


def tarballs(source: Path) -> list[Path]:
    """NITRC tarballs (`*.tar.gz`, `*.tgz`) of a source folder."""
    if not source.is_dir():
        return []
    return sorted(p for p in source.iterdir()
                  if p.is_file() and p.name.endswith((".tar.gz", ".tgz")))


def wants_raw(ctx: RoastContext) -> bool:
    """Whether any raw scan is wanted."""
    return any(ctx.wants(kind.key) for kind in KINDS.values())


def tarball_site(path: Path) -> tuple[str, str | None] | None:
    """(SITE, sample) of a tarball name (None if it names no site)."""
    stem = path.name.split(".")[0]
    try:
        return parse_site(stem)
    except TemplateError:
        return None


def template_config(ctx: RoastContext, path: Path) -> dict:
    """
    Describe the sidecar templates of a tarball (in its shard's signature).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    path : Path
        Tarball

    Returns
    -------
    dict
        `{"site": "<SITE>/<sample>", "templates": {name: [path, sha256]
        | None}}`; `{"site": None}` if the name gives no site (the
        members do)
    """
    site = tarball_site(path)
    if site is None:
        return {"site": None}
    return scans.template_config(
        lambda name: site_template(*site, name, ctx.resource),
        TEMPLATE_NAMES, f"{site[0]}/{site[1] or '1'}")


def roast_nitrc(ctx: RoastContext) -> Iterator:
    """
    Roast the NITRC tarballs (sources nitrc, s3).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action | Shard
        Metadata actions, then one shard per tarball
    """
    yield from scans.roast_meta(
        ctx, phenotype_files(ctx.source), "ABIDEII-<SITE>_<sample>.csv",
        PARTICIPANTS, PHENOTYPES, pristine_phenotypes=True)
    if not wants_raw(ctx):
        return
    bucket = (ctx.source / RAWDATA).is_dir()
    if bucket:
        lg.warning(f"{RAWDATA}/ (the bucket copy, harvested with --source "
                   f"s3) is not roasted: only the NITRC tarballs are")
    archives = tarballs(ctx.source)
    if not archives:
        note = f" ({RAWDATA}/ is not roasted)" if bucket else ""
        yield Fail(f"no NITRC tarball (ABIDEII-<SITE>_<sample>.tar.gz) "
                   f"found{note}", name="raw", tag="raw")
        return
    ctx.progress(total=len(archives), tag="raw")
    for path in archives:
        yield Shard(roast_archive, path, tag="raw", advance="raw",
                    id=f"roast/raw/{path.name}", inputs=[path],
                    config=template_config(ctx, path))


class Member(NamedTuple):
    """
    A member of a tarball that may be converted.

    Parameters
    ----------
    site : str
        Site (`SITES`)
    sample : str | None
        Sample (`"1"`, `"Long"`)
    sub : str
        Subject label (5 digits)
    ses : str
        Session label (2 digits)
    folder : str
        Kind of scan folder (`anat`, `rest`, `dti`, or another)
    run : int
        Number of the scan folder (`anat_2`: 2)
    file : str
        File name
    """

    site: str
    sample: str | None
    sub: str
    ses: str
    folder: str
    run: int
    file: str


def parse_member(name: str, default: tuple | None = None) -> Member | None:
    """
    Parse a member of a NITRC tarball.

    Parameters
    ----------
    name : str
        `[...]/ABIDEII-<SITE>_<sample>/<id>/session_<s>/<kind>_<n>/<file>`
        (leading folders, `./` and `/` are ignored)
    default : (site, sample), optional
        Site of the tarball, if the folder of the member names none

    Returns
    -------
    Member | None
        None for files that are not in a scan folder of a subject
        (subject ids that are not numbers are logged)
    """
    parts = member_parts(name)
    if parts is None:
        lg.warning(f"{name}: a member outside the archive: skipped")
        return None
    if len(parts) < 5:
        return None
    root, sub, ses, folder, file = parts[-5:]
    ses_match = SESSION_FOLDER.fullmatch(ses)
    scan_match = SCAN_FOLDER.fullmatch(folder)
    if not (ses_match and scan_match):
        return None
    try:
        site, sample = parse_site(root)
    except TemplateError:
        if default is None:
            lg.warning(f"{name}: no ABIDE-II site in {root!r}: skipped")
            return None
        site, sample = default
    if not sub.isdigit():
        lg.warning(f"{name}: subject {sub!r} is not a number: skipped")
        return None
    return Member(site, sample, participant_label(sub),
                  f"{int(ses_match['n']):02d}",
                  scan_match["kind"].lower(), int(scan_match["n"]), file)


def roast_archive(ctx: RoastContext, path: Path) -> Iterator[Action]:
    """
    Convert the scans of one tarball (a shard).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    path : Path
        Tarball

    Yields
    ------
    Action
        Per selected scan: its image, then its sidecar (and gradient
        tables); a `Fail` if the tarball cannot be read

    Notes
    -----
    Members are read lazily and each selected one is extracted when it
    is reached (the engine runs an action before asking for the next):
    a tarball is decompressed once.
    """
    default = tarball_site(path)
    seen = set()
    unknown = Counter()
    try:
        with TarArchive(path) as archive:
            for member in archive:
                if not member.isfile():
                    continue
                parsed = parse_member(member.name, default)
                if parsed is None:
                    continue
                kind = KINDS.get(parsed.folder)
                if kind is None:
                    unknown[f"{parsed.folder}_<n>"] += 1
                    continue
                if not (ctx.selection.keeps_subject(parsed.sub)
                        and ctx.wants(kind.key)):
                    continue
                yield from member_actions(ctx, path, archive, member,
                                          parsed, kind, seen, unknown)
    except ARCHIVE_ERRORS as e:
        yield Fail(f"{path.name}: cannot be read ({type(e).__name__}: "
                   f"{e})", name=path.name, tag="raw")
    finally:
        if unknown:
            files = ", ".join(f"{k} ({n} files)"
                              for k, n in sorted(unknown.items()))
            lg.warning(f"{path.name}: no BIDS name for {files}: skipped")


def member_actions(ctx: RoastContext, path: Path, archive, member,
                   parsed: Member, kind: Kind, seen: set,
                   unknown: Counter) -> Iterator[Action]:
    """
    Convert one member: an image (and its sidecar) or a gradient table.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    path : Path
        Tarball
    archive : TarArchive
        The open tarball
    member : TarInfo
        The member
    parsed : Member
        What its name says
    kind : Kind
        Its kind of scan
    seen : set
        Outputs already written by this shard (updated)
    unknown : Counter
        Files without a BIDS name (updated)

    Yields
    ------
    Action
    """
    name = PurePosixPath(member.name).name
    if kind.key == "T1w" and parsed.run > 1 and parsed.site in HIRES_SITES:
        lg.warning(f"{path.name}: {member.name}: {parsed.site} "
                   f"anat_{parsed.run} may be the high-resolution scan "
                   f"(no template): skipped")
        return
    stem = (ctx.layout.raw / f"sub-{parsed.sub}" / f"ses-{parsed.ses}"
            / kind.datatype / kind.name(parsed.sub, parsed.ses, parsed.run))
    ext = image_ext(name)
    if ext is None and kind.datatype == "dwi":
        ext = next((v for k, v in GRADIENTS.items() if name.endswith(k)),
                   None)
        if ext is None:
            unknown[f"{parsed.folder}_<n>/{name}"] += 1
            return
        if (stem, ext) in seen:
            lg.warning(f"{path.name}: {member.name}: a second "
                       f"{stem.name}{ext}: skipped")
            return
        seen.add((stem, ext))
        yield ExtractMember(archive, member, f"{stem}{ext}", tag=kind.key)
        return
    if ext is None:
        unknown[f"{parsed.folder}_<n>/{name}"] += 1
        return
    if (stem, "image") in seen:
        lg.warning(f"{path.name}: {member.name}: a second image of "
                   f"{stem.name}: skipped")
        return
    seen.add((stem, "image"))
    yield from scans.scan_actions(
        ctx, stem, ext, kind.key,
        partial(site_template, parsed.site, parsed.sample, kind.template,
                ctx.resource),
        lambda dst: ExtractMember(archive, member, dst, tag=kind.key),
        partial(archive.extractfile, member), name,
        bold=kind.suffix == "bold")
