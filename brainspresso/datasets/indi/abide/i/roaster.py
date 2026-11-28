"""
ABIDE-I: convert the harvested files to BIDS.

Two roasters, one per kind of source data:

* `roast_nitrc` (sources `s3` and `nitrc`): NITRC tarballs and the
  bucket copy, which share one layout once unpacked.
* `roast_xnat` (source `xnat`): the NITRC-IR archives, one per scan
  (and per assessor: FreeSurfer outputs).

Expected input
--------------
ABIDE-1/sourcedata/
    nitrc, s3:
        phenotypic_<SITE>[_<sample>].csv
        <Site>[_<sample>][_<part>].tgz      (<Site>/<id>/session_1/
                                             {anat,rest}_<n>/<file>.nii.gz)
        RawData/<Site>[_<sample>]/<id>/session_1/{anat,rest}_<n>/...
    xnat:
        Phenotypic_V1_0b.csv
        <site>[_<sample>]_<id>/anat.tar.gz, rest.tar.gz
        <site>[_<sample>]_<id>/<FreeSurfer assessor>.tar.gz
        <site>/{anat,rest}.pdf              (scan parameters, not read)

Output (study layout)
---------------------
ABIDE-1/
    dataset_description.json        (DatasetType "study")
    rawbids/
        README, dataset_description.json, participants.{tsv,json}
        phenotype/{adir,ados,ados_gotham,srs,vineland,wisc_iv}.{tsv,json}
        sub-{05d}/anat/sub-{05d}_T1w.{nii.gz,json}
        sub-{05d}/anat/sub-{05d}_acq-hires_T2w.{nii.gz,json}   (UCLA)
        sub-{05d}/func/sub-{05d}_task-rest_bold.{nii.gz,json}
    derivatives/abide-freesurfer/   (xnat, keys `fs`, `fs-all`)
        atlas-*_dseg.tsv
        sourcedata/sub-{05d}/{mri,surf,label,...}/   (unpacked)
        sub-{05d}/anat/...          (converted, `freesurfer.bidsify`)

With `--layout flat`, the content of `rawbids/` is in `ABIDE-1/` itself.
The participants table is written complete (every subject of the CSV
files) to `.brainspresso/orig/` and trimmed to the subjects on disk.

Each gzip archive is decompressed once per run: a NITRC tarball (or a
site folder of the bucket copy) is one shard, which reads its members
lazily, in archive order, and extracts each selected one when it is
reached; an XNAT subject (one small archive per scan) is one shard. The
sidecar templates that a shard may use (paths, hashes) are its
`Shard.config`. A corrupt archive fails its shard only.
"""  # noqa: E501
from __future__ import annotations

import re
from contextlib import nullcontext
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator

from brainspresso.actions import Action, CopyBytes, Fail, WriteJSON
from brainspresso.actions.action import Claim
from brainspresso.actions.archive import ExtractMember, ExtractMembers
from brainspresso.bids.keys import KeyAliases
from brainspresso.convert.nifti import image_ext
from brainspresso.datasets.base import KeySpec
from brainspresso.datasets.indi.abide import scans
from brainspresso.datasets.indi.abide.i.keys import allkeys
from brainspresso.datasets.indi.abide.i.sites import (
    TEMPLATES,
    TemplateError,
    parse_site,
    site_template,
)
from brainspresso.datasets.indi.abide.i.tables import (
    PARTICIPANTS,
    PHENOTYPES,
    WIDTH,
    participant_label,
)
from brainspresso.freesurfer import bidsify as fs
from brainspresso.roast import (
    RoastContext,
    RoastReport,
    Shard,
    archives,
    as_list,
    roast_dataset,
    scope,
    sidecars,
)
from brainspresso.roast.helpers import archive_shards
from brainspresso.sources.base import subject_labels
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.path import sorted_paths

lg = getLogger(__name__)

__all__ = ["KEYS", "ROASTERS", "roast", "roast_nitrc", "roast_xnat"]

FS_NAME = "abide-freesurfer"
FS_VERSION = (5, 1)             # FreeSurfer of the ABIDE-I outputs
PHENOTYPES_XNAT = "Phenotypic_V1_0b.csv"
RAWDATA = "RawData"

# Keys: those of the harvest (`keys.py`), where "meta" is also the
# category of the dataset files and participants table (`--keys pheno`:
# phenotypes only). `fs` and `fs-all` are siblings: `fs` converts the
# files that BIDS describes; `fs-all`, an add-on of `fs` (`FS_ADD_ON`),
# also unpacks every other file; it is not selected by default (only
# when it or a group above it is named, `ROAST_SPEC`).
KEYS = KeyAliases(
    {"all": {**allkeys["all"], "meta": {"meta", "pheno"}}},
    bids={
        "T1w": {"datatypes": ["anat"], "suffixes": ["T1w"]},
        "T2w": {"datatypes": ["anat"], "suffixes": ["T2w"]},
        "rest": {"datatypes": ["func"], "suffixes": ["bold"]},
    },
    folders={
        "pheno": ["phenotype"],
        "fs": [f"derivatives/{FS_NAME}"],
        "fs-all": [f"derivatives/{FS_NAME}"],
    },
)

#: `fs-all` implies `fs`; excluding `fs` excludes `fs-all`
FS_ADD_ON = (("fs-all", "fs"),)

#: Keys of the roast (`--keys`): `fs-all` not selected by default
ROAST_SPEC = KeySpec(KEYS, off=("fs-all",), implies=FS_ADD_ON)

#: Sidecar templates of a site (`templates/<Site>[_<sample>]/`)
TEMPLATE_NAMES = ("T1w.json", "acq-hires_T2w.json", "bold.json")

#: Keys of the raw scans, and of the FreeSurfer outputs (`roast.scope`)
RAW_KEYS = ("T1w", "T2w", "rest")
FS_KEYS = ("fs", "fs-all")

#: Sites with a high-resolution anatomical (`anat_2`): a 2D spin-echo EPI
#: ("Matched Bandwidth HiRes"), so `acq-hires_T2w` (owner decision)
HIRES_SITES = ("UCLA",)


# ----------------------------------------------------------------------
#   Python API
# ----------------------------------------------------------------------

def roast(root, *, source: str = "s3", keys=(), exclude_keys=(), subs=(),
          exclude_subs=(), **options) -> RoastReport:
    """
    Roast `<root>` (the `ABIDE-1` folder).

    Parameters
    ----------
    root : path
        Dataset folder
    source : {"s3", "nitrc", "xnat"}
        Source whose files are roasted (as `--source`)
    subs, exclude_subs : list[int | str]
        Subjects: numbers (`51456` is `sub-51456`), labels with or
        without `sub-`, ranges, files (as `--subs`)
    keys, exclude_keys : list[str]
        `--keys`, `--exclude-keys` (a name or a list; `fs-all` is not
        selected by default, see `ROAST_SPEC`)
    **options
        Those of the command line (`json`, `ifexists`, `layout`), `jobs`
        and `display`

    Returns
    -------
    RoastReport
    """
    if source not in ROASTERS:
        raise ValueError(f"Unknown source {source!r}: "
                         f"{', '.join(ROASTERS)}")
    keys, exclude_keys = as_list(keys), as_list(exclude_keys)
    select, exclude = ROAST_SPEC.effective(keys, exclude_keys)
    return roast_dataset(
        ROASTERS[source], root, KEYS, templates=TEMPLATES,
        keys=select, exclude_keys=exclude,
        subs=sorted(subject_labels(subs, WIDTH)),
        exclude_subs=sorted(subject_labels(exclude_subs, WIDTH)),
        options=roast_options(keys), **options,
    )


def roast_options(keys) -> dict:
    """
    Return the options of the roasters (`RoastContext.options`).

    Parameters
    ----------
    keys : str | list[str]
        `--keys`, as given

    Returns
    -------
    dict
        `fs_named`: whether `--keys` names `fs`, `fs-all` or a group
        above them (`all`, `derivatives`, `proc`; any case): then a
        missing FreeSurfer archive is an error
    """
    keys = as_list(keys)
    return {"fs_named": any(ROAST_SPEC.selects(keys, k)
                            for k in ("fs", "fs-all"))}


# ----------------------------------------------------------------------
#   Shared: metadata, scans, sidecars
# ----------------------------------------------------------------------

def roast_meta(ctx: RoastContext, csvs: list[Path],
               missing: str) -> Iterator[Action]:
    """
    Write the dataset files, the participants and phenotype tables.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    csvs : list[Path]
        Phenotypic CSV files (empty: they are missing)
    missing : str
        What is missing if `csvs` is empty (message of the `Fail`)

    Yields
    ------
    Action
        See `datasets.indi.abide.scans.roast_meta` (phenotype tables
        written directly)
    """
    yield from scans.roast_meta(ctx, csvs, missing, PARTICIPANTS,
                                PHENOTYPES)


def scan_name(site: str, mod: str, number: int
              ) -> tuple[str, str, str, str] | None:
    """
    BIDS name of a scan.

    Parameters
    ----------
    site : str
        Site name, in the spelling of the data
    mod : {"anat", "rest"}
        Kind of scan
    number : int
        Its number (`anat_2`: 2)

    Returns
    -------
    (key, datatype, name, template) | None
        E.g. `("T2w", "anat", "acq-hires_T2w", "acq-hires_T2w.json")`;
        None for a scan without a BIDS name (other numbers)
    """
    if mod == "anat" and number == 1:
        return "T1w", "anat", "T1w", "T1w.json"
    if mod == "anat" and number == 2:
        try:
            hires = parse_site(site)[0] in HIRES_SITES
        except TemplateError:
            hires = False
        if hires:
            return "T2w", "anat", "acq-hires_T2w", "acq-hires_T2w.json"
    if mod == "rest" and number == 1:
        return "rest", "func", "task-rest_bold", "bold.json"
    return None


def template_config(ctx: RoastContext, site: str) -> dict:
    """
    Describe the sidecar templates of a site (in a shard's signature).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    site : str
        Site name, in the spelling of the data

    Returns
    -------
    dict
        `{"site": site, "templates": {name: [path, sha256] | None}}`: a
        changed template re-runs the shard
    """
    return {"site": site, "templates": sidecars.template_config(
        lambda name: site_template(site, name, ctx.resource),
        TEMPLATE_NAMES)}


def scan_actions(
    ctx: RoastContext, sub: str, site: str, scan: tuple, ext: str,
    extract: Callable[[Path], Action], open_image: Callable,
    image_name: str,
) -> Iterator[Action]:
    """
    Convert one scan: the image, then its sidecar.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label
    site : str
        Site name, in the spelling of the data
    scan : (key, datatype, name, template)
        See `scan_name`
    ext : str
        Extension of the image (`.nii.gz`, `.nii`)
    extract : callable(dst) -> Action
        Action that writes the image to `dst`
    open_image : callable() -> file
        Opens the source image (bold templates without RepetitionTime)
    image_name : str
        Name of the source image

    Yields
    ------
    Action
        The image, then its sidecar (or a `Fail` naming the missing
        template, when sidecars are written)
    """
    key, datatype, name, template = scan
    stem = ctx.layout.raw / f"sub-{sub}" / datatype / f"sub-{sub}_{name}"
    yield from scans.scan_actions(
        ctx, stem, ext, key,
        lambda: site_template(site, template, ctx.resource),
        extract, open_image, image_name, bold=template == "bold.json")


def wants_raw(ctx: RoastContext) -> bool:
    """Whether any raw scan is wanted."""
    return any(map(ctx.wants, RAW_KEYS))


# ----------------------------------------------------------------------
#   NITRC tarballs, bucket copy (sources `nitrc`, `s3`)
# ----------------------------------------------------------------------

def roast_nitrc(ctx: RoastContext) -> Iterator:
    """
    Roast the NITRC tarballs or the bucket copy (sources nitrc, s3).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action | Shard
        Metadata actions, then one shard per tarball (or site folder of
        `RawData/`)
    """
    csvs = sorted_paths(p for p in ctx.source.iterdir()
                        if p.is_file() and p.name.startswith("phenotypic_")
                        and p.name.endswith(".csv")) \
        if ctx.source.is_dir() else []
    yield from roast_meta(ctx, csvs, "phenotypic_<SITE>.csv")
    if not wants_raw(ctx):
        return
    found = {path: (name, inputs)
             for name, path, inputs in nitrc_archives(ctx)}
    if not found:
        yield Fail("no NITRC tarball (<Site>.tgz) nor RawData/<Site>/ "
                   "folder found", name="raw", tag="raw")
        return
    yield from archive_shards(
        ctx, found, roast_archive, tag="raw", keys=RAW_KEYS,
        id=lambda path: f"roast/raw/{found[path][0]}",
        inputs=lambda path: found[path][1],
        config=lambda path: template_config(ctx, path.name.split(".")[0]))


def nitrc_archives(ctx: RoastContext) -> list[tuple[str, Path, list]]:
    """
    Source archives of the nitrc roaster.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Returns
    -------
    list[(name, path, inputs)]
        Tarballs (`<Site>.tgz`), then site folders of `RawData/`, with
        the files each one reads
    """
    if not ctx.source.is_dir():
        return []
    out = [(p.name, p, [p]) for p in sorted_paths(ctx.source.iterdir())
           if p.is_file() and p.name.endswith((".tgz", ".tar.gz"))]
    rawdata = ctx.source / RAWDATA
    if rawdata.is_dir():
        for folder in sorted_paths(rawdata.iterdir()):
            if folder.is_dir():
                files = sorted_paths(p for p in folder.rglob("*")
                                     if p.is_file())
                out.append((f"{RAWDATA}/{folder.name}", folder, files))
    return out


def parse_member(name: str) -> tuple[str, str, str, int, str] | None:
    """
    Parse a member of a NITRC tarball (or a file of `RawData/`).

    Parameters
    ----------
    name : str
        `[...]/<Site>/<id>/session_<n>/<mod>_<n>/<file>.nii.gz`

    Returns
    -------
    (site, sub, mod, number, ext) | None
        E.g. `("UCLA_1", "51201", "anat", 2, ".nii.gz")`; None for
        files that are not images of a scan (logged when the path looks
        like a scan of another session)
    """
    parts = PurePosixPath(name).parts
    ext = image_ext(name)
    if len(parts) < 5 or ext is None:
        return None
    site, sub, ses, scan, _ = parts[-5:]
    match = re.fullmatch(r"(anat|rest)_(\d+)", scan)
    if not match or not re.fullmatch(r"session_\d+", ses):
        return None
    if not sub.isdigit():
        lg.warning(f"{name}: subject {sub!r} is not a number: skipped")
        return None
    if ses != "session_1":
        lg.warning(f"{name}: not the first session: skipped")
        return None
    return site, participant_label(sub), match[1], int(match[2]), ext


def open_source(path: Path):
    """Open a tarball (`roast.archives.reading`), or nothing for a folder."""
    if path.is_file():
        return archives.reading(path, opener=TarArchive)
    return nullcontext()


def iter_members(path: Path, tar: archives.Reader | None) -> Iterator[tuple]:
    """
    Members of a tarball (lazily, in archive order) or files of a folder.

    Parameters
    ----------
    path : Path
        Tarball, or site folder of `RawData/`
    tar : roast.archives.Reader | None
        The open tarball (None: a folder)

    Yields
    ------
    (name, extract, open_image)
        `name` relative to the archive (normalised); `extract(dst, tag)`
        the action that writes it, `open_image()` opens it

    Raises
    ------
    roast.archives.ArchiveError
        If the tarball cannot be read
    """
    if tar is None:
        for file in sorted_paths(p for p in path.rglob("*") if p.is_file()):
            name = file.relative_to(path.parent).as_posix()
            yield (name, partial(CopyBytes, file),
                   partial(open, file, "rb"))
        return
    for member in tar:
        yield (member.path, partial(ExtractMember, tar.archive, member.info),
               member.open)


def roast_archive(ctx: RoastContext, path: Path) -> Iterator[Action]:
    """
    Convert the scans of one tarball or `RawData/` site folder (a shard).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    path : Path
        Tarball, or site folder

    Yields
    ------
    Action
        Per selected scan: its image, then its sidecar; a `Fail` if the
        archive cannot be read

    Notes
    -----
    Members are read lazily and each selected one is extracted when it
    is reached (the engine runs an action before asking for the next):
    a tarball is decompressed once.
    """
    seen = set()
    try:
        with open_source(path) as tar:
            for name, extract, open_image in iter_members(path, tar):
                parsed = parse_member(name)
                if parsed is None:
                    continue
                site, sub, mod, number, ext = parsed
                # (every subject is reported, before anything prunes it)
                kept = ctx.keeps_subject(sub)
                scan = scan_name(site, mod, number)
                if scan is None:
                    lg.warning(f"{path.name}: {name}: no BIDS name for "
                               f"{mod}_{number}: skipped")
                    continue
                key = scan[0]
                if not (kept and ctx.wants(key)):
                    continue
                if (sub, scan[2]) in seen:
                    lg.warning(f"{path.name}: {name}: a second image of "
                               f"sub-{sub}_{scan[2]}: skipped")
                    continue
                seen.add((sub, scan[2]))
                yield from scan_actions(
                    ctx, sub, site, scan, ext,
                    lambda dst, extract=extract, key=key:
                        extract(dst, tag=key),
                    open_image, PurePosixPath(name).name)
    except archives.ArchiveError as e:
        yield e.fail(tag="raw")


# ----------------------------------------------------------------------
#   NITRC-IR (source `xnat`)
# ----------------------------------------------------------------------

#: XNAT subject folder: `<site>[_<sample>]_<id>` (id of 5 or 7 digits;
#: the site must be one of ABIDE-I, `parse_site`)
SUBJECT_FOLDER = re.compile(r"(?P<site>.+?)[_-](?P<id>\d{5}|\d{7})")

#: XNAT scan id (archive name): `anat`, `mprage`, `anat_2`, `rest`...
SCAN_ID = re.compile(r"(?P<mod>anat|mprage|hires|anat_hires|rest)"
                     r"(?:[_-]?(?P<n>\d+))?", re.IGNORECASE)

#: Top-level folders of a FreeSurfer subject (they mark its root)
FS_DIRS = frozenset({"mri", "surf", "label", "stats", "scripts", "touch"})

#: FreeSurfer template subjects (never the subject's tree)
FS_TEMPLATES = re.compile(r"fsaverage\d*|[lr]h\.EC_average|bert",
                          re.IGNORECASE)

#: Name of a FreeSurfer archive or assessor (`...freesurfer...`,
#: `..._fs`, `...-fs`)
FS_NAME_PATTERN = re.compile(r"freesurfer|(^|[-_.])fs($|[-_.])",
                             re.IGNORECASE)


def is_freesurfer(name: str | Path) -> bool:
    """
    Whether an archive or XNAT assessor holds FreeSurfer outputs.

    The harvest (`sources.assessor_keys`) and the roast share it: the
    harvest saves a FreeSurfer assessor whose label it does not match
    under a name that it matches (`sources.XNATImages.local_path`).

    Parameters
    ----------
    name : str | Path
        Archive (`<label>.tar.gz`) or assessor label

    Returns
    -------
    bool
    """
    name = Path(name).name
    if name.endswith(".tar.gz"):
        name = name[:-len(".tar.gz")]
    return bool(FS_NAME_PATTERN.search(name))


def roast_xnat(ctx: RoastContext) -> Iterator:
    """
    Roast the NITRC-IR archives (source xnat).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action | Shard
        Metadata actions, then one shard per subject (its scans) and one
        per FreeSurfer archive, and the FreeSurfer lookup tables
    """
    csv = ctx.source / PHENOTYPES_XNAT
    yield from roast_meta(ctx, [csv] if csv.is_file() else [],
                          PHENOTYPES_XNAT)
    raw, freesurfer = wants_raw(ctx), wants_fs(ctx)
    if not (raw or freesurfer):
        return
    subjects = {label: folders for label, folders in xnat_subjects(ctx)
                .items() if ctx.selection.keeps_subject(label)}
    if not subjects:
        yield Fail("no NITRC-IR subject folder (<site>_<id>/*.tar.gz) "
                   "found for the selected subjects", name="raw", tag="raw")
        return
    if raw:
        ctx.progress(total=len(subjects), tag="subjects")
        for label, folders in subjects.items():
            yield Shard(
                roast_xnat_subject, label, folders, tag="subjects",
                advance="subjects", id=f"roast/subjects/sub-{label}",
                unit={"sub": label},
                inputs=[a for f in folders for a in scan_archives(f)],
                config=[template_config(ctx, folder_site(f))
                        for f in folders],
                scope=scope(keys=RAW_KEYS, subs=[label]),
            )
    if freesurfer:
        yield from freesurfer_shards(ctx, subjects)


def freesurfer_shards(ctx: RoastContext,
                      subjects: dict[str, list[Path]]) -> Iterator:
    """
    Yield the FreeSurfer work of the selected subjects.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    subjects : dict[str, list[Path]]
        Selected subjects and their folders

    Yields
    ------
    Shard | Fail
        One shard per subject that has a FreeSurfer archive (siblings of
        the scan shards, so that each is skipped by its own signature),
        the lookup tables; if there is no archive, a `Fail` when `--keys`
        names `fs` or `fs-all`, else an info line only
    """
    fs_archives = {}
    for label, folders in subjects.items():
        found = [a for f in folders for a in sorted_paths(f.glob("*.tar.gz"))
                 if is_freesurfer(a)]
        if len(found) > 1:
            lg.warning(f"sub-{label}: {len(found)} FreeSurfer archives: "
                       f"{found[0].name} converted, the others skipped")
        if found:
            fs_archives[label] = found[0]
    if not fs_archives:
        message = ("no FreeSurfer archive found in the folders of the "
                   "selected subjects")
        if ctx.options.get("fs_named"):
            yield Fail(message, name=FS_NAME, tag="fs")
        else:
            lg.info(f"{message}: FreeSurfer not converted")
        return
    yield Shard(roast_freesurfer_tables, tag="fs")
    ctx.progress(total=len(fs_archives), tag="fs")
    for label, archive in fs_archives.items():
        yield Shard(roast_freesurfer, label, archive, tag="fs",
                    advance="fs", id=f"roast/fs/sub-{label}",
                    unit={"sub": label}, inputs=[archive],
                    scope=scope(keys=FS_KEYS, subs=[label]))


def wants_fs(ctx: RoastContext) -> bool:
    """Whether FreeSurfer outputs are wanted (`fs`, or `fs-all`)."""
    return any(map(ctx.wants, FS_KEYS))


def xnat_subjects(ctx: RoastContext) -> dict[str, list[Path]]:
    """
    Subject folders of the NITRC-IR archives.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Returns
    -------
    dict[str, list[Path]]
        `{label: [folders]}`, sorted: folders `<site>[_<sample>]_<id>`
        (`Caltech_0051456`, `MaxMun_a_0051330`, `UCLA1_51201`, ids of 5
        or 7 digits, sites of ABIDE-I) that hold `*.tar.gz` archives;
        other folders with archives are skipped with a warning
    """
    out = {}
    if not ctx.source.is_dir():
        return out
    for folder in sorted_paths(ctx.source.iterdir()):
        if not folder.is_dir() or not any(folder.glob("*.tar.gz")):
            continue
        match = SUBJECT_FOLDER.fullmatch(folder.name)
        site = match["site"] if match else None
        try:
            if site is not None:
                parse_site(site)
        except TemplateError:
            site = None
        if site is None:
            lg.warning(f"{folder.name}/: not a NITRC-IR subject folder "
                       f"(<site>[_<sample>]_<id>, an id of 5 or 7 "
                       f"digits): skipped")
            continue
        out.setdefault(participant_label(match["id"]), []).append(folder)
    return dict(sorted(out.items()))


def folder_site(folder: Path) -> str:
    """Site of a subject folder (`MaxMun_a_0051330` -> `MaxMun_a`)."""
    return SUBJECT_FOLDER.fullmatch(folder.name)["site"]


def scan_archives(folder: Path) -> list[Path]:
    """Return the archives of a subject folder but FreeSurfer outputs."""
    return [a for a in sorted_paths(folder.glob("*.tar.gz"))
            if not is_freesurfer(a)]


def xnat_scan(site: str, archive: Path) -> tuple | None:
    """
    BIDS name of the scan of an archive (`<scan id>.tar.gz`).

    Parameters
    ----------
    site : str
        Site name, in the spelling of the data
    archive : Path
        The archive

    Returns
    -------
    (key, datatype, name, template) | None
        See `scan_name`
    """
    stem = archive.name[:-len(".tar.gz")]
    match = SCAN_ID.fullmatch(stem)
    if not match:
        lg.debug(f"{archive}: not a scan (assessor): not converted")
        return None
    mod = match["mod"].lower()
    number = int(match["n"] or 1)
    if mod in ("hires", "anat_hires"):
        mod, number = "anat", 2
    scan = scan_name(site, "rest" if mod == "rest" else "anat", number)
    if scan is None:
        lg.warning(f"{archive}: no BIDS name for scan {stem!r}: skipped")
    return scan


def roast_xnat_subject(ctx: RoastContext, sub: str,
                       folders: list[Path]) -> Iterator[Action]:
    """
    Convert the scan archives of one subject (a shard).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label
    folders : list[Path]
        Its folders

    Yields
    ------
    Action
        Per wanted scan: its image, then its sidecar
    """
    seen = set()
    for folder in folders:
        site = folder_site(folder)
        for archive in scan_archives(folder):
            scan = xnat_scan(site, archive)
            if scan is None:
                continue
            if not ctx.wants(scan[0]) or scan[2] in seen:
                continue
            seen.add(scan[2])
            yield from roast_xnat_scan(ctx, sub, site, archive, scan)


def roast_xnat_scan(ctx: RoastContext, sub: str, site: str, archive: Path,
                    scan: tuple) -> Iterator[Action]:
    """
    Convert the image of one scan archive.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label
    site : str
        Site name, in the spelling of the data
    archive : Path
        `<scan id>.tar.gz`
    scan : tuple
        See `scan_name`

    Yields
    ------
    Action
        The image (its first NIfTI member), then its sidecar; a `Fail`
        if the archive holds no image or cannot be read
    """
    found = None
    try:
        with archives.reading(
                archive, opener=TarArchive,
                label=f"{archive.parent.name}/{archive.name}") as tar:
            for member in tar:
                ext = image_ext(member.name)
                if ext is None:
                    continue
                if found is not None:
                    lg.warning(f"{archive}: {member.name}: more than one "
                               f"image ({found} converted): skipped")
                    continue
                found = member.name
                yield from scan_actions(
                    ctx, sub, site, scan, ext,
                    lambda dst, member=member: ExtractMember(
                        tar.archive, member.info, dst, tag=scan[0]),
                    member.open, PurePosixPath(member.name).name)
    except archives.ArchiveError as e:
        yield e.fail(tag=scan[0])
        return
    if found is None:
        yield Fail(f"{archive.parent.name}/{archive.name}: no NIfTI image",
                   name=archive.name, tag=scan[0])


# ----------------------------------------------------------------------
#   FreeSurfer (keys `fs`, `fs-all`)
# ----------------------------------------------------------------------

class FreeSurferTree:
    """
    Locate the FreeSurfer subject in the members of an archive.

    Members may sit under any number of wrapper folders
    (`<exp>/assessors/<label>/<resource>/files/<subject>/mri/...`). The
    root of a member is the folder just before its deepest FreeSurfer
    folder (`mri`, `surf`, `label`, `stats`, `scripts`, `touch`), so a
    wrapper named like one does not re-root the tree. Members are read
    in archive order (one pass): the first root is the subject's (a
    warning if its name lacks the subject's id), and every later member
    under it belongs to the tree (also outside the FreeSurfer folders:
    `tmp/`, `bem/`, files at the root; `fs-all` unpacks them); members of
    another root (a second tree, e.g. `fsaverage/`) are skipped with a
    warning, as are template subjects, absolute paths and paths with
    `..`. Members under the root that come before its first FreeSurfer
    folder cannot be placed yet and are skipped (one pass).

    Parameters
    ----------
    archive : str
        Name of the archive (for the warnings)
    sub : str
        Subject label (`51456`): the root should contain its id
    """

    def __init__(self, archive: str = "", sub: str = ""):
        self.archive = archive
        self.sub = sub
        self.root: tuple | None = None
        self.others: set = set()

    @staticmethod
    def split(member: str) -> tuple[tuple, PurePosixPath] | None:
        """
        Split a member into its root and its path in the tree.

        Parameters
        ----------
        member : str
            Name of the member

        Returns
        -------
        (root, relpath) | None
            None if the member is in no FreeSurfer folder, or its path is
            absolute or has `..`
        """
        path = PurePosixPath(member)
        parts = path.parts
        if path.is_absolute() or ".." in parts:
            return None
        anchors = [i for i, part in enumerate(parts[:-1]) if part in FS_DIRS]
        if not anchors:
            return None
        i = anchors[-1]
        return parts[:i], PurePosixPath(*parts[i:])

    def relpath(self, member: str) -> PurePosixPath | None:
        """
        Path of a member in the subject's tree.

        Parameters
        ----------
        member : str
            Name of the member

        Returns
        -------
        PurePosixPath | None
            None for members outside it (warned once per other tree)
        """
        path = PurePosixPath(member)
        if path.is_absolute() or ".." in path.parts:
            return None
        if self.root is not None and len(path.parts) > len(self.root) \
                and path.parts[:len(self.root)] == self.root:
            return PurePosixPath(*path.parts[len(self.root):])
        split = self.split(member)
        if split is None:
            return None
        root, rel = split
        template = bool(root) and FS_TEMPLATES.fullmatch(root[-1])
        if self.root is None and not template:
            self.root = root
            self.check_root()
            return rel
        if root not in self.others:
            self.others.add(root)
            kept = (f"the subject's is {'/'.join(self.root) or '.'}/"
                    if self.root is not None else "template subject")
            lg.warning(f"{self.archive}: FreeSurfer tree "
                       f"{'/'.join(root) or '.'}/ skipped ({kept})")
        return None

    def check_root(self) -> None:
        """Warn if the chosen root is not named after the subject."""
        number = self.sub.lstrip("0") if self.sub.isdigit() else self.sub
        if self.root and number and number not in self.root[-1]:
            lg.warning(f"{self.archive}: FreeSurfer tree "
                       f"{'/'.join(self.root)}/ does not name subject "
                       f"{self.sub}: converted as sub-{self.sub}, check "
                       f"that it is not another subject's tree")


def fs_relpath(member: str) -> PurePosixPath | None:
    """
    Path of a member in its FreeSurfer subject (`mri/aseg.mgz`).

    Parameters
    ----------
    member : str
        Member of a FreeSurfer archive, under any number of folders

    Returns
    -------
    PurePosixPath | None
        From its deepest FreeSurfer folder on (`FreeSurferTree.split`);
        None if there is none
    """
    split = FreeSurferTree.split(member)
    return None if split is None else split[1]


def fs_source(ctx: RoastContext, sub: str) -> Path:
    """Unpacked FreeSurfer subject (`derivatives/<fs>/sourcedata/`)."""
    return ctx.layout.derivative(FS_NAME).source / f"sub-{sub}"


def roast_freesurfer_tables(ctx: RoastContext) -> Iterator[Action]:
    """
    Write the description and lookup tables of the FreeSurfer derivative.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action
        The description of the derivative, then one table per atlas
    """
    drv = ctx.layout.derivative(FS_NAME)
    yield WriteJSON(freesurfer_description(drv), drv.description,
                    tag="derivatives")
    yield from fs.bidsify_toplevel(drv.root, FS_VERSION)


def freesurfer_description(drv) -> dict:
    """Describe the FreeSurfer outputs (`derivatives/<fs>`)."""
    desc = drv.dataset_description()
    desc["Name"] = "ABIDE-I - FreeSurfer"
    desc["GeneratedBy"] = [{
        "Name": "FreeSurfer",
        "Version": ".".join(map(str, FS_VERSION)),
        "Description": "FreeSurfer outputs distributed with the dataset "
                       "(NITRC-IR assessors).",
    }, {
        "Name": "brainspresso",
        "Description": "Conversion of the ABIDE-I archives to BIDS.",
        "CodeURL": "https://github.com/balbasty/brainspresso",
    }]
    return desc


def roast_freesurfer(ctx: RoastContext, sub: str,
                     archive: Path) -> Iterator[Action]:
    """
    Unpack a FreeSurfer subject, then convert it (a shard).

    Only the files that BIDS describes are unpacked, unless `fs-all`;
    with `--sidecars only`, nothing is unpacked: the sidecars are
    written from the names of the members.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label
    archive : Path
        FreeSurfer archive

    Yields
    ------
    Action
        The unpacking (`staging`), then the converted files
    """
    drv = ctx.layout.derivative(FS_NAME)
    src = fs_source(ctx, sub)
    tree = FreeSurferTree(f"{archive.parent.name}/{archive.name}", sub)
    t1 = f"bids:raw:sub-{sub}/anat/sub-{sub}_T1w.nii.gz"
    dst = drv.root / f"sub-{sub}"
    if ctx.json == "only":
        # sidecars only: the names of the members are enough (those that
        # `ExtractMembers` would give to `rename`)
        try:
            names = archives.names(
                archive, opener=TarArchive, extractable=True,
                label=f"{archive.parent.name}/{archive.name}")
        except archives.ArchiveError as e:
            yield e.fail(tag="fs")
            return
        members = {src.joinpath(*rel.parts) for rel in
                   map(tree.relpath, names) if rel is not None}
        yield from fs.bidsify(src, dst, [t1], json=ctx.json,
                              exists=members.__contains__)
        return
    everything = ctx.wants("fs-all")

    def rename(member: str) -> Path | None:
        rel = tree.relpath(member)
        if rel is None:
            return None
        if not everything and not rel.as_posix().endswith(
                fs.bidsifiable_outputs):
            return None
        return src.joinpath(*rel.parts)

    # unpacked files are inputs of the conversion (`staging`: the JSON
    # policy does not apply to them)
    yield ExtractMembers(archive, Claim(src), rename, tag="fs",
                         root=ctx.layout.root, staging=True)
    # (converted once the tree is unpacked: the engine runs each action
    # before asking for the next one)
    yield from fs.bidsify(src, dst, [t1], json=ctx.json)


#: Roaster of each source
ROASTERS: dict[str, Callable] = {
    "s3": roast_nitrc,
    "nitrc": roast_nitrc,
    "xnat": roast_xnat,
}
