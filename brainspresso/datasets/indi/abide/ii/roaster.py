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

`<id>` has 5 digits, `<ss>` 2 (`session_1` is `ses-01`). Runs (owner
decision): a subject whose session has several scans of a kind
(`rest_1`, `rest_2`) has `run-<n>` on every one of them (`<n>` the
number of the folder), a subject with a single scan of that kind has no
run entity. With `--layout flat`, the content of `rawbids/` is in
`ABIDE-2/` itself. The participants and phenotype tables are written
complete (every subject of the CSV files) to `.brainspresso/orig/` and
trimmed to the subjects on disk.

Each tarball is one shard, decompressed once, read lazily in archive
order. Names are only known once every scan of a subject's session has
been read (a tarball lists a subject's folders together): the selected
images of a session are spooled to a temporary folder of the dataset
(`.brainspresso/tmp/`) as they are reached, then moved into place when
the next session starts (or the tarball ends). The sidecar templates
that a shard may use (paths, hashes) are its `Shard.config`. A corrupt
or truncated tarball fails its shard only (the session it was reading
is not written).
"""
from __future__ import annotations

import hashlib
import io
import re
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterator, NamedTuple

from brainspresso.actions import Action, Fail
from brainspresso.bids.harvest import update_files
from brainspresso.bids.keys import KeyAliases
from brainspresso.bids.tables import trim_tables
from brainspresso.convert.nifti import image_ext
from brainspresso.datasets.base import KeySpec
from brainspresso.datasets.indi.abide import scans
from brainspresso.datasets.indi.abide.ii.keys import allkeys
from brainspresso.datasets.indi.abide.ii.sites import (
    NO_TEMPLATES,
    SITES,
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
from brainspresso.datasets.indi.abide.tables import WIDTH, participant_label
from brainspresso.roast import (
    RoastContext,
    archives,
    sidecars,
)
from brainspresso.roast.archives import place, spool
from brainspresso.roast.archives import spool_host as _host  # noqa: F401
from brainspresso.roast.helpers import archive_shards
from brainspresso.roast.run import python_api
from brainspresso.roast.sidecars import TemplateError, template_sidecar
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.path import sorted_paths

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

#: Gradient tables of a DWI folder: member suffix (any case) -> BIDS
#: extension
GRADIENTS = {".bval": ".bval", ".bvals": ".bval",
             ".bvec": ".bvec", ".bvecs": ".bvec"}

#: Bytes of a member read for its NIfTI header (`--sidecars only`)
HEADER_BYTES = 65536

#: Name of the spool folders (`.brainspresso/tmp/abide2-<host>-<pid>-*`,
#: `roast.archives.spool_prefix`)
SPOOL_NAME = "abide2"


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

    def name(self, sub: str, ses: str, run: int | None) -> str:
        """`sub-<id>_ses-<ss>_<entities>[run-<n>_]<suffix>`."""
        run_ = "" if run is None else f"run-{run}_"
        return f"sub-{sub}_ses-{ses}_{self.entities}{run_}{self.suffix}"


#: Scan folders (`anat_1`, `rest_2`, `dti_1`) -> kind
KINDS = {
    "anat": Kind("T1w", "anat", "", "T1w", "T1w.json"),
    "rest": Kind("rest", "func", "task-rest_", "bold", "bold.json"),
    "dti": Kind("dti", "dwi", "", "dwi", "dwi.json"),
    "dwi": Kind("dti", "dwi", "", "dwi", "dwi.json"),
}

#: Keys of the raw scans (`roast.scope` of the tarball shards)
RAW_KEYS = tuple(sorted({kind.key for kind in KINDS.values()}))

SCAN_FOLDER = re.compile(r"(?P<kind>[A-Za-z]+)_(?P<n>\d+)")
SESSION_FOLDER = re.compile(r"session_(?P<n>\d+)", re.IGNORECASE)


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
    csvs = sorted_paths(p for p in source.glob(CSV_GLOB) if p.is_file())
    if not csvs and (source / COMPOSITE).is_file():
        csvs = [source / COMPOSITE]
    return csvs


def tarballs(source: Path) -> list[Path]:
    """NITRC tarballs (`*.tar.gz`, `*.tgz`) of a source folder."""
    if not source.is_dir():
        return []
    return sorted_paths(
        p for p in source.iterdir()
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
        | None}}`; if the name gives no site (the members do), `{"site":
        None, "templates": {"<SITE>/<sample>": {name: ...}}}` for every
        site and sample with templates (packaged or in `sourcedata/`)
    """
    site = tarball_site(path)
    if site is not None:
        return {"site": f"{site[0]}/{site[1] or '1'}",
                "templates": sidecars.template_config(
                    lambda name: site_template(*site, name, ctx.resource),
                    TEMPLATE_NAMES)}
    out = {}
    for site, sample in template_folders(ctx.source):
        out[f"{site}/{sample}"] = sidecars.template_config(
            partial(site_template, site, sample, resource=ctx.resource),
            TEMPLATE_NAMES)
    return {"site": None, "templates": out}


def template_folders(source: Path) -> list[tuple[str, str]]:
    """(SITE, sample) of the template folders, packaged or in `source`."""
    out = set()
    for root in (TEMPLATES, source):
        for site in SITES:
            folder = Path(root) / site
            if folder.is_dir():
                out.update((site, p.name) for p in folder.iterdir()
                           if p.is_dir())
    return sorted(out)


#: Phenotype tables renamed (old name -> new name): a `.` in a stem is
#: not BIDS
RENAMED_PHENOTYPES = {"cbcl_1.5-5": "cbcl_1p5-5"}


def migrate_renamed_phenotypes(ctx: RoastContext) -> None:
    """
    Remove the phenotype tables of an earlier roast under an old name.

    Only when the roast writes the phenotype tables (`pheno`), so that
    the new table replaces the old one. The pristine copy
    (`.brainspresso/orig/`) is removed, so that the visible table is not
    derived from it again (`trim_originals`); the visible table and its
    data dictionary too, unless they were edited (the table differs
    from its trimmed pristine copy, the dictionary from the hash in the
    manifest): then they are kept, with a warning. The manifest entries
    of the files removed are dropped. The roast owns these files: done
    whatever `--if-exists` says. A file that cannot be checked or
    removed is warned about and left.
    """
    if not ctx.wants("pheno"):
        return
    folder = ctx.layout.phenotype
    removed = []
    for old, new in RENAMED_PHENOTYPES.items():
        table, sidecar = folder / f"{old}.tsv", folder / f"{old}.json"
        pristine = ctx.layout.original(table)
        if not any(p.exists() for p in (pristine, table, sidecar)):
            continue
        checks = {table: partial(_is_trim_of, ctx, table, pristine),
                  sidecar: partial(_is_recorded, ctx, sidecar)}
        for path, unedited in checks.items():
            if not path.exists():
                continue
            try:
                if not unedited():
                    lg.warning(f"phenotype/{path.name}: renamed {new}"
                               f"{path.suffix}, but edited since "
                               f"brainspresso wrote it: kept (delete it "
                               f"by hand)")
                    continue
                path.unlink()
            except Exception as e:
                lg.warning(f"phenotype/{path.name} (renamed {new}"
                           f"{path.suffix}): not removed "
                           f"({type(e).__name__}: {e})")
                continue
            removed.append(path)
            lg.info(f"phenotype/{path.name}: removed (renamed "
                    f"{new}{path.suffix})")
        if pristine.exists():
            rel = pristine.relative_to(ctx.root).as_posix()
            try:
                pristine.unlink()
            except OSError as e:
                lg.warning(f"{rel}: not removed ({type(e).__name__}: {e})")
                continue
            removed.append(pristine)
            lg.info(f"{rel}: removed (renamed {new}.tsv)")
    if removed:
        update_files(ctx.layout, deleted=[
            p.relative_to(ctx.root).as_posix() for p in removed])


def _is_trim_of(ctx: RoastContext, table: Path, pristine: Path) -> bool:
    """Whether a visible table is the trimmed copy of its pristine one."""
    if not table.exists() or not pristine.exists():
        return False
    rel = table.relative_to(ctx.root).as_posix()
    with tempfile.TemporaryDirectory() as tmp:
        trim_tables(Path(tmp), {rel: pristine}, subjects=ctx.layout.raw,
                    sessions=True)
        trimmed = Path(tmp) / rel
        return trimmed.exists() and \
            trimmed.read_bytes() == table.read_bytes()


def _is_recorded(ctx: RoastContext, path: Path) -> bool:
    """Whether a file holds the hash that the manifest records for it."""
    digest = ctx.recorded(path)
    return path.exists() and digest is not None and \
        hashlib.sha256(path.read_bytes()).hexdigest() == digest


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
    migrate_renamed_phenotypes(ctx)
    yield from scans.roast_meta(
        ctx, phenotype_files(ctx.source), "ABIDEII-<SITE>_<sample>.csv",
        PARTICIPANTS, PHENOTYPES, pristine_phenotypes=True)
    if not wants_raw(ctx):
        return
    archives.sweep_spools(ctx, SPOOL_NAME)
    bucket = (ctx.source / RAWDATA).is_dir()
    if bucket:
        lg.warning(f"{RAWDATA}/ (the bucket copy, harvested with --source "
                   f"s3) is not roasted: only the NITRC tarballs are")
    found = tarballs(ctx.source)
    if not found:
        note = f" ({RAWDATA}/ is not roasted)" if bucket else ""
        yield Fail(f"no NITRC tarball (ABIDEII-<SITE>_<sample>.tar.gz) "
                   f"found{note}", name="raw", tag="raw")
        return
    yield from archive_shards(ctx, found, roast_archive, tag="raw",
                              keys=RAW_KEYS,
                              config=partial(template_config, ctx))


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


def parse_member(parts: tuple[str, ...],
                 default: tuple | None = None) -> Member | None:
    """
    Parse a member of a NITRC tarball.

    Parameters
    ----------
    parts : tuple[str]
        Normalised parts of its name (`roast.archives.Member.parts`):
        `[...]/ABIDEII-<SITE>_<sample>/<id>/session_<s>/<kind>_<n>/<file>`
        (leading folders are ignored)
    default : (site, sample), optional
        Site of the tarball, if the folder of the member names none

    Returns
    -------
    Member | None
        None for files that are not in a scan folder of a subject
        (subject ids that are not numbers are logged)
    """
    name = "/".join(parts)
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


@dataclass
class Candidate:
    """
    A file of a scan folder (an image or a gradient table).

    Parameters
    ----------
    name : str
        Member name
    ext : str
        BIDS extension (`.nii.gz`, `.bval`)
    preferred : bool
        Its stem is the kind of its folder (`anat_1/anat.nii.gz`)
    spool : Path | None
        Spooled copy of an image
    data : bytes | None
        Content of a gradient table, or the first bytes of an image
        (its header, when it is not spooled)
    error : str | None
        Why the image could not be spooled
    """

    name: str
    ext: str
    preferred: bool
    spool: Path | None = None
    data: bytes | None = None
    error: str | None = None


@dataclass
class Scan:
    """
    A scan folder of a session (`anat_1/`).

    Parameters
    ----------
    member : Member
        What the name of its first file says
    kind : Kind
        Its kind
    images : list[Candidate]
        Its images, in archive order
    gradients : dict[str, list[Candidate]]
        Its gradient tables, by extension
    """

    member: Member
    kind: Kind
    images: list = field(default_factory=list)
    gradients: dict = field(default_factory=dict)

    def chosen(self, candidates: list, path: Path) -> Candidate | None:
        """Choose the file of a scan: the preferred one, else the first."""
        if not candidates:
            return None
        best = next((c for c in candidates if c.preferred), candidates[0])
        if len(candidates) > 1:
            lg.warning(f"{path.name}: {len(candidates)} files for "
                       f"{best.ext} in {PurePosixPath(best.name).parent}: "
                       f"{PurePosixPath(best.name).name} used")
        return best


def preferred(folder: str, name: str, ext: str) -> bool:
    """Whether a file's stem is the kind of its folder (any case)."""
    stem = name[:len(name) - len(ext)].lower()
    kinds = {"dti", "dwi"} if folder in ("dti", "dwi") else {folder}
    return stem in kinds


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
        Per session of a subject, once all its scans are read: per
        selected scan, its image, its sidecar and gradient tables; a
        `Fail` if the tarball cannot be read

    Notes
    -----
    Members are read lazily, in archive order, so that a tarball is
    decompressed once. The selected images of the session being read
    are spooled as they are reached (their names depend on the other
    scans of the session); its actions are yielded, and run, when the
    next session starts (the engine runs an action before asking for
    the next), then its spooled files are removed. Sessions start and
    end on the members of every subject and key, selected or not, so
    that the selection decides which outputs exist, never their names
    (`roast.scope`); every subject is reported (`ctx.keeps_subject`).
    """
    default = tarball_site(path)
    unknown = Counter()
    notes = set()
    with tempfile.TemporaryDirectory(
            prefix=archives.spool_prefix(SPOOL_NAME),
            dir=archives.spool_folder(ctx)) as tmp:
        session = None
        current = None          # (sub, ses) of the member being read
        done = set()
        try:
            with archives.reading(path, opener=TarArchive) as archive:
                for member in archive:
                    parsed = parse_member(member.parts, default)
                    if parsed is None:
                        continue
                    kind = KINDS.get(parsed.folder)
                    if kind is None:
                        unknown[f"{parsed.folder}_<n>"] += 1
                        continue
                    kept = ctx.keeps_subject(parsed.sub)
                    key = (parsed.sub, parsed.ses)
                    if current is not None and current != key:
                        if session is not None:
                            yield from session.actions(ctx, path, notes)
                            session.close()
                            session = None
                        done.add(current)
                    current = key
                    if not (kept and ctx.wants(kind.key)):
                        continue
                    if key in done:
                        if key not in notes:
                            notes.add(key)
                            yield Fail(
                                f"{path.name}: the folders of sub-{key[0]} "
                                f"ses-{key[1]} are not together: "
                                f"{member.name} and the later ones skipped "
                                f"(their runs could contradict the names "
                                f"already written)",
                                name=f"sub-{key[0]}_ses-{key[1]}",
                                tag=kind.key)
                        continue
                    if session is None:
                        session = Session(key, Path(tmp))
                    session.add(ctx, path, archive, member, parsed, kind,
                                unknown)
                if session is not None:
                    yield from session.actions(ctx, path, notes)
        except archives.ArchiveError as e:
            lost = "" if session is None else \
                f"; sub-{session.key[0]} ses-{session.key[1]} not written"
            yield e.fail(tag="raw", note=lost)
        finally:
            if unknown:
                files = ", ".join(f"{k} ({n} files)"
                                  for k, n in sorted(unknown.items()))
                lg.warning(f"{path.name}: no BIDS name for {files}: "
                           f"skipped")


class Session:
    """
    The scans of one session of a subject, as they are read.

    Parameters
    ----------
    key : (str, str)
        Subject and session labels
    tmp : Path
        Spool folder of the shard
    """

    def __init__(self, key: tuple[str, str], tmp: Path):
        self.key = key
        self.tmp = tmp
        self.scans: dict[tuple[str, int], Scan] = {}
        self.spooled: list[Path] = []
        self.previous: dict[tuple[str, str], set] = {}

    def add(self, ctx: RoastContext, path: Path, archive, member,
            parsed: Member, kind: Kind, unknown: Counter) -> None:
        """
        Read one member: spool an image, keep a gradient table.

        Parameters
        ----------
        ctx : RoastContext
            Context of the roast
        path : Path
            Tarball
        archive : roast.archives.Reader
            The open tarball
        member : roast.archives.Member
            The member (the current one: read forward only)
        parsed : Member
            What its name says
        kind : Kind
            Its kind of scan
        unknown : Counter
            Files without a BIDS name (updated)
        """
        name = PurePosixPath(member.name).name
        if name.startswith("."):
            unknown["hidden files"] += 1      # (`._anat.nii.gz`: macOS)
            return
        if kind.key == "T1w" and parsed.run > 1 and \
                parsed.site in HIRES_SITES:
            lg.warning(f"{path.name}: {member.name}: {parsed.site} "
                       f"anat_{parsed.run} may be the high-resolution scan "
                       f"(no template): skipped")
            return
        scan = self.scans.setdefault((kind.key, parsed.run),
                                     Scan(parsed, kind))
        ext = image_ext(name)
        if ext is not None:
            candidate = Candidate(member.name, ext,
                                  preferred(parsed.folder, name, ext))
            if any(c.preferred for c in scan.images) or \
                    (scan.images and not candidate.preferred):
                scan.images.append(candidate)   # (never used: not read)
                return
            if ctx.accepts("data") and not self.present(ctx, parsed, kind,
                                                        ext):
                candidate.spool = self.tmp / f"{len(self.spooled)}{ext}"
                self.spooled.append(candidate.spool)
                with member.open() as src:
                    candidate.error = spool(src, candidate.spool)
            else:       # (`--sidecars only`, or the outputs are there)
                candidate.data = member.read(HEADER_BYTES)
            scan.images.append(candidate)
            return
        lower = name.lower()
        ext = next((v for k, v in GRADIENTS.items() if lower.endswith(k)),
                   None)
        if ext is None or kind.datatype != "dwi":
            unknown[f"{parsed.folder}_<n>/{name}"] += 1
            return
        data = member.read()
        scan.gradients.setdefault(ext, []).append(Candidate(
            member.name, ext, preferred(parsed.folder, name, ext),
            data=data))

    def present(self, ctx: RoastContext, parsed: Member, kind: Kind,
                ext: str) -> bool:
        """
        Whether an image need not be spooled (`--if-exists skip`).

        The images of the kind that an earlier roast wrote for this
        session tell how it named them: without `run-<n>` (one scan), or
        with it. An image need not be spooled if the name it got then
        exists: its action will skip it. If the tarball changed so that
        it now gets another name (a scan was added), its output is a
        `Fail` that names `--if-exists overwrite`, which spools it.

        Parameters
        ----------
        ctx : RoastContext
            Context of the roast
        parsed : Member
            What the name of the image says
        kind : Kind
            Its kind of scan
        ext : str
            Its extension

        Returns
        -------
        bool
        """
        if ctx.ifexists != "skip":
            return False
        runs = self.written(ctx, parsed, kind, ext)
        if None in runs:
            # one scan before: this one, if it is the first of its kind
            return runs == {None} and not any(
                s.images for (k, _), s in self.scans.items()
                if k == kind.key)
        return parsed.run in runs

    def written(self, ctx: RoastContext, parsed: Member, kind: Kind,
                ext: str) -> set:
        """Find the runs of the images of a kind on disk (None: no run)."""
        key = (kind.key, ext)
        if key not in self.previous:
            folder = (ctx.layout.raw / f"sub-{parsed.sub}"
                      / f"ses-{parsed.ses}" / kind.datatype)
            head = f"sub-{parsed.sub}_ses-{parsed.ses}_{kind.entities}"
            name = re.compile(rf"{re.escape(head)}(?:run-(?P<run>\d+)_)?"
                              rf"{re.escape(kind.suffix + ext)}")
            runs = set()
            for file in folder.iterdir() if folder.is_dir() else ():
                match = name.fullmatch(file.name)
                if match:
                    runs.add(int(match["run"]) if match["run"] else None)
            self.previous[key] = runs
        return self.previous[key]

    def actions(self, ctx: RoastContext, path: Path,
                notes: set) -> Iterator[Action]:
        """
        Convert the scans of the session (all of them have been read).

        Parameters
        ----------
        ctx : RoastContext
            Context of the roast
        path : Path
            Tarball
        notes : set
            Warnings already logged by the shard (updated)

        Yields
        ------
        Action
            Per scan with an image: the image, its sidecar, its gradient
            tables
        """
        scans_ = [s for s in self.scans.values() if s.images]
        counts = Counter(s.kind.key for s in scans_)
        for scan in sorted(scans_, key=lambda s: (s.kind.key, s.member.run)):
            m, kind = scan.member, scan.kind
            run = m.run if counts[kind.key] > 1 else None
            stem = (ctx.layout.raw / f"sub-{m.sub}" / f"ses-{m.ses}"
                    / kind.datatype / kind.name(m.sub, m.ses, run))
            image = scan.chosen(scan.images, path)
            dst = Path(f"{stem}{image.ext}")
            if image.error is not None:
                yield Fail(f"{PurePosixPath(image.name).name}: cannot be "
                           f"spooled ({image.error})", name=dst.name,
                           tag=kind.key)
                image.spool = None
            elif image.spool is None and ctx.accepts("data") and \
                    not dst.exists():
                # (not spooled: an earlier roast wrote the other name,
                # `Session.present`)
                yield Fail(f"{PurePosixPath(image.name).name}: not "
                           f"spooled (an earlier roast wrote it with or "
                           f"without `run-<n>`): roast again with "
                           f"--if-exists overwrite", name=dst.name,
                           tag=kind.key)
            elif image.spool is not None or ctx.accepts("data"):
                # (not spooled: it is there, and the action skips it)
                yield Action(path, dst, partial(place, image.spool),
                             input="path", kind="data", tag=kind.key)
            yield from sidecar_actions(ctx, path, scan, stem, dst, image,
                                       notes)
            for ext, candidates in sorted(scan.gradients.items()):
                table = scan.chosen(candidates, path)
                yield Action(path, f"{stem}{ext}",
                             partial(write_bytes, table.data),
                             tag=kind.key)

    def close(self) -> None:
        """Remove the spooled files that were not moved into place."""
        for spooled in self.spooled:
            try:
                spooled.unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                lg.debug(f"{spooled}: {e}")


def write_bytes(data: bytes, file) -> None:
    """Write bytes to an opened file."""
    file.write(data)


def sidecar_actions(ctx: RoastContext, path: Path, scan: Scan, stem: Path,
                    image_path: Path, image: Candidate,
                    notes: set) -> Iterator[Action]:
    """
    Write the sidecar of a scan.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    path : Path
        Tarball
    scan : Scan
        The scan
    stem : Path
        Its output path, without extension
    image_path : Path
        Its output image
    image : Candidate
        Its source image
    notes : set
        Warnings already logged by the shard (updated)

    Yields
    ------
    Action
        Its sidecar (a copy of the template, or the template and the
        RepetitionTime of the header); a `Fail` if the template is
        missing (a warning, once per shard, for the sites without
        templates: `sites.NO_TEMPLATES`) or cannot be read
    """
    if not ctx.accepts("sidecar"):
        return
    m, kind = scan.member, scan.kind
    image_name = PurePosixPath(image.name).name
    try:
        template = site_template(m.site, m.sample, kind.template,
                                 ctx.resource)
    except TemplateError as e:
        if m.site in NO_TEMPLATES:
            if m.site not in notes:
                notes.add(m.site)
                lg.warning(f"{path.name}: {m.site} has no sidecar "
                           f"templates (no protocol printout): its images "
                           f"are written without sidecars")
            return
        yield Fail(f"{image_name}: {e}", name=f"{stem.name}.json",
                   tag=kind.key)
        return
    def open_image(image=image):
        if image.spool is not None and image.spool.is_file():
            return open(image.spool, "rb")
        return io.BytesIO(image.data or b"")

    yield from template_sidecar(template, stem, kind.key,
                                bold=kind.suffix == "bold", image=image_path,
                                open_image=open_image, image_name=image_name)


# ----------------------------------------------------------------------
#   Python API
# ----------------------------------------------------------------------

#: Roast `<root>` (the `ABIDE-2` folder): `roast(root, keys=..., subs=...)`
roast = python_api(roast_nitrc, KEYS, folder="ABIDE-2",
                   width=WIDTH, example=29001, spec=ROAST_SPEC)
