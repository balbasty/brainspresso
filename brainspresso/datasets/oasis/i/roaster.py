"""
OASIS-I: convert the harvested discs to BIDS.

Expected input
--------------
OASIS-1/sourcedata/
    oasis_cross-sectional_disc{1..12}.tar.gz
    oasis_cs_freesurfer_disc{1..11}.tar.gz     (no FreeSurfer disc 12)
    oasis_cross-sectional.xlsx
    oasis_cross-sectional-reliability.xlsx
    oasis_cross-sectional_facts.pdf

Output (study layout)
---------------------
OASIS-1/
    dataset_description.json      (DatasetType "study")
    rawbids/
        README, dataset_description.json, participants.{tsv,json}
        sub-{04d}/anat/sub-{04d}_run-{d}_T1w.{nii.gz,json}
    derivatives/oasis-processed/            (keys avg, tal, fsl)
        space-Talairach_res-1mm_dseg.tsv    (classes of the FSL segmentation)
        sub-{04d}/anat/
            sub-{04d}_res-1mm_T1w.{nii.gz,json}
            sub-{04d}_space-Talairach_res-1mm_T1w.{nii.gz,json}
            sub-{04d}_space-Talairach_res-1mm_desc-skullstripped_T1w.{nii.gz,json}
            sub-{04d}_space-Talairach_res-1mm_dseg.{nii.gz,json}
    derivatives/oasis-freesurfer/           (keys fs, fs-all)
        atlas-*_dseg.tsv
        sourcedata/sub-{04d}/{mri,surf,label,...}/   (unpacked)
        sub-{04d}/anat/...      (converted to BIDS, `freesurfer.bidsify`)

With `--layout flat`, the content of `rawbids/` is in `OASIS-1/` itself.

A disc is a gz tarball that holds the images (or FreeSurfer trees) of
about 40 subjects, and reading a member of a gz tarball decompresses
everything before it. Each disc is therefore read by one shard (task id
`roast/discs/<kind>-disc<NN>`, inputs: the disc; config: the sidecar
template of an image disc, path and hash; so that an unchanged disc is
skipped on a re-run): an image disc
is listed once, then its members are extracted in archive order (one
forward pass); a FreeSurfer disc is unpacked in one pass. Discs that
are not in `sourcedata/` are skipped with a warning, unless named with
`--discs` (then an error). A disc shard declares its keys and the
subjects it sees (`roast.scope`): a later roast that keeps no more skips
it. Only the first session (`MR1`) of each
subject is converted (`MR2` are repeats). The participants table is
written complete to `.brainspresso/orig/` and trimmed to the subjects
on disk.
"""  # noqa: E501
import re
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Iterator

import nibabel as nib
import numpy as np

from brainspresso.actions import (
    Action,
    Claim,
    CopyJSON,
    Fail,
    WriteJSON,
    WriteTSV,
)
from brainspresso.actions.archive import ExtractMembers
from brainspresso.actions.state import read_task_states
from brainspresso.bids.keys import KeyAliases
from brainspresso.errors import UsageError
from brainspresso.freesurfer import bidsify as fs
from brainspresso.roast import (
    SEEN,
    RoastContext,
    Shard,
    archives,
    copy_templates,
    roast_dataset,
    scope,
)
from brainspresso.roast.helpers import template_config
from brainspresso.sources.base import subject_labels
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.io import (
    nibabel_convert,
    read_json,
    write_from_buffer,
    write_tsv,
)
from brainspresso.utils.vol import make_affine
from brainspresso.utils.vol import relabel as vol_relabel

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"
PROC_NAME = "oasis-processed"
FS_NAME = "oasis-freesurfer"
FS_VERSION = (4, 0)
XLSX = "oasis_cross-sectional.xlsx"
SIDECAR = "T1w.json"
WIDTH = 4               # subject labels: `sub-0001`

#: Keys of the roast
ROAST_KEYS = ("raw", "fs", "fs-all", "fsl", "avg", "tal", "meta")
#: Discs of images (raw, avg, tal, fsl) and of FreeSurfer trees
DISCS = {"raw": range(1, 13), "fs": range(1, 12)}
#: Keys read from each kind of disc
DISC_KEYS = {"raw": ("raw", "avg", "tal", "fsl"), "fs": ("fs", "fs-all")}

# "raw" is the only key of the raw dataset; the others are categories
# (folders of the derivatives, or metadata)
KEYS = KeyAliases(
    set(ROAST_KEYS),
    bids={"raw": {"datatypes": ["anat"], "suffixes": ["T1w"]}},
    folders={
        **{key: [f"derivatives/{PROC_NAME}"] for key in ("avg", "tal", "fsl")},
        **{key: [f"derivatives/{FS_NAME}"] for key in ("fs", "fs-all")},
    },
)

# OASIS images are stored in ASL orientation
# https://brainder.org/2011/08/13/converting-oasis-brains-to-nifti/
AFFINE_RAW = make_affine(
    [256, 256, 128], [1.0, 1.0, 1.25], orient="ASL", center="x/2"
)
AFFINE_AVG = make_affine(
    [256, 256, 160], [1.0, 1.0, 1.0], orient="ASL", center="x/2"
)
AFFINE_TAL = make_affine(
    [176, 208, 176], [1.0, 1.0, 1.0], orient="LAS", center="x/2"
)

# FSL (FAST) classes 1, 2, 3 are CSF, GM, WM; they are relabelled in
# the SPM order (`utils.vol.relabel` takes `{new: old}`)
FSL_RELABEL = {1: 2, 2: 3, 3: 1}
#: Lookup table of the relabelled segmentation (dataset level, inherited)
DSEG_NAME = "space-Talairach_res-1mm_dseg.tsv"
DSEG_TABLE = [["index", "name"], [1, "GM"], [2, "WM"], [3, "CSF"]]


def roast(root, *, subs=(), exclude_subs=(), discs=None, **options):
    """
    Roast `<root>` (the `OASIS-1` folder).

    Parameters
    ----------
    root : path
        Dataset folder
    subs, exclude_subs : list[int | str]
        Subjects: numbers (`1` is `sub-0001`), labels with or without
        `sub-`, ranges, files (as `--subs`)
    discs : list[int]
        Only these discs, 1 to 12 (default: all; a disc named here that
        is not in `sourcedata/` is an error, other missing discs are
        skipped)
    **options
        Those of the command line (`keys`, `exclude_keys`, `json`,
        `ifexists`, `layout`), `jobs` and `display`

    Returns
    -------
    RoastReport
        See `roast.run_roast`

    Raises
    ------
    UsageError
        If a disc is not one of 1 to 12
    """
    if discs:
        check_discs(discs)
    return roast_dataset(
        roast_oasis1, root, KEYS, templates=TEMPLATES,
        subs=sorted(subject_labels(subs, WIDTH)),
        exclude_subs=sorted(subject_labels(exclude_subs, WIDTH)),
        options={"discs": sorted(discs)} if discs else None, **options,
    )


def check_discs(discs) -> None:
    """
    Check disc numbers.

    Parameters
    ----------
    discs : list[int]
        Values of `--discs`

    Raises
    ------
    UsageError
        If a disc is not one of 1 to 12
    """
    wrong = sorted(set(discs) - set(DISCS["raw"]))
    if wrong:
        raise UsageError(f"--discs: no disc {', '.join(map(str, wrong))} "
                         f"(discs are 1 to 12)")


def roast_oasis1(ctx: RoastContext) -> Iterator:
    """
    Roast OASIS-I: metadata, then one shard per disc.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast (`ctx.options["discs"]`: discs named with
        `--discs`; absent: all)

    Yields
    ------
    Action | Shard
        Metadata actions, then disc shards
    """
    if ctx.wants("meta"):
        yield from roast_meta(ctx)
    if ctx.wants("fsl"):
        drv = ctx.layout.derivative(PROC_NAME)
        yield WriteTSV(DSEG_TABLE, drv.root / DSEG_NAME, tag="fsl")
    if ctx.wants("fs") or ctx.wants("fs-all"):
        drv = ctx.layout.derivative(FS_NAME)
        for action in fs.bidsify_toplevel(drv.root, FS_VERSION):
            action.tag = "fs"
            yield action

    named = ctx.options.get("discs")
    shards = []
    for kind, roaster in (("raw", roast_image_disc),
                          ("fs", roast_freesurfer_disc)):
        if not any(map(ctx.wants, DISC_KEYS[kind])):
            continue
        for disc in DISCS[kind]:
            if named is not None and disc not in named:
                continue
            path = disc_path(ctx, kind, disc)
            if not path.exists():
                message = f"{path.name} not found"
                if named is not None:
                    yield Fail(message, name=path.name, tag="discs")
                else:
                    lg.warning(f"{message} (skipped)")
                continue
            shards.append(Shard(
                roaster, path, tag="discs", advance="discs",
                id=f"roast/discs/{kind}-disc{disc:02d}", inputs=[path],
                config=template_config(ctx, [SIDECAR]) if kind == "raw"
                else None,
                scope=scope(keys=DISC_KEYS[kind], subs=SEEN),
            ))
    ctx.progress(total=len(shards), tag="discs")
    yield from shards


def warn_duplicates(ctx: RoastContext) -> Iterator[Action]:
    """
    Warn about subjects found on two discs of the same kind.

    The `finalize` hook of `roast_oasis1`: it reads the outputs that
    the task states of the disc shards record.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action
        Nothing
    """
    found = {}
    for task, record in read_task_states(ctx.layout.state).items():
        match = re.fullmatch(r"roast/discs/(raw|fs)-disc\d+", task)
        if not match:
            continue
        subjects = {m[1] for m in map(
            partial(re.search, r"(?:^|/)sub-(\d+)/"),
            record.get("outputs") or []) if m}
        for sub in subjects:
            found.setdefault((match[1], sub), []).append(task)
    for (kind, sub), tasks in sorted(found.items()):
        if len(tasks) > 1:
            lg.warning(f"sub-{sub} is on several {kind} discs: "
                       f"{', '.join(sorted(tasks))}")
    yield from ()


roast_oasis1.finalize = warn_duplicates


def disc_path(ctx: RoastContext, kind: str, disc: int) -> Path:
    """
    Return the path of a disc.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    kind : {"raw", "fs"}
        Images or FreeSurfer trees
    disc : int
        Disc number

    Returns
    -------
    Path
        `oasis_cross-sectional_disc<d>.tar.gz` or
        `oasis_cs_freesurfer_disc<d>.tar.gz` in the source folder
    """
    name = "cross-sectional" if kind == "raw" else "cs_freesurfer"
    return ctx.source / f"oasis_{name}_disc{disc}.tar.gz"


def raw_scan(name: str) -> tuple[str, str, int] | None:
    """
    Parse the name of a raw scan (first session only).

    Parameters
    ----------
    name : str
        Member of an image disc

    Returns
    -------
    (str, str, int) | None
        Subject label, session folder (`<disc>/OAS1_<sub>_MR1`) and run
        of `<folder>/RAW/OAS1_<sub>_MR1_mpr-<run>_anon.img`
    """
    match = re.fullmatch(
        r"(.+)/RAW/OAS1_(\d+)_MR1_mpr-(\d+)_anon\.img", name)
    if not match:
        return None
    folder, sub, run = match.groups()
    return f"{int(sub):04d}", folder, int(run)


def fs_subject(name: str, everything: bool = False) -> str | None:
    """
    Return the subject of a member of a FreeSurfer disc.

    Parameters
    ----------
    name : str
        Member `<disc>/OAS1_<sub>_MR<ses>/...` of a FreeSurfer disc
    everything : bool
        Every file of the tree (`fs-all`), else only those that BIDS
        describes

    Returns
    -------
    str | None
        Subject label (first session only)
    """
    if not everything and not name.endswith(fs.bidsifiable_outputs):
        return None
    parts = PurePosixPath(name).parts
    match = re.fullmatch(r"OAS1_(\d+)_MR1", parts[1]) \
        if len(parts) > 2 else None
    return f"{int(match[1]):04d}" if match else None


# ----------------------------------------------------------------------
#   Image discs
# ----------------------------------------------------------------------

def roast_image_disc(ctx: RoastContext, path: Path) -> Iterator[Action]:
    """
    Convert the scans of the subjects of an image disc (a shard).

    The disc is listed once, then its members are extracted in archive
    order: actions are sorted by the position of their first member, and
    each one reads its members in that order (a backward seek in a gz
    tarball decompresses it again from the start).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    path : Path
        The disc

    Yields
    ------
    Action
        Conversions (Analyze -> NIfTI), sidecars, missing members; a
        `Fail` if the disc cannot be read
    """
    try:
        # (closed on every path: Windows locks open files)
        with archives.reading(path, opener=TarArchive) as tar:
            index = tar.index()
            subjects = {}       # {sub: (folder, [runs])}, in archive order
            for name in index:
                scan = raw_scan(name)
                if scan is not None:
                    sub, folder, run = scan
                    subjects.setdefault(sub, (folder, []))[1].append(run)
            actions = []        # [(members, action)]
            for sub, (folder, runs) in subjects.items():
                if ctx.keeps_subject(sub):
                    actions += subject_actions(ctx, tar.archive, path, sub,
                                               folder, runs, index)
            yield from archives.in_order(actions)
    except archives.ArchiveError as e:
        yield e.fail(tag="discs")


def subject_actions(ctx: RoastContext, archive: TarArchive, path: Path,
                    sub: str, folder: str, runs: list[int],
                    index: dict[str, archives.Member]) -> list:
    """
    Return the actions of a subject of an image disc.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    archive : TarArchive
        The open disc
    path : Path
        Its path
    sub : str
        Subject label
    folder : str
        Folder of its first session in the disc
    runs : list[int]
        Its raw runs
    index : dict[str, roast.archives.Member]
        The regular files of the disc, by normalised name

    Returns
    -------
    list[tuple[list[roast.archives.Member], Action]]
        `(members it reads, action)`, in no particular order
        (`roast.archives.in_order`)
    """
    sidecar = ctx.resource(SIDECAR)
    base = read_json(sidecar)
    raw = ctx.layout.raw / f"sub-{sub}" / "anat"
    proc = ctx.layout.derivative(PROC_NAME).root / f"sub-{sub}" / "anat"
    out = []

    def convert(stem: str, dst: str, affine, tag: str, **kwargs) -> None:
        """Conversion of `<stem>.{hdr,img}`, or a `Fail` if missing."""
        names = [f"{stem}{ext}" for ext in (".hdr", ".img")]
        missing = [m for m in names if m not in index]
        if missing and ctx.json == "only":
            return              # (no data written: nothing is missing)
        if missing:
            out.append(([], Fail(f"{', '.join(missing)} not found in "
                                 f"{path.name}", name=Path(dst).name,
                                 tag=tag)))
            return
        members = sorted((index[m] for m in names), key=lambda m: m.offset)
        out.append((members, analyze_action(
            archive, path, [m.name for m in members], dst, affine, tag=tag,
            **kwargs)))

    def find_bias(stem: str, tag: str) -> tuple[str | None, str | None]:
        """Bias correction (N4, else N3) found in the disc."""
        for bias in ("n4", "n3"):
            if f"{stem.format(bias=bias)}.img" in index:
                return bias, stem.format(bias=bias)
        if ctx.json == "only":
            return None, None   # (no data written: nothing is missing)
        stem = stem.format(bias="{n4,n3}")
        out.append(([], Fail(f"{stem}.img not found in {path.name}",
                             tag=tag)))
        return None, None

    if ctx.wants("raw"):
        for run in runs:
            stem = f"{folder}/RAW/OAS1_{sub}_MR1_mpr-{run}_anon"
            dst = raw / f"sub-{sub}_run-{run}_T1w"
            out.append(([], CopyJSON(sidecar, f"{dst}.json", tag="raw")))
            convert(stem, f"{dst}.nii.gz", AFFINE_RAW, "raw")

    # T1w from the raw runs (bids:raw)
    sources = [f"bids:raw:sub-{sub}/anat/sub-{sub}_run-{run}_T1w.nii.gz"
               for run in runs]

    def t1w_sidecar(bias: str, skullstripped: bool = False) -> dict:
        """Sidecar of an average of the runs."""
        return {
            **base,
            "SkullStripped": skullstripped,
            "BiasCorrected": bias.upper(),
            "Resolution":
                "Resampled and averaged across runs (1mm, isotropic)",
            "Sources": sources,
        }

    mpr = f"{folder}/PROCESSED/MPRAGE"
    if ctx.wants("avg"):
        # average of the runs, 1mm
        bias, stem = find_bias(f"{mpr}/SUBJ_111/OAS1_{sub}_MR1_mpr_{{bias}}"
                               f"_anon_sbj_111", "avg")
        if stem:
            dst = proc / f"sub-{sub}_res-1mm_T1w"
            out.append(([], WriteJSON(t1w_sidecar(bias), f"{dst}.json",
                                      src=sidecar, tag="avg")))
            convert(stem, f"{dst}.nii.gz", AFFINE_AVG, "avg")

    if ctx.wants("tal"):
        # average of the runs, in Talairach space (with and without
        # skull)
        bias, stem = find_bias(f"{mpr}/T88_111/OAS1_{sub}_MR1_mpr_{{bias}}"
                               f"_anon_111_t88_gfc", "tal")
        if stem:
            dst = proc / f"sub-{sub}_space-Talairach_res-1mm"
            out.append(([], WriteJSON(t1w_sidecar(bias), f"{dst}_T1w.json",
                                      src=sidecar, tag="tal")))
            convert(stem, f"{dst}_T1w.nii.gz", AFFINE_TAL, "tal")
            out.append(([], WriteJSON(
                t1w_sidecar(bias, True),
                f"{dst}_desc-skullstripped_T1w.json", src=sidecar,
                tag="tal")))
            convert(stem[:-3] + "masked_gfc",
                    f"{dst}_desc-skullstripped_T1w.nii.gz", AFFINE_TAL,
                    "tal")

    if ctx.wants("fsl"):
        # FSL segmentation of the skull-stripped Talairach scan
        _, stem = find_bias(f"{folder}/FSL_SEG/OAS1_{sub}_MR1_mpr_{{bias}}"
                            f"_anon_111_t88_masked_gfc_fseg", "fsl")
        if stem:
            dst = proc / f"sub-{sub}_space-Talairach_res-1mm_dseg"
            flags = "space-Talairach_res-1mm_desc-skullstripped"
            meta = {
                "Manual": False,
                "Resolution": "In the space of the 1mm Talairach T1w "
                              "scan (1mm, isotropic)",
                "Sources": [f"bids::sub-{sub}/anat/"
                            f"sub-{sub}_{flags}_T1w.nii.gz"],
            }
            out.append(([], WriteJSON(meta, f"{dst}.json", src=sidecar,
                                      tag="fsl")))
            convert(stem, f"{dst}.nii.gz", AFFINE_TAL, "fsl", relabel=True)
    return out


def analyze_action(archive: TarArchive, tarball: Path, members: list[str],
                   dst: str, affine: np.ndarray, *, relabel: bool = False,
                   tag: str | None = None) -> Action:
    """
    Convert an Analyze image of an open disc to NIfTI.

    Parameters
    ----------
    archive : TarArchive
        Open disc (it stays open until the action has run)
    tarball : Path
        Its path (source of the action)
    members : list[str]
        Its `.hdr` and `.img` members, in the order they are read
        (archive order)
    dst : str
        Output file (`.nii.gz`)
    affine : np.ndarray
        4x4 orientation matrix
    relabel : bool
        FSL segmentation: relabel its classes (`FSL_RELABEL`, `{new:
        old}`: CSF, GM, WM (1, 2, 3) become 3, 1, 2, see `DSEG_TABLE`)
    tag : str
        Progress group

    Returns
    -------
    Action
        The conversion
    """
    def convert(path: Path) -> None:
        """Unpack the image in a temporary folder, then convert it."""
        with TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            for member in members:
                with archive.extractfile(member) as f:
                    write_from_buffer(f, tmp / f"tmp{Path(member).suffix}")
            nibabel_convert(tmp / "tmp.img", path, affine=affine,
                            inp_format=nib.AnalyzeImage)
        if relabel:
            img = nib.load(path)
            data = vol_relabel(np.asarray(img.dataobj), FSL_RELABEL)
            nib.save(type(img)(data, img.affine, img.header), path)

    return Action(tarball, Path(dst), convert, input="path", tag=tag)


# ----------------------------------------------------------------------
#   FreeSurfer discs
# ----------------------------------------------------------------------

def roast_freesurfer_disc(ctx: RoastContext,
                          path: Path) -> Iterator[Action]:
    """
    Unpack and roast the FreeSurfer trees of a disc (a shard).

    The trees are unpacked in `derivatives/<fs>/sourcedata/` in one pass
    (only the files that BIDS describes, unless `fs-all`), then
    converted. With `--sidecars only` (and not `fs-all`), the sidecars
    are written from the member names, and nothing is unpacked.

    The raw runs that a tree was made from (its `Sources`) are its input
    volumes `mri/orig/<NNN>.mgz` (run N, the order of `recon-all -i`),
    seen in the same pass; for a tree without them, the runs of the
    subject in the image disc of the same number (listed only then).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    path : Path
        The disc

    Yields
    ------
    Action
        Unpacking (`staging`), conversions and sidecars
    """
    drv = ctx.layout.derivative(FS_NAME)
    everything = ctx.wants("fs-all")
    subjects = set()
    runs = {}               # {sub: {run}} from `mri/orig/<NNN>.mgz`

    def rename(member: str) -> Path | None:
        """Path of an unpacked member (None: not unpacked)."""
        match = re.fullmatch(r"[^/]+/OAS1_(\d+)_MR1/mri/orig/(\d{3})\.mgz",
                             member)
        if match:
            runs.setdefault(f"{int(match[1]):04d}", set()).add(
                int(match[2]))
        # (every subject is reported, before `everything` prunes it)
        sub = fs_subject(member, True)
        if sub is None or not ctx.keeps_subject(sub):
            return None
        if not everything and fs_subject(member) is None:
            return None
        subjects.add(sub)
        return drv.source.joinpath(f"sub-{sub}",
                                   *PurePosixPath(member).parts[2:])

    exists = None
    if ctx.json == "only" and not everything:
        try:
            members = set(map(rename, archives.names(path,
                                                     opener=TarArchive)))
        except archives.ArchiveError as e:
            yield e.fail(tag="discs")
            return
        exists = members.__contains__
    else:
        # unpacked files are inputs of the conversion (`staging`: the
        # JSON policy does not apply to them)
        yield ExtractMembers(path, Claim(drv.source), rename, tag="fs",
                             staging=True)
    # (converted once unpacked: the engine runs each action before
    # asking for the next one)
    if any(sub not in runs for sub in subjects):
        for sub, found in image_runs(ctx, path).items():
            runs.setdefault(sub, set(found))
    for sub in sorted(subjects):
        if sub not in runs:
            lg.warning(f"sub-{sub}: raw runs unknown ({path.name} has no "
                       f"mri/orig/ volumes, no image disc lists them)")
        t1w = [f"bids:raw:sub-{sub}/anat/sub-{sub}_run-{run}_T1w.nii.gz"
               for run in sorted(runs.get(sub, ()))]
        for action in fs.bidsify(drv.source / f"sub-{sub}",
                                 drv.root / f"sub-{sub}", t1w,
                                 json=ctx.json, exists=exists):
            action.tag = "fs"
            yield action


def image_runs(ctx: RoastContext, fs_disc: Path) -> dict[str, list[int]]:
    """
    Return the raw runs of the subjects of an image disc.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    fs_disc : Path
        A FreeSurfer disc: the image disc of the same number is listed

    Returns
    -------
    dict[str, list[int]]
        `{sub: [runs]}` (empty if the image disc is missing or
        unreadable)
    """
    disc = int(re.search(r"disc(\d+)\.tar\.gz$", fs_disc.name)[1])
    path = disc_path(ctx, "raw", disc)
    out = {}
    try:
        names = archives.names(path, opener=TarArchive)
    except archives.ArchiveError:       # (missing, or unreadable)
        return out
    for scan in map(raw_scan, names):
        if scan is not None:
            out.setdefault(scan[0], []).append(scan[2])
    return out


# ----------------------------------------------------------------------
#   Participants
# ----------------------------------------------------------------------

PARTICIPANTS_HEADER = [
    "participant_id",
    "sex",
    "handedness",
    "age",
    "educ",
    "ses",
    "mmse",
    "cdr",
    "etiv",
    "nwbv",
    "asf",
]


def roast_meta(ctx: RoastContext) -> Iterator[Action]:
    """
    Write the dataset files and the participants table.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action
        Copies of templates, participants table (complete, in
        `.brainspresso/orig/`)
    """
    raw = ctx.layout.raw
    yield from copy_templates(ctx, ["README", "dataset_description.json",
                                    "participants.json"])
    xlsx = ctx.source / XLSX
    if not xlsx.exists():
        yield Fail(f"sourcedata/{XLSX} not found: harvest with --keys meta",
                   name="participants.tsv", tag="meta")
        return
    yield Action(
        xlsx, ctx.layout.original(raw / "participants.tsv"),
        partial(write_participants, xlsx), mode="t", input="path",
        tag="meta",
    )


def write_participants(path_xlsx: Path, path_tsv: Path) -> None:
    """
    Write the participants table from the OASIS spreadsheet.

    Repeat sessions (`MR2`) are dropped, and so is the `Delay` column
    (time to the repeat). Empty and `N/A` cells are `n/a`; rows without
    an ID (blank or formatted trailing rows) are skipped.

    Parameters
    ----------
    path_xlsx : Path
        `oasis_cross-sectional.xlsx`
    path_tsv : Path
        Output table
    """
    import openpyxl  # noqa: PLC0415  (lazy: optional dependency)

    book = openpyxl.load_workbook(path_xlsx, data_only=True)
    sheet = book[book.sheetnames[0]]

    def iter_rows():
        """Header, then one row per subject."""
        yield PARTICIPANTS_HEADER
        for nrow in range(2, sheet.max_row + 1):
            row = sheet[nrow]
            if not str(row[0].value or "").strip():
                continue                # blank or formatted trailing row
            values = [row[i].value for i in range(1, 11)]
            values = ["n/a" if v in (None, "", "N/A") else v for v in values]
            _, sub, ses = str(row[0].value).strip().split("_")
            if int(ses[2:]) == 2:
                continue
            yield [f"sub-{int(sub):04d}", *values]

    write_tsv(iter_rows(), path_tsv)
