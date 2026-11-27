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

A disc holds the images of many subjects: the top-level roaster lists
the discs (`--discs`, a full decompression of each tarball) to find
their subjects, then yields one shard per subject, which reads its
disc(s). Only the first session (`MR1`) of each subject is converted
(`MR2` are repeats). The participants table is written complete to
`.brainspresso/orig/` and trimmed to the subjects on disk.
"""  # noqa: E501
import re
from functools import partial
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Iterator

import nibabel as nib
import numpy as np

from brainspresso.actions import Action, Claim, CopyJSON, Fail, WriteJSON
from brainspresso.actions.archive import ExtractMembers
from brainspresso.bids.keys import KeyAliases
from brainspresso.freesurfer import bidsify as fs
from brainspresso.roast import (
    RoastContext,
    as_list,
    copy_templates,
    roast_dataset,
    subject_shards,
)
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.io import (
    nibabel_convert,
    read_json,
    write_from_buffer,
    write_tsv,
)
from brainspresso.utils.vol import make_affine
from brainspresso.utils.vol import relabel as vol_relabel

TEMPLATES = Path(__file__).parent / "templates"
PROC_NAME = "oasis-processed"
FS_NAME = "oasis-freesurfer"
FS_VERSION = (4, 0)
XLSX = "oasis_cross-sectional.xlsx"

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


def roast(root, *, subs=(), exclude_subs=(), discs=None, **options):
    """
    Roast `<root>` (the `OASIS-1` folder).

    Parameters
    ----------
    root : path
        Dataset folder
    subs, exclude_subs : list[int | str]
        Subject numbers (or labels)
    discs : list[int]
        Only these discs (default: all)
    **options
        Those of the command line (`keys`, `exclude_keys`, `json`,
        `ifexists`, `layout`), `jobs` and `display`

    Returns
    -------
    RoastReport
        See `roast.run_roast`
    """
    return roast_dataset(
        roast_oasis1, root, KEYS, templates=TEMPLATES,
        subs=as_list(subs, "{:04d}"),
        exclude_subs=as_list(exclude_subs, "{:04d}"),
        options={"discs": sorted(discs)} if discs else None, **options,
    )


def roast_oasis1(ctx: RoastContext) -> Iterator:
    """
    Roast OASIS-I: metadata, then one shard per subject.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast (`ctx.options["discs"]`: discs to read)

    Yields
    ------
    Action | Shard
        Metadata actions, then subject shards
    """
    if ctx.wants("meta"):
        yield from roast_meta(ctx)
    if ctx.wants("fs") or ctx.wants("fs-all"):
        drv = ctx.layout.derivative(FS_NAME)
        for action in fs.bidsify_toplevel(drv.root, FS_VERSION):
            action.tag = "fs"
            yield action
    archives = {}
    for kind in DISCS:
        if any(map(ctx.wants, DISC_KEYS[kind])):
            yield from list_discs(ctx, kind, archives)
    yield from subject_shards(
        ctx, sorted(archives), roast_subject, archives,
        inputs=lambda sub: sorted(archives[sub].values()),
    )


# ----------------------------------------------------------------------
#   Discs
# ----------------------------------------------------------------------

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


def list_discs(ctx: RoastContext, kind: str,
               archives: dict[str, dict[str, Path]]) -> Iterator[Fail]:
    """
    Find the subjects of the selected discs of one kind.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast (`ctx.options["discs"]`)
    kind : {"raw", "fs"}
        Images or FreeSurfer trees
    archives : dict[str, dict[str, Path]]
        `{sub: {kind: disc}}`, filled in place

    Yields
    ------
    Fail
        A missing disc
    """
    selected = set(ctx.options.get("discs") or DISCS[kind])
    for disc in DISCS[kind]:
        if disc not in selected:
            continue
        path = disc_path(ctx, kind, disc)
        if not path.exists():
            yield Fail(f"{path.name} not found", name=path.name, tag=kind)
            continue
        with TarArchive(path) as archive:
            names = archive.getnames()
        everything = ctx.wants("fs-all")
        for name in names:
            sub = (raw_subject(name) if kind == "raw" else
                   fs_subject(name, everything))
            if sub is not None:
                archives.setdefault(sub, {})[kind] = path


def raw_subject(name: str) -> str | None:
    """
    Return the subject of a raw scan (first session only).

    Parameters
    ----------
    name : str
        Member of an image disc

    Returns
    -------
    str | None
        Subject label of `.../RAW/OAS1_<sub>_MR1_mpr-<run>_anon.img`
    """
    match = re.search(r"/RAW/OAS1_(\d+)_MR1_mpr-\d+_anon\.img$", name)
    return f"{int(match[1]):04d}" if match else None


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
#   Subjects
# ----------------------------------------------------------------------

def roast_subject(ctx: RoastContext, sub: str,
                  archives: dict[str, dict[str, Path]]) -> Iterator[Action]:
    """
    Roast the images and FreeSurfer tree of a subject (a shard).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label
    archives : dict[str, dict[str, Path]]
        `{sub: {"raw" | "fs": disc}}`

    Yields
    ------
    Action
        Conversions and sidecars
    """
    discs = archives[sub]
    if "raw" in discs:
        yield from roast_images(ctx, sub, discs["raw"])
    if "fs" in discs:
        yield from roast_freesurfer(ctx, sub, discs["fs"])


def roast_images(ctx: RoastContext, sub: str,
                 tarball: Path) -> Iterator[Action]:
    """
    Convert the scans of a subject (raw, avg, tal and fsl keys).

    Actions are yielded in the order of their members in the disc, so
    that a compressed disc is read forward.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label
    tarball : Path
        Image disc of the subject

    Yields
    ------
    Action
        Conversions (Analyze -> NIfTI) and sidecars
    """
    with TarArchive(tarball) as archive:
        names = archive.getnames()
        found = set(names)
        folder = session_folder(names, sub)
        runs = [int(m[1]) for m in (
            re.fullmatch(f"{re.escape(folder)}/RAW/OAS1_{sub}_MR1_"
                         r"mpr-(\d+)_anon\.img", name) for name in names
        ) if m]
        convert = partial(analyze_action, archive, tarball)
        sidecar = ctx.resource("T1w.json")
        base = read_json(sidecar)
        raw = ctx.layout.raw / f"sub-{sub}" / "anat"
        proc = ctx.layout.derivative(PROC_NAME).root / f"sub-{sub}" / "anat"
        actions = []        # (member, action)

        if ctx.wants("raw"):
            for run in runs:
                stem = f"{folder}/RAW/OAS1_{sub}_MR1_mpr-{run}_anon"
                out = raw / f"sub-{sub}_run-{run}_T1w"
                actions += [
                    (None, CopyJSON(sidecar, f"{out}.json", tag="raw")),
                    (stem, convert(stem, f"{out}.nii.gz", AFFINE_RAW,
                                   tag="raw")),
                ]

        def find_bias(stem: str, tag: str):
            """Bias correction (N4, else N3) found in the disc."""
            for bias in ("n4", "n3"):
                if f"{stem.format(bias=bias)}.img" in found:
                    return bias, stem.format(bias=bias)
            stem = stem.format(bias="{n4,n3}")
            actions.append((None, Fail(f"{stem}.img not found in "
                                       f"{tarball.name}", tag=tag)))
            return None, None

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
            n, stem = find_bias(f"{mpr}/SUBJ_111/OAS1_{sub}_MR1_mpr_{{bias}}"
                           f"_anon_sbj_111", "avg")
            if stem:
                out = proc / f"sub-{sub}_res-1mm_T1w"
                actions += [
                    (None, WriteJSON(t1w_sidecar(n), f"{out}.json",
                                     src=sidecar, tag="avg")),
                    (stem, convert(stem, f"{out}.nii.gz", AFFINE_AVG,
                                   tag="avg")),
                ]

        if ctx.wants("tal"):
            # average of the runs, in Talairach space (with and
            # without skull)
            n, stem = find_bias(f"{mpr}/T88_111/OAS1_{sub}_MR1_mpr_{{bias}}"
                           f"_anon_111_t88_gfc", "tal")
            if stem:
                out = proc / f"sub-{sub}_space-Talairach_res-1mm"
                masked = stem[:-3] + "masked_gfc"
                actions += [
                    (None, WriteJSON(t1w_sidecar(n), f"{out}_T1w.json",
                                     src=sidecar, tag="tal")),
                    (stem, convert(stem, f"{out}_T1w.nii.gz", AFFINE_TAL,
                                   tag="tal")),
                    (None, WriteJSON(t1w_sidecar(n, True),
                                     f"{out}_desc-skullstripped_T1w.json",
                                     src=sidecar, tag="tal")),
                    (masked, convert(masked,
                                     f"{out}_desc-skullstripped_T1w.nii.gz",
                                     AFFINE_TAL, tag="tal")),
                ]

        if ctx.wants("fsl"):
            # FSL segmentation of the skull-stripped Talairach scan
            _, stem = find_bias(f"{folder}/FSL_SEG/OAS1_{sub}_MR1_mpr_{{bias}}"
                           f"_anon_111_t88_masked_gfc_fseg", "fsl")
            if stem:
                out = proc / f"sub-{sub}_space-Talairach_res-1mm_dseg"
                flags = "space-Talairach_res-1mm_desc-skullstripped"
                meta = {
                    "Manual": "False",
                    "Resolution": "In the space of the 1mm Talairach T1w "
                                  "scan (1mm, isotropic)",
                    "Sources": [f"bids::sub-{sub}/anat/"
                                f"sub-{sub}_{flags}_T1w.nii.gz"],
                }
                actions += [
                    (None, WriteJSON(meta, f"{out}.json", src=sidecar,
                                     tag="fsl")),
                    (stem, convert(stem, f"{out}.nii.gz", AFFINE_TAL,
                                   relabel=True, tag="fsl")),
                ]

        def offset(item) -> int:
            """Position of the member of an action (-1: none)."""
            member, _ = item
            if member is None:
                return -1
            return archive.getmember(f"{member}.img").offset

        for _, action in sorted(actions, key=offset):
            yield action


def session_folder(names: list[str], sub: str) -> str:
    """
    Return the folder of the first session of a subject in a disc.

    Parameters
    ----------
    names : list[str]
        Members of the disc
    sub : str
        Subject label

    Returns
    -------
    str
        `<disc>/OAS1_<sub>_MR1`
    """
    session = f"OAS1_{sub}_MR1"
    for name in names:
        parts = PurePosixPath(name).parts
        if session in parts[:-1]:
            return "/".join(parts[:parts.index(session) + 1])
    return session


def analyze_action(archive: TarArchive, tarball: Path, stem: str,
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
    stem : str
        Member, without `.img` / `.hdr`
    dst : str
        Output file (`.nii.gz`)
    affine : np.ndarray
        4x4 orientation matrix
    relabel : bool
        FSL segmentation: relabel its classes, with the lookup table
        `{1: 2, 2: 3, 3: 1}` of the legacy roaster (`utils.vol.relabel`:
        `{new: old}`, so 1, 2, 3 become 3, 1, 2)
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
            for ext in (".img", ".hdr"):
                with archive.extractfile(f"{stem}{ext}") as f:
                    write_from_buffer(f, tmp / f"tmp{ext}")
            nibabel_convert(tmp / "tmp.img", path, affine=affine,
                            inp_format=nib.AnalyzeImage)
        if relabel:
            img = nib.load(path)
            data = vol_relabel(np.asarray(img.dataobj), {1: 2, 2: 3, 3: 1})
            nib.save(type(img)(data, img.affine, img.header), path)

    return Action(tarball, Path(dst), convert, input="path", tag=tag)


# ----------------------------------------------------------------------
#   FreeSurfer
# ----------------------------------------------------------------------

def roast_freesurfer(ctx: RoastContext, sub: str,
                     tarball: Path) -> Iterator[Action]:
    """
    Unpack and roast the FreeSurfer tree of a subject.

    The tree is unpacked in `derivatives/<fs>/sourcedata/` (only its
    files that BIDS describes, unless `fs-all`), then converted. With
    `--sidecars only` (and not `fs-all`), the sidecars are written from
    the member names, and nothing is unpacked.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label
    tarball : Path
        FreeSurfer disc of the subject

    Yields
    ------
    Action
        Unpacking (`staging`), conversions and sidecars
    """
    drv = ctx.layout.derivative(FS_NAME)
    src = drv.source / f"sub-{sub}"
    everything = ctx.wants("fs-all")

    def rename(member: str) -> Path | None:
        """Path of an unpacked member (None: not unpacked)."""
        if fs_subject(member, everything) != sub:
            return None
        return src.joinpath(*PurePosixPath(member).parts[2:])

    exists = None
    if ctx.json == "only" and not everything:
        with TarArchive(tarball) as archive:
            members = set(map(rename, archive.getnames()))
        exists = members.__contains__
    else:
        # unpacked files are inputs of the conversion (`staging`: the
        # JSON policy does not apply to them)
        yield ExtractMembers(tarball, Claim(src), rename, tag="fs",
                             staging=True)
    # (converted once unpacked: the engine runs each action before
    # asking for the next one)
    t1w = [f"bids:raw:sub-{sub}/anat/sub-{sub}_run-{run}_T1w.nii.gz"
           for run in (1, 2, 3, 4)]
    for action in fs.bidsify(src, drv.root / f"sub-{sub}", t1w,
                             json=ctx.json, exists=exists):
        action.tag = "fs"
        yield action


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
    (time to the repeat). Empty and `N/A` cells are `n/a`.

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
            values = [row[i].value for i in range(1, 11)]
            values = ["n/a" if v in (None, "", "N/A") else v for v in values]
            _, sub, ses = row[0].value.split("_")
            if int(ses[2:]) == 2:
                continue
            yield [f"sub-{int(sub):04d}", *values]

    write_tsv(iter_rows(), path_tsv)
