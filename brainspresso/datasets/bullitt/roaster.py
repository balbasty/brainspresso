"""
Bullitt healthy MR database: convert the harvested MetaImage files to BIDS.

Expected input
--------------
Bullitt/sourcedata/                 (the Girder download, `harvest.py`)
    Normal-<NNN>/T1-Flash/*.mha     (also "T!-Flash")
    Normal-<NNN>/T1-MPRage/*.mha    (also "MPRage")
    Normal-<NNN>/T2/*.mha
    Normal-<NNN>/MRA/*.mha
    Normal-<NNN>/AuxillaryData/SkullStripped-T1-Flash.mha
    Normal-<NNN>/AuxillaryData/VascularNetwork.tre

Output (study layout)
---------------------
Bullitt/
    dataset_description.json        (DatasetType "study")
    rawbids/
        README, dataset_description.json, participants.{tsv,json}
        sub-<NNN>/anat/sub-<NNN>_{acq-flash_T1w,acq-mprage_T1w,T2w,angio}.{nii.gz,json}
    derivatives/bullitt/            (SourceDatasets: ../../rawbids/)
        README, dataset_description.json, .bidsignore
        sub-<NNN>/anat/sub-<NNN>_acq-flash_desc-brain_T1w.{nii.gz,json}
        sub-<NNN>/anat/sub-<NNN>_desc-vessels_tubes.tre

With `--layout flat`, the content of `rawbids/` is in `Bullitt/` itself.

Each subject is a folder of small files: one shard per subject (task id
`roast/subjects/sub-<NNN>`; inputs: its files and the sidecar
templates, so that an unchanged subject is skipped on a re-run). The
source distributes scans incompletely (no MPRAGE for many subjects, see
`participants.tsv`): a missing scan is an error only when `--keys`
names its key (or a group above it). The participants table (a
template transcribed from the PDF of the dataset) is written complete
to `.brainspresso/orig/` and trimmed to the subjects on disk. DTI is not
converted (no gradient table).
"""  # noqa: E501
import re
from logging import getLogger
from pathlib import Path
from typing import Iterator

from brainspresso.actions import (
    Action,
    CopyBytes,
    CopyJSON,
    Fail,
    WriteJSON,
    WriteTSV,
)
from brainspresso.bids.keys import KeyAliases
from brainspresso.datasets.base import KeySpec
from brainspresso.roast import (
    RoastContext,
    as_list,
    copy_templates,
    roast_dataset,
    subject_shards,
)
from brainspresso.sources.base import subject_labels
from brainspresso.utils.metaimage import metaimage_to_nifti
from brainspresso.utils.path import sorted_paths

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"
WIDTH = 3                   # subject labels: `sub-001`
DERIV_NAME = "bullitt"      # derivatives/bullitt/

#: Subject sub-folder (source) -> key
FOLDER_KEYS = {
    "T1-Flash": "flash", "T!-Flash": "flash",
    "T1-MPRage": "mprage", "MPRage": "mprage",
    "T2": "T2w", "MRA": "angio", "DTI": "dwi",
}
#: AuxillaryData file name prefix -> key
AUX_KEYS = {"SkullStripped": "skullstripped", "VascularNetwork": "vessels"}
#: Raw key -> BIDS entities and suffix (also the name of its template)
RAW = {
    "flash": "acq-flash_T1w",
    "mprage": "acq-mprage_T1w",
    "T2w": "T2w",
    "angio": "angio",
}
#: Derivative key -> (AuxillaryData file, output entities and suffix)
DERIVATIVES = {
    "skullstripped": ("SkullStripped-T1-Flash.mha",
                      "acq-flash_desc-brain_T1w.nii.gz"),
    "vessels": ("VascularNetwork.tre", "desc-vessels_tubes.tre"),
}

#: Keys of the roast (DTI and phantoms are harvested only)
TREE = {
    "meta": {},
    "raw": {"anat": {"T1w": {"flash", "mprage"}, "T2w": {}, "angio": {}}},
    "derivatives": {"skullstripped": {}, "vessels": {}},
}
#: Leaves, the default of `--keys` (all)
ROAST_KEYS = ("meta", "flash", "mprage", "T2w", "angio", "skullstripped",
              "vessels")
KEYS = KeyAliases(
    TREE,
    bids={
        "flash": {"suffixes": ["T1w"], "entities": {"acq": ["flash"]}},
        "mprage": {"suffixes": ["T1w"], "entities": {"acq": ["mprage"]}},
        "T2w": {"suffixes": ["T2w"]},
        "angio": {"suffixes": ["angio"]},
    },
    folders={key: [f"derivatives/{DERIV_NAME}"] for key in DERIVATIVES},
)
#: Values of `--keys`: groups and leaves
ROAST_SPEC = KeySpec(KEYS, ("meta", "raw", "anat", "T1w", "flash", "mprage",
                            "T2w", "angio", "derivatives", "skullstripped",
                            "vessels"), ROAST_KEYS)

PARTICIPANTS = "participants.tsv"
PARTICIPANT_ID = re.compile(r"(?:sub-)?(\d{3})")


def roast(root, *, subs=(), exclude_subs=(), keys=(), exclude_keys=(),
          **options):
    """
    Roast `<root>` (the `Bullitt` folder).

    Parameters
    ----------
    root : path
        Dataset folder
    subs, exclude_subs : list[int | str]
        Subjects: numbers (`1` is `sub-001`), labels with or without
        `sub-`, ranges, files (as `--subs`)
    keys, exclude_keys : str | list[str]
        `--keys`, `--exclude-keys` (a name or a list); a missing scan is
        an error only for the keys named here
    **options
        Those of the command line (`json`, `ifexists`, `layout`), `jobs`
        and `display`

    Returns
    -------
    RoastReport
        See `roast.run_roast`
    """
    keys, exclude_keys = as_list(keys), as_list(exclude_keys)
    select, exclude = ROAST_SPEC.effective(keys, exclude_keys)
    return roast_dataset(
        roast_bullitt, root, KEYS, templates=TEMPLATES,
        keys=select, exclude_keys=exclude,
        subs=sorted(subject_labels(subs, WIDTH)),
        exclude_subs=sorted(subject_labels(exclude_subs, WIDTH)),
        options=roast_options(keys), **options,
    )


def roast_options(keys) -> dict:
    """
    Return the options of the roaster (`RoastContext.options`).

    Parameters
    ----------
    keys : str | list[str]
        `--keys` as given (not `KeySpec.effective`, which turns keys
        that cover every choice into "all")

    Returns
    -------
    dict
        `named`: the leaves that `--keys` names, itself or through a
        group above them (any case); a missing scan of these keys is an
        error. Nothing is named without `--keys` or with its CLI
        default (the seven leaves: the command line passes them when
        the flag is absent, so they cannot be told apart)
    """
    keys = as_list(keys)
    if not keys or {str(k).lower() for k in keys} == \
            {k.lower() for k in ROAST_SPEC.default}:
        return {"named": []}
    return {"named": sorted(leaf for leaf in (*RAW, *DERIVATIVES)
                            if ROAST_SPEC.selects(keys, leaf))}


def subject_label(folder: str) -> str | None:
    """
    Return the label of a subject folder.

    Parameters
    ----------
    folder : str
        Name of a folder of the source (`Normal-001`)

    Returns
    -------
    str | None
        `001`, or None if it is not a subject folder
    """
    if not folder.startswith("Normal-"):
        return None
    return folder[len("Normal-"):]


def subject_folders(ctx: RoastContext) -> dict[str, Path]:
    """
    Find the subject folders of the source.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Returns
    -------
    dict[str, Path]
        `{label: folder}`; folders `Normal-<label>` whose label is not 3
        digits are skipped with a warning
    """
    out = {}
    if not ctx.source.is_dir():
        return out
    for path in sorted_paths(ctx.source.iterdir()):
        label = subject_label(path.name)
        if label is None or not path.is_dir():
            continue
        if not re.fullmatch(r"\d{3}", label):
            lg.warning(f"{path.name}: not a subject folder "
                       f"(Normal-<NNN>; skipped)")
            continue
        out[label] = path
    return out


def roast_bullitt(ctx: RoastContext) -> Iterator:
    """
    Roast Bullitt: metadata, then one shard per subject.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast (`ctx.options["named"]`: keys named by
        `--keys`)

    Yields
    ------
    Action | Shard
        Metadata actions, then subject shards
    """
    if ctx.wants("meta"):
        yield from roast_meta(ctx)
    if any(map(ctx.wants, DERIVATIVES)):
        drv = ctx.layout.derivative(DERIV_NAME)
        yield from copy_templates(ctx, {
            "derivatives_description.json": drv.description,
            "derivatives_README": drv.root / "README",
            "derivatives_bidsignore": drv.bidsignore,
        }, tag="derivatives")
    if not any(map(ctx.wants, (*RAW, *DERIVATIVES))):
        return
    folders = subject_folders(ctx)
    if not folders:
        yield Fail("no subject folder (Normal-<NNN>) in sourcedata/: "
                   "harvest first", name="sourcedata", tag="subjects")
        return
    # (`named` decides which missing scans are errors: a later run that
    # names more keys must not skip the shards)
    yield from subject_shards(
        ctx, folders, roast_subject,
        inputs=lambda label: shard_inputs(ctx, folders[label]),
        config={"named": sorted(ctx.options.get("named", ()))},
        scope_keys=(*RAW, *DERIVATIVES),
    )


def raw_image(folder: Path, key: str) -> Path | None:
    """
    Find the image of a raw key in a subject folder.

    Parameters
    ----------
    folder : Path
        Folder of the subject (`Normal-<NNN>`)
    key : str
        Raw key (`RAW`)

    Returns
    -------
    Path | None
        The first `.mha` of the folders of the key (with a warning if
        there are several), or None
    """
    images = sorted(
        image for name, k in FOLDER_KEYS.items() if k == key
        for image in (folder / name).glob("*.mha") if image.is_file())
    if len(images) > 1:
        lg.warning(f"{folder.name}: {len(images)} {key} images, "
                   f"{images[0].name} is converted")
    return images[0] if images else None


def shard_inputs(ctx: RoastContext, folder: Path) -> list[Path]:
    """
    Return the inputs of a subject shard (its input signature).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    folder : Path
        Folder of the subject

    Returns
    -------
    list[Path]
        The files its actions read, for the wanted keys: images, the
        sidecar templates (when sidecars are written), auxiliary files
    """
    out = []
    for key, suffix in RAW.items():
        if not ctx.wants(key):
            continue
        image = raw_image(folder, key)
        if image is None:
            continue
        out.append(image)
        template = ctx.resource(f"{suffix}.json")
        if template is not None and ctx.accepts("sidecar"):
            out.append(template)
    for key, (name, _) in DERIVATIVES.items():
        src = folder / "AuxillaryData" / name
        if ctx.wants(key) and src.is_file():
            out.append(src)
    return out


# ----------------------------------------------------------------------
#   Metadata
# ----------------------------------------------------------------------

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
        Copies of the templates, the participants table (complete, in
        `.brainspresso/orig/`)
    """
    raw = ctx.layout.raw
    yield from copy_templates(ctx, ["README", "dataset_description.json",
                                    "participants.json"])
    src = ctx.resource(PARTICIPANTS)
    if src is None:
        yield Fail(f"{PARTICIPANTS} not found (in {ctx.source} or in the "
                   f"templates of the dataset)", name=PARTICIPANTS,
                   tag="meta")
        return
    try:
        rows = participants(src)
    except (OSError, UnicodeDecodeError, ValueError) as e:
        yield Fail(f"{src} cannot be read ({type(e).__name__}: {e})",
                   name=PARTICIPANTS, tag="meta")
        return
    yield WriteTSV(rows, ctx.layout.original(raw / PARTICIPANTS), src=src,
                   tag="meta")


def participants(path: Path) -> list[list[str]]:
    """
    Read the participants table (template).

    Parameters
    ----------
    path : Path
        `participants.tsv`

    Returns
    -------
    list[list[str]]
        Header and rows: `participant_id` with `sub-` and 3 digits,
        empty cells `n/a`; blank rows are skipped, rows whose ID cannot
        be read or whose number of cells differs from the header are
        skipped with a warning

    Raises
    ------
    UnicodeDecodeError
        If it is not UTF-8
    ValueError
        If it has no header (`participant_id` first)
    """
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    header = [cell.strip() for cell in lines[0].split("\t")] \
        if lines else []
    if not header or header[0] != "participant_id":
        raise ValueError("no header (participant_id, ...)")
    rows = []
    for nrow, line in enumerate(lines[1:], 2):
        if not line.strip():
            continue
        row = [cell.strip() or "n/a" for cell in line.split("\t")]
        match = PARTICIPANT_ID.fullmatch(row[0])
        if not match or len(row) != len(header):
            lg.warning(f"{path.name}, row {nrow}: {row[0]!r} with "
                       f"{len(row)} cells (skipped)")
            continue
        rows.append([f"sub-{match[1]}", *row[1:]])
    return [header, *rows]


# ----------------------------------------------------------------------
#   Images (MetaImage -> NIfTI)
# ----------------------------------------------------------------------

def roast_subject(ctx: RoastContext, sub: str) -> Iterator[Action]:
    """
    Convert the scans and auxiliary data of a subject (a shard).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label (`001`)

    Yields
    ------
    Action
        Conversions, sidecars, copies of vascular trees, missing inputs
    """
    folder = ctx.source / f"Normal-{sub}"
    named = set(ctx.options.get("named", ()))
    anat = ctx.layout.raw / f"sub-{sub}" / "anat"
    for key, suffix in RAW.items():
        if not ctx.wants(key):
            continue
        dst = anat / f"sub-{sub}_{suffix}"
        image = raw_image(folder, key)
        if image is None:
            if key in named and ctx.accepts("data"):
                yield Fail(f"Normal-{sub}: no {key} image (*.mha)",
                           name=f"{dst.name}.nii.gz", tag="subjects")
            else:
                lg.info(f"Normal-{sub}: no {key} image")
            continue
        yield convert(image, f"{dst}.nii.gz")
        template = ctx.resource(f"{suffix}.json")
        if template is not None:
            yield CopyJSON(template, f"{dst}.json", tag="subjects")
        elif ctx.accepts("sidecar"):
            yield Fail(f"{suffix}.json not found (sidecar template)",
                       name=f"{dst.name}.json", tag="subjects")

    out = ctx.layout.derivative(DERIV_NAME).root / f"sub-{sub}" / "anat"
    for key, (name, suffix) in DERIVATIVES.items():
        if not ctx.wants(key):
            continue
        src = folder / "AuxillaryData" / name
        dst = out / f"sub-{sub}_{suffix}"
        if not src.is_file():
            if key in named and ctx.accepts("data"):
                yield Fail(f"Normal-{sub}: AuxillaryData/{name} not found",
                           name=dst.name, tag="subjects")
            else:
                lg.info(f"Normal-{sub}: no AuxillaryData/{name}")
            continue
        if key == "vessels":
            yield CopyBytes(src, dst, tag="subjects")
            continue
        yield convert(src, dst)
        sidecar = dst.parent / dst.name.replace(".nii.gz", ".json")
        yield WriteJSON({"SkullStripped": True}, sidecar, src=src,
                        tag="subjects")


def convert(src: Path, dst) -> Action:
    """
    Convert a MetaImage file to NIfTI.

    Parameters
    ----------
    src : Path
        `.mha` file
    dst : path
        `.nii.gz` file

    Returns
    -------
    Action
        The conversion (an error of its own output if `src` is corrupt)
    """
    return Action(src, Path(dst), lambda path: metaimage_to_nifti(src, path),
                  input="path", tag="subjects")
