"""
COBRE: convert the harvested NITRC files to BIDS.

Expected input
--------------
COBRE/sourcedata/
    COBRE_parameters_mprage.csv
    COBRE_parameters_rest.csv
    COBRE_phenotypic_data.csv
    COBRE_scan_data.tgz             (COBRE/<sub>/session_1/anat_1/mprage.nii.gz,
                                     COBRE/<sub>/session_1/rest_1/rest.nii.gz)

Output (study layout)
---------------------
COBRE/
    dataset_description.json        (DatasetType "study")
    rawbids/
        README, dataset_description.json, participants.{tsv,json}
        sub-{07d}/anat/sub-{07d}_T1w.{nii.gz,json}
        sub-{07d}/func/sub-{07d}_task-rest_bold.{nii.gz,json}

With `--layout flat`, the content of `rawbids/` is in `COBRE/` itself.
The participants table is written complete (every subject of
`COBRE_phenotypic_data.csv`) to `.brainspresso/orig/` and trimmed to the
subjects on disk (a roast of `--keys meta` alone, before any image, has
a table with a header only).

All images are members of one gzip-compressed tarball, whose listing is
a full decompression: they are extracted by one shard that reads it
once, member by member (subjects that the selection drops are skipped
before anything is extracted), not by one shard per subject, each of
which would decompress the tarball up to its own members. Its scope
(`roast.scope`) is the image keys and the subjects it sees: a later
roast that keeps no more (fewer subjects, `meta` toggled) skips it; its
sidecar templates (paths, hashes) are its `Shard.config`. A corrupt
tarball is one `Fail` of that shard, named after the tarball.
"""  # noqa: E501
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterator

from brainspresso.actions import Action, Fail
from brainspresso.actions.archive import ExtractMember
from brainspresso.bids.keys import KeyAliases
from brainspresso.roast import (
    SEEN,
    RoastContext,
    Shard,
    archives,
    copy_templates,
    scope,
)
from brainspresso.roast.run import python_api
from brainspresso.roast.sidecars import template_config, template_sidecar
from brainspresso.roast.tables import read_csv, table_action
from brainspresso.utils.archive import TarArchive

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"

TARBALL = "COBRE_scan_data.tgz"
PHENOTYPES = "COBRE_phenotypic_data.csv"

WIDTH = 7               # subject labels: `sub-0040000`

KEYS = KeyAliases({"meta", "T1w", "func"}, bids={
    "T1w": {"datatypes": ["anat"], "suffixes": ["T1w"]},
    "func": {"datatypes": ["func"], "suffixes": ["bold"]},
})

# member name -> (key, datatype, entities, suffix)
SCANS = {
    "mprage.nii.gz": ("T1w", "anat", "", "T1w"),
    "rest.nii.gz": ("func", "func", "task-rest_", "bold"),
}

#: RepetitionTime (s) of the rest scans, from the COBRE fact sheet
#: (`templates/README`: "TR: 2 s"): written when a NIfTI header gives
#: none (warned, with a count)
DOCUMENTED_TR = 2.0

#: Subtypes of a diagnosis (`<code> <subtype>`; the `Levels` of
#: `icd9_subtype` in `participants.json`)
SUBTYPES = ("depressed", "bipolar")

PARTICIPANTS_HEADER = [
    "participant_id",
    "age",
    "sex",
    "handedness",
    "patient",
    "icd9",
    "icd9_subtype",
]


def roast_cobre(ctx: RoastContext) -> Iterator:
    """
    Roast COBRE (the roaster of the NITRC source).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action | Shard
        Metadata actions, then the shard of the images
    """
    if ctx.wants("meta"):
        yield from roast_meta(ctx)
    if ctx.wants("T1w") or ctx.wants("func"):
        tarball = ctx.source / TARBALL
        if not tarball.exists():
            yield Fail(f"{TARBALL} not found", name=TARBALL, tag="scans")
            return
        yield Shard(roast_scans, tag="scans", id="roast/scans",
                    inputs=[tarball], config=sidecar_config(ctx),
                    scope=scope(keys=("T1w", "func"), subs=SEEN))


def sidecar_config(ctx: RoastContext) -> dict:
    """
    Describe the sidecar resources (in the signature of the image shard).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Returns
    -------
    dict
        `{name: [path, sha256] | None}` (`roast.sidecars.template_config`):
        a changed template re-runs the shard
    """
    return template_config(ctx.resource, [
        f"{suffix}.json" for _, _, _, suffix in SCANS.values()])


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
        Copies of the templates, then the pristine participants table
    """
    raw = ctx.layout.raw
    yield from copy_templates(ctx, ["README", "dataset_description.json",
                                    "participants.json"])
    phenotypes = ctx.source / PHENOTYPES
    if not phenotypes.exists():
        yield Fail(f"{PHENOTYPES} not found", name="participants.tsv",
                   tag="meta")
        return
    yield table_action(ctx, raw / "participants.tsv",
                       partial(participant_rows, phenotypes), phenotypes)


def participant_label(value: str) -> str:
    """
    Label of a subject (numbers zero-padded to 7 digits).

    Parameters
    ----------
    value : str
        Subject identifier of the CSV file or of the tarball

    Returns
    -------
    str
        `"40000"` -> `"0040000"`; other values stripped, as they are
    """
    value = value.strip()
    return f"{int(value):0{WIDTH}d}" if value.isdigit() else value


def participant_row(row: list[str],
                    warned: set | None = None) -> list[str] | None:
    """
    Convert a row of `COBRE_phenotypic_data.csv`.

    Parameters
    ----------
    row : list[str]
        Subject, age, gender, handedness, subject type, diagnosis
    warned : set, optional
        Words after a diagnosis code already warned about as not
        subtypes (one table: updated; default: warn every time)

    Returns
    -------
    list[str] | None
        A row of `participants.tsv` ("n/a" for empty cells and missing
        diagnoses, all "n/a" for disenrolled subjects; a word after the
        diagnosis code that is not one of `SUBTYPES` is not a subtype:
        "n/a", warned); None for a row without a subject or with fewer
        than 6 cells
    """
    row = [cell.strip() for cell in row]
    if not any(row):
        return None
    if len(row) < 6 or not row[0]:
        lg.warning(f"{PHENOTYPES}: row skipped (no subject, or fewer "
                   f"than 6 cells): {row}")
        return None
    age, gender, hand, kind, diagnosis = row[1:6]
    codes = diagnosis.split() if diagnosis != "None" else []
    subtype = " ".join(codes[1:]) or "n/a"
    if subtype != "n/a" and subtype.lower() not in SUBTYPES:
        warned = set() if warned is None else warned
        if subtype not in warned and "Disenrolled" not in row:
            warned.add(subtype)
            lg.warning(f"{PHENOTYPES}: {subtype!r} (diagnosis "
                       f"{diagnosis!r}, subject {row[0]}) is not a "
                       f"subtype ({', '.join(SUBTYPES)}): icd9_subtype "
                       f"n/a (warned once)")
        subtype = "n/a"
    out = [
        f"sub-{participant_label(row[0])}",
        age or "n/a",
        gender[:1] or "n/a",
        hand[:1] or "n/a",
        ("Y" if kind[0] == "P" else "N") if kind else "n/a",
        codes[0] if codes else "n/a",
        subtype.lower() if subtype != "n/a" else subtype,
    ]
    if "Disenrolled" in out:
        out = out[:1] + ["n/a"] * 6
    return out


def participant_rows(path_csv) -> list[list[str]]:
    """
    Read the participants table (every subject of the CSV file).

    Cells are read by position (the CSV header names no subject column);
    a word after a diagnosis code that is not a subtype is warned about
    once per table (`participant_row`), i.e. once per roast.

    Parameters
    ----------
    path_csv : path
        `COBRE_phenotypic_data.csv` (UTF-8, else Windows-1252)

    Returns
    -------
    list[list[str]]
        Header and rows

    Raises
    ------
    roast.tables.TableError
        If the file cannot be read
    """
    warned = set()
    rows = [PARTICIPANTS_HEADER]
    for _, row in read_csv(path_csv).rows:
        row = participant_row(list(row), warned)
        if row is not None:
            rows.append(row)
    return rows


# ----------------------------------------------------------------------
#   Images
# ----------------------------------------------------------------------

def parse_member(name: str) -> tuple[str, tuple] | None:
    """
    Parse a member of the tarball.

    Parameters
    ----------
    name : str
        `COBRE/<sub>/session_1/<scan>_1/<scan>.nii.gz`

    Returns
    -------
    (str, tuple) | None
        Subject label and `SCANS` entry; None for other members
    """
    path = PurePosixPath(name)
    if len(path.parts) < 3 or path.name not in SCANS:
        return None
    return participant_label(path.parts[1]), SCANS[path.name]


def roast_scans(ctx: RoastContext) -> Iterator[Action]:
    """
    Extract the images of the selected subjects (a shard).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action
        Per image: the image, then its sidecar (a template; for bold,
        with the `TaskName` of its name and the `RepetitionTime` of its
        NIfTI header, which the template lacks: `roast.sidecars.bold_sidecar`;
        `DOCUMENTED_TR` if the header gives none, one warning per roast);
        a `Fail` if the tarball cannot be read

    Notes
    -----
    Members are read lazily and each selected one is extracted when it
    is reached (the engine runs an action before asking for the next):
    the tarball is decompressed once. Its size is unknown until then,
    so the progress group has no total.
    """
    defaults = []                   # bold images given `DOCUMENTED_TR`
    try:
        with archives.reading(ctx.source / TARBALL, opener=TarArchive) as tar:
            for member in tar:
                parsed = parse_member(member.path)
                if parsed is None:
                    continue
                sub, (key, datatype, entities, suffix) = parsed
                # (every subject is reported, before the key prunes it)
                if not (ctx.keeps_subject(sub) and ctx.wants(key)):
                    continue
                stem = (ctx.layout.raw / f"sub-{sub}" / datatype /
                        f"sub-{sub}_{entities}{suffix}")
                image = Path(f"{stem}.nii.gz")
                yield ExtractMember(tar.archive, member.info, image, tag=key)
                yield from sidecar_actions(
                    ctx, stem, suffix, key, image, member.open,
                    PurePosixPath(member.name).name,
                    on_default=partial(defaults.append, stem.name))
                ctx.progress(advance=1)
    except archives.ArchiveError as e:
        yield e.fail(tag="scans")
    if defaults:
        lg.warning(f"{len(defaults)} bold image(s) without a usable "
                   f"RepetitionTime in their NIfTI header: "
                   f"{DOCUMENTED_TR} s, the TR of the COBRE fact sheet "
                   f"(templates/README), e.g. {defaults[0]}")


def sidecar_actions(ctx: RoastContext, stem: Path, suffix: str, key: str,
                    image: Path, open_image, image_name: str,
                    on_default=None) -> Iterator[Action]:
    """
    Write the sidecar of an image (after the image: its header is read).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    stem : Path
        Output path of the scan, without extension
    suffix : str
        BIDS suffix (the template is `<suffix>.json`)
    key : str
        Key of the scan (progress tag)
    image : Path
        Output image
    open_image : callable() -> file
        Opens the source image (`--sidecars only`)
    image_name : str
        Name of the source image
    on_default : callable(), optional
        Called when a bold header gives no RepetitionTime
        (`DOCUMENTED_TR` is written)

    Yields
    ------
    Action
        A copy of the template (bold: `bold_sidecar`, RepetitionTime of
        the header, else `DOCUMENTED_TR`); a `Fail` if it is missing or
        cannot be read
    """
    if not ctx.accepts("sidecar"):
        return
    sidecar = ctx.resource(f"{suffix}.json")
    if sidecar is None:
        yield Fail(f"{suffix}.json not found", name=f"{stem.name}.json",
                   tag=key)
        return
    yield from template_sidecar(
        Path(sidecar), stem, key, bold=suffix == "bold", image=image,
        open_image=open_image, image_name=image_name,
        default_tr=DOCUMENTED_TR, on_default=on_default)


# ----------------------------------------------------------------------
#   Python API
# ----------------------------------------------------------------------

#: Roast `<root>` (the `COBRE` folder): `roast(root, keys=..., sub=...)`
roast = python_api(roast_cobre, KEYS, folder="COBRE",
                   width=WIDTH, example=40000)
