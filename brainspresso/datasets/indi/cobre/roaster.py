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
roast that keeps no more (fewer subjects, `meta` toggled) skips it.
"""  # noqa: E501
import csv
import hashlib
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterator

from brainspresso.actions import Action, CopyJSON, Fail
from brainspresso.actions.archive import ExtractMember
from brainspresso.bids.keys import KeyAliases
from brainspresso.roast import (
    SEEN,
    RoastContext,
    RoastReport,
    Shard,
    copy_templates,
    roast_dataset,
    scope,
)
from brainspresso.sources.base import subject_labels
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.io import write_tsv

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

PARTICIPANTS_HEADER = [
    "participant_id",
    "age",
    "sex",
    "handedness",
    "patient",
    "icd9",
    "icd9_subtype",
]


def roast(root, *, subs=(), exclude_subs=(), **options) -> RoastReport:
    """
    Roast `<root>` (the `COBRE` folder).

    Parameters
    ----------
    root : path
        Dataset folder
    subs, exclude_subs : list[int | str]
        Subjects: numbers (`40000` is `sub-0040000`), labels with or
        without `sub-`, ranges, files (as `--subs`)
    **options
        Those of the command line (`keys`, `exclude_keys`, `json`,
        `ifexists`, `layout`), `jobs` and `display`

    Returns
    -------
    RoastReport
    """
    return roast_dataset(
        roast_cobre, root, KEYS, templates=TEMPLATES,
        subs=sorted(subject_labels(subs, WIDTH)),
        exclude_subs=sorted(subject_labels(exclude_subs, WIDTH)), **options,
    )


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
        `{name: [path, sha256] | None}`: a changed template re-runs the
        shard
    """
    out = {}
    for _, _, _, suffix in SCANS.values():
        path = ctx.resource(f"{suffix}.json")
        out[f"{suffix}.json"] = None if path is None else [
            Path(path).as_posix(),
            hashlib.sha256(Path(path).read_bytes()).hexdigest()]
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
    yield Action(
        phenotypes, ctx.layout.original(raw / "participants.tsv"),
        partial(write_participants, phenotypes),
        mode="t", input="path", tag="meta",
    )


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


def participant_row(row: list[str]) -> list[str] | None:
    """
    Convert a row of `COBRE_phenotypic_data.csv`.

    Parameters
    ----------
    row : list[str]
        Subject, age, gender, handedness, subject type, diagnosis

    Returns
    -------
    list[str] | None
        A row of `participants.tsv` ("n/a" for empty cells and missing
        diagnoses, all "n/a" for disenrolled subjects); None for a row
        without a subject or with fewer than 6 cells
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
    out = [
        f"sub-{participant_label(row[0])}",
        age or "n/a",
        gender[:1] or "n/a",
        hand[:1] or "n/a",
        ("Y" if kind[0] == "P" else "N") if kind else "n/a",
        codes[0] if codes else "n/a",
        codes[1] if len(codes) > 1 else "n/a",
    ]
    if "Disenrolled" in out:
        out = out[:1] + ["n/a"] * 6
    return out


def write_participants(path_csv, path_tsv) -> None:
    """
    Write the participants table (every subject of the CSV file).

    Parameters
    ----------
    path_csv : path
        `COBRE_phenotypic_data.csv`
    path_tsv : path
        Output table
    """
    def iter_rows():
        with open(path_csv, "rt", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            yield PARTICIPANTS_HEADER
            for row in reader:
                row = participant_row(list(row))
                if row is not None:
                    yield row

    write_tsv(iter_rows(), path_tsv)


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
        Per image: its sidecar (a template), then the image

    Notes
    -----
    Members are read lazily and each selected one is extracted when it
    is reached (the engine runs an action before asking for the next):
    the tarball is decompressed once. Its size is unknown until then,
    so the progress group has no total.
    """
    with TarArchive(ctx.source / TARBALL) as tar:
        for member in tar:
            parsed = parse_member(member.name)
            if parsed is None or not member.isfile():
                continue
            sub, (key, datatype, entities, suffix) = parsed
            # (every subject is reported, before the key prunes it)
            if not (ctx.keeps_subject(sub) and ctx.wants(key)):
                continue
            stem = (ctx.layout.raw / f"sub-{sub}" / datatype /
                    f"sub-{sub}_{entities}{suffix}")
            sidecar = ctx.resource(f"{suffix}.json")
            if sidecar is not None:
                yield CopyJSON(sidecar, f"{stem}.json", tag=key)
            elif ctx.accepts("sidecar"):
                yield Fail(f"{suffix}.json not found",
                           name=f"{stem.name}.json", tag=key)
            yield ExtractMember(tar, member, f"{stem}.nii.gz", tag=key)
            ctx.progress(advance=1)
