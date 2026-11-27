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
subjects on disk.

All images are members of one gzip-compressed tarball, whose listing is
a full decompression: they are extracted by one shard that reads it
once (subjects that the selection drops are skipped before anything is
extracted), not by one shard per subject, each of which would
decompress the tarball up to its own members.
"""  # noqa: E501
import csv
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Iterator

from brainspresso.actions import Action, CopyJSON, Fail
from brainspresso.actions.archive import ExtractMember
from brainspresso.bids.keys import KeyAliases
from brainspresso.roast import (
    RoastContext,
    RoastReport,
    Shard,
    as_list,
    copy_templates,
    roast_dataset,
)
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.io import write_tsv

TEMPLATES = Path(__file__).parent / "templates"

TARBALL = "COBRE_scan_data.tgz"
PHENOTYPES = "COBRE_phenotypic_data.csv"

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
        Subject numbers (`40000` is `sub-0040000`)
    **options
        Those of the command line (`keys`, `exclude_keys`, `json`,
        `ifexists`, `layout`), `jobs` and `display`

    Returns
    -------
    RoastReport
    """
    return roast_dataset(
        roast_cobre, root, KEYS, templates=TEMPLATES,
        subs=as_list(subs, "{:07d}"),
        exclude_subs=as_list(exclude_subs, "{:07d}"), **options,
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
                    inputs=[tarball])


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


def participant_row(row: list[str]) -> list[str]:
    """
    Convert a row of `COBRE_phenotypic_data.csv`.

    Parameters
    ----------
    row : list[str]
        Subject, age, gender, handedness, subject type, diagnosis

    Returns
    -------
    list[str]
        A row of `participants.tsv` (all "n/a" for disenrolled
        subjects)
    """
    diagnosis = row[5].split()
    out = [
        f"sub-{row[0]}",
        row[1],
        row[2][0],
        row[3][0],
        "Y" if row[4][0] == "P" else "N",
        "" if row[5] == "None" else diagnosis[0],
        diagnosis[1] if len(diagnosis) > 1 else "",
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
                yield participant_row(list(row))

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
    return path.parts[1], SCANS[path.name]


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
    """
    with TarArchive(ctx.source / TARBALL) as tar:
        scans = []
        for member in tar.getmembers():
            parsed = parse_member(member.name)
            if parsed is None:
                continue
            sub, (key, *_) = parsed
            if ctx.selection.keeps_subject(sub) and ctx.wants(key):
                scans.append((member, parsed))
        ctx.progress(total=len(scans))
        for member, (sub, (key, datatype, entities, suffix)) in scans:
            stem = (ctx.layout.raw / f"sub-{sub}" / datatype /
                    f"sub-{sub}_{entities}{suffix}")
            sidecar = ctx.resource(f"{suffix}.json")
            if sidecar is None:
                yield Fail(f"{suffix}.json not found",
                           name=f"{stem.name}.json", tag=key)
            else:
                yield CopyJSON(sidecar, f"{stem}.json", tag=key)
            yield ExtractMember(tar, member, f"{stem}.nii.gz", tag=key)
            ctx.progress(advance=1)
