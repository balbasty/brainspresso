"""
IXI: convert the original tarballs (or the NITRC-IR folders) to BIDS.

Expected input
--------------
IXI/sourcedata/
    IXI-{T1,T2,PD,MRA,DTI}.tar      (original release), or
    IXI-{T1,T2,PD,MRA,DTI}/         (harvested from NITRC-IR, with
                                     IXI-DTI/<IXI###-Site-####>-{bvals,bvecs}.txt)
    bvals.txt, bvecs.txt, IXI.xls   (else: shipped templates)

Output (study layout)
---------------------
IXI/rawbids/
    README, dataset_description.json, participants.{tsv,json}
    dwi.bval, dwi.bvec              (original release: common to all)
    sub-{03d}/anat/sub-{03d}_{T1w,T2w,PDw,angio}.{nii.gz,json}
    sub-{03d}/dwi/sub-{03d}_dwi.{nii.gz,json[,bval,bvec]}

With `--layout flat`, the content of `rawbids/` is in `IXI/` itself.
The participants table is written complete (all subjects of `IXI.xls`)
to `.brainspresso/orig/` and trimmed to the subjects on disk.
"""  # noqa: E501
from functools import partial
from gzip import GzipFile
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterator

import nibabel as nib
import numpy as np
import xlrd  # extra: brainspresso[ixi]

from brainspresso.actions import Action, CopyBytes, CopyJSON, Fail
from brainspresso.actions.archive import ExtractMember
from brainspresso.roast import RoastContext, Shard
from brainspresso.roast import as_list, copy_templates, roast_dataset
from brainspresso.bids.keys import KeyAliases
from brainspresso.utils.archive import open_archive
from brainspresso.utils.io import write_tsv

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"

# IXI modality -> BIDS suffix (= dataset key)
IXI2BIDS = {"T1": "T1w", "T2": "T2w", "PD": "PDw", "MRA": "angio",
            "DTI": "dwi"}
KEYS = KeyAliases(
    {"meta", *IXI2BIDS.values()},
    bids={key: {"suffixes": [key]} for key in IXI2BIDS.values()},
)


def roast(root, *, subs=(), exclude_subs=(), **options):
    """Roast `<root>` (the `IXI` folder). Subjects are numbers. Other
    options: those of the command line (`keys`, `json`, `ifexists`,
    `layout`), `jobs` and `display`."""
    return roast_dataset(
        roast_ixi, root, KEYS, templates=TEMPLATES,
        subs=as_list(subs, "{:03d}"),
        exclude_subs=as_list(exclude_subs, "{:03d}"), **options,
    )


def roast_ixi(ctx: RoastContext) -> Iterator:
    if ctx.wants("meta"):
        yield from roast_meta(ctx)
    for ixi, key in IXI2BIDS.items():
        if key != "dwi" and ctx.wants(key):
            yield Shard(roast_anat, ixi, key, tag=key)
    if ctx.wants("dwi"):
        yield Shard(roast_dwi, tag="dwi")


def open_images(ctx: RoastContext, ixi: str):
    """Tarball `IXI-<ixi>.tar`, else folder `IXI-<ixi>/`, else None"""
    src = ctx.source
    return open_archive(src / f"IXI-{ixi}.tar", src / f"IXI-{ixi}")


def missing_images(ixi: str) -> Fail:
    return Fail(f"IXI-{ixi}.tar (or IXI-{ixi}/) not found",
                name=f"IXI-{ixi}")


def parse_name(name: str) -> tuple[str, str]:
    """`IXI002-Guys-0828-T1.nii.gz` -> ("002", "Guys")"""
    ixi_id, site, *_ = PurePosixPath(name).name.split("-")
    return f"{int(ixi_id[3:]):03d}", site


# ----------------------------------------------------------------------
#   Metadata
# ----------------------------------------------------------------------

def roast_meta(ctx: RoastContext) -> Iterator[Action]:
    raw = ctx.layout.raw
    yield from copy_templates(ctx, ["README", "dataset_description.json",
                                    "participants.json"])

    # Sites are only given by image names
    sites = {}
    for ixi in IXI2BIDS:
        archive = open_images(ctx, ixi)
        if archive is not None:
            with archive:
                sites.update(map(parse_name, archive.getnames()))
    xls = ctx.resource("IXI.xls")
    if xls is None:
        yield Fail("IXI.xls not found", name="participants.tsv", tag="meta")
        return
    yield Action(
        xls, ctx.layout.original(raw / "participants.tsv"),
        partial(write_participants, xls, sites=sites),
        mode="t", input="path", tag="meta",
    )


# ----------------------------------------------------------------------
#   T1w, T2w, PDw, angio: one image per subject
# ----------------------------------------------------------------------

def roast_anat(ctx: RoastContext, ixi: str, key: str) -> Iterator[Action]:
    archive = open_images(ctx, ixi)
    if archive is None:
        yield missing_images(ixi)
        return
    with archive:
        members = archive.getmembers()
        ctx.progress(total=len(members))
        for member in members:
            sub, site = parse_name(member.name)
            anat = ctx.layout.raw / f"sub-{sub}" / "anat"
            yield CopyJSON(ctx.resource(f"{site}/{key}.json"),
                           anat / f"sub-{sub}_{key}.json")
            yield ExtractMember(archive, member,
                                anat / f"sub-{sub}_{key}.nii.gz")
            ctx.progress(advance=1)


# ----------------------------------------------------------------------
#   DWI: one 3D image per direction, stacked into a 4D image
# ----------------------------------------------------------------------

def roast_dwi(ctx: RoastContext) -> Iterator[Action]:
    raw, src = ctx.layout.raw, ctx.source

    # The original release has a single bvals.txt/bvecs.txt, common to
    # all subjects, placed at the top of the raw dataset. NITRC-IR has
    # one pair per subject, written next to each subject's dwi file.
    common = {}
    for ext, source in (("bval", "bvals.txt"), ("bvec", "bvecs.txt")):
        path = ctx.resource(source)
        if path is not None:
            common[source] = path
            yield CopyBytes(path, raw / f"dwi.{ext}")

    archive = open_images(ctx, "DTI")
    if archive is None:
        yield missing_images("DTI")
        return
    with archive:
        # subject -> (site, volumes ordered by direction)
        series = {}
        for name in archive.getnames():
            sub, site = parse_name(name)
            series.setdefault(sub, (site, []))[1].append(name)
        ctx.progress(total=len(series))
        for sub, (site, names) in sorted(series.items()):
            names.sort(key=direction)
            dwi = raw / f"sub-{sub}" / "dwi"
            base = f"sub-{sub}_dwi"
            yield CopyJSON(ctx.resource(f"{site}/dwi.json"),
                           dwi / f"{base}.json")
            yield Action(
                archive.sources(*names), dwi / f"{base}.nii.gz",
                partial(stack_volumes, archive, names), input="path",
            )
            # Per-subject b-values/vectors (NITRC-IR)
            prefix = "-".join(PurePosixPath(names[0]).name.split("-")[:3])
            for ext, source in (("bval", "bvals.txt"), ("bvec", "bvecs.txt")):
                if source in common:
                    continue
                path = src / "IXI-DTI" / f"{prefix}-{source}"
                if path.exists():
                    yield CopyBytes(path, dwi / f"{base}.{ext}")
                else:
                    yield Fail(f"{source} not found for sub-{sub}",
                               name=f"{base}.{ext}")
            ctx.progress(advance=1)


def direction(name: str) -> int:
    """`IXI002-Guys-0828-DTI-05.nii.gz` -> 5"""
    return int(PurePosixPath(name).name.split(".")[0].split("-")[-1])


def stack_volumes(archive, names: list[str], path: Path) -> Iterator[dict]:
    """
    Concatenate 3D volumes along the 4th dimension (which nifti reserves
    for time, but a DWI series is acquired like an fMRI time series, one
    gradient per volume).
    """
    dat = []
    for i, name in enumerate(names):
        yield {"status": f"load ch-{i:02d}"}
        with archive.extractfile(archive.getmember(name)) as f:
            nii = nib.Nifti1Image.from_stream(GzipFile(fileobj=f))
            dat.append(np.asarray(nii.dataobj).squeeze())
    if len({d.shape for d in dat}) > 1:
        # this happened in one of the subjects...
        raise RuntimeError("incompatible shapes")
    yield {"status": "stacking channels"}
    dat = np.stack(dat, axis=3)
    yield {"status": "writing stack"}
    nib.save(nib.Nifti1Image(dat, nii.affine, nii.header), path)


# ----------------------------------------------------------------------
#   Participants
# ----------------------------------------------------------------------

IXI_HEADER = [
    "IXI_ID", "SEX_ID", "HEIGHT", "WEIGHT", "ETHNIC_ID", "MARITAL_ID",
    "OCCUPATION_ID", "QUALIFICATION_ID", "DOB", "DATE_AVAILABLE",
    "STUDY_DATE", "AGE",
]
IXI_SEX = {
    1: "M",     # Male
    2: "F",     # Female
}
IXI_ETHNICITY = {
    1: "W",     # White
    2: "B",     # Black or black british
    3: "A",     # Asian or asian british
    5: "C",     # Chinese
    6: "O",     # Other
}
IXI_MARITAL_STATUS = {
    1: "S",     # Single
    2: "M",     # Married
    3: "C",     # Cohabiting
    4: "D",     # Divorced/separated
    5: "W",     # Widowed
}
IXI_OCCUPATION = {
    1: "FT",    # Go out to full time employment
    2: "PT",    # Go out to part time employment (<25hrs)
    3: "S",     # Study at college or university
    4: "H",     # Full-time housework
    5: "R",     # Retired
    6: "U",     # Unemployed
    7: "WFH",   # Work for pay at home
    8: "O",     # Other
}
IXI_QUALIFICATION = {
    1: "N",     # No qualifications
    2: "O",     # O-levels, GCSEs, or CSEs
    3: "A",     # A-levels
    4: "F",     # Further education e.g. City & Guilds / NVQs
    5: "U",     # University or Polytechnic degree
}
PARTICIPANTS_HEADER = [
    "participant_id", "site", "age", "sex", "height", "weight", "dob",
    "ethnicity", "marital_status", "occupation", "qualification",
    "study_date",
]


def write_participants(path_xls, path_tsv, sites: dict[str, str]) -> None:
    """All subjects of `IXI.xls` with images available (`DATE_AVAILABLE`);
    `site` from image names (n/a for subjects without local images)"""
    book = xlrd.open_workbook(path_xls)
    sheet = book.sheet_by_index(0)

    def parse_date(date):
        if not isinstance(date, str):
            date = xlrd.xldate.xldate_as_tuple(date, book.datemode)
            date = f"{date[0]:04d}-{date[1]:02d}-{date[2]:02d}"
        return date

    def iter_rows():
        yield PARTICIPANTS_HEADER
        for n in range(1, sheet.nrows):
            row = dict(zip(IXI_HEADER, (c.value for c in sheet.row(n))))
            if row["DATE_AVAILABLE"] == 0:
                continue
            sub = f"{int(row['IXI_ID']):03d}"
            yield [
                f"sub-{sub}",
                sites.get(sub, "n/a"),
                row["AGE"] or "n/a",
                IXI_SEX.get(row["SEX_ID"], "n/a"),
                row["HEIGHT"] or "n/a",
                row["WEIGHT"] or "n/a",
                parse_date(row["DOB"]) or "n/a",
                IXI_ETHNICITY.get(row["ETHNIC_ID"], "n/a"),
                IXI_MARITAL_STATUS.get(row["MARITAL_ID"], "n/a"),
                IXI_OCCUPATION.get(row["OCCUPATION_ID"], "n/a"),
                IXI_QUALIFICATION.get(row["QUALIFICATION_ID"], "n/a"),
                parse_date(row["STUDY_DATE"]) or "n/a",
            ]

    write_tsv(iter_rows(), path_tsv)
