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
    sub-{03d}/anat/sub-{03d}_{T1w,T2w,PDw,angio}.{nii.gz,json}
    sub-{03d}/dwi/sub-{03d}_dwi.{nii.gz,json,bval,bvec}  (16 volumes)
IXI/derivatives/ixi-scanner/        (key `trace`: 17th DTI volume, a
    sub-{03d}/dwi/sub-{03d}_desc-{trace,powderavg}_dwimap.{nii.gz,json}
                                     scanner-derived isotropic image)

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
from brainspresso.actions.writers import WriteJSON, WriteText
from brainspresso.actions.archive import ExtractMember
from brainspresso.actions.helpers import copy_templates
from brainspresso.actions.runner import RoastContext, Shard
from brainspresso.actions.runner import as_list, roast_dataset
from brainspresso.bids.keys import KeyAliases
from brainspresso.utils.archive import open_archive
from brainspresso.utils.io import write_tsv

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"

# IXI modality -> BIDS suffix (= dataset key)
IXI2BIDS = {"T1": "T1w", "T2": "T2w", "PD": "PDw", "MRA": "angio",
            "DTI": "dwi"}
KEYS = KeyAliases(
    {"meta", "trace", *IXI2BIDS.values()},
    bids={key: {"suffixes": [key]} for key in IXI2BIDS.values()},
    folders={"trace": ["derivatives/ixi-scanner"]},
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

NDIR = 16               # entries of the IXI gradient table: b=0 + 15 DW
DERIV_NAME = "ixi-scanner"
# scanner-derived isotropic image (17th volume): mean type -> desc
DERIVED_DESC = {"geometric": "trace", "arithmetic": "powderavg"}
MIN_CORRELATION = 0.95


def roast_dwi(ctx: RoastContext) -> Iterator[Action]:
    drv = ctx.layout.derivative(DERIV_NAME)
    want_trace = ctx.wants("trace")

    archive = open_images(ctx, "DTI")
    if archive is None:
        yield missing_images("DTI")
        return
    with archive:
        series = group_members(archive)
        ctx.progress(total=len(series))
        if want_trace:
            yield from derivative_description(drv)
        for sub, (site, volumes) in sorted(series.items()):
            yield from roast_dwi_subject(ctx, archive, drv, sub, site,
                                         volumes, want_trace)
            ctx.progress(advance=1)


def roast_dwi_subject(ctx, archive, drv, sub, site, volumes, want_trace):
    raw, src = ctx.layout.raw, ctx.source
    dwi = raw / f"sub-{sub}" / "dwi"
    base = f"sub-{sub}_dwi"

    # 1. de-duplicate (the original tarball has duplicated members)
    names = {}
    for index, members in sorted(volumes.items()):
        first = members[0]
        if len(members) > 1:
            blobs = {member_bytes(archive, m) for m in members}
            if len(blobs) > 1:
                yield Fail(f"sub-{sub}: differing duplicates of DTI "
                           f"volume {index:02d}", name=f"{base}.nii.gz")
                return
            lg.debug(f"sub-{sub}: identical duplicates of DTI volume "
                     f"{index:02d} skipped")
        names[index] = first.name
    count = len(names)
    if sorted(names) != list(range(count)) or count not in (NDIR, NDIR + 1):
        yield Fail(f"sub-{sub}: unexpected number of DTI volumes ({count}, "
                   f"indices {sorted(names)}; expected {NDIR} or {NDIR + 1})",
                   name=f"{base}.nii.gz")
        return
    names = [names[i] for i in range(count)]

    # 2. 17th volume: scanner-derived isotropic image?
    desc = None
    if count == NDIR + 1:
        vols = [load_volume(archive, archive.getmember(n)) for n in names]
        desc = classify_derived(vols)
        if desc is None:
            yield Fail(f"sub-{sub}: unrecognised DWI volume layout "
                       f"(volume {NDIR} is not a mean of the DW volumes)",
                       name=f"{base}.nii.gz")
            return

    # 3. gradient table (per-subject NITRC table, else global one)
    prefix = "-".join(PurePosixPath(names[0]).name.split("-")[:3])
    tables = {}
    for ext, source in (("bval", "bvals.txt"), ("bvec", "bvecs.txt")):
        path = src / "IXI-DTI" / f"{prefix}-{source}"
        if not path.exists():
            path = ctx.resource(source)
        if path is None:
            yield Fail(f"{source} not found for sub-{sub}",
                       name=f"{base}.{ext}")
            return
        rows = [line.split() for line in path.read_text().splitlines()
                if line.strip()]
        n = {len(row) for row in rows}
        n = n.pop() if len(n) == 1 else None
        if n == NDIR + 1 and desc is not None:
            tables[ext] = (path, "\n".join(" ".join(r[:NDIR])
                                           for r in rows) + "\n")
        elif n == NDIR:
            tables[ext] = (path, None)
        else:
            yield Fail(f"sub-{sub}: {path.name} has {n} entries, expected "
                       f"{NDIR}", name=f"{base}.{ext}")
            return

    yield CopyJSON(ctx.resource(f"{site}/dwi.json"), dwi / f"{base}.json")
    yield Action(
        archive.sources(*names), dwi / f"{base}.nii.gz",
        partial(stack_volumes, archive, names[:NDIR]), input="path",
    )
    for ext, (path, text) in tables.items():
        if text is None:
            yield CopyBytes(path, dwi / f"{base}.{ext}")
        else:
            yield WriteText(text, dwi / f"{base}.{ext}", src=path)

    if desc is not None and want_trace:
        out = drv.root / f"sub-{sub}" / "dwi"
        stem = f"sub-{sub}_desc-{desc}_dwimap"
        yield Action(
            archive.sources(names[-1]), out / f"{stem}.nii.gz",
            partial(stack_volumes, archive, names[-1:]), input="path",
            tag="trace",
        )
        mean = "geometric" if desc == "trace" else "arithmetic"
        yield WriteJSON({
            "Description": f"Scanner-derived isotropic DWI ({mean} mean "
                           f"of the {NDIR - 1} b=1000 volumes), "
                           f"volume {NDIR} of the IXI DTI series",
            "Sources": [f"bids:raw:sub-{sub}/dwi/{base}.nii.gz"],
            "SkullStripped": False,
        }, out / f"{stem}.json", tag="trace")


def derivative_description(drv) -> Iterator[Action]:
    desc = drv.dataset_description()
    desc["Name"] = "IXI - scanner-derived diffusion images"
    desc["GeneratedBy"] = [{
        "Name": "scanner",
        "Description": "Isotropic (trace) image computed by the scanner "
                       "and exported as the last volume of the DTI series",
    }]
    desc["DatasetLinks"] = {"raw": drv.source_url}
    yield WriteJSON(desc, drv.description, tag="trace")
    yield WriteText(
        "# Diffusion maps (BEP016 naming) are not (yet) in the BIDS schema\n"
        "*_dwimap.nii.gz\n*_dwimap.json\n", drv.bidsignore, tag="trace")


def group_members(archive) -> dict:
    """subject -> (site, {volume index: [members]})"""
    series = {}
    for member in archive.getmembers():
        name = member.name
        if "-DTI-" not in PurePosixPath(name).name:
            continue
        sub, site = parse_name(name)
        volumes = series.setdefault(sub, (site, {}))[1]
        volumes.setdefault(direction(name), []).append(member)
    return series


def member_bytes(archive, member) -> bytes:
    with archive.extractfile(member) as f:
        return f.read()


def load_volume(archive, member) -> np.ndarray:
    with archive.extractfile(member) as f:
        nii = nib.Nifti1Image.from_stream(GzipFile(fileobj=f))
        return np.asarray(nii.dataobj, dtype=np.float64).squeeze()


def correlation(x: np.ndarray, y: np.ndarray) -> float:
    if x.std() == 0 or y.std() == 0:
        return 0.0
    return float(np.corrcoef(x, y)[0, 1])


def classify_derived(vols: list[np.ndarray]) -> str | None:
    """`desc` of the 17th volume if it is the geometric ("trace") or
    arithmetic ("powderavg") mean of the DW volumes 1..15, else None"""
    if len({v.shape for v in vols}) > 1:
        return None
    b0, dw, extra = vols[0], np.stack(vols[1:NDIR]), vols[NDIR]
    mask = b0 > np.percentile(b0, 60)
    if mask.sum() < 3:
        mask = np.ones_like(b0, dtype=bool)
    means = {
        "geometric": np.exp(np.log(np.maximum(dw, 1e-3)).mean(0)),
        "arithmetic": dw.mean(0),
    }
    corr = {k: correlation(extra[mask], m[mask]) for k, m in means.items()}
    best = max(corr, key=corr.get)
    lg.debug(f"correlation of volume {NDIR} with means: {corr}")
    return DERIVED_DESC[best] if corr[best] >= MIN_CORRELATION else None


def direction(name: str) -> int:
    """`IXI002-Guys-0828-DTI-05.nii.gz` -> 5"""
    return int(PurePosixPath(name).name.split(".")[0].split("-")[-1])


def stack_volumes(archive, names: list[str], path: Path) -> Iterator[dict]:
    """
    Concatenate 3D volumes along the 4th dimension (which nifti reserves
    for time, but a DWI series is acquired like an fMRI time series, one
    gradient per volume). A single name writes a 3D image.
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
    dat = np.stack(dat, axis=3) if len(dat) > 1 else dat[0]
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
