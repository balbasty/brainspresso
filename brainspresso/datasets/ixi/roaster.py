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
    dwi.{bval,bvec}  (canonical gradient table, inherited)
    sub-{03d}/dwi/sub-{03d}_dwi.{nii.gz,json}  (16 volumes; .bval/.bvec
                                 only when the subject's table differs)
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

from brainspresso.actions import Action, CopyBytes, Fail
from brainspresso.actions.action import IfExists
from brainspresso.actions.archive import ExtractMember
from brainspresso.actions.writers import WriteJSON, WriteText
from brainspresso.bids.keys import KeyAliases
from brainspresso.datasets.base import KeySpec
from brainspresso.datasets.ixi.dataset import KEYS as DATASET_KEYS
from brainspresso.roast import (
    RoastContext,
    Shard,
    archives,
    copy_templates,
)
from brainspresso.roast.helpers import missing_input, named_keys
from brainspresso.roast.run import python_api
from brainspresso.roast.sidecars import copy_sidecar
from brainspresso.roast.tables import read_xls, table_action, xls_date
from brainspresso.utils.archive import FolderArchive, TarArchive, open_archive

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"
WIDTH = 3               # subject labels: `sub-002`

# IXI modality -> BIDS suffix (= dataset key)
IXI2BIDS = {"T1": "T1w", "T2": "T2w", "PD": "PDw", "MRA": "angio",
            "DTI": "dwi"}
KEYS = KeyAliases(
    {"meta", "trace", *IXI2BIDS.values()},
    bids={key: {"suffixes": [key]} for key in IXI2BIDS.values()},
    folders={"trace": ["derivatives/ixi-scanner"]},
)
#: `--keys` of the roast (`IXI.keys_for("roast")`)
ROAST_SPEC = KeySpec(KEYS, (*DATASET_KEYS, "trace"), DATASET_KEYS,
                     ("trace",))


def roast_options(keys) -> dict:
    """
    Return the options of the roaster (`RoastContext.options`).

    Parameters
    ----------
    keys : str | list[str]
        `--keys` as given

    Returns
    -------
    dict
        `named`: the leaves that `--keys` names, itself or through a
        group above them (any case); a missing tarball of these keys is
        an error, of the others an info line. Nothing is named without
        `--keys` or with its CLI default (the six keys: the command line
        passes them when the flag is absent, so they cannot be told
        apart)
    """
    return {"named": named_keys(ROAST_SPEC, keys, KEYS.leaves - {"meta"})}


def roast_ixi(ctx: RoastContext) -> Iterator:
    named = set(ctx.options.get("named", ()))
    if ctx.wants("meta"):
        yield from roast_meta(ctx)
    # (`named` decides whether a missing tarball is an error)
    for ixi, key in IXI2BIDS.items():
        if key != "dwi" and ctx.wants(key):
            yield Shard(roast_anat, ixi, key, tag=key,
                        config={"named": sorted(named & {key})})
    if ctx.wants("dwi") or ctx.wants("trace"):
        yield Shard(roast_dwi, tag="dwi",
                    config={"named": sorted(named & {"dwi", "trace"})})


def open_images(ctx: RoastContext, ixi: str):
    """Tarball `IXI-<ixi>.tar`, else folder `IXI-<ixi>/`, else None"""
    src = ctx.source
    return open_archive(src / f"IXI-{ixi}.tar", src / f"IXI-{ixi}")


def missing_images(ctx: RoastContext, ixi: str, *keys: str
                   ) -> Iterator[Fail]:
    """
    A missing tarball (and folder): a `Fail` if `--keys` names one of
    its keys, else an info line (`roast_options`).
    """
    yield from missing_input(
        f"IXI-{ixi}.tar (or IXI-{ixi}/) not found", name=f"IXI-{ixi}",
        required=bool(set(keys) & set(ctx.options.get("named", ()))),
        logger=lg)


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

    # Sites are only given by image names (an unreadable tarball is the
    # error of its own shard)
    sites = {}
    for ixi in IXI2BIDS:
        tar, folder = ctx.source / f"IXI-{ixi}.tar", ctx.source / f"IXI-{ixi}"
        path = tar if tar.exists() else folder if folder.is_dir() else None
        if path is None:
            continue
        opener = TarArchive if path == tar else FolderArchive
        try:
            sites.update(map(parse_name,
                             archives.names(path, opener=opener)))
        except archives.ArchiveError as e:
            lg.warning(f"{e}: the sites of its subjects are not in "
                       f"participants.tsv")
    xls = ctx.resource("IXI.xls")
    if xls is None:
        yield Fail("IXI.xls not found", name="participants.tsv", tag="meta")
        return
    yield table_action(ctx, raw / "participants.tsv",
                       partial(participant_rows, xls, sites=sites), xls)


# ----------------------------------------------------------------------
#   T1w, T2w, PDw, angio: one image per subject
# ----------------------------------------------------------------------

def roast_anat(ctx: RoastContext, ixi: str, key: str) -> Iterator[Action]:
    archive = open_images(ctx, ixi)
    if archive is None:
        yield from missing_images(ctx, ixi, key)
        return
    with archive:
        members = archive.getmembers()
        ctx.progress(total=len(members))
        for member in members:
            sub, site = parse_name(member.name)
            anat = ctx.layout.raw / f"sub-{sub}" / "anat"
            yield from copy_sidecar(ctx, f"{site}/{key}.json",
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
    want_dwi, want_trace = ctx.wants("dwi"), ctx.wants("trace")

    archive = open_images(ctx, "DTI")
    if archive is None:
        yield from missing_images(ctx, "DTI", "dwi", "trace")
        return
    with archive:
        # canonical (dataset-level) table: None if missing or invalid,
        # subjects then need their own NITRC table
        canonical = {"bval": None, "bvec": None}
        for ext, source in (("bval", "bvals.txt"), ("bvec", "bvecs.txt")):
            if not want_dwi:
                break
            path = ctx.resource(source)
            if path is None:
                yield Fail(f"{source} not found", name=f"dwi.{ext}")
                continue
            rows = read_table(path)
            n = table_length(rows)
            if n not in (NDIR, NDIR + 1):
                yield Fail(f"{path.name} has {describe_length(rows)}, "
                           f"expected {NDIR}", name=f"dwi.{ext}")
                continue
            try:
                canonical[ext] = np.asarray(rows, dtype=float)[:, :NDIR]
            except ValueError:
                yield Fail(f"{path.name} is not numeric", name=f"dwi.{ext}")
                continue
            if n == NDIR:
                yield CopyBytes(path, ctx.layout.raw / f"dwi.{ext}")
            else:
                yield WriteText(format_table(rows), ctx.layout.raw /
                                f"dwi.{ext}", src=path)
        series = {sub: value for sub, value in group_members(archive).items()
                  if ctx.selection.keeps_subject(sub)}
        ctx.progress(total=len(series))
        described = []      # derivative description written (once)
        for sub, (site, volumes) in sorted(series.items()):
            yield from roast_dwi_subject(ctx, archive, drv, sub, site,
                                         volumes, want_dwi, want_trace,
                                         canonical, described)
            ctx.progress(advance=1)


def describe_length(rows) -> str:
    """"16 entries", or the row lengths of a ragged table"""
    n = table_length(rows)
    if n is not None:
        return f"{n} entries"
    return f"rows of {', '.join(str(len(row)) for row in rows)} entries"


def needed(ctx, path, kind="data") -> bool:
    """Whether an output may be written by this roast (selected, accepted
    by the JSON policy, and not already there under `IfExists` skip)"""
    if not ctx.accepts(kind) or ctx.keeps(path) is False:
        return False
    skip = IfExists.from_any(ctx.ifexists) is IfExists.SKIP
    return not (skip and Path(path).exists())


def read_table(path) -> list[list[str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line.split() for line in lines
            if line.strip()]


def table_length(rows) -> int | None:
    n = {len(row) for row in rows}
    return n.pop() if len(n) == 1 else None


def format_table(rows) -> str:
    return "\n".join(" ".join(r[:NDIR]) for r in rows) + "\n"


def same_table(ext, rows, canonical) -> bool:
    try:
        table = np.asarray(rows, dtype=float)[:, :NDIR]
    except ValueError:
        return False
    ref = canonical[ext]
    if ref is None:
        return False
    if table.shape != ref.shape:
        return False
    if ext == "bval":
        return bool(np.allclose(table, ref, rtol=1e-3, atol=1))
    return bool(np.allclose(table, ref, atol=1e-4))


def roast_dwi_subject(ctx, archive, drv, sub, site, volumes, want_dwi,
                      want_trace, canonical, described):
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

    # 2. 17th volume: scanner-derived isotropic image? (only computed
    #    when the raw DWI or the derived map will be written)
    out = drv.root / f"sub-{sub}" / "dwi"
    want_nii = want_dwi and needed(ctx, dwi / f"{base}.nii.gz")
    want_map = want_trace and all(
        needed(ctx, out / f"sub-{sub}_desc-{d}_dwimap.nii.gz")
        for d in DERIVED_DESC.values())
    desc = None
    if count == NDIR + 1 and (want_nii or want_map):
        vols = [load_volume(archive, archive.getmember(n)) for n in names]
        desc = classify_derived(vols)
        if desc is None:
            yield Fail(f"sub-{sub}: unrecognised DWI volume layout "
                       f"(volume {NDIR} is not a mean of the DW volumes)",
                       name=f"{base}.nii.gz")
            return

    # 3. gradient table: per-subject NITRC table, written only when it
    #    differs from the canonical (dataset-level, inherited) one
    prefix = "-".join(PurePosixPath(names[0]).name.split("-")[:3])
    tables = {}
    for ext, source in (("bval", "bvals.txt"), ("bvec", "bvecs.txt")):
        path = src / "IXI-DTI" / f"{prefix}-{source}"
        if not path.exists():
            continue
        rows = read_table(path)
        n = table_length(rows)
        if n == NDIR + 1 and count == NDIR + 1:
            text = format_table(rows)
        elif n == NDIR:
            text = None
        else:
            yield Fail(f"sub-{sub}: {path.name} has {describe_length(rows)}"
                       f", expected {NDIR}", name=f"{base}.{ext}")
            return
        if not same_table(ext, rows, canonical):
            tables[ext] = (path, text)
    if want_dwi:
        for ext, source in (("bval", "bvals.txt"), ("bvec", "bvecs.txt")):
            if canonical[ext] is None and ext not in tables:
                yield Fail(f"sub-{sub}: no valid {source} (neither "
                           f"dataset-level nor per-subject)",
                           name=f"{base}.{ext}")
                return

    if want_dwi:
        yield from roast_dwi_raw(ctx, archive, dwi, base, site, names,
                                 tables)
    if desc is not None and want_map:
        if not described:
            described.append(True)
            yield from derivative_description(drv)
        yield from roast_dwi_map(archive, out, sub, base, desc, names)


def roast_dwi_raw(ctx, archive, dwi, base, site, names, tables):
    yield from copy_sidecar(ctx, f"{site}/dwi.json", dwi / f"{base}.json")
    yield Action(
        archive.sources(*names), dwi / f"{base}.nii.gz",
        partial(stack_volumes, archive, names[:NDIR]), input="path",
    )
    for ext, (path, text) in tables.items():
        if text is None:
            yield CopyBytes(path, dwi / f"{base}.{ext}")
        else:
            yield WriteText(text, dwi / f"{base}.{ext}", src=path)



def roast_dwi_map(archive, out, sub, base, desc, names):
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


def participant_rows(path_xls, sites: dict[str, str]) -> list[list]:
    """
    Read the participants table from `IXI.xls`.

    Every subject with images available (`DATE_AVAILABLE`); columns read
    by position (`IXI_HEADER`); `site` from image names (n/a for
    subjects without local images).

    Parameters
    ----------
    path_xls : path
        `IXI.xls`
    sites : dict[str, str]
        `{subject label: site}`

    Returns
    -------
    list[list]
        Header and one row per subject

    Raises
    ------
    roast.tables.TableError
        If the workbook cannot be read (or xlrd is missing)
    """
    sheet = read_xls(path_xls, extra="ixi")

    def parse_date(date):
        return date if isinstance(date, str) else \
            xls_date(date, sheet.datemode)

    rows = [PARTICIPANTS_HEADER]
    for _, cells in sheet.rows:
        row = dict(zip(IXI_HEADER, cells))
        if row["DATE_AVAILABLE"] == 0:
            continue
        sub = f"{int(row['IXI_ID']):03d}"
        rows.append([
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
        ])
    return rows


# ----------------------------------------------------------------------
#   Python API
# ----------------------------------------------------------------------

#: Roast `<root>` (the `IXI` folder): `roast(root, keys=..., sub=...)`
roast = python_api(
    roast_ixi, KEYS, folder="IXI", width=WIDTH,
    example=2, spec=ROAST_SPEC, options=roast_options,
    keys_doc="as on the command line (`ROAST_SPEC`: `trace` only when "
             "named); a missing tarball (or folder) is an error only for "
             "the keys named here (`roast_options`)",
)
