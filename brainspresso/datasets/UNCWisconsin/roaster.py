"""
UNC-Wisconsin: convert the harvested NRRD files to BIDS.

Expected input
--------------
UNC-Wisconsin/sourcedata/
    folders.json                    (subject/session metadata)
    scan_data/<sub>/<age>months/sMRI/ORIG/<sub>_<age>months_T1_ORIG.nrrd
    scan_data/<sub>/<age>months/sMRI/ORIG/<sub>_<age>months_T2_ORIG.nrrd
    scan_data/<sub>/<age>months/DTI/ORIG/*/<sub>_<age>months_dwi.nrrd

Output (study layout)
---------------------
UNC-Wisconsin/
    dataset_description.json        (DatasetType "study")
    rawbids/
        README, dataset_description.json
        participants.tsv, participants.json, sessions.json
        sub-<sub>/sub-<sub>_sessions.tsv
        sub-<sub>/ses-<age>months/anat/sub-<sub>_ses-<age>months_T1w.nii.gz
        sub-<sub>/ses-<age>months/anat/sub-<sub>_ses-<age>months_T2w.nii.gz
        sub-<sub>/ses-<age>months/dwi/sub-<sub>_ses-<age>months_dwi.{nii.gz,bval,bvec}
    derivatives/unc-wisconsin/      (SourceDatasets: ../../rawbids/)
        README, dataset_description.json, .bidsignore
        sub-<sub>/ses-<age>months/{anat,dwi}/...

With `--layout flat`, the content of `rawbids/` is in `UNC-Wisconsin/`
itself. Participants and sessions tables are written complete to
`.brainspresso/orig/` and trimmed to what is on disk by the runner.
"""  # noqa: E501
import json
import re
from pathlib import Path
from typing import Iterator

from brainspresso.actions import Action, Fail, WriteJSON, WriteTSV
from brainspresso.roast import RoastContext, copy_templates, roast_dataset
from brainspresso.roast import subject_shards
from brainspresso.bids.keys import KeyAliases
from brainspresso.datasets.UNCWisconsin.keys import allkeys
from brainspresso.utils.nrrd import nrrd_to_nifti

TEMPLATES = Path(__file__).parent / "templates"

# Derivatives -> derivatives/unc-wisconsin/
DERIV_NAME = "unc-wisconsin"
DERIV_KEYS = ("reg", "dti-qc", "dti-proc")

KEYS = KeyAliases(allkeys, bids={
    "T1w": {"suffixes": ["T1w"]},
    "T2w": {"suffixes": ["T2w"]},
    "dwi": {"suffixes": ["dwi"]},
}, folders={key: [f"derivatives/{DERIV_NAME}"] for key in DERIV_KEYS})

SPECIES = "Macaca mulatta"
ANAT = {"T1": "T1w", "T2": "T2w"}

SPACE = "UNCEmoryWisconsin12mo"     # UNC-Emory-Wisconsin 12 month atlas
ATLAS_URL = "https://www.nitrc.org/projects/primate_atlas/"
# (key, folder in session, file stem after "<sub>_<ses>_", datatype,
#  output entities+suffix, is_dwi)
DERIVATIVES = [
    ("reg", "sMRI/Reg2Atlas", "T1_Reg2Atlas",
     "anat", f"space-{SPACE}_T1w", False),
    ("reg", "sMRI/Reg2Atlas", "T2_Reg2Atlas",
     "anat", f"space-{SPACE}_T2w", False),
    ("reg", "sMRI/Reg2Atlas/AUTO_MASK", "Reg2Atlas_AutoMask",
     "anat", f"space-{SPACE}_desc-auto_mask", False),
    ("reg", "sMRI/Reg2Atlas/Manual_Brain_Mask", "Reg2Atlas_ManualBrainMask",
     "anat", f"space-{SPACE}_desc-brain_mask", False),
    ("reg", "sMRI/Reg2Atlas/Manual_Stripped",
     "T1_Reg2Atlas_ManualBrainMask_Stripped",
     "anat", f"space-{SPACE}_desc-brain_T1w", False),
    ("reg", "sMRI/Reg2Atlas/Manual_Stripped",
     "T2_RegT1_Reg2Atlas_ManualBrainMask_Stripped",
     "anat", f"space-{SPACE}_desc-brain_T2w", False),
    ("dti-qc", "DTI/AUTOQC", "AutoQCed_DWI",
     "dwi", "desc-autoqc_dwi", True),
    ("dti-qc", "DTI/AUTOQC", "AutoQCed_Baseline",
     "dwi", "param-b0_desc-autoqc_dwimap", False),
    ("dti-qc", "DTI/AUTOQC", "AutoQCed_IDWI",
     "dwi", "param-idwi_desc-autoqc_dwimap", False),
    ("dti-qc", "DTI/AUTOQC", "AutoQCed_DTI_FA",
     "dwi", "model-tensor_param-fa_desc-autoqc_dwimap", False),
    ("dti-qc", "DTI/AUTOQC", "AutoQCed_DTI_MD",
     "dwi", "model-tensor_param-md_desc-autoqc_dwimap", False),
    ("dti-proc", "DTI/PROCESSED", "B0_antsManualBM",
     "dwi", "param-b0_desc-brain_dwimap", False),
    ("dti-proc", "DTI/PROCESSED", "IDWI_antsManualBM",
     "dwi", "param-idwi_desc-brain_dwimap", False),
    ("dti-proc", "DTI/PROCESSED", "FA_antsManualBM",
     "dwi", "model-tensor_param-fa_desc-brain_dwimap", False),
    ("dti-proc", "DTI/PROCESSED", "MD_antsManualBM",
     "dwi", "model-tensor_param-md_desc-brain_dwimap", False),
    ("dti-proc", "DTI/MASKMANUAL", "brainmask_final",
     "dwi", "desc-brain_mask", False),
]


def roast(root, **options):
    """Roast `<root>` (the `UNC-Wisconsin` folder). Options: those of
    the command line (`keys`, `subs`, `exclude_subs`, `sessions`,
    `ifexists`, `layout`), `jobs` and `display`."""
    return roast_dataset(roast_uncwisc, root, KEYS, templates=TEMPLATES,
                         **options)


def roast_uncwisc(ctx: RoastContext) -> Iterator:
    meta = folders(ctx)
    if ctx.wants("meta"):
        yield from roast_meta(ctx, meta)
    if any(map(ctx.wants, DERIV_KEYS)):
        drv = ctx.layout.derivative(DERIV_NAME)
        yield from copy_templates(ctx, {
            "derivatives_description.json": drv.description,
            "derivatives_README": drv.root / "README",
            "derivatives_bidsignore": drv.bidsignore,
        })
    subjects = [p.name for p in _dirs(ctx.source / "scan_data")]
    yield from subject_shards(ctx, subjects, roast_subject)


# ----------------------------------------------------------------------
#   Metadata
# ----------------------------------------------------------------------

def folders(ctx: RoastContext) -> dict:
    """Girder metadata of subject and session folders"""
    path = ctx.source / "folders.json"
    return json.loads(path.read_text()) if path.exists() else {}


def roast_meta(ctx: RoastContext, meta: dict) -> Iterator[Action]:
    raw = ctx.layout.raw
    yield from copy_templates(ctx, ["README", "dataset_description.json",
                                    "participants.json", "sessions.json"])
    if not meta:
        yield Fail("sourcedata/folders.json not found: harvest with "
                   "--keys meta", name="folders.json", tag="meta")
        return
    src = ctx.source / "folders.json"
    original = ctx.layout.original
    yield WriteTSV(participants(meta), original(raw / "participants.tsv"),
                   src=src, tag="meta")
    for sub in sorted({key.split("/")[0] for key in meta}):
        table = raw / f"sub-{sub}" / f"sub-{sub}_sessions.tsv"
        yield WriteTSV(sessions(meta, sub), original(table), src=src,
                       tag="meta")


def participants(meta: dict) -> list[list[str]]:
    rows = [["participant_id", "species", "sex", "birth_date"]]
    for key, m in sorted(meta.items()):
        if m.get("folder_type") != "subject":
            continue
        rows.append([f"sub-{m.get('subject_id') or key}", SPECIES,
                     m.get("sex") or "n/a", m.get("dob") or "n/a"])
    return rows


def sessions(meta: dict, sub: str) -> list[list[str]]:
    rows = [["session_id", "acq_time", "age", "weight"]]
    for key, m in sorted(meta.items()):
        parts = key.split("/")
        if m.get("folder_type") != "scan" or parts[0] != sub:
            continue
        weight = m.get("scan_weight_kg")
        rows.append([f"ses-{parts[1]}", m.get("scan_date") or "n/a",
                     _age_months(m.get("scan_age") or parts[1]),
                     weight if weight not in (None, "") else "n/a"])
    return rows


def _age_months(label: str) -> str:
    """'12months' -> '12'; '02weeks' -> '0.46' (in months)"""
    m = re.match(r"(\d+(?:\.\d+)?)\s*(month|week|day)s?$", str(label))
    if not m:
        return "n/a"
    value = float(m.group(1))
    value = {"month": value, "week": value * 7 / 30.4375,
             "day": value / 30.4375}[m.group(2)]
    return f"{value:g}" if value == int(value) else f"{value:.2f}"


# ----------------------------------------------------------------------
#   Images (NRRD -> NIfTI)
# ----------------------------------------------------------------------

def roast_subject(ctx: RoastContext, sub: str) -> Iterator[Action]:
    """All sessions of a subject (a shard)"""
    for sesdir in _dirs(ctx.source / "scan_data" / sub):
        yield from roast_session(ctx, sub, sesdir.name, sesdir)


def roast_session(ctx, sub: str, ses: str, sesdir: Path) -> Iterator[Action]:
    base = f"sub-{sub}_ses-{ses}"
    out = ctx.layout.raw / f"sub-{sub}" / f"ses-{ses}"

    for contrast, suffix in ANAT.items():
        src = sesdir / "sMRI" / "ORIG" / f"{sub}_{ses}_{contrast}_ORIG.nrrd"
        if src.exists():
            yield _convert(src, out / "anat" / f"{base}_{suffix}", suffix)

    # one DWI series per session (Girder item folder holding the file)
    dwis = sorted(p for p in (sesdir / "DTI" / "ORIG").glob("**/*.nrrd")
                  if p.is_file())
    if dwis:
        yield _convert(dwis[0], out / "dwi" / f"{base}_dwi", "dwi", dwi=True)

    drv = ctx.layout.derivative(DERIV_NAME).root / f"sub-{sub}" / f"ses-{ses}"
    for key, folder, stem, datatype, suffix, is_dwi in DERIVATIVES:
        src = sesdir / folder / f"{sub}_{ses}_{stem}.nrrd"
        if not ctx.wants(key) or not src.exists():
            continue
        name = drv / datatype / f"{base}_{suffix}"
        # b-values/vectors are named without `desc` so that they
        # validate (BIDS inheritance makes them apply to the single
        # derivative DWI series of the session)
        yield _convert(src, name, key,
                       dwi=drv / datatype / f"{base}_dwi" if is_dwi else None)
        yield WriteJSON(deriv_sidecar(suffix), f"{name}.json", src=src,
                        tag=key)


def _convert(src: Path, stem: Path, tag: str, dwi=None) -> Action:
    """NRRD -> `<stem>.nii.gz` (+ `<dwi>.bval`, `<dwi>.bvec`)"""
    nii = Path(f"{stem}.nii.gz")
    if not dwi:
        return Action(src, nii, lambda path: nrrd_to_nifti(src, path),
                      input="path", tag=tag)
    dwi = stem if dwi is True else dwi
    # one conversion writes the three files: a data action (`--json
    # only` writes none of them)
    return Action(
        src, [nii, Path(f"{dwi}.bval"), Path(f"{dwi}.bvec")],
        lambda nii, bval, bvec: nrrd_to_nifti(src, nii, bval=bval,
                                              bvec=bvec),
        input="path", tag=tag, kind="data",
    )


def deriv_sidecar(suffix: str) -> dict:
    """Required BIDS derivative metadata for an output file"""
    out = {}
    if "space-" in suffix:
        out["SpatialReference"] = ATLAS_URL
    if suffix.endswith("_mask"):
        out["Type"] = "Brain"
    else:
        out["SkullStripped"] = "desc-brain" in suffix
    return out


# ----------------------------------------------------------------------

def _dirs(path: Path) -> list[Path]:
    return sorted(p for p in path.iterdir() if p.is_dir()) \
        if path.is_dir() else []

