"""
OASIS-III: unpack the harvested tarballs into a BIDS dataset.

Expected input
--------------
OASIS-3/sourcedata/
    OASIS3_data_files/
        demo.tar.gz                     (demographics)
        UDS{a1,a2,a3,a4d,a4g,a5,b1,...,b9,d1,d2}.tar.gz, pychometrics.tar.gz
    OAS3{04d}_MR_d{04d}/
        {anat,func,fmap,dwi,swi}{d}.tar.gz
        *Freesurfer*.tar.gz
    OAS3{04d}_CT_d{04d}/CT{d}.tar.gz
    OAS3{04d}_{FDG,PIB,AV45,AV1451}_d{04d}/pet{d}.tar.gz

Output (study layout)
---------------------
OASIS-3/
    rawbids/
        README, dataset_description.json, participants.{tsv,json},
        sessions.json
        phenotype/UDS_<form>.{tsv,json}
        sub-{04d}/sub-{04d}_sessions.tsv
        sub-{04d}/ses-d{04d}/{anat,func,perf,fmap,dwi,pet,ct}/
            sub-{04d}_ses-d{04d}_<entities>_<suffix>.{nii.gz,json,bval,bvec}
    derivatives/oasis-freesurfer/
        atlas-*_dseg.tsv
        sourcedata/sub-{04d}/ses-d{04d}/{mri,surf,label,...}/  (unpacked)
        sub-{04d}/ses-d{04d}/anat/...   (bidsified, `freesurfer.bidsify`)

With `--layout flat`, the content of `rawbids/` is in `OASIS-3/` itself.

Images are members of tarballs whose content is only known once they
are opened: each tarball is a *claim* on its session folder, and its
members are selected one by one (`ExtractMembers`). Sessions are parsed
from the member names. The participants and sessions tables are
written complete to `.brainspresso/orig/` (all subjects, all sessions
found in `sourcedata/`) and trimmed to what is on disk.
"""  # noqa: E501
import csv
import re
import tarfile
from functools import partial
from io import TextIOWrapper
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterator

from brainspresso.actions import Action, Claim, CopyBytes, CopyJSON, Fail
from brainspresso.actions import WriteJSON, WriteTSV
from brainspresso.actions.archive import ExtractMembers
from brainspresso.actions.runner import RoastContext, Shard
from brainspresso.actions.runner import make_context, run_roast
from brainspresso.bids.keys import KeyAliases
from brainspresso.datasets.OASIS.III.keys import allkeys
from brainspresso.freesurfer import bidsify as fs
from brainspresso.utils.io import read_json

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"
FS_NAME = "oasis-freesurfer"
DATA_FILES = "OASIS3_data_files"


def _anat(*suffixes, **entities):
    return {"datatypes": ["anat"], "suffixes": list(suffixes),
            **({"entities": entities} if entities else {})}


def _pet(tracer):
    return {"datatypes": ["pet"], "suffixes": ["pet"],
            "entities": {"acq": [tracer]}}


# Keys are matched against the output names (after `fix_name`). "meta"
# (README, participants, sessions) is a category, and the group of all
# metadata ("pheno" included).
KEYS = KeyAliases({**allkeys, "meta": {"meta", "pheno"}}, bids={
    "T1w": _anat("T1w"),
    "T2w": _anat("T2w"),
    "TSE": _anat("T2w", acq=["TSE"]),
    "FLAIR": _anat("FLAIR"),
    "T2star": _anat("t2starw", "T2starw", "T2star"),
    "angio": _anat("angio"),
    "swi": _anat("swi"),
    "bold": {"datatypes": ["func"], "suffixes": ["bold"]},
    "pasl": {"datatypes": ["perf"], "suffixes": ["pasl"]},
    "asl": {"datatypes": ["perf"], "suffixes": ["asl"]},
    "fmap": {"datatypes": ["fmap"]},
    "fieldmap": {"datatypes": ["fmap"]},
    "dwi": {"datatypes": ["dwi"]},
    "FDG": _pet("FDG"),
    "PIB": _pet("PIB"),
    "AV45": _pet("AV45"),
    "AV1451": _pet("AV1451"),
    "CT": {"datatypes": ["ct"], "suffixes": ["CT"]},
})

# Tarball name prefix -> datatype of its members (None: func or perf)
TARBALLS = {"anat": "anat", "swi": "anat", "func": None, "fmap": "fmap",
            "dwi": "dwi", "pet": "pet", "CT": "ct"}
PERF_SUFFIXES = ("asl", "pasl", "m0scan", "aslcontext")
EXTENSIONS = (".nii.gz", ".json", ".bval", ".bvec")


def roast(root, *, keys=(), exclude_keys=(), subs=(), exclude_subs=(),
          json="yes", ifexists="skip", layout=None, jobs=1, display=True):
    """Roast `<root>` (the `OASIS-3` folder). Subjects are numbers."""
    ctx = make_context(
        root, KEYS, layout=layout, select_keys=keys,
        exclude_keys=exclude_keys,
        subs=[f"{int(s):04d}" for s in subs],
        exclude_subs=[f"{int(s):04d}" for s in exclude_subs],
        json=json, ifexists=ifexists, templates=TEMPLATES,
    )
    return run_roast(roast_oasis3, ctx, jobs=jobs, display=display)


def roast_oasis3(ctx: RoastContext) -> Iterator:
    sessions = source_sessions(ctx)
    if ctx.wants("meta"):
        yield from roast_meta(ctx, sessions)
    if ctx.wants("pheno"):
        yield from roast_phenotypes(ctx)
    if ctx.wants("fs") or ctx.wants("fs-all"):
        root = ctx.layout.derivative(FS_NAME).root
        for action in fs.bidsify_toplevel(root, (5, 3)):
            action.tag = "fs"
            yield action
    subjects = [sub for sub in sorted(sessions)
                if ctx.selection.keeps_subject(sub)]   # early pruning
    ctx.progress(total=len(subjects), tag="subjects")
    for sub in subjects:
        folders = sorted(f for group in sessions[sub].values()
                         for f in group)
        yield Shard(roast_subject, sub, folders, tag="subjects")


def source_sessions(ctx: RoastContext) -> dict[str, dict[str, Path]]:
    """{sub: {session: [folders]}} of `OAS3<sub>_<category>_<session>/`"""
    out = {}
    for folder in sorted(ctx.layout.source.glob("OAS3*_*_d*")):
        match = re.fullmatch(r"OAS3(\d+)_\w+_(d\d+)", folder.name)
        if match and folder.is_dir():
            sub, ses = match.groups()
            out.setdefault(sub, {}).setdefault(ses, []).append(folder)
    return out


def fix_name(name: str, sub: str) -> str:
    """OASIS member name -> BIDS name"""
    substitutions = {
        "sess-": "ses-",
        f"sub-OAS3{sub}": f"sub-{sub}",
        "task-restingstateMB4": "task-rest_acq-MB4",
        "task-restingstate_": "task-rest_",
        "t2star": "t2starw",
    }
    for old, new in substitutions.items():
        name = name.replace(old, new)
    return name


# ----------------------------------------------------------------------
#   Images
# ----------------------------------------------------------------------

def roast_subject(ctx: RoastContext, sub: str, folders: list[Path]):
    """All tarballs of a subject (a shard)"""
    for folder in folders:
        for tarball in sorted(folder.glob("*.tar.gz")):
            if "Freesurfer" in tarball.name:
                if ctx.wants("fs") or ctx.wants("fs-all"):
                    yield from roast_freesurfer(ctx, sub, tarball)
                continue
            prefix = re.match(r"[A-Za-z]*", tarball.name)[0]
            if prefix not in TARBALLS:
                lg.debug(f"{tarball}: unknown kind of tarball, ignored")
                continue
            datatype = TARBALLS[prefix]
            ses = folder.name.split("_")[-1]
            claim = ctx.layout.raw / f"sub-{sub}" / f"ses-{ses}"
            yield ExtractMembers(
                tarball, Claim(claim / datatype if datatype else claim),
                partial(raw_path, ctx.layout.raw, sub, ses, datatype),
                tag=datatype or "func",
            )
    ctx.progress(advance=1, tag="subjects")


def raw_path(raw: Path, sub: str, ses: str, datatype: str | None,
             member: str) -> Path | None:
    """Output path of a member of an image tarball (None: ignored)"""
    name = PurePosixPath(member).name
    if not name.endswith(EXTENSIONS):
        return None
    name = fix_name(name, sub)
    match = re.search(r"(?:^|_)ses-([A-Za-z0-9]+)", name)
    ses = match[1] if match else ses
    if datatype is None:
        suffix = name.split(".")[0].split("_")[-1]
        datatype = "perf" if suffix in PERF_SUFFIXES else "func"
    return raw / f"sub-{sub}" / f"ses-{ses}" / datatype / name


def roast_freesurfer(ctx: RoastContext, sub: str, tarball: Path):
    """Unpack a FreeSurfer tree in `derivatives/<fs>/sourcedata/`,
    then bidsify it"""
    drv = ctx.layout.derivative(FS_NAME)
    ses = tarball.name.split(".")[0].split("_")[-1]
    src = drv.source / f"sub-{sub}" / f"ses-{ses}"
    everything = ctx.wants("fs-all")

    def rename(member: str) -> Path | None:
        if not everything and not member.endswith(fs.bidsifiable_outputs):
            return None
        return src.joinpath(*PurePosixPath(member).parts[6:])

    yield ExtractMembers(tarball, Claim(src), rename, tag="fs")
    t1 = f"bids:raw:sub-{sub}/ses-{ses}/anat/sub-{sub}_ses-{ses}_T1w.nii.gz"
    # (actions are created once the tree is unpacked: the runner runs
    # each action before asking for the next one)
    for action in fs.bidsify(src, drv.root / f"sub-{sub}" / f"ses-{ses}",
                             [t1], json="yes"):
        action.tag = "fs"
        yield action


# ----------------------------------------------------------------------
#   Participants and sessions
# ----------------------------------------------------------------------

PARTICIPANTS_HEADER_MAP = {
    'participant_id': 'OASISID',
    'sex': 'GENDER',
    'handedness': 'HAND',
    'age': 'AgeatEntry',
    'age_at_death': 'AgeatDeath',
    'educ': 'EDUC',
    'ses': 'SES',
    'race': 'racecode',
    'race_aian': 'AIAN',
    'race_nhpi': 'NHPI',
    'race_asian': 'ASIAN',
    'race_black': 'AA',
    'race_white': 'WHITE',
    'daddem': 'daddem',
    'momdem': 'momdem',
    'apoe': 'APOE',
}

RACE_MAP = {
    '0': 'U',   # Unknown
    '1': 'N',   # Native American (or Alaska)
    '2': 'A',   # Asian
    '3': 'H',   # Hawaiian or Pacific islanders
    '4': 'B',   # Black
    '5': 'W',   # White
    '6': 'M'    # More than one
}
YES_NO = {'0': 'N', '1': 'Y'}.get
YES_NO_UNKNOWN = {'0': 'N', '1': 'Y', '5': 'U'}.get

PARTICIPANTS_ROW_MAP = {
    'participant_id': lambda x: f"sub-{int(x[4:]):04d}",
    'sex': lambda x: {'1': 'M', '2': 'F'}.get(x, 'n/a'),
    'handedness': lambda x: x or 'n/a',
    'age': lambda x: x or 'n/a',
    'age_at_death': lambda x: x or 'n/a',
    'educ': lambda x: x or 'n/a',
    'ses': lambda x: x or 'n/a',
    'race': lambda x: RACE_MAP.get(x, 'n/a'),
    'race_aian': lambda x: YES_NO(x, 'n/a'),
    'race_nhpi': lambda x: YES_NO(x, 'n/a'),
    'race_asian': lambda x: YES_NO(x, 'n/a'),
    'race_black': lambda x: YES_NO(x, 'n/a'),
    'race_white': lambda x: YES_NO(x, 'n/a'),
    'daddem': lambda x: YES_NO_UNKNOWN(x, 'n/a'),
    'momdem': lambda x: YES_NO_UNKNOWN(x, 'n/a'),
    'apoe': lambda x: x or 'n/a'
}


def read_csv(tarball: Path, **kwargs) -> tuple[list[str], list[list[str]]]:
    """Header and rows of the (first) CSV file of a tarball"""
    with tarfile.open(tarball, "r:gz") as tar:
        member = next(m for m in tar if m.isfile())
        with TextIOWrapper(tar.extractfile(member), newline="") as f:
            rows = list(csv.reader(f, **kwargs))
    return (rows[0], rows[1:]) if rows else ([], [])


def roast_meta(ctx: RoastContext, sessions: dict) -> Iterator[Action]:
    raw, original = ctx.layout.raw, ctx.layout.original
    for name in ("README", "dataset_description.json", "participants.json",
                 "sessions.json"):
        copy = CopyJSON if name.endswith(".json") else CopyBytes
        yield copy(ctx.resource(name), raw / name, tag="meta")

    demo = ctx.layout.source / DATA_FILES / "demo.tar.gz"
    cdr = ctx.layout.source / DATA_FILES / "UDSb4.tar.gz"
    demo = demo if demo.exists() else None
    cdr = cdr if cdr.exists() else None
    if demo:
        yield WriteTSV(participants(demo), original(raw / "participants.tsv"),
                       src=demo, tag="meta")
    else:
        yield Fail(f"sourcedata/{DATA_FILES}/demo.tar.gz not found",
                   name="participants.tsv", tag="meta")
    ages = entry_ages(demo) if demo else {}
    visits = diagnoses(cdr) if cdr else {}
    for sub, labels in sorted(sessions.items()):
        labels = sorted(labels, key=lambda ses: int(ses[1:]))
        table = raw / f"sub-{sub}" / f"sub-{sub}_sessions.tsv"
        yield WriteTSV(
            session_rows(labels, ages.get(sub), visits.get(sub, [])),
            original(table), src=[p for p in (demo, cdr) if p], tag="meta",
        )


def participants(demo: Path) -> list[list[str]]:
    header, rows = read_csv(demo)
    out = [list(PARTICIPANTS_HEADER_MAP)]
    for row in rows:
        row = dict(zip(header, row))
        out.append([fix(row[PARTICIPANTS_HEADER_MAP[col]])
                    for col, fix in PARTICIPANTS_ROW_MAP.items()])
    return out


def entry_ages(demo: Path) -> dict[str, float]:
    """{sub: age at entry (day 0)}"""
    header, rows = read_csv(demo)
    out = {}
    for row in rows:
        row = dict(zip(header, row))
        if row.get("AgeatEntry"):
            out[row["OASISID"][4:]] = float(row["AgeatEntry"])
    return out


def diagnoses(cdr: Path) -> dict[str, list[tuple[int, str]]]:
    """{sub: [(days after entry, diagnosis code)]} from the CDR form"""
    header, rows = read_csv(cdr)
    header = [h.lower() for h in header]
    out = {}
    for row in rows:
        row = dict(zip(header, row))
        out.setdefault(row["oasisid"][4:], []).append(
            (int(row["days_to_visit"]), row.get("dx1_code") or "n/a")
        )
    return out


def session_rows(labels, age_at_entry, visits) -> list[list]:
    """Sessions table: diagnosis at the closest CDR visit (the earlier
    one if two are as close), age at the session"""
    rows = [["session_id", "pathology", "age"]]
    for ses in labels:
        delay = int(ses[1:])         # time after entry, in days
        diag = "n/a"
        if visits:
            diag = min(visits, key=lambda v: (abs(v[0] - delay), v[0]))[1]
        age = "n/a"
        if age_at_entry is not None:
            age = round((age_at_entry + delay / 365.25) * 1E4) / 1E4
        rows.append([f"ses-{ses}", diag, age])
    return rows


# ----------------------------------------------------------------------
#   Phenotypes
# ----------------------------------------------------------------------

PHENOFILES = {
    'a1': 'a1_demographics',
    'a2': 'a2_informant',
    'a3': 'a3_family_history',
    'a4d': 'a4d_medications',
    'a4g': 'a4g_medications',
    'a5': 'a5_health_history',
    'b1': 'b1_physical',
    'b2': 'b2_hiscvd',
    'b3': 'b3_updrs',
    'b4': 'b4_cdr',
    'b5': 'b5_npiq',
    'b6': 'b6_gds',
    'b7': 'b7_fas',
    'b8': 'b8_neurofind',
    'b9': 'b9_symptoms',
    'c1': 'c1_neuropsy',
    'd1': 'd1_diagnosis',
    'd2': 'd2_medical_conditions',
}
# Columns that BIDS names differently
PHENOCOLMAP = {
    'OASISID': 'participant_id',
    'OASIS_session_label': 'session_id',
    'days_to_visit': 'delay',
    'age at visit': 'age',
}


def roast_phenotypes(ctx: RoastContext) -> Iterator[Action]:
    for form, base in PHENOFILES.items():
        name = "pychometrics" if form == "c1" else f"UDS{form}"
        tarball = ctx.layout.source / DATA_FILES / f"{name}.tar.gz"
        if not tarball.exists():
            lg.debug(f"{tarball} not found")
            continue
        out = ctx.layout.phenotype / f"UDS_{base}"
        medications = form.startswith("a4")
        csv_opt = dict(quotechar='"') if medications else {}
        header, rows = read_csv(tarball, **csv_opt)
        header = [fix_pheno_column(h, medications) for h in header]
        rows = (fix_pheno_row(row, header) for row in rows)
        yield WriteTSV([header, *rows], f"{out}.tsv", src=tarball,
                       tag="pheno", **({"escapechar": "\\"}
                                       if medications else {}))
        yield WriteJSON(phenotype_json(ctx, form, header), f"{out}.json",
                        src=tarball, tag="pheno")


def fix_pheno_column(name: str, medications: bool = False) -> str:
    if name in PHENOCOLMAP:
        return PHENOCOLMAP[name]
    if medications:
        if name == 'INRASEC':
            return 'INRACESC'
        if name.startswith('meds_'):
            return f'MEDNAME{int(name.split("_")[-1]):02d}'
        if name.startswith('drug'):
            return f'DRUGID{int(name.split("drug")[-1]):02d}'
    return name.upper()


def fix_pheno_row(row: list[str], header: list[str]) -> list:
    out = []
    for value, column in zip(row, header):
        if column == 'participant_id':
            value = f"sub-{int(value[4:]):04d}"
        elif column == 'session_id':
            value = "ses-" + value.split('_')[-1]
        elif not value:
            value = 'n/a'
        elif column == 'delay':
            value = int(value)
        elif column == 'age':
            value = float(value)
        elif column in ('DX1', 'DX2', 'DX3', 'DX4', 'DX5') and \
                value in ('A', '.'):
            value = 'n/a'
        out.append(value)
    return out


def phenotype_json(ctx: RoastContext, form: str, header: list[str]) -> dict:
    """
    Data dictionary of a phenotype table, from the templates of its form
    (OASIS-specific, UDSv3, UDSv2; UDSv2 first for medications), each
    column taken from the first template that describes it.
    """
    base = PHENOFILES[form]
    if form.startswith('a4'):
        names = ['UDSv2/UDSv2_a4_medications.json',
                 'UDSv3/UDSv3_a4_medications.json']
    else:
        v3 = 'c2_neuropsy' if form == 'c1' else base
        names = [f'UDSvOAS3/UDS_{v3}.json', f'UDSv3/UDSv3_{v3}.json',
                 f'UDSv2/UDSv2_{base}.json']
    templates = []
    for name in names:
        path = ctx.resource(f'phenotype/{name}')
        version = name.split('/')[0][3:]           # vOAS3, v3, v2
        if path is not None:
            templates.append((version, expand_template(form,
                                                       read_json(path))))
    if not templates:
        raise RuntimeError(f'No template json for {form}')

    def lookup(key):
        for version, template in templates:
            if key in template:
                return version, template[key]
        if key.startswith('DRUGID'):
            return None, lookup('DRUGID')[1]
        if key.startswith('MEDNAME'):
            return None, {"Description": "What is the name of the "
                                         "medication?"}
        return None, {"Description": "[WARNING] Unknown phenotype"}

    out = {key: lookup(key)[1]
           for key in ("MeasurementToolMetadata", "delay", "age")}
    versions = set()
    for key in header:
        if key not in ("participant_id", "session_id", "delay", "age"):
            version, out[key] = lookup(key)
            versions.add(version)
    # The description names the version of the template used as a base
    first = templates[0][0]
    other = {"v3": "v2", "v2": "v3"}.get(first)
    if other in versions:
        meta = dict(out["MeasurementToolMetadata"])
        meta["Description"] = meta["Description"].replace(
            first, "v2+v3")
        out["MeasurementToolMetadata"] = meta
    return out


def expand_template(form: str, obj: dict) -> dict:
    """Repeat the `{d}` entries of a template (parents/siblings, DX)"""
    if form not in ('a3', 'b4'):
        return obj
    for key, val in dict(obj).items():
        if "{d}" not in key:
            continue
        if form == 'a3':
            assert key.startswith(("SIB", "KID", "REL"))
            nb = 20 if key.startswith("SIB") else 15
        else:
            assert key.startswith("DX")
            nb = 5
        del obj[key]
        desc = val.pop("Description")
        for d in range(nb):
            obj[key.format(d=d)] = {'Description': desc.format(d=d), **val}
    return obj
