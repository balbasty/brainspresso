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
        sub-{04d}/ses-d{04d}/anat/...   (converted to BIDS, `freesurfer.bidsify`)

With `--layout flat`, the content of `rawbids/` is in `OASIS-3/` itself.

Images are members of tarballs whose content is only known once they
are opened: each tarball is a *claim* on its session folder, and its
members are selected one by one (`ExtractMembers`). Sessions are parsed
from the member names. The participants and sessions tables are
written complete to `.brainspresso/orig/` (all subjects, all sessions
found in `sourcedata/`) and trimmed to what is on disk.
"""  # noqa: E501
import math
import re
from collections import Counter
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterator

from brainspresso.actions import Action, Claim, Fail, WriteJSON
from brainspresso.actions.archive import ExtractMembers
from brainspresso.bids.keys import KeyAliases
from brainspresso.datasets.oasis.iii.keys import allkeys
from brainspresso.freesurfer import bidsify as fs
from brainspresso.freesurfer.archive import fixed_root, tree_actions
from brainspresso.roast import (
    RoastContext,
    Shard,
    archives,
    attempt,
    copy_templates,
    subject_shards,
    tables,
)
from brainspresso.roast.run import python_api
from brainspresso.roast.tables import (
    NANS,
    Sheet,
    TableError,
    clean,
    integer,
    row_warning,
    table_action,
)
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.io import read_json
from brainspresso.utils.path import sorted_paths

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"
FS_NAME = "oasis-freesurfer"
FS_VERSION = (5, 3)             # FreeSurfer of the OASIS-3 outputs
#: Path of a member of a FreeSurfer tarball in its tree
#: (`<6 folders>/mri/...`)
FS_RELPATH = fixed_root(6)
DATA_FILES = "OASIS3_data_files"
WIDTH = 4               # subject labels: `sub-0001`


def _anat(*suffixes, **entities):
    return {"datatype": ["anat"], "suffix": list(suffixes),
            **({"entities": entities} if entities else {})}


def _pet(tracer):
    return {"datatype": ["pet"], "suffix": ["pet"],
            "entities": {"acq": [tracer]}}


# Keys are matched against the output names (after `fix_name`), in
# this order (first match: `TSE` before `T2w`). "meta" (README,
# participants, sessions) is a category, and the group of all metadata
# ("pheno" included); "all" is the group of every key (`--keys all`).
KEYS = KeyAliases({"all": {**allkeys, "meta": {"meta", "pheno"}}}, bids={
    "T1w": _anat("T1w"),
    "TSE": _anat("T2w", acq=["TSE"]),
    "T2w": _anat("T2w"),
    "FLAIR": _anat("FLAIR"),
    "T2star": _anat("t2starw", "T2starw", "T2star"),
    "angio": _anat("angio"),
    "swi": _anat("swi"),
    "bold": {"datatype": ["func"], "suffix": ["bold"]},
    "pasl": {"datatype": ["perf"], "suffix": ["pasl"]},
    "asl": {"datatype": ["perf"], "suffix": ["asl"]},
    "fmap": {"datatype": ["fmap"]},
    "dwi": {"datatype": ["dwi"]},
    "FDG": _pet("FDG"),
    "PIB": _pet("PIB"),
    "AV45": _pet("AV45"),
    "AV1451": _pet("AV1451"),
    "CT": {"datatype": ["ct"], "suffix": ["CT"]},
}, folders={"fs": [f"derivatives/{FS_NAME}"],
            "fs-all": [f"derivatives/{FS_NAME}"],
            "pheno": ["phenotype"]})

# Tarball name prefix -> datatype of its members (None: func or perf)
TARBALLS = {"anat": "anat", "swi": "anat", "func": None, "fmap": "fmap",
            "dwi": "dwi", "pet": "pet", "CT": "ct"}
PERF_SUFFIXES = ("asl", "pasl", "m0scan", "aslcontext")
EXTENSIONS = (".nii.gz", ".json", ".bval", ".bvec")
#: Keys that a subject shard can write (`roast.scope`): all but the
#: metadata, which the top-level roaster writes
SCOPE_KEYS = tuple(sorted(KEYS.leaves - {"meta", "pheno"}))


def roast_oasis3(ctx: RoastContext) -> Iterator:
    sessions = source_sessions(ctx)
    if ctx.wants("meta"):
        yield from roast_meta(ctx, sessions)
    if ctx.wants("pheno"):
        yield from roast_phenotypes(ctx)
    if ctx.wants_any(("fs", "fs-all")):
        yield Shard(roast_freesurfer_tables, tag="fs")
    yield from subject_shards(ctx, sorted(sessions), roast_subject,
                              inputs=partial(subject_tarballs, ctx),
                              scope_keys=SCOPE_KEYS)


def source_sessions(ctx: RoastContext) -> dict[str, dict[str, Path]]:
    """{sub: {session: [folders]}} of `OAS3<sub>_<category>_<session>/`"""
    out = {}
    for folder in sorted_paths(ctx.source.glob("OAS3*_*_d*")):
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

def subject_folders(ctx: RoastContext, sub: str) -> list[Path]:
    """Folders `OAS3<sub>_<category>_<session>/` of a subject"""
    return sorted_paths(f for f in ctx.source.glob(f"OAS3{sub}_*_d*")
                        if f.is_dir())


def subject_tarballs(ctx: RoastContext, sub: str) -> list[Path]:
    """Tarballs of a subject (inputs of its shard)"""
    return [tarball for folder in subject_folders(ctx, sub)
            for tarball in sorted_paths(folder.glob("*.tar.gz"))]


def roast_subject(ctx: RoastContext, sub: str):
    """All tarballs of a subject (a shard)"""
    for folder in subject_folders(ctx, sub):
        for tarball in sorted_paths(folder.glob("*.tar.gz")):
            if "Freesurfer" in tarball.name:
                if ctx.wants_any(("fs", "fs-all")):
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
                root=ctx.layout.root, tag=datatype or "func",
            )


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


def roast_freesurfer_tables(ctx: RoastContext) -> Iterator[Action]:
    """Description and lookup tables of the FreeSurfer derivative"""
    drv = ctx.layout.derivative(FS_NAME)
    yield WriteJSON(freesurfer_description(drv), drv.description,
                    tag="derivatives")
    yield from fs.bidsify_toplevel(drv.root, FS_VERSION)


def freesurfer_description(drv) -> dict:
    """Describe the FreeSurfer outputs (`derivatives/<fs>`)."""
    desc = drv.dataset_description()
    desc["Name"] = "OASIS-3 - FreeSurfer"
    desc["GeneratedBy"] = [{
        "Name": "FreeSurfer",
        "Version": ".".join(map(str, FS_VERSION)),
        "Description": "FreeSurfer outputs distributed with the dataset.",
    }, {
        "Name": "brainspresso",
        "Description": "Conversion of the OASIS-3 tarballs to BIDS.",
        "CodeURL": "https://github.com/balbasty/brainspresso",
    }]
    return desc


def roast_freesurfer(ctx: RoastContext, sub: str,
                     tarball: Path) -> Iterator[Shard]:
    """
    Unpack a FreeSurfer tree, then convert it (in a shard).

    The tree (members `<6 folders>/mri/...`) is unpacked in
    `derivatives/<fs>/sourcedata/sub-<sub>/ses-<ses>/`: only its files
    that BIDS describes, unless `fs-all`. With `--sidecars only` (and not
    `fs-all`), nothing is unpacked: the sidecars are written from the
    names of the members.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    sub : str
        Subject label
    tarball : Path
        FreeSurfer tarball of a session (`..._<ses>.tar.gz`)

    Yields
    ------
    Shard
        `freesurfer.archive.tree_actions`: the unpacking (`staging`),
        then the converted files
    """
    drv = ctx.layout.derivative(FS_NAME)
    ses = tarball.name.split(".")[0].split("_")[-1]
    everything = ctx.wants("fs-all")
    t1 = f"bids:raw:sub-{sub}/ses-{ses}/anat/sub-{sub}_ses-{ses}_T1w.nii.gz"
    session = PurePosixPath(f"sub-{sub}", f"ses-{ses}")
    # (a shard of its own, in the progress group `fs`)
    yield Shard(tree_actions, tarball, drv.source / session,
                drv.root / session, [t1],
                relpath=FS_RELPATH, everything=everything,
                list_only=ctx.json == "only" and not everything, tag="fs")


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


def read_csv(tarball: Path, **kwargs) -> Sheet:
    """
    Read the (first) CSV file of a tarball.

    Parameters
    ----------
    tarball : Path
        `OASIS3_data_files/<form>.tar.gz`
    **kwargs
        Options of `roast.tables.read_csv`

    Returns
    -------
    Sheet
        Its header and rows (blank rows dropped), named after the tarball

    Raises
    ------
    roast.archives.ArchiveError
        If the tarball cannot be read
    roast.tables.TableError
        If it holds no file, or its CSV file cannot be read
    """
    with archives.reading(tarball, opener=TarArchive) as tar:
        member = next(iter(tar), None)
        if member is None:
            raise TableError(f"{tarball.name}: no CSV file")
        with member.open() as f:
            return tables.read_csv(f, name=tarball.name, **kwargs)


#: Errors of a CSV tarball that cannot be read (`attempt`; a `TableError`
#: is a `ValueError`, and so are the numbers that cannot be read)
READ_ERRORS = (archives.ArchiveError, ValueError)


def roast_meta(ctx: RoastContext, sessions: dict) -> Iterator[Action]:
    """
    Write the dataset files, the participants and sessions tables.

    The participants table is read when its action runs (a demographics
    tarball that cannot be read is an error of `participants.tsv`); the
    ages and diagnoses that the sessions tables need are read first
    (an error of the sessions tables if they cannot be).
    """
    raw = ctx.layout.raw
    yield from copy_templates(ctx, ["README", "dataset_description.json",
                                    "participants.json", "sessions.json"])

    demo = ctx.source / DATA_FILES / "demo.tar.gz"
    cdr = ctx.source / DATA_FILES / "UDSb4.tar.gz"
    demo = demo if demo.exists() else None
    cdr = cdr if cdr.exists() else None
    if demo:
        yield table_action(ctx, raw / "participants.tsv",
                           partial(participants, demo), demo)
    else:
        yield Fail(f"sourcedata/{DATA_FILES}/demo.tar.gz not found",
                   name="participants.tsv", tag="meta")
    ages = visits = {}
    if demo:
        ages, error = attempt(entry_ages, demo, fail_name="sessions.tsv",
                              fail_tag="meta", fail_what=demo.name,
                              fail_errors=READ_ERRORS)
        if error:
            yield error
            return
    if cdr:
        visits, error = attempt(diagnoses, cdr, fail_name="sessions.tsv",
                                fail_tag="meta", fail_what=cdr.name,
                                fail_errors=READ_ERRORS)
        if error:
            yield error
            return
    for sub, labels in sorted(sessions.items()):
        labels = sorted(labels, key=lambda ses: int(ses[1:]))
        table = raw / f"sub-{sub}" / f"sub-{sub}_sessions.tsv"
        yield table_action(
            ctx, table,
            session_rows(labels, ages.get(sub), visits.get(sub, [])),
            [p for p in (demo, cdr) if p])


def participants(demo: Path) -> list[list[str]]:
    """
    Read the participants table (every subject of the demographics).

    Rows whose `OASISID` is not one are skipped with a warning.

    Raises
    ------
    roast.tables.TableError
        If the CSV file cannot be read or lacks a column
    """
    sheet = read_csv(demo)
    index = {col: sheet.require(name)
             for col, name in PARTICIPANTS_HEADER_MAP.items()}
    out = [list(PARTICIPANTS_HEADER_MAP)]
    for line, row in sheet.rows:
        sid = str(sheet.cell(row, index["participant_id"])).strip()
        if not re.fullmatch(r"OAS3\d+", sid):
            row_warning(sheet, line, f"row skipped (subject {sid!r})")
            continue
        out.append([fix(str(sheet.cell(row, index[col])).strip())
                    for col, fix in PARTICIPANTS_ROW_MAP.items()])
    return out


def entry_ages(demo: Path) -> dict[str, float]:
    """{sub: age at entry (day 0)}"""
    sheet = read_csv(demo)
    sid, age = sheet.require("OASISID"), sheet.require("AgeatEntry")
    out = {}
    for _, row in sheet.rows:
        if str(sheet.cell(row, age)).strip():
            out[str(sheet.cell(row, sid)).strip()[4:]] = \
                float(sheet.cell(row, age))
    return out


def diagnoses(cdr: Path) -> dict[str, list[tuple[int, str]]]:
    """{sub: [(days after entry, diagnosis code)]} from the CDR form"""
    sheet = read_csv(cdr)
    sid, days = sheet.require("OASISID"), sheet.require("days_to_visit")
    code = sheet.find("dx1_code")
    out = {}
    for _, row in sheet.rows:
        sub, day = sheet.cell(row, sid), sheet.cell(row, days)
        if not sub or not day:
            continue                    # (truncated row)
        out.setdefault(sub[4:], []).append(
            (int(day), sheet.cell(row, code) or "n/a"))
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
    """
    Phenotype tables (one row per visit) and their data dictionaries.

    Each tarball is read when the tables are yielded (one that cannot be
    is an error of its table only). The tables are written complete to
    `.brainspresso/orig/`, with the
    `session_id` of the day of each visit (`ses-d<days>`); the visible
    tables are derived from them after every roast (`trim_originals`,
    `bids.tables.trim_tables`), as the participants table: the rows of
    the subjects on disk, and `session_id` `n/a` when the subject has no
    session that day in the raw dataset (the day is kept in `delay`).
    So they follow what is on disk, whatever `--if-exists` says.
    Truncated rows are skipped (`complete_rows`); cells are cleaned
    (`roast.tables.clean`: `"` and tabs replaced, no quoting needed).
    """
    for form, base in PHENOFILES.items():
        name = "pychometrics" if form == "c1" else f"UDS{form}"
        tarball = ctx.source / DATA_FILES / f"{name}.tar.gz"
        if not tarball.exists():
            lg.debug(f"{tarball} not found")
            continue
        out = ctx.layout.phenotype / f"UDS_{base}"
        medications = form.startswith("a4")
        table = f"UDS_{base}.tsv"
        sheet, error = attempt(read_csv, tarball, fail_name=table,
                               fail_tag="pheno", fail_what=tarball.name,
                               fail_errors=READ_ERRORS)
        if error is None:
            header, error = attempt(phenotype_header, sheet, medications,
                                    fail_name=table, fail_tag="pheno")
        if error is None:
            sidecar, error = attempt(phenotype_json, ctx, form, header,
                                     fail_name=f"UDS_{base}.json",
                                     fail_tag="pheno")
        if error:
            yield error
            continue
        name = f"phenotype/{table}"
        yield table_action(ctx, Path(f"{out}.tsv"),
                           partial(phenotype_rows, sheet, header, name),
                           tarball, tag="pheno", name=name, sidecar=sidecar)
        yield WriteJSON(sidecar, f"{out}.json", src=tarball, tag="pheno")


def phenotype_header(sheet: Sheet, medications: bool = False) -> list[str]:
    """
    Columns of a phenotype table (BIDS names, cleaned once: the table
    and its data dictionary use the same names).

    Raises
    ------
    roast.tables.TableError
        If a column name cannot be converted, or there is no subject
        column (`OASISID`)
    """
    header = []
    for column in sheet.header:
        try:
            header.append(clean(fix_pheno_column(column, medications),
                                nans=()))
        except ValueError as e:
            raise TableError(f"{sheet.name}: column {column!r} cannot be "
                             f"read ({e})") from e
    if "participant_id" not in header:
        raise TableError(f"{sheet.name}: no column 'OASISID'")
    return header


def phenotype_rows(sheet: Sheet, header: list[str], name: str) -> list:
    """
    Rows of a phenotype table (run by its action).

    A row whose subject is not an OASIS-III ID is skipped; a cell of
    `delay` or `age` that is not a number is `n/a`: one warning per
    table and column, with a count.
    """
    bad = Counter()
    rows = [header]
    for row in complete_rows((row for _, row in sheet.rows), header, name):
        row = fix_pheno_row(row, header, bad)
        if row is not None:
            rows.append(row)
    for column, count in sorted(bad.items()):
        what = ("row(s) skipped (subject not an OASIS-III ID)"
                if column == "participant_id" else
                f"cell(s) of {column} not a number (n/a)")
        lg.warning(f"{name}: {count} {what}")
    return rows


def complete_rows(rows, header: list[str], name: str):
    """
    The rows that name a participant (and a visit, if the table has
    them); the others (truncated rows) are skipped, warned once with a
    count when the rows are consumed.
    """
    ids = [header.index(c) for c in ("participant_id", "session_id")
           if c in header]
    skipped = 0
    for row in rows:
        if ids and (len(row) <= max(ids) or
                    not all(row[i].strip() for i in ids)):
            skipped += 1
            continue
        yield row + [""] * (len(header) - len(row))
    if skipped:
        lg.warning(f"{name}: {skipped} row(s) skipped (truncated: no "
                   f"participant or visit)")


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


def fix_pheno_row(row: list[str], header: list[str],
                  bad: Counter | None = None) -> list | None:
    """
    Convert a row of a phenotype table.

    Parameters
    ----------
    row : list[str]
        Its cells
    header : list[str]
        Its columns (`phenotype_header`)
    bad : Counter, optional
        Incremented per column whose cell cannot be read

    Returns
    -------
    list | None
        The row (other cells: `roast.tables.clean`, by `table_action`);
        None if its subject is not an OASIS-III ID
    """
    bad = Counter() if bad is None else bad
    out = []
    for value, column in zip(row, header):
        value = value.strip()
        if column == 'participant_id':
            match = re.fullmatch(r"OAS3(\d+)", value)
            if not match:
                bad[column] += 1
                return None
            value = f"sub-{int(match[1]):04d}"
        elif column == 'session_id':
            value = "ses-" + value.split('_')[-1]
        elif not value or (column in ('delay', 'age') and value in NANS):
            value = 'n/a'
        elif column in ('delay', 'age'):
            number = integer(value) if column == 'delay' else \
                _float(value)
            if number is None:
                bad[column] += 1
            value = 'n/a' if number is None else number
        elif column in ('DX1', 'DX2', 'DX3', 'DX4', 'DX5') and \
                value in ('A', '.'):
            value = 'n/a'
        out.append(value)
    return out


def _float(value: str) -> float | None:
    """A finite number, or None."""
    try:
        value = float(value)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


#: Data dictionary of `session_id` in the phenotype tables
SESSION_ID = {
    "Description": "Session of the subject on the day of the visit "
                   "(n/a: none that day; the day of the visit is in "
                   "`delay`)",
}


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
    if "session_id" in header:
        out["session_id"] = SESSION_ID
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


# ----------------------------------------------------------------------
#   Python API
# ----------------------------------------------------------------------

#: Roast `<root>` (the `OASIS-3` folder): `roast(root, keys=..., sub=...)`
roast = python_api(roast_oasis3, KEYS, folder="OASIS-3",
                   width=WIDTH)
