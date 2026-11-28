"""
OASIS-II: convert the harvested parts to BIDS.

Expected input
--------------
OASIS-2/sourcedata/
    OAS2_RAW_PART{1,2}.tar.gz
    oasis_longitudinal_demographics.xlsx

Output (study layout)
---------------------
OASIS-2/
    dataset_description.json      (DatasetType "study")
    rawbids/
        README, dataset_description.json, participants.{tsv,json},
        sessions.json
        sub-{04d}/
            sub-{04d}_sessions.tsv
            ses-{d}/anat/sub-{04d}_ses-{d}_run-{d}_T1w.{nii.gz,json}

With `--layout flat`, the content of `rawbids/` is in `OASIS-2/` itself.

A part is a gz tarball that holds the scans of about 75 subjects (all
their sessions), and reading a member of a gz tarball decompresses
everything before it. Each part is therefore read by one shard (task id
`roast/parts/part<N>`, inputs: the part; config: the sidecar template,
path and hash; so that an unchanged part is skipped on a re-run, also
by a roast that
keeps fewer subjects or toggles `meta`: its scope, `roast.scope`): it
is listed once, then its members are extracted in archive order (one
forward pass). Parts
that are not in `sourcedata/` are skipped with a warning, unless named
with `--parts` (then an error). Sessions are the MR numbers of the
member folders (`OAS2_<sub>_MR<n>` is `ses-<n>`). The participants and
sessions tables are written complete to `.brainspresso/orig/` and
trimmed to the subjects and sessions on disk.
"""
import re
from logging import getLogger
from pathlib import Path
from typing import Iterator

from brainspresso.actions import Action, CopyJSON, Fail, WriteTSV
from brainspresso.bids.keys import KeyAliases
from brainspresso.datasets.oasis.i.roaster import analyze_action
from brainspresso.errors import UsageError
from brainspresso.roast import (
    RoastContext,
    archives,
    copy_templates,
    roast_dataset,
)
from brainspresso.roast.helpers import archive_shards, template_config
from brainspresso.sources.base import subject_labels
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.vol import make_affine

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"
XLSX = "oasis_longitudinal_demographics.xlsx"
SIDECAR = "T1w.json"
WIDTH = 4               # subject labels: `sub-0001`

#: Keys of the roast
ROAST_KEYS = ("raw", "meta")
#: Parts (tarballs of raw scans)
PARTS = (1, 2)

# "raw" is the only key of the raw dataset; "meta" is a category
KEYS = KeyAliases(
    set(ROAST_KEYS),
    bids={"raw": {"datatypes": ["anat"], "suffixes": ["T1w"]}},
)

# OASIS images are stored in ASL orientation
# https://brainder.org/2011/08/13/converting-oasis-brains-to-nifti/
AFFINE_RAW = make_affine(
    [256, 256, 128], [1.0, 1.0, 1.25], orient="ASL", center="x/2"
)


def roast(root, *, subs=(), exclude_subs=(), parts=None, **options):
    """
    Roast `<root>` (the `OASIS-2` folder).

    Parameters
    ----------
    root : path
        Dataset folder
    subs, exclude_subs : list[int | str]
        Subjects: numbers (`1` is `sub-0001`), labels with or without
        `sub-`, ranges, files (as `--subs`)
    parts : list[int]
        Only these parts, 1 or 2 (default: all; a part named here that
        is not in `sourcedata/` is an error, other missing parts are
        skipped)
    **options
        Those of the command line (`keys`, `exclude_keys`, `sessions`,
        `json`, `ifexists`, `layout`), `jobs` and `display`

    Returns
    -------
    RoastReport
        See `roast.run_roast`

    Raises
    ------
    UsageError
        If a part is not 1 or 2
    """
    if parts:
        check_parts(parts)
    return roast_dataset(
        roast_oasis2, root, KEYS, templates=TEMPLATES,
        subs=sorted(subject_labels(subs, WIDTH)),
        exclude_subs=sorted(subject_labels(exclude_subs, WIDTH)),
        options={"parts": sorted(set(parts))} if parts else None,
        **options,
    )


def check_parts(parts) -> None:
    """
    Check part numbers.

    Parameters
    ----------
    parts : list[int]
        Values of `--parts`

    Raises
    ------
    UsageError
        If a part is not 1 or 2
    """
    wrong = sorted(set(parts) - set(PARTS))
    if wrong:
        raise UsageError(f"--parts: no part {', '.join(map(str, wrong))} "
                         f"(parts are 1 and 2)")


def part_path(ctx: RoastContext, part: int) -> Path:
    """
    Return the path of a part.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    part : int
        Part number

    Returns
    -------
    Path
        `OAS2_RAW_PART<N>.tar.gz` in the source folder
    """
    return ctx.source / f"OAS2_RAW_PART{part}.tar.gz"


def roast_oasis2(ctx: RoastContext) -> Iterator:
    """
    Roast OASIS-II: metadata, then one shard per part.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast (`ctx.options["parts"]`: parts named with
        `--parts`; absent: all)

    Yields
    ------
    Action | Shard
        Metadata actions, then part shards
    """
    if ctx.wants("meta"):
        yield from roast_meta(ctx)
    if not ctx.wants("raw"):
        return
    named = ctx.options.get("parts")
    found = {}              # {path: part}
    for part in PARTS:
        if named is not None and part not in named:
            continue
        path = part_path(ctx, part)
        if not path.exists():
            message = f"{path.name} not found"
            if named is not None:
                yield Fail(message, name=path.name, tag="parts")
            else:
                lg.warning(f"{message} (skipped)")
            continue
        found[path] = part
    config = template_config(ctx, [SIDECAR])
    yield from archive_shards(
        ctx, found, roast_part, tag="parts", keys=("raw",),
        id=lambda path: f"roast/parts/part{found[path]}",
        config=lambda path: config)


def raw_scan(name: str) -> tuple[str, int, int, str] | None:
    """
    Parse the name of a raw scan.

    Parameters
    ----------
    name : str
        Member of a part (normalised, `roast.archives.Member.path`):
        `[<prefix>/]OAS2_<sub>_MR<ses>/RAW/mpr-<run>.nifti.img`, with any
        prefix (the top folder `OAS2_RAW_PART<N>/` of the legacy roaster
        is not confirmed on the real tarballs)

    Returns
    -------
    (str, int, int, str) | None
        Subject label, session, run and stem (member without `.img`)
    """
    match = re.fullmatch(
        r"((?:.+/)?OAS2_(\d+)_MR(\d+)/RAW/mpr-(\d+)\.nifti)\.img", name)
    if not match:
        return None
    stem, sub, ses, run = match.groups()
    return f"{int(sub):0{WIDTH}d}", int(ses), int(run), stem


# ----------------------------------------------------------------------
#   Parts
# ----------------------------------------------------------------------

def roast_part(ctx: RoastContext, path: Path) -> Iterator[Action]:
    """
    Convert the scans of the selected subjects of a part (a shard).

    The part is listed once, then its members are extracted in archive
    order: actions are sorted by the position of their first member,
    and each one reads its members in that order (a backward seek in a
    gz tarball decompresses it again from the start).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    path : Path
        The part

    Yields
    ------
    Action
        Sidecars, conversions (Analyze -> NIfTI), missing members; a
        `Fail` if the part cannot be read
    """
    try:
        # (closed on every path: Windows locks open files)
        with archives.reading(path, opener=TarArchive) as tar:
            members = tar.index()
            found = list(filter(None, map(raw_scan, members)))
            if not found:
                yield Fail(f"{path.name}: no scan found (members "
                           f"`OAS2_<sub>_MR<n>/RAW/mpr-<run>.nifti.img`)",
                           name=path.name, tag="parts")
                return
            actions = []            # [(members, action)]
            sessions = ctx.selection.base.sessions
            for scan in found:
                if ctx.keeps_subject(scan[0]) and (
                        not sessions or str(scan[1]) in sessions):
                    actions += scan_actions(ctx, tar.archive, path, *scan,
                                            members=members)
            yield from archives.in_order(actions)
    except archives.ArchiveError as e:
        yield e.fail(tag="parts")


def scan_actions(ctx: RoastContext, archive: TarArchive, path: Path,
                 sub: str, ses: int, run: int, stem: str, *,
                 members: dict) -> list:
    """
    Return the actions of a raw scan.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    archive : TarArchive
        The open part
    path : Path
        Its path
    sub : str
        Subject label
    ses, run : int
        Session and run
    stem : str
        Member of the scan (normalised), without `.img`/`.hdr`
    members : dict[str, roast.archives.Member]
        `{normalised name: member}` of the part

    Returns
    -------
    list[tuple[list[roast.archives.Member], Action]]
        `(members it reads, action)` (`roast.archives.in_order`)
    """
    anat = ctx.layout.raw / f"sub-{sub}" / f"ses-{ses}" / "anat"
    dst = anat / f"sub-{sub}_ses-{ses}_run-{run}_T1w"
    out = []
    sidecar = ctx.resource(SIDECAR)
    if sidecar is not None:
        out.append(([], CopyJSON(sidecar, f"{dst}.json", tag="raw")))
    elif ctx.accepts("sidecar"):
        out.append(([], Fail(f"{SIDECAR} not found", name=f"{dst.name}.json",
                             tag="raw")))
    names = [f"{stem}{ext}" for ext in (".hdr", ".img")]
    missing = [m for m in names if m not in members]
    if missing:
        if ctx.accepts("data"):
            out.append(([], Fail(f"{', '.join(missing)} not found in "
                                 f"{path.name}", name=f"{dst.name}.nii.gz",
                                 tag="raw")))
        return out
    infos = sorted((members[m] for m in names), key=lambda m: m.offset)
    out.append((infos, analyze_action(
        archive, path, [m.name for m in infos], f"{dst}.nii.gz", AFFINE_RAW,
        tag="raw")))
    return out


# ----------------------------------------------------------------------
#   Participants and sessions
# ----------------------------------------------------------------------

#: Columns of the spreadsheet (in this order)
XLSX_HEADER = [
    "Subject ID", "MRI ID", "Group", "Visit", "MR Delay", "M/F", "Hand",
    "Age", "EDUC", "SES", "MMSE", "CDR", "eTIV", "nWBV", "ASF",
]

PARTICIPANTS_HEADER = [
    "participant_id",
    "sex",
    "handedness",
    "age",
]

SESSIONS_HEADER = [
    "session_id",
    "visit",
    "delay",
    "pathology",
    "age",
    "educ",
    "ses",
    "mmse",
    "cdr",
    "etiv",
    "nwbv",
    "asf",
]

#: Group of the subject (lower case) -> level of `pathology`
PATHOLOGY_MAP = {
    "nondemented": "N",
    "demented": "D",
    "converted": "C",
}


def roast_meta(ctx: RoastContext) -> Iterator[Action]:
    """
    Write the dataset files, the participants and sessions tables.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast

    Yields
    ------
    Action
        Copies of templates, participants and sessions tables (complete,
        in `.brainspresso/orig/`)
    """
    raw, original = ctx.layout.raw, ctx.layout.original
    yield from copy_templates(ctx, ["README", "dataset_description.json",
                                    "participants.json", "sessions.json"])
    xlsx = ctx.source / XLSX
    if not xlsx.exists():
        yield Fail(f"sourcedata/{XLSX} not found: harvest with --keys meta",
                   name="participants.tsv", tag="meta")
        return
    try:
        import openpyxl  # noqa: F401, PLC0415  (lazy: optional dependency)
    except ImportError:
        yield Fail("openpyxl is needed to read the spreadsheet: pip install "
                   "brainspresso[oasis2]", name="participants.tsv",
                   tag="meta")
        return
    try:
        participants, sessions = read_demographics(xlsx)
    except Exception as e:      # (openpyxl raises many kinds of errors)
        yield Fail(f"{XLSX} cannot be read ({type(e).__name__}: {e})",
                   name="participants.tsv", tag="meta")
        return
    yield WriteTSV(participants, original(raw / "participants.tsv"),
                   src=xlsx, tag="meta")
    for sub, rows in sorted(sessions.items()):
        table = raw / f"sub-{sub}" / f"sub-{sub}_sessions.tsv"
        yield WriteTSV(rows, original(table), src=xlsx, tag="meta")


def cell(value) -> str:
    """
    Format a cell of the spreadsheet.

    Parameters
    ----------
    value : object
        Its value

    Returns
    -------
    str
        `n/a` if empty or `N/A`; integral numbers without decimals
        (`27.0` is `27`, so that CDR `0.0` is the level `0`)
    """
    if value is None:
        return "n/a"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    value = str(value).strip()
    return "n/a" if value.upper() in ("", "N/A", "NA") else value


def number(value) -> int | None:
    """
    Read an integer cell (`1`, `1.0`, `"1"`).

    Parameters
    ----------
    value : object
        Its value

    Returns
    -------
    int | None
        The integer, or None
    """
    try:
        value = float(str(value).strip())
    except ValueError:
        return None
    return int(value) if value.is_integer() else None


def column_index(header) -> list[int]:
    """
    Find the columns of `XLSX_HEADER` in the header of the spreadsheet.

    Names are compared without case and spaces; a column that is not
    found is read at its position in `XLSX_HEADER` (with a warning).

    Parameters
    ----------
    header : sequence
        First row of the spreadsheet

    Returns
    -------
    list[int]
        Index of each column of `XLSX_HEADER`
    """
    def norm(name) -> str:
        """Name without case and spaces."""
        return re.sub(r"\s+", "", str(name or "")).lower()

    names = {}
    for i, name in enumerate(header):
        names.setdefault(norm(name), i)
    index, missing = [], []
    for i, name in enumerate(XLSX_HEADER):
        if norm(name) in names:
            index.append(names[norm(name)])
        else:
            index.append(i)
            missing.append(f"{name!r} (column {i + 1}: "
                           f"{header[i] if i < len(header) else None!r})")
    if missing:
        lg.warning(f"{XLSX}: columns not found by name, read by position: "
                   f"{', '.join(missing)}")
    return index


def read_demographics(path_xlsx: Path) -> tuple[list, dict[str, list]]:
    """
    Read the participants and sessions tables from the spreadsheet.

    Columns are found by name (`XLSX_HEADER`, else by position). Rows
    without a subject ID (blank or formatted rows) are skipped; rows
    whose IDs cannot be parsed, or whose MRI ID is of another subject,
    are skipped with a warning. The session of a row is the number of
    its MRI ID (`OAS2_0001_MR2` is `ses-2`, the folder of its scans),
    else (with a warning) its visit. Participants are described by
    their first visit. Empty and `N/A` cells are `n/a`.

    Parameters
    ----------
    path_xlsx : Path
        `oasis_longitudinal_demographics.xlsx`

    Returns
    -------
    participants : list[list[str]]
        Header and one row per subject
    sessions : dict[str, list[list[str]]]
        `{sub: header and one row per session}`
    """
    import openpyxl  # noqa: PLC0415  (lazy: optional dependency)

    # (not `read_only`: it trusts the stored dimension of the sheet)
    book = openpyxl.load_workbook(path_xlsx, data_only=True)
    try:
        sheet = book[book.sheetnames[0]]
        rows = [list(row) for row in sheet.iter_rows(values_only=True)]
    finally:
        book.close()
    if not rows:
        raise ValueError("empty sheet")
    index = column_index(rows[0])
    width = max(index) + 1

    found = {}          # {sub: {ses: (visit, row)}}
    for nrow, row in enumerate(rows[1:], 2):
        row = row + [None] * (width - len(row))     # (empty trailing cells)
        (sid, mri, group, visit, delay, sex, hand, age, educ, socio, mmse,
         cdr, etiv, nwbv, asf) = (row[i] for i in index)
        if not str(sid or "").strip():
            continue                    # blank or formatted row
        match = re.fullmatch(r"OAS2_(\d+)", str(sid).strip())
        if not match:
            lg.warning(f"{XLSX}, row {nrow}: subject ID {sid!r} (skipped)")
            continue
        sub = f"{int(match[1]):0{WIDTH}d}"
        visit = number(visit)
        match = re.fullmatch(r"OAS2_(\d+)_MR(\d+)", str(mri or "").strip())
        if match and int(match[1]) != int(sub):
            lg.warning(f"{XLSX}, row {nrow}: MRI ID {mri!r} is not of "
                       f"subject {sid!r} (skipped)")
            continue
        if match:
            ses = int(match[2])
        elif visit is not None:
            ses = visit
            lg.warning(f"{XLSX}, row {nrow}: MRI ID {mri!r}: session from "
                       f"the visit (ses-{ses})")
        else:
            lg.warning(f"{XLSX}, row {nrow}: no session in MRI ID "
                       f"{mri!r} or visit {row[index[3]]!r} (skipped)")
            continue
        if ses in found.get(sub, {}):
            lg.warning(f"{XLSX}, row {nrow}: sub-{sub} ses-{ses} is listed "
                       f"twice (skipped)")
            continue
        group = str(group or "").strip()
        pathology = PATHOLOGY_MAP.get(group.lower(), "n/a")
        if group and pathology == "n/a":
            lg.warning(f"{XLSX}, row {nrow}: unknown group {group!r} (n/a)")
        found.setdefault(sub, {})[ses] = (visit, [
            [sex, hand, age],
            [visit, delay, pathology, age, educ, socio, mmse, cdr, etiv,
             nwbv, asf]])

    participants = [PARTICIPANTS_HEADER]
    sessions = {}
    for sub, rows in sorted(found.items()):
        first = min(rows, key=lambda ses: (
            rows[ses][0] if rows[ses][0] is not None else ses, ses))
        participants.append([f"sub-{sub}", *map(cell, rows[first][1][0])])
        sessions[sub] = [SESSIONS_HEADER] + [
            [f"ses-{ses}", *map(cell, values[1])]
            for ses, (_, values) in sorted(rows.items())]
    return participants, sessions
