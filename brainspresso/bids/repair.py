"""
Repair common defects of BIDS metadata, after download.

Hosted copies of older datasets often have metadata that the BIDS
validator rejects. These repairs are conservative: they only rewrite
values whose meaning is unambiguous, and they never touch images.

JSON files
----------
* Invalid JSON: control characters in strings, non-breaking spaces,
  trailing commas, empty values (`"FlipAngle" :, 7.0`), single quotes,
  missing commas between lines.
* Numbers stored as strings: `"3T"` -> 3, `"6/8"` -> 0.75, `"6:00"`
  (mm:ss durations) -> 360, and fractions mangled into dates by
  spreadsheets (`"8-Jul"` -> 7/8). Other text values of numeric
  fields (`"EchoTime": "Min Full"`) are moved to `<Field>Text`.
* `SliceTiming` in milliseconds (all larger than `RepetitionTime` in
  seconds) -> seconds.
* `dataset_description.json`: string fields given as lists are joined,
  list fields (`Authors`, `Funding`...) given as a string become lists.
* `*_mt-on_*.json`/`*_mt-off_*.json` without `MTState`: true/false.
* `*_MP2RAGE.json` with `RepetitionTime` but no
  `RepetitionTimePreparation`: the former is the latter.
* `*_phasediff.json` without `EchoTime1`/`EchoTime2` (dcm2niix output):
  taken from `EchoTime` of the sibling `magnitude1` and of the phase
  difference image itself.

Field maps
----------
* `IntendedFor` given as a free-text tag instead of paths (`"fMRI"`,
  `"dwi"`): replaced with the subject's (and session's) local images of
  that datatype, or removed if there are none.
* With `trim`, targets that are not present locally are removed (as
  for tables, so that a subset is self-consistent).

TSV files
---------
* Windows line endings, header cells separated by spaces.
* Missing values (`N/A`, `NA`, `NaN`, empty) -> `n/a`.
* `participants.tsv`: `sub-` prefix added to `participant_id`.
* `*_sessions.tsv`: a `session`/`session_id` column is moved first, as
  `session_id` with a `ses-` prefix; rows that repeat a session (one
  row per scan) are merged, dropping the columns that differ.
"""
from __future__ import annotations

import fnmatch
import json
import re
from logging import getLogger
from pathlib import Path
from typing import Iterable

lg = getLogger(__name__)

NUMERIC_KEYS = frozenset({
    "MagneticFieldStrength", "RepetitionTime", "EchoTime", "EchoTime1",
    "EchoTime2", "FlipAngle", "InversionTime", "EffectiveEchoSpacing",
    "TotalReadoutTime", "SliceThickness", "SpacingBetweenSlices",
    "PartialFourier", "ParallelReductionFactorInPlane",
    "MultibandAccelerationFactor", "DwellTime", "PixelBandwidth",
    "AcquisitionDuration", "DelayTime", "NumberOfVolumesDiscardedByScanner",
    "NumberOfVolumesDiscardedByUser", "PhaseResolution",
    "PercentPhaseFieldOfView", "PercentSampling",
})
LIST_KEYS = frozenset({
    "Authors", "Funding", "ReferencesAndLinks", "EthicsApprovals",
})
STRING_KEYS = frozenset({
    "Acknowledgements", "HowToAcknowledge", "License", "DatasetDOI",
    "Name", "BIDSVersion",
})
MISSING = frozenset({"", "N/A", "NA", "NaN", "nan", "NULL", "null", "None"})
MONTHS = {m: i + 1 for i, m in enumerate(
    "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
)}


# ----------------------------------------------------------------------
#   JSON
# ----------------------------------------------------------------------

def loads(text: str):
    """Parse JSON, repairing common syntax errors if needed."""
    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError:
        pass
    fixed = text.replace("\xa0", " ")
    try:
        return json.loads(fixed, strict=False)
    except json.JSONDecodeError:
        pass
    fixed = re.sub(r":\s*,", ":", fixed)                  # "a" :, 7
    fixed = re.sub(r",(\s*[}\]])", r"\1", fixed)          # trailing comma
    fixed = re.sub(r"'([^'\"\n]*)'", r'"\1"', fixed)      # 'single'
    # missing comma at the end of a line: `"a": 1\n "b": 2`
    fixed = re.sub(r'([0-9"\]}]|true|false|null)(\s*\n\s*")', r"\1,\2",
                   fixed)
    return json.loads(fixed, strict=False)


def to_number(value, key: str = ""):
    """Convert a string holding a number to a number (or return None)."""
    if isinstance(value, (int, float)) or not isinstance(value, str):
        return None
    text = value.strip()
    try:
        number = float(text)
        return int(number) if number.is_integer() else number
    except ValueError:
        pass
    m = re.fullmatch(r"([0-9.]+)\s*/\s*([0-9.]+)", text)        # 6/8
    if m:
        return round(float(m.group(1)) / float(m.group(2)), 10)
    m = re.fullmatch(r"(\d+)-([A-Z][a-z]{2})", text)            # 8-Jul
    if m and m.group(2) in MONTHS and key == "PartialFourier":
        return MONTHS[m.group(2)] / int(m.group(1))
    m = re.fullmatch(r"(\d+):(\d{2})", text)                    # 6:00
    if m and key == "AcquisitionDuration":
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.fullmatch(r"([0-9.]+)\s*T", text)                    # 3T
    if m and key == "MagneticFieldStrength":
        return float(m.group(1)) if "." in m.group(1) else int(m.group(1))
    return None


def fix_sidecar(meta: dict, name: str = "") -> dict:
    """Coerce values of a sidecar or dataset description."""
    out = {}
    for key, value in meta.items():
        if isinstance(value, str):
            value = value.replace("\t", " ")
        if key in NUMERIC_KEYS and isinstance(value, str):
            number = to_number(value, key)
            if number is None:
                out[f"{key}Text"] = value
                continue
            value = number
        if name == "dataset_description.json":
            if key in STRING_KEYS and isinstance(value, list):
                value = "; ".join(str(v) for v in value)
            if key in LIST_KEYS and isinstance(value, str):
                value = [value]
            if key == "Authors" and isinstance(value, list):
                value = [re.sub(r"\s+", " ", str(v)).strip() for v in value]
        out[key] = value
    tr = out.get("RepetitionTime")
    timing = out.get("SliceTiming")
    if isinstance(tr, (int, float)) and isinstance(timing, list) and \
            timing and all(isinstance(t, (int, float)) for t in timing):
        if max(timing) > tr and max(timing) / 1000 < tr:
            out["SliceTiming"] = [round(t / 1000, 6) for t in timing]
    return out


def _read_utf8(path: Path) -> str | None:
    """
    Text of a file to repair in place (None: not valid UTF-8).

    Decoded strictly: a file that is not valid UTF-8 is left as it is,
    with a warning, as text decoded with replacement characters must
    never be written back (it would lose the original bytes).
    """
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as e:
        lg.warning(f"{path}: not valid UTF-8, not repaired ({e})")
        return None


def repair_json(path: Path) -> bool:
    """Repair a JSON file in place. Returns whether it was changed."""
    path = Path(path)
    text = _read_utf8(path)
    if text is None:
        return False
    try:
        meta = loads(text)
    except json.JSONDecodeError as e:
        lg.warning(f"{path}: invalid JSON, not repaired ({e})")
        return False
    if not isinstance(meta, dict):
        return False
    fixed = fix_sidecar(meta, path.name)
    mt = re.search(r"_mt-(on|off)_", path.name)
    if mt and "MTState" not in fixed:
        fixed["MTState"] = mt.group(1) == "on"
    if path.name.endswith("_MP2RAGE.json") and \
            "RepetitionTimePreparation" not in fixed and \
            isinstance(fixed.get("RepetitionTime"), (int, float)):
        fixed["RepetitionTimePreparation"] = fixed.pop("RepetitionTime")
    if path.name.endswith("_phasediff.json"):
        fixed = _phasediff(path, fixed)
    try:
        unchanged = json.loads(text) == fixed
    except json.JSONDecodeError:
        unchanged = False
    if unchanged:
        return False
    path.write_text(json.dumps(fixed, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8", newline="\n")
    return True


def _phasediff(path: Path, meta: dict) -> dict:
    if "EchoTime1" in meta and "EchoTime2" in meta:
        return meta
    magnitude = path.with_name(
        path.name.replace("_phasediff.json", "_magnitude1.json")
    )
    if "EchoTime" not in meta or not magnitude.exists():
        return meta
    try:
        # (a magnitude sidecar that is not valid UTF-8 is read with
        # replacement characters: only its EchoTime, a number, is used,
        # and it is never written back; `repair_json` itself decodes
        # strictly)
        text = magnitude.read_text(encoding="utf-8", errors="replace")
        te1 = loads(text).get("EchoTime")
    except (ValueError, OSError, AttributeError):
        return meta
    te2 = meta["EchoTime"]
    if isinstance(te1, (int, float)) and isinstance(te2, (int, float)) \
            and te1 < te2:
        meta = dict(meta)
        meta["EchoTime1"], meta["EchoTime2"] = te1, te2
    return meta


# ----------------------------------------------------------------------
#   TSV
# ----------------------------------------------------------------------

def read_rows(text: str) -> list[list[str]]:
    """Parse TSV text into rows (blank lines dropped)."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    rows = [line.split("\t") for line in text.split("\n")]
    rows = [r for r in rows if any(c.strip() for c in r)]
    if not rows:
        return rows
    header, width = rows[0], max((len(r) for r in rows[1:]), default=0)
    if len(header) < width:
        # header cells separated by spaces instead of tabs
        split = [c for cell in header for c in cell.split()]
        if len(split) == width:
            rows[0] = split
    return rows


def repair_tsv(path: Path) -> bool:
    """Repair a TSV file in place. Returns whether it was changed."""
    path = Path(path)
    text = _read_utf8(path)
    if text is None:
        return False
    rows = read_rows(text)
    if not rows:
        return False
    header = [c.strip() for c in rows[0]]
    body = [[("n/a" if c.strip() in MISSING else c.strip()) for c in r]
            for r in rows[1:]]
    body = [r + ["n/a"] * (len(header) - len(r)) for r in body]
    if path.name == "participants.tsv" and header[0] == "participant_id":
        for r in body:
            if not r[0].startswith("sub-"):
                r[0] = "sub-" + r[0]
    if path.name.endswith("_sessions.tsv"):
        header, body = _sessions(header, body)
    out = "".join("\t".join(r) + "\n" for r in [header, *body])
    if out == text:
        return False
    path.write_text(out, encoding="utf-8", newline="\n")
    return True


def _sessions(header: list[str], body: list[list[str]]):
    for name in ("session_id", "session"):
        if name in header:
            break
    else:
        return header, body
    i = header.index(name)
    order = [i] + [j for j in range(len(header)) if j != i]
    header = ["session_id"] + [header[j] for j in order[1:]]
    body = [[r[j] for j in order] for r in body]
    for r in body:
        if r[0] != "n/a" and not r[0].startswith("ses-"):
            r[0] = "ses-" + r[0]
    # one row per session: drop columns that vary within a session
    groups: dict[str, list[list[str]]] = {}
    for r in body:
        groups.setdefault(r[0], []).append(r)
    if all(len(g) == 1 for g in groups.values()):
        return header, body
    varying = {
        j for g in groups.values() for j in range(len(header))
        if len({r[j] for r in g}) > 1
    }
    keep = [j for j in range(len(header)) if j not in varying]
    return [header[j] for j in keep], [
        [g[0][j] for j in keep] for g in groups.values()
    ]


# ----------------------------------------------------------------------
#   Field maps
# ----------------------------------------------------------------------

# free-text tags found in `IntendedFor` -> datatype
INTENDED_TAGS = {
    "fmri": "func", "func": "func", "bold": "func", "rest": "func",
    "dwi": "dwi", "dti": "dwi", "diffusion": "dwi",
    "asl": "perf", "perf": "perf", "anat": "anat",
}
IMAGE_EXTENSIONS = (".nii.gz", ".nii")


def fix_intended_for(path: Path, root: Path, trim: bool = False) -> bool:
    """Repair (and trim) `IntendedFor` of a field map sidecar in place."""
    path, root = Path(path), Path(root)
    try:
        rel = path.relative_to(root)
    except ValueError:
        return False
    if not rel.parts or not rel.parts[0].startswith("sub-"):
        return False
    subject = root / rel.parts[0]
    session = subject / rel.parts[1] \
        if len(rel.parts) > 2 and rel.parts[1].startswith("ses-") else None
    try:
        meta = loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError as e:
        # (not rewritten: replacement characters would be lossy)
        lg.warning(f"{path}: not valid UTF-8, IntendedFor not repaired "
                   f"({e})")
        return False
    except (json.JSONDecodeError, OSError):
        return False
    if not isinstance(meta, dict) or "IntendedFor" not in meta:
        return False
    targets = meta["IntendedFor"]
    targets = [targets] if isinstance(targets, str) else list(targets or [])

    ignore = _bidsignore(root)
    out = []
    for target in targets:
        if not isinstance(target, str):
            continue
        datatype = INTENDED_TAGS.get(target.strip().lower())
        if datatype and "/" not in target:
            folder = (session or subject) / datatype
            out += sorted(
                p.relative_to(subject).as_posix()
                for p in folder.glob("*")
                if p.name.endswith(IMAGE_EXTENSIONS)
                and not any(fnmatch.fnmatch(p.name, pat) for pat in ignore)
            )
            continue
        if trim:
            if target.startswith("bids::"):
                exists = (root / target[len("bids::"):]).exists()
            else:
                exists = (subject / target).exists()
            if not exists:
                continue
        out.append(target)

    fixed = dict(meta)
    if out:
        fixed["IntendedFor"] = out
    else:
        fixed.pop("IntendedFor")
    if fixed == meta:
        return False
    path.write_text(json.dumps(fixed, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8", newline="\n")
    return True


def _bidsignore(root: Path) -> list[str]:
    path = Path(root) / ".bidsignore"
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return [line.strip().lstrip("/") for line in lines
            if line.strip() and not line.startswith("#")]


# ----------------------------------------------------------------------

def repair(paths: Iterable[Path]) -> list[Path]:
    """Repair JSON and TSV files (others are ignored)."""
    changed = []
    paths = sorted(Path(p) for p in paths)
    # phase difference maps need their magnitude sidecar: do them last
    paths.sort(key=lambda p: p.name.endswith("_phasediff.json"))
    for path in paths:
        if not path.exists():
            continue
        try:
            if path.suffix == ".json":
                done = repair_json(path)
            elif path.suffix == ".tsv":
                done = repair_tsv(path)
            else:
                continue
        except (OSError, UnicodeError, ValueError) as e:
            lg.warning(f"{path}: not repaired ({e})")
            continue
        if done:
            changed.append(path)
    return changed
