"""
Generate the templates of the Bullitt dataset (a maintainer tool).

From the files of its "Administration" folder (data.kitware.com):

* `Adjunct information/NormalDatabase2.pdf` -> `participants.tsv`
* `DICOM Headers/dicom_header_*.txt`        -> `*_T1w.json`, `T2w.json`,
                                               `angio.json` (sidecars)

This is a maintainer tool (it needs `pymupdf`); its outputs are
committed, so users do not need to run it.

    python make_templates.py /path/to/Administration
"""
import csv
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).parent
N_SUBJECTS = 109    # Normal-001 ... Normal-109 on data.kitware.com

# ----------------------------------------------------------------------
#   Demographics
# ----------------------------------------------------------------------

# Entries whose cells are merged or shifted in the PDF text, transcribed
# by hand from the PDF: (age, sex, race, handedness, notes)
FIXES = {
    41: ("64", "M->F", "A", "R", "moved"),
    46: ("61", "M->F", "W", "R", ""),
    84: ("67", "M", "W", "A", "no flash"),
    91: ("56", "M", "B", "R", "no rage; no DTI (clip, move); split circ"),
    98: ("64", "F", "W", "R", "no flash"),
    99: ("63", "M", "W", "A", "no flash"),
}
# "ABNORMALS" section of the PDF
FINDINGS = {
    48: "asymmetrical head from suture closure, large ventricles, "
        "intermittent headaches; retained as normal",
    52: "ethmoid polyp; retained as normal",
    63: "tiny intracanalicular acoustic tumor; retained as normal "
        "(brain not affected)",
    70: "epidermoid cyst (scalp)",
    84: "Waldenstrom's macroglobulinemia; probably should not use",
    98: "unexpected minor stroke (asymptomatic, tiny); "
        "probably should not use",
}
# Table column headers repeated at the top of each PDF page
HEADERS = {"T1", "T2", "DTI", "MRA", "trees"}
# "Genetic Relationships" section of the PDF
FAMILY = [
    (24, "mother", 4), (49, "mother", 59), (41, "sister", 66),
    (67, "mother", 47), (68, "father", 47), (73, "mother", 42),
    (69, "mother", 74),
]
FAMILY_EXTRA = {24: "sister of Tumor069 (not in this database)"}


def parse_pdf(path: Path) -> dict[int, dict]:
    """
    Read the demographics table of `NormalDatabase2.pdf`.

    Parameters
    ----------
    path : Path
        The PDF

    Returns
    -------
    dict[int, dict]
        `{subject number: {age, sex, race, handedness, vessels_quality,
        notes}}`
    """
    import pymupdf  # noqa: PLC0415  lazy: optional dependency
    text = "\n".join(page.get_text() for page in pymupdf.open(str(path)))
    text = text.split("ABNORMALS")[0]
    rows = {}
    for block in re.split(r"(?=NormalA\d{3})", text):
        m = re.match(r"NormalA(\d{3})(.*)", block, re.S)
        if not m:
            continue
        sub = int(m.group(1))
        lines = [ln.strip() for ln in m.group(2).splitlines() if ln.strip()]
        quality = next(
            (re.search(r"\+{2,}", ln).group(0) for ln in lines
             if re.search(r"\+{2,}", ln)), "n/a"
        )
        if sub in FIXES:
            age, sex, race, hand, notes = FIXES[sub]
        else:
            i = next(i for i, ln in enumerate(lines)
                     if re.fullmatch(r"\d{2}", ln))
            age, sex, race, hand = lines[i:i + 4]
            rest = lines[:i] + lines[i + 4:]
            notes = [re.sub(r"\+{2,}", "", ln).strip(" ,;") for ln in rest]
            notes = "; ".join(
                n for n in notes
                if n and n != "-" * len(n) and n not in HEADERS
            )
            if hand.startswith("L (Ambidextrous)"):
                hand, notes = "L", "; ".join(
                    x for x in ("ambidextrous", notes) if x
                )
        assert sex in ("M", "F", "M->F"), (sub, sex)
        assert race in ("W", "A", "B"), (sub, race)
        assert hand in ("R", "L", "A"), (sub, hand)
        rows[sub] = dict(age=age, sex=sex, race=race, handedness=hand,
                         vessels_quality=quality, notes=notes)
    return rows


def write_participants(rows: dict[int, dict]) -> None:
    """
    Write `participants.tsv` (findings and family relationships added).

    Parameters
    ----------
    rows : dict[int, dict]
        Rows of `parse_pdf`
    """
    notes = {sub: [r["notes"]] if r["notes"] else []
             for sub, r in rows.items()}
    for sub, finding in FINDINGS.items():
        notes.setdefault(sub, []).append(finding)
    for a, relation, b in FAMILY:
        notes.setdefault(a, []).append(f"{relation} of sub-{b:03d}")
        notes.setdefault(b, []).append(f"child of sub-{a:03d} ({relation})")
    for sub, extra in FAMILY_EXTRA.items():
        notes.setdefault(sub, []).append(extra)

    header = ["participant_id", "age", "sex", "race", "handedness",
              "vessels_quality", "exclude_suggested", "notes"]
    with open(HERE / "participants.tsv", "w", newline="",
              encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        for sub in range(1, N_SUBJECTS + 1):
            r = rows.get(sub, {})
            text = "; ".join(notes.get(sub, []))
            # "Probably should not use" (ABNORMALS) / "don't use" (table)
            exclude = bool(re.search(r"should not use|don.t use", text))
            writer.writerow([
                f"sub-{sub:03d}",
                r.get("age", "n/a"), r.get("sex", "n/a"),
                r.get("race", "n/a"), r.get("handedness", "n/a"),
                r.get("vessels_quality", "n/a"),
                "yes" if exclude else ("no" if sub in rows else "n/a"),
                text or "n/a",
            ])


# ----------------------------------------------------------------------
#   Sidecars from sample DICOM headers
# ----------------------------------------------------------------------

SIDECARS = {
    "dicom_header_T1_flash.txt": "acq-flash_T1w.json",
    "dicom_header_T1_mprage.txt": "acq-mprage_T1w.json",
    "dicom_header_T2_tra.txt": "T2w.json",
    "dicom_header_MRA.txt": "angio.json",
}
STRINGS = {
    "Manufacturer": "Manufacturer",
    "ManufacturersModelName": "ManufacturerModelName",
    "SoftwareVersions": "SoftwareVersions",
    "SequenceName": "SequenceName",
    "ScanningSequence": "ScanningSequence",
    "SequenceVariant": "SequenceVariant",
    "ScanOptions": "ScanOptions",
    "MRAcquisitionType": "MRAcquisitionType",
    "ProtocolName": "ProtocolName",
    "SeriesDescription": "SeriesDescription",
}
SECONDS = {"RepetitionTime": "RepetitionTime", "EchoTime": "EchoTime",
           "InversionTime": "InversionTime"}
NUMBERS = {"FlipAngle": "FlipAngle", "PixelBandwidth": "PixelBandwidth",
           "EchoTrainLength": "EchoTrainLength",
           "ImagingFrequency": "ImagingFrequency",
           "SliceThickness": "SliceThickness"}


def parse_dicom_text(path: Path) -> dict:
    """
    Read a sample DICOM header (text dump).

    Parameters
    ----------
    path : Path
        `dicom_header_*.txt`

    Returns
    -------
    dict
        `{DICOM keyword: value or list of values}`
    """
    values = {}
    text = path.read_text(encoding="utf-8", errors="replace")
    for line in text.splitlines():
        m = re.match(r"\([0-9A-F]{4},[0-9A-F]{4}\) \w+\[\d+\] (\w+): (.*)",
                     line)
        if not m:
            continue
        name, value = m.groups()
        items = re.findall(r"\[([^\]]*)\]", value)
        if items:
            values[name] = items if len(items) > 1 else items[0]
    return values


def make_sidecar(path: Path) -> dict:
    """
    Build a BIDS sidecar from a sample DICOM header.

    Parameters
    ----------
    path : Path
        `dicom_header_*.txt`

    Returns
    -------
    dict
        Sidecar (times in seconds, `SidecarSource`)
    """
    v = parse_dicom_text(path)
    out = {}
    for src, dst in STRINGS.items():
        if src in v:
            out[dst] = v[src]
    if "MagneticFieldStrength" in v:   # nominal field strength
        out["MagneticFieldStrength"] = round(float(v["MagneticFieldStrength"]))
    for src, dst in SECONDS.items():
        if src in v:
            out[dst] = round(float(v[src]) / 1000, 6)
    for src, dst in NUMBERS.items():
        if src in v:
            x = float(v[src])
            out[dst] = int(x) if x == int(x) else x
    out["SidecarSource"] = (
        f"Sample DICOM header ({path.name}) distributed with the dataset; "
        f"may differ for individual subjects."
    )
    return out


def main(admin: Path) -> None:
    """
    Write every template.

    Parameters
    ----------
    admin : Path
        The "Administration" folder of the dataset
    """
    pdf = admin / "Adjunct information" / "NormalDatabase2.pdf"
    write_participants(parse_pdf(pdf))
    for src, dst in SIDECARS.items():
        sidecar = make_sidecar(admin / "DICOM Headers" / src)
        (HERE / dst).write_text(json.dumps(sidecar, indent=2) + "\n",
                                encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
