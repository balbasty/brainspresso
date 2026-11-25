"""
UNC-Wisconsin: convert the harvested NRRD files to BIDS.

Expected input
--------------
UNC-Wisconsin/sourcedata/
    folders.json                    (subject/session metadata)
    scan_data/<sub>/<age>months/sMRI/ORIG/<sub>_<age>months_T1_ORIG.nrrd
    scan_data/<sub>/<age>months/sMRI/ORIG/<sub>_<age>months_T2_ORIG.nrrd
    scan_data/<sub>/<age>months/DTI/ORIG/*/<sub>_<age>months_dwi.nrrd

Output
------
UNC-Wisconsin/
    README, dataset_description.json
    participants.tsv, participants.json, sessions.json
    rawdata/sub-<sub>/sub-<sub>_sessions.tsv
    rawdata/sub-<sub>/ses-<age>months/anat/sub-<sub>_ses-<age>months_T1w.nii.gz
    rawdata/sub-<sub>/ses-<age>months/anat/sub-<sub>_ses-<age>months_T2w.nii.gz
    rawdata/sub-<sub>/ses-<age>months/dwi/sub-<sub>_ses-<age>months_dwi.{nii.gz,bval,bvec}
"""
import csv
import json
import re
from logging import getLogger
from pathlib import Path
from typing import Iterable, Iterator

from brainspresso.actions import Action, CopyBytes, CopyJSON
from brainspresso.utils.nrrd import nrrd_to_nifti
from brainspresso.utils.tabular import bidsify_tab

lg = getLogger(__name__)

SPECIES = "Macaca mulatta"
ANAT = {"T1": "T1w", "T2": "T2w"}


class Bidsifier:

    TPLDIR = Path(__file__).parent / "templates"

    def __init__(
        self,
        root: Path,
        *,
        keys: Iterable[str] = ("meta", "T1w", "T2w", "dwi"),
        subs: Iterable[str] = (),
        exclude_subs: Iterable[str] = (),
        sessions: Iterable[str] = (),
        ifexists: str = "skip",
    ):
        self.root = Path(root)
        self.keys = set(keys)
        self.subs = set(subs)
        self.exclude_subs = set(exclude_subs)
        self.sessions = set(sessions)
        self.ifexists = ifexists
        self.src = self.root / "sourcedata"
        self.raw = self.root / "rawdata"
        self.nb_errors = 0
        self.nb_skipped = 0

    # ------------------------------------------------------------------
    def run(self) -> None:
        with bidsify_tab() as self.out:
            for status in self.make_meta():
                status.setdefault("modality", "meta")
                self.out(status)
            for status in self.make_raw():
                self.out(status)

    def fixstatus(self, status: dict, fname: str, mod: str) -> Iterator:
        status.setdefault("path", fname)
        status.setdefault("modality", mod)
        yield status
        if status.get("status", "") == "error":
            self.nb_errors += 1
            yield {"errors": self.nb_errors, "modality": mod}
        elif status.get("status", "") == "skipped":
            self.nb_skipped += 1
            yield {"skipped": self.nb_skipped, "modality": mod}

    def keep_subject(self, sub: str) -> bool:
        if self.subs and sub not in self.subs:
            return False
        return sub not in self.exclude_subs

    def keep_session(self, ses: str) -> bool:
        return not self.sessions or ses in self.sessions

    # ------------------------------------------------------------------
    #   Metadata
    # ------------------------------------------------------------------
    def folders(self) -> dict:
        path = self.src / "folders.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text())

    def make_meta(self) -> Iterator[dict]:
        if "meta" not in self.keys:
            return
        opt = dict(ifexists=self.ifexists)
        meta = self.folders()
        if not meta:
            lg.error("sourcedata/folders.json not found: harvest with "
                     "--keys meta")
        actions = {
            "README": CopyBytes(
                self.TPLDIR / "README", self.root / "README", **opt
            ),
            "dataset_description.json": CopyJSON(
                self.TPLDIR / "dataset_description.json",
                self.root / "dataset_description.json", **opt
            ),
            "participants.json": CopyJSON(
                self.TPLDIR / "participants.json",
                self.root / "participants.json", **opt
            ),
            "sessions.json": CopyJSON(
                self.TPLDIR / "sessions.json",
                self.root / "sessions.json", **opt
            ),
        }
        if meta:
            actions["participants.tsv"] = Action(
                self.src / "folders.json", self.root / "participants.tsv",
                lambda path: self.write_participants(meta, path),
                input="path", mode="t", **opt,
            )
            for sub in sorted({k.split("/")[0] for k in meta}):
                if not self.keep_subject(sub):
                    continue
                name = f"sub-{sub}_sessions.tsv"
                actions[name] = Action(
                    self.src / "folders.json",
                    self.raw / f"sub-{sub}" / name,
                    lambda path, sub=sub: self.write_sessions(
                        meta, sub, path
                    ),
                    input="path", mode="t", **opt,
                )
        for fname, action in actions.items():
            for status in action:
                yield from self.fixstatus(status, fname, "meta")
        yield {"status": "done", "message": "", "modality": "meta"}

    def write_participants(self, meta: dict, path: Path) -> None:
        rows = []
        for key, m in sorted(meta.items()):
            if m.get("folder_type") != "subject":
                continue
            sub = m.get("subject_id") or key
            if not self.keep_subject(sub):
                continue
            rows.append({
                "participant_id": f"sub-{sub}",
                "species": SPECIES,
                "sex": m.get("sex") or "n/a",
                "birth_date": m.get("dob") or "n/a",
            })
        _write_tsv(path, rows, [
            "participant_id", "species", "sex", "birth_date"
        ])

    def write_sessions(self, meta: dict, sub: str, path: Path) -> None:
        rows = []
        for key, m in sorted(meta.items()):
            parts = key.split("/")
            if m.get("folder_type") != "scan" or parts[0] != sub:
                continue
            ses = parts[1]
            if not self.keep_session(ses):
                continue
            age = re.match(r"(\d+)\s*months?$", str(m.get("scan_age", "")))
            weight = m.get("scan_weight_kg")
            rows.append({
                "session_id": f"ses-{ses}",
                "acq_time": m.get("scan_date") or "n/a",
                "age": age.group(1) if age else "n/a",
                "weight": weight if weight not in (None, "") else "n/a",
            })
        _write_tsv(path, rows, ["session_id", "acq_time", "age", "weight"])

    # ------------------------------------------------------------------
    #   Raw data (NRRD -> NIfTI)
    # ------------------------------------------------------------------
    def make_raw(self) -> Iterator[dict]:
        scan_data = self.src / "scan_data"
        if not scan_data.is_dir():
            lg.warning(f"{scan_data} not found")
            return
        for subdir in sorted(p for p in scan_data.iterdir() if p.is_dir()):
            sub = subdir.name
            if not self.keep_subject(sub):
                continue
            for sesdir in sorted(p for p in subdir.iterdir() if p.is_dir()):
                ses = sesdir.name
                if not self.keep_session(ses):
                    continue
                for action, mod in self.raw_actions(sub, ses, sesdir):
                    for status in action:
                        yield from self.fixstatus(
                            status, Path(_first(action.dst)).name, mod
                        )
        for mod in ("T1w", "T2w", "dwi"):
            if mod in self.keys:
                yield {"status": "done", "message": "", "modality": mod}

    def raw_actions(self, sub: str, ses: str, sesdir: Path):
        opt = dict(ifexists=self.ifexists, input="path")
        base = f"sub-{sub}_ses-{ses}"
        out = self.raw / f"sub-{sub}" / f"ses-{ses}"

        for contrast, suffix in ANAT.items():
            if suffix not in self.keys:
                continue
            name = f"{sub}_{ses}_{contrast}_ORIG.nrrd"
            src = sesdir / "sMRI" / "ORIG" / name
            if not src.exists():
                continue
            yield Action(
                src, out / "anat" / f"{base}_{suffix}.nii.gz",
                lambda path, src=src: nrrd_to_nifti(src, path),
                **opt,
            ), suffix

        if "dwi" in self.keys:
            for src in sorted((sesdir / "DTI" / "ORIG").glob("**/*.nrrd")):
                if not src.is_file():
                    continue
                dst = out / "dwi"
                yield Action(
                    src,
                    [dst / f"{base}_dwi.nii.gz",
                     dst / f"{base}_dwi.bval",
                     dst / f"{base}_dwi.bvec"],
                    lambda nii, bval, bvec, src=src: nrrd_to_nifti(
                        src, nii, bval=bval, bvec=bvec
                    ),
                    **opt,
                ), "dwi"
                break


def _first(x):
    return x[0] if isinstance(x, (list, tuple)) else x


def _write_tsv(path: Path, rows: list[dict], header: list[str]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, header, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
