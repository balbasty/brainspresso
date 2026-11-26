"""
Bullitt healthy MR database: convert the harvested MetaImage files to BIDS.

Output
------
Bullitt/
    README, dataset_description.json, participants.{tsv,json}
    rawdata/sub-<NNN>/anat/sub-<NNN>_{acq-flash_T1w,acq-mprage_T1w,T2w,angio}.{nii.gz,json}
    derivatives/bullitt/sub-<NNN>/anat/sub-<NNN>_acq-flash_desc-brain_T1w.nii.gz
    derivatives/bullitt/sub-<NNN>/anat/sub-<NNN>_desc-vessels_tubes.tre
"""
from logging import getLogger
from pathlib import Path
from typing import Iterable, Iterator

from brainspresso.actions import Action, CopyBytes, CopyJSON, WriteJSON
from brainspresso.utils.metaimage import metaimage_to_nifti
from brainspresso.utils.tabular import bidsify_tab
from brainspresso.datasets.Bullitt.download import (
    FOLDER_KEYS, subject_label,
)

lg = getLogger(__name__)

DERIV_NAME = "bullitt"
# key -> BIDS entities + suffix (also the name of the sidecar template)
RAW = {
    "flash": "acq-flash_T1w",
    "mprage": "acq-mprage_T1w",
    "T2w": "T2w",
    "angio": "angio",
}
KEYS = ("meta", *RAW, "skullstripped", "vessels")


class Bidsifier:

    TPLDIR = Path(__file__).parent / "templates"

    def __init__(
        self,
        root: Path,
        *,
        keys: Iterable[str] = KEYS,
        subs: Iterable[str] = (),
        exclude_subs: Iterable[str] = (),
        json: str = "yes",
        ifexists: str = "skip",
    ):
        self.root = Path(root)
        self.keys = set(keys)
        self.subs = set(subs)
        self.exclude_subs = set(exclude_subs)
        self.json = json
        self.ifexists = ifexists
        self.src = self.root / "sourcedata"
        self.raw = self.root / "rawdata"
        self.drv = self.root / "derivatives" / DERIV_NAME
        self.nb_errors = 0
        self.nb_skipped = 0

    def run(self) -> None:
        with bidsify_tab() as self.out:
            for status in self.make_meta():
                self.out(status)
            for status in self.make_subjects():
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

    # ------------------------------------------------------------------
    def make_meta(self) -> Iterator[dict]:
        if "meta" not in self.keys:
            return
        opt = dict(ifexists=self.ifexists)
        actions = {
            "README": CopyBytes(
                self.TPLDIR / "README", self.root / "README", **opt),
            "dataset_description.json": CopyJSON(
                self.TPLDIR / "dataset_description.json",
                self.root / "dataset_description.json", **opt),
            "participants.json": CopyJSON(
                self.TPLDIR / "participants.json",
                self.root / "participants.json", **opt),
            "participants.tsv": CopyBytes(
                self.TPLDIR / "participants.tsv",
                self.root / "participants.tsv", **opt),
        }
        if self.keys & {"skullstripped", "vessels"}:
            actions["derivatives/dataset_description.json"] = CopyJSON(
                self.TPLDIR / "derivatives_description.json",
                self.drv / "dataset_description.json", **opt)
            actions["derivatives/README"] = CopyBytes(
                self.TPLDIR / "derivatives_README",
                self.drv / "README", **opt)
            actions["derivatives/.bidsignore"] = CopyBytes(
                self.TPLDIR / "derivatives_bidsignore",
                self.drv / ".bidsignore", **opt)
        for fname, action in actions.items():
            for status in action:
                yield from self.fixstatus(status, fname, "meta")
        yield {"status": "done", "message": "", "modality": "meta"}

    # ------------------------------------------------------------------
    def make_subjects(self) -> Iterator[dict]:
        if not self.src.is_dir():
            lg.warning(f"{self.src} not found")
            return
        for subdir in sorted(self.src.iterdir()):
            sub = subject_label(subdir.name)
            if sub is None or not subdir.is_dir():
                continue
            if (self.subs and sub not in self.subs) or \
                    sub in self.exclude_subs:
                continue
            for action, mod in self.subject_actions(sub, subdir):
                for status in action:
                    yield from self.fixstatus(
                        status, Path(action.dst).name, mod
                    )
        for mod in (*RAW, "skullstripped", "vessels"):
            if mod in self.keys:
                yield {"status": "done", "message": "", "modality": mod}

    def subject_actions(self, sub: str, subdir: Path):
        opt = dict(ifexists=self.ifexists)
        anat = self.raw / f"sub-{sub}" / "anat"
        for folder in sorted(p for p in subdir.iterdir() if p.is_dir()):
            key = FOLDER_KEYS.get(folder.name)
            if key not in RAW or key not in self.keys:
                continue
            images = sorted(folder.glob("*.mha"))
            if not images:
                continue
            name = f"sub-{sub}_{RAW[key]}"
            if self.json != "only":
                yield Action(
                    images[0], anat / f"{name}.nii.gz",
                    lambda path, src=images[0]: metaimage_to_nifti(src, path),
                    input="path", **opt,
                ), key
            if self.json != "no":
                yield CopyJSON(
                    self.TPLDIR / f"{RAW[key]}.json", anat / f"{name}.json",
                    **opt,
                ), key

        aux = subdir / "AuxillaryData"
        out = self.drv / f"sub-{sub}" / "anat"
        brain = aux / "SkullStripped-T1-Flash.mha"
        if "skullstripped" in self.keys and brain.exists():
            name = f"sub-{sub}_acq-flash_desc-brain_T1w"
            yield Action(
                brain, out / f"{name}.nii.gz",
                lambda path: metaimage_to_nifti(brain, path),
                input="path", **opt,
            ), "skullstripped"
            yield WriteJSON(
                {"SkullStripped": True}, out / f"{name}.json", src=brain,
                ifexists=self.ifexists,
            ), "skullstripped"
        tree = aux / "VascularNetwork.tre"
        if "vessels" in self.keys and tree.exists():
            yield CopyBytes(
                tree, out / f"sub-{sub}_desc-vessels_tubes.tre", **opt
            ), "vessels"
