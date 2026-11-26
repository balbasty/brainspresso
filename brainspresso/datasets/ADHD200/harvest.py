# stdlib
import json
from pathlib import Path
from logging import getLogger

# externals

# internals
from brainspresso.datasets.ADHD200.keys import allkeys
from brainspresso.sources.base import (
    RemoteEntry, Wants, numeric_labels,
)
from brainspresso.sources.fcp import FCP

lg = getLogger(__name__)

# Paths relative to s3://fcp-indi/data/Projects
DATA = "ADHD200/RawData"
PROC = "ADHD200/Outputs"
SURF = "ADHD200/surfaces"

PROC_KEYS = (
    "cpa", "denoise", "fmriprep", "mindboggle", "mriqc",
    "mindboggle/ants", "mindboggle/freesurfer"
)
EXPECTED = {"anat": "mprage.nii.gz", "rest": "rest.nii.gz"}


class S3(FCP):
    """
    ADHD-200 from s3://fcp-indi/data/Projects/ADHD200.

    * raw data (`RawData/<site>/<sub>/<ses>/<mod>_<run>/*`) go directly
      to the raw BIDS dataset (`bidsify`), else to `sourcedata/`, with
      BIDS names;
    * phenotypes (`RawData/*.csv`), FreeSurfer (`surfaces/freesurfer/`,
      as `freesurfer-<version>/<sub>/`) and pipeline outputs
      (`Outputs/<pipeline>/`) go to `sourcedata/`;
    * `sourcedata/sub2site.json` maps subjects to sites.

    Per-run options: `sites`, `exclude_sites`, `bidsify`.
    """

    name = "s3"
    provides = frozenset({"raw", "meta", "derivatives"})
    labels = numeric_labels(7)
    sites: frozenset = frozenset()
    exclude_sites: frozenset = frozenset()
    bidsify: bool = True

    def __init__(self, **kwargs):
        super().__init__("ADHD200", **kwargs)

    def keep_site(self, site) -> bool:
        return not (site in self.exclude_sites
                    or (self.sites and site not in self.sites))

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        wants = keys if callable(keys) else Wants(keys, allkeys)
        out = []
        # raw data (also used to map subjects to sites)
        raw_entries = self.entries("RawData/")
        sub2site = {}
        for e in raw_entries:
            parts = e.relpath.parts[1:]
            if len(parts) == 5 and parts[1].isdigit():
                sub2site[f"{int(parts[1]):07d}"] = parts[0]
        out.append(RemoteEntry("sub2site.json", meta={
            "content": json.dumps(sub2site, indent=2) + "\n"}))

        def keep_sub(sub, site=None) -> bool:
            if not self.keeps_subject(selection, sub):
                return False
            if site is None:
                site = sub2site.get(f"{int(sub):07d}", None)
            return self.keep_site(site)

        if wants("meta"):
            for e in raw_entries:
                parts = e.relpath.parts[1:]
                if len(parts) == 1 and parts[0].endswith(".csv"):
                    out.append(self._local(e, parts[0]))

        if wants("raw"):
            for e in raw_entries:
                parts = e.relpath.parts[1:]
                if len(parts) != 5:
                    continue
                site, sub, ses, mod_run, fname = parts
                if not sub.isdigit() or not keep_sub(sub, site):
                    continue
                mod, run = mod_run.split('_')
                if not wants(mod) or fname != EXPECTED.get(mod):
                    continue
                run, ses = int(run), int(ses.split('_')[-1])
                sub = f"{int(sub):07d}"
                path = f"sub-{sub}/ses-{ses}"
                if mod == "anat":
                    path += f"/anat/sub-{sub}_ses-{ses}_T1w.nii.gz"
                else:
                    path += (f"/func/sub-{sub}_ses-{ses}_task-rest_"
                             f"run-{run}_bold.nii.gz")
                out.append(self._local(e, path, bids=True))

        # freesurfer: surfaces/freesurfer/<ver>/<sub>/...
        if wants("fs"):
            for e in self.entries("surfaces/"):
                parts = e.relpath.parts[1:]
                if len(parts) < 4 or not parts[2].isdigit():
                    continue
                _, fsver, sub, *rest = parts
                if not keep_sub(sub):
                    continue
                out.append(self._local(
                    e, "/".join([f"freesurfer-{fsver}", sub, *rest])))

        # preprocessed data: Outputs/<pipeline>/...
        if wants("proc"):
            for e in self.entries("Outputs/"):
                parts = e.relpath.parts[1:]
                if len(parts) < 4:
                    continue
                dirpath = "/".join(parts[:-1])
                if not all(wants(key) for key in PROC_KEYS
                           if key in dirpath):
                    continue
                sub = None
                for part in parts:
                    if part.startswith("sub-"):
                        label = part.split("-")[-1]
                        sub = label if label.isdigit() else None
                        break
                if sub is not None and not keep_sub(sub):
                    continue
                out.append(self._local(e, "/".join(parts)))
        return out

    @staticmethod
    def _local(entry: RemoteEntry, path: str, bids: bool = False):
        entry.meta.update(local=path, bids=bids)
        return entry

    def resolve(self, layout, entry: RemoteEntry) -> Path:
        path = entry.meta.get("local", entry.path)
        if entry.meta.get("bids") and self.bidsify:
            return Path(layout.raw) / path
        return Path(layout.source) / path


