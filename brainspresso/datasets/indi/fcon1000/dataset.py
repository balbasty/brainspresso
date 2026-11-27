"""
1000 Functional Connectomes Project (FCON1000 / "F1000"), 33 sites.

Bucket layout (`s3://fcp-indi/data/Projects/FCON1000/<site>/`):

    participants.tsv, task-rest_bold.json, README.txt, ...
    sourcedata/sub-<id>/anat/sub-<id>_T1w.nii.gz
    sourcedata/sub-<id>/func[_<run>]/sub-<id>_task-rest_bold.nii.gz
    sourcedata/sub-<id>/ses-<n>/{anat,func}/...           (NYU_TRT)
    derivatives/sub-<id>_T1w_brain.nii.gz                 (skull-stripped)

Labels are only unique within a site, so they are prefixed with a site
code (`AnnArbor_a/sub-04111` -> `sub-AnnArborA04111`). Files are
recognised by their folder, which absorbs the typos of the bucket
(`tast-rest`, `test-rest`, `_boldt`, `T1wnii.gz`, ...); runs stored in
`func_<n>` folders become `run-<n>`. Sidecars with the repetition time
(read from the NIfTI headers, see `make_templates.py`) are generated.

Output:

    participants.tsv     (merged; site, original label, age, sex, ...)
    sub-<site><id>/anat/sub-<site><id>_T1w.nii.gz
    sub-<site><id>/func/sub-<site><id>_task-rest[_run-<n>]_bold.nii.gz
    sub-<site><id>/sub-<site><id>_task-rest_bold.json
    derivatives/fcon1000/sub-<site><id>/anat/*_desc-brain_T1w.nii.gz
    sourcedata/<site>/...   (site notes and extra tables)
"""
from __future__ import annotations

import re
from collections import defaultdict
from logging import getLogger
from pathlib import Path

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import (
    InlineEntry,
    Item,
    Project,
    Site,
    json_bytes,
    read_table,
)

lg = getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"

# Site name on the bucket -> code prepended to subject labels
CODES = {
    "AnnArbor_a": "AnnArborA",
    "AnnArbor_b": "AnnArborB",
    "Atlanta": "Atlanta",
    "Baltimore": "Baltimore",
    "Bangor": "Bangor",
    "Beijing_Zang": "Beijing",
    "Berlin_Margulies": "Berlin",
    "Cambridge_Buckner": "Cambridge",
    "Dallas": "Dallas",
    "ICBM": "ICBM",
    "Leiden_2180": "LeidenA",
    "Leiden_2200": "LeidenB",
    "Leipzig": "Leipzig",
    "Milwaukee_a": "MilwaukeeA",
    "Milwaukee_b": "MilwaukeeB",
    "Munchen": "Munchen",
    "NYU_TRT": "NYUTRT",
    "NewHaven_a": "NewHavenA",
    "NewHaven_b": "NewHavenB",
    "NewYork_a": "NewYorkA",
    "NewYork_a_ADHD": "NewYorkADHD",
    "NewYork_b": "NewYorkB",
    "Newark": "Newark",
    "Ontario": "Ontario",
    "Orangeburg": "Orangeburg",
    "Oulu": "Oulu",
    "Oxford": "Oxford",
    "PaloAlto": "PaloAlto",
    "Pittsburgh": "Pittsburgh",
    "Queensland": "Queensland",
    "SaintLouis": "SaintLouis",
    "Taipei_a": "TaipeiA",
    "Taipei_b": "TaipeiB",
}
PIPELINE = "derivatives/fcon1000"


def repetition_times() -> tuple[dict[str, float], dict[str, float]]:
    """
    Repetition time of each site (`_sites.tsv`), and of the subjects
    whose scans differ from their site (`_repetition_times.tsv`).
    """
    out = []
    for name, key in (("_sites.tsv", "site"),
                      ("_repetition_times.tsv", "participant_id")):
        path = TEMPLATES / name
        rows = (read_table(path.read_text(encoding="utf-8"))
                if path.exists() else [])
        out.append({r[key]: float(r["repetition_time"]) for r in rows})
    return tuple(out)


class FCON1000(Project):
    name = "fcon1000"
    folder = "FCON1000"
    category = "fcp"
    title = "1000 Functional Connectomes Project (FCON1000)"
    help = """
1000 Functional Connectomes Project (FCON1000, "F1000")

* **Modalities**    T1w, resting-state fMRI (skull-stripped T1w)
* **Populations**   ~1300 participants (mostly healthy), 33 sites
* **Host**          s3://fcp-indi (INDI), no login needed
* **Citation**      https://doi.org/10.1073/pnas.0911855107
"""
    sites = tuple(
        Site(name, f"FCON1000/{name}", code) for name, code in CODES.items()
    )

    def participants(self, tables):
        """Merged site tables (built by `make_templates.py`)"""
        path = TEMPLATES / "_participants.tsv"
        return path.read_text(encoding="utf-8")

    def remap(self, items: list[Item]):
        # Runs: a subject whose rest scans are split across `func`,
        # `func_2`... gets `run-<n>` entities (`func` is run 1)
        folders = defaultdict(set)
        for it in items:
            m = re.match(r"sourcedata/(sub-[^/]+)/(?:ses-[^/]+/)?"
                         r"(func(?:_\d+)?)/", it.rel)
            if m:
                folders[(it.site, m.group(1))].add(m.group(2))

        brains = []
        for it in items:
            if it.obj.size == 0:            # `no_T1w` and other markers
                continue
            parts = it.rel.split("/")
            site = it.site
            if len(parts) == 1:
                if parts[0] not in ("participants.tsv", "task-rest_bold.json",
                                    "dataset_description.json"):
                    folder = f"sourcedata/{site.name}" if self.multisite \
                        else "sourcedata"
                    yield f"{folder}/{parts[0]}", it
                continue
            if parts[0] == "derivatives":
                m = re.match(r"sub-([A-Za-z0-9]+)", parts[-1])
                ses = re.search(r"_(ses-[0-9]+)_", parts[-1])
                if m:
                    brains.append((site.label(m.group(1)),
                                   ses.group(1) if ses else None, it))
                continue
            if parts[0] != "sourcedata" or not parts[1].startswith("sub-"):
                lg.debug(f"fcon1000: unexpected file {site.name}/{it.rel}")
                continue
            label = site.label(parts[1][4:])
            rest = parts[2:]
            ses = rest.pop(0) if rest and rest[0].startswith("ses-") else None
            if len(rest) != 2:
                lg.debug(f"fcon1000: unexpected file {site.name}/{it.rel}")
                continue
            folder, fname = rest
            prefix = f"sub-{label}/" + (f"{ses}/" if ses else "")
            stem = f"sub-{label}" + (f"_{ses}" if ses else "")
            if folder == "anat":
                if "brain" in fname:
                    brains.append((label, ses, it))
                else:
                    yield f"{prefix}anat/{stem}_T1w.nii.gz", it
            elif folder.startswith("func"):
                runs = folders[(site, parts[1])]
                run = ""
                if len(runs) > 1 or folder != "func":
                    n = folder[5:] or "1"
                    run = f"_run-{n}"
                yield f"{prefix}func/{stem}_task-rest{run}_bold.nii.gz", it

        # Skull-stripped T1w (in `derivatives/`, or in `anat/` when they
        # are the only anatomical images available). The first one wins.
        brains.sort(key=lambda b: not b[2].rel.startswith("derivatives"))
        for label, ses, it in brains:
            prefix = f"{PIPELINE}/sub-{label}/" + (f"{ses}/" if ses else "")
            stem = f"sub-{label}" + (f"_{ses}" if ses else "")
            yield f"{prefix}anat/{stem}_desc-brain_T1w.nii.gz", it

    def generated(self, items: list[Item], tables):
        """
        Bold sidecars (task name and repetition time): per subject when
        sites are merged; for a site dataset, at the top level, with
        subject sidecars only for subjects whose TR differs.
        """
        site_trs, subject_trs = repetition_times()
        subjects = {}
        for it in items:
            m = re.match(r"sourcedata/sub-([A-Za-z0-9]+)/(?:ses-[^/]+/)?func",
                         it.rel)
            if m and it.obj.size > 0:
                subjects[it.site.label(m.group(1))] = (it.site, m.group(1))
        sites = {site.name for site, _ in subjects.values()}
        if not self.multisite and len(sites) == 1:
            name, = sites
            yield InlineEntry("task-rest_bold.json", json_bytes(
                {"TaskName": "rest", "RepetitionTime": site_trs[name]}
                if name in site_trs else {"TaskName": "rest"}
            ))
        for label, (site, original) in sorted(subjects.items()):
            merged = f"sub-{CODES.get(site.name, '')}{original}"
            if not self.multisite and len(sites) == 1:
                if merged not in subject_trs:
                    continue
            sidecar = {"TaskName": "rest"}
            tr = subject_trs.get(merged, site_trs.get(site.name))
            if tr:
                sidecar["RepetitionTime"] = tr
            yield InlineEntry(
                f"sub-{label}/sub-{label}_task-rest_bold.json",
                json_bytes(sidecar),
            )


#: catalog entry (`bx fcon1000`)
dataset = project_dataset(FCON1000)
