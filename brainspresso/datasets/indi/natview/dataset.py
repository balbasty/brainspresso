"""
Naturalistic viewing: simultaneous EEG-fMRI (NATVIEW): `bx natview`.

Curated files and fill-in sidecar templates, if any: `templates/` next
to this module (`Project.template_dir`).
"""
from __future__ import annotations

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import Project, Site


class NATVIEW(Project):
    name = "natview"
    folder = "NATVIEW"
    category = "retro"
    title = "Naturalistic viewing: simultaneous EEG-fMRI (NATVIEW)"
    help = """
Naturalistic viewing: an open-access dataset using simultaneous EEG-fMRI

* **Modalities**    T1w, movie/task/rest fMRI with simultaneous EEG,
                    eye tracking, physiology; 2 sessions
* **Populations**   22 healthy adults
* **Host**          s3://fcp-indi (INDI), no login needed
"""
    sites = (Site("", "NATVIEW_EEGFMRI/raw_data"),)
    # nycq and last_night_sleep have one row per session, which BIDS
    # phenotype files do not allow: they go to sourcedata/phenotype
    phenotype = ("psqi",)

    def remap(self, items):
        for path, it in super().remap(items):
            name = path.rsplit("/", 1)[-1]
            if path == f"sourcedata/{name}" and \
                    name.split(".")[0] in ("nycq", "last_night_sleep"):
                path = f"sourcedata/phenotype/{name}"
            yield path, it


#: catalog entry (`bx natview`)
dataset = project_dataset(NATVIEW)
