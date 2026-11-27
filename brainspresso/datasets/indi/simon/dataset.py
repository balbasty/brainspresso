"""
SIMON (single individual, many sessions): `bx simon`.

Curated files and fill-in sidecar templates, if any: `templates/` next
to this module (`Project.template_dir`).
"""
from __future__ import annotations

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import Project, Site


class SIMON(Project):
    name = "simon"
    folder = "SIMON"
    category = "retro"
    title = "Single Individual volunteer for Multiple Observations " \
            "across Networks (SIMON)"
    help = """
SIMON: one healthy individual scanned in ~70 sessions at many sites

* **Modalities**    T1w, T2w, PD, FLAIR, DWI, resting-state fMRI
* **Populations**   1 healthy male (29 to 46 years over the sessions)
* **Host**          s3://fcp-indi (INDI), no login needed
"""
    sites = (
        Site("", "INDI/SIMON/rawdata"),
        Site("", "INDI/SIMON/derivatives", target="derivatives/niak",
             derivative=True),
    )


#: catalog entry (`bx simon`)
dataset = project_dataset(SIMON)
