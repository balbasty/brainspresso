"""
Southwest University Adult Lifespan Dataset (SALD): `bx sald`.

Curated files and fill-in sidecar templates, if any: `templates/` next
to this module (`Project.template_dir`).
"""
from __future__ import annotations

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import Project, Site


class SALD(Project):
    name = "sald"
    folder = "SALD"
    category = "retro"
    title = "Southwest University Adult Lifespan Dataset (SALD)"
    help = """
Southwest University Adult Lifespan Dataset (SALD)

* **Modalities**    T1w, resting-state fMRI
* **Populations**   494 healthy adults, 19 to 80 years
* **Host**          s3://fcp-indi (INDI), no login needed
* **Citation**      https://doi.org/10.1038/sdata.2018.134
"""
    sites = (Site("", "INDI/SALD/RawData_BIDS"),)


#: catalog entry (`bx sald`)
dataset = project_dataset(SALD)
