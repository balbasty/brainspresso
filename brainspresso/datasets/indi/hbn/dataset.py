"""
Healthy Brain Network (HBN), MRI: `bx hbn`.

Curated files and fill-in sidecar templates, if any: `templates/` next
to this module (`Project.template_dir`).
"""
from __future__ import annotations

import re

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import (
    Project,
    Site,
    merge_sites,
    read_table,
)


class HBN(Project):
    name = "hbn"
    folder = "HBN"
    category = "pro"
    title = "Healthy Brain Network (HBN), MRI"
    help = """
Child Mind Institute Healthy Brain Network (HBN), MRI data

* **Modalities**    T1w, T2w, FLAIR, DWI, rest/movie/task fMRI, field maps
* **Populations**   ~3900 children and adolescents (5 to 21 years), 4 sites
* **Host**          s3://fcp-indi (INDI), no login needed (phenotypes need
                    a data usage agreement, not included)
* **Citation**      https://doi.org/10.1038/sdata.2017.181
"""
    SITES = ("Site-CBIC", "Site-CUNY", "Site-RU", "Site-SI")
    sites = tuple(Site(s, f"HBN/MRI/{s}") for s in SITES)
    ignore = ("derivatives", "CPAC_preprocess")
    tables = {s: f"HBN/MRI/{s}/participants.tsv" for s in SITES}

    def participants(self, tables):
        return merge_sites({s: read_table(t) for s, t in tables.items()})

    def remap(self, items):
        # Magnetization transfer on/off pairs (`MT-on_PD`): the BIDS
        # entity is `mt`, and the file collection is `MTR`
        for path, it in super().remap(items):
            yield re.sub(r"_MT-(on|off)_PD\.", r"_mt-\1_MTR.", path), it


#: catalog entry (`bx hbn`)
dataset = project_dataset(HBN)
