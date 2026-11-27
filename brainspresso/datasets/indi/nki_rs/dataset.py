"""
NKI Rockland Sample (enhanced): `bx nki-rs`.

Curated files and fill-in sidecar templates, if any: `templates/` next
to this module (`Project.template_dir`).
"""
from __future__ import annotations

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import Project, Site, one_table


class NKIRS(Project):
    name = "nki-rs"
    folder = "NKI-RS"
    category = "enhanced"
    title = "NKI Rockland Sample (enhanced)"
    help = """
Nathan Kline Institute Rockland Sample (NKI-RS, enhanced)

* **Modalities**    T1w, T2w, FLAIR, DWI, rest/task fMRI (several protocols)
* **Populations**   ~1300 participants, community sample, 6 to 85 years
* **Host**          s3://fcp-indi (INDI), no login needed
* **Citation**      https://doi.org/10.3389/fnins.2012.00152
"""
    PREFIX = "RocklandSample/RawDataBIDSLatest"
    sites = (Site("", PREFIX),)
    tables = {"participants": f"{PREFIX}/participants.tsv"}

    def participants(self, tables):
        return one_table(tables)


#: catalog entry (`bx nki-rs`)
dataset = project_dataset(NKIRS)
