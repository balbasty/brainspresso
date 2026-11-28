"""
MPI-Leipzig Mind-Brain-Body: LEMON (MRI): `bx mpi-lemon`.

Curated files and fill-in sidecar templates, if any: `templates/` next
to this module (`Project.template_dir`).
"""
from __future__ import annotations

import re

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import (
    Project,
    Site,
    format_tsv,
    read_table,
    subject_id,
)


class LEMON(Project):
    name = "mpi-lemon"
    folder = "MPI-LEMON"
    category = "retro"
    title = "MPI-Leipzig Mind-Brain-Body: LEMON (MRI)"
    help = """
MPI-Leipzig Mind-Brain-Body dataset: LEMON (MRI)

* **Modalities**    MP2RAGE (T1w, T1map), T2w, FLAIR, SWI, DWI, rest fMRI,
                    field maps
* **Populations**   227 healthy adults (young: 20-35, elderly: 59-77)
* **Host**          s3://fcp-indi (INDI), no login needed
* **Citation**      https://doi.org/10.1038/sdata.2018.308
"""
    ROOT = "INDI/MPI-LEMON/MRI_MPILMBB_LEMON"
    sites = (Site("", f"{ROOT}/MRI_Raw"),)
    tables = {"participants": f"{ROOT}/Participants_LEMON.csv"}
    extra_files = {
        "README": f"{ROOT}/README",
        "dataset_description.json": f"{ROOT}/dataset_description.json",
    }

    def participants(self, tables):
        text = tables.get("participants")
        if text is None:
            return None
        rows = []
        for row in read_table(text, ","):
            values = list(row.values())
            rows.append({
                "participant_id": subject_id(values[0]),
                "sex": {"1": "F", "2": "M"}.get(values[1], "n/a"),
                "age_group": values[2] or "n/a",
            })
        return format_tsv(rows, ["participant_id", "sex", "age_group"])

    def remap(self, items):
        # `acq-{mag,phase}_GRE` (susceptibility-weighted GRE) is not a
        # BIDS suffix: use `part-{mag,phase}_T2starw`. The suffix of the
        # MP2RAGE inversions is `MP2RAGE`, not `mp2rage`.
        for path, it in super().remap(items):
            path = re.sub(r"_acq-(mag|phase)_GRE\.", r"_part-\1_T2starw.",
                          path)
            path = re.sub(r"_mp2rage\.", "_MP2RAGE.", path)
            yield path, it


#: catalog entry (`bx mpi-lemon`)
dataset = project_dataset(LEMON)
