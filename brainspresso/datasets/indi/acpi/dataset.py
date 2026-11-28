"""
Addiction Connectome Preprocessed Initiative (ACPI), raw data: `bx acpi`.

Curated files and fill-in sidecar templates, if any: `templates/` next
to this module (`Project.template_dir`).
"""
from __future__ import annotations

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import (
    Project,
    Site,
    merge_sites,
    read_table,
)

_DU1 = {    # codes of du_1_phenotypic_key.csv
    "SEX": {"1": "F", "2": "M"},
    "HANDEDNESS": {"1": "L", "2": "R", "3": "A"},
}
_WORDS = {
    "sex": {"male": "M", "female": "F"},
    "handedness": {"right": "R", "left": "L", "ambidextrous": "A"},
}


class ACPI(Project):
    name = "acpi"
    folder = "ACPI"
    title = "Addiction Connectome Preprocessed Initiative (ACPI), raw data"
    help = """
Addiction Connectome Preprocessed Initiative (ACPI), raw data

* **Modalities**    T1w, resting-state fMRI
* **Populations**   ~190 participants (cannabis, nicotine, ADHD), 3 sites
* **Host**          s3://fcp-indi (INDI), no login needed
"""
    SITES = ("du_1", "mta_1", "nyu_1")
    sites = tuple(Site(s, f"ACPI/RawDataBIDS/{s}") for s in SITES)
    tables = {
        "mta_1": "ACPI/RawDataBIDS/mta_1/participants.tsv",
        "nyu_1": "ACPI/RawDataBIDS/nyu_1/participants.tsv",
        "du_1": "ACPI/PhenotypicData/du_1_phenotypic_data.csv",
    }
    extra_files = {
        f"sourcedata/phenotype/{name}": f"ACPI/PhenotypicData/{name}"
        for name in (
            "acpi_aggregated_phenotypic_data.csv",
            "du_1_phenotypic_data.csv", "du_1_phenotypic_key.csv",
            "mta_1_phenotypic_data.csv", "mta_1_phenotypic_key.docx",
            "nyu_1_phenotypic_data.csv",
        )
    }

    def participants(self, tables):
        per_site = {}
        for site, text in tables.items():
            rows = []
            if site == "du_1":
                seen = set()
                for row in read_table(text, ","):
                    if row["SUB_ID"] in seen:      # one row per scan
                        continue
                    seen.add(row["SUB_ID"])
                    rows.append({
                        "participant_id": row["SUB_ID"],
                        "age": row.get("AGE_AT_SCAN_1", "n/a"),
                        "sex": _DU1["SEX"].get(row.get("SEX"), "n/a"),
                        "handedness": _DU1["HANDEDNESS"].get(
                            row.get("HANDEDNESS"), "n/a"),
                        "weight": row.get("WEIGHT", "n/a"),
                    })
            else:
                for row in read_table(text):
                    row = dict(row)
                    if "age_at_scan_1" in row:
                        row["age"] = row.pop("age_at_scan_1")
                    if "sexmf" in row:
                        row["sex"] = row.pop("sexmf")
                    if "testageyears" in row and "age" not in row:
                        try:
                            row["age"] = str(round(
                                float(row["testageyears"])
                                + float(row.get("testagemonth") or 0) / 12,
                                2))
                        except ValueError:
                            row["age"] = "n/a"
                    for key, words in _WORDS.items():
                        if key in row:
                            row[key] = words.get(row[key].lower(), row[key])
                    rows.append(row)
            per_site[site] = rows
        return merge_sites(per_site, ("age", "sex", "handedness"))


#: catalog entry (`bx acpi`)
dataset = project_dataset(ACPI)
