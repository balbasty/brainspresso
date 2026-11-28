"""
Southwest University Longitudinal Imaging Multimodal (SLIM): `bx slim`.

Curated files and fill-in sidecar templates, if any: `templates/` next
to this module (`Project.template_dir`).
"""
from __future__ import annotations

import datetime
from collections import defaultdict

from brainspresso.datasets.indi.dataset import project_dataset
from brainspresso.datasets.indi.engine import (
    InlineEntry,
    Project,
    Site,
    format_tsv,
    read_table,
    subject_id,
)


def _date(value: str) -> str:
    """`'19-Nov-2011'` -> `2011-11-19`"""
    value = value.strip().strip("'\"")
    try:
        return datetime.datetime.strptime(value, "%d-%b-%Y").date().isoformat()
    except ValueError:
        return "n/a"


class SLIM(Project):
    name = "slim"
    folder = "SLIM"
    category = "retro"
    title = "Southwest University Longitudinal Imaging Multimodal (SLIM)"
    help = """
Southwest University Longitudinal Imaging Multimodal Brain Data (SLIM)

* **Modalities**    T1w, DWI, resting-state fMRI; up to 3 sessions
* **Populations**   ~600 healthy young adults (university students)
* **Host**          s3://fcp-indi (INDI), no login needed
* **Citation**      https://doi.org/10.1038/sdata.2017.17
"""
    sites = (Site("", "INDI/SLIM/RawData"),)
    tables = {f"time{n}": f"INDI/SLIM/swu_slim_phenodata_time{n}.tsv"
              for n in (1, 2, 3)}

    def _rows(self, tables):
        return {name: read_table(text) for name, text in tables.items()}

    def participants(self, tables):
        rows = []
        for row in self._rows(tables).get("time1", []):
            rows.append({
                "participant_id": subject_id(row["Match_ID"]),
                "sex": {"female": "F", "male": "M"}.get(
                    row.get("Sex", "").lower(), "n/a"),
                "age": row.get("age_at_Scan1", "n/a"),
            })
        return format_tsv(rows, ["participant_id", "sex", "age"])

    def generated(self, items, tables):
        """Sessions tables (scan date, age), from the three time points"""
        listed = {it.rel.split("/")[0] for it in items}
        per_subject = defaultdict(list)
        for name, rows in self._rows(tables).items():
            for row in rows:
                pid = subject_id(row["Match_ID"])
                per_subject[pid].append({
                    "session_id": f"ses-{row.get('Session', name[-1])}",
                    "scan_date": _date(row.get("Scan_Time", "")),
                    "age": row.get("age_at_Scan1", "n/a"),
                })
        return [
            InlineEntry(
                f"{pid}/{pid}_sessions.tsv",
                format_tsv(sorted(rows, key=lambda r: r["session_id"]),
                           ["session_id", "scan_date", "age"]),
            )
            for pid, rows in sorted(per_subject.items()) if pid in listed
        ]


#: catalog entry (`bx slim`)
dataset = project_dataset(SLIM)
