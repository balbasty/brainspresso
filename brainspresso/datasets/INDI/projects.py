"""
Curated INDI projects whose bucket copy is (nearly) BIDS.

See `engine.py` for what a `Project` describes, and `fcon1000.py` for
a project that needs more work.
"""
from __future__ import annotations

import datetime
import re
from collections import defaultdict

from brainspresso.datasets.INDI.engine import (
    InlineEntry, Project, Site, format_tsv, read_table, subject_id,
)


def _one_table(tables: dict[str, str], name: str = "participants"):
    text = tables.get(name)
    if text is None:
        return None
    rows = read_table(text)
    for row in rows:
        row["participant_id"] = subject_id(row["participant_id"])
    return format_tsv(rows, ["participant_id"])


def _merge(per_site: dict[str, list[dict]], columns=()):
    """Merge site tables, adding a `site` column (`columns` come next)"""
    rows = []
    for site, site_rows in per_site.items():
        for row in site_rows:
            row = dict(row)
            pid = subject_id(row.pop("participant_id"))
            rows.append({"participant_id": pid, "site": site, **row})
    return format_tsv(rows, ["participant_id", "site", *columns])


# ----------------------------------------------------------------------
#   NKI Rockland Sample
# ----------------------------------------------------------------------

class NKIRS(Project):
    name = "nki-rs"
    folder = "NKI-RS"
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
        return _one_table(tables)


# ----------------------------------------------------------------------
#   Healthy Brain Network
# ----------------------------------------------------------------------

class HBN(Project):
    name = "hbn"
    folder = "HBN"
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
        return _merge({s: read_table(t) for s, t in tables.items()})


# ----------------------------------------------------------------------
#   Southwest University (SALD, SLIM)
# ----------------------------------------------------------------------

class SALD(Project):
    name = "sald"
    folder = "SALD"
    title = "Southwest University Adult Lifespan Dataset (SALD)"
    help = """
Southwest University Adult Lifespan Dataset (SALD)

* **Modalities**    T1w, resting-state fMRI
* **Populations**   494 healthy adults, 19 to 80 years
* **Host**          s3://fcp-indi (INDI), no login needed
* **Citation**      https://doi.org/10.1038/sdata.2018.134
"""
    sites = (Site("", "INDI/SALD/RawData_BIDS"),)


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


# ----------------------------------------------------------------------
#   MPI-Leipzig Mind-Brain-Body (LEMON)
# ----------------------------------------------------------------------

class LEMON(Project):
    name = "mpi-lemon"
    folder = "MPI-LEMON"
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


# ----------------------------------------------------------------------
#   Addiction Connectome Preprocessed Initiative (ACPI)
# ----------------------------------------------------------------------

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
        return _merge(per_site, ("age", "sex", "handedness"))


# ----------------------------------------------------------------------
#   NATVIEW (EEG-fMRI)
# ----------------------------------------------------------------------

class NATVIEW(Project):
    name = "natview"
    folder = "NATVIEW"
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


# ----------------------------------------------------------------------
#   SIMON (single individual, many sessions)
# ----------------------------------------------------------------------

class SIMON(Project):
    name = "simon"
    folder = "SIMON"
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


PROJECTS = [NKIRS(), HBN(), SALD(), SLIM(), LEMON(), ACPI(), NATVIEW(),
            SIMON()]
