"""
Participants and phenotype tables of ABIDE-I.

The phenotypic CSV files (`phenotypic_<SITE>.csv` of NITRC and of the
bucket, one per site and sample; `Phenotypic_V1_0b.csv` of NITRC-IR, all
sites) share their columns. Each table is one mapping of columns
(`Table.columns`: BIDS name -> CSV column) and of values
(`Table.values`). Cells are read by name in each file, so files may
order (or lack) columns differently; a column that a file lacks is
`n/a` in its rows.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import Iterable, Iterator, Mapping

from brainspresso.datasets.indi.abide.i.sites import TemplateError, parse_site
from brainspresso.utils.io import write_tsv

lg = getLogger(__name__)

__all__ = [
    "PARTICIPANTS",
    "PHENOTYPES",
    "Table",
    "participant_label",
    "write_table",
]

WIDTH = 5               # subject labels: `sub-51456`

#: Values that mean "missing" (written `n/a`)
NANS = frozenset({'', 'n/a', 'N/A', 'nan', 'NaN', 'NAN', 'na', 'NA',
                  '-9999', '-9999.0'})


_warned: set = set()


def _warn(path: Path, line: int, message: str) -> None:
    """Log a warning about a row once (every table reads the files)."""
    key = (str(path), path.stat().st_mtime_ns, line, message)
    if key not in _warned:
        _warned.add(key)
        lg.warning(f"{path.name}, line {line}: {message}")


def participant_label(value: str) -> str:
    """
    Label of a subject (numbers zero-padded to 5 digits).

    Parameters
    ----------
    value : str
        Subject identifier of a CSV file, an archive or a folder

    Returns
    -------
    str
        `"51456"`, `"0051456"` -> `"51456"`; other values stripped, as
        they are
    """
    value = str(value).strip()
    if value.startswith("sub-"):
        value = value[4:]
    return f"{int(value):0{WIDTH}d}" if value.isdigit() else value


def _site(value: str) -> str:
    """Site of a `SITE_ID` (`MAX_MUN` -> `MaxMun`)."""
    try:
        return parse_site(value)[0]
    except TemplateError:
        return value


def _sample(value: str) -> str:
    """Sample of a `SITE_ID` (`UCLA_1` -> `UCLA1`, `CALTECH` -> `Caltech`)."""
    try:
        site, sample = parse_site(value)
    except TemplateError:
        return value
    return f"{site}{sample or ''}"


@dataclass(frozen=True)
class Table:
    """
    A table derived from the phenotypic CSV files.

    Parameters
    ----------
    columns : dict[str, str]
        BIDS column -> CSV column (`participant_id` first)
    values : dict[str, dict | callable]
        BIDS column -> value mapping (values not in a dict are kept)
    """

    columns: Mapping[str, str]
    values: Mapping[str, object] = field(default_factory=dict)

    def convert(self, column: str, value: str) -> str:
        """
        Convert one cell.

        Parameters
        ----------
        column : str
            BIDS column
        value : str
            CSV cell

        Returns
        -------
        str
            `n/a` for missing values
        """
        value = value.strip()
        if value in NANS:
            return "n/a"
        mapping = self.values.get(column)
        if callable(mapping):
            return mapping(value)
        if mapping is not None:
            return mapping.get(value, value)
        return value

    def rows(self, paths: Iterable[Path]) -> Iterator[list[str]]:
        """
        Rows of the table (header first) from CSV files.

        Blank rows are skipped; rows without a subject, or that do not
        reach the subject column, are skipped with a warning, as are the
        later rows of a subject listed twice.

        Parameters
        ----------
        paths : list[path]
            Phenotypic CSV files

        Yields
        ------
        list[str]
            The header, then one row per subject
        """
        yield list(self.columns)
        seen = set()
        for path in paths:
            path = Path(path)
            with open(path, "rt", newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                header = [h.strip() for h in next(reader, [])]
                index = {h: i for i, h in enumerate(header)}
                missing = [c for c in self.columns.values()
                           if c not in index]
                if self.columns["participant_id"] in missing:
                    lg.warning(f"{path.name}: no column "
                               f"{self.columns['participant_id']}: skipped")
                    continue
                if missing:
                    lg.debug(f"{path.name}: no column(s) {missing} (n/a)")
                sub = index[self.columns["participant_id"]]
                for row in reader:
                    if not any(cell.strip() for cell in row):
                        continue
                    line = reader.line_num
                    if len(row) <= sub or not row[sub].strip():
                        _warn(path, line, "row skipped (no subject)")
                        continue
                    label = participant_label(row[sub])
                    if not label.isalnum():
                        _warn(path, line, f"row skipped (subject "
                                          f"{label!r} is not a label)")
                        continue
                    if label in seen:
                        _warn(path, line, f"row skipped (subject {label} "
                                          f"listed twice)")
                        continue
                    seen.add(label)
                    out = [f"sub-{label}"]
                    for column, name in list(self.columns.items())[1:]:
                        i = index.get(name)
                        cell = row[i] if i is not None and i < len(row) \
                            else ""
                        out.append(self.convert(column, cell))
                    yield out


def write_table(table: Table, paths: list[Path], path_tsv) -> None:
    """
    Write a table (every subject of the CSV files).

    Parameters
    ----------
    table : Table
        The table
    paths : list[path]
        Phenotypic CSV files
    path_tsv : path
        Output table
    """
    write_tsv(table.rows(paths), path_tsv)


# ----------------------------------------------------------------------
#   Participants
# ----------------------------------------------------------------------

# NOTE
#   There are two different ages in the csv: AGE_AT_SCAN and
#   AGE_AT_MPRAGE. Only the first one is kept.

IQ_TYPES = {
    'DAS_II_SA': 'DAS-II-SA',
    'WISC': 'WISC',
    'WISC_III': 'WISC-III',
    'WISC_III_DUTCH': 'WISC-III-DUTCH',
    'WISC_IV_FULL': 'WISC-IV_FULL',
    'WISC_IV_4_SUBTESTS': 'WISC-IV_SUB',
    'WAIS_III': 'WAIS-III',
    'HAWIK_IV': 'HAWIK-IV',
}
YES_NO = {'0': 'N', '1': 'Y'}

PARTICIPANTS = Table(
    columns={
        'participant_id': 'SUB_ID',
        'site': 'SITE_ID',
        'sample': 'SITE_ID',
        'sex': 'SEX',
        'age': 'AGE_AT_SCAN',
        'handedness': 'HANDEDNESS_CATEGORY',
        'handedness_score': 'HANDEDNESS_SCORES',
        'group': 'DX_GROUP',
        'dsm4': 'DSM_IV_TR',
        'fiq': 'FIQ',
        'viq': 'VIQ',
        'piq': 'PIQ',
        'fiq_type': 'FIQ_TEST_TYPE',
        'viq_type': 'VIQ_TEST_TYPE',
        'piq_type': 'PIQ_TEST_TYPE',
        'scq': 'SCQ_TOTAL',
        'aq': 'AQ_TOTAL',
        'comorbidity': 'COMORBIDITY',
        'med_status': 'CURRENT_MED_STATUS',
        'med_name': 'MEDICATION_NAME',
        'off_stimulant': 'OFF_STIMULANTS_AT_SCAN',
        'eye_status': 'EYE_STATUS_AT_SCAN',
        'bmi': 'BMI',
    },
    values={
        'site': _site,
        'sample': _sample,
        'sex': {'1': 'M', '2': 'F'},
        'handedness': {'Ambi': 'B'},
        'group': {'1': 'A', '2': 'C'},
        'fiq_type': IQ_TYPES,
        'viq_type': IQ_TYPES,
        'piq_type': IQ_TYPES,
        'med_status': YES_NO,
        'off_stimulant': YES_NO,
        'eye_status': {'1': 'O', '2': 'C'},
    },
)

# ----------------------------------------------------------------------
#   Phenotypes (`phenotype/<name>.tsv`)
# ----------------------------------------------------------------------

PHENOTYPES: dict[str, Table] = {
    'adir': Table(
        columns={
            'participant_id': 'SUB_ID',
            'social': 'ADI_R_SOCIAL_TOTAL_A',
            'verbal': 'ADI_R_VERBAL_TOTAL_BV',
            'rrb': 'ADI_RRB_TOTAL_C',
            'onset': 'ADI_R_ONSET_TOTAL_D',
            'reliable': 'ADI_R_RSRCH_RELIABLE',
        },
        values={'reliable': YES_NO},
    ),
    'ados': Table(
        columns={
            'participant_id': 'SUB_ID',
            'module': 'ADOS_MODULE',
            'total': 'ADOS_TOTAL',
            'comm': 'ADOS_COMM',
            'social': 'ADOS_SOCIAL',
            'stereo_behav': 'ADOS_STEREO_BEHAV',
            'reliable': 'ADOS_RSRCH_RELIABLE',
        },
        values={'reliable': YES_NO},
    ),
    'ados_gotham': Table(
        columns={
            'participant_id': 'SUB_ID',
            'soc_affect': 'ADOS_GOTHAM_SOCAFFECT',
            'rrb': 'ADOS_GOTHAM_RRB',
            'total': 'ADOS_GOTHAM_TOTAL',
            'severity': 'ADOS_GOTHAM_SEVERITY',
        },
    ),
    'srs': Table(
        columns={
            'participant_id': 'SUB_ID',
            'version': 'SRS_VERSION',
            'raw_total': 'SRS_RAW_TOTAL',
            'awareness': 'SRS_AWARENESS',
            'cognition': 'SRS_COGNITION',
            'communication': 'SRS_COMMUNICATION',
            'motivation': 'SRS_MOTIVATION',
            'mannerisms': 'SRS_MANNERISMS',
        },
        values={'version': {'1': 'C', '2': 'A'}},
    ),
    'vineland': Table(
        columns={
            'participant_id': 'SUB_ID',
            'receptive': 'VINELAND_RECEPTIVE_V_SCALED',
            'expressive': 'VINELAND_EXPRESSIVE_V_SCALED',
            'written': 'VINELAND_WRITTEN_V_SCALED',
            'communication': 'VINELAND_COMMUNICATION_STANDARD',
            'personal': 'VINELAND_PERSONAL_V_SCALED',
            'domestic': 'VINELAND_DOMESTIC_V_SCALED',
            'community': 'VINELAND_COMMUNITY_V_SCALED',
            'dailylvng': 'VINELAND_DAILYLVNG_STANDARD',
            'interpersonal': 'VINELAND_INTERPERSONAL_V_SCALED',
            'play': 'VINELAND_PLAY_V_SCALED',
            'coping': 'VINELAND_COPING_V_SCALED',
            'social': 'VINELAND_SOCIAL_STANDARD',
            'sum': 'VINELAND_SUM_SCORES',
            'abc': 'VINELAND_ABC_STANDARD',
            'informant': 'VINELAND_INFORMANT',
        },
        values={'informant': {'1': 'P', '2': 'S'}},
    ),
    'wisc_iv': Table(
        columns={
            'participant_id': 'SUB_ID',
            'vci': 'WISC_IV_VCI',
            'pri': 'WISC_IV_PRI',
            'wmi': 'WISC_IV_WMI',
            'psi': 'WISC_IV_PSI',
            'sim': 'WISC_IV_SIM_SCALED',
            'vocab': 'WISC_IV_VOCAB_SCALED',
            'info': 'WISC_IV_INFO_SCALED',
            'blk_dsn': 'WISC_IV_BLK_DSN_SCALED',
            'pic_con': 'WISC_IV_PIC_CON_SCALED',
            'matrix': 'WISC_IV_MATRIX_SCALED',
            'digit_span': 'WISC_IV_DIGIT_SPAN_SCALED',
            'let_num': 'WISC_IV_LET_NUM_SCALED',
            'coding': 'WISC_IV_CODING_SCALED',
            'sym': 'WISC_IV_SYM_SCALED',
        },
    ),
}
