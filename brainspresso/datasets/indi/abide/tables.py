"""
Tables of the ABIDE phenotypic CSV files (shared by ABIDE-I and II).

Each table (`participants.tsv`, `phenotype/<name>.tsv`) is one mapping
of columns (`Table.columns`: BIDS name -> CSV column, or alternative
CSV columns) and of values (`Table.values`): a `roast.tables.Table`
with the subject labels (`participant_label`) and missing values
(`NANS`: `-9999` too) of ABIDE. Cells are read by the name of their
column in each file, so files may order (or lack) columns differently;
a column that a file lacks is `n/a` in its rows. Column names are
matched without case and whitespace (`ADI_R_A3_Total` is
`ADI_R_A3_TOTAL`, `RBSR_5SUBSCALE_RITUAL   ISTIC` is
`RBSR_5SUBSCALE_RITUALISTIC`).

```python
table = Table({"participant_id": "SUB_ID", "sex": "SEX"},
              {"sex": {"1": "M", "2": "F"}})
rows = table.rows(["phenotypic_CALTECH.csv"], "participants.tsv")
```
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from brainspresso.roast import tables

__all__ = [
    "NANS",
    "WIDTH",
    "Table",
    "participant_label",
]

WIDTH = 5               # subject labels: `sub-51456`

#: Values that mean "missing" (written `n/a`)
NANS = tables.NANS | {'-9999', '-9999.0', '-9999.00'}


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


@dataclass(frozen=True)
class Table(tables.Table):
    """
    A table derived from the phenotypic CSV files.

    A `roast.tables.Table` with the missing values (`NANS`) and subject
    labels (`participant_label`) of ABIDE.

    Parameters
    ----------
    columns : dict[str, str | tuple[str]]
        BIDS column -> CSV column (`participant_id` first); a tuple
        lists alternative names, the first one a file has is read
    values : dict[str, dict | callable]
        BIDS column -> value mapping (values not in a dict are kept)
    baseline : str, optional
        CSV column (`AGE_AT_SCAN`) that tells, among the rows of a
        subject listed more than once in a file (baseline and
        follow-up), which one is kept: the lowest value. Without it,
        the first row is kept
    """

    nans: frozenset = NANS
    label: Callable[[str], str] = participant_label
