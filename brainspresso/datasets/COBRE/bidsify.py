"""
Expected input
--------------
COBRE/
  sourcedata/
    COBRE_parameters_mprage.csv
    COBRE_parameters_rest.csv
    COBRE_phenotypic_data.csv
    COBRE_scan_data.tgz

Expected output
---------------
COBRE/
  dataset_description.json
  participants.tsv
  participants.json
  rawdata/
      sub-{07d}/
          anat/
              sub-{07d}_T1w.nii.gz
              sub-{07d}_T1w.json
          func/
              sub-{07d}_bold.nii.gz
              sub-{07d}_bold.json
"""

from pathlib import Path
from typing import Literal, Iterable

from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.actions import IfExists
from brainspresso.datasets.COBRE.command import cobre
from brainspresso.datasets.COBRE.bidsifier import Bidsifier

KeyChoice = Literal["meta", "T1w", "func"]


@cobre.command(name="roast")
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
    subs: Iterable[int] | None = tuple(),
    exclude_subs: Iterable[int] | None = tuple(),
    json: Literal["yes", "no", "only"] | bool = "yes",
    if_exists: IfExists.Choice = "skip",
    log: str | None = None,
):
    """
    Convert source data into a BIDS-compliant directory

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `IXI/sourcedata` folder must exist.
    keys : [list of] {"meta", "T1w", "func"}
        Only bidsify these keys
    subs : [list of] int
        Only bidsify these subjects (all if empty)
    exclude_subs : [list of] int
        Do not bidsify these subjects
    json : {"yes", "no", "only"} | bool
        Whether to write (only) sidecar JSON files
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour when a file already exists
    log : str
        Path to log file
    """
    setup_filelog(log)

    # Format keys
    if isinstance(keys, str):
        keys = [keys]
    keys = set(keys or KeyChoice.__args__)

    # Format subjects
    if isinstance(subs, int):
        subs = [subs]
    subs = list(subs or [])

    if isinstance(exclude_subs, int):
        exclude_subs = [exclude_subs]
    exclude_subs = list(exclude_subs or [])

    # Format json
    if isinstance(json, bool):
        json = 'yes' if json else 'no'
    json = json.lower()

    # Get root
    root = get_tree_path(path) / 'COBRE'

    # Bidsify
    Bidsifier(
        root,
        keys=keys,
        subs=subs,
        exclude_subs=exclude_subs,
        json=json,
        ifexists=if_exists,
    ).run()
