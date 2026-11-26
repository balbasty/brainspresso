"""
`bx ixi roast`: see `IXI/roaster.py` for the expected input and output.
"""
from pathlib import Path
from typing import Literal, Iterable

from brainspresso.bids.layout import LayoutChoice
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.actions import IfExists
from brainspresso.datasets.IXI.command import ixi
from brainspresso.datasets.IXI.roaster import roast

KeyChoice = Literal["meta", "T1w", "T2w", "PDw", "angio", "dwi"]


@ixi.command(name="roast")
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
    subs: Iterable[int] | None = tuple(),
    exclude_subs: Iterable[int] | None = tuple(),
    json: Literal["yes", "no", "only"] | bool = "yes",
    if_exists: IfExists.Choice = "skip",
    layout: LayoutChoice | None = None,
    jobs: int = 1,
    log: str | None = None,
):
    """
    Convert source data into a BIDS-compliant directory

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `IXI/sourcedata` folder must exist.
    keys : [list of] {"meta", "T1w", "T2w", "PDw", "angio", "dwi"}
        Only bidsify these keys
    subs : [list of] int
        Only bidsify these subjects (all if empty)
    exclude_subs : [list of] int
        Do not bidsify these subjects
    json : {"yes", "no", "only"} | bool
        Whether to write (only) sidecar JSON files
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour when a file already exists
    layout : {"study", "flat"}
        Layout of the dataset: "study" (raw dataset in `rawbids/`)
        or "flat" (raw dataset at the root). Default: the layout of
        an existing dataset, else "study".
    jobs : int
        Number of processes (modalities are converted in parallel)
    log : str
        Path to log file
    """
    setup_filelog(log)
    if isinstance(keys, str):
        keys = [keys]
    if isinstance(subs, int):
        subs = [subs]
    if isinstance(exclude_subs, int):
        exclude_subs = [exclude_subs]
    roast(
        get_tree_path(path) / 'IXI',
        keys=keys or (),
        subs=subs or (),
        exclude_subs=exclude_subs or (),
        json=json,
        layout=layout,
        ifexists=if_exists,
        jobs=jobs,
    )
