from pathlib import Path
from typing import Annotated, Iterable, Literal

from cyclopts import Parameter

from brainspresso.actions import IfExists
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.datasets.Bullitt.command import bullitt
from brainspresso.datasets.Bullitt.bidsifier import Bidsifier
from brainspresso.datasets.Bullitt.download import DATASET, _labels

KeyChoice = Literal[
    "meta", "flash", "mprage", "T2w", "angio", "skullstripped", "vessels"
]
Many = Parameter(consume_multiple=True)


@bullitt.command(name="roast")
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Annotated[Iterable[KeyChoice], Many] = KeyChoice.__args__,
    subs: Annotated[Iterable[str], Many] = (),
    exclude_subs: Annotated[Iterable[str], Many] = (),
    json: Literal["yes", "no", "only"] = "yes",
    if_exists: IfExists.Choice = "skip",
    log: str | None = None,
):
    """
    Convert the harvested MetaImage files to BIDS (NIfTI).

    DTI is not converted: the source files have no gradient table.

    Parameters
    ----------
    path
        Path to root of all datasets. A `Bullitt/sourcedata` folder
        must exist.
    keys
        Only convert these keys. Raw data: flash, mprage (T1w), T2w,
        angio; metadata: meta; derivatives (-> derivatives/bullitt):
        skullstripped, vessels
    subs
        Only convert these subjects (e.g. 1 2 or 001 002)
    exclude_subs
        Do not convert these subjects
    json
        Whether to write (only) the sidecar JSON files
    if_exists
        Behaviour when a file already exists
    log
        Path to log file
    """
    setup_filelog(log)
    if isinstance(keys, str):
        keys = [keys]
    Bidsifier(
        Path(get_tree_path(path)) / DATASET,
        keys=set(keys or KeyChoice.__args__),
        subs=_labels(subs, "sub-"),
        exclude_subs=_labels(exclude_subs, "sub-"),
        json=json,
        ifexists=if_exists,
    ).run()
