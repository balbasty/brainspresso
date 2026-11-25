from pathlib import Path
from typing import Annotated, Iterable, Literal

from cyclopts import Parameter

from brainspresso.actions import IfExists
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.datasets.UNCWisconsin.command import uncwisc
from brainspresso.datasets.UNCWisconsin.bidsifier import Bidsifier
from brainspresso.datasets.UNCWisconsin.download import DATASET, _labels

KeyChoice = Literal["meta", "T1w", "T2w", "dwi"]
Many = Parameter(consume_multiple=True)


@uncwisc.command(name="roast")
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Annotated[Iterable[KeyChoice], Many] = KeyChoice.__args__,
    subs: Annotated[Iterable[str], Many] = (),
    exclude_subs: Annotated[Iterable[str], Many] = (),
    sessions: Annotated[Iterable[str], Many] = (),
    if_exists: IfExists.Choice = "skip",
    log: str | None = None,
):
    """
    Convert the harvested NRRD files to BIDS (NIfTI).

    Parameters
    ----------
    path
        Path to root of all datasets. An `UNC-Wisconsin/sourcedata`
        folder must exist.
    keys
        Only convert these keys
    subs
        Only convert these subjects (e.g. 001 002)
    exclude_subs
        Do not convert these subjects
    sessions
        Only convert these sessions (e.g. 12months)
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
        sessions=_labels(sessions, "ses-"),
        ifexists=if_exists,
    ).run()
