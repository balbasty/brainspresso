from typing import Annotated, Iterable
from pathlib import Path

from cyclopts import Parameter

from brainspresso.bids.layout import LayoutChoice
from brainspresso.datasets.compost import Many, make_doc, run_compost
from brainspresso.datasets.compost import user_errors
from brainspresso.datasets.OpenNeuro.command import openneuro
from brainspresso.utils.path import get_tree_path


@user_errors
def compost(
    dataset: str,
    path: str | None = None,
    *,
    out: str | None = None,
    sourcedata: bool | None = None,
    nested: bool = False,
    keep: Annotated[Iterable[str], Many] = (),
    subs: Annotated[Iterable[str], Many] = (),
    exclude_subs: Annotated[Iterable[str], Many] = (),
    sessions: Annotated[Iterable[str], Many] = (),
    datatypes: Annotated[Iterable[str], Many] = (),
    suffixes: Annotated[Iterable[str], Many] = (),
    tasks: Annotated[Iterable[str], Many] = (),
    include: Annotated[Iterable[str], Many] = (),
    exclude: Annotated[Iterable[str], Many] = (),
    where: Annotated[Iterable[str], Many] = (),
    derivatives: bool = False,
    layout: LayoutChoice | None = None,
    trim: bool = True,
    force: bool = False,
    yes: Annotated[bool, Parameter(name=["--yes", "-y"])] = False,
    dry_run: bool = False,
    list: bool = False,
    log: str | None = None,
    level: str = "info",
):
    root = Path(out) if out else (
        get_tree_path(path) / "OpenNeuro" / dataset
    )
    run_compost(
        root,
        sourcedata=sourcedata, nested=nested, keep=keep,
        subs=subs, exclude_subs=exclude_subs, sessions=sessions,
        datatypes=datatypes, suffixes=suffixes, tasks=tasks,
        include=include, exclude=exclude, where=where,
        derivatives=derivatives, trim=trim, force=force, yes=yes,
        dry_run=dry_run, list=list, log=log, level=level, layout=layout,
    )


compost.__doc__ = make_doc("openneuro", """
dataset
    Accession number (e.g. ds000224)
path
    Path to root of all datasets. An `OpenNeuro/<dataset>` folder
    must exist. Ignored if `--out` is used.
out
    Dataset folder (overrides `path`)""").replace(
    "bx openneuro compost", "bx openneuro compost ds000224"
)
openneuro.command(compost, name="compost")
