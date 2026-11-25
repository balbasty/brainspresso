"""
`compost` command, shared by all datasets.

```python
from brainspresso.datasets.compost import add_compost
add_compost(ixi, "IXI")
```
"""
import functools
import sys
from pathlib import Path
from typing import Annotated, Callable, Iterable

from cyclopts import App, Parameter

from brainspresso.bids.compost import compost
from brainspresso.bids.select import Selection
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path

# Options that accept several values after a single flag (`--subs 01 02`)
Many = Parameter(consume_multiple=True)

SUMMARY = """
Delete source data that has been roasted, or BIDS data outside a filter.

By default, `sourcedata/` is deleted (once it has been roasted into
BIDS). If BIDS filters are given, BIDS data that does *not* match them
is deleted instead (and source data is kept, unless `--sourcedata` is
used). Filters describe what is kept, exactly as in `harvest`: the
metadata that still applies to a kept file is kept, dataset-level files
are never deleted, and participants/sessions/scans tables are rewritten
to only list what is left.

The files to delete are always listed (`--dry-run`) or confirmed first
(`--yes` to skip the confirmation).
"""

EXAMPLES = """
Examples
--------
```
bx {cmd} compost --dry-run
bx {cmd} compost /data --keep "*.xls" "*.csv"
bx {cmd} compost /data --exclude-subs 0005 0012
bx {cmd} compost /data --datatypes anat --suffixes T1w
bx {cmd} compost /data --where "age>=60" --derivatives
```
"""

PARAMETERS = """
sourcedata
    Delete source data. Default: only if no BIDS filter is given.
nested
    Also delete `derivatives/*/sourcedata/` (raw files unpacked
    while roasting, e.g. FreeSurfer outputs)
keep
    Do not delete source files matching these glob patterns
    (relative to the dataset folder, `*` matches `/`), e.g. `"*.xls"`
subs
    Only keep these subjects (labels, with or without `sub-`)
exclude_subs
    Delete these subjects
sessions
    Only keep these sessions
datatypes
    Only keep these datatypes (anat, func, dwi, fmap, perf...)
suffixes
    Only keep these suffixes (T1w, T2w, bold, dwi...)
tasks
    Only keep these tasks
include
    Only keep data files matching these glob patterns
    (relative to the BIDS root, e.g. `"sub-*/anat/*"`)
exclude
    Delete files matching these glob patterns (relative to the BIDS root)
where
    Only keep participants matching these queries on participants.tsv
    columns, e.g. `"age>=60"` or `"sex==F"` (all queries must match)
derivatives
    Apply subject and session filters to `derivatives/*/` too
trim
    Rewrite participants/sessions/scans tables so that they only list
    what is left
force
    Delete source data even if no BIDS subject is found
    (i.e., if the dataset does not seem to have been roasted)
yes
    Do not ask for confirmation
dry_run
    Print the files that would be deleted, then exit
list
    Print the files to delete before asking for confirmation
log
    Path to log file
level
    Logging level
"""


def user_errors(func):
    """Print user-facing errors without a traceback"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (KeyError, ValueError, FileNotFoundError,
                PermissionError) as e:
            message = e.args[0] if isinstance(e, KeyError) else e
            print(f"Error: {message}", file=sys.stderr)
            sys.exit(1)
    return wrapper


def run_compost(
    root: Path,
    *,
    sourcedata, nested, keep,
    subs, exclude_subs, sessions, datatypes, suffixes, tasks,
    include, exclude, where, derivatives,
    trim, force, yes, dry_run, list, log, level,
):
    """Common implementation of all `compost` commands"""
    setup_filelog(log, level=level)
    selection = Selection(
        subs=subs,
        exclude_subs=exclude_subs,
        sessions=sessions,
        datatypes=datatypes,
        suffixes=suffixes,
        tasks=tasks,
        include=include,
        exclude=exclude,
    )
    return compost(
        root,
        sourcedata=sourcedata,
        nested=nested,
        keep=keep,
        selection=selection,
        where=where,
        derivatives=derivatives,
        trim=trim,
        force=force,
        yes=yes,
        dry_run=dry_run,
        listing=list,
    )


def make_doc(cmd: str, root_params: str) -> str:
    return (
        SUMMARY
        + EXAMPLES.format(cmd=cmd)
        + "\nParameters\n----------"
        + root_params
        + PARAMETERS
    )


def add_compost(app: App, folder: str) -> Callable:
    """
    Register a `compost` command in a dataset app.

    Parameters
    ----------
    app : App
        Dataset command (e.g. `ixi`)
    folder : str
        Name of the dataset folder under the root of all datasets
        (e.g. "IXI")
    """

    @user_errors
    def compost_(
        path: str | None = None,
        *,
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
        trim: bool = True,
        force: bool = False,
        yes: Annotated[bool, Parameter(name=["--yes", "-y"])] = False,
        dry_run: bool = False,
        list: bool = False,
        log: str | None = None,
        level: str = "info",
    ):
        run_compost(
            get_tree_path(path) / folder,
            sourcedata=sourcedata, nested=nested, keep=keep,
            subs=subs, exclude_subs=exclude_subs, sessions=sessions,
            datatypes=datatypes, suffixes=suffixes, tasks=tasks,
            include=include, exclude=exclude, where=where,
            derivatives=derivatives, trim=trim, force=force, yes=yes,
            dry_run=dry_run, list=list, log=log, level=level,
        )

    compost_.__doc__ = make_doc(app.name[0], f"""
path
    Path to root of all datasets (which contains `{folder}/`)""")
    app.command(compost_, name="compost")
    return compost_
