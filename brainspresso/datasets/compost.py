"""
Body of the `compost` command, shared by all datasets.

`Dataset.compost` calls `run_compost`; the options are those of
`datasets.options.CompostOptions`, `SubjectOptions`, `SessionOptions`
and `FilterOptions`.
"""
from pathlib import Path
from typing import Iterable

from brainspresso.bids.compost import compost
from brainspresso.bids.layout import Layout, SitesLayout, resolve_layout
from brainspresso.bids.select import Selection
from brainspresso.errors import UsageError

SUMMARY = """
Delete source data that has been roasted, or BIDS data outside a filter.

By default, `sourcedata/` is deleted (once it has been roasted into
BIDS). If BIDS filters are given, BIDS data that does *not* match them
is deleted instead (and source data is kept, unless `--sourcedata` is
used). Filters describe what is kept, exactly as in `harvest`: the
metadata that still applies to a kept file is kept, dataset-level files
are never deleted, and participants/sessions/scans tables (and the
phenotype tables a roast wrote, with a pristine copy) are rewritten
to only list what is left. A dataset with one dataset per site is
composted site by site (`--site`: only these sites).

The files to delete are always listed (`--dry-run`) or confirmed first
(`--yes` to skip the confirmation). Compost fails while a harvest or
roast of the dataset runs (`--lock-wait`).
"""

EXAMPLES = """
Examples
--------
```
bx {cmd} compost --dry-run
bx {cmd} compost /data --keep "*.xls" "*.csv"
bx {cmd} compost /data --exclude-sub 0005 0012
bx {cmd} compost /data --datatype anat --suffix T1w
bx {cmd} compost /data --where "age>=60" --derivatives
```
"""


def run_compost(
    root: str | Path | Layout,
    *,
    sourcedata: bool | None = None,
    nested: bool = False,
    keep: Iterable[str] = (),
    sub: Iterable[str] = (),
    exclude_sub: Iterable[str] = (),
    ses: Iterable[str] = (),
    datatype: Iterable[str] = (),
    suffix: Iterable[str] = (),
    task: Iterable[str] = (),
    include: Iterable[str] = (),
    exclude: Iterable[str] = (),
    where: Iterable[str] = (),
    derivatives: bool = False,
    trim: bool = True,
    force: bool = False,
    yes: bool = False,
    dry_run: bool = False,
    list: bool = False,
    layout: str | None = None,
    site: Iterable[str] = (),
    exclude_site: Iterable[str] = (),
    after_data=None,
):
    """
    Compost a dataset (a "sites" layout: site by site).

    Parameters
    ----------
    root : path | Layout
        Dataset folder, or its layout
    sourcedata, nested, keep, derivatives, trim, force, yes, dry_run, list
        `CompostOptions`
    sub, exclude_sub, ses, datatype, suffix, task, include, exclude, where
        Filters (what is kept): those of the command line (`--sub`...)
    layout : str, optional
        Expected layout
    site : list[str]
        Only these sites (a "sites" layout only)
    exclude_site : list[str]
        Not these sites (a "sites" layout only)
    after_data : callable(layout), optional
        Run on each dataset after its BIDS data is deleted, before its
        source data is (`bids.compost.compost`)

    Returns
    -------
    CompostPlan | list[CompostPlan]
        The plan (one per site)
    """
    found = resolve_layout(root, layout)
    sites = [str(s) for s in site or ()]
    excluded = {str(s).lower() for s in exclude_site or ()}
    options = dict(
        sourcedata=sourcedata, nested=nested, keep=keep, sub=sub,
        exclude_sub=exclude_sub, ses=ses, datatype=datatype,
        suffix=suffix, task=task, include=include, exclude=exclude,
        where=where, derivatives=derivatives, trim=trim, force=force,
        yes=yes, dry_run=dry_run, list=list, after_data=after_data,
    )
    if isinstance(found, SitesLayout):
        datasets = [d for d in found.datasets() if Path(d.root).is_dir()]
        if sites:
            names = {s.lower() for s in sites}
            datasets = [d for d in datasets
                        if Path(d.root).name.lower() in names]
        datasets = [d for d in datasets
                    if Path(d.root).name.lower() not in excluded]
        if not datasets:
            raise UsageError(f"No site dataset found in {found.root}")
        plans = []
        for dataset in datasets:
            print(f"--- {Path(dataset.root).name}")
            plans.append(run_compost(dataset, **options))
        return plans
    if sites or excluded:
        raise UsageError(
            f"--site/--exclude-site only apply to a dataset harvested "
            f"with one dataset per site (--layout sites); {found.root} has "
            f"layout {found.kind!r}"
        )
    selection = Selection(
        sub=sub, exclude_sub=exclude_sub, ses=ses,
        datatype=datatype, suffix=suffix, task=task,
        include=include, exclude=exclude,
    )
    return compost(
        found, sourcedata=sourcedata, nested=nested, keep=keep,
        selection=selection, where=where, derivatives=derivatives,
        trim=trim, force=force, yes=yes, dry_run=dry_run, listing=list,
        after_data=after_data,
    )
