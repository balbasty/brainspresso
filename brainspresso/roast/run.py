"""
Run a roast: the engine with the roast policy, then the common steps.

```python
def roast_ixi(ctx):
    yield from copy_templates(ctx, ["README", "dataset_description.json"])
    for key in ("T1w", "T2w"):
        yield Shard(roast_anat, key, tag=key)     # picklable sub-roaster

run_roast(roast_ixi, ctx, jobs=4)
```

A **roaster** is a producer of the engine (`actions.engine`): a plain
callable ``roaster(ctx) -> Iterator[Action | Roaster]`` that reads
source data (`utils.archive`, from `ctx.source`) and yields actions that
write the BIDS dataset, or sub-roasters (shards). The engine rules
(order, shards, parallelism, task states, `finalize` hooks) are those
of `actions.engine`; which actions run, and with what `IfExists`, is
decided by `roast.RoastPolicy`.

After all actions, shards and `finalize` hooks, even if the roast
failed: the visible tables are trimmed from their pristine copies
(`bids.harvest.trim_originals`), then the layout descriptions and the
manifest are written (`bids.harvest.finish_roast`, with a `status`:
"complete", "partial" if anything failed, "failed" if the engine itself
stopped).
"""
from __future__ import annotations

import os
import traceback
from logging import getLogger
from typing import Any, Callable, Iterable

from brainspresso.actions.engine import Producer, Report, run
from brainspresso.bids.keys import KeyAliases
from brainspresso.bids.layout import Layout
from brainspresso.roast.context import RoastContext, as_list, make_context
from brainspresso.roast.policy import RoastPolicy

lg = getLogger(__name__)

__all__ = ["RoastReport", "Roaster", "roast_dataset", "run_roast"]

#: A roaster: a producer of the engine (`actions.engine.Producer`)
Roaster = Producer
#: What a roast did (`actions.engine.Report`)
RoastReport = Report


def run_roast(
    roaster: Roaster,
    ctx: RoastContext,
    *,
    jobs: int | None = 1,
    display: bool | Callable[[dict], Any] = True,
    info: dict | None = None,
    executor=None,
    policy: RoastPolicy | None = None,
) -> RoastReport:
    """
    Run a roaster and the common steps that follow every roast.

    Parameters
    ----------
    roaster : callable(ctx) -> iterator of Action | roaster
        The top-level roaster
    ctx : RoastContext
        Context of the roast
    jobs : int
        Number of processes for shards (1: sequential)
    display : bool | callable(row)
        Show the progress table (or send its rows to a callable)
    info : dict
        Extra fields of the run, in the manifest
    executor : object with `submit(task, resources=None) -> Future`
        Backend for shards (default: `LocalExecutor(jobs)` if
        `jobs > 1`)
    policy : RoastPolicy | None
        Acceptance policy (default: `RoastPolicy()`)

    Returns
    -------
    RoastReport
        Counts per tag, errors, outputs, progress and `status`
    """
    from brainspresso.bids.harvest import finish_roast, trim_originals

    report = RoastReport(status="failed")
    try:
        # the state folder holds the lock files of the dataset
        # (`actions.locks`): create it before anything is read or written
        ctx.layout.ensure_state()
        run(roaster, ctx, policy=policy or RoastPolicy(), jobs=jobs,
            display=display, executor=executor, report=report)
    finally:
        status = report.status
        try:
            trim_originals(ctx.layout)
        except Exception as e:
            lg.error(f"Could not trim tables: {e}\n{traceback.format_exc()}")
            report.status = status = "failed" if status == "failed" \
                else "partial"
        keys = ctx.selection.keys
        finish_roast(ctx.layout, **{
            **({"keys": sorted(keys)} if keys is not None else {}),
            "json": ctx.json,
            "status": status,
            "counts": report.summary(),
            **(info or {}),
        })
    return report


def roast_dataset(
    roaster: Roaster,
    root: str | os.PathLike | Layout,
    aliases: KeyAliases,
    *,
    keys: Iterable[str] = (),
    exclude_keys: Iterable[str] = (),
    templates: str | os.PathLike | None = None,
    jobs: int = 1,
    display: bool = True,
    info: dict | None = None,
    **options,
) -> RoastReport:
    """
    Build the context and run a roast: the `roast()` of a dataset.

    `make_context` + `run_roast`, with the names of the command line.

    Parameters
    ----------
    roaster : callable
        The top-level roaster
    root : path
        Dataset folder
    aliases : KeyAliases
        Keys of the dataset
    keys, exclude_keys : list[str]
        `--keys`, `--exclude-keys`
    templates : path
        Templates shipped with the dataset
    jobs, display, info
        See `run_roast`
    **options
        `make_context` options (`layout`, `json`, `ifexists`, `subs`,
        `exclude_subs`, `sessions`...)

    Returns
    -------
    RoastReport
        See `run_roast`
    """
    ctx = make_context(root, aliases, select_keys=as_list(keys),
                       exclude_keys=as_list(exclude_keys),
                       templates=templates, **options)
    return run_roast(roaster, ctx, jobs=jobs, display=display, info=info)
