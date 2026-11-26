"""
Helpers shared by roasters.

```python
yield from copy_templates(ctx, ["README", "dataset_description.json"])
yield from subject_shards(ctx, labels, roast_subject)
```

They only factor the plumbing (resources, pruning, progress, task ids);
what a dataset converts stays in its roaster.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping

from brainspresso.actions.action import Action, Fail
from brainspresso.actions.engine import Shard
from brainspresso.actions.writers import CopyBytes, CopyJSON
from brainspresso.roast.context import RoastContext

__all__ = ["copy_templates", "subject_shards"]


def copy_templates(
    ctx: RoastContext,
    names: Iterable[str] | Mapping[str, str | os.PathLike],
    folder: str | os.PathLike | None = None,
    *,
    tag: str = "meta",
) -> Iterator[Action]:
    """
    Copy metadata files found with `ctx.resource`.

    `ctx.source` first, then the templates of the dataset: `CopyJSON`
    for `*.json`, `CopyBytes` otherwise. A file found nowhere is a
    `Fail`.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    names : list[str] | dict[str, path]
        Resource names, copied to `folder / name`; or
        `{resource name: destination}`
    folder : path
        Destination folder (default: the raw dataset)
    tag : str
        Progress group

    Yields
    ------
    Action
        One copy (or `Fail`) per name
    """
    folder = Path(ctx.layout.raw if folder is None else folder)
    if isinstance(names, Mapping):
        items = [(name, Path(dst)) for name, dst in names.items()]
    else:
        items = [(name, folder / name) for name in names]
    for name, dst in items:
        src = ctx.resource(name)
        if src is None:
            yield Fail(f"{name} not found (in {ctx.source} or in the "
                       f"templates of the dataset)", name=dst.name, tag=tag)
            continue
        copy = CopyJSON if str(name).endswith(".json") else CopyBytes
        yield copy(src, dst, tag=tag)


def subject_shards(
    ctx: RoastContext,
    labels: Iterable[str],
    fn: Callable,
    *args,
    tag: str = "subjects",
    inputs: Callable[[str], list] | None = None,
    **kwargs,
) -> Iterator[Shard]:
    """
    Yield one shard per subject, `fn(ctx, label, *args, **kwargs)`.

    Subjects that the selection drops are pruned first (no archive is
    listed for them), the number of subjects is the total of the
    progress group `tag`, and each shard advances it when done. Shards
    have the task id `roast/<tag>/sub-<label>`.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    labels : iterable of str
        Subject labels (without `sub-`)
    fn : callable(ctx, label, *args, **kwargs)
        Roaster of a subject (picklable)
    *args
        More positional arguments of `fn`
    tag : str
        Progress group
    inputs : callable(label) -> list[path] | None
        Source files of a subject (see `Shard(inputs=)`)
    **kwargs
        Keyword arguments of `fn`

    Yields
    ------
    Shard
        One per selected subject
    """
    labels = [label for label in labels
              if ctx.selection.keeps_subject(label)]
    ctx.progress(total=len(labels), tag=tag)
    for label in labels:
        yield Shard(
            fn, label, *args, tag=tag, advance=tag,
            id=f"roast/{tag}/sub-{label}", unit={"sub": label},
            inputs=inputs(label) if inputs else None, **kwargs,
        )
