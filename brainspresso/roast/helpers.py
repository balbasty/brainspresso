"""
Helpers shared by roasters.

```python
yield from copy_templates(ctx, ["README", "dataset_description.json"])
yield from subject_shards(ctx, labels, roast_subject)
yield from archive_shards(ctx, tarballs, roast_archive, tag="raw",
                          keys=("T1w", "bold"),
                          config=lambda path: template_config(ctx.resource,
                                                              NAMES))
header, error = attempt(read_header, path, name="sub-01_T1w.nii.gz",
                        tag="T1w")
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
from brainspresso.convert import ConvertError
from brainspresso.roast.context import RoastContext
from brainspresso.roast.policy import SEEN

__all__ = ["archive_shards", "attempt", "copy_templates", "scope",
           "subject_shards"]


def scope(keys: Iterable[str] | None = None,
          subs: Iterable[str] | str | None = None) -> dict:
    """
    Declare what of a roast can change the outputs of a shard.

    `Shard(scope=scope(...))`: the shard is then signed by its inputs and
    `config` only, and a later roast skips it if it writes no more than
    a recorded one (`RoastPolicy.covers`). The roaster promises that the
    selection decides which of its outputs exist, never their content
    (what else does goes in `config`), and that a selection that keeps
    no more writes no more.

    Parameters
    ----------
    keys : list[str] | None
        Every key whose being wanted can change what the shard writes:
        the BIDS keys of its raw outputs and the categories it reads
        with `ctx.wants`; None: all keys
    subs : list[str] | SEEN | None
        Its subjects: a list of labels (it writes only for them);
        `SEEN`: it asks `ctx.keeps_subject(label)` for every subject its
        inputs hold, before any other pruning, on every run; None:
        unknown (the whole subject filter counts)

    Returns
    -------
    dict
        The declaration (`Shard(scope=)`)
    """
    if isinstance(keys, str):
        keys = [keys]
    if isinstance(subs, str) and subs != SEEN:
        raise ValueError(f"subs must be a list of labels, SEEN or None: "
                         f"{subs!r}")
    if subs is not None and subs != SEEN:
        subs = sorted({str(s)[4:] if str(s).startswith("sub-") else str(s)
                       for s in subs})
    return {"keys": None if keys is None else sorted(set(keys)),
            "subs": subs}


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
    config=None,
    scope_keys: Iterable[str] | str | None = None,
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
    config : JSON-able | callable(label) -> JSON-able
        What else decides the outputs of a shard (`Shard(config=)`): the
        same for every subject, or a function of its label
    scope_keys : list[str] | "all" | None
        Keys that a subject shard can write (`scope`: each shard gets
        `scope(keys=scope_keys, subs=[label])`, "all": every key);
        None: no scope declared
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
        declared = None if scope_keys is None else scope(
            keys=None if scope_keys == "all" else scope_keys, subs=[label])
        yield Shard(
            fn, label, *args, tag=tag, advance=tag,
            id=f"roast/{tag}/sub-{label}", unit={"sub": label},
            inputs=inputs(label) if inputs else None,
            config=config(label) if callable(config) else config,
            scope=declared, **kwargs,
        )


def archive_shards(
    ctx: RoastContext,
    archives: Iterable[Path],
    fn: Callable,
    *,
    tag: str,
    keys: Iterable[str] | None,
    id: Callable[[Path], str] | None = None,
    inputs: Callable[[Path], list] | None = None,
    config: Callable[[Path], object] | None = None,
) -> Iterator[Shard]:
    """
    Yield one shard per archive, `fn(ctx, path)`.

    The sibling of `subject_shards` for archives that hold many subjects
    (a gz tarball is read by one shard, once): the number of archives is
    the total of the progress group `tag`, each shard advances it when
    done, and declares the keys it may write and the subjects it sees
    (`scope(keys=keys, subs=SEEN)`: `fn` calls `ctx.keeps_subject` for
    every subject of the archive, before any other pruning).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    archives : iterable of Path
        The archives (or folders that stand for one)
    fn : callable(ctx, path)
        Roaster of an archive (picklable)
    tag : str
        Progress group
    keys : list[str] | None
        Keys that a shard may write (`scope`; None: all keys)
    id : callable(path) -> str
        Task id (default: `roast/<tag>/<file name>`)
    inputs : callable(path) -> list[path]
        Source files of a shard (default: `[path]`)
    config : callable(path) -> JSON-able
        What else decides its outputs, e.g. its sidecar templates
        (`roast.sidecars.template_config`; default: None)

    Yields
    ------
    Shard
        One per archive
    """
    archives = list(archives)
    ctx.progress(total=len(archives), tag=tag)
    for path in archives:
        yield Shard(
            fn, path, tag=tag, advance=tag,
            id=id(path) if id else f"roast/{tag}/{path.name}",
            inputs=inputs(path) if inputs else [path],
            config=config(path) if config else None,
            scope=scope(keys=keys, subs=SEEN),
        )


def attempt(fn: Callable, *args, name: str, tag: str, what: str | None = None,
            errors: tuple[type[BaseException], ...] = (OSError, ValueError),
            **kwargs) -> tuple[object, Fail | None]:
    """
    Call a reader while producing actions; its error is a `Fail`.

    For sources that must be read when actions are yielded (a template
    whose content decides the action, a header read to choose a name),
    so that nothing escapes the producer: the error is that of the
    output `name` only.

    ```python
    content, error = attempt(read_json, path, name="sub-01_T1w.json",
                             tag="T1w", what=f"template {path}")
    if error:
        yield error
        return
    ```

    Parameters
    ----------
    fn : callable
        The reader, `fn(*args, **kwargs)`
    *args
        Its positional arguments
    name : str
        Output that the error is of (the name of the `Fail`)
    tag : str
        Progress group of the `Fail`
    what : str, optional
        What is read, in the message (default: `name`); a
        `convert.ConvertError` names its file itself
    errors : tuple[type]
        Errors converted (others propagate)
    **kwargs
        Its keyword arguments

    Returns
    -------
    result : object | None
        What `fn` returned (None on error)
    error : Fail | None
        The `Fail` (None on success)
    """
    try:
        return fn(*args, **kwargs), None
    except errors as e:
        if isinstance(e, ConvertError):
            message = str(e)
        else:
            message = (f"{what or name}: cannot be read "
                       f"({type(e).__name__}: {e})")
        return None, Fail(message, name=name, tag=tag)
