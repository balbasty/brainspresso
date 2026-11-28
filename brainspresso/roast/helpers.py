"""
Helpers shared by roasters.

```python
yield from copy_templates(ctx, ["README", "dataset_description.json"])
yield from subject_shards(ctx, labels, roast_subject)
yield from archive_shards(ctx, tarballs, roast_archive, tag="raw",
                          keys=("T1w", "bold"),
                          config=lambda path: template_config(ctx.resource,
                                                              NAMES))
header, error = attempt(read_header, path, fail_name="sub-01_T1w.nii.gz",
                        fail_tag="T1w")
named = named_keys(ROAST_SPEC, keys, ("T1w", "T2w"))   # (`--keys` as given)
yield from missing_input("IXI-T1.tar not found", name="IXI-T1",
                         required="T1w" in named, logger=lg)
```

They only factor the plumbing (resources, pruning, progress, task ids);
what a dataset converts stays in its roaster.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping

from brainspresso.actions.action import Action, Fail
from brainspresso.actions.engine import Shard
from brainspresso.actions.writers import CopyBytes, CopyJSON
from brainspresso.convert import ConvertError
from brainspresso.roast.context import RoastContext, as_list
from brainspresso.roast.policy import SEEN
from brainspresso.roast.tables import TableError

__all__ = ["archive_shards", "attempt", "copy_templates", "missing_input",
           "named_keys", "scope", "subject_shards"]

lg = logging.getLogger(__name__)


def scope(keys: Iterable[str] | None = None,
          sub: Iterable[str] | str | None = None) -> dict:
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
    sub : list[str] | SEEN | None
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
    if isinstance(sub, str) and sub != SEEN:
        raise ValueError(f"sub must be a list of labels, SEEN or None: "
                         f"{sub!r}")
    if sub is not None and sub != SEEN:
        sub = sorted({str(s)[4:] if str(s).startswith("sub-") else str(s)
                      for s in sub})
    return {"keys": None if keys is None else sorted(set(keys)),
            "sub": sub}


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
        `scope(keys=scope_keys, sub=[label])`, "all": every key);
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
            keys=None if scope_keys == "all" else scope_keys, sub=[label])
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
    (`scope(keys=keys, sub=SEEN)`: `fn` calls `ctx.keeps_subject` for
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
            scope=scope(keys=keys, sub=SEEN),
        )


def attempt(fn: Callable, *args, fail_name: str, fail_tag: str,
            fail_what: str | None = None,
            fail_errors: tuple[type[BaseException], ...] = (OSError,
                                                            ValueError),
            **kwargs) -> tuple[object, Fail | None]:
    """
    Call a reader while producing actions; its error is a `Fail`.

    For sources that must be read when actions are yielded (a sheet
    that gives several tables, the header of a table that its data
    dictionary needs), so that nothing escapes the producer: the error
    is that of the output `fail_name` only. The parameters of `attempt`
    are prefixed (`fail_`), so that every other keyword argument is the
    reader's (`name=`, `tag=`...).

    ```python
    sheet, error = attempt(read_xlsx, path, extra="oasis2",
                           fail_name="participants.tsv", fail_tag="meta")
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
    fail_name : str
        Output that the error is of (the name of the `Fail`)
    fail_tag : str
        Progress group of the `Fail`
    fail_what : str, optional
        What is read, in the message (default: `fail_name`); a
        `roast.tables.TableError` or a `convert.ConvertError` names
        its file itself
    fail_errors : tuple[type]
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
    except fail_errors as e:
        if isinstance(e, (TableError, ConvertError)):
            message = str(e)
        else:
            message = (f"{fail_what or fail_name}: cannot be read "
                       f"({type(e).__name__}: {e})")
        return None, Fail(message, name=fail_name, tag=fail_tag)


def named_keys(spec, keys, leaves: Iterable[str]) -> list[str]:
    """
    Return the leaves that `--keys` names, itself or through a group.

    The rule of the inputs that only some subjects or sources have (a
    missing input is an error only when the user asked for its key):
    a leaf is named when `--keys`, as given, selects it (any case, a
    group selects the leaves below it: `KeySpec.selects`). Nothing is
    named without `--keys`, or with its CLI default spelled out (the
    command line passes the default when the flag is absent, so the two
    cannot be told apart).

    ```python
    named_keys(ROAST_SPEC, ["raw"], ("T1w", "T2w", "vessels"))
    # ["T1w", "T2w"]
    ```

    Parameters
    ----------
    spec : KeySpec
        Keys of the roast (duck-typed: `selects(keys, leaf)` and
        `default`)
    keys : str | list[str] | None
        `--keys` as given (not `KeySpec.effective`, which turns keys
        that cover every choice into "all")
    leaves : iterable of str
        The leaves whose inputs may be missing

    Returns
    -------
    list[str]
        The named leaves, sorted (picklable: it goes in
        `RoastContext.options` and in a shard's `config`)
    """
    keys = as_list(keys)
    if not keys or {str(k).lower() for k in keys} == \
            {str(k).lower() for k in spec.default}:
        return []
    return sorted(leaf for leaf in leaves if spec.selects(keys, leaf))


def missing_input(message: str, *, name: str, required: bool,
                  tag: str | None = None, info: str | None = None,
                  level: int = logging.INFO,
                  logger: logging.Logger | None = None) -> Iterator[Fail]:
    """
    Report an input that is not there: a `Fail` if required, else a line.

    An input is required when the user named it (`--keys` names its key
    or a group above it, `named_keys`; `--discs`, `--parts`): its
    absence is then an error of the roast; otherwise it is skipped with
    one log line, and the roast can be complete.

    ```python
    yield from missing_input(f"{path.name} not found", name=path.name,
                             tag="discs", required=named is not None,
                             level=logging.WARNING, logger=lg)
    ```

    Parameters
    ----------
    message : str
        What is missing (the message of the `Fail`)
    name : str
        Output that the error is of (the name of the `Fail`)
    required : bool
        Whether the input was named (a `Fail`), else a log line
    tag : str | None
        Progress group of the `Fail` (None: that of the producer)
    info : str | None
        The log line (default: `"<message> (skipped)"`)
    level : int
        Its level (`logging.INFO`; OASIS-I/II discs and parts:
        `logging.WARNING`)
    logger : logging.Logger | None
        Its logger: the roaster's, so that the record names its dataset
        (default: that of this module)

    Yields
    ------
    Fail
        Only if `required`
    """
    if required:
        yield Fail(message, name=name, tag=tag)
        return
    (logger or lg).log(level, f"{message} (skipped)" if info is None
                       else info)
