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
stopped), with the hashes of the tracked files that hold what the roast
wrote (`Report.digests`, origin "converted": the edit test of
`--if-exists update`).
"""
from __future__ import annotations

import functools
import inspect
import os
import sys
import textwrap
import traceback
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from brainspresso.actions import WriteText
from brainspresso.actions.engine import Producer, Report, run
from brainspresso.bids.harvest import (
    description_hashes,
    finish_roast,
    trim_originals,
)
from brainspresso.bids.keys import KeyAliases
from brainspresso.bids.layout import README, Layout, study_readme
from brainspresso.roast.context import RoastContext, as_list, make_context
from brainspresso.roast.policy import RoastPolicy
from brainspresso.sources.base import subject_labels

lg = getLogger(__name__)

__all__ = ["RoastReport", "Roaster", "python_api", "roast_dataset",
           "run_roast"]

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
    trim: bool = True,
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
    trim : bool
        Trim the tables from their pristine copies in `<state>/orig/`
        (`trim_originals`); hosted datasets trim theirs as they roast
        them (`roast.hosted`)

    Returns
    -------
    RoastReport
        Counts per tag, errors, outputs, progress and `status`
    """
    report = RoastReport(status="failed")
    if isinstance(ctx, RoastContext):
        ctx.forget()                # (hashes recorded by an earlier run)
    descriptions = None
    try:
        # the state folder holds the lock files of the dataset
        # (`actions.locks`): create it before anything is read or written
        ctx.layout.ensure_state()
        # (descriptions that exist: completed only if the roast writes
        # them, `finish_roast`)
        descriptions = description_hashes(ctx.layout)
        run(roaster, ctx, policy=policy or RoastPolicy(), jobs=jobs,
            display=display, executor=executor, report=report)
    finally:
        status = report.status
        try:
            if trim:
                trim_originals(ctx.layout)
        except Exception as e:
            lg.error(f"Could not trim tables: {e}\n{traceback.format_exc()}")
            report.status = status = "failed" if status == "failed" \
                else "partial"
        keys = ctx.selection.keys
        files = {}
        for path, digest in report.digests.items():
            try:
                key = Path(path).relative_to(ctx.layout.root).as_posix()
            except ValueError:
                continue
            files[key] = {"origin": "converted", "sha256": digest}
        finish_roast(ctx.layout, files=files, descriptions=descriptions, **{
            **({"keys": sorted(keys)} if keys is not None else {}),
            "json": ctx.json,
            "status": status,
            "counts": report.summary(),
            **(info or {}),
        })
    return report


def with_study_readme(roaster: Roaster) -> Roaster:
    """
    Add the README of a study dataset to a roaster.

    A study (`layout.kind == "study"`) gets `<DS>/README`
    (`bids.layout.study_readme`, BIDS: recommended) through the engine,
    as any output (`--if-exists`, its hash in the manifest), unless the
    user has a README of their own (`StudyLayout.readme_of_user`: any
    `README*` name, in any case, or a `README` that we did not write):
    that one is never written, in any mode. It is written by the
    `finalize` hook, after that of the roaster: the raw description
    (the name of the study) exists by then. The attributes of the
    roaster are kept (`functools.update_wrapper`, `tag`, `source`).
    """
    def producer(ctx):
        yield from roaster(ctx)

    def finalize(ctx):
        inner = getattr(roaster, "finalize", None)
        if inner is not None:
            yield from inner(ctx)
        layout = ctx.layout
        if layout.kind != "study" or \
                layout.readme_of_user(ctx.recorded) is not None:
            return
        name = layout.dataset_description("study")["Name"]
        yield WriteText(study_readme(name), layout.root / README,
                        tag="study")

    functools.update_wrapper(producer, roaster)
    for attr in ("tag", "source"):
        if hasattr(roaster, attr):
            setattr(producer, attr, getattr(roaster, attr))
    producer.finalize = finalize
    return producer


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
    return run_roast(with_study_readme(roaster), ctx, jobs=jobs,
                     display=display, info=info)


#: Keyword names of the selection arguments of every `roast()`
#: (`python_api`), by role: the one place to rename them (the signature,
#: the docstring and the parsing all read this table)
SELECTION_ARGS = {
    "keys": "keys",
    "exclude_keys": "exclude_keys",
    "subs": "subs",
    "exclude_subs": "exclude_subs",
}

#: Options of `roast_dataset` (and `make_context`) that every `roast()`
#: takes, in the docstrings that `python_api` writes
COMMON_OPTIONS = (
    "Those of the command line: `json` (`--sidecars`: \"yes\", \"no\", "
    "\"only\"), `ifexists` (`--if-exists`), `layout` (`--layout`), "
    "`sessions` (`--sessions`, for a dataset with sessions); `jobs` "
    "(processes for the shards), `display` (the progress table) and "
    "`info` (extra fields of the run, in the manifest)")


def doc_entry(name: str, text: str) -> str:
    """
    Write a numpydoc parameter entry (`python_api`).

    Parameters
    ----------
    name : str
        `name : type`
    text : str
        Its description (wrapped, indented by four spaces)

    Returns
    -------
    str
        The entry, without a final line break
    """
    return name + "\n" + textwrap.fill(
        " ".join(text.split()), 72, initial_indent="    ",
        subsequent_indent="    ")


def python_api(
    roasters: Roaster | Mapping[str, Roaster],
    aliases: KeyAliases,
    *,
    folder: str,
    templates: str | os.PathLike | None = None,
    width: int | None = None,
    example: int = 1,
    spec=None,
    keys_doc: str = "",
    options: Callable[..., dict | None] | None = None,
    options_doc: str = "",
    default_source: str | None = None,
    module: str | None = None,
) -> Callable[..., RoastReport]:
    """
    Build the `roast()` of a dataset: its Python API.

    Every ported dataset roasts through it, so that its arguments mean
    what they mean on the command line, in every dataset:

    * `subs`, `exclude_subs`: `sources.base.subject_labels` with the
      width of the dataset (numbers, labels with or without `sub-`,
      ranges `start:stop[:step]`, files that list one subject per line);
    * `keys`, `exclude_keys`: a name or a list (`as_list`); with a
      `spec`, through `spec.effective` as the command line does (keys
      that cover every choice count as none, `off` keys and add-ons);
    * the options of the dataset (`--discs`, `--parts`...): the keyword
      arguments of `options`, which returns `RoastContext.options` from
      them and from `--keys` as given (`options(keys, **extra)`);
    * with several roasters, `source` picks one (`--source`).

    ```python
    roast = python_api(roast_oasis1, KEYS, folder="OASIS-1",
                       templates=TEMPLATES, width=4,
                       options=disc_options, options_doc=DISCS_DOC)
    ```

    Parameters
    ----------
    roasters : roaster | dict[str, roaster]
        The top-level roaster, or one per source (`source=` picks it)
    aliases : KeyAliases
        Keys of the dataset
    folder : str
        Name of the dataset folder (in the docstring)
    templates : path | None
        Templates shipped with the dataset (default: the `TEMPLATES` of
        the module, looked up at each call, so that a test can patch it)
    width : int | None
        Digits of a numeric subject label (`Dataset.subject_width`;
        None: labels verbatim, no ranges)
    example : int
        A subject number, for the docstring
    spec : KeySpec | None
        Keys of the roast (duck-typed: `effective(keys, exclude)`); None:
        `keys` and `exclude_keys` as given
    keys_doc : str
        More about `keys`, for the docstring
    options : callable(keys, **extra) -> dict | None
        Options of the roasters (`RoastContext.options`): its keyword
        arguments after `keys` are those of `roast()`
    options_doc : str
        Their numpydoc entries, for the docstring
    default_source : str | None
        Default `source` (several roasters)
    module : str | None
        Module that the function belongs to (default: the caller's)

    Returns
    -------
    callable
        `roast(root, *, keys=(), exclude_keys=(), subs=(),
        exclude_subs=(), <options>, **options) -> RoastReport` (the
        names of the selection arguments: `SELECTION_ARGS`)
    """
    sources = roasters if isinstance(roasters, Mapping) else None
    extra = {}
    if options is not None:
        extra = {name: param for name, param in
                 list(inspect.signature(options).parameters.items())[1:]}

    names = dict(SELECTION_ARGS)

    def roast(root, **kwargs) -> RoastReport:
        # (keyword-only, by `SELECTION_ARGS`: see `__signature__`)
        keys, exclude_keys, subs, exclude_subs = (
            as_list(kwargs.pop(names[role], ())) for role in
            ("keys", "exclude_keys", "subs", "exclude_subs"))
        if sources is not None:
            source = kwargs.pop("source", default_source)
            if source not in sources:
                raise ValueError(f"Unknown source {source!r}: "
                                 f"{', '.join(sources)}")
            roaster = sources[source]
        else:
            roaster = roasters
        given = {name: kwargs.pop(name) for name in extra if name in kwargs}
        if spec is not None:
            select, exclude = spec.effective(keys, exclude_keys)
        else:
            select, exclude = keys, exclude_keys
        if options is not None:
            kwargs["options"] = options(keys, **given)
        return roast_dataset(
            roaster, root, aliases,
            templates=templates if templates is not None else getattr(
                sys.modules.get(module), "TEMPLATES", None),
            keys=select, exclude_keys=exclude,
            subs=sorted(subject_labels(subs, width)),
            exclude_subs=sorted(subject_labels(exclude_subs, width)),
            **kwargs,
        )

    keyword = inspect.Parameter.KEYWORD_ONLY
    params = [inspect.Parameter("root",
                                inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    if sources is not None:
        params.append(inspect.Parameter("source", keyword,
                                        default=default_source))
    params += [inspect.Parameter(name, keyword, default=())
               for name in names.values()]
    params += [param.replace(kind=keyword) for param in extra.values()]
    params.append(inspect.Parameter("options",
                                    inspect.Parameter.VAR_KEYWORD))
    roast.__signature__ = inspect.Signature(params,
                                            return_annotation=RoastReport)
    roast.__doc__ = _roast_doc(names, folder, width, example, sources,
                               keys_doc, options_doc)
    if module is None:
        module = sys._getframe(1).f_globals.get("__name__")
    roast.__module__ = module
    roast.__qualname__ = "roast"
    return roast


def _roast_doc(names: dict, folder: str, width: int | None, example: int,
               sources: dict | None, keys_doc: str, options_doc: str) -> str:
    """Return the docstring of a `roast()` (`python_api`)."""
    if width is None:
        subjects = ("Subjects: labels with or without `sub-`, files that "
                    "list one per line (as `--subs`)")
    else:
        subjects = (f"Subjects: numbers (`{example}` is "
                    f"`sub-{example:0{width}d}`), labels with or without "
                    f"`sub-`, ranges `start:stop[:step]`, files (as "
                    f"`--subs`)")
    entries = [doc_entry("root : path", "Dataset folder")]
    if sources is not None:
        choices = ", ".join(f'"{s}"' for s in sources)
        entries.append(doc_entry(f"source : {{{choices}}}",
                                 "Source whose files are roasted (as "
                                 "`--source`)"))
    entries += [
        doc_entry(f"{names['keys']}, {names['exclude_keys']} : "
                  f"str | list[str]",
                  "`--keys`, `--exclude-keys` (a name or a list)"
                  + (f"; {keys_doc}" if keys_doc else "")),
        doc_entry(f"{names['subs']}, {names['exclude_subs']} : "
                  f"int | str | list[int | str]", subjects),
    ]
    if options_doc:
        entries.append(options_doc.strip("\n"))
    entries.append(doc_entry("**options", COMMON_OPTIONS))
    return "\n".join([
        f"Roast `<root>` (the `{folder}` folder).", "",
        "Parameters", "----------", *entries, "",
        "Returns", "-------",
        doc_entry("RoastReport", "Counts per tag, errors, outputs and "
                                 "`status` (`run_roast`)"),
    ])
