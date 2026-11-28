"""
Build the command line of a dataset (`bx <dataset> <command>`).

`register(dataset)` returns a cyclopts `App` with one command per entry
of `Dataset.commands` that has a builder in `COMMANDS` (a plain dict:
later stages add theirs), then lets the dataset add or replace commands
(`Dataset.cli`).

Each command is a function whose `__signature__` is synthesised: the
dataset's positional `arguments`, `path` and `--out` (except `info`),
then one keyword-only parameter per option group (`Dataset.groups`
and `Dataset.options`), each a flattened dataclass
(`datasets.options`), shown in its help panels (`command_panels`: those
of the sources for credentials). Its body sets up the file log and calls
`Dataset.execute`.

User errors (`USER_ERRORS`) are printed without a traceback (exit
status 1); other exceptions keep theirs. Tracebacks always go to the
log files (`--log`), and are printed with `--level debug`.
"""
import dataclasses
import functools
import inspect
import logging
import sys
import traceback
import typing
from typing import Annotated, Callable

from cyclopts import App, Group, Parameter

from brainspresso.actions.locks import LockError
from brainspresso.bids.layout import LayoutError
from brainspresso.config import OUT, PATH, OptionInfo
from brainspresso.datasets import options as opt
from brainspresso.datasets.base import Dataset
from brainspresso.errors import UsageError
from brainspresso.sources.base import SourceUnavailable
from brainspresso.sources.credentials import CredentialsError
from brainspresso.utils import deps
from brainspresso.utils.log import setup_filelog

lg = logging.getLogger(__name__)

#: Exceptions printed without a traceback
USER_ERRORS = (UsageError, LayoutError, CredentialsError, SourceUnavailable,
               LockError)


def _log_traceback(error: BaseException) -> None:
    """Write the traceback of an error to the log files only."""
    record = logging.LogRecord(
        "brainspresso", logging.ERROR, __file__, 0, f"{error}", None,
        (type(error), error, error.__traceback__),
    )
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.FileHandler):
            handler.handle(record)


def user_errors(func: Callable) -> Callable:
    """
    Print user errors without a traceback, and exit with status 1.

    Parameters
    ----------
    func : callable
        A command

    Returns
    -------
    callable
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except USER_ERRORS as e:
            _log_traceback(e)
            if logging.getLogger("brainspresso").isEnabledFor(
                    logging.DEBUG):
                traceback.print_exc()
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            _log_traceback(e)
            raise
    return wrapper


def _docstring(dataset: Dataset, command: str) -> str:
    """Docstring of a command (its parameters that are not groups)."""
    lines = [dataset.describe(command), ""]
    keys = dataset.key_help(command) if command in ("harvest", "roast") \
        else ""
    if keys:
        lines += ["**Keys** (a group selects the keys below it):", "",
                  keys, ""]
    lines += ["Parameters", "----------"]
    for arg in dataset.arguments:
        lines += [arg.name, f"    {arg.help or arg.name}"]
    if command != "info":
        lines += [
            "path",
            f"    Path to the root of all datasets (which contains "
            f"`{dataset.folder}/`; default: $BDP_PATH, else the current "
            f"folder). Ignored with `--out`.",
            "out",
            "    Dataset folder (overrides `path`)",
        ]
    return "\n".join(lines) + "\n"


def command_groups(dataset: Dataset, command: str) -> dict[str, type]:
    """
    Option groups of a command, with the choices of the dataset.

    Parameters
    ----------
    dataset : Dataset
    command : str

    Returns
    -------
    dict[str, type]
        Parameter name -> group (`OptionGroup.for_dataset`)
    """
    groups = {name: opt.STANDARD[name] for name in dataset.groups(command)}
    groups.update(dataset.options(command))
    return {name: group.for_dataset(dataset, command)
            for name, group in groups.items()}


def command_panels(dataset: Dataset, command: str
                   ) -> dict[str, tuple[Group, ...]]:
    """
    Help panels of the option groups of a command.

    Parameters
    ----------
    dataset : Dataset
    command : str

    Returns
    -------
    dict[str, tuple[cyclopts.Group]]
        Parameter name -> panels (`datasets.options.group_panels`); a
        field with a panel of its own (`--folders`, `--jobs`...) stays in
        it
    """
    return {name: opt.group_panels(dataset, name, group)
            for name, group in command_groups(dataset, command).items()}


def command_names(dataset: Dataset) -> tuple[str, ...]:
    """
    Return the standard commands that `register` builds.

    Parameters
    ----------
    dataset : Dataset

    Returns
    -------
    tuple[str]
    """
    return tuple(c for c in dataset.commands if c in COMMANDS and (
        c != "roast" or dataset.has_roast()))


def _flag_names(hint) -> tuple[str, ...]:
    """Long names of a field (`Parameter(name=...)`), without `--`."""
    names = ()
    for meta in getattr(hint, "__metadata__", ()):
        if isinstance(meta, Parameter) and meta.name:
            names = (meta.name,) if isinstance(meta.name, str) \
                else tuple(meta.name)
    return tuple(n[2:] for n in names if n.startswith("--"))


def config_options(dataset: Dataset, command: str
                   ) -> dict[str, OptionInfo]:
    """
    Options of a command as config keys (`brainspresso.config`).

    Parameters
    ----------
    dataset : Dataset
    command : str

    Returns
    -------
    dict[str, OptionInfo]
        Key (the long flag without `--`) -> option; empty if `register`
        does not build the command
    """
    if command not in command_names(dataset):
        return {}
    out = {} if command == "info" else {"path": PATH, "out": OUT}
    for name, group in command_groups(dataset, command).items():
        hints = opt.field_hints(group)
        fields = {}
        for f in dataclasses.fields(group):
            hint = hints[f.name]
            base = hint.__origin__ if hasattr(hint, "__metadata__") \
                else hint
            fields[f.name] = (_flag_names(hint) or (
                f.name.replace("_", "-"),))[0]
            args = typing.get_args(base)
            is_list = typing.get_origin(base) in (tuple, list)
            fields[f.name] = OptionInfo(
                fields[f.name], f.name, name, f.default, is_list=is_list,
                is_bool=base is bool or bool in args,
                choices=_choices(args[0] if is_list and args else base),
            )
        # an alias and the fields it stands for are one option
        pairs = []
        for old, (new, instead) in group.deprecated.items():
            new = (new,) if isinstance(new, str) else tuple(new)
            fields[old] = dataclasses.replace(
                fields[old], deprecated=instead,
                instead=tuple(fields[n].key for n in new))
            members = {old, *new}
            for other in [p for p in pairs if p & members]:
                pairs.remove(other)
                members |= other
            pairs.append(members)
        for members in pairs:
            for name in members:
                fields[name] = dataclasses.replace(
                    fields[name], aliases=tuple(sorted(
                        fields[m].key for m in members if m != name)))
        out.update({o.key: o for o in fields.values()})
    return out


def _choices(hint) -> tuple[str, ...] | None:
    """Values of a `Literal` (in a union too), else None."""
    if typing.get_origin(hint) is typing.Literal:
        return tuple(str(v) for v in typing.get_args(hint))
    values = [_choices(a) for a in typing.get_args(hint)
              if typing.get_origin(a) is typing.Literal]
    return tuple(v for vs in values for v in vs) if values else None


def build_command(dataset: Dataset, command: str) -> Callable:
    """
    Build the function of a standard command (harvest, roast...).

    Parameters
    ----------
    dataset : Dataset
    command : str

    Returns
    -------
    callable
        A function with a synthesised `__signature__`, for cyclopts
    """
    P = inspect.Parameter
    # the arguments, `path` and `--out` are in the panel of `--layout`
    where = Parameter(group=opt.panel(*opt.LayoutOptions.panel))
    params = [P(a.name, P.POSITIONAL_OR_KEYWORD,
                annotation=Annotated[a.type, where])
              for a in dataset.arguments]
    if command != "info":
        params += [
            P("path", P.POSITIONAL_OR_KEYWORD, default=None,
              annotation=Annotated[str | None, where]),
            P("out", P.KEYWORD_ONLY, default=None,
              annotation=Annotated[str | None, where]),
        ]
    panels = command_panels(dataset, command)
    for name, group in command_groups(dataset, command).items():
        own = (opt.panel(*group.panel),)
        hint = group | None
        if panels[name] != own:
            # (`name="*"` again: cyclopts 3 does not merge it with that
            # of the class)
            hint = Annotated[hint, Parameter(name="*", group=panels[name])]
        params.append(P(name, P.KEYWORD_ONLY, default=None,
                        annotation=hint))
    signature = inspect.Signature(params)
    names = [a.name for a in dataset.arguments]

    @user_errors
    def run(*args, **kwargs):
        values = dict(signature.bind(*args, **kwargs).arguments)
        arguments = [values.pop(n) for n in names]
        path, out = values.pop("path", None), values.pop("out", None)
        groups = {k: v for k, v in values.items() if v is not None}
        log = groups.get("log") or opt.LogOptions()
        setup_filelog(log.log, level=log.level)
        # nothing is returned: cyclopts >= 4 prints what a command
        # returns, and the console script (`sys.exit(main())`) of
        # cyclopts 3 would print it and exit 1 (`execute` returns it)
        dataset.execute(command, *arguments, path=path, out=out, **groups)

    run.__name__ = run.__qualname__ = command.replace("-", "_")
    run.__signature__ = signature
    run.__doc__ = _docstring(dataset, command)
    del run.__wrapped__
    return run


def missing_command(dataset: Dataset, command: str,
                    missing: deps.Unavailable) -> Callable:
    """
    Build a command whose optional dependencies are missing.

    Parameters
    ----------
    dataset : Dataset
    command : str
    missing : utils.deps.Unavailable

    Returns
    -------
    callable
        A command that prints the hint and exits with status 1
    """
    def stub(*tokens: Annotated[str, Parameter(allow_leading_hyphen=True)]):
        print(f"Error: `bx {dataset.name} {command}` needs "
              f"{', '.join(missing.missing)}: {missing.hint}",
              file=sys.stderr)
        sys.exit(1)

    stub.__name__ = command.replace("-", "_")
    stub.__doc__ = (f"{dataset.describe(command)} (needs "
                    f"{missing.hint})\n")
    return stub


#: Builders of the standard commands: name -> builder(dataset, command)
COMMANDS: dict = {
    "harvest": build_command,
    "roast": build_command,
    "compost": build_command,
    "info": build_command,
}


def unavailable(dataset: Dataset, command: str) -> deps.Unavailable | None:
    """
    Missing optional dependencies of a command (recorded once).

    Parameters
    ----------
    dataset : Dataset
    command : str

    Returns
    -------
    utils.deps.Unavailable | None
    """
    modules = tuple(dataset.requires.get(command, ()))
    absent = deps.missing(*modules)
    if not absent:
        return None
    entry = deps.Unavailable(f"{dataset.name} {command}", tuple(absent),
                             dataset.extra)
    if entry not in deps.UNAVAILABLE:
        deps.UNAVAILABLE.append(entry)
    return entry


def register(dataset: Dataset | type) -> App:
    """
    Build the cyclopts app of a dataset.

    Parameters
    ----------
    dataset : Dataset | type[Dataset]
        A dataset (a class is instantiated)

    Returns
    -------
    App
        `bx <dataset.name>`, with its commands
    """
    if isinstance(dataset, type):
        dataset = dataset()
    app = App(name=dataset.name, help=dataset.help or dataset.title,
              help_format="markdown", group_commands="Commands")
    for command in dataset.commands:
        builder = COMMANDS.get(command)
        if builder is None:
            continue
        if command == "roast" and not dataset.has_roast():
            continue
        missing = unavailable(dataset, command)
        if missing is not None:
            app.command(missing_command(dataset, command, missing),
                        name=command)
            continue
        app.command(builder(dataset, command), name=command)
    dataset.cli(app)
    return app


def broken(name: str, summary: str, error: BaseException) -> App:
    """
    Build the app of a dataset that cannot be imported.

    Parameters
    ----------
    name : str
        Command name
    summary : str
        Its summary
    error : Exception
        Why it cannot be imported

    Returns
    -------
    App
        An app that prints the error and exits with status 1
    """
    message = f"{type(error).__name__}: {error}"
    app = App(name=name, help=f"{summary} (unavailable: {message})")

    @app.default
    def _unavailable(
        *tokens: Annotated[str, Parameter(allow_leading_hyphen=True)]
    ):
        print(f"Error: dataset {name} is unavailable [{message}]",
              file=sys.stderr)
        sys.exit(1)

    return app
