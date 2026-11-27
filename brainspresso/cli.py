"""
Command line: `bx` (or `brainspresso`, `python -m brainspresso`).

`main(tokens)` builds the root app for one invocation: the dataset
named by the first token is registered (`datasets.register`), every
other entry of the catalog is a placeholder that only carries its
summary, so `bx --help` imports no dataset and `bx ixi --help` shows the
full help of IXI. `app` (a module attribute, built on first access) is
the root app with every dataset registered, for tests and scripts.

The root app's config callable (`config.Loader`) fills the options that
the command line does not give from the config files and
`BRAINSPRESSO_OPT_*` variables; `bx --config FILE` (before the dataset,
repeatable) adds files. `bx config show|paths` shows them.
"""
import difflib
import json
import logging
import sys
from typing import Annotated

import cyclopts
from cyclopts import CycloptsError, MissingArgumentError, Parameter

from brainspresso import config
from brainspresso.config import ConfigError, Loader, Origin
from brainspresso.datasets.catalog import CATALOG, load
from brainspresso.utils.path import get_tree_path

HELP = """
Brainspresso : Harvest, Roast and Grind public neuroimaging datasets
====================================================================

* Plantation : A public neuroimaging dataset supported by brainspresso
* Harvest    : Download raw data
* Roast      : Convert source data to BIDS
* Grind      : Preprocess data with standard pipelines
* Compost    : Delete source data, or data outside a filter

Options of the dataset commands default to the values of config files
(`~/.brainspresso/config.toml`, `<root>/.brainspresso/config.toml`,
`<dataset>/.brainspresso/config.toml`, then `--config FILE`, given
before the dataset and repeatable) and of `BRAINSPRESSO_OPT_*`
variables: `bx config show`, `bx config paths`.
"""

Tokens = Annotated[str, Parameter(allow_leading_hyphen=True)]


def root_app(loader: Loader | None = None) -> cyclopts.App:
    """
    Build the root app, without datasets.

    Parameters
    ----------
    loader : config.Loader, optional
        Config callable (default: no `--config` file)

    Returns
    -------
    cyclopts.App
    """
    loader = loader if loader is not None else Loader()
    app = cyclopts.App(
        "brainspresso",
        help=HELP,
        help_format="markdown",
        group_commands="Plantations",
        group_parameters="Options",
        config=loader,
    )
    app._commands["--help"].group = "Help"
    app._commands["--version"].group = "Help"
    app.command(config_app(loader), name="config")
    return app


def dataset_app(name: str, loader: Loader | None = None) -> cyclopts.App:
    """
    Build the app of a catalog entry (a stub if it cannot be imported).

    Parameters
    ----------
    name : str
        Command name
    loader : config.Loader, optional
        Config callable of the root app (the dataset is recorded there)

    Returns
    -------
    cyclopts.App
    """
    from brainspresso.datasets.register import (  # noqa: PLC0415
        broken,  # lazy: startup time (`bx --help`)
        config_options,
        register,
    )
    try:
        dataset = load(name)
        app = register(dataset)
    except Exception as e:
        logging.getLogger(__name__).warning(
            f"Dataset {name} is unavailable [{type(e).__name__}: {e}]")
        return broken(name, CATALOG[name][1], e)
    if loader is not None:
        loader.datasets[name] = dataset
        loader.describe = config_options
    return app


def build_app(first: str | None = None, *, all: bool = False,
              loader: Loader | None = None) -> cyclopts.App:
    """
    Build the root app.

    Parameters
    ----------
    first : str, optional
        Dataset to register (the others are placeholders)
    all : bool
        Register every dataset
    loader : config.Loader, optional
        Config callable (default: no `--config` file)

    Returns
    -------
    cyclopts.App
    """
    loader = loader if loader is not None else Loader()
    app = root_app(loader)
    for name, (_, summary) in CATALOG.items():
        if all or name == first:
            app.command(dataset_app(name, loader), name=name)
        else:
            app.command(cyclopts.App(name=name, help=summary), name=name)
    return app


# ----------------------------------------------------------------------
#   `bx config`
# ----------------------------------------------------------------------

def _toml(value) -> str:
    """Format a value as TOML (`"x"`, `["a", "b"]`, `true`)."""
    if isinstance(value, (tuple, list, set, frozenset)):
        value = [getattr(v, "value", v) for v in value]
    return json.dumps(value, default=str)


#: Effective values of options whose default is None (to tell what the
#: command line gave)
EFFECTIVE = {"sidecars": "yes", "lock-wait": 0}


def _line(key: str, value, origin) -> str:
    """`key = value  # origin` (a comment if unset: TOML has no null)."""
    if value is None:
        value = EFFECTIVE.get(key)
    if value is None:
        return f"# {key}: not set  # {origin}"
    return f"{key} = {_toml(value)}  # {origin}"


def _registered(loader: Loader, name: str):
    """Build the app of a dataset for `bx config` (its dataset too)."""
    if name not in CATALOG:
        close = difflib.get_close_matches(name, CATALOG, n=1)
        raise ConfigError(f"Unknown dataset {name!r}" + (
            f" (did you mean {close[0]!r}?)" if close else ""))
    app = build_app(name, loader=loader)
    if name not in loader.datasets:
        raise ConfigError(f"Dataset {name} is unavailable")
    return app, loader.datasets[name]


def show_sections(loader: Loader) -> None:
    """
    Print the values of every section of every layer (later wins).

    Parameters
    ----------
    loader : config.Loader
    """
    root, _ = loader.locate((), "harvest")
    merged: dict = {}
    for layer in loader.layers(root):
        for entry in layer.entries:
            name = ".".join((*entry.parts, *(
                [entry.command] if entry.command else []))) or "defaults"
            table = merged.setdefault((entry.rank, name), {})
            for key in entry.values:
                table[key] = (config.plain_value(entry, key), Origin(
                    layer.kind, entry.source, entry.section))
    if not merged:
        print("No config values (`bx config paths` lists the files)")
    for (_, name), table in sorted(merged.items()):
        print(f"[{name}]")
        for key, (value, origin) in sorted(table.items()):
            print(_line(key, value, origin))
        print()


def show_options(loader: Loader, name: str, tokens=()) -> None:
    """
    Print the effective value and the origin of every option.

    Parameters
    ----------
    loader : config.Loader
    name : str
        Dataset (command name)
    tokens : list[str]
        A command (default: every command), then what follows it on the
        command line (arguments, options)
    """
    from brainspresso.datasets.options import (  # noqa: PLC0415
        lock_wait,  # lazy: startup time (`bx --help`)
    )
    from brainspresso.datasets.register import (  # noqa: PLC0415
        command_names,  # lazy: startup time (`bx --help`)
        config_options,
    )
    app, dataset = _registered(loader, name)
    tokens = list(tokens)
    commands = command_names(dataset)
    if tokens and tokens[0] in commands:
        commands, tokens = (tokens[0],), tokens[1:]
    elif tokens and tokens[0] in config.COMMANDS:
        raise ConfigError(f"{name} has no command {tokens[0]!r} "
                          f"({', '.join(commands)})")
    for command in commands:
        loader.report = None
        try:
            _, bound, _ = app.parse_args([name, command, *tokens],
                                         exit_on_error=False,
                                         print_error=False)
        except MissingArgumentError as e:
            missing = getattr(getattr(e.argument, "field_info", None),
                              "name", None)
            if missing in {a.name for a in dataset.arguments}:
                show_unbound(loader, dataset, command, tokens)
                continue
            app.parse_args([name, command, *tokens])    # prints, exits
            raise
        except CycloptsError:
            app.parse_args([name, command, *tokens])    # prints, exits
            raise
        report = loader.report or {}
        origins = report.get("origins", {})
        section = ".".join((*report.get("parts", (name,)), command))
        print(f"[{section}]")
        for key, option in config_options(dataset, command).items():
            if option.deprecated is not None:
                continue                # (hidden aliases)
            if option.group is None:
                value = bound.arguments.get(option.field, option.default)
            else:
                group = bound.arguments.get(option.group)
                value = getattr(group, option.field) if group is not None \
                    else option.default
                if group is not None and option.field == "sidecars":
                    value = group.sidecar_policy      # (with its aliases)
                elif group is not None and option.field == "lock_wait":
                    value = lock_wait(group)
            origin = origins.get(key, Origin("built-in"))
            print(_line(key, value, origin))
        print()


def _location(tokens) -> dict[str, str]:
    """`--path` and `--out` of command-line tokens (`--path=X` too)."""
    out, tokens = {}, list(tokens)
    for i, token in enumerate(tokens):
        for key in ("path", "out"):
            if token == f"--{key}" and i + 1 < len(tokens):
                out[key] = tokens[i + 1]
            elif token.startswith(f"--{key}="):
                out[key] = token.partition("=")[2]
    return out


def show_unbound(loader: Loader, dataset, command: str, tokens=()) -> None:
    """
    Print the options of a dataset whose arguments are not given.

    Only the built-in values and the sections that apply to every value
    of the arguments (`[openneuro.harvest]`, not
    `[openneuro.ds000224.harvest]`), and the dataset file of `--out`.
    Nothing is parsed but `--path` and `--out`.

    Parameters
    ----------
    loader : config.Loader
    dataset : Dataset
    command : str
    tokens : list[str]
        What follows the command (`--path`, `--out` are used)
    """
    from brainspresso.datasets.register import (  # noqa: PLC0415
        config_options,  # lazy: startup time (`bx --help`)
    )
    options = config_options(dataset, command)
    parts, own = config.dataset_parts(dataset)
    location = _location(tokens)
    root, folder = loader.locate(parts, command, **location)
    found = config.resolve(loader.layers(root, folder), parts, command,
                           options, own=own,
                           everywhere=lambda: loader.everywhere(dataset))
    names = " ".join(a.name.upper() for a in dataset.arguments)
    print(f"[{'.'.join((*parts, command))}]")
    print(f"# without {names}: `bx config show {dataset.name} {command} "
          f"{names}` also reads [{dataset.name}.<{names}>] sections and "
          f"the dataset file")
    for key, option in options.items():
        if option.deprecated is not None:
            continue                    # (hidden aliases)
        if key in location:
            value, origin = location[key], Origin("CLI")
        elif key in config.LOCATION and location:
            # (one location: the other is not read from config)
            value, origin = option.default, Origin("built-in")
        else:
            value, origin = found.get(key, (option.default,
                                            Origin("built-in")))
        print(_line(key, value, origin))
    print()


def show_paths(loader: Loader, name: str | None = None, arguments=(),
               path: str | None = None, out: str | None = None) -> None:
    """
    Print the files that are looked up, and whether they exist.

    Parameters
    ----------
    loader : config.Loader
    name : str, optional
        Dataset (command name): also its dataset file
    arguments : list[str]
        A command (default: harvest; the location is that of its
        sections), then the arguments of the dataset
    path, out : str, optional
        Root of all datasets, dataset folder
    """
    parts, folder, arguments = (), None, list(arguments)
    command = "harvest"
    if arguments and arguments[0] in config.COMMANDS:
        command = arguments.pop(0)
    if name is not None:
        _, dataset = _registered(loader, name)
        dataset = dataset.bind(**dict(zip(
            (a.name for a in dataset.arguments), arguments)))
        parts, _ = config.dataset_parts(dataset)
        if len(arguments) >= len(dataset.arguments):
            folder = dataset.folder_name
    try:
        root, folder = loader.locate(parts, command, path=path, out=out,
                                     folder=folder)
    except ConfigError as e:
        print(f"({e})")
        root = get_tree_path(path)
        folder = out or (root / folder if folder is not None else None)
    print(f"{'command':9} {command:8} (the sections that locate the "
          f"files)")
    rows = [("user", config.user_dir() / config.FILENAME),
            ("root", config.located_file(root))]
    if folder is not None:
        rows.append(("dataset", config.located_file(folder)))
    rows += [("--config", f) for f in loader.files]
    for kind, file in rows:
        state = "found" if file.is_file() else "missing"
        print(f"{kind:9} {state:8} {file}")
    for var in sorted(loader.environ):
        if var.startswith(config.ENV_PREFIX):
            print(f"{'env':9} {'set':8} ${var}")


def config_app(loader: Loader) -> cyclopts.App:
    """
    Build `bx config` (imports no dataset but the one it shows).

    Parameters
    ----------
    loader : config.Loader

    Returns
    -------
    cyclopts.App
    """
    app = cyclopts.App(
        name="config", group="Config", help_format="markdown",
        help="Show config values and the files they come from.",
    )

    @app.command
    def show(dataset: str | None = None, *tokens: Tokens):
        """
        Print the effective value of every option, and its origin.

        Without a dataset, print the sections of every file. With one,
        every option of its commands (or of COMMAND), with the arguments
        and options that follow, as on the command line: its value and
        where it comes from (built-in, a file and its section, an
        environment variable, or the command line).

        `bx config show openneuro harvest ds000224 --jobs 2`

        Parameters
        ----------
        dataset
            Dataset (its command name, e.g. ixi)
        tokens
            COMMAND, then its arguments and options
        """
        try:
            if dataset is None:
                show_sections(loader)
            else:
                show_options(loader, dataset, tokens)
        except ConfigError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    @app.command
    def paths(dataset: str | None = None, *arguments: str,
              path: str | None = None, out: str | None = None):
        """
        List the config files that are looked up, and which exist.

        `bx config paths openneuro compost ds000224 --path /data`

        Parameters
        ----------
        dataset
            Dataset (its command name): also its dataset file
        arguments
            COMMAND (default: harvest: its sections give a configured
            `path` or `out`), then the arguments of the dataset
            (OpenNeuro: the accession)
        path
            Root of all datasets (default: a configured path, else
            $BDP_PATH, else the current folder)
        out
            Dataset folder
        """
        try:
            show_paths(loader, dataset, arguments, path=path, out=out)
        except ConfigError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    return app


def global_options(tokens: list[str]) -> tuple[list[str], list[str]]:
    """
    Split `--config FILE` (before the dataset) from the tokens.

    Parameters
    ----------
    tokens : list[str]

    Returns
    -------
    files : list[str]
        Config files, in order
    tokens : list[str]
        The other tokens

    Raises
    ------
    ConfigError
        `--config` without a file
    """
    files, tokens = [], list(tokens)
    while tokens and (tokens[0] == "--config"
                      or tokens[0].startswith("--config=")):
        token = tokens.pop(0)
        if token.startswith("--config="):
            value = token.partition("=")[2]
        elif tokens:
            value = tokens.pop(0)
        else:
            value = ""
        if not value:
            raise ConfigError("--config needs a file")
        files.append(value)
    return files, tokens


def main(tokens=None):
    """
    Run the command line.

    Parameters
    ----------
    tokens : list[str], optional
        Arguments (default: `sys.argv[1:]`)

    Returns
    -------
    int | None
        Exit status (None: success)
    """
    tokens = list(sys.argv[1:] if tokens is None else tokens)
    try:
        files, tokens = global_options(tokens)
        loader = Loader(files)
        result = build_app(tokens[0] if tokens else None,
                           loader=loader)(tokens)
    except ConfigError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    # the console script calls `sys.exit(main())`: only an exit status
    return result if isinstance(result, int) and \
        not isinstance(result, bool) else None


_app = None


def __getattr__(name):
    """`app`: the root app with every dataset registered."""
    global _app
    if name == "app":
        if _app is None:
            _app = build_app(all=True)
        return _app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


if __name__ == "__main__":
    sys.exit(main())
