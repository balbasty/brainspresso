"""
Command line: `bx` (or `brainspresso`, `python -m brainspresso`).

`main(tokens)` builds the root app for one invocation: the dataset
named by the first token is registered (`datasets.register`), every
other entry of the catalog is a placeholder that only carries its
summary, so `bx --help` imports no dataset and `bx ixi --help` shows the
full help of IXI. `app` (a module attribute, built on first access) is
the root app with every dataset registered, for tests and scripts.
"""
import sys

import cyclopts

HELP = """
Brainspresso : Harvest, Roast and Grind public neuroimaging datasets
====================================================================

* Plantation : A public neuroimaging dataset supported by brainspresso
* Harvest    : Download raw data
* Roast      : Bidsify raw data
* Grind      : Preprocess data with standard pipelines
* Compost    : Delete source data, or data outside a filter
"""


def root_app() -> cyclopts.App:
    """Build the root app, without datasets."""
    app = cyclopts.App(
        "brainspresso",
        help=HELP,
        help_format="markdown",
        group_commands="Plantations",
        group_parameters="Options",
    )
    app._commands["--help"].group = "Help"
    app._commands["--version"].group = "Help"
    return app


def dataset_app(name: str) -> cyclopts.App:
    """
    Build the app of a catalog entry (a stub if it cannot be imported).

    Parameters
    ----------
    name : str
        Command name

    Returns
    -------
    cyclopts.App
    """
    import logging
    from brainspresso.datasets.catalog import CATALOG, load
    from brainspresso.datasets.register import broken, register
    try:
        return register(load(name))
    except Exception as e:
        logging.getLogger(__name__).warning(
            f"Dataset {name} is unavailable [{type(e).__name__}: {e}]")
        return broken(name, CATALOG[name][1], e)


def build_app(first: str | None = None, *, all: bool = False
              ) -> cyclopts.App:
    """
    Build the root app.

    Parameters
    ----------
    first : str, optional
        Dataset to register (the others are placeholders)
    all : bool
        Register every dataset

    Returns
    -------
    cyclopts.App
    """
    from brainspresso.datasets.catalog import CATALOG
    app = root_app()
    for name, (_, summary) in CATALOG.items():
        if all or name == first:
            app.command(dataset_app(name), name=name)
        else:
            app.command(cyclopts.App(name=name, help=summary), name=name)
    return app


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
    result = build_app(tokens[0] if tokens else None)(tokens)
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
