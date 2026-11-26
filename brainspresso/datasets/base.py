"""
Datasets: declarations, command bodies and the Python API.

A dataset is a `Dataset` subclass that declares what it is (name,
folder, keys, sources and their per-key defaults, layouts, sites) and
gets `bx <name> {harvest,roast,compost,info}` from
`datasets.register.register`, built from the option groups of
`datasets.options`. It overrides a command body, edits the option
groups of a command (`options`, `groups`), or adds raw cyclopts
commands (`cli`) when it needs more.

```python
class IXI(Dataset):
    name = "ixi"
    folder = "IXI"
    keys = KeySpec(choices=KEYS, default=KEYS)
    subject_width = 3
    credentials = "login"
    defaults = DEFAULTS

    def sources(self, args=None):
        return sources()
```

`Dataset.run(command, layout, args)` is the one entry point of the
command line and of the Python API: it takes the run lock of the
dataset (`actions.runlock`), then calls the command body.
`Dataset.execute(command, path=..., **groups)` also resolves the
dataset folder and its layout, as the command line does.

This module does not use `from __future__ import annotations` (the
`Args` dataclass is read by tools that resolve annotations).
"""
import copy
import dataclasses
import logging
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, ClassVar, Iterable, Literal, Mapping

from brainspresso.datasets import options as opt
from brainspresso.errors import DatasetNotFound, UsageError

lg = logging.getLogger(__name__)

EMPTY: Mapping = MappingProxyType({})

#: Run lock of each command: "shared", "exclusive" or None
LOCKS: Mapping[str, str | None] = MappingProxyType({
    "harvest": "shared", "roast": "shared", "compost": "exclusive",
    "info": None,
})


def _names(keys) -> tuple:
    """Key names of command-line values."""
    return tuple(str(getattr(k, "value", k)) for k in keys or ())


def tree_lines(tree, indent: int = 0) -> list[str]:
    """
    Lines of a key hierarchy (nested dicts and sets), for the help.

    Parameters
    ----------
    tree : dict | set | list | str
    indent : int

    Returns
    -------
    list[str]
    """
    if isinstance(tree, str):
        return [" " * indent + f"* {tree}"]
    if not isinstance(tree, Mapping):
        return [" " * indent + f"* {k}" for k in sorted(tree)]
    out = []
    for name, sub in tree.items():
        out.append(" " * indent + f"* {name}")
        if sub:
            out += tree_lines(sub, indent + 2)
    return out


@dataclasses.dataclass(frozen=True)
class KeySpec:
    """
    The keys of a command (`--keys`, `--exclude-keys`).

    Parameters
    ----------
    tree : KeyAliases | dict | callable | None
        Key hierarchy: a `bids.keys.KeyAliases` (roast), a `utils.keys`
        nested dict, or a function returning one (imported on use, e.g.
        a roaster module with optional dependencies)
    choices : tuple[str] | None
        Values of `--keys` (None: the keys of a nested-dict `tree`, else
        any text, checked against the hierarchy before the command runs)
    default : tuple[str]
        Default of `--keys` (empty: all)
    off : tuple[str]
        Keys only selected when asked for (`--keys` names them): with the
        default `--keys`, they are excluded (IXI roast: `trace`)
    """

    tree: Any = None
    choices: tuple | None = None
    default: tuple = ()
    off: tuple = ()

    def resolve(self) -> Any:
        """Return the key hierarchy (calls `tree` if it is a function)."""
        tree = self.tree
        if callable(tree) and not isinstance(tree, (dict, Mapping)) \
                and not hasattr(tree, "selection"):
            tree = tree()
        return tree

    def all_choices(self) -> tuple | None:
        """Return the values of `--keys` (None: any text)."""
        if self.choices is not None:
            return tuple(self.choices)
        if isinstance(self.tree, Mapping):
            from brainspresso.utils.keys import flatten_keys
            return tuple(sorted(flatten_keys(self.tree)))
        return None

    def static_tree(self) -> Any:
        """Return the hierarchy if it is known without importing anything."""
        tree = self.tree
        if callable(tree) and not isinstance(tree, Mapping) and \
                not hasattr(tree, "selection"):
            return None
        return getattr(tree, "tree", tree)

    def effective(self, keys: Iterable[str] = (),
                  exclude: Iterable[str] = ()) -> tuple[tuple, tuple]:
        """
        Keys to select and to exclude, from `--keys` and `--exclude-keys`.

        `--keys` that covers every choice, or the default `--keys` of a
        spec whose default is "all but `off`", counts as no `--keys`, so
        that `--exclude-keys` alone means "all but" (and, in a roast,
        keeps the files that match no key). With the default `--keys`,
        the `off` keys are excluded.

        Parameters
        ----------
        keys, exclude : list[str]

        Returns
        -------
        keys : tuple[str]
            Empty: all keys
        exclude : tuple[str]
        """
        keys, exclude = _names(keys), _names(exclude)
        choices = self.all_choices()
        if not keys or set(keys) == set(self.default):
            covers = choices is None or not self.default or (
                set(self.default) | set(self.off) >= set(choices))
            if covers:
                return (), exclude + tuple(
                    k for k in self.off if k not in exclude)
            return keys or tuple(self.default), exclude
        if choices is not None and set(keys) >= set(choices):
            return (), exclude
        return keys, exclude

    def check(self, keys: Iterable[str] = (),
              exclude: Iterable[str] = ()) -> None:
        """
        Refuse unknown keys.

        Parameters
        ----------
        keys, exclude : list[str]

        Raises
        ------
        UsageError
            If a key is not a key of the hierarchy
        """
        keys, exclude = _names(keys), _names(exclude)
        choices = self.all_choices()
        if choices is not None:
            unknown = [k for k in (*keys, *exclude) if k not in choices]
            if unknown:
                raise UsageError(f"Unknown keys {unknown}. Choose from: "
                                 f"{', '.join(choices)}")
            return
        tree = self.resolve()
        if hasattr(tree, "expand"):
            tree.expand(keys)
            tree.expand(exclude)


@dataclasses.dataclass(frozen=True)
class Argument:
    """
    A leading positional argument of every command of a dataset.

    Parameters
    ----------
    name : str
        Python name (e.g. "dataset")
    type : type
        Its type
    help : str
        Its help
    """

    name: str
    type: Any = str
    help: str = ""


@dataclasses.dataclass
class Args:
    """
    Parsed options of one command.

    Every standard group is always there (with its defaults when the
    command does not show it); dataset groups are in `extras`.

    Attributes
    ----------
    command : str
        The command ("harvest", "roast", "compost", "info"...)
    keys, select, sessions, sites, filters, hosted, source, login, token,
    harvest, roast, compost, layout, log : OptionGroup
        Standard groups (`datasets.options.STANDARD`)
    extras : dict[str, OptionGroup]
        Groups of the dataset (`Dataset.options`), by parameter name
    """

    command: str
    keys: opt.KeyOptions
    select: opt.SelectionOptions
    sessions: opt.SessionOptions
    sites: opt.SiteOptions
    filters: opt.FilterOptions
    hosted: opt.HostedOptions
    source: opt.SourceOptions
    login: opt.LoginOptions
    token: opt.TokenOptions
    harvest: opt.HarvestOptions
    roast: opt.RoastOptions
    compost: opt.CompostOptions
    layout: opt.LayoutOptions
    log: opt.LogOptions
    extras: dict = dataclasses.field(default_factory=dict)

    @classmethod
    def of(cls, dataset: "Dataset", command: str, **groups) -> "Args":
        """
        Options of a command, with defaults for the groups not given.

        Parameters
        ----------
        dataset : Dataset
        command : str
        **groups : OptionGroup
            Standard groups (by field name) and dataset groups (by
            parameter name of `Dataset.options`)

        Returns
        -------
        Args
        """
        fields = {}
        for name, group in opt.STANDARD.items():
            value = groups.pop(name, None)
            fields[name] = value if value is not None else \
                group.for_dataset(dataset, command)()
        extras = {}
        for name, group in dataset.options(command).items():
            value = groups.pop(name, None)
            extras[name] = value if value is not None else \
                group.for_dataset(dataset, command)()
        if groups:
            raise TypeError(f"Unknown option groups of {command}: "
                            f"{sorted(groups)}")
        return cls(command=command, extras=extras, **fields)

    def groups(self) -> list:
        """Every group (standard ones, then the dataset's)."""
        return [getattr(self, n) for n in opt.STANDARD] + list(
            self.extras.values())

    def extra(self, name: str, field: str | None = None, default=None):
        """
        Return a dataset group (or one of its fields).

        Parameters
        ----------
        name : str
            Parameter name of the group
        field : str, optional
            One of its fields
        default
            Returned if the group (or field) does not exist

        Returns
        -------
        OptionGroup | Any
        """
        group = self.extras.get(name)
        if group is None or field is None:
            return group if group is not None else default
        return getattr(group, field, default)

    @property
    def dry_run(self) -> bool:
        """Whether the command only prints what it would do."""
        if self.command == "harvest":
            return self.harvest.dry_run
        if self.command == "compost":
            return self.compost.dry_run
        return any(getattr(g, "dry_run", False) is True
                   for g in self.extras.values())

    @property
    def wait(self) -> float:
        """Seconds to wait for the run lock."""
        group = {"harvest": self.harvest, "roast": self.roast,
                 "compost": self.compost}.get(self.command)
        return float(getattr(group, "wait", 0) or 0)


class Dataset:
    """
    A dataset of the command line.

    Class attributes are declarations (see the module docstring and
    `docs/design/architecture.md`, §6); methods are the command bodies
    and their hooks.

    Attributes
    ----------
    name : str
        CLI command ("ixi", "nki-rs")
    folder : str
        Dataset folder under the root of all datasets; may use the
        `arguments` ("OpenNeuro/{dataset}")
    title, help : str
        Title, and markdown help (its first line is the catalog summary)
    keys : KeySpec | None
        Keys of the commands (see `keys_for`); None: no `--keys`
    subject_width : int | None
        None: subject labels verbatim (strip `sub-`); an int: numbers
        formatted with this many digits, ranges `start:stop`
    sessions : bool
        Whether harvest and roast take `--sessions`
    site_names : tuple[str]
        Sites (`--sites` is checked against them, case-insensitive)
    has_sites : bool
        `--sites` without a list of names (checked by the sources)
    layouts : tuple[str]
        `--layout` values (`ALL_LAYOUTS` for one dataset per site)
    template_dir : Path | None
        Templates shipped with the dataset (roast)
    defaults : dict[str, list[str]]
        Per-key default sources (empty: the first source, alone)
    roaster : callable | None
        Roaster when no source has one (a `staticmethod`)
    credentials : {"login", "token"} | None
        Credential options of harvest
    harvest_jobs : int | None
        Default of `harvest --jobs`
    download : dict
        Fixed `DownloadOptions` of harvest (`on_error`, `path`)
    option_defaults : dict[str, dict[str, Any]]
        Other defaults: `{command: {field: default}}`
    folder_choices : tuple[str]
        Values of `--folders` (hosted harvests)
    hosted : bool
        Hosted as BIDS: harvest takes the BIDS filters
    harvest_subjects : bool
        Whether harvest takes `--subs` (its sources can prune subjects)
    legacy_subjects : bool
        Whether roast takes subject numbers (a legacy bidsifier, see
        `numbers`)
    requires : dict[str, tuple[str]]
        Optional modules of each command
    extra : str | None
        pip extra that installs them
    commands : tuple[str]
        Commands built by `register`
    arguments : tuple[Argument]
        Leading positional arguments of every command
    """

    name: ClassVar[str] = ""
    folder: ClassVar[str] = ""
    title: ClassVar[str] = ""
    help: ClassVar[str] = ""
    keys: ClassVar[KeySpec | None] = None
    subject_width: ClassVar[int | None] = None
    sessions: ClassVar[bool] = False
    site_names: ClassVar[tuple] = ()
    has_sites: ClassVar[bool] = False
    layouts: ClassVar[tuple] = ("study", "flat")
    template_dir: ClassVar[Path | None] = None
    defaults: ClassVar[Mapping[str, tuple]] = EMPTY
    roaster: ClassVar[Callable | None] = None
    credentials: ClassVar[Literal["login", "token"] | None] = None
    harvest_jobs: ClassVar[int | None] = None
    download: ClassVar[Mapping[str, Any]] = EMPTY
    option_defaults: ClassVar[Mapping[str, Mapping[str, Any]]] = EMPTY
    folder_choices: ClassVar[tuple] = ()
    hosted: ClassVar[bool] = False
    harvest_subjects: ClassVar[bool] = True
    legacy_subjects: ClassVar[bool] = False
    requires: ClassVar[Mapping[str, tuple]] = EMPTY
    extra: ClassVar[str | None] = None
    commands: ClassVar[tuple] = ("harvest", "roast", "compost", "info")
    arguments: ClassVar[tuple] = ()

    def __init__(self, **arguments):
        self.bound = dict(arguments)

    def __repr__(self) -> str:
        """`<Dataset ixi>`."""
        bound = "".join(f" {k}={v!r}" for k, v in self.bound.items())
        return f"<{type(self).__name__} {self.name}{bound}>"

    # ------------------------------------------------------------------
    #   Declarations
    # ------------------------------------------------------------------
    @property
    def summary(self) -> str:
        """First line of the help."""
        lines = [x for x in (self.help or self.title).strip().splitlines()
                 if x.strip()]
        return lines[0].strip() if lines else self.name

    @property
    def multisite(self) -> bool:
        """Whether `--sites` exists."""
        return bool(self.site_names) or self.has_sites

    def bind(self, **arguments) -> "Dataset":
        """
        Return a copy for given `arguments` (OpenNeuro: one accession).

        Parameters
        ----------
        **arguments
            Values of `arguments`

        Returns
        -------
        Dataset
        """
        out = copy.copy(self)
        out.bound = {**self.bound, **arguments}
        return out

    def sources(self, args: Args | None = None) -> dict:
        """
        Return the sources (fresh instances, not configured).

        No network access and no optional import here: it runs when
        the commands are built (choices of `--source`).

        Parameters
        ----------
        args : Args, optional
            Options of the command (e.g. ADNI's `--urls`)

        Returns
        -------
        dict[str, Source]
        """
        return {}

    def configured_sources(self, args: Args | None = None) -> dict:
        """
        Return the sources, configured with the options of a command.

        Parameters
        ----------
        args : Args, optional

        Returns
        -------
        dict[str, Source]
            `Source.configure(**self.source_options(args))` of each
        """
        options = self.source_options(args) if args is not None else {}
        return {name: src.configure(**options)
                for name, src in self.sources(args).items()}

    def source_names(self) -> list[str]:
        """Names of the sources (choices of `--source`)."""
        return list(self.sources())

    def source_options(self, args: Args) -> dict[str, Any]:
        """
        Per-run options of the sources: those of every group.

        Parameters
        ----------
        args : Args

        Returns
        -------
        dict
        """
        out = {}
        for group in args.groups():
            out.update(group.source_options(self))
        return out

    def keys_for(self, command: str) -> KeySpec | None:
        """
        Keys of a command (default: `keys`).

        Parameters
        ----------
        command : str

        Returns
        -------
        KeySpec | None
        """
        return self.keys

    def select_sites(self, values: Iterable[str]) -> tuple:
        """
        Check `--sites` values.

        Parameters
        ----------
        values : list[str]

        Returns
        -------
        tuple[str]
            Site names (case of `site_names`)

        Raises
        ------
        UsageError
            If a name is not a site
        """
        values = [str(v) for v in values or ()]
        if not self.site_names:
            return tuple(values)
        known = {s.lower(): s for s in self.site_names}
        unknown = [v for v in values if v.lower() not in known]
        if unknown:
            raise UsageError(f"Unknown sites {unknown}. Available: "
                             f"{', '.join(self.site_names)}")
        return tuple(known[v.lower()] for v in values)

    def chosen_sites(self, args: Args) -> tuple:
        """
        Sites of `--sites` minus those of `--exclude-sites`.

        Parameters
        ----------
        args : Args

        Returns
        -------
        tuple[str]
            Site names (empty: all)

        Raises
        ------
        UsageError
            If every site is excluded
        """
        sites = self.select_sites(args.sites.sites)
        exclude = set(self.select_sites(args.sites.exclude_sites))
        if not exclude:
            return sites
        pool = sites or self.site_names
        if not pool:            # (the sources apply `exclude_sites`)
            return sites
        sites = tuple(s for s in pool if s not in exclude)
        if not sites:
            raise UsageError("Every site is excluded")
        return sites

    def groups(self, command: str) -> tuple:
        """
        Return the standard groups of a command (`Args` field names).

        Parameters
        ----------
        command : str

        Returns
        -------
        tuple[str]
        """
        creds = {"login": ("login",), "token": ("token",)}.get(
            self.credentials, ())
        sites = ("sites",) if self.multisite else ()
        sessions = ("sessions",) if self.sessions else ()
        keys = ("keys",) if self.keys_for(command) is not None else ()
        source = ("source",) if self.source_names() else ()
        if command == "harvest":
            hosted = ("filters", "hosted") if self.hosted else ()
            select = ("select",) if self.harvest_subjects else ()
            return (*keys, *select, *sessions, *sites, *hosted, *source,
                    *creds, "harvest", "layout", "log")
        if command == "roast":
            roasters = {id(s.roaster) for s in self.sources().values()
                        if getattr(s, "roaster", None) is not None}
            source = ("source",) if len(roasters) > 1 else ()
            return (*keys, "select", *sessions, *source, "roast",
                    "layout", "log")
        if command == "compost":
            sites = ("sites",) if "sites" in self.layouts else ()
            return ("select", "sessions", *sites, "filters", "compost",
                    "layout", "log")
        if command == "info":
            return ("log",)
        return ("log",)

    def options(self, command: str) -> dict:
        """
        Dataset groups of a command (`Args.extras`).

        Parameters
        ----------
        command : str

        Returns
        -------
        dict[str, type[OptionGroup]]
            Parameter name -> group (fields must not reuse a standard
            name)
        """
        return {}

    def describe(self, command: str) -> str:
        """
        Help of a command (its summary; options document themselves).

        Parameters
        ----------
        command : str

        Returns
        -------
        str
        """
        title = self.title or self.summary
        return {
            "harvest": f"Download {title}.",
            "roast": f"Convert the source data of {title} to BIDS.",
            "compost": "Delete source data, or BIDS data outside a filter.",
            "info": f"Show what {title} provides.",
        }.get(command, f"{command} {title}.")

    def key_help(self, command: str, resolve: bool = False) -> str:
        """
        Return the key hierarchy of a command, for its help.

        Parameters
        ----------
        command : str
        resolve : bool
            Also resolve hierarchies that need an import (`info`)

        Returns
        -------
        str
            A markdown list ("" without a hierarchy)
        """
        spec = self.keys_for(command)
        if spec is None:
            return ""
        tree = spec.static_tree()
        if tree is None and resolve:
            try:
                tree = getattr(spec.resolve(), "tree", None)
            except ImportError:
                tree = None
        if not isinstance(tree, Mapping) or not tree:
            return ""
        return "\n".join(tree_lines(tree))

    def legacy_roast(self) -> bool:
        """Whether `roast` is overridden (not the roast engine)."""
        return type(self).roast is not Dataset.roast

    def has_roast(self) -> bool:
        """Whether the dataset can be roasted."""
        if type(self).roast is not Dataset.roast:
            return True
        return self.roaster is not None or any(
            getattr(s, "roaster", None) is not None
            for s in self.sources().values()
        )

    # ------------------------------------------------------------------
    #   Values of the command line
    # ------------------------------------------------------------------
    @property
    def folder_name(self) -> str:
        """`folder`, with the bound arguments."""
        return self.folder.format(**self.bound)

    def root(self, path=None, out=None) -> Path:
        """
        Dataset folder.

        Parameters
        ----------
        path : path, optional
            Root of all datasets (default: `$BDP_PATH`, else ".")
        out : path, optional
            The dataset folder itself (overrides `path`)

        Returns
        -------
        Path
        """
        from brainspresso.utils.path import get_tree_path
        if out:
            return Path(out)
        return Path(get_tree_path(path)) / self.folder_name

    def layout(self, root, layout: str | None = None):
        """
        Layout of the dataset (`--layout`).

        Parameters
        ----------
        root : path | Layout
            Dataset folder
        layout : str, optional
            `--layout` value (`sites:<kind>` included)

        Returns
        -------
        Layout
        """
        from brainspresso.bids.layout import parse_layout, resolve_layout
        if layout is not None and layout not in self.layouts:
            raise UsageError(f"Layout {layout!r} is not available for "
                             f"{self.name}: {', '.join(self.layouts)}")
        kind, child = parse_layout(layout)
        kwargs = {"child_kind": child} if kind == "sites" and child else {}
        return resolve_layout(root, kind, **kwargs)

    def labels(self, values: Iterable[str]) -> set:
        """
        Subject labels of `--subs` values (`subject_width`).

        Parameters
        ----------
        values : list[str]

        Returns
        -------
        set[str]
        """
        from brainspresso.sources.base import subject_labels
        return subject_labels(values, self.subject_width)

    def selection(self, subs=(), exclude_subs=(), sessions=(), **filters):
        """
        Build the selection of subjects, sessions and BIDS filters.

        Parameters
        ----------
        subs, exclude_subs : list[str]
            `--subs`, `--exclude-subs` (see `labels`)
        sessions : list[str]
            `--sessions` (with or without `ses-`)
        **filters
            Other `Selection` filters

        Returns
        -------
        Selection | None
            None if nothing is filtered
        """
        from brainspresso.bids.select import Selection
        subs, exclude_subs = self.labels(subs), self.labels(exclude_subs)
        sessions = {s[4:] if str(s).startswith("ses-") else str(s)
                    for s in sessions or ()}
        filters = {k: v for k, v in filters.items() if v}
        if not (subs or exclude_subs or sessions or filters):
            return None
        return Selection(subs=subs, exclude_subs=exclude_subs,
                         sessions=sessions, **filters)

    def wants(self, command: str, keys=(), exclude_keys=()):
        """
        Wanted keys of a harvest.

        Parameters
        ----------
        command : str
        keys, exclude_keys : list[str]

        Returns
        -------
        Wants
        """
        from brainspresso.sources.base import Wants
        spec = self.keys_for(command) or KeySpec()
        keys, exclude = spec.effective(keys, exclude_keys)
        return Wants(keys or None, spec.resolve(), exclude=exclude)

    def legacy_keys(self, command: str, args: Args) -> set:
        """
        Keys of a legacy bidsifier: `--keys` (else all) minus excluded.

        Parameters
        ----------
        command : str
        args : Args

        Returns
        -------
        set[str]
        """
        spec = self.keys_for(command) or KeySpec()
        keys, exclude = spec.effective(args.keys.keys, args.keys.exclude_keys)
        keys = set(keys or spec.all_choices() or ())
        return keys - set(exclude)

    def numbers(self, values: Iterable[str]) -> list:
        """
        Subject numbers of a legacy bidsifier (`--subs 1 002 3:5 subs.txt`).

        Values are expanded as for the other commands (`labels`: files
        and `start:stop` ranges), then converted to numbers.

        Parameters
        ----------
        values : list[str]

        Returns
        -------
        list[int]

        Raises
        ------
        UsageError
            If a value is not a number
        """
        from brainspresso.sources.base import subject_labels
        out = []
        for value in sorted(subject_labels(values, self.subject_width or 0)):
            if not value.isdigit():
                raise UsageError(f"Subjects are numbers here: {value!r}")
            out.append(int(value))
        return sorted(out)

    def select_roaster(self, ctx, override=None) -> tuple:
        """
        Choose the roaster of `sourcedata/`.

        Parameters
        ----------
        ctx : RoastContext
        override : list[str], optional
            `roast --source`: the source whose data are roasted

        Returns
        -------
        source : Source | None
        roaster : callable
        """
        from brainspresso.sources.base import select_roaster
        sources = self.sources()
        if override:
            name = str(override[0])
            if name not in sources:
                raise UsageError(f"Unknown source {name!r}. Available: "
                                 f"{', '.join(sources)}")
            source = sources[name]
            roaster = source.roaster or self.roaster
            if roaster is None:
                raise UsageError(f"{name}: no roaster for this source")
            return source, roaster
        source = select_roaster(sources.values(), ctx)
        if source is not None:
            return source, source.roaster
        if self.roaster is not None:
            return None, self.roaster
        raise UsageError(f"{self.name} has no roaster")

    # ------------------------------------------------------------------
    #   Running
    # ------------------------------------------------------------------
    def execute(self, command: str, *arguments, path=None, out=None,
                **groups):
        """
        Run a command as the command line does.

        Parameters
        ----------
        command : str
        *arguments
            Values of `arguments`
        path, out : path, optional
            Root of all datasets, or the dataset folder
        **groups : OptionGroup
            Option groups (see `Args.of`)

        Returns
        -------
        Any
            What the command body returns
        """
        names = [a.name for a in self.arguments]
        if len(arguments) > len(names):
            raise TypeError(f"{self.name}: too many arguments")
        bound = self.bind(**dict(zip(names, arguments)))
        args = Args.of(bound, command, **groups)
        if LOCKS.get(command, "shared") is None and command == "info":
            return bound.run(command, None, args)
        layout = bound.layout(bound.root(path, out),
                              args.layout.value(bound))
        return bound.run(command, layout, args)

    def check(self, command: str, args: Args) -> None:
        """
        Check the options of a command before anything is written.

        Keys (against the key hierarchy), sites and subjects.

        Parameters
        ----------
        command : str
        args : Args

        Raises
        ------
        UsageError
            If an option value cannot be used
        """
        spec = self.keys_for(command)
        if spec is not None and command in ("harvest", "roast"):
            spec.check(args.keys.keys, args.keys.exclude_keys)
        self.chosen_sites(args)
        subjects = self.numbers if command == "roast" and \
            self.legacy_subjects else self.labels
        subjects(args.select.subs)
        subjects(args.select.exclude_subs)

    @staticmethod
    def _parent_states(layout) -> list:
        """State of the parent of a site dataset (`--out <DS>/<site>`)."""
        from brainspresso.bids.layout import STATE_DIR, Layout, SitesLayout
        if isinstance(layout, SitesLayout):
            return []
        root = Path(layout.root)
        parent = root.parent
        if not (parent / STATE_DIR).is_dir():
            return []
        found = Layout.detect(parent)
        if isinstance(found, SitesLayout) and root.name in found.children:
            return [found.state]
        return []

    def run(self, command: str, layout=None, args: Args | None = None):
        """
        Run a command body under the run lock of the dataset.

        Not meant to be overridden: the lock and its order (state
        folder, lock, then the body, which records the run) are the
        same for every dataset. `--dry-run` and `info` take no lock.

        Parameters
        ----------
        command : str
        layout : Layout | None
            Layout of the dataset (None for `info`)
        args : Args, optional
            Options (default: those of `Args.of`)

        Returns
        -------
        Any
            What the command body returns
        """
        from brainspresso.actions.runlock import run_lock
        args = args if args is not None else Args.of(self, command)
        self.check(command, args)
        body = getattr(self, command.replace("-", "_"))
        mode = LOCKS.get(command, "shared")
        if layout is None or mode is None or args.dry_run:
            return body(layout, args)
        if command in ("compost", "roast") and \
                not Path(layout.root).is_dir():
            raise DatasetNotFound(f"Dataset folder not found: {layout.root}"
                                  + (" (harvest it first)"
                                     if command == "roast" else ""))
        layout.ensure_state()
        states = [layout.state] + [
            d.state for d in layout.datasets()
            if d is not layout and Path(d.state).is_dir()
        ] + self._parent_states(layout)
        with run_lock(states, command, exclusive=mode == "exclusive",
                      wait=args.wait, force=args.compost.ignore_runs):
            return body(layout, args)

    # ------------------------------------------------------------------
    #   Command bodies
    # ------------------------------------------------------------------
    def harvest(self, layout, args: Args):
        """
        Download source data (`harvest_sources` over `sources`).

        Parameters
        ----------
        layout : Layout
        args : Args

        Returns
        -------
        dict[str, str]
            Source used for each key
        """
        from brainspresso.bids.harvest import record_run
        from brainspresso.sources.base import harvest_sources
        options = args.harvest.runtime(**self.download)
        sources = self.configured_sources(args)
        selection = self.selection(args.select.subs, args.select.exclude_subs,
                                   args.sessions.sessions)
        wants = self.wants("harvest", args.keys.keys, args.keys.exclude_keys)
        override = list(args.source.source) or None
        if not options.dry_run:
            record_run(layout, "harvest", {"dataset": Path(layout.root).name})
            Path(layout.source).mkdir(parents=True, exist_ok=True)
        if self.defaults:
            return harvest_sources(sources, self.defaults, layout,
                                   selection=selection, keys=wants,
                                   options=options, override=override)
        if not sources:
            raise UsageError(f"{self.name} has no source")
        if override:
            unknown = [s for s in override if s not in sources]
            if unknown:
                raise UsageError(f"Unknown sources {unknown}. Available: "
                                 f"{', '.join(sources)}")
        name = override[0] if override else next(iter(sources))
        sources[name].harvest(layout, selection, wants, options)
        return {"*": name}

    def roast(self, layout, args: Args):
        """
        Convert source data to BIDS (the roaster of a source).

        Parameters
        ----------
        layout : Layout
        args : Args

        Returns
        -------
        RoastReport
        """
        from brainspresso.bids.layout import SitesLayout
        from brainspresso.roast import make_context, run_roast
        if isinstance(layout, SitesLayout):
            raise UsageError(f"{self.name}: roast of a dataset with one "
                             f"dataset per site is not supported")
        spec = self.keys_for("roast") or KeySpec()
        filters = {}
        if self.sessions:
            filters["sessions"] = sorted(
                s[4:] if s.startswith("ses-") else s
                for s in args.sessions.sessions)
        keys, exclude = spec.effective(args.keys.keys, args.keys.exclude_keys)
        ctx = make_context(
            layout, spec.resolve(), select_keys=keys, exclude_keys=exclude,
            json=args.roast.json_policy, ifexists=args.roast.if_exists,
            templates=self.template_dir,
            subs=sorted(self.labels(args.select.subs)),
            exclude_subs=sorted(self.labels(args.select.exclude_subs)),
            **filters,
        )
        source, roaster = self.select_roaster(ctx, args.source.source)
        if source is not None:
            ctx.source = Path(source.destination(layout))
        return run_roast(roaster, ctx, jobs=args.roast.jobs)

    def compost(self, layout, args: Args):
        """
        Delete source data, or BIDS data outside a filter.

        Parameters
        ----------
        layout : Layout
        args : Args

        Returns
        -------
        CompostPlan | list[CompostPlan]
        """
        from brainspresso.datasets.compost import run_compost
        c = args.compost
        return run_compost(
            layout, sourcedata=c.sourcedata, nested=c.nested, keep=c.keep,
            subs=sorted(self.labels(args.select.subs)),
            exclude_subs=sorted(self.labels(args.select.exclude_subs)),
            sessions=args.sessions.sessions,
            where=args.filters.where, derivatives=c.derivatives,
            trim=c.trim, force=c.force, yes=c.yes, dry_run=c.dry_run,
            list=c.list, sites=self.select_sites(args.sites.sites),
            exclude_sites=self.select_sites(args.sites.exclude_sites),
            **args.filters.filters(),
        )

    def info(self, layout=None, args: Args | None = None):
        """
        Print what the dataset provides (keys, sources, defaults).

        Parameters
        ----------
        layout : Layout | None
            Unused (the dataset may be absent)
        args : Args, optional
        """
        print(self.title or self.summary)
        for command in ("harvest", "roast"):
            spec = self.keys_for(command)
            if spec is None or (command == "roast" and not self.has_roast()):
                continue
            tree = self.key_help(command, resolve=True)
            if tree:
                print(f"  {command} keys:")
                print("\n".join("    " + line for line in tree.splitlines()))
            elif spec.all_choices():
                print(f"  {command} keys: {', '.join(spec.all_choices())}")
        sources = self.sources()
        for name, src in sources.items():
            provides = ", ".join(sorted(getattr(src, "provides", ()) or ()))
            print(f"  source {name}: {provides or '(any key)'}")
        for key, names in self.defaults.items():
            print(f"  default for {key}: {' then '.join(names)}")
        if self.site_names:
            print(f"  sites ({len(self.site_names)}): "
                  f"{', '.join(self.site_names)}")

    def cli(self, app) -> None:
        """
        Add or replace commands of the dataset app (after the defaults).

        Parameters
        ----------
        app : cyclopts.App
        """
