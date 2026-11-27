"""
Option groups of the dataset commands.

Each group is a frozen dataclass whose fields become flags of the
command line (`@Parameter(name="*")`: the fields are flattened, under a
help panel), with their help in `Parameter(help=...)`. A dataset adds
its own groups (`Dataset.options`), subclasses a group to add fields,
and gets per-dataset choices and defaults through
`OptionGroup.for_dataset` (a subclass built with
`dataclasses.make_dataclass`: a closure over a local `Literal` would not
resolve under `from __future__ import annotations`).

This module does not use `from __future__ import annotations`: cyclopts
reads the annotations of the fields.
"""
import dataclasses
import functools
import logging
from typing import Annotated, Any, ClassVar, Literal, Mapping

from cyclopts import Group, Parameter
from humanize import naturalsize

from brainspresso.actions.action import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.errors import UsageError
from brainspresso.sources.base import DownloadOptions
from brainspresso.utils.ui import human2bytes

lg = logging.getLogger(__name__)

Many = Parameter(consume_multiple=True)
Strings = tuple[str, ...]


def deprecated(old: str, new: str) -> None:
    """
    Warn that a command-line option is deprecated (a log message).

    Parameters
    ----------
    old : str
        The deprecated option (e.g. "--site-layout")
    new : str
        What to use instead
    """
    lg.warning(f"{old} is deprecated: use {new}")


def _many(help: str, **kwargs) -> Parameter:
    """Parameter of an option that takes several values."""
    return Parameter(help=help, consume_multiple=True, **kwargs)


def option_group(title: str, sort_key: int):
    """
    Decorate an option group: frozen dataclass, flattened flags.

    Parameters
    ----------
    title : str
        Title of its help panel
    sort_key : int
        Position of the panel

    Returns
    -------
    callable
        Class decorator
    """
    def decorate(cls):
        cls = dataclasses.dataclass(frozen=True)(cls)
        cls.panel = (title, sort_key)
        return Parameter(name="*", group=Group(title, sort_key=sort_key))(cls)
    return decorate


class OptionGroup:
    """
    Base of the option groups.

    Attributes
    ----------
    panel : tuple[str, int]
        Title and position of the help panel
    deprecated : dict[str, tuple[str, str]]
        Deprecated alias fields: field -> (the field it stands for, what
        to use instead). Config files refuse them (`brainspresso.config`)
    """

    panel: ClassVar[tuple[str, int]] = ("Options", 50)
    deprecated: ClassVar[Mapping[str, tuple[str, str]]] = {}

    def source_options(self, dataset) -> dict[str, Any]:
        """
        Per-run options offered to every source (`Source.configure`).

        Parameters
        ----------
        dataset : Dataset
            The dataset of the command

        Returns
        -------
        dict
            Options (unknown options are ignored by sources)
        """
        return {}

    @classmethod
    def choices(cls, dataset, command: str) -> dict[str, Any]:
        """
        Per-dataset types of some fields (choices).

        Parameters
        ----------
        dataset : Dataset
        command : str

        Returns
        -------
        dict[str, type]
            New type of each field (without its `Parameter`)
        """
        return {}

    @classmethod
    def defaults(cls, dataset, command: str) -> dict[str, Any]:
        """
        Per-dataset defaults of some fields.

        `Dataset.option_defaults[command]` is applied on top.

        Parameters
        ----------
        dataset : Dataset
        command : str

        Returns
        -------
        dict[str, Any]
            Default of each field
        """
        return {}

    @classmethod
    def helps(cls, dataset, command: str) -> dict[str, str]:
        """
        Per-dataset help of some fields.

        Parameters
        ----------
        dataset : Dataset
        command : str

        Returns
        -------
        dict[str, str]
            Help of each field
        """
        return {}

    @classmethod
    def for_dataset(cls, dataset, command: str) -> type:
        """
        Return the group with the choices and defaults of a dataset.

        Parameters
        ----------
        dataset : Dataset
        command : str

        Returns
        -------
        type
            `cls`, or a subclass with other field types or defaults
        """
        names = {f.name for f in dataclasses.fields(cls)}
        types = cls.choices(dataset, command)
        helps = cls.helps(dataset, command)
        defaults = dict(cls.defaults(dataset, command))
        defaults.update({
            k: v for k, v in
            dict(dataset.option_defaults.get(command, {})).items()
            if k in names
        })
        if not types and not defaults and not helps:
            return cls
        hints = field_hints(cls)
        fields = []
        for f in dataclasses.fields(cls):
            if f.name not in types and f.name not in defaults and \
                    f.name not in helps:
                continue
            hint = hints[f.name]
            meta = getattr(hint, "__metadata__", ())
            base = getattr(hint, "__origin__", hint) if meta else hint
            if f.name in types:
                base = types[f.name]
            if f.name in helps:
                meta = (*meta, Parameter(help=helps[f.name]))
            hint = Annotated[(base, *meta)] if meta else base
            default = defaults.get(f.name, f.default)
            fields.append((f.name, hint, dataclasses.field(default=default)))
        new = dataclasses.make_dataclass(
            cls.__name__, fields, bases=(cls,), frozen=True,
            namespace={"__module__": cls.__module__,
                       "__doc__": cls.__doc__},
        )
        title, sort_key = cls.panel
        return Parameter(name="*",
                         group=Group(title, sort_key=sort_key))(new)


def field_hints(cls) -> dict[str, Any]:
    """
    Annotations of the fields of a dataclass (with `Annotated`).

    Parameters
    ----------
    cls : type
        An option group

    Returns
    -------
    dict[str, Any]
        Field -> annotation (of the class that defines it last)
    """
    out = {}
    for klass in reversed(cls.__mro__):
        out.update(getattr(klass, "__annotations__", {}))
    return out


def literal(values) -> Any:
    """`Literal[values]`."""
    return Literal[tuple(str(v) for v in values)]


# ----------------------------------------------------------------------
#   Selection
# ----------------------------------------------------------------------

@option_group("Keys", 10)
class KeyOptions(OptionGroup):
    """`--keys`, `--exclude-keys` (see `Dataset.keys_for`)."""

    keys: Annotated[Strings, _many(
        "Only these keys (data categories of the dataset; default: all)"
    )] = ()
    exclude_keys: Annotated[Strings, _many("Not these keys")] = ()

    @classmethod
    def choices(cls, dataset, command):
        """Choices of the keys of the command."""
        spec = dataset.keys_for(command)
        choices = spec.all_choices() if spec is not None else None
        if choices is None:
            return {}
        return {"keys": tuple[literal(choices), ...],
                "exclude_keys": tuple[literal(choices), ...]}

    @classmethod
    def helps(cls, dataset, command):
        """Roast (on the roast engine): what `--exclude-keys` alone does."""
        if command != "roast" or dataset.legacy_roast():
            return {}
        spec = dataset.keys_for(command)
        off = f" ({', '.join(spec.off)}: only if asked for with `--keys`)" \
            if spec is not None and spec.off else ""
        return {
            "keys": "Only these keys (data categories of the dataset; "
                    "default: all" + off + ")",
            "exclude_keys": "Not these keys. Without `--keys`, files that "
                            "match no key are kept",
        }

    @classmethod
    def defaults(cls, dataset, command):
        """Default keys of the command."""
        spec = dataset.keys_for(command)
        return {"keys": tuple(spec.default)} if spec and spec.default else {}


@option_group("Selection", 20)
class SelectionOptions(OptionGroup):
    """`--subs`, `--exclude-subs`."""

    subs: Annotated[Strings, _many(
        "Only these subjects (labels, with or without `sub-`; a file "
        "lists one per line). Default: all"
    )] = ()
    exclude_subs: Annotated[Strings, _many("Not these subjects")] = ()


@option_group("Selection", 20)
class SessionOptions(OptionGroup):
    """`--sessions`."""

    sessions: Annotated[Strings, _many(
        "Only these sessions (labels, with or without `ses-`)"
    )] = ()


@option_group("Selection", 20)
class SiteOptions(OptionGroup):
    """`--sites`, `--exclude-sites` (checked by `Dataset.select_sites`)."""

    sites: Annotated[Strings, _many("Only these sites (default: all)")] = ()
    exclude_sites: Annotated[Strings, _many("Not these sites")] = ()

    @classmethod
    def helps(cls, dataset, command):
        """List the sites of the dataset in the help."""
        names = dataset.site_names
        if not names:
            return {}
        return {"sites": f"Only these sites (default: all): "
                         f"{', '.join(names)}"}

    def source_options(self, dataset):
        """`sites` (and `exclude_sites`) for the sources."""
        sites = set(dataset.select_sites(self.sites))
        exclude = set(dataset.select_sites(self.exclude_sites))
        out = {}
        if exclude and dataset.site_names:
            sites = (sites or set(dataset.site_names)) - exclude
            if not sites:
                raise UsageError("Every site is excluded")
        elif exclude:
            out["exclude_sites"] = frozenset(exclude)
        out["sites"] = frozenset(sites)
        return out


@option_group("Filters", 25)
class FilterOptions(OptionGroup):
    """BIDS filters: datatypes, suffixes, tasks, globs, participants."""

    datatypes: Annotated[Strings, _many(
        "Only these datatypes (anat, func, dwi, fmap, perf...)"
    )] = ()
    suffixes: Annotated[Strings, _many(
        "Only these suffixes (T1w, T2w, bold, dwi...)"
    )] = ()
    tasks: Annotated[Strings, _many("Only these tasks")] = ()
    include: Annotated[Strings, _many(
        "Only data files matching these glob patterns (relative to the "
        "BIDS root, e.g. `\"sub-*/anat/*\"`)"
    )] = ()
    exclude: Annotated[Strings, _many(
        "Not the files matching these glob patterns"
    )] = ()
    where: Annotated[Strings, _many(
        "Only participants matching these queries on participants.tsv "
        "columns, e.g. `\"age>=60\"` or `\"sex==F\"` (all must match)"
    )] = ()

    def filters(self) -> dict[str, Strings]:
        """Return the `Selection` filters (without `where`)."""
        return {"datatypes": self.datatypes, "suffixes": self.suffixes,
                "tasks": self.tasks, "include": self.include,
                "exclude": self.exclude}


@option_group("Filters", 25)
class HostedOptions(OptionGroup):
    """Options of harvests of datasets hosted as BIDS."""

    folders: Annotated[Strings, _many(
        "Also download these optional folders"
    )] = ()
    trim: Annotated[bool, Parameter(
        help="Rewrite participants/sessions/scans tables so that they only "
        "list what is present locally (originals in `.brainspresso/orig/`)"
    )] = True
    list: Annotated[bool, Parameter(
        help="Print the selected files before downloading"
    )] = False

    @classmethod
    def choices(cls, dataset, command):
        """Return the optional folders of the dataset."""
        if not dataset.folder_choices:
            return {}
        return {"folders": tuple[literal(dataset.folder_choices), ...]}


# ----------------------------------------------------------------------
#   Sources and credentials
# ----------------------------------------------------------------------

@option_group("Sources", 30)
class SourceOptions(OptionGroup):
    """`--source`: sources to use instead of the per-key defaults."""

    source: Annotated[Strings, _many(
        "Sources to use instead of the defaults (in this order); for "
        "roast, the source whose data are roasted"
    )] = ()

    @classmethod
    def choices(cls, dataset, command):
        """Names of the sources."""
        names = tuple(dataset.source_names())
        return {"source": tuple[literal(names), ...]} if names else {}


@option_group("Credentials", 35)
class LoginOptions(OptionGroup):
    """`--user`, `--password` (else environment variables, or a prompt)."""

    user: Annotated[str | None, Parameter(help="Username")] = None
    password: Annotated[str | None, Parameter(help="Password")] = None

    def source_options(self, dataset):
        """`user` and `password`."""
        return {"user": self.user, "password": self.password}


@option_group("Credentials", 35)
class TokenOptions(OptionGroup):
    """`--token` (else an environment variable)."""

    token: Annotated[str | None, Parameter(help="API token")] = None

    def source_options(self, dataset):
        """`token`."""
        return {"token": self.token}


# ----------------------------------------------------------------------
#   Commands
# ----------------------------------------------------------------------

_LOCK_WAIT = Parameter(help="Seconds to wait for a dataset in use by "
                            "another command (default: fail at once)")
# `--wait SECONDS`: the deprecated name of `--lock-wait` (harvest and
# compost only: on roast, `--wait` is kept for the execution flag
# `--wait/--detach` of grind, §7)
_WAIT = Parameter(show=False, help="Deprecated: use `--lock-wait`")
_WAIT_ALIAS = {"wait": ("lock_wait", "--lock-wait")}


def lock_wait(group) -> float:
    """
    Seconds to wait for the run lock (`--lock-wait`, or `--wait`).

    Parameters
    ----------
    group : OptionGroup
        Options of the command (harvest, roast, compost)

    Returns
    -------
    float
        The deprecated `--wait` (logged) if given, else `--lock-wait`

    Raises
    ------
    UsageError
        `--wait` and `--lock-wait` with different values
    """
    old = getattr(group, "wait", None)
    new = float(getattr(group, "lock_wait", 0) or 0)
    if old is None:
        return new
    deprecated("--wait", "--lock-wait")
    if new and float(old) != new:
        raise UsageError(f"--wait {old:g} and --lock-wait {new:g} "
                         f"contradict each other")
    return float(old)


@option_group("Harvest", 40)
class HarvestOptions(OptionGroup):
    """Options of downloads."""

    if_exists: Annotated[IfExists.Choice, Parameter(
        help="Behaviour if a file already exists"
    )] = "skip"
    jobs: Annotated[int | None, Parameter(
        help="Number of parallel downloads (default: that of the source)"
    )] = None
    packet: Annotated[int | str, Parameter(
        help="Packet size to download, in bytes"
    )] = naturalsize(CHUNK_SIZE)
    insecure: Annotated[bool | None, Parameter(
        help="Do not verify TLS certificates (default: that of the source)"
    )] = None
    dry_run: Annotated[bool, Parameter(
        help="Print what would be downloaded, then exit (nothing is "
        "written)"
    )] = False
    lock_wait: Annotated[float, _LOCK_WAIT] = 0
    wait: Annotated[float | None, _WAIT] = None

    deprecated = _WAIT_ALIAS

    @classmethod
    def defaults(cls, dataset, command):
        """Return the number of downloads of the dataset."""
        return {"jobs": dataset.harvest_jobs}

    def runtime(self, **fixed):
        """
        Options of the sources.

        Parameters
        ----------
        **fixed
            Fixed options of the dataset (`Dataset.download`)

        Returns
        -------
        sources.base.DownloadOptions
        """
        return DownloadOptions(
            ifexists=self.if_exists, jobs=self.jobs,
            chunk_size=human2bytes(self.packet), insecure=self.insecure,
            dry_run=self.dry_run, **fixed,
        )


@option_group("Roast", 40)
class RoastOptions(OptionGroup):
    """
    Options of a roast.

    `--sidecars yes|no|only` replaces `--json [yes|no|only]` (a `Literal |
    bool` field, an option of zero or one value, which cyclopts only
    parses from 5.1); `--json VALUE` and `--no-json` (`--sidecars no`) are
    hidden deprecated aliases. `sidecar_policy` is the value the roasters
    take.
    """

    sidecars: Annotated[Literal["yes", "no", "only"] | None, Parameter(
        help="Sidecars: \"yes\" writes them; \"no\" skips the JSON "
        "sidecars (bval/bvec and tables are still written); \"only\" "
        "writes the sidecars and metadata only, not the images. Default: "
        "\"yes\""
    )] = None
    json: Annotated[Literal["yes", "no", "only"] | None, Parameter(
        show=False, help="Deprecated: use `--sidecars`"
    )] = None
    no_json: Annotated[bool, Parameter(
        name="--no-json", negative="", show=False,
        help="Deprecated: use `--sidecars no`"
    )] = False
    if_exists: Annotated[IfExists.Choice, Parameter(
        help="Behaviour when a file already exists"
    )] = "skip"
    jobs: Annotated[int, Parameter(help="Number of processes")] = 1
    lock_wait: Annotated[float, _LOCK_WAIT] = 0

    deprecated = {
        "json": ("sidecars", "--sidecars"),
        "no_json": ("sidecars", "--sidecars no"),
    }

    @functools.cached_property
    def sidecar_policy(self) -> str:
        """
        The sidecar policy of the roasters: "yes", "no" or "only".

        `--sidecars` (default "yes"), else the deprecated `--json VALUE`
        or `--no-json` ("no"), logged once.

        Raises
        ------
        UsageError
            Two of them with different values
        """
        given = []
        if self.sidecars is not None:
            given.append(("--sidecars", str(self.sidecars)))
        if self.json is not None:
            deprecated("--json", "--sidecars")
            given.append(("--json", str(self.json)))
        if self.no_json:
            deprecated("--no-json", "--sidecars no")
            given.append(("--no-json", "no"))
        values = {value for _, value in given}
        if len(values) > 1:
            raise UsageError(" and ".join(
                f"{flag} {value}" if flag != "--no-json" else flag
                for flag, value in given) + " contradict each other")
        return values.pop() if values else "yes"


@option_group("Compost", 40)
class CompostOptions(OptionGroup):
    """Options of compost."""

    sourcedata: Annotated[bool | None, Parameter(
        help="Delete source data. Default: only if no BIDS filter is given"
    )] = None
    nested: Annotated[bool, Parameter(
        help="Also delete `derivatives/*/sourcedata/` (raw files unpacked "
        "while roasting, e.g. FreeSurfer outputs)"
    )] = False
    keep: Annotated[Strings, _many(
        "Do not delete source files matching these glob patterns "
        "(relative to the dataset folder, `*` matches `/`), e.g. "
        "`\"*.xls\"`"
    )] = ()
    derivatives: Annotated[bool, Parameter(
        help="Apply subject and session filters to `derivatives/*/` too"
    )] = False
    trim: Annotated[bool, Parameter(
        help="Rewrite participants/sessions/scans tables so that they only "
        "list what is left"
    )] = True
    force: Annotated[bool, Parameter(
        help="Delete source data even if no BIDS subject is found (the "
        "dataset does not seem to have been roasted)"
    )] = False
    ignore_runs: Annotated[bool, Parameter(
        help="Go on although other commands left run records (a run that "
        "crashed, or locks that this filesystem does not share between "
        "machines): only if no other command runs"
    )] = False
    yes: Annotated[bool, Parameter(
        name=["--yes", "-y"], help="Do not ask for confirmation"
    )] = False
    dry_run: Annotated[bool, Parameter(
        help="Print the files that would be deleted, then exit"
    )] = False
    list: Annotated[bool, Parameter(
        help="Print the files to delete before asking for confirmation"
    )] = False
    lock_wait: Annotated[float, _LOCK_WAIT] = 0
    wait: Annotated[float | None, _WAIT] = None

    deprecated = _WAIT_ALIAS


@option_group("Dataset", 60)
class LayoutOptions(OptionGroup):
    """`--layout` (and the deprecated `--site-layout`)."""

    deprecated = {"site_layout": ("layout", "--layout sites:<layout>")}

    layout: Annotated[Literal["study", "flat"] | None, Parameter(
        help="Layout of the dataset: \"study\" (raw dataset in "
        "`rawbids/`) or \"flat\" (raw dataset at the root); \"sites\": "
        "one dataset per site (with the layout of existing sites, else "
        "study), \"sites:study\", \"sites:flat\". Default: the layout of "
        "an existing dataset, else \"study\""
    )] = None
    site_layout: Annotated[Literal["study", "flat"] | None, Parameter(
        show=False, help="Deprecated: use `--layout sites:<layout>`"
    )] = None

    @classmethod
    def choices(cls, dataset, command):
        """Layouts of the dataset."""
        return {"layout": literal(dataset.layouts) | None}

    def value(self, dataset=None) -> str | None:
        """
        Return the `--layout` value (with the deprecated `--site-layout`).

        Parameters
        ----------
        dataset : Dataset, optional
            The dataset (its `layouts` are checked)

        Returns
        -------
        str | None
        """
        value = self.layout
        if self.site_layout is not None:
            if value != "sites":
                raise UsageError(
                    f"--site-layout requires --layout sites (use "
                    f"--layout sites:{self.site_layout})"
                )
            deprecated("--site-layout", f"--layout sites:{self.site_layout}")
            value = f"sites:{self.site_layout}"
        if value is not None and dataset is not None \
                and value not in dataset.layouts:
            raise UsageError(f"Layout {value!r} is not available for "
                             f"{dataset.name}: {', '.join(dataset.layouts)}")
        return value


@option_group("Logging", 90)
class LogOptions(OptionGroup):
    """`--log`, `--level`."""

    log: Annotated[str | None, Parameter(help="Path to log file")] = None
    level: Annotated[str, Parameter(
        help="Logging level (debug: tracebacks of user errors too)"
    )] = "info"


#: Standard groups: `Args` field -> group
STANDARD: Mapping[str, type] = {
    "keys": KeyOptions,
    "select": SelectionOptions,
    "sessions": SessionOptions,
    "sites": SiteOptions,
    "filters": FilterOptions,
    "hosted": HostedOptions,
    "source": SourceOptions,
    "login": LoginOptions,
    "token": TokenOptions,
    "harvest": HarvestOptions,
    "roast": RoastOptions,
    "compost": CompostOptions,
    "layout": LayoutOptions,
    "log": LogOptions,
}
