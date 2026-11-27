"""
Config files: defaults of the options of the dataset commands.

Layers, later wins (`Loader.layers`):

1. built-in defaults (those of the option groups and datasets);
2. the user file, `~/.brainspresso/config.toml` (the folder is
   `$BRAINSPRESSO_CONFIG_HOME` when set);
3. the root file, `<root>/.brainspresso/config.toml` (`<root>`: the
   `path` argument, else a configured `path`, else `$BDP_PATH`, else the
   current folder);
4. the dataset file, `<DS>/.brainspresso/config.toml` (`<DS>`: `--out`,
   else `<root>/<folder>`);
5. the files of `bx --config FILE` (repeatable, in order; TOML, or YAML
   if PyYAML is installed);
6. `BRAINSPRESSO_OPT_*` environment variables (`env_data`);
7. the command line.

Sections of a file, least to most specific (`Entry.rank`): `[defaults]`
(every command), `[<command>]` (`[roast]`), `[<dataset>]` (every
command of a dataset, named as on the command line, `dataset_names`:
`[abide1]`, case-insensitive), `[<dataset>.<command>]`
(`[abide1.roast]`); a dataset that takes an argument has one more level
(`[openneuro.ds000224]`, `[openneuro.ds000224.harvest]`). A later layer
wins over any section of an earlier one. Keys are the option names of
the command line (`exclude-keys` or `exclude_keys`; `no-<flag> = true`
for a boolean flag). `[profiles.*]` and `grind` tables are reserved
(grind, §7) and ignored.

`Loader` is the cyclopts config callable of the root app
(`App(config=...)`): it runs after the command line is parsed, reads
the parsed `path`/`--out` to find the root and dataset files, and adds
tokens only to the options that the command line did not give.

This module imports no dataset and no option group: it runs for every
command, and `bx --help` imports it.
"""
import dataclasses
import difflib
import functools
import logging
import os
import shlex
import tomllib
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from cyclopts import Token

from brainspresso.datasets.catalog import ARGUMENTS, CATALOG, DEPRECATED, KEYS
from brainspresso.errors import UsageError
from brainspresso.utils.path import get_tree_path

lg = logging.getLogger(__name__)

#: Name of the config files
FILENAME = "config.toml"
#: Folder of the user, root and dataset files (`bids.layout.STATE_DIR`)
STATE_DIR = ".brainspresso"
#: Folder of the user file (instead of `~/.brainspresso`)
HOME_ENV = "BRAINSPRESSO_CONFIG_HOME"
#: Prefix of the environment variables of options
ENV_PREFIX = "BRAINSPRESSO_OPT_"
#: Separator of the sections in their names
ENV_SEP = "__"
#: Command sections
COMMANDS = ("harvest", "roast", "compost", "info")
#: Reserved: `[profiles.<name>]` (top level) and `grind` tables
# TODO(§7, grind): execution profiles and `[<Dataset>.grind.<pipeline>]`
RESERVED = ("profiles",)
RESERVED_COMMANDS = ("grind",)
#: Keys that a config file never sets -> why
FORBIDDEN = {
    "yes": "a destructive confirmation",
    "y": "a destructive confirmation",
    "force": "a destructive confirmation",
    "ignore-runs": "a destructive confirmation",
    "password": "a secret",
    "token": "a secret",
    "config": "the option that names config files",
}
#: Keys that locate the files (not in root and dataset files)
LOCATION = ("path", "out")
#: Layers that are found from the location
LOCATED = ("root", "dataset")


class ConfigError(UsageError):
    """A config file (or variable) that cannot be used."""


def _did_you_mean(word: str, candidates: Iterable[str]) -> str:
    """` (did you mean 'x'?)`, or nothing."""
    close = difflib.get_close_matches(word, sorted(set(candidates)), n=1,
                                      cutoff=0.6)
    return f" (did you mean {close[0]!r}?)" if close else ""


# ----------------------------------------------------------------------
#   Names of the dataset sections
# ----------------------------------------------------------------------

def dataset_names(name: str) -> tuple[str, tuple[str, ...]]:
    """
    Section name and aliases of a catalog entry (the one naming rule).

    Sections are named after the command name (`[abide1]`), matched
    case-insensitively, with one more part per argument of the dataset
    (`[openneuro.ds000224]`).

    Parameters
    ----------
    name : str
        Command name (`abide1`)

    Returns
    -------
    section : str
        Dotted section name, with `{argument}` parts (`abide1`,
        `openneuro.{dataset}`)
    aliases : tuple[str]
        Other names of the dataset (one part each, matched
        case-insensitively): none
    """
    return name + "".join(f".{{{a}}}" for a in ARGUMENTS.get(name, ())), ()


@dataclasses.dataclass
class Node:
    """
    A section of the dataset tree (`abide1`; a dotted name nests).

    Attributes
    ----------
    parts : tuple[str]
        Canonical name
    children : dict[str, Node]
        Sub-sections (case-sensitive)
    dataset : str | None
        Command name if the node is a dataset
    arguments : tuple[str]
        Arguments of the dataset (one more part each)
    """

    parts: tuple
    children: dict = dataclasses.field(default_factory=dict)
    dataset: str | None = None
    arguments: tuple = ()

    @property
    def section(self) -> str:
        """Dotted name."""
        return ".".join(self.parts)


@functools.cache
def tree() -> tuple[Node, dict[str, Node]]:
    """
    Tree of the dataset sections, and its index of first parts.

    Returns
    -------
    root : Node
    index : dict[str, Node]
        Lower-case first part (canonical names and aliases) -> node

    Raises
    ------
    ValueError
        If one name means two datasets
    """
    root = Node(())
    index: dict[str, Node] = {}

    def add(name, node):
        if index.setdefault(name.lower(), node) is not node:
            raise ValueError(f"Config section {name!r} names "
                             f"{index[name.lower()].section} and "
                             f"{node.section}")

    for name in CATALOG:
        section, aliases = dataset_names(name)
        parts = section.split(".")
        node = root
        for part in (p for p in parts if not p.startswith("{")):
            node = node.children.setdefault(
                part, Node((*node.parts, part)))
        node.dataset = name
        node.arguments = tuple(p[1:-1] for p in parts if p.startswith("{"))
        add(parts[0], root.children[parts[0]])
        for alias in aliases:
            add(alias, node)
    return root, index


def dataset_parts(dataset) -> tuple[tuple[str, ...], int]:
    """
    Section parts of a dataset (with its bound arguments).

    Parameters
    ----------
    dataset : Dataset

    Returns
    -------
    parts : tuple[str]
        `("abide1",)`, `("openneuro", "ds000224")`; `("<name>",)` for a
        dataset that is not in the catalog (no section matches it)
    own : int
        Number of parts of the dataset itself (sections with at least
        these parts are checked against its options)
    """
    if dataset.name not in CATALOG:
        return (f"<{dataset.name}>",), 1
    section, _ = dataset_names(dataset.name)
    parts = [p for p in section.split(".") if not p.startswith("{")]
    own = len(parts)
    for p in section.split("."):
        if p.startswith("{"):
            value = dataset.bound.get(p[1:-1])
            if value is None:
                break
            parts.append(str(value).lower())
    return tuple(parts), own


# ----------------------------------------------------------------------
#   Options of a command
# ----------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class OptionInfo:
    """
    An option of a command, as a config key.

    Attributes
    ----------
    key : str
        Its name on the command line, without `--` (`exclude-keys`)
    field : str
        Python name of the field (or parameter)
    group : str | None
        Parameter of its option group (None: `path`, `out`)
    default : Any
        Built-in default
    is_list : bool
        Takes several values
    is_bool : bool
        Has a negative flag (`no-<key> = true`)
    choices : tuple[str] | None
        Values of a `Literal` (of the dataset)
    aliases : tuple[str]
        Keys of the same option (a deprecated alias and its option): one
        given on the command line, the others are not read from config
    deprecated : str | None
        What to use instead of a deprecated alias (never read from
        config)
    """

    key: str
    field: str
    group: str | None = None
    default: Any = None
    is_list: bool = False
    is_bool: bool = False
    choices: tuple | None = None
    aliases: tuple = ()
    deprecated: str | None = None

    def fits(self, value) -> bool:
        """Whether a value (or each value of a list) is a choice."""
        if self.choices is None:
            return True
        values = value if isinstance(value, list) else [value]
        return all((isinstance(v, bool) and self.is_bool)
                   or str(v) in self.choices for v in values)


#: `path` and `out` of the commands that take them
PATH = OptionInfo("path", "path")
OUT = OptionInfo("out", "out")


@dataclasses.dataclass(frozen=True)
class Origin:
    """
    Where a value comes from.

    Attributes
    ----------
    kind : str
        "built-in", "user", "root", "dataset", "file", "env" or "CLI"
    source : str | None
        File, or environment variable
    section : str | None
        Section of the file
    """

    kind: str
    source: str | None = None
    section: str | None = None

    def __str__(self) -> str:
        """`file [section]`, `env VAR`, `CLI` or `built-in`."""
        if self.kind == "env":
            return f"env {self.source}"
        if self.source:
            return f"{self.source} [{self.section}]"
        return self.kind


# ----------------------------------------------------------------------
#   Files and layers
# ----------------------------------------------------------------------

@dataclasses.dataclass
class Entry:
    """
    The options of one section.

    Attributes
    ----------
    parts : tuple[str]
        Canonical dataset parts (bound arguments included; empty for
        `[defaults]` and `[<command>]`)
    command : str | None
        Command of the section (None: every command)
    section : str
        Name of the section as written
    source : str
        File, or environment variable
    values : dict[str, tuple]
        Key -> (value, key as written, negated)
    """

    parts: tuple
    command: str | None
    section: str
    source: str
    values: dict

    @property
    def rank(self) -> tuple[int, bool]:
        """Specificity: dataset parts first, then the command."""
        return len(self.parts), self.command is not None

    def applies(self, parts: tuple, command: str) -> bool:
        """Whether the section applies to a dataset and a command."""
        return tuple(parts[:len(self.parts)]) == self.parts and \
            self.command in (None, command)


@dataclasses.dataclass
class Layer:
    """
    A config file (or the environment).

    Attributes
    ----------
    kind : str
        "user", "root", "dataset", "file" or "env"
    path : Path | None
        File (None: environment)
    exists : bool
        Whether the file exists
    entries : list[Entry]
    """

    kind: str
    path: Path | None
    exists: bool = False
    entries: list = dataclasses.field(default_factory=list)


class EnvValue(str):
    """A value from an environment variable (lists split on spaces)."""


def _fail(source, section, message):
    where = f" [{section}]" if section else ""
    return ConfigError(f"Config {source}{where}: {message}")


def _all_keys() -> set:
    """Config keys of any command."""
    return set().union(*KEYS.values()) if KEYS else set()


def _entry(table: Mapping, parts: tuple, command: str | None,
           section: str, source: str, kind: str,
           dataset: bool) -> Entry:
    """Check the keys of a section (any dataset: `catalog.KEYS`)."""
    if command is None:
        known = _all_keys()
    else:
        known = set(KEYS.get(command, ()))
    known |= set(LOCATION)
    values = {}
    for raw, value in table.items():
        if isinstance(value, Mapping):
            raise _fail(source, section,
                        f"unknown section [{section}.{raw}]"
                        + _did_you_mean(raw, COMMANDS))
        key, negated = str(raw).replace("_", "-"), False
        if key not in known and key.startswith("no-") and (
                key[3:] in known or key[3:] in DEPRECATED):
            key, negated = key[3:], True
        if key in FORBIDDEN:
            raise _fail(source, section,
                        f"{raw!r} cannot be set in a config file "
                        f"({FORBIDDEN[key]}): give it on the command line")
        if key in DEPRECATED:
            raise _fail(source, section,
                        f"{raw!r} is a deprecated alias, not read from "
                        f"config files: use {DEPRECATED[key][0]}")
        if key not in known:
            raise _fail(source, section, f"unknown key {raw!r}"
                        + _did_you_mean(key, known))
        if key in LOCATION and kind in LOCATED:
            raise _fail(source, section,
                        f"{raw!r} cannot be set in a {kind} file (it "
                        f"locates that file)")
        if key == "out" and not dataset:
            raise _fail(source, section,
                        "'out' (a dataset folder) only goes in the section "
                        "of a dataset")
        if negated and not isinstance(value, (bool, EnvValue)):
            raise _fail(source, section, f"{raw!r} takes true or false")
        items = value if isinstance(value, list) else [value]
        for item in items:
            if item is None or isinstance(item, (list, Mapping)):
                raise _fail(source, section,
                            f"{raw!r}: unsupported value {value!r}")
        if key in values:
            raise _fail(source, section, f"{raw!r} is set twice")
        values[key] = (value, raw, negated)
    return Entry(parts, command, section, source, values)


def parse(data: Mapping, source: str, kind: str = "file") -> list[Entry]:
    """
    Check the sections of a file and return them.

    Parameters
    ----------
    data : dict
        Content of the file
    source : str
        Its name (in messages)
    kind : str
        Its layer ("user", "root", "dataset", "file", "env")

    Returns
    -------
    list[Entry]

    Raises
    ------
    ConfigError
        Unknown sections or keys, keys that cannot be set, bad values
    """
    _, index = tree()
    entries = []

    def walk(node: Node, table, written: tuple, bound: tuple):
        section = ".".join(written)
        if not isinstance(table, Mapping):
            raise _fail(source, None, f"[{section}] is not a table")
        # one dataset (`out` names its folder)
        one = node.dataset is not None and len(bound) == len(node.arguments)
        options = {}
        for key, value in table.items():
            if not isinstance(value, Mapping):
                options[key] = value
            elif key in RESERVED_COMMANDS:
                continue        # TODO(§7): [<Dataset>.grind.<pipeline>]
            elif key in COMMANDS:
                entries.append(_entry(
                    value, (*node.parts, *bound), key,
                    f"{section}.{key}", source, kind, one))
            elif key in node.children and not bound:
                walk(node.children[key], value, (*written, key), ())
            elif len(bound) < len(node.arguments):
                # (arguments compare case-insensitively, as names do)
                walk(node, value, (*written, key), (*bound, str(key).lower()))
            else:
                choices = [*COMMANDS] if bound or node.arguments else [
                    *COMMANDS, *node.children]
                raise _fail(source, None,
                            f"unknown section [{section}.{key}]"
                            + _did_you_mean(key, choices))
        if options:
            entries.append(_entry(options, (*node.parts, *bound), None,
                                  section, source, kind, one))

    for key, value in data.items():
        if key in RESERVED or key in RESERVED_COMMANDS:
            continue            # TODO(§7): execution profiles, grind
        if key == "defaults" or key in COMMANDS:
            if not isinstance(value, Mapping):
                raise _fail(source, None, f"[{key}] is not a table")
            entries.append(_entry(value, (), None if key == "defaults"
                                  else key, key, source, kind, False))
        elif str(key).lower() in index:
            walk(index[str(key).lower()], value, (key,), ())
        elif not isinstance(value, Mapping):
            raise _fail(source, None,
                        f"{key!r} is outside a section (use [defaults])")
        else:
            hint = _did_you_mean(key, ["defaults", *COMMANDS, *index, *(
                n.section for n in tree()[0].children.values())])
            raise _fail(source, None, f"unknown section [{key}]{hint}")
    return entries


def read_file(path: Path) -> dict:
    """
    Read a config file (TOML; YAML if PyYAML is installed).

    Parameters
    ----------
    path : Path

    Returns
    -------
    dict

    Raises
    ------
    ConfigError
        If it cannot be read
    """
    path = Path(path)
    suffix = path.suffix.lower()
    try:
        if suffix in (".yaml", ".yml"):
            try:
                import yaml  # noqa: PLC0415 (lazy: optional dependency)
            except ImportError as e:
                raise ConfigError(f"Config {path}: YAML files need PyYAML "
                                  f"(pip install pyyaml)") from e
            data = yaml.safe_load(path.read_text()) or {}
        elif suffix == ".toml":
            with path.open("rb") as f:
                data = tomllib.load(f)
        else:
            raise ConfigError(f"Config {path}: not a .toml (or .yaml) file")
    except ConfigError:
        raise
    except Exception as e:
        raise ConfigError(f"Config {path}: {type(e).__name__}: {e}") from e
    if not isinstance(data, Mapping):
        raise ConfigError(f"Config {path}: not a table of sections")
    return dict(data)


def user_dir() -> Path:
    """Folder of the user file (`$BRAINSPRESSO_CONFIG_HOME`)."""
    home = os.environ.get(HOME_ENV)
    return Path(home).expanduser() if home else Path.home() / STATE_DIR


def located_file(folder) -> Path:
    """`<folder>/.brainspresso/config.toml` (root and dataset files)."""
    return Path(folder) / STATE_DIR / FILENAME


def env_data(environ: Mapping[str, str]) -> list[tuple[str, dict]]:
    """
    Sections of the `BRAINSPRESSO_OPT_*` variables.

    `BRAINSPRESSO_OPT_[<SECTION>__...]<KEY>`: parts separated by `__`,
    matched case-insensitively (`BRAINSPRESSO_OPT_JOBS`: `[defaults]`,
    `BRAINSPRESSO_OPT_ROAST__JOBS`: `[roast]`,
    `BRAINSPRESSO_OPT_ABIDE1__ROAST__EXCLUDE_KEYS`:
    `[abide1.roast] exclude-keys`; `_` stands for `-` in dataset names
    and keys, `BRAINSPRESSO_OPT_NKI_RS__HARVEST__JOBS`). Values of
    options that take several values are split as a shell does
    (`shlex.split`: `"age >= 60" "sex == F"`).

    Parameters
    ----------
    environ : dict

    Returns
    -------
    list[tuple[str, dict]]
        Variable -> its content as a file's
    """
    _, index = tree()
    out = []
    for var in sorted(environ):
        if not var.startswith(ENV_PREFIX):
            continue
        parts = var[len(ENV_PREFIX):].split(ENV_SEP)
        key, sections = parts[-1].lower(), parts[:-1]
        if not key or not all(sections):
            raise ConfigError(f"${var}: not BRAINSPRESSO_OPT_[<SECTION>"
                              f"__...]<KEY>")
        names: list[str] = []
        node = None
        for i, part in enumerate(sections):
            low = part.lower()
            if i == 0:
                if len(sections) == 1 and (low == "defaults"
                                           or low in COMMANDS):
                    names = [low]
                elif low in index or low.replace("_", "-") in index:
                    node = index.get(low) or index[low.replace("_", "-")]
                    names = list(node.parts)
                else:
                    raise ConfigError(f"${var}: unknown section {part!r}")
            elif low in COMMANDS:
                names.append(low)
            else:
                child = {k.lower().replace("-", "_"): k
                         for k in node.children}.get(low)
                if child is not None:
                    node = node.children[child]
                names.append(child or low)  # (else an argument)
        data: dict = {}
        table = data
        for name in names or ["defaults"]:
            table = table.setdefault(name, {})
        table[key] = EnvValue(environ[var])
        out.append((var, data))
    return out


# ----------------------------------------------------------------------
#   Effective values
# ----------------------------------------------------------------------

_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off")


def plain_value(entry: Entry, key: str):
    """
    Value of a key as written, negative flags applied (`bx config show`).

    Parameters
    ----------
    entry : Entry
    key : str

    Returns
    -------
    Any
    """
    value, raw, negated = entry.values[key]
    if not negated:
        return value
    if isinstance(value, EnvValue):
        if value.lower() not in (*_TRUE, *_FALSE):
            raise _fail(entry.source, entry.section,
                        f"{raw!r} takes true or false")
        value = value.lower() in _TRUE
    return not value


def _value(option: OptionInfo, entry: Entry, key: str):
    """Value of a key for an option (lists, negative flags)."""
    value, raw, negated = entry.values[key]
    if negated:
        if not option.is_bool:
            raise _fail(entry.source, entry.section,
                        f"{raw!r}: --{option.key} has no negative flag")
        return plain_value(entry, key)
    if isinstance(value, EnvValue) and option.is_list:
        try:
            return shlex.split(value)
        except ValueError as e:
            raise _fail(entry.source, entry.section, f"{raw!r}: {e}") from e
    if isinstance(value, list) and not option.is_list:
        raise _fail(entry.source, entry.section,
                    f"{raw!r} takes one value, not a list")
    return value


def resolve(layers: Iterable[Layer], parts: tuple, command: str,
            options: Mapping[str, OptionInfo], own: int | None = None,
            everywhere: Callable[[], set] | None = None
            ) -> dict[str, tuple[Any, Origin]]:
    """
    Effective config values of the options of a command.

    Parameters
    ----------
    layers : list[Layer]
        In order (later wins)
    parts : tuple[str]
        Dataset parts (`dataset_parts`)
    command : str
    options : dict[str, OptionInfo]
        Options of the command, by key
    own : int, optional
        Sections with at least this many parts are the dataset's own:
        their keys must be options of the dataset (None: not checked)
    everywhere : callable, optional
        Keys of every command of the dataset (checks `[<Dataset>]`)

    Returns
    -------
    dict[str, tuple[Any, Origin]]
        Key -> value and origin, for the keys that are set

    Raises
    ------
    ConfigError
        A key of the dataset's own sections that it does not have
    """
    out = {}
    for layer in layers:
        entries = [e for e in layer.entries if e.applies(parts, command)]
        entries.sort(key=lambda e: e.rank)
        ranks: dict[str, tuple] = {}
        for entry in entries:
            strict = own is not None and len(entry.parts) >= own
            for key in entry.values:
                option = options.get(key)
                if option is None:
                    if not strict:
                        continue
                    if entry.command is not None:
                        raise _fail(entry.source, entry.section,
                                    f"{entry.values[key][1]!r} is not an "
                                    f"option of {command}"
                                    + _did_you_mean(key, options))
                    if everywhere is not None and key not in everywhere():
                        raise _fail(entry.source, entry.section,
                                    f"{entry.values[key][1]!r} is not an "
                                    f"option of this dataset")
                    continue
                previous = ranks.get(key)
                if previous is not None and previous[0] == entry.rank and \
                        previous[1] != entry.section:
                    raise _fail(entry.source, entry.section,
                                f"{entry.values[key][1]!r} is also set in "
                                f"[{previous[1]}]")
                value = _value(option, entry, key)
                if not entry.parts and not option.fits(value):
                    # a shared section: not a choice of this dataset
                    lg.debug(f"Config {entry.source} [{entry.section}]: "
                             f"{key} = {value!r} is not a choice here "
                             f"({', '.join(option.choices)}): ignored")
                    continue
                ranks[key] = (entry.rank, entry.section)
                out[key] = (value,
                            Origin(layer.kind, entry.source, entry.section))
    return out


def tokens(key: str, value, origin: Origin) -> list[Token]:
    """
    Cyclopts tokens of a config value (converted as a CLI value is).

    Parameters
    ----------
    key : str
        Option (without `--`)
    value : Any
        Scalar or list (empty: the empty list)
    origin : Origin
        Named in conversion errors

    Returns
    -------
    list[Token]
    """
    keyword, source = f"--{key}", str(origin)
    if isinstance(value, list) and not value:
        return [Token(keyword=keyword, implicit_value=(), source=source)]
    values = value if isinstance(value, list) else [value]
    return [Token(keyword=keyword, source=source, index=i,
                  value=("true" if v else "false") if isinstance(v, bool)
                  else str(v))
            for i, v in enumerate(values)]


# ----------------------------------------------------------------------
#   The cyclopts config callable
# ----------------------------------------------------------------------

def _resolved(path) -> Path:
    """Absolute path, symbolic links resolved (where it exists)."""
    return Path(path).expanduser().resolve()


def _given(arguments, key: str):
    """Tokens of an option given on the command line (None: unknown)."""
    try:
        argument = arguments.match(f"--{key}")[0]
    except ValueError:
        return None, None
    return argument, list(argument.tokens)


class Loader:
    """
    Config callable of the root app (`App(config=Loader(...))`).

    Parameters
    ----------
    files : list[path]
        Files of `bx --config` (in order)
    environ : dict, optional
        Environment (default: `os.environ`)

    Attributes
    ----------
    datasets : dict[str, Dataset]
        Registered datasets, by command name (`cli.dataset_app`)
    describe : callable | None
        `(dataset, command) -> {key: OptionInfo}`
        (`register.config_options`)
    report : dict | None
        Last run: "dataset", "command", "layers", and "origins" (key ->
        Origin, every option of the command)
    """

    def __init__(self, files=(), environ=None):
        self.files = [Path(f) for f in files]
        self.environ = os.environ if environ is None else environ
        self.datasets: dict = {}
        self.describe: Callable | None = None
        self.report: dict | None = None
        self._cache: dict = {}

    def layer(self, kind: str, path: Path | None) -> Layer:
        """
        Read and check one file (cached).

        Parameters
        ----------
        kind : str
            "user", "root", "dataset" or "file"
        path : Path

        Returns
        -------
        Layer
        """
        path = Path(path).expanduser()
        key = (kind, str(path.absolute()))
        if key not in self._cache:
            exists = path.is_file()
            if not exists and kind == "file":
                raise ConfigError(f"Config file not found: {path}")
            entries = parse(read_file(path), str(path), kind) \
                if exists else []
            self._cache[key] = Layer(kind, path, exists, entries)
        return self._cache[key]

    def env_layer(self) -> Layer:
        """
        Read the `BRAINSPRESSO_OPT_*` variables as a layer.

        Returns
        -------
        Layer

        Raises
        ------
        ConfigError
            Two variables for one key of one section
        """
        entries, seen = [], {}
        for var, data in env_data(self.environ):
            for entry in parse(data, f"${var}", "env"):
                for key in entry.values:
                    where = (entry.parts, entry.command, key)
                    if where in seen:
                        raise ConfigError(f"${seen[where]} and ${var} set "
                                          f"the same option")
                    seen[where] = var
                entries.append(entry)
        return Layer("env", None, bool(entries), entries)

    def layers(self, root=None, folder=None) -> list[Layer]:
        """
        Every layer, in order (later wins).

        Parameters
        ----------
        root : path, optional
            Root of all datasets (its file is not read if None)
        folder : path, optional
            Dataset folder (its file is not read if None)

        Returns
        -------
        list[Layer]
        """
        user = user_dir() / FILENAME
        out = [self.layer("user", user)]
        # (a root or dataset file that is the user file or a `--config`
        # file is read once, as that: running from `~`)
        named = {_resolved(f) for f in (user, *self.files)}
        for kind, folder in (("root", root), ("dataset", folder)):
            if folder is not None and \
                    _resolved(located_file(folder)) not in named:
                out.append(self.layer(kind, located_file(folder)))
                named.add(_resolved(located_file(folder)))
        out += [self.layer("file", f) for f in self.files]
        out.append(self.env_layer())
        return out

    def locate(self, parts: tuple, command: str, path=None, out=None,
               folder: str | None = None) -> tuple[Path, Path | None]:
        """
        Root of all datasets and dataset folder.

        Parameters
        ----------
        parts : tuple[str]
            Dataset parts
        command : str
        path, out : str, optional
            Given on the command line: `path` and `out` are one location,
            so that if either is given, neither is read from config
        folder : str, optional
            Folder of the dataset under the root (None: unknown)

        Returns
        -------
        root : Path
        folder : Path | None
        """
        if not (path or out):
            early = self.layers()
            found = resolve(early, parts, command,
                            {"path": PATH, "out": OUT})
            path = found.get("path", (None,))[0]
            out = found.get("out", (None,))[0]
        root = get_tree_path(path)
        if out:
            return root, Path(out)
        return root, (root / folder if folder is not None else None)

    def __call__(self, apps, commands, arguments):
        """
        Add the config values that the command line did not give.

        Parameters
        ----------
        apps : list[App] | App
            Apps of the command (cyclopts 3: a list, >= 4: the app)
        commands : tuple[str]
            Command chain (`("ixi", "harvest")`)
        arguments : cyclopts.ArgumentCollection
            Parsed arguments (with the tokens of the command line)
        """
        commands = tuple(commands)
        if len(commands) < 2 or commands[0] not in self.datasets or \
                self.describe is None:
            return
        dataset, command = self.datasets[commands[0]], commands[1]
        options = self.describe(dataset, command)
        if not options:
            return
        bound = {}
        for argument in dataset.arguments:
            _, given = _given(arguments, argument.name.replace("_", "-"))
            if given:
                bound[argument.name] = given[0].value
        dataset = dataset.bind(**bound) if bound else dataset
        parts, own = dataset_parts(dataset)
        cli = {}
        for key in options:
            _, given = _given(arguments, key)
            if given:
                cli[key] = given
        folder = None
        if len(bound) == len(dataset.arguments):
            folder = dataset.folder_name
        path, out = (cli[k][0].value if k in cli else None
                     for k in ("path", "out"))
        root, folder = self.locate(parts, command, path=path, out=out,
                                   folder=folder)
        layers = self.layers(root, folder)
        found = resolve(layers, parts, command, options, own=own,
                        everywhere=lambda: self.everywhere(dataset))
        origins = {}
        for key, option in options.items():
            if key in cli or any(a in cli for a in option.aliases):
                # (a deprecated alias and its option are one option)
                origins[key] = Origin("CLI" if key in cli else "built-in")
            elif key in LOCATION and (path or out):
                origins[key] = Origin("built-in")   # (one location)
            elif key in found:
                argument, _ = _given(arguments, key)
                if argument is None:
                    continue
                value, origin = found[key]
                for token in tokens(key, value, origin):
                    argument.append(token)
                origins[key] = origin
            else:
                origins[key] = Origin("built-in")
        self.report = {"dataset": dataset, "command": command,
                       "layers": layers, "origins": origins,
                       "parts": parts}

    def everywhere(self, dataset) -> set:
        """Keys of every command of a dataset."""
        out = set()
        for command in COMMANDS:
            out |= set(self.describe(dataset, command) or ())
        return out
