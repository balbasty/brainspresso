"""
Regenerate the catalog of datasets (`brainspresso/datasets/catalog.py`).

```
python scripts/make_catalog.py          # rewrite the catalog
python scripts/make_catalog.py --check  # exit 1 if it is out of date
```

Every `Dataset` subclass with a `name` in `brainspresso/datasets/**/
dataset.py` is an entry (summary: the first line of its help), and so
is every curated INDI project (`indi/<project>/dataset.py`: its
`project_dataset(<Project>)`, see `indi.dataset.project_dataset`). The
config file reads two more tables: `ARGUMENTS` (the arguments of the
entries that take some: OpenNeuro's accession) and `KEYS` (the config
keys of each command, over every dataset), and `DEPRECATED` (the
deprecated aliases, which config files refuse) and `CHOICES` (the
choices of each option over every dataset).
"""
import importlib
import inspect
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))   # (this checkout, not an installed copy)

from brainspresso.config import FORBIDDEN  # noqa: E402
from brainspresso.datasets.base import Dataset  # noqa: E402
from brainspresso.datasets.indi.engine import Project  # noqa: E402
from brainspresso.datasets.register import (  # noqa: E402
    command_names,
    config_options,
)

CATALOG = ROOT / "brainspresso" / "datasets" / "catalog.py"
# Datasets kept out of the command line
SKIP = {"adni1"}            # not usable yet (as before step 4)


def datasets() -> dict[str, tuple[str, Dataset]]:
    """`name: (import path, dataset)` of every dataset."""
    out = {}
    folder = ROOT / "brainspresso" / "datasets"
    for path in sorted(folder.glob("**/dataset.py")):
        module = ".".join(path.relative_to(ROOT).with_suffix("").parts)
        mod = importlib.import_module(module)
        for attr, obj in vars(mod).items():
            if inspect.isclass(obj) and issubclass(obj, Dataset) and \
                    obj.__module__ == module and \
                    isinstance(obj.__dict__.get("name"), str) and obj.name:
                if obj.name not in SKIP:
                    out[obj.name] = (f"{module}:{attr}", obj())
            # an INDI project (`indi.dataset.project_dataset`)
            project = getattr(obj, "project", None)
            if callable(obj) and inspect.isclass(project) and \
                    issubclass(project, Project) and \
                    project.__module__ == module and project.name:
                out[project.name] = (f"{module}:{attr}", obj(project.name))
    return dict(sorted(out.items()))


def entries() -> dict[str, tuple[str, str]]:
    """`name: (import path, summary)` of every dataset."""
    return {name: (path, ds.summary)
            for name, (path, ds) in datasets().items()}


def arguments() -> dict[str, tuple[str, ...]]:
    """`name: argument names` of the datasets that take arguments."""
    return {name: tuple(a.name for a in ds.arguments)
            for name, (_, ds) in datasets().items() if ds.arguments}


def keys() -> dict[str, tuple[str, ...]]:
    """`command: config keys` over every dataset."""
    out: dict[str, set] = {}
    for _, ds in datasets().values():
        for command in command_names(ds):
            out.setdefault(command, set()).update(
                k for k, o in config_options(ds, command).items()
                if k not in FORBIDDEN and o.deprecated is None)
    return {c: tuple(sorted(v)) for c, v in sorted(out.items())}


def choices() -> dict[str, tuple[str, ...]]:
    """
    `"command:key": choices` over every dataset.

    Only options whose every dataset has choices (a value outside all of
    them fits no dataset); free text somewhere: not listed.
    """
    out: dict[str, set] = {}
    free = set()
    for _, ds in datasets().values():
        for command in command_names(ds):
            for key, o in config_options(ds, command).items():
                name = f"{command}:{key}"
                if o.deprecated is not None or key in FORBIDDEN:
                    continue
                if o.choices is None:
                    free.add(name)
                else:
                    out.setdefault(name, set()).update(o.choices)
    return {k: tuple(sorted(v)) for k, v in sorted(out.items())
            if k not in free}


def deprecated() -> dict[str, tuple[str, str]]:
    """`key: (config key, flag)` to use instead of the deprecated aliases."""
    out = {}
    for _, ds in datasets().values():
        for command in command_names(ds):
            for key, o in config_options(ds, command).items():
                if o.deprecated is not None:
                    out[key] = (" / ".join(o.instead), o.deprecated)
    return dict(sorted(out.items()))


def _line(name: str, values: tuple, indent: int = 4) -> str:
    """`"name": (values),` within 79 columns."""
    q = json.dumps
    pad = " " * indent
    comma = "," if len(values) == 1 else ""
    line = f"{pad}{q(name)}: ({', '.join(map(q, values))}{comma}),"
    if len(line) <= 79:
        return line
    if all(len(f"{pad}    {q(v)},") <= 79 for v in values) and \
            len(values) <= 3:
        body = "\n".join(f"{pad}    {q(v)}," + (
            "  # noqa: E501" if len(f"{pad}    {q(v)},") > 79 else "")
            for v in values)
        return f"{pad}{q(name)}: (\n{body}\n{pad}),"
    rows, row = [], pad + "   "
    for v in values:
        item = f" {q(v)},"
        if len(row) + len(item) > 79:
            rows.append(row)
            row = pad + "   "
        row += item
    rows.append(row)
    return f"{pad}{q(name)}: (\n" + "\n".join(rows) + f"\n{pad}),"


def render(catalog: dict[str, tuple[str, str]]) -> str:
    """Lines of the generated part of `catalog.py`."""
    q = json.dumps
    lines = []
    for name, (path, summary) in catalog.items():
        line = f"    {q(name)}: ({q(path)}, {q(summary)}),"
        if len(line) > 79:
            text = f"        {q(summary)},"
            if len(text) > 79:
                text += "  # noqa: E501"
            line = f"    {q(name)}: (\n        {q(path)},\n{text}\n    ),"
        lines.append(line)
    return "\n".join(lines) + "\n"


def render_table(table: dict[str, tuple]) -> str:
    """Lines of a generated table of tuples of strings."""
    return "\n".join(_line(k, v) for k, v in table.items()) + "\n"


def _replace(text: str, block: str, body: str) -> str:
    """Replace a generated block of `catalog.py`."""
    return re.sub(
        rf"(    # BEGIN {block}[^\n]*\n)(.*?)(    # END {block})",
        lambda m: m.group(1) + body + m.group(3), text, flags=re.S,
    )


def main(check: bool = False) -> int:
    """Rewrite (or check) the catalog."""
    text = CATALOG.read_text(encoding="utf-8")
    new = _replace(text, "CATALOG", render(entries()))
    new = _replace(new, "ARGUMENTS", render_table(arguments()))
    new = _replace(new, "KEYS", render_table(keys()))
    new = _replace(new, "DEPRECATED", render_table(deprecated()))
    new = _replace(new, "CHOICES", render_table(choices()))
    if check:
        if new != text:
            print(f"{CATALOG} is out of date: run {__file__}")
            return 1
        return 0
    CATALOG.write_text(new, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main("--check" in sys.argv[1:]))
