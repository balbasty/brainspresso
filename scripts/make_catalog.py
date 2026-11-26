"""
Regenerate the catalog of datasets (`brainspresso/datasets/catalog.py`).

```
python scripts/make_catalog.py          # rewrite the catalog
python scripts/make_catalog.py --check  # exit 1 if it is out of date
```

Every `Dataset` subclass with a `name` in `brainspresso/datasets/**/
dataset.py` is an entry (summary: the first line of its help), and so
is every curated INDI project (`INDI.dataset.project_dataset`).
"""
import importlib
import json
import inspect
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "brainspresso" / "datasets" / "catalog.py"
# Datasets kept out of the command line
SKIP = {"adni1"}            # not usable yet (as before step 4)


def entries() -> dict[str, tuple[str, str]]:
    """`name: (import path, summary)` of every dataset."""
    sys.path.insert(0, str(ROOT))
    from brainspresso.datasets.base import Dataset
    out = {}
    datasets = ROOT / "brainspresso" / "datasets"
    for path in sorted(datasets.glob("**/dataset.py")):
        module = ".".join(path.relative_to(ROOT).with_suffix("").parts)
        mod = importlib.import_module(module)
        for attr, obj in vars(mod).items():
            if inspect.isclass(obj) and issubclass(obj, Dataset) and \
                    obj.__module__ == module and \
                    isinstance(obj.__dict__.get("name"), str) and obj.name:
                if obj.name not in SKIP:
                    out[obj.name] = (f"{module}:{attr}", obj().summary)
    from brainspresso.datasets.INDI.dataset import project_dataset, projects
    for name in projects():
        out[name] = ("brainspresso.datasets.INDI.dataset:project_dataset",
                     project_dataset(name).summary)
    return dict(sorted(out.items()))


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


def main(check: bool = False) -> int:
    """Rewrite (or check) the catalog."""
    text = CATALOG.read_text()
    new = re.sub(
        r"(    # BEGIN CATALOG[^\n]*\n)(.*?)(    # END CATALOG)",
        lambda m: m.group(1) + render(entries()) + m.group(3),
        text, flags=re.S,
    )
    if check:
        if new != text:
            print(f"{CATALOG} is out of date: run {__file__}")
            return 1
        return 0
    CATALOG.write_text(new)
    return 0


if __name__ == "__main__":
    sys.exit(main("--check" in sys.argv[1:]))
