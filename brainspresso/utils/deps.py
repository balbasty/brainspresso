"""
Optional dependencies.

Commands that need an optional dependency (installed through an extra,
e.g. `pip install brainspresso[ixi]`) are only registered in the CLI if
the dependency is installed. Unavailable commands are recorded in
`UNAVAILABLE`, so that they can be reported (e.g. listed with the
missing dependencies) without being imported.

```python
from brainspresso.utils.deps import requires

if requires("ixi roast", "xlrd", extra="ixi"):
    from .roast import bidsify as _bidsify
```
"""
from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from logging import getLogger

lg = getLogger(__name__)


@dataclass(frozen=True)
class Unavailable:
    command: str            # e.g. "ixi roast"
    missing: tuple[str]     # missing modules, e.g. ("xlrd",)
    extra: str | None       # extra that installs them, e.g. "ixi"

    @property
    def hint(self) -> str:
        if self.extra:
            return f"pip install 'brainspresso[{self.extra}]'"
        return f"pip install {' '.join(self.missing)}"


UNAVAILABLE: list[Unavailable] = []


def missing(*modules: str) -> list[str]:
    """Return the modules (top-level names) that cannot be imported"""
    out = []
    for module in modules:
        try:
            if importlib.util.find_spec(module) is None:
                out.append(module)
        except (ImportError, ValueError):
            out.append(module)
    return out


def available(*modules: str) -> bool:
    """Whether all modules can be imported"""
    return not missing(*modules)


def requires(command: str, *modules: str, extra: str | None = None) -> bool:
    """
    Check the dependencies of a command.

    Returns True if all modules are available. Otherwise, records the
    command in `UNAVAILABLE` and returns False.
    """
    absent = missing(*modules)
    if not absent:
        return True
    entry = Unavailable(command, tuple(absent), extra)
    UNAVAILABLE.append(entry)
    lg.debug(
        f"`{command}` unavailable: missing {', '.join(absent)} "
        f"({entry.hint})"
    )
    return False
