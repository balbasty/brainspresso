from brainspresso.utils.deps import requires

from .command import oasis2 as _oasis2          # noqa: F401
from .harvest import download as _download     # noqa: F401

if requires("oasis2 roast", "openpyxl", extra="oasis2"):
    from .roast import bidsify as _bidsify    # noqa: F401
