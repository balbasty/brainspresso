from brainspresso.utils.deps import requires

from .command import oasis1 as _oasis1          # noqa: F401
from .download import download as _download     # noqa: F401

if requires("oasis1 roast", "openpyxl", extra="oasis1"):
    from .bidsify import bidsify as _bidsify    # noqa: F401
