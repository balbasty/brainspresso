from brainspresso.utils.deps import requires

from .command import ixi as _ixi                # noqa: F401
from .download import download as _download     # noqa: F401

if requires("ixi roast", "xlrd", extra="ixi"):
    from .bidsify import bidsify as _bidsify    # noqa: F401
