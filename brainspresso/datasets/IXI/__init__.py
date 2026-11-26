from brainspresso.utils.deps import requires

from .command import ixi as _ixi                # noqa: F401
from .harvest import download as _download     # noqa: F401

if requires("ixi roast", "xlrd", extra="ixi"):
    from .roast import bidsify as _bidsify    # noqa: F401
