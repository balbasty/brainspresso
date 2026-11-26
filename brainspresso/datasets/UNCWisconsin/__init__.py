from brainspresso.utils.deps import requires

from .command import uncwisc as _uncwisc          # noqa: F401
from .harvest import download as _download     # noqa: F401

if requires("unc-wisconsin roast", "nrrd", extra="unc-wisconsin"):
    from .roast import bidsify as _bidsify    # noqa: F401
