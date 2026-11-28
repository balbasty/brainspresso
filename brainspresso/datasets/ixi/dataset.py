"""`bx ixi`: the IXI dataset."""
from pathlib import Path

from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.ixi.harvest import DEFAULTS, sources

HELP = """
IXI dataset (controls)

* **Project**       Information eXtraction from Images (IXI)
* **Modalities**    T1w, T2w, PD2, MRA, DTI
* **Populations**   Controls
* **Funding**       EPSRC GR/S21533/02
* **License**       CC BY-SA 3.0
* **URL**           https://brain-development.org/ixi-dataset/

Harvest: images from NITRC-IR (no login needed; the original tarballs
of brain-development.org with `--source imperial`), saved in
`IXI/sourcedata/IXI-{T1,T2,PD,MRA,DTI}/` with the file names of the
original tarballs. NITRC-IR does not host `IXI.xls` (demographics):
roast uses the copy shipped with brainspresso if there is none in
`sourcedata/`. Roast: see `datasets/ixi/roaster.py`.
"""

KEYS = ("meta", "T1w", "T2w", "PDw", "angio", "dwi")


def roast_keys():
    """Keys of the roast (imported on use: the roaster imports `KEYS`)."""
    from brainspresso.datasets.ixi.roaster import (  # noqa: PLC0415
        KEYS,  # lazy: import cycle (the roaster imports this module)
    )
    return KEYS


class IXI(Dataset):
    """The IXI dataset."""

    name = "ixi"
    folder = "IXI"
    title = "the IXI dataset"
    help = HELP
    keys = KeySpec(choices=KEYS, default=KEYS)
    subject_width = 3
    credentials = "login"
    template_dir = Path(__file__).parent / "templates"
    requires = {"roast": ("xlrd",)}
    extra = "ixi"

    @property
    def defaults(self):
        """Per-key sources: NITRC-IR then Imperial, bundled `IXI.xls`."""
        return DEFAULTS

    def keys_for(self, command):
        """Roast keys include `trace` (derivative of the DWI scans)."""
        if command == "roast":
            return KeySpec(roast_keys, (*KEYS, "trace"), KEYS, ("trace",))
        return self.keys

    def roast_options(self, args):
        """
        Return the options of the roaster (`roaster.roast_options`).

        Parameters
        ----------
        args : Args
            Values of the command line

        Returns
        -------
        dict
            `named`: the keys whose missing tarballs are errors (from
            `--keys` as given; none with its default)
        """
        from brainspresso.datasets.ixi.roaster import (  # noqa: PLC0415
            roast_options,  # lazy: import cycle (the roaster imports KEYS)
        )
        return roast_options(args.keys.keys)

    def sources(self, args=None):
        """NITRC-IR, Imperial (offline), bundled metadata."""
        return sources()
