"""`bx unc-wisconsin`: UNC-Wisconsin Rhesus Macaque Neurodevelopment."""
from pathlib import Path

from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.UNCWisconsin.harvest import Girder
from brainspresso.datasets.UNCWisconsin.keys import allkeys
from brainspresso.datasets.UNCWisconsin.roaster import KEYS

HELP = """
UNC-Wisconsin Rhesus Macaque Neurodevelopment Database

* **Species**       Rhesus macaque (Macaca mulatta)
* **Modalities**    T1w, T2w, DWI (NRRD, converted to NIfTI)
* **Populations**   Healthy, longitudinal (a few weeks to 3 years)
* **Host**          https://data.kitware.com (Girder), no login needed
* **Citation**      https://doi.org/10.3389/fnins.2017.00029
"""

ROAST_KEYS = ("meta", "T1w", "T2w", "dwi", "reg", "dti-qc", "dti-proc",
              "raw", "anat", "derivatives")


def roast_keys():
    """Keys of the roast (imported on use: the roaster needs nrrd)."""
    return KEYS


class UNCWisconsin(Dataset):
    """UNC-Wisconsin Rhesus Macaque Neurodevelopment Database."""

    name = "unc-wisconsin"
    folder = "UNC-Wisconsin"
    title = "the UNC-Wisconsin database"
    help = HELP
    keys = KeySpec(allkeys, None, ("raw", "meta"))
    sessions = True
    credentials = "token"
    template_dir = Path(__file__).parent / "templates"
    requires = {"roast": ("nrrd",)}
    extra = "unc-wisconsin"

    def keys_for(self, command):
        """Roast keys: those of the roaster."""
        if command == "roast":
            return KeySpec(roast_keys, ROAST_KEYS, ())
        return self.keys

    def sources(self, args=None):
        """data.kitware.com (Girder)."""
        return {"girder": Girder()}
