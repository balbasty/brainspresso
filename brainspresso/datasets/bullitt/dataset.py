"""`bx bullitt`: Designed Database of MR Brain Images (Bullitt, UNC)."""
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.bullitt.harvest import Girder
from brainspresso.datasets.bullitt.keys import allkeys
from brainspresso.datasets.bullitt.roaster import (
    ROAST_SPEC,
    TEMPLATES,
    roast_options,
)

HELP = """
Designed Database of MR Brain Images of Healthy Volunteers (Bullitt, UNC)

* **Modalities**    T1w (FLASH, MPRAGE), T2w, MRA (DTI: harvest only)
* **Populations**   100+ healthy adults, 18 to 72 years
* **Host**          https://data.kitware.com (Girder), no login needed
"""


class Bullitt(Dataset):
    """Designed Database of MR Brain Images of Healthy Volunteers."""

    name = "bullitt"
    folder = "Bullitt"
    title = "the Bullitt database"
    help = HELP
    keys = KeySpec(allkeys, None, ("raw", "meta", "derivatives"))
    subject_width = 3
    credentials = "token"
    template_dir = TEMPLATES

    def keys_for(self, command):
        """
        Return the keys of a command.

        Parameters
        ----------
        command : str
            Name of the command

        Returns
        -------
        KeySpec
            Those of the roaster for `roast` (no `dwi`, `phantoms`), else
            those of the source
        """
        if command == "roast":
            return ROAST_SPEC
        return self.keys

    def sources(self, args=None):
        """
        Return the sources of the dataset.

        Parameters
        ----------
        args : Args, optional
            Values of the command line (unused)

        Returns
        -------
        dict[str, Source]
            data.kitware.com (Girder)
        """
        return {"girder": Girder()}

    def describe(self, command):
        """
        Return the help of a command.

        Parameters
        ----------
        command : str
            Name of the command

        Returns
        -------
        str
            Roast: what is converted (DTI is not)
        """
        if command == "roast":
            return ("Convert the harvested MetaImage files to BIDS (NIfTI)."
                    "\n\nDTI is not converted: the source files have no "
                    "gradient table. Keys: raw data flash, mprage (T1w), "
                    "T2w, angio; metadata: meta; derivatives (-> "
                    "derivatives/bullitt): skullstripped, vessels. A "
                    "missing scan is an error only when `--keys` names "
                    "its key or a group above it (many subjects have no "
                    "MPRAGE). `participants.tsv` lists the subjects on "
                    "disk.")
        return super().describe(command)

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
            `named`: the keys whose missing scans are errors (from
            `--keys` as given; none with its default)
        """
        return roast_options(args.keys.keys)
