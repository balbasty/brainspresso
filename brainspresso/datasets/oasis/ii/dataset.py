"""`bx oasis2`: OASIS-2 (longitudinal, dementia)."""
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.oasis.ii.harvest import WUSTL
from brainspresso.datasets.oasis.ii.roaster import KEYS as ROAST_ALIASES
from brainspresso.datasets.oasis.ii.roaster import (
    PARTS,
    ROAST_KEYS,
    check_parts,
)
from brainspresso.datasets.options import OptionGroup, option_group

HELP = """
OASIS-2 dataset (longitudinal, dementia)

* **Project**       Open Access Series of Imaging Studies (OASIS)
* **Subproject**    Longitudinal MRI Data in Nondemented and Demented Older Adults (OASIS-2)
* **Modalities**    T1w
* **Populations**   Controls, Dementia
* **Funding**       NIH: P50 AG05681,  P01 AG03991,  P01 AG026276,
                         R01 AG021910, P20 MH071616, U24 RR021382
* **Reference**     https://doi.org/10.1162/jocn.2009.21407
* **URL**           https://sites.wustl.edu/oasisbrains/home/oasis-2/
"""  # noqa: E501

KEYS = ("raw", "meta")


@option_group("OASIS-2", 50)
class PartOptions(OptionGroup):
    """
    `--parts`.

    Attributes
    ----------
    parts : tuple[int, ...]
        Parts to harvest or roast (empty: all)
    """

    parts: Annotated[tuple[int, ...], Parameter(
        help="Only these parts (1, 2; default: all). A named part that "
             "is not in sourcedata/ is an error for roast, other missing "
             "parts are skipped", consume_multiple=True,
    )] = ()

    def source_options(self, dataset):
        """
        Return the options of the sources.

        Parameters
        ----------
        dataset : Dataset
            The dataset

        Returns
        -------
        dict
            `{"parts": frozenset}` (empty `--parts`: all)
        """
        return {"parts": frozenset(self.parts or PARTS)}


class OASIS2(Dataset):
    """OASIS-2, from download.nrg.wustl.edu (no login)."""

    name = "oasis2"
    folder = "OASIS-2"
    title = "OASIS-2"
    help = HELP
    keys = KeySpec(None, KEYS, KEYS)
    subject_width = 4
    has_sessions = True         # (roast; harvest: parts hold every session)
    harvest_subjects = False
    template_dir = Path(__file__).parent / "templates"
    requires = {"roast": ("openpyxl",)}
    extra = "oasis2"

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
            Those of the roaster for `roast`, else those of the sources
        """
        if command == "roast":
            return KeySpec(ROAST_ALIASES, ROAST_KEYS, ROAST_KEYS)
        return self.keys

    def options(self, command):
        """
        Return the extra option groups of a command.

        Parameters
        ----------
        command : str
            Name of the command

        Returns
        -------
        dict
            `--parts` (harvest, roast)
        """
        return {"parts": PartOptions} if command in ("harvest", "roast") \
            else {}

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
            The WUSTL parts and demographics
        """
        return {"wustl": WUSTL()}

    def roast_options(self, args):
        """
        Return the options of the roaster.

        Parameters
        ----------
        args : Args
            Values of the command line

        Returns
        -------
        dict
            `{"parts": [...]}` when `--parts` is given (a missing named
            part is then an error), else `{}` (every part; missing ones
            are skipped with a warning)
        """
        parts = args.extra("parts", "parts", ())
        return {"parts": sorted(set(parts))} if parts else {}

    def check(self, command, args):
        """
        Check the options of a command (also `--parts`: 1 or 2).

        Parameters
        ----------
        command : str
            Name of the command
        args : Args
            Values of the command line

        Raises
        ------
        UsageError
            If an option value cannot be used
        """
        super().check(command, args)
        check_parts(args.extra("parts", "parts", ()))
