"""`bx oasis1`: OASIS-1 (cross-sectional, adult lifespan, dementia)."""
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.oasis.i.harvest import WUSTL
from brainspresso.datasets.oasis.i.roaster import KEYS as ROAST_ALIASES
from brainspresso.datasets.oasis.i.roaster import ROAST_KEYS, check_discs
from brainspresso.datasets.options import OptionGroup, option_group

HELP = """
OASIS-1 dataset (adult lifespan, dementia)

* **Project**       Open Access Series of Imaging Studies (OASIS)
* **Subproject**    Cross-sectional data across the adult lifespan (OASIS-1)
* **Modalities**    T1w
* **Populations**   Controls, Dementia
* **Funding**       NIH: P50 AG05681, P01 AG03991, P01 AG026276,
                    R01 AG021910, P20 MH071616, U24 RR021382
* **Reference**     https://doi.org/10.1162/jocn.2007.19.9.1498
* **URL**           https://sites.wustl.edu/oasisbrains/home/oasis-1/
"""  # noqa: E501

KEYS = ("raw", "fs", "meta", "reliability", "facts")
DISCS = tuple(range(1, 13))


@option_group("OASIS-1", 50)
class DiscOptions(OptionGroup):
    """
    `--discs`.

    Attributes
    ----------
    discs : tuple[int, ...]
        Discs to harvest or roast (empty: all)
    """

    discs: Annotated[tuple[int, ...], Parameter(
        help="Only these discs (1 to 12; default: all). A named disc "
             "that is not in sourcedata/ is an error for roast, other "
             "missing discs are skipped", consume_multiple=True,
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
            `{"discs": frozenset}` (empty `--discs`: all)
        """
        return {"discs": frozenset(self.discs or DISCS)}


class OASIS1(Dataset):
    """OASIS-1, from download.nrg.wustl.edu (no login)."""

    name = "oasis1"
    folder = "OASIS-1"
    title = "OASIS-1"
    help = HELP
    keys = KeySpec(None, KEYS, KEYS)
    subject_width = 4
    harvest_subjects = False
    template_dir = Path(__file__).parent / "templates"
    requires = {"roast": ("openpyxl",)}
    extra = "oasis1"

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
            `--discs` (harvest, roast)
        """
        return {"discs": DiscOptions} if command in ("harvest", "roast") \
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
            The WUSTL discs and spreadsheets
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
            `{"discs": [...]}` when `--discs` is given (a missing named
            disc is then an error), else `{}` (every disc; missing ones
            are skipped with a warning)
        """
        discs = args.extra("discs", "discs", ())
        return {"discs": sorted(set(discs))} if discs else {}

    def check(self, command, args):
        """
        Check the options of a command (also `--discs`: 1 to 12).

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
        check_discs(args.extra("discs", "discs", ()))
