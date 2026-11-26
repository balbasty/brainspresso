"""`bx oasis1`: OASIS-1 (cross-sectional, adult lifespan, dementia)."""
from typing import Annotated

from cyclopts import Parameter

from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.OASIS.I.harvest import WUSTL
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
ROAST_KEYS = ("raw", "fs", "fs-all", "fsl", "avg", "tal", "meta")
DISCS = tuple(range(1, 13))


@option_group("OASIS-1", 50)
class DiscOptions(OptionGroup):
    """`--discs`."""

    discs: Annotated[tuple[int, ...], Parameter(
        help="Only these discs (1 to 12)", consume_multiple=True,
    )] = DISCS

    def source_options(self, dataset):
        """`discs` (empty: all)."""
        return {"discs": frozenset(self.discs or DISCS)}


class OASIS1(Dataset):
    """OASIS-1, from download.nrg.wustl.edu (no login)."""

    name = "oasis1"
    folder = "OASIS-1"
    title = "OASIS-1"
    help = HELP
    legacy_subjects = True
    keys = KeySpec(None, KEYS, KEYS)
    harvest_subjects = False
    requires = {"roast": ("openpyxl",)}
    extra = "oasis1"

    def keys_for(self, command):
        """Roast keys (legacy roaster, `Bidsifier` class)."""
        if command == "roast":
            return KeySpec(None, ROAST_KEYS, ROAST_KEYS)
        return self.keys

    def options(self, command):
        """`--discs` (harvest, roast)."""
        return {"discs": DiscOptions} if command in ("harvest", "roast") \
            else {}

    def sources(self, args=None):
        """WUSTL discs and spreadsheets."""
        return {"wustl": WUSTL()}

    def roast(self, layout, args):
        """Legacy roaster (`Bidsifier` class, rewritten in step 5)."""
        from brainspresso.datasets.OASIS.I.bidsifier import (  # noqa: PLC0415
            Bidsifier,  # lazy: optional dependency (openpyxl)
        )
        Bidsifier(
            layout.root, keys=self.legacy_keys("roast", args),
            discs=list(args.extra("discs", "discs", ())),
            subs=self.numbers(args.select.subs),
            exclude_subs=self.numbers(args.select.exclude_subs),
            json=args.roast.json_policy, layout=layout.kind,
            ifexists=args.roast.if_exists,
        ).run()
