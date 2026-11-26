"""`bx oasis2`: OASIS-2 (longitudinal, dementia)."""
from typing import Annotated

from cyclopts import Parameter

from brainspresso.datasets.base import Dataset, KeySpec
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
PARTS = (1, 2)


@option_group("OASIS-2", 50)
class PartOptions(OptionGroup):
    """`--parts`."""

    parts: Annotated[tuple[int, ...], Parameter(
        help="Only these parts (1, 2)", consume_multiple=True,
    )] = PARTS

    def source_options(self, dataset):
        """`parts` (empty: all)."""
        return {"parts": frozenset(self.parts or PARTS)}


class OASIS2(Dataset):
    """OASIS-2, from download.nrg.wustl.edu (no login)."""

    name = "oasis2"
    folder = "OASIS-2"
    title = "OASIS-2"
    help = HELP
    legacy_subjects = True
    keys = KeySpec(None, KEYS, KEYS)
    harvest_subjects = False
    requires = {"roast": ("openpyxl",)}
    extra = "oasis2"

    def options(self, command):
        """`--parts` (harvest, roast)."""
        return {"parts": PartOptions} if command in ("harvest", "roast") \
            else {}

    def sources(self, args=None):
        """WUSTL parts and demographics."""
        from brainspresso.datasets.OASIS.II.harvest import WUSTL
        return {"wustl": WUSTL()}

    def roast(self, layout, args):
        """Legacy bidsifier (ported in step 5)."""
        from brainspresso.datasets.OASIS.II.bidsifier import Bidsifier
        Bidsifier(
            layout.root, keys=self.legacy_keys("roast", args),
            parts=list(args.extra("parts", "parts", ())),
            subs=self.numbers(args.select.subs),
            exclude_subs=self.numbers(args.select.exclude_subs),
            json=args.roast.json_policy, layout=layout.kind,
            ifexists=args.roast.if_exists,
        ).run()
