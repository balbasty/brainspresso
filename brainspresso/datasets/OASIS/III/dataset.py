"""`bx oasis3`: OASIS-3 (longitudinal, PET, dementia)."""
from pathlib import Path

from brainspresso.datasets.base import Dataset, KeySpec

HELP = """
OASIS-3 dataset (longitudinal, pet, dementia)

* **Project**       Open Access Series of Imaging Studies (OASIS)
* **Subproject**    Longitudinal Multimodal Neuroimaging, Clinical,
                    and Cognitive Dataset for Normal Aging and
                    Alzheimer's Disease (OASIS-3)
* **Modalities**    T1w, T2w, TSE, FLAIR, T2star, angio, pasl, asl, bold,
                    dwi, swi, fdg, pib, av45, av1451
* **Populations**   Controls, Dementia
* **Funding**       NIH: P30 AG066444, P50 AG00561,  P30 NS09857781,
                         P01 AG026276, P01 AG003991, R01 AG043434,
                         UL1 TR000448, R01 EB009352
* **Reference**     https://doi.org/10.1101/2019.12.13.19014902
* **URL**           https://sites.wustl.edu/oasisbrains/home/oasis-3/
"""  # noqa: E501


def roast_keys():
    """Keys of the roast."""
    from brainspresso.datasets.OASIS.III.roaster import KEYS
    return KEYS


class OASIS3(Dataset):
    """OASIS-3, from NITRC-IR (login required)."""

    name = "oasis3"
    folder = "OASIS-3"
    title = "OASIS-3"
    help = HELP
    keys = KeySpec()
    subject_width = 4
    credentials = "login"
    harvest_jobs = 1
    download = {"path": "full"}
    template_dir = Path(__file__).parent / "templates"

    def keys_for(self, command):
        """Roast keys: those of the roaster."""
        if command == "roast":
            return KeySpec(roast_keys)
        return self.keys

    def wants(self, command, keys=(), exclude_keys=()):
        """Harvest keys over the generic key tree (all OASIS-3 keys)."""
        from brainspresso.datasets.OASIS.III.keys import allkeys
        from brainspresso.sources.base import Wants
        from brainspresso.utils.keys import allkeys as generic_keys
        from brainspresso.utils.keys import flatten_keys
        keys = list(keys or flatten_keys(allkeys))
        return Wants(keys, generic_keys, exclude=tuple(exclude_keys))

    def sources(self, args=None):
        """NITRC-IR (XNAT project OASIS3)."""
        from brainspresso.datasets.OASIS.III.harvest import sources
        return sources()
