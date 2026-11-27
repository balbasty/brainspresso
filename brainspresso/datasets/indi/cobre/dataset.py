"""`bx cobre`: COBRE (schizophrenia)."""
from pathlib import Path

from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.indi.cobre.harvest import sources
from brainspresso.datasets.indi.cobre.roaster import KEYS as ROAST_ALIASES

HELP = """
COBRE dataset (schizophrenia)

* **Project**       Center for Biomedical Research Excellence (COBRE)
* **Modalities**    T1w, rest
* **Populations**   Controls, Schizophrenia
* **License**       CC BY-NC
* **Funding**       NIH 1P20 RR021938-01A2
* **URL**           https://fcon_1000.projects.nitrc.org/indi/retro/cobre.html
"""  # noqa: E501

KEYS = ("raw", "meta")
ROAST_KEYS = ("meta", "T1w", "func")


class COBRE(Dataset):
    """COBRE, from NITRC (login required)."""

    name = "cobre"
    folder = "COBRE"
    indi_category = "retro"
    title = "COBRE"
    help = HELP
    keys = KeySpec(None, KEYS, KEYS)
    subject_width = 7
    harvest_subjects = False
    credentials = "login"
    download = {"on_error": "raise"}
    template_dir = Path(__file__).parent / "templates"

    def keys_for(self, command):
        """Roast keys: those of the roaster."""
        if command == "roast":
            return KeySpec(ROAST_ALIASES, ROAST_KEYS, ROAST_KEYS)
        return self.keys

    def sources(self, args=None):
        """NITRC files (roasted by `roaster.roast_cobre`)."""
        return sources()

    def describe(self, command):
        """Roast: how the participants table is trimmed."""
        if command == "roast":
            return (
                "Convert the source data of COBRE to BIDS.\n\n"
                "`participants.tsv` lists the subjects whose folder is on "
                "disk (all of them are in the pristine copy, "
                "`.brainspresso/orig/`): a roast of `--keys meta` alone, "
                "before any image, writes its header only."
            )
        return super().describe(command)
