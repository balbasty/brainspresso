"""`bx cobre`: COBRE (schizophrenia)."""
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.indi.cobre.bidsifier import Bidsifier
from brainspresso.datasets.indi.cobre.harvest import sources

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
    legacy_subjects = True
    keys = KeySpec(None, KEYS, KEYS)
    harvest_subjects = False
    credentials = "login"
    download = {"on_error": "raise"}

    def keys_for(self, command):
        """Roast keys (legacy roaster, `Bidsifier` class)."""
        if command == "roast":
            return KeySpec(None, ROAST_KEYS, ROAST_KEYS)
        return self.keys

    def sources(self, args=None):
        """NITRC files."""
        return sources()

    def roast(self, layout, args):
        """Legacy roaster (`Bidsifier` class, rewritten in step 5)."""
        Bidsifier(
            layout.root, keys=self.legacy_keys("roast", args),
            subs=self.numbers(args.select.subs),
            exclude_subs=self.numbers(args.select.exclude_subs),
            json=args.roast.sidecar_policy, layout=layout.kind,
            ifexists=args.roast.if_exists,
        ).run()
