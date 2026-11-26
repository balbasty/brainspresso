"""`bx bullitt`: Designed Database of MR Brain Images (Bullitt, UNC)."""
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.Bullitt.bidsifier import Bidsifier
from brainspresso.datasets.Bullitt.harvest import Girder
from brainspresso.datasets.Bullitt.keys import allkeys

HELP = """
Designed Database of MR Brain Images of Healthy Volunteers (Bullitt, UNC)

* **Modalities**    T1w (FLASH, MPRAGE), T2w, MRA (DTI: harvest only)
* **Populations**   100+ healthy adults, 18 to 72 years
* **Host**          https://data.kitware.com (Girder), no login needed
"""

ROAST_KEYS = ("meta", "flash", "mprage", "T2w", "angio", "skullstripped",
              "vessels")


class Bullitt(Dataset):
    """Designed Database of MR Brain Images of Healthy Volunteers."""

    name = "bullitt"
    folder = "Bullitt"
    title = "the Bullitt database"
    help = HELP
    keys = KeySpec(allkeys, None, ("raw", "meta", "derivatives"))
    subject_width = 3
    credentials = "token"

    def keys_for(self, command):
        """Roast keys (legacy bidsifier): raw scans, meta, derivatives."""
        if command == "roast":
            return KeySpec(None, ROAST_KEYS, ROAST_KEYS)
        return self.keys

    def sources(self, args=None):
        """data.kitware.com (Girder)."""
        return {"girder": Girder()}

    def describe(self, command):
        """Roast: MetaImage to NIfTI (DTI is not converted)."""
        if command == "roast":
            return ("Convert the harvested MetaImage files to BIDS (NIfTI)."
                    "\n\nDTI is not converted: the source files have no "
                    "gradient table. Keys: raw data flash, mprage (T1w), "
                    "T2w, angio; metadata: meta; derivatives (-> "
                    "derivatives/bullitt): skullstripped, vessels.")
        return super().describe(command)

    def roast(self, layout, args):
        """Legacy bidsifier (ported in step 5)."""
        Bidsifier(
            layout.root, keys=self.legacy_keys("roast", args),
            subs=self.labels(args.select.subs),
            exclude_subs=self.labels(args.select.exclude_subs),
            json=args.roast.json_policy, layout=layout.kind,
            ifexists=args.roast.if_exists,
        ).run()
