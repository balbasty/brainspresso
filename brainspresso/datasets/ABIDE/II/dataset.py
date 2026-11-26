"""`bx abide2`: ABIDE-II (autism spectrum disorders)."""
from brainspresso.datasets.base import Dataset, KeySpec

HELP = """
ABIDE-II dataset (autism spectrum disorders)

* **Project**       Autism Brain Imaging Data Exchange (ABIDE)
* **Subproject**    ABIDE II (additional 19 sites)
* **Modalities**    T1w, rest, (DWI, FLAIR)
* **Populations**   Controls, Autism Spectrum Disorders
* **License**       CC BY-NC
* **Funding**       NIH: R21 MH107045, and more.
* **URL**           https://fcon_1000.projects.nitrc.org/indi/abide/abide_II.html
"""  # noqa: E501

KEYS = ("raw", "meta")
SITES = (
    'BNI', 'EMC', 'ETH', 'GU', 'IU', 'IP', 'KUL', 'KKI', 'NYU', 'ONRC',
    'OHSU', 'TCD', 'SDSU', 'SU', 'UCD', 'UCLA', 'U_MIA', 'USM', 'UPSM'
)


class ABIDE2(Dataset):
    """ABIDE-II: NITRC tarballs (login); `--source s3` for raw data."""

    name = "abide2"
    folder = "ABIDE-2"
    title = "ABIDE-II"
    help = HELP
    legacy_subjects = True
    keys = KeySpec(None, KEYS, KEYS)
    harvest_subjects = False
    site_names = SITES
    credentials = "login"
    harvest_jobs = 1

    def keys_for(self, command):
        """Roast keys: the key hierarchy of the legacy bidsifier."""
        if command == "roast":
            from brainspresso.datasets.ABIDE.II.keys import allkeys
            return KeySpec(allkeys)
        return self.keys

    @property
    def defaults(self):
        """NITRC."""
        from brainspresso.datasets.ABIDE.II.harvest import DEFAULTS
        return DEFAULTS

    def sources(self, args=None):
        """NITRC tarballs; s3://fcp-indi (raw data, BIDS-like copy)."""
        from brainspresso.datasets.ABIDE.II.harvest import sources
        return sources()

    def roast(self, layout, args):
        """Legacy bidsifier (ported in step 5)."""
        from brainspresso.datasets.ABIDE.II.bidsifier import Bidsifier
        from brainspresso.datasets.ABIDE.II.keys import allleaves
        Bidsifier(
            layout.root, keys=set(args.keys.keys or allleaves),
            exclude_keys=set(args.keys.exclude_keys),
            subs=self.numbers(args.select.subs),
            exclude_subs=self.numbers(args.select.exclude_subs),
            json=args.roast.json_policy, layout=layout.kind,
            ifexists=args.roast.if_exists,
        ).run()
