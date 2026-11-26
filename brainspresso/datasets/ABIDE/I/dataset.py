"""`bx abide1`: ABIDE-I (autism spectrum disorders)."""
from brainspresso.datasets.ABIDE.I.keys import allkeys
from brainspresso.datasets.ABIDE.I.sources import SITES
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.utils.keys import flatten_keys

HELP = """
ABIDE-I dataset (autism spectrum disorders)

* **Project**       Autism Brain Imaging Data Exchange (ABIDE)
* **Subproject**    ABIDE I (original 17 sites)
* **Modalities**    T1w, rest
* **Populations**   Controls, Autism Spectrum Disorders
* **License**       CC BY-NC
* **Funding**       NIH: K23 MH087770, R03 MH096321, and more.
* **URL**           https://fcon_1000.projects.nitrc.org/indi/abide/abide_I.html
"""  # noqa: E501



class ABIDE1(Dataset):
    """ABIDE-I: s3://fcp-indi, NITRC tarballs, NITRC-IR (derivatives)."""

    name = "abide1"
    folder = "ABIDE-1"
    title = "ABIDE-I"
    help = HELP
    keys = KeySpec(allkeys, tuple(flatten_keys(allkeys)), ("all",))
    subject_width = 5
    site_names = SITES
    credentials = "login"
    option_defaults = {"roast": {"source": ("s3",)}}

    def keys_for(self, command):
        """Roast keys: those of the legacy bidsifiers (any text)."""
        if command == "roast":
            return KeySpec()
        return self.keys

    @property
    def defaults(self):
        """s3, then nitrc (derivatives: `--source xnat`)."""
        from brainspresso.datasets.ABIDE.I.sources import DEFAULTS
        return DEFAULTS

    def sources(self, args=None):
        """s3://fcp-indi, NITRC tarballs, NITRC-IR."""
        from brainspresso.datasets.ABIDE.I.sources import sources
        return sources()

    def groups(self, command):
        """Roast: `--source` picks the bidsifier."""
        groups = super().groups(command)
        if command == "roast" and "source" not in groups:
            i = groups.index("roast")
            groups = (*groups[:i], "source", *groups[i:])
        return groups

    def roast(self, layout, args):
        """Legacy bidsifiers (ported in step 5): one per source."""
        from brainspresso.datasets.ABIDE.I.bidsifier_nitrc import (
            Bidsifier as BidsifierNITRC,
        )
        from brainspresso.datasets.ABIDE.I.bidsifier_xnat import (
            Bidsifier as BidsifierXNAT,
        )
        from brainspresso.datasets.ABIDE.I.keys import allleaves
        from brainspresso.errors import UsageError
        source = (args.source.source or ("s3",))[0]
        bidsifiers = {"xnat": BidsifierXNAT, "nitrc": BidsifierNITRC,
                      "s3": BidsifierNITRC}
        if source not in bidsifiers:
            raise UsageError(f"Unknown source {source!r}")
        bidsifiers[source](
            layout.root, keys=set(args.keys.keys or allleaves),
            exclude_keys=set(args.keys.exclude_keys),
            subs=self.numbers(args.select.subs),
            exclude_subs=self.numbers(args.select.exclude_subs),
            json=args.roast.json_policy, layout=layout.kind,
            ifexists=args.roast.if_exists,
        ).run()
