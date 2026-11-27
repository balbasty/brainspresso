"""`bx abide1`: ABIDE-I (autism spectrum disorders)."""
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.indi.abide.i.bidsifier_nitrc import (
    Bidsifier as BidsifierNITRC,
)
from brainspresso.datasets.indi.abide.i.bidsifier_xnat import (
    Bidsifier as BidsifierXNAT,
)
from brainspresso.datasets.indi.abide.i.keys import allkeys
from brainspresso.datasets.indi.abide.i.sources import DEFAULTS, SITES, sources
from brainspresso.errors import UsageError
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
    indi_category = "retro"
    title = "ABIDE-I"
    help = HELP
    legacy_subjects = True
    keys = KeySpec(allkeys, tuple(flatten_keys(allkeys)), ("all",))
    subject_width = 5
    site_names = SITES
    credentials = "login"
    option_defaults = {"roast": {"source": ("s3",)}}

    def keys_for(self, command):
        """Roast keys: the key hierarchy of the legacy roaster."""
        if command == "roast":
            return KeySpec(allkeys)
        return self.keys

    @property
    def defaults(self):
        """s3, then nitrc (derivatives: `--source xnat`)."""
        return DEFAULTS

    def sources(self, args=None):
        """s3://fcp-indi, NITRC tarballs, NITRC-IR."""
        return sources()

    def groups(self, command):
        """Roast: `--source` picks the legacy roaster."""
        groups = super().groups(command)
        if command == "roast" and "source" not in groups:
            i = groups.index("roast")
            groups = (*groups[:i], "source", *groups[i:])
        return groups

    def describe(self, command):
        """Roast: what the legacy roasters filter."""
        if command == "roast":
            return (
                "Convert the source data of ABIDE-I to BIDS (legacy "
                "roasters, `Bidsifier` classes, rewritten in step 5).\n\n"
                "`--source s3` (default) or `nitrc`: the NITRC tarballs or "
                "the bucket copy; `--keys` selects raw data (T1w, rest) and "
                "metadata, derivatives are not converted. `--source xnat`: "
                "the NITRC-IR archives; `--keys` selects raw scans, "
                "metadata and FreeSurfer outputs (`fs`: the files that BIDS "
                "describes, `fs-all`: every file), but FreeSurfer archives "
                "are not found yet (a known bug, fixed in step 5)."
            )
        return super().describe(command)

    def roast(self, layout, args):
        """Legacy roasters (`Bidsifier` classes, step 5): one per source."""
        source = (args.source.source or ("s3",))[0]
        roasters = {"xnat": BidsifierXNAT, "nitrc": BidsifierNITRC,
                      "s3": BidsifierNITRC}
        if source not in roasters:
            raise UsageError(f"Unknown source {source!r}")
        roasters[source](
            layout.root, keys=self.legacy_keys("roast", args),
            exclude_keys=set(args.keys.exclude_keys),
            subs=self.numbers(args.select.subs),
            exclude_subs=self.numbers(args.select.exclude_subs),
            json=args.roast.sidecar_policy, layout=layout.kind,
            ifexists=args.roast.if_exists,
        ).run()
