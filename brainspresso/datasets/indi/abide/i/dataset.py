"""`bx abide1`: ABIDE-I (autism spectrum disorders)."""
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.indi.abide.i.keys import allkeys
from brainspresso.datasets.indi.abide.i.roaster import (
    FS_ADD_ON,
    ROAST_SPEC,
    roast_options,
)
from brainspresso.datasets.indi.abide.i.sites import SITES, TEMPLATES
from brainspresso.datasets.indi.abide.i.sources import DEFAULTS, sources
from brainspresso.utils.keys import flatten_keys

HELP = """
ABIDE-I dataset (autism spectrum disorders)

* **Project**       Autism Brain Imaging Data Exchange (ABIDE)
* **Subproject**    ABIDE I (original 17 sites)
* **Modalities**    T1w, rest
* **Populations**   Controls, Autism Spectrum Disorders
* **License**       CC BY-NC-SA
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
    keys = KeySpec(allkeys, tuple(flatten_keys(allkeys)), ("all",),
                   implies=FS_ADD_ON)
    subject_width = 5
    site_names = SITES
    credentials = "login"
    template_dir = TEMPLATES
    option_defaults = {"roast": {"source": ("s3",)}}

    def keys_for(self, command):
        """Roast keys: those of the roasters (the same hierarchy)."""
        if command == "roast":
            return ROAST_SPEC
        return self.keys

    def roast_options(self, args):
        """Options of the roasters (`roaster.roast_options`)."""
        return roast_options(args.keys.keys)

    @property
    def defaults(self):
        """s3, then nitrc (derivatives: `--source xnat`)."""
        return DEFAULTS

    def sources(self, args=None):
        """s3://fcp-indi, NITRC tarballs, NITRC-IR (and their roasters)."""
        return sources()

    def describe(self, command):
        """Roast: what the roaster of each source converts."""
        if command == "roast":
            return (
                "Convert the source data of ABIDE-I to BIDS.\n\n"
                "`--source s3` (default) or `nitrc`: the bucket copy or "
                "the NITRC tarballs (T1w, rest, phenotypes; the UCLA "
                "high-resolution anatomical is `acq-hires_T2w`, key "
                "`T2w`). `--source xnat`: the NITRC-IR archives (the same, "
                "and "
                "FreeSurfer outputs: `fs`, the files that BIDS describes; "
                "`fs-all`, an add-on of `fs` not selected by default "
                "(only when it or a group above it is named), "
                "also unpacks every other file, and `--exclude-keys fs` "
                "excludes both); other derivatives are not converted. A "
                "missing FreeSurfer archive is an error only when "
                "`--keys` names `fs`, `fs-all` or a group above them. "
                "`participants.tsv` lists the subjects whose "
                "folder is on disk (all of them are in the pristine copy, "
                "`.brainspresso/orig/`)."
            )
        return super().describe(command)
