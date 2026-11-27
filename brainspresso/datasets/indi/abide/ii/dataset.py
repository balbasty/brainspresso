"""`bx abide2`: ABIDE-II (autism spectrum disorders)."""
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.indi.abide.ii.harvest import DEFAULTS, sources
from brainspresso.datasets.indi.abide.ii.roaster import ROAST_SPEC
from brainspresso.datasets.indi.abide.ii.sites import SITES, TEMPLATES

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


class ABIDE2(Dataset):
    """ABIDE-II: NITRC tarballs (login); `--source s3` for raw data."""

    name = "abide2"
    folder = "ABIDE-2"
    indi_category = "retro"
    title = "ABIDE-II"
    help = HELP
    keys = KeySpec(None, KEYS, KEYS)
    subject_width = 5
    harvest_subjects = False
    site_names = SITES
    credentials = "login"
    harvest_jobs = 1
    template_dir = TEMPLATES

    def keys_for(self, command):
        """Roast keys: those of the roaster (`roaster.KEYS`)."""
        if command == "roast":
            return ROAST_SPEC
        return self.keys

    @property
    def defaults(self):
        """NITRC."""
        return DEFAULTS

    def sources(self, args=None):
        """NITRC tarballs; s3://fcp-indi (raw data, BIDS-like copy)."""
        return sources()

    def describe(self, command):
        """Roast: what the roaster converts."""
        if command == "roast":
            return (
                "Convert the source data of ABIDE-II to BIDS.\n\n"
                "The NITRC tarballs (`--source nitrc`, and those that "
                "`--source s3` fetches for the sites the bucket lacks: "
                "SU, U_MIA): T1w, rest and DWI (`dti`) of every session "
                "(`ses-01`, `ses-02`), a second scan of a kind as "
                "`run-<n>`; the phenotypes. The bucket copy of `--source "
                "s3` (`sourcedata/RawData/`, BIDS-like) is not roasted. "
                "`participants.tsv` and the phenotype tables list the "
                "subjects whose folder is on disk (all of them are in the "
                "pristine copies, `.brainspresso/orig/`)."
            )
        return super().describe(command)
