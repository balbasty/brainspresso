"""`bx gsp`: Brain Genomics Superstruct Project."""
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.gsp.harvest import sources

HELP = """
GSP dataset (controls, genomics)

* **Project**       Brain Genomics Superstruct Project (CoRR)
* **Modalities**    T1w, rest
* **Populations**   Controls
* **URL**           https://doi.org/10.7910/DVN/25833
"""  # noqa: E501

KEYS = ("raw", "meta")


class GSP(Dataset):
    """GSP, from Harvard Dataverse (token required)."""

    name = "gsp"
    folder = "GSP"
    title = "GSP"
    help = HELP
    keys = KeySpec(None, KEYS, KEYS)
    harvest_subjects = False
    credentials = "token"
    harvest_jobs = 1

    def sources(self, args=None):
        """Harvard Dataverse."""
        return sources()
