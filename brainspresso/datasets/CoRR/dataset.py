"""`bx corr`: Consortium for Reliability and Reproducibility."""
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.CoRR.harvest import DEFAULTS, sources

HELP = """
CoRR dataset (controls)

* **Project**       Consortium for Reliability and Reproducibility (CoRR)
* **Modalities**    T1w, rest, DWI, CBF, ASL
* **Populations**   Controls
* **URL**           https://fcon_1000.projects.nitrc.org/indi/CoRR/html/index.html
"""  # noqa: E501

KEYS = ("raw", "meta")
SITES = (
    'BMB', 'BNU', 'DC', 'HNU', 'IACAS', 'IBA', 'IPCAS', 'JHNU', 'LMU', 'MPG',
    'MRN', 'NKI', 'NYU', 'SWU', 'UM', 'UPSM', 'Utah', 'UWM', 'XHCUMS',
)


class CoRR(Dataset):
    """CoRR: s3://fcp-indi, else NITRC tarballs (login)."""

    name = "corr"
    folder = "CoRR"
    title = "CoRR"
    help = HELP
    keys = KeySpec(None, KEYS, KEYS)
    subject_width = 0           # host `0003001` <-> BIDS `3001`
    site_names = SITES
    credentials = "login"

    @property
    def defaults(self):
        """s3, then nitrc."""
        return DEFAULTS

    def sources(self, args=None):
        """s3://fcp-indi, NITRC tarballs."""
        return sources()
