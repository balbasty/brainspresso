"""`bx adhd200`: ADHD-200."""
from typing import Annotated

from cyclopts import Parameter

from brainspresso.datasets.ADHD200.keys import allkeys
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.options import OptionGroup, option_group

HELP = """
ADHD-200 dataset (attention deficit hyperactivity disorder)

* **Project**       ADHD-200
* **Modalities**    T1w, rest
* **Populations**   ADHD
* **License**       CC BY-NC
* **Funding**       See dataset.json
* **URL**           https://fcon_1000.projects.nitrc.org/indi/adhd200/
"""


@option_group("ADHD-200", 50)
class BidsifyOptions(OptionGroup):
    """`--bidsify`."""

    bidsify: Annotated[bool, Parameter(
        help="Place the files of the BIDS-like copy in the raw BIDS "
        "dataset (else in `sourcedata/`)"
    )] = True

    def source_options(self, dataset):
        """`bidsify`."""
        return {"bidsify": self.bidsify}


class ADHD200(Dataset):
    """ADHD-200, from s3://fcp-indi."""

    name = "adhd200"
    folder = "ADHD-200"
    title = "ADHD-200"
    help = HELP
    keys = KeySpec(allkeys, None, ("all",))
    subject_width = 7
    has_sites = True
    download = {"path": "full", "on_error": "raise"}

    def options(self, command):
        """`--bidsify` (harvest)."""
        return {"bidsify": BidsifyOptions} if command == "harvest" else {}

    def sources(self, args=None):
        """s3://fcp-indi."""
        from brainspresso.datasets.ADHD200.harvest import S3
        return {"s3": S3()}
