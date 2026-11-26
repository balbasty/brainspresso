"""`bx adhd200`: ADHD-200."""
from typing import Annotated

from cyclopts import Parameter

from brainspresso.datasets.ADHD200.harvest import S3
from brainspresso.datasets.ADHD200.keys import allkeys
from brainspresso.datasets.base import Dataset, KeySpec
from brainspresso.datasets.options import (
    OptionGroup,
    deprecated,
    option_group,
)

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
class BidsCopyOptions(OptionGroup):
    """`--bids/--no-bids` (deprecated alias: `--bidsify/--no-bidsify`)."""

    bids: Annotated[bool, Parameter(
        help="Place the files of the BIDS-like copy of the bucket in the "
        "raw BIDS dataset (else in `sourcedata/`)"
    )] = True
    bidsify: Annotated[bool | None, Parameter(
        show=False, help="Deprecated: use `--bids/--no-bids`"
    )] = None

    def source_options(self, dataset):
        """`bidsify` (the option of the S3 source)."""
        value = self.bids
        if self.bidsify is not None:
            deprecated("--bidsify/--no-bidsify", "--bids/--no-bids")
            value = self.bidsify
        return {"bidsify": value}


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
        """`--bids` (harvest)."""
        return {"bids": BidsCopyOptions} if command == "harvest" else {}

    def sources(self, args=None):
        """s3://fcp-indi."""
        return {"s3": S3()}
