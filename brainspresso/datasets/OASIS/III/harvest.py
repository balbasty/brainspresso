"""
Harvest OASIS-III from NITRC-IR (XNAT project "OASIS3", login
required): one archive per scan (`<experiment>/<scan>.tar.gz`),
FreeSurfer and PUP assessors (`<experiment>/<assessor>.tar.gz`), and
the data files (`OASIS3_data_files/<scan>.tar.gz`).
"""

from logging import getLogger

from brainspresso.datasets.OASIS.III.roaster import roast_oasis3
from brainspresso.sources.base import (
    numeric_labels,
)
from brainspresso.sources.xnat import XNAT, filter_list

lg = getLogger(__name__)

PROJECT = "OASIS3"
DATA_SUBJECT = "0AS_data_files"
DATA_EXPERIMENT = "OASIS3_data_files"

# experiment type -> keys that need it
EXPERIMENT_KEYS = {
    "MR": ("mri", "fs"),
    "CT": ("ct",),
    "FDG": ("FDG", "pup"),
    "PIB": ("PIB", "pup"),
    "AV45": ("AV45", "pup"),
    "AV1451": ("AV1451", "pup"),
}
# assessor label pattern -> key
ASSESSOR_KEYS = {"*Freesurfer*": "fs", "*PUPTIMECOURSE*": "pup"}


class NitrcIR(XNAT):
    """OASIS-III scans, assessors and data files from NITRC-IR"""

    name = "xnat"
    project = PROJECT
    provides = frozenset({"raw", "derivatives", "meta"})
    labels = numeric_labels(4, "OAS3")
    roaster = staticmethod(roast_oasis3)

    def keep_experiment(self, subject, experiment, wants) -> bool:
        parts = experiment["label"].split("_")
        keys = EXPERIMENT_KEYS.get(parts[1] if len(parts) > 1 else "")
        return keys is not None and any(wants(k) for k in keys)

    def keep_scan(self, subject, experiment, scan, wants) -> bool:
        return wants(scan.get("type", ""))

    def keep_assessor(self, subject, experiment, assessor, wants) -> bool:
        label = assessor.get("label", "")
        return any(wants(key) and filter_list([label], pattern)
                   for pattern, key in ASSESSOR_KEYS.items())

    async def extra(self, client, selection, wants) -> list:
        if not wants("meta"):
            return []
        subject = {"label": DATA_SUBJECT}
        experiment = {"label": DATA_EXPERIMENT}
        scans = await client.scans(self.project, DATA_SUBJECT,
                                   DATA_EXPERIMENT)
        return [self.entry(client, subject, experiment, scan)
                for scan in scans]


def sources() -> dict:
    """Sources of OASIS-III"""
    return {"xnat": NitrcIR()}



