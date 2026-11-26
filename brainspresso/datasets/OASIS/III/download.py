"""
Harvest OASIS-III from NITRC-IR (XNAT project "OASIS3", login
required): one archive per scan (`<experiment>/<scan>.tar.gz`),
FreeSurfer and PUP assessors (`<experiment>/<assessor>.tar.gz`), and
the data files (`OASIS3_data_files/<scan>.tar.gz`).
"""
from pathlib import Path
from typing import Iterable
from humanize import naturalsize

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.utils.ui import human2bytes
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.keys import allkeys as generic_keys
from brainspresso.utils.keys import flatten_keys
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.sources.base import (
    DownloadOptions, Wants, numeric_labels, subject_selection,
)
from brainspresso.sources.xnat import XNAT, filter_list
from brainspresso.datasets.OASIS.III.command import oasis3
from brainspresso.datasets.OASIS.III.keys import allkeys

from logging import getLogger
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


def roast_oasis3(ctx):
    """Roaster of what `NitrcIR` writes"""
    from brainspresso.datasets.OASIS.III.roaster import roast_oasis3
    return roast_oasis3(ctx)


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



@oasis3.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[str] = tuple(),
    subs: Iterable[int | str] | None = tuple(),
    exclude_subs: Iterable[int | str] | None = tuple(),
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    layout: LayoutChoice | None = None,
    log: str | None = None,
):
    """
    Download source data for the OASIS-III dataset.

    **Hierarchy of keys:**

    * raw :              All the raw imaging data
        * mri :           All the MRI data
            * anat :       All the anatomical MRI data
                * T1w :     T1-weighted MRI scans
                * T2w :     T2-weighted MRI scans
                * TSE :     Turbo Spin Echo MRI scans
                * FLAIR :   Fluid-inversion Recovery MRI scans
                * T2star :  T2-star quantitative scans
                * angio :   MR angiography scans
                * swi :     All susceptibility-weighted MRI data
            * func :       All fthe functional MRI data
                * pasl :    Pulsed arterial spin labeling
                * asl :     Arterial spin labelling
                * bold :    Blood-oxygenation level dependent (fMRI) scans
            * fmap :       All field maps
            * dwi :        All diffusion-weighted MRI data
        * pet :           All the PET data
            * fdg :        Fludeoxyglucose
            * pib :        Pittsburgh Compound B (amyloid)
            * av45 :       18F Florpiramine (tau)
            * av1451 :     18F Flortaucipir (tau)
        * ct              All the CT data
    * derivatives :      All derivatives
        * fs :            Freesurfer derivatives
        * pup :           PET derivatives
    * meta :             All metadata
        * pheno :         Phenotypes


    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-3` folder will be created.
    keys : [list of] str
        Data categories to download
    subs : [list of] int
        Only bidsify these subjects (all if empty)
    exclude_subs : [list of] int
        Do not bidsify these subjects
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    user : str
        NITRC username
    password : str
        NITRC password
    packet : int
        Packet size to download, in bytes
    jobs : int
        Number of parallel downloaders
    layout : {"study", "flat"}
        Layout of the dataset (where source data does not move, but
        later commands reuse it): "study" (raw dataset in
        `rawbids/`) or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log : str
        Path to log file

    """
    setup_filelog(log)
    path = Path(get_tree_path(path))
    if isinstance(keys, str):
        keys = [keys]
    keys = set(keys or flatten_keys(allkeys))
    dataset = resolve_layout(path / 'OASIS-3', layout)
    record_run(dataset, "harvest", {"dataset": "OASIS-3"})
    source = sources()["xnat"].configure(user=user, password=password)
    source.harvest(
        dataset, subject_selection(subs, exclude_subs, width=4),
        Wants(keys, generic_keys),
        DownloadOptions(ifexists=if_exists, jobs=jobs, path="full",
                        chunk_size=human2bytes(packet)),
    )
