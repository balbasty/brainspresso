"""
`bx oasis3 roast`: see `OASIS/III/roaster.py` for the expected input and
output.
"""
from pathlib import Path
from typing import Literal, Iterable

from brainspresso.bids.layout import LayoutChoice
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.actions.action import IfExists
from brainspresso.datasets.OASIS.III.command import oasis3
from brainspresso.datasets.OASIS.III.roaster import roast


DATASET = 'OASIS-3'


@oasis3.command(name="roast")
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[str] = tuple(),
    exclude_keys: Iterable[str] | None = tuple(),
    subs: Iterable[int] | None = tuple(),
    exclude_subs: Iterable[int] | None = tuple(),
    json: Literal["yes", "no", "only"] | bool = "yes",
    if_exists: IfExists.Choice = "skip",
    layout: LayoutChoice | None = None,
    jobs: int = 1,
    log: str | None = None,
):
    """
    Convert source data into a BIDS-compliant directory

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
        * fs-all :        Freesurfer derivatives (even non bidsifiable ones)
        * pup :           PET derivatives
    * meta :             All metadata
        * pheno :         Phenotypes

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-3/sourcedata` folder must
        exist.
    keys : [list of] str
        Only bidsify these keys (all if empty)
    exclude_keys : [list of] str
        Do not bidsify these keys
    subs : [list of] int
        Only bidsify these subjects (all if empty)
    exclude_subs : [list of] int
        Do not bidsify these subjects
    json : {"yes", "no", "only"} | bool
        Whether to write (only) sidecar JSON files
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour when a file already exists
    layout : {"study", "flat"}
        Layout of the dataset: "study" (raw dataset in `rawbids/`)
        or "flat" (raw dataset at the root). Default: the layout of
        an existing dataset, else "study".
    jobs : int
        Number of processes (subjects are converted in parallel)
    log : str
        Path to log file
    """
    setup_filelog(log)
    roast(              # (one value or a list: see `runner.as_list`)
        get_tree_path(path) / DATASET,
        keys=keys,
        exclude_keys=exclude_keys,
        subs=subs,
        exclude_subs=exclude_subs,
        json=json,
        layout=layout,
        ifexists=if_exists,
        jobs=jobs,
    )
