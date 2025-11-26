"""
Expected input
--------------
ABIDE-2/
    sourcedata/
        ABIDEII-{SITE}_{SAMPLE}.csv
        ABIDEII-{SITE}_{SAMPLE}.tar.gz/
            ABIDEII-{SITE}_{SAMPLE}/{id}/session_{ses}/
                anat_{ses}/anat.nii.gz
                rest_{ses}/rest.nii.gz
                dti_{ses}/dti.nii.gz
                dti_{ses}/dti.bval
                dti_{ses}/dti.bvec

Expected output
---------------
ABIDE-2/
    dataset_description.json
    participants.{tsv|json}
    phenotype/
        adir.{tsv|json}
        ados.{tsv|json}
        ados_gotham.{tsv|json}
        srs.{tsv|json}
        vineland.{tsv|json}
        wisc_iv.{tsv|json}
    rawdata/
        sub-{:05d}/
            ses-{:02d}/
                anat/
                    sub-{:05d}_ses-{:02d}_T1w.{nii.gz|json}
                func/
                    sub-{:05d}_ses-{:02d}_task-rest_run-{:02d}_bold.nii.gz
                dwi/
                    sub-{:05d}_ses-{:02d}_dwi.{nii.gz|json|bval|bvec}
"""
from pathlib import Path
from typing import Literal, Iterable

from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.actions.action import IfExists
from brainspresso.datasets.ABIDE.II.command import abide2
from brainspresso.datasets.ABIDE.II.bidsifier import Bidsifier
from brainspresso.datasets.ABIDE.II.keys import allleaves


DATASET = 'ABIDE-2'


@abide2.command(name="roast")
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[str] = tuple(),
    exclude_keys: Iterable[str] | None = tuple(),
    subs: Iterable[int] | None = tuple(),
    exclude_subs: Iterable[int] | None = tuple(),
    json: Literal["yes", "no", "only"] | bool = "yes",
    if_exists: IfExists.Choice = "skip",
    log: str | None = None,
):
    """
    Convert source data into a BIDS-compliant directory

    **Hierarchy of keys:**

    * all
      * meta               Metadata
      * raw                All raw data
        * anat             Anatomical T1w scans
        * rest             Resting-state fMRI
        * dwi              Diffusion-weighted MRI

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `ABIDE-1/sourcedata` folder must
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
    log : str
        Path to log file
    """
    setup_filelog(log)

    # Format keys
    if isinstance(keys, str):
        keys = [keys]
    keys = set(keys or allleaves)

    if isinstance(exclude_keys, str):
        exclude_keys = [exclude_keys]
    exclude_keys = set(exclude_keys)

    # Format subjects
    if isinstance(subs, int):
        subs = [subs]
    subs = list(subs or [])

    if isinstance(exclude_subs, int):
        exclude_subs = [exclude_subs]
    exclude_subs = list(exclude_subs or [])

    # Format json
    if isinstance(json, bool):
        json = 'yes' if json else 'no'
    json = json.lower()

    # Get root
    root = get_tree_path(path) / DATASET

    # Bidsify
    Bidsifier(
        root,
        keys=keys,
        exclude_keys=exclude_keys,
        subs=subs,
        exclude_subs=exclude_subs,
        json=json,
        ifexists=if_exists,
    ).run()
