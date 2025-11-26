"""
Expected input
--------------
ABIDE-1/
    sourcedata/
        Phenotypic_V1_0b.csv
        {site}_{participant_id}/anat.tar.gz
        {site}_{participant_id}/rest.tar.gz

Expected output
---------------
ABIDE-1/
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
            anat/
                sub-{:05d}_T1w.{nii.gz|json}
            func/
                sub-{:05d}_task-rest_run-{:02d}_bold.nii.gz
"""
from enum import Enum
from pathlib import Path
from typing import Literal, Iterable

from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.actions.action import IfExists
from brainspresso.datasets.ABIDE.I.command import abide1
from brainspresso.datasets.ABIDE.I.keys import allleaves
from brainspresso.datasets.ABIDE.I.bidsifier_xnat import (
    Bidsifier as BidsifierXNAT
)
from brainspresso.datasets.ABIDE.I.bidsifier_nitrc import (
    Bidsifier as BidsifierNITRC
)


DATASET = 'ABIDE-1'

SourceChoice = Enum(
    "SourceChoice", [("nitrc", "nitrc"), ("xnat", "xnat")], type=str
)


@abide1.command(name="roast")
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[str] = tuple(),
    exclude_keys: Iterable[str] | None = tuple(),
    subs: Iterable[int] | None = tuple(),
    exclude_subs: Iterable[int] | None = tuple(),
    json: Literal["yes", "no", "only"] | bool = "yes",
    if_exists: IfExists.Choice = "skip",
    source: SourceChoice = SourceChoice.nitrc,
    log: str | None = None,
    level: str = "info",
):
    """
    Convert source data into a BIDS-compliant directory

    **Hierarchy of keys:**

    * all
      * meta              Metadata
      * raw               All raw data
        * anat            Anatomical T1w scans
        * rest            Resting-state fMRI
      * derivatives       All derivatives
        * proc            All processed data
          * proc-min      Minimally processed data
          * ants          ANTs
          * ccs           Connectome Computation System
          * civet         Civet
          * cpac          Configurable Pipeline for the Analysis of Connectomes
          * dparsf        Data Processing Assistant for Resting-State fMRI
          * fs            FreeSurfer
          * niak          NeuroImaging Analysis Kit
        * qa              All quality assesment
          * qa-man        Manual QA
          * qa-pcp        Preprocessed Connectomes Project

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
    level
        Loggin level
    """
    setup_filelog(log, level=level)

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
    Bidsifier = {"xnat": BidsifierXNAT, "nitrc": BidsifierNITRC}[source]
    Bidsifier(
        root,
        keys=keys,
        exclude_keys=exclude_keys,
        subs=subs,
        exclude_subs=exclude_subs,
        json=json,
        ifexists=if_exists,
    ).run()
