"""
Keys of ABIDE-I (`--keys`).

`fs` and `fs-all` are siblings (they were a group and its leaf): `fs`
converts the FreeSurfer files that BIDS describes, `fs-all` also
unpacks every other file. The roast keys (`roaster.KEYS`) are these,
with `meta` also the category of the dataset files.
"""
from brainspresso.utils.keys import get_leaves

allkeys: dict = {
    "all": {
        "meta": {"pheno"},
        "raw": {
            "mri": {
                "anat": {"T1w", "T2w"},     # T2w: UCLA hi-res (acq-hires)
                "func": {"bold": {"rest"}},
            },
        },
        "derivatives": {
            "proc": {
                "proc-min": {},     # Minimally processed
                "fs": {},           # FreeSurfer: files that BIDS describes
                "fs-all": {},       # FreeSurfer: every file
                "ants": {},         # ANTs
                "civet": {},        # Civet
                "ccs": {},          # Connectome Computation System
                "cpac": {},         # Configurable Pipeline for the Analysis of Connectomes     # noqa: E501
                "dparsf": {},       # Data Processing Assistant for Resting-State fMRI          # noqa: E501
                "niak": {},         # NeuroImaging Analysis Kit @ SIMEXP
            },
            "qa": {
                "qa-man",           # Manual QA
                "qa-pcp",           # Preprocessed Connectomes Project
            }
        },
    }
}

allleaves: set[str] = get_leaves(allkeys)
