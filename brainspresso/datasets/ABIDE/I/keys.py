from brainspresso.utils.keys import get_leaves

allkeys: dict = {
    "all": {
        "meta": {"pheno"},
        "raw": {
            "mri": {
                "anat": {"T1w"},
                "func": {"bold": {"rest"}},
            },
        },
        "derivatives": {
            "proc": {
                "proc-min": {},     # Minimally processed
                "fs": {"fs-all"},   # FreeSurfer
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
