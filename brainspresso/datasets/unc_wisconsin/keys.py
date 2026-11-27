from brainspresso.utils.keys import get_leaves

allkeys: dict = {
    "all": {
        "meta": {},
        "raw": {
            "anat": {"T1w", "T2w"},
            "dwi": {},
        },
        "derivatives": {
            "reg": {},          # sMRI/Reg2Atlas (+ masks, skull-stripped)
            "dti-qc": {},       # DTI/AUTOQC (QCed DWI, tensors, FA, MD)
            "dti-proc": {},     # DTI/PROCESSED + DTI/MASKMANUAL
        },
    }
}

allleaves: set[str] = get_leaves(allkeys)
