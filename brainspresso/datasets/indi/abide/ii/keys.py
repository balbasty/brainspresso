from brainspresso.utils.keys import get_leaves

allkeys: dict = {
    "all": {
        "meta": {"pheno"},
        "raw": {
            "mri": {
                "anat": {"T1w"},
                "func": {"bold": {"rest"}},
                "dwi": {"dti"},
            },
        },
    }
}

allleaves: set[str] = get_leaves(allkeys)
