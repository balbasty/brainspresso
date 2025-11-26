from braindataprep.utils.keys import get_leaves

allkeys: dict = {
    "raw": {
        "mri": {
            "anat": {"T1w", "T2w", "TSE", "FLAIR", "T2star", "angio", "swi"},
            "func": {"bold"},
            "perf": {"pasl", "asl"},
            "": {"fmap", "fieldmap", "dwi"}
        },
        "pet": {"FDG", "PIB", "AV45", "AV1451"},
        "ct": {"CT"},
    },
    "derivatives": {"fs", "fs-all", "pup"},
    "meta": {"pheno"},
}

allleaves: set[str] = get_leaves(allkeys)
