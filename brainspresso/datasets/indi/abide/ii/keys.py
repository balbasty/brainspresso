"""Keys of ABIDE-II (`--keys`; the roast keys are `roaster.KEYS`)."""

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
