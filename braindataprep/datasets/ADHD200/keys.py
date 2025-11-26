from braindataprep.utils.keys import get_leaves

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
                "cpa": {},
                "denoise": {},
                "fmriprep": {},
                "mindboggle": {"mindboggle/ants", "mindboggle/freesurfer"},
                "fs": {"fs-all"},
            },
            "qa": {
                "mriqc",
            }
        },
    }
}

allleaves: set[str] = get_leaves(allkeys)
