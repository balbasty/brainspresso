"""
Expected input
--------------
OASIS-2/
    sourcedata/
        OAS2_RAW_PART{1|2}.tar.gz
        oasis_longitudinal_demographics.xlsx

Expected output
---------------
OASIS-2/
    dataset_description.json      (DatasetType "study")
    sourcedata/
    rawbids/
        dataset_description.json
        participants.{tsv|json}
        sessions.json
        sub-{04d}/
            sub-{04d}_sessions.tsv
            ses-{d}/
                anat/
                    sub-{04d}_run-{d}_T1w.{nii.gz|json}

With `--layout flat`, the content of `rawbids/` is in `OASIS-2/` itself
(and `sourcedata/`, `derivatives/` stay where they are).
"""
