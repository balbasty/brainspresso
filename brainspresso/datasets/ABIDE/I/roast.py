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
    dataset_description.json      (DatasetType "study")
    sourcedata/
    rawbids/
        dataset_description.json
        participants.{tsv|json}
        phenotype/
            adir.{tsv|json}
            ados.{tsv|json}
            ados_gotham.{tsv|json}
            srs.{tsv|json}
            vineland.{tsv|json}
            wisc_iv.{tsv|json}
        sub-{:05d}/
            anat/
                sub-{:05d}_T1w.{nii.gz|json}
            func/
                sub-{:05d}_task-rest_run-{:02d}_bold.nii.gz

With `--layout flat`, the content of `rawbids/` is in `ABIDE-1/` itself
(and `sourcedata/`, `derivatives/` stay where they are).
"""
