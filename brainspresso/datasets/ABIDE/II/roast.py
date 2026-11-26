"""
Expected input
--------------
ABIDE-2/
    sourcedata/
        ABIDEII-{SITE}_{SAMPLE}.csv
        ABIDEII-{SITE}_{SAMPLE}.tar.gz/
            ABIDEII-{SITE}_{SAMPLE}/{id}/session_{ses}/
                anat_{ses}/anat.nii.gz
                rest_{ses}/rest.nii.gz
                dti_{ses}/dti.nii.gz
                dti_{ses}/dti.bval
                dti_{ses}/dti.bvec

Expected output
---------------
ABIDE-2/
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
            ses-{:02d}/
                anat/
                    sub-{:05d}_ses-{:02d}_T1w.{nii.gz|json}
                func/
                    sub-{:05d}_ses-{:02d}_task-rest_run-{:02d}_bold.nii.gz
                dwi/
                    sub-{:05d}_ses-{:02d}_dwi.{nii.gz|json|bval|bvec}

With `--layout flat`, the content of `rawbids/` is in `ABIDE-2/` itself
(and `sourcedata/`, `derivatives/` stay where they are).
"""
