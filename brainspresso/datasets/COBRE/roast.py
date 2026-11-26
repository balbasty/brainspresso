"""
Expected input
--------------
COBRE/
  sourcedata/
    COBRE_parameters_mprage.csv
    COBRE_parameters_rest.csv
    COBRE_phenotypic_data.csv
    COBRE_scan_data.tgz

Expected output
---------------
COBRE/
  dataset_description.json          (DatasetType "study")
  sourcedata/
  rawbids/
      README
      dataset_description.json      (DatasetType "raw")
      participants.tsv
      participants.json
      sub-{07d}/
          anat/
              sub-{07d}_T1w.nii.gz
              sub-{07d}_T1w.json
          func/
              sub-{07d}_bold.nii.gz
              sub-{07d}_bold.json

With `--layout flat`, the content of `rawbids/` is in `COBRE/` itself
(and `sourcedata/`, `derivatives/` stay where they are).
"""
