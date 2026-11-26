"""
Expected input
--------------
OASIS-1/
    sourcedata/
        oasis_cross-sectional_disc{1..12}.tar.gz
        oasis_cs_freesurfer_disc{1..12}.tar.gz
        oasis_cross-sectional.csv
        oasis_cross-sectional-reliability.csv
        oasis_cross-sectional_facts.pdf

Expected output
---------------
OASIS-1/
    dataset_description.json      (DatasetType "study")
    sourcedata/
    rawbids/
        dataset_description.json
        participants.tsv
        participants.json
        sub-{04d}/
            anat/
                sub-{04d}_run-{d}_T1w.{nii.gz|json}
    derivatives/
        oasis-processed/
            sub-{04d}/
                anat/
                    sub-{04d}_res-1mm_T1w.{nii.gz|json}
                    sub-{04d}_space-Talairach_res-1mm_T1w.{nii.gz|json}
                    sub-{04d}_space-Talairach_res-1mm_desc-skullstripped_T1w.{nii.gz|json}
                    sub-{04d}_space-Talairach_res-1mm_dseg.{nii.gz|json}
        oasis-freesurfer/
            sub-{04d}/
                anat/
                    sub-{04d}_desc-orig_T1w.{nii.gz|json}
                    sub-{04d}_res-1mm_desc-orig_T1w.{nii.gz|json}
                    sub-{04d}_res-1mm_desc-norm_T1w.{nii.gz|json}
                    sub-{04d}_atlas-Aseg_dseg.{nii.gz|json}
                    sub-{04d}_atlas-AsegDesikanKilliany_dseg.{nii.gz|json}
                    sub-{04d}_hemi-L_wm.surf.{gii|json}
                    sub-{04d}_hemi-L_pial.surf.{gii|json}
                    sub-{04d}_hemi-L_smoothwm.surf.{gii|json}
                    sub-{04d}_hemi-L_inflated.surf.{gii|json}
                    sub-{04d}_hemi-L_sphere.surf.{gii|json}
                    sub-{04d}_hemi-L_curv.shape.{gii|json}
                    sub-{04d}_hemi-L_sulc.shape.{gii|json}
                    sub-{04d}_hemi-L_thickness.shape.{gii|json}
                    sub-{04d}_hemi-L_desc-wm_area.shape.{gii|json}
                    sub-{04d}_hemi-L_desc-pial_area.shape.{gii|json}
                    sub-{04d}_hemi-L_atlas-DesikanKilliany_dseg.label.{gii|json}
                    sub-{04d}_hemi-R_wm.surf.{gii|json}
                    sub-{04d}_hemi-R_pial.surf.{gii|json}
                    sub-{04d}_hemi-R_smoothwm.surf.{gii|json}
                    sub-{04d}_hemi-R_inflated.surf.{gii|json}
                    sub-{04d}_hemi-R_sphere.surf.{gii|json}
                    sub-{04d}_hemi-R_curv.shape.{gii|json}
                    sub-{04d}_hemi-R_sulc.shape.{gii|json}
                    sub-{04d}_hemi-R_thickness.shape.{gii|json}
                    sub-{04d}_hemi-R_desc-wm_area.shape.{gii|json}
                    sub-{04d}_hemi-R_desc-pial_area.shape.{gii|json}
                    sub-{04d}_hemi-R_atlas-DesikanKilliany_dseg.label.{gii|json}

With `--layout flat`, the content of `rawbids/` is in `OASIS-1/` itself
(and `sourcedata/`, `derivatives/` stay where they are).
"""
