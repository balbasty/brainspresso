# ADHD-200

The ADHD-200 Consortium released resting-state fMRI and anatomical MRI of
children and adolescents with attention deficit hyperactivity disorder (ADHD)
and typically developing controls, collected at several sites, together with
phenotypic information. The release was the basis of the ADHD-200 Global
Competition (2011), which invited teams to classify ADHD from imaging data.
The INDI bucket also holds the outputs of several preprocessing pipelines.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx adhd200` |
| Folder | `ADHD-200/` |
| Population | ADHD and typically developing children and adolescents, several sites |
| Modalities | T1w, resting-state fMRI |
| Derivatives | FreeSurfer, fMRIPrep, Mindboggle, MRIQC, ... (download only) |
| Access | Open, no registration |
| Host | `s3://fcp-indi` |
| License | CC BY-NC, non-commercial use |
| Website | <https://fcon_1000.projects.nitrc.org/indi/adhd200/> |

</div>

!!! note "Harvest only"

    brainspresso downloads ADHD-200 and renames its images to BIDS names, but
    does not yet roast it: no sidecars, `participants.tsv` or
    `dataset_description.json` are written.

## Contents

| Key | Files |
|-----|-------|
| `T1w` | `sub-<label>/ses-<n>/anat/*_T1w.nii.gz` |
| `rest` | `sub-<label>/ses-<n>/func/*_task-rest_run-<n>_bold.nii.gz` |
| `pheno` | Phenotype tables, in `sourcedata/` |
| `fs`, `fs-all` | FreeSurfer outputs, in `sourcedata/` |
| `cpa`, `denoise`, `fmriprep`, `mindboggle` | Pipeline outputs, in `sourcedata/` |
| `mriqc` | MRIQC reports, in `sourcedata/` |

`--no-bids` keeps the images in `sourcedata/` with their original names
(`mprage.nii.gz`, `rest.nii.gz`). Subject labels have seven digits.

## Examples

```shell
bx adhd200 info
bx adhd200 harvest /data --keys T1w rest --sites NYU --dry-run
bx adhd200 harvest /data --keys pheno fs
```

## How to cite

!!! references "References"

    1. ADHD-200 Consortium.
       *The ADHD-200 Consortium: a model to advance the translational potential
       of neuroimaging in clinical neuroscience.*
       Front Syst Neurosci **6**:62, 2012.
       [doi:10.3389/fnsys.2012.00062](https://doi.org/10.3389/fnsys.2012.00062)
