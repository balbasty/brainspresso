# OASIS-1

OASIS-1 is a cross-sectional collection of 416 right-handed subjects aged 18
to 96. One hundred of the subjects over 60 have a clinical diagnosis of very
mild to mild Alzheimer's disease. Each subject has three or four T1-weighted
MPRAGE scans from a single session, together with the average of these scans,
its registration to the Talairach atlas, a brain mask and a tissue
segmentation. Twenty non-demented subjects were scanned again within 90 days,
to measure reliability.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx oasis1`, extra `brainspresso[oasis1]` for roast |
| Folder | `OASIS-1/` |
| Population | 416 adults, 18–96 years, including mild Alzheimer's disease |
| Modalities | T1w (3–4 runs per subject) |
| Derivatives | Averaged and atlas-registered T1w, brain mask, FSL segmentation, FreeSurfer |
| Size | 16 GB compressed, 50 GB uncompressed |
| Access | Open, no registration |
| Host | WUSTL |
| Website | <https://sites.wustl.edu/oasisbrains/home/oasis-1/> |

</div>

## Contents

| Key | BIDS files |
|-----|------------|
| `raw` | `sub-<label>/anat/sub-<label>_run-<n>_T1w.nii.gz` |
| `meta` | `participants.tsv`: age, sex, education, socio-economic status, MMSE, CDR, eTIV, nWBV, ... |
| `avg` | `derivatives/oasis-processed/`: the average of the runs (`res-1mm_T1w`) |
| `tal` | `derivatives/oasis-processed/`: the average registered to Talairach space, and its skull-stripped version |
| `fsl` | `derivatives/oasis-processed/`: FSL FAST tissue segmentation in Talairach space (`dseg`) |
| `fs` | `derivatives/oasis-freesurfer/`: FreeSurfer 4.0 outputs that BIDS describes |
| `fs-all` | as `fs`, plus every other FreeSurfer file |

On harvest, `raw` downloads the image discs, which also contain the processed
images; `fs` downloads the FreeSurfer discs.

The dataset is distributed in twelve *discs*, each holding a range of
subjects. `--discs 1 2` restricts harvest and roast to some of them.

## Conversion

* Only the first session is converted. The reliability rescans of 20 subjects
  are not.
* The FSL segmentation labels are reordered to grey matter = 1, white matter =
  2, CSF = 3, and described in a `dseg.tsv`.
* Each disc is read once. A disc that has not changed is skipped on the next
  roast.

## Examples

```shell
bx oasis1 harvest /data --keys raw meta --discs 1 2
bx oasis1 roast /data --keys raw meta --sub 1:20
bx oasis1 roast /data --keys avg tal fsl fs --discs 1
```

## How to cite

Acknowledge OASIS as described in the [data use terms](index.md#access-and-terms),
with the following funding: P50 AG05681, P01 AG03991, P01 AG026276, R01
AG021910, P20 MH071616, U24 RR021382.

!!! references "References"

    1. Marcus DS, Wang TH, Parker J, Csernansky JG, Morris JC, Buckner RL.
       *Open Access Series of Imaging Studies (OASIS): cross-sectional MRI data
       in young, middle aged, nondemented, and demented older adults.*
       J Cogn Neurosci **19**(9):1498-1507, 2007.
       [doi:10.1162/jocn.2007.19.9.1498](https://doi.org/10.1162/jocn.2007.19.9.1498)

    ??? quote "BibTeX"

        ```bibtex
        @article{marcus2007open,
          author  = {Marcus, D. S. and Wang, T. H. and Parker, J. and Csernansky, J. G. and Morris, J. C. and Buckner, R. L.},
          title   = {Open Access Series of Imaging Studies ({OASIS}): cross-sectional {MRI} data in young, middle aged, nondemented, and demented older adults},
          journal = {Journal of Cognitive Neuroscience},
          volume  = {19},
          number  = {9},
          pages   = {1498--1507},
          year    = {2007},
          doi     = {10.1162/jocn.2007.19.9.1498},
        }
        ```
