# SLIM

The *Southwest University Longitudinal Imaging Multimodal* (SLIM) dataset
follows about 600 healthy young adults, mostly university students, scanned up
to three times between 2011 and 2014 at Southwest University (Chongqing,
China) on a 3 T Siemens Trio. It provides test-retest data for anatomical,
diffusion and resting-state imaging, alongside behavioural measures.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx slim` |
| Folder | `SLIM/` |
| Population | About 600 healthy young adults, 1 site, up to 3 sessions |
| Modalities | T1w, DWI, resting-state fMRI |
| Size | 125 GB |
| Access | Open, no registration |
| Host | `s3://fcp-indi` |
| Website | <https://fcon_1000.projects.nitrc.org/indi/retro/southwestuni_qiu_index.html> |

</div>

## Contents

* `anat/*_T1w.nii.gz`, `dwi/*_dwi.nii.gz` with `bval`/`bvec`, and
  `func/*_task-rest_bold.nii.gz`, in sessions `ses-1` to `ses-3`.
* `participants.tsv` (sex, age at the first scan) and a `sessions.tsv` per
  subject (scan date, age), built from the phenotype tables on the bucket.

## Curation

* The bucket has no functional sidecars. brainspresso writes one with a
  repetition time of 2 s. The NIfTI headers of most first-session scans give
  1 s, but the acquisition matches that of SALD, whose DICOM-derived sidecars
  give 2 s.
* The T1w and DWI sidecars are completed from protocol templates.
* Scan dates are converted to ISO 8601 (`19-Nov-2011` becomes `2011-11-19`).

## Examples

```shell
bx slim info
bx slim harvest /data --ses 1 --datatype anat dwi --dry-run
bx slim harvest /data --task rest --where "sex==F"
```

## How to cite

!!! references "References"

    1. Liu W, Wei D, Chen Q, Yang W, Meng J, Wu G, et al.
       *Longitudinal test-retest neuroimaging data from healthy young adults in
       southwest China.*
       Sci Data **4**:170017, 2017.
       [doi:10.1038/sdata.2017.17](https://doi.org/10.1038/sdata.2017.17)

    ??? quote "BibTeX"

        ```bibtex
        @article{liu2017longitudinal,
          author  = {Liu, Wei and Wei, Dongtao and Chen, Qunlin and Yang, Wenjing and Meng, Jie and Wu, Guorong and Bi, Taiyong and Zhang, Qinglin and Zuo, Xi-Nian and Qiu, Jiang},
          title   = {Longitudinal test-retest neuroimaging data from healthy young adults in southwest China},
          journal = {Scientific Data},
          volume  = {4},
          pages   = {170017},
          year    = {2017},
          doi     = {10.1038/sdata.2017.17},
        }
        ```
