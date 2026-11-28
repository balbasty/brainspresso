# SALD

The *Southwest University Adult Lifespan Dataset* (SALD) is a cross-sectional
sample of 494 healthy adults aged 19 to 80, scanned at Southwest University
(Chongqing, China) on a 3 T Siemens Trio. It was collected to study how brain
structure and intrinsic functional connectivity change across adulthood.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx sald` |
| Folder | `SALD/` |
| Population | 494 healthy adults, 19–80 years, 1 site |
| Modalities | T1w, resting-state fMRI |
| Size | 24 GB |
| Access | Open, no registration |
| Host | `s3://fcp-indi` |
| Website | <https://fcon_1000.projects.nitrc.org/indi/retro/sald.html> |

</div>

## Contents

`anat/*_T1w.nii.gz` and `func/*_task-rest_bold.nii.gz`, one session per
subject.

The bucket has no participant table: the demographics are only published on
the INDI website, so the roasted dataset has no `participants.tsv`.

## Curation

The bucket copy is in BIDS. The sidecars are completed with the acquisition
parameters of the T1w and resting-state protocols, and a README and dataset
description are written.

## Examples

```shell
bx sald info
bx sald harvest /data --suffixes T1w --dry-run
```

## How to cite

!!! references "References"

    1. Wei D, Zhuang K, Ai L, Chen Q, Yang W, Liu W, et al.
       *Structural and functional brain scans from the cross-sectional Southwest
       University adult lifespan dataset.*
       Sci Data **5**:180134, 2018.
       [doi:10.1038/sdata.2018.134](https://doi.org/10.1038/sdata.2018.134)
