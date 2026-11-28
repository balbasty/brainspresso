# GSP

The *Brain Genomics Superstruct Project* (GSP) is a study of healthy young
adults that combines structural and resting-state functional MRI with
behavioural, personality and cognitive measures. Its open release is
distributed through Harvard Dataverse.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx gsp` |
| Folder | `GSP/` |
| Population | Healthy young adults, with a retest subset |
| Modalities | T1w, resting-state fMRI |
| Access | Harvard Dataverse account and acceptance of the terms of use |
| Host | [Harvard Dataverse](https://doi.org/10.7910/DVN/25833) |

</div>

!!! note "Harvest only"

    brainspresso downloads GSP but does not yet convert it to BIDS. The
    archives are saved in `GSP/sourcedata/` as distributed.

## Contents

| Key | Files |
|-----|-------|
| `raw` | `GSP_part{1..10}_140630.tar` and `GSP_retest_140630.tar` |
| `meta` | `GSP_list_140630.csv`, `GSP_retest_140630.csv` |

The archives are not split by subject, so `--subs` has no effect.

## Access

The files are released under terms of use that must be accepted on Dataverse.
Downloading them requires a Dataverse API token, which can be created from your
account page. Pass it with `--token`, or set `DATAVERSE_TOKEN`. When neither is
given, brainspresso prompts for it.

## Examples

```shell
bx gsp harvest /data --dry-run
bx gsp harvest /data --keys meta
DATAVERSE_TOKEN=... bx gsp harvest /data
```

## How to cite

!!! references "References"

    1. Holmes AJ, Hollinshead MO, O'Keefe TM, Petrov VI, Fariello GR, Wald LL, et al.
       *Brain Genomics Superstruct Project initial data release with structural,
       functional, and behavioral measures.*
       Sci Data **2**:150031, 2015.
       [doi:10.1038/sdata.2015.31](https://doi.org/10.1038/sdata.2015.31)

    ??? quote "BibTeX"

        ```bibtex
        @article{holmes2015brain,
          author  = {Holmes, A. J. and Hollinshead, M. O. and O'Keefe, T. M. and Petrov, V. I. and Fariello, G. R. and Wald, L. L. and others},
          title   = {Brain {Genomics} {Superstruct} {Project} initial data release with structural, functional, and behavioral measures},
          journal = {Scientific Data},
          volume  = {2},
          pages   = {150031},
          year    = {2015},
          doi     = {10.1038/sdata.2015.31},
        }
        ```
