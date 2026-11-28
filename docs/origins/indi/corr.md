# CoRR

The *Consortium for Reliability and Reproducibility* (CoRR) gathered
test-retest data from healthy participants at 19 sites, to measure the
reliability and reproducibility of resting-state fMRI and other connectivity
measures. Each participant was scanned at least twice, with intervals ranging
from less than an hour to several months depending on the site. Several sites
also acquired diffusion imaging and perfusion data.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx corr` |
| Folder | `CoRR/` |
| Population | Healthy participants, 19 sites, test-retest |
| Modalities | T1w, resting-state fMRI, DWI, ASL (depending on the site) |
| Access | Open on S3; NITRC account for the tarballs |
| Hosts | `s3://fcp-indi`, NITRC |
| Website | <https://fcon_1000.projects.nitrc.org/indi/CoRR/html/index.html> |

</div>

!!! note "Harvest only"

    brainspresso downloads CoRR but does not yet convert it to BIDS. The files
    are saved in `CoRR/sourcedata/` as distributed.

## Contents

| Key | Files |
|-----|-------|
| `raw` | The images of each site, sample, subject and session (`sourcedata/RawData/`) |
| `meta` | The phenotype tables |

`--sites` selects sites by name (`BNU`, `HNU`, `NKI`, ...). Subjects can be
given with or without their leading zeros (`3001` or `0003001`).

## Access

The images are on the public INDI bucket (the default, no login). The
original per-site tarballs are on NITRC (`--source nitrc`), which needs a
NITRC account.

## Examples

```shell
bx corr info
bx corr harvest /data --sites BNU HNU --dry-run
bx corr harvest /data --source nitrc --keys meta
```

## How to cite

The consortium asks users to cite the consortium paper and, where applicable,
the paper of each site used. The 7 T test-retest data from the MPI Leipzig has
its own paper.

!!! references "References"

    1. Zuo XN, Anderson JS, Bellec P, Birn RM, Biswal BB, Blautzik J, et al.
       *An open science resource for establishing reliability and reproducibility
       in functional connectomics.*
       Sci Data **1**:140049, 2014.
       [doi:10.1038/sdata.2014.49](https://doi.org/10.1038/sdata.2014.49)
    2. Gorgolewski KJ, Mendes N, Wilfling D, Wladimirow E, Gauthier CJ, Bonnen T, et al.
       *A high resolution 7-Tesla resting-state fMRI test-retest dataset with
       cognitive and physiological measures.*
       Sci Data **2**:140054, 2015.
       [doi:10.1038/sdata.2014.54](https://doi.org/10.1038/sdata.2014.54)

    ??? quote "BibTeX"

        ```bibtex
        @article{zuo2014open,
          author  = {Zuo, X. N. and Anderson, J. S. and Bellec, P. and Birn, R. M. and Biswal, B. B. and Blautzik, J. and others},
          title   = {An open science resource for establishing reliability and reproducibility in functional connectomics},
          journal = {Scientific Data},
          volume  = {1},
          pages   = {140049},
          year    = {2014},
          doi     = {10.1038/sdata.2014.49},
        }

        @article{gorgolewski2015high,
          author  = {Gorgolewski, K. J. and Mendes, N. and Wilfling, D. and Wladimirow, E. and Gauthier, C. J. and Bonnen, T. and others},
          title   = {A high resolution 7-{Tesla} resting-state {fMRI} test-retest dataset with cognitive and physiological measures},
          journal = {Scientific Data},
          volume  = {2},
          pages   = {140054},
          year    = {2015},
          doi     = {10.1038/sdata.2014.54},
        }
        ```
