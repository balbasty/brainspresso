# ABIDE-II

ABIDE-II is the second release of the *Autism Brain Imaging Data Exchange*
(June 2016), funded by the NIMH (R21 MH107045). It adds 1114 datasets from 19
sites: 521 individuals with autism spectrum disorder and 593 controls, aged 5
to 64 years. Compared to ABIDE-I, it has more extensive phenotypic
characterisation, diffusion imaging at several sites, and two longitudinal
collections in which 38 individuals were scanned twice, one to four years
apart.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx abide2` |
| Folder | `ABIDE-2/` |
| Population | Autism spectrum disorder and controls, 5–64 years, 19 sites |
| Modalities | T1w, resting-state fMRI, DWI (some sites) |
| Access | NITRC account |
| Hosts | NITRC; `s3://fcp-indi` (raw images, download only) |
| License | CC BY-NC-SA, non-commercial use |
| Website | <https://fcon_1000.projects.nitrc.org/indi/abide/abide_II.html> |

</div>

## Contents

| Key | BIDS files |
|-----|------------|
| `T1w` | `sub-<label>/ses-<n>/anat/*_T1w.nii.gz` |
| `rest` | `sub-<label>/ses-<n>/func/*_task-rest_bold.nii.gz` |
| `dti` | `sub-<label>/ses-<n>/dwi/*_dwi.nii.gz`, with `bval`/`bvec` |
| `pheno` | `participants.tsv`; `phenotype/*.tsv` (ADI-R, ADOS-2, SRS, ...) |

On harvest, the keys are only `raw` and `meta`, since each tarball contains all
the images of a site.

## Access

The default source, `nitrc`, downloads the original tarballs and phenotype
tables, and needs a NITRC account. The INDI bucket also has a copy of the raw
images (`--source s3`, no login), which brainspresso downloads as is to
`sourcedata/RawData/` but does not convert. Two sites, SU and U_MIA, are
missing from the bucket and are always fetched from NITRC.

## Conversion

* Sessions become `ses-01` and `ses-02`. When a session has several scans of
  the same kind, each gets a `run-<n>` entity.
* The sidecars come from the sites' protocol printouts. The SU and U_MIA sites
  did not provide any, so their images are written without sidecars, with a
  warning.
* In the phenotype tables, `-9999` becomes `n/a`, and handedness is coded
  `L`, `R` or `B`.

## Examples

```shell
bx abide2 harvest /data --sites KKI NYU
bx abide2 roast /data --keys dti pheno
```

## How to cite

The ABIDE usage agreement asks users to specify which ABIDE datasets were
included in their analyses, and to acknowledge the funding sources listed on
the ABIDE-II website. The dataset is described in:

!!! references "References"

    1. Di Martino A, O'Connor D, Chen B, Alaerts K, Anderson JS, Assaf M, et al.
       *Enhancing studies of the connectome in autism using the autism brain
       imaging data exchange II.*
       Sci Data **4**:170010, 2017.
       [doi:10.1038/sdata.2017.10](https://doi.org/10.1038/sdata.2017.10)

    ??? quote "BibTeX"

        ```bibtex
        @article{dimartino2017enhancing,
          author  = {Di Martino, Adriana and O'Connor, David and Chen, Bosi and Alaerts, Kaat and Anderson, Jeffrey S and Assaf, Michal and Balsters, Joshua H and Baxter, Leslie and Beggiato, Anita and Bernaerts, Sylvie and Blanken, Laura M E and Bookheimer, Susan Y and Braden, B Blair and Byrge, Lisa and Castellanos, F Xavier and Dapretto, Mirella and Delorme, Richard and Fair, Damien A and Fishman, Inna and Fitzgerald, Jacqueline and Gallagher, Louise and Keehn, R Joanne Jao and Kennedy, Daniel P and Lainhart, Janet E and Luna, Beatriz and Mostofsky, Stewart H and M{\"u}ller, Ralph-Axel and Nebel, Mary Beth and Nigg, Joel T and O'Hearn, Kirsten and others},
          title   = {Enhancing studies of the connectome in autism using the autism brain imaging data exchange {II}},
          journal = {Scientific Data},
          volume  = {4},
          pages   = {170010},
          year    = {2017},
          doi     = {10.1038/sdata.2017.10},
        }
        ```
