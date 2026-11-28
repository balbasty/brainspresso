# HBN

The *Healthy Brain Network* (HBN) is a study of child and adolescent mental
health and learning disorders run by the Child Mind Institute in the New York
area. Participants aged 5 to 21 are recruited through a community-referred
model, so the sample covers a broad range of clinical presentations rather
than a single diagnosis. The MRI protocol includes anatomical, diffusion,
resting-state and naturalistic movie-viewing scans, acquired at four sites.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx hbn` |
| Folder | `HBN/` |
| Population | Children and adolescents, 5–21 years, 4 sites |
| Modalities | T1w, T2w, FLAIR, MTR, DWI, field maps, resting-state, movie and task fMRI |
| Size | 3.9 TB, about 3900 subjects |
| Access | Imaging open; phenotypes need a data usage agreement |
| Host | `s3://fcp-indi` |
| Website | <https://healthybrainnetwork.org> |

</div>

## Contents

* `anat`: T1w, T2w, FLAIR, and magnetisation transfer pairs (`mt-on`,
  `mt-off`).
* `dwi`, with `bval`/`bvec`, and `fmap`.
* `func`: resting state, movie viewing and tasks.
* `participants.tsv`, merged from the four site tables, with a `site` column.
  `Commercial_Use` records whether the participant consented to commercial use
  of their data.

The sites are `Site-SI` (Staten Island, a mobile 1.5 T scanner), `Site-RU`
(Rutgers), `Site-CBIC` (CitiGroup Biomedical Imaging Center) and `Site-CUNY`.

The preprocessed derivatives on the bucket are not included.

## Access

The imaging data is downloaded anonymously from the INDI bucket. The
phenotypic data is distributed separately by the Child Mind Institute, under a
data usage agreement.

## Curation

* Field-map entities are put in BIDS order (`dir-PA_acq-fMRI` becomes
  `acq-fMRI_dir-PA`).
* Magnetisation transfer pairs are renamed from `_MT-on_PD` to `_mt-on_MTR`.
* Missing `EchoTime1` and `EchoTime2` of phase-difference field maps are filled
  in from the magnitude images.
* The trace-weighted images computed by the scanner (`*TRACEW_dwi`) have no
  gradient table and are listed in `.bidsignore`.

## Examples

```shell
bx hbn info
bx hbn harvest /data --sites Site-RU --suffixes T1w --where "Age<10"
bx hbn harvest /data --layout sites --sites Site-CBIC --datatypes anat --dry-run
```

## How to cite

!!! references "References"

    1. Alexander LM, Escalera J, Ai L, Andreotti C, Febre K, Mangone A, et al.
       *An open resource for transdiagnostic research in pediatric mental health
       and learning disorders.*
       Sci Data **4**:170181, 2017.
       [doi:10.1038/sdata.2017.181](https://doi.org/10.1038/sdata.2017.181)

    ??? quote "BibTeX"

        ```bibtex
        @article{alexander2017open,
          author  = {Alexander, L. M. and Escalera, J. and Ai, L. and Andreotti, C. and Febre, K. and Mangone, A. and others},
          title   = {An open resource for transdiagnostic research in pediatric mental health and learning disorders},
          journal = {Scientific Data},
          volume  = {4},
          pages   = {170181},
          year    = {2017},
          doi     = {10.1038/sdata.2017.181},
        }
        ```
