# ABIDE-I

The *Autism Brain Imaging Data Exchange* (ABIDE) aggregates resting-state
fMRI, anatomical and phenotypic data collected at independent sites, to support
large-scale studies of the brain in autism spectrum disorder (ASD). Its first
release, ABIDE-I (August 2012), combines 1112 datasets from 17 international
sites: 539 individuals with ASD and 573 typical controls, aged 7 to 64 years
(median 14.7). The phenotypic data includes diagnostic instruments such as the
ADI-R and ADOS, and cognitive and behavioural scales.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx abide1` |
| Folder | `ABIDE-1/` |
| Population | Autism spectrum disorder and controls, 7–64 years, 17 sites |
| Modalities | T1w, resting-state fMRI; high-resolution T2w at UCLA |
| Derivatives | FreeSurfer (converted); several other pipelines (download only) |
| Access | Open on S3; NITRC account for the tarballs and the derivatives |
| Hosts | `s3://fcp-indi`, NITRC, NITRC-IR (XNAT) |
| License | CC BY-NC-SA, non-commercial use |
| Website | <https://fcon_1000.projects.nitrc.org/indi/abide/abide_I.html> |

</div>

## Contents

| Key | BIDS files |
|-----|------------|
| `T1w` | `sub-<label>/anat/sub-<label>_T1w.nii.gz` |
| `T2w` | `sub-<label>/anat/sub-<label>_acq-hires_T2w.nii.gz` (UCLA only) |
| `rest` | `sub-<label>/func/sub-<label>_task-rest_bold.nii.gz` |
| `pheno` | `participants.tsv`; `phenotype/{adir,ados,ados_gotham,srs,vineland,wisc_iv}.tsv` |
| `fs` | `derivatives/abide-freesurfer/`: FreeSurfer outputs that BIDS describes, with `dseg.tsv` lookup tables |
| `fs-all` | as `fs`, plus every other FreeSurfer file (not selected by default) |
| `proc-min`, `ants`, `civet`, `ccs`, `cpac`, `dparsf`, `niak` | Outputs of other pipelines: downloaded to `sourcedata/`, not converted |
| `qa-man`, `qa-pcp` | Quality assessments: downloaded, not converted |

The keys are organised in a tree: `raw` (`T1w`, `T2w`, `rest`), `meta`
(`pheno`), and `derivatives` (`proc`, with the pipelines, and `qa`).

`--sites` and `--exclude-sites` select sites by name (`NYU`, `Caltech`, ...).

## Access

Three sources are available:

* `s3`: the raw images and the phenotypes on the public INDI bucket. No login.
  This is the default for raw data.
* `nitrc`: the original per-site tarballs and phenotype tables. Needs a NITRC
  account.
* `xnat`: NITRC-IR, which also hosts the derivatives. Needs a NITRC account,
  and must be named explicitly (`--source xnat`).

See [INDI](index.md#access) for credentials and the INDI terms of use.

## Conversion

* Subject labels lose their leading zeros: `0051456` becomes `sub-51456`.
* The sidecars come from per-site protocol templates transcribed from the
  sites' scan parameter documents. The repetition time is also checked against
  the NIfTI header.
* The second, high-resolution anatomical scan of the UCLA sites is a T2-weighted
  image and is written as `acq-hires_T2w`.
* `participants.tsv` keeps the age at the time of the functional scan
  (`AGE_AT_SCAN`).
* Each tarball is read once. A corrupted archive fails only the subjects it
  contains.

## Examples

```shell
bx abide1 harvest /data --sites NYU Caltech --keys T1w rest pheno
bx abide1 roast /data --keys raw pheno
bx abide1 harvest /data --source xnat --keys fs
bx abide1 roast /data --source xnat --keys fs
```

## How to cite

The ABIDE usage agreement asks users to specify which ABIDE datasets were
included in their analyses, and to acknowledge the funding sources listed on
the ABIDE website. The dataset is described in the paper below. The
`ReferencesAndLinks` of the roasted `dataset_description.json` also lists the
papers of the individual sites.

!!! references "References"

    1. Di Martino A, Yan CG, Li Q, Denio E, Castellanos FX, Alaerts K, et al.
       *The autism brain imaging data exchange: towards a large-scale evaluation
       of the intrinsic brain architecture in autism.*
       Mol Psychiatry **19**(6):659-667, 2014.
       [doi:10.1038/mp.2013.78](https://doi.org/10.1038/mp.2013.78)

    ??? quote "BibTeX"

        ```bibtex
        @article{dimartino2014autism,
          author  = {Di Martino, Adriana and Yan, Chao-Gan and Li, Qingyang and Denio, Erin and Castellanos, Francisco X and Alaerts, Kaat and Anderson, Jeffrey S and Assaf, Michal and Bookheimer, Susan Y and Dapretto, Mirella and Deen, Ben and Delmonte, Sonja and Dinstein, Ilan and Ertl-Wagner, Birgit and Fair, Damien A and Gallagher, Louise and Kennedy, Daniel P and Keown, Christopher L and Keysers, Christian and Lainhart, Janet E and Lord, Catherine and Luna, Beatriz and Menon, Vinod and Minshew, Nancy and Monk, Christopher S and Mueller, Sophia and M{\"u}ller, Ralph-Axel and Nebel, Mary Beth and Nigg, Joel T and O'Hearn, Kirsten and others},
          title   = {The autism brain imaging data exchange: towards a large-scale evaluation of the intrinsic brain architecture in autism},
          journal = {Molecular Psychiatry},
          volume  = {19},
          number  = {6},
          pages   = {659--667},
          year    = {2014},
          doi     = {10.1038/mp.2013.78},
        }
        ```
