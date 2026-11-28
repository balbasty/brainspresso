# NKI-RS

The *Nathan Kline Institute Rockland Sample* (NKI-RS) is a community-ascertained
lifespan sample from Rockland County, New York, aged 6 to 85 years. Its
*enhanced* phase combines a multimodal imaging protocol (anatomical, diffusion,
resting-state and task fMRI acquired with multiband sequences) with deep
phenotyping. Many participants have several visits.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx nki-rs` |
| Folder | `NKI-RS/` |
| Population | Community sample, 6–85 years, 1 site, longitudinal |
| Modalities | T1w, T2w, FLAIR, DWI, resting-state and task fMRI |
| Size | 2.6 TB, about 1300 subjects |
| Access | Imaging open; phenotypes need a data usage agreement with NKI |
| Host | `s3://fcp-indi` |
| Website | <https://fcon_1000.projects.nitrc.org/indi/enhanced/> |

</div>

## Contents

* `anat`: T1w, T2w, FLAIR.
* `dwi`, with `bval`/`bvec`.
* `func`: resting state with three protocols (`acq-645`, `acq-1400`,
  `acq-CAP`), and the `CHECKERBOARD` and `BREATHHOLD` tasks.
* `participants.tsv` from the bucket.

Sessions are named after the visit: `ses-BAS1`, `ses-BAS2`, `ses-FLU1`, ...

## Access

The imaging data is downloaded anonymously from the INDI bucket. The phenotypic
data is not on the bucket: it is distributed by NKI under a data usage
agreement.

## Curation

The bucket copy is already in BIDS, but its sidecars lack most acquisition
parameters. brainspresso fills them in from templates of the T1w, DWI and BOLD
protocols. Keys that the bucket provides are never changed. Files that are not
part of BIDS (link lists, conversion logs) are moved to `sourcedata/`.

## Examples

```shell
bx nki-rs info
bx nki-rs harvest /data --suffixes T1w --dry-run
bx nki-rs harvest /data --tasks rest --sessions BAS1 --where "age>=60"
```

## How to cite

!!! references "References"

    1. Nooner KB, Colcombe SJ, Tobe RH, Mennes M, Benedict MM, Moreno AL, et al.
       *The NKI-Rockland Sample: a model for accelerating the pace of discovery
       science in psychiatry.*
       Front Neurosci **6**:152, 2012.
       [doi:10.3389/fnins.2012.00152](https://doi.org/10.3389/fnins.2012.00152)

    ??? quote "BibTeX"

        ```bibtex
        @article{nooner2012nki,
          author  = {Nooner, Kate Brody and Colcombe, Stanley J and Tobe, Russell H and Mennes, Maarten and Benedict, Melissa M and Moreno, Alexis L and Panek, Laura J and Brown, Shaquanna and Zavitz, Stephen T and Li, Qingyang and Sikka, Sharad and Gutman, David and Bangaru, Saroja and Schlachter, Rochelle Tziona and Kamiel, Stephanie M and Anwar, Ayesha R and Hinz, Caitlin M and Kaplan, Michelle S and Rachlin, Anna B and Adelsberg, Samantha and Cheung, Brian and Khanuja, Ranjit and Yan, Chaogan and Craddock, Cameron C and Calhoun, Vincent and Courtney, William and King, Margaret and Wood, Dylan and Cox, Christine L and Kelly, A M Clare and others},
          title   = {The {NKI}-{Rockland} sample: a model for accelerating the pace of discovery science in psychiatry},
          journal = {Frontiers in Neuroscience},
          volume  = {6},
          pages   = {152},
          year    = {2012},
          doi     = {10.3389/fnins.2012.00152},
        }
        ```
