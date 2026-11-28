# UNC-Wisconsin

The *UNC-Wisconsin Rhesus Macaque Neurodevelopment Database* is a longitudinal
structural MRI and DTI database of typically developing rhesus macaques
(*Macaca mulatta*), scanned repeatedly from a few weeks of age to 3 years. It
was created by the Harlow Primate Laboratory (University of Wisconsin–Madison)
and the Neuro Image Research and Analysis Laboratories (University of North
Carolina at Chapel Hill), with NIH funding (5R01MH091645). The database also
distributes the outputs of the authors' processing pipelines: atlas
registration, brain masks, quality-controlled diffusion data and tensor maps.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx unc-wisconsin`, extra `brainspresso[unc-wisconsin]` for roast |
| Folder | `UNC-Wisconsin/` |
| Population | Rhesus macaques, 2 weeks to 3 years, longitudinal |
| Modalities | T1w, T2w, DWI |
| Derivatives | Atlas-registered images, brain masks, QC'd DWI, DTI maps |
| Size | About 260 GB |
| Access | Open, no registration |
| Host | [data.kitware.com](https://data.kitware.com/#collection/54b582c38d777f4362aa9cb3) (Girder) |
| Website | <https://www.nitrc.org/projects/uncuw_macdevmri/> |

</div>

## Contents

Sessions are labelled by age, e.g. `ses-12months`.

| Key | BIDS files |
|-----|------------|
| `T1w`, `T2w` | `sub-<label>/ses-<age>/anat/*_{T1w,T2w}.nii.gz` |
| `dwi` | `sub-<label>/ses-<age>/dwi/*_dwi.nii.gz`, with `bval`/`bvec` |
| `meta` | `participants.tsv` (species, sex, date of birth); `sessions.tsv` per subject (scan date, age, weight) |
| `reg` | `derivatives/unc-wisconsin/`: T1w and T2w registered to the 12-month atlas (`space-UNCEmoryWisconsin12mo`), brain masks, skull-stripped images |
| `dti-qc` | `derivatives/unc-wisconsin/`: the quality-controlled DWI series (`desc-autoqc`) |
| `dti-proc` | `derivatives/unc-wisconsin/`: b0, iDWI, FA and MD maps, brain mask in DWI space |

`raw` groups `T1w`, `T2w` and `dwi`; `derivatives` groups `reg`, `dti-qc` and
`dti-proc`. By default, harvest selects `raw` and `meta`.

## Access

The data is public on Kitware's Girder server and needs no account. Questions
about the data go to the forum of the
[NITRC project](https://www.nitrc.org/projects/uncuw_macdevmri/).

## Conversion

* The images are distributed in NRRD format and converted to NIfTI. The
  gradient table stored in each NRRD header is written as `bval`/`bvec`.
* Subject and session metadata (sex, date of birth, scan date, age, weight) is
  stored on the Girder folders. Harvest saves it to
  `sourcedata/folders.json`, and roast needs it: always harvest `meta`.
* The DTI maps use the `*_dwimap` names of BIDS extension proposal BEP016,
  which are not yet part of the BIDS specification. They are listed in
  `.bidsignore`.
* The tensor images themselves are not converted. `--keys dti-qc dti-proc`
  downloads the NRRD files to `sourcedata/`.
* FA and MD maps are kept as distributed, which may mean scaled integers.

## Examples

```shell
bx unc-wisconsin harvest /data --subs 001 002 --sessions 12months
bx unc-wisconsin harvest /data --keys raw derivatives --dry-run
bx unc-wisconsin roast /data
```

## How to cite

!!! references "References"

    1. Young JT, Shi Y, Niethammer M, Grauer M, Coe CL, Lubach GR, et al.
       *The UNC-Wisconsin rhesus macaque neurodevelopment database: a structural
       MRI and DTI database of early postnatal development.*
       Front Neurosci **11**:29, 2017.
       [doi:10.3389/fnins.2017.00029](https://doi.org/10.3389/fnins.2017.00029)

    ??? quote "BibTeX"

        ```bibtex
        @article{young2017unc,
          author  = {Young, Jeffrey T and Shi, Yundi and Niethammer, Marc and Grauer, Michael and Coe, Christopher L and Lubach, Gabriele R and Davis, Bradley and Budin, Francois and Knickmeyer, Rebecca C and Alexander, Andrew L and Styner, Martin A},
          title   = {The {UNC}-{Wisconsin} rhesus macaque neurodevelopment database: a structural {MRI} and {DTI} database of early postnatal development},
          journal = {Frontiers in Neuroscience},
          volume  = {11},
          pages   = {29},
          year    = {2017},
          doi     = {10.3389/fnins.2017.00029},
        }
        ```
