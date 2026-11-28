# NATVIEW

NATVIEW is an open dataset of simultaneous EEG and fMRI recorded during
naturalistic viewing. Twenty-two healthy adults were scanned in two sessions at
the Nathan S. Kline Institute while resting, watching the abstract animation
*Inscapes*, and watching short movie clips. Eye tracking and physiological
signals were recorded at the same time.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx natview` |
| Folder | `NATVIEW/` |
| Population | 22 healthy adults, 2 sessions |
| Modalities | T1w, fMRI (rest, Inscapes, movies), EEG, eye tracking, physiology |
| Size | 275 GB |
| Access | Open, no registration |
| Host | `s3://fcp-indi` |

</div>

## Contents

* `anat/*_T1w.nii.gz`, `func/*_bold.nii.gz`, and `eeg/` recordings, with eye
  tracking and physiology as on the bucket.
* `phenotype/psqi.tsv` (Pittsburgh Sleep Quality Index). The tables with one
  row per session (`nycq`, `last_night_sleep`) are kept in
  `sourcedata/phenotype/`.

EEG files are large: `--datatypes anat func` downloads the MRI alone.

## Examples

```shell
bx natview info
bx natview harvest /data --datatypes anat func --dry-run
bx natview harvest /data --datatypes eeg
```

## How to cite

!!! references "References"

    1. Telesford QK, Gonzalez-Moreira E, Xu T, Tian Y, Colcombe SJ, Cloud J, et al.
       *An open-access dataset of naturalistic viewing using simultaneous EEG-fMRI.*
       Sci Data **10**:554, 2023.
       [doi:10.1038/s41597-023-02458-8](https://doi.org/10.1038/s41597-023-02458-8)
