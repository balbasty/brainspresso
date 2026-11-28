# Bullitt

The *Designed Database of MR Brain Images of Healthy Volunteers* was collected
by Elizabeth Bullitt and colleagues at the CASILab, University of North
Carolina at Chapel Hill. It was designed for studies of the cerebral
vasculature in health: 100 healthy adults were recruited, 20 per decade from 18
to over 60, with equal numbers of men and women in each group. People with
diabetes, hypertension, head trauma, psychiatric disease or other conditions
likely to affect the brain were excluded. All subjects were scanned on the
same 3 T Siemens scanner with a standardised protocol, and consented to the
public release of their data.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx bullitt` |
| Folder | `Bullitt/` |
| Population | 109 healthy adults, 18 to 72 years |
| Modalities | T1w (FLASH, MPRAGE), T2w, MR angiography (TOF), DTI |
| Derivatives | Skull-stripped T1w, segmented vessels |
| Access | Open, no registration |
| Host | [data.kitware.com](https://data.kitware.com/#collection/591086ee8d777f16d01e0724) (Girder) |

</div>

## Contents

| Key | BIDS files |
|-----|------------|
| `flash`, `mprage` | `anat/sub-<label>_acq-{flash,mprage}_T1w.nii.gz` (1 mm) |
| `T2w` | `anat/sub-<label>_T2w.nii.gz` |
| `angio` | `anat/sub-<label>_angio.nii.gz` (0.5 × 0.5 × 0.8 mm) |
| `meta` | `participants.tsv`: age, sex, race, handedness, quality of the vessel segmentation, notes |
| `skullstripped` | `derivatives/bullitt/`: skull-stripped FLASH T1w (`desc-brain`) |
| `vessels` | `derivatives/bullitt/`: vascular networks in TubeTK format (`desc-vessels_tubes.tre`) |
| `dwi` | Downloaded only, not converted (see below) |
| `phantoms` | Downloaded only |

`raw` groups `flash`, `mprage`, `T2w`, `angio` and `dwi`; `derivatives` groups
`skullstripped` and `vessels`. By default, harvest selects `raw`, `meta` and
`derivatives`.

## Access

The data is public on Kitware's Girder server and needs no account.

## Conversion

* The images are distributed in MetaImage format (`.mha`) and converted to
  NIfTI.
* The sidecars are built from the sample DICOM headers distributed with the
  data, one per sequence. They describe the protocol, not each subject's
  individual scan.
* Many subjects have no MPRAGE, and some have no FLASH. A missing scan is an
  error only when its key is named with `--keys`.
* `participants.tsv` is transcribed from the dataset's demographics document.
  Subjects 100 to 109 are not described in it. The `notes` column records
  incidental findings and family relationships between subjects.
* The diffusion series are not converted: their seven volumes are stacked
  along the slice axis with an incorrect slice spacing, and no gradient table
  is provided. `--keys dwi` downloads them to `sourcedata/`.
* The `.tre` files are copied as they are and listed in `.bidsignore`.

## Examples

```shell
bx bullitt harvest /data --subs 1:20
bx bullitt harvest /data --keys flash angio vessels
bx bullitt roast /data
```

## How to cite

The data providers ask for the following acknowledgement:

> Designed Database of MR Brain Images of Healthy Volunteers, CASILab,
> Department of Surgery, The University of North Carolina at Chapel Hill
> (E. Bullitt, J.K. Smith, W. Lin).

