# COBRE

The COBRE dataset was collected by the Mind Research Network and the
University of New Mexico as part of a Center of Biomedical Research Excellence
(COBRE) grant on schizophrenia. It contains anatomical and resting-state
functional MRI from 72 patients with schizophrenia and 75 healthy controls,
aged 18 to 65 years, with diagnoses established by structured clinical
interview (SCID).

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx cobre` |
| Folder | `COBRE/` |
| Population | Schizophrenia and controls, 18–65 years, 1 site |
| Modalities | T1w (multi-echo MPRAGE), resting-state fMRI |
| Access | NITRC account |
| Host | NITRC |
| License | CC BY-NC, non-commercial use |
| Website | <https://fcon_1000.projects.nitrc.org/indi/retro/cobre.html> |

</div>

## Contents

| Key | BIDS files |
|-----|------------|
| `T1w` | `sub-<label>/anat/sub-<label>_T1w.nii.gz` |
| `func` | `sub-<label>/func/sub-<label>_task-rest_bold.nii.gz` |
| `meta` | `participants.tsv`: age, sex, handedness, diagnosis (ICD-9 code and subtype) |

Subject labels have seven digits (`sub-0040000`).

## Access

All images are in a single tarball on NITRC, which needs a NITRC account. See
[INDI](index.md#access).

## Conversion

* The sidecars come from templates written from the published acquisition
  parameters (Siemens TrioTim 3 T). The repetition time of each functional run
  is read from its NIfTI header, or set to the documented 2 s when the header
  does not have it.
* The diagnosis column is split into an ICD-9 code and a subtype
  (`depressed`, `bipolar`).

## Examples

```shell
bx cobre harvest /data
bx cobre roast /data --keys T1w meta
```

## How to cite

The data providers ask users to acknowledge the source of the data:

> Data were collected and shared by the Mind Research Network and the
> University of New Mexico, funded by a National Institutes of Health Center of
> Biomedical Research Excellence (COBRE) grant 1P20RR021938-01A2.

The dataset is described in:

!!! references "References"

    1. Aine CJ, Bockholt HJ, Bustillo JR, Cañive JM, Caprihan A, Gasparovic C, et al.
       *Multimodal neuroimaging in schizophrenia: description and dissemination.*
       Neuroinformatics **15**(4):343-364, 2017.
       [doi:10.1007/s12021-017-9338-9](https://doi.org/10.1007/s12021-017-9338-9)
