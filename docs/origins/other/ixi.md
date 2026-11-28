# IXI

The IXI dataset (*Information eXtraction from Images*) contains MR scans of
nearly 600 healthy adults. It was collected for the EPSRC-funded IXI project
(GR/S21533/02) at three London hospitals: Guy's Hospital (Philips 1.5 T),
Hammersmith Hospital (Philips 3 T) and the Institute of Psychiatry (GE 1.5 T).
Each subject has T1-, T2- and PD-weighted images, an MR angiogram and a
diffusion series, along with basic demographics.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx ixi`, extra `brainspresso[ixi]` for roast |
| Folder | `IXI/` |
| Population | Healthy adults, 3 sites |
| Modalities | T1w, T2w, PDw, MRA, DWI (15 directions, b = 1000) |
| Access | Open, no registration |
| Hosts | NITRC-IR; Imperial College (original tarballs, offline) |
| License | [CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/) |
| Website | <https://brain-development.org/ixi-dataset/> |

</div>

## Contents

| Key | BIDS files |
|-----|------------|
| `T1w`, `T2w`, `PDw` | `sub-<label>/anat/sub-<label>_{T1w,T2w,PDw}.nii.gz` |
| `angio` | `sub-<label>/anat/sub-<label>_angio.nii.gz` |
| `dwi` | `sub-<label>/dwi/sub-<label>_dwi.nii.gz`, 16 volumes |
| `meta` | `participants.tsv`: site, age, sex, height, weight, date of birth, ethnicity, marital status, occupation, qualification, study date |
| `trace` | `derivatives/ixi-scanner/`: the isotropic image computed by the scanner (roast only, not selected by default) |

Subject labels are three-digit numbers, so `--subs 2 12` selects `sub-002` and
`sub-012`.

## Access

The images are downloaded anonymously from the `ixi` project of
[NITRC-IR](https://www.nitrc.org/ir). The original tarballs from Imperial
College (`--source imperial`) are used as a fallback, but that server is
currently offline. NITRC-IR does not host the demographics spreadsheet
(`IXI.xls`) or the gradient tables, so brainspresso ships copies of them.

## Conversion

* Each diffusion series is distributed as one 3D file per volume. Roast stacks
  them into a single 4D image. The gradient table is the same for most
  subjects, so it is written once at the top of the dataset (`dwi.bval`,
  `dwi.bvec`) and inherited. A subject whose table differs gets its own.
* At the two Philips sites, a 17th volume holds an isotropic image computed by
  the scanner. Roast identifies it and writes it to
  `derivatives/ixi-scanner/`, as `desc-trace` (geometric mean) or
  `desc-powderavg` (arithmetic mean). It is not part of the raw series.
* Sidecars come from per-site templates: manufacturer, field strength, echo
  and repetition times, flip angle.
* The site of each subject is read from the image file names.

## Examples

```shell
bx ixi harvest /data --keys T1w T2w meta
bx ixi harvest /data --subs 1:50 --dry-run
bx ixi roast /data --keys dwi trace
```

## How to cite

The data providers ask users to acknowledge the source of the data, for
example by citing the website:

> IXI – Information eXtraction from Images (EPSRC GR/S21533/02),
> <https://brain-development.org/ixi-dataset/>.
