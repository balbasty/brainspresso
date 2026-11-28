# OASIS-3

OASIS-3 is a retrospective compilation of data collected over about 30 years
through the WUSTL Knight ADRC and its affiliated studies. It includes 1378
participants aged 42 to 95: 755 cognitively normal adults and 622 at various
stages of cognitive decline. It contains 2842 MR sessions with a wide range of
sequences, more than 2000 PET scans with amyloid (PIB, AV45) and metabolic
(FDG) tracers, tau PET (AV1451), CT, FreeSurfer segmentations and the outputs
of the PET Unified Pipeline, along with extensive clinical and cognitive
assessments. Dates are replaced by the number of days since the participant's
entry into the study.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx oasis3` |
| Folder | `OASIS-3/` |
| Population | 1378 adults, 42–95 years, normal ageing and Alzheimer's disease |
| Modalities | T1w, T2w, FLAIR, T2\*w, SWI, TOF angiography, ASL, DWI, resting-state fMRI, PET, CT |
| Derivatives | FreeSurfer 5.3; PET Unified Pipeline (download only) |
| Access | Application on the OASIS website and NITRC account |
| Host | NITRC-IR (XNAT project `OASIS3`) |
| Website | <https://sites.wustl.edu/oasisbrains/home/oasis-3/> |

</div>

## Contents

Sessions are labelled by the number of days since entry: `ses-d0000`,
`ses-d0712`, ...

| Key | BIDS files |
|-----|------------|
| `T1w`, `T2w`, `TSE`, `FLAIR`, `T2star`, `swi`, `angio` | `anat/` |
| `bold` | `func/*_task-rest_bold` (and `task-rest_acq-MB4` for the multiband runs) |
| `asl`, `pasl` | `perf/`, with `m0scan` and `aslcontext` |
| `dwi` | `dwi/`, with `bval`/`bvec` |
| `fmap` | `fmap/` |
| `PIB`, `AV45`, `FDG`, `AV1451` | `pet/*_acq-<tracer>_pet` |
| `CT` | `ct/` |
| `meta` | `participants.tsv`, and a `sessions.tsv` per subject with the age and diagnosis at the closest clinical visit |
| `pheno` | `phenotype/UDS_<form>.tsv`: the forms of the Uniform Data Set, with their data dictionaries |
| `fs`, `fs-all` | `derivatives/oasis-freesurfer/` |
| `pup` | PET Unified Pipeline outputs, downloaded to `sourcedata/`, not converted |

The keys form a tree: `raw` (`mri`, `pet`, `ct`), `derivatives` and `meta`.

## Access

Access requires an application on the
[OASIS website](https://sites.wustl.edu/oasisbrains/), which is then linked
to your NITRC account. See [OASIS](index.md#access-and-terms) for the terms of use.

## Conversion

* Each scan is a separate tarball on NITRC-IR. Only the files that the
  selection needs are extracted.
* Some names are fixed to follow BIDS: `sess-` becomes `ses-`,
  `sub-OAS30001` becomes `sub-0001`, `task-restingstate` becomes `task-rest`.
* In the phenotype tables, `OASISID` becomes `participant_id`,
  `days_to_visit` becomes `delay`, and medications are mapped to drug classes.

## Examples

```shell
bx oasis3 harvest /data --keys T1w meta --subs 1 2
bx oasis3 harvest /data --keys PIB AV45 --dry-run
bx oasis3 roast /data --keys raw pheno
```

## How to cite

Acknowledge OASIS as described in the [data use terms](index.md#access-and-terms),
with the following funding: P30 AG066444, P50 AG00561, P30 NS09857781, P01
AG026276, P01 AG003991, R01 AG043434, UL1 TR000448, R01 EB009352. Amyloid
studies should also state that AV-45 doses were provided by Avid
Radiopharmaceuticals, a wholly owned subsidiary of Eli Lilly.

!!! references "References"

    1. LaMontagne PJ, Benzinger TLS, Morris JC, Keefe S, Hornbeck R, Xiong C, et al.
       *OASIS-3: longitudinal neuroimaging, clinical, and cognitive dataset for
       normal aging and Alzheimer disease.*
       medRxiv (preprint), 2019.
       [doi:10.1101/2019.12.13.19014902](https://doi.org/10.1101/2019.12.13.19014902)
