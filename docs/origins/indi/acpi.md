# ACPI

The *Addiction Connectome Preprocessed Initiative* (ACPI) gathers INDI
collections on substance use, released with both raw and preprocessed data.
`bx acpi` downloads the raw data of its three collections, about 190
participants in total:

* `du_1` (Duke University): nicotine, with scans before and after smoking;
* `mta_1`: cannabis use, in participants of the Multimodal Treatment of ADHD
  study;
* `nyu_1` (New York University): nicotine dependence.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx acpi` |
| Folder | `ACPI/` |
| Population | About 190 participants, 3 collections |
| Modalities | T1w, resting-state fMRI |
| Size | 13 GB |
| Access | Open, no registration |
| Host | `s3://fcp-indi` |
| Website | <https://fcon_1000.projects.nitrc.org/indi/ACPI/html/index.html> |

</div>

## Contents

* `anat/*_T1w.nii.gz` and `func/*_task-rest_bold.nii.gz`.
* `participants.tsv`, merged from the collection tables, with a `site` column.
  The phenotype tables themselves are kept in `sourcedata/phenotype/`.

The collections are selected with `--sites du_1 mta_1 nyu_1`, and
`--layout sites` writes one dataset per collection. The preprocessed data is
not included.

## Curation

* Age, sex and handedness are harmonised across collections (`M`/`F`,
  `R`/`L`/`A`). `du_1` has no BIDS participant table, so its rows are built
  from the phenotype table and its key.
* Site-level sidecars are copied to each subject.
* Sidecar values stored as text (`"3T"`, `"6:00"`) are converted to numbers,
  and slice timing is converted from milliseconds to seconds.

## Examples

```shell
bx acpi info
bx acpi harvest /data --sites du_1 --datatypes anat --dry-run
bx acpi harvest /data --layout sites --sites nyu_1 mta_1
```

## How to cite

ACPI has no dedicated paper. Cite the website and acknowledge its funding:

> The ACPI is primarily supported by a grant supplement (R01 MH094639), with
> additional support from the Child Mind Institute and the Nathan Kline
> Institute.
