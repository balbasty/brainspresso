# Selecting data

Every command accepts options that restrict it to part of a dataset. They are
grouped in the same panels in every `--help`.

## Subjects and sessions

| Option | Meaning |
|--------|---------|
| `--sub`                    | Only these subjects |
| `--exclude-sub`            | Not these subjects |
| `--ses`                    | Only these sessions (datasets with sessions) |
| `--site`, `--exclude-site` | Only, or not, these sites (multi-site datasets) |

Subject labels can be given with or without the `sub-` prefix. A file name is
also accepted, in which case the file lists one label per line. For datasets
with numbered subjects, numbers and ranges are expanded to labels of the right
width: in IXI, `--sub 2 12` means `002 012`, and `--sub 1:10` means `001` to
`010`. Labels that are not numbers are kept as they are (`--sub 00448`).

## Keys

*Keys* are the categories of data a dataset provides: image types, metadata,
derivatives. They differ between datasets, and between harvest and roast.
`bx <dataset> info` lists them.

```shell
bx ixi harvest /data --keys T1w T2w          # only these keys
bx ixi harvest /data --exclude-keys dwi      # every key but dwi
```

Some keys are groups of other keys. In Bullitt, for example, `raw` stands for
`flash`, `mprage`, `T2w`, `angio` and `dwi`. Some keys are not selected by
default because they are large or rarely needed. They are listed in the
dataset's page and are only used when named.

## BIDS filters

Datasets hosted in BIDS (OpenNeuro, INDI projects) accept BIDS filters on
`harvest`. Every dataset accepts them on `compost`.

| Option | Meaning |
|--------|---------|
| `--datatype` | Only these datatypes (`anat`, `func`, `dwi`, `fmap`, `perf`...) |
| `--suffix`   | Only these suffixes (`T1w`, `T2w`, `bold`, `dwi`...) |
| `--task`     | Only these tasks |
| `--include`  | Only data files matching these glob patterns, relative to the BIDS root (`"sub-*/anat/*"`) |
| `--exclude`  | Not the files matching these glob patterns |
| `--where`    | Only participants whose `participants.tsv` row matches all these queries (`"age>=60"`, `"sex==F"`) |
| `--folders`  | Also download these optional folders (`derivatives`, `sourcedata`, `code`, `stimuli`) |

Filters select data files: images and recordings. The metadata that applies to
a selected file is added automatically, following the BIDS inheritance
principle. This includes its sidecars, `events.tsv`, the `scans.tsv` and
`sessions.tsv` that list it, the top-level files and `phenotype/`.

```shell
bx openneuro harvest ds000224 /data --suffix T1w T2w --where "age>=30" "gender==M"
bx fcon1000 harvest /data --site ICBM NYU_TRT --datatype func --dry-run
```

## Sidecars

`--sidecars` controls the JSON sidecars:

* `yes` (default): write them.
* `no`: do not write JSON sidecars. `bval`/`bvec` files and tables are still
  written.
* `only`: write the sidecars and other metadata but no images. This is useful
  to inspect the metadata of a large dataset before downloading it.

`--sidecars` replaces the older `--json` option, which still works with a
deprecation warning.
