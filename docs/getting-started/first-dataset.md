# Your first dataset

This page walks through a complete run on [IXI](../origins/other/ixi.md), a
dataset of nearly 600 healthy adults that is freely available without
registration.

## 1. Inspect the dataset

`info` lists the *keys* of a dataset (the categories of data that can be
selected) and the *sources* it can be downloaded from:

```console
$ bx ixi info
the IXI dataset
  harvest keys: meta, T1w, T2w, PDw, angio, dwi
  roast keys: meta, T1w, T2w, PDw, angio, dwi, trace
  source nitrc: PDw, T1w, T2w, angio, dwi
  source imperial: PDw, T1w, T2w, angio, dwi, meta
  source bundled: meta
  default for T1w: nitrc then imperial
  ...
  default for meta: bundled
```

Images are downloaded from NITRC-IR, with the original tarballs from Imperial
College as a fallback. The demographics spreadsheet (`meta`) ships with
brainspresso.

## 2. Harvest

`--dry-run` prints what would be downloaded without writing anything:

```shell
bx ixi harvest /data --keys T1w meta --sub 2 12 --dry-run
```

Subjects can be given as labels (`002`) or, for datasets with numbered
subjects, as numbers (`2`) and ranges (`1:10`). Remove `--dry-run` to
download:

```shell
bx ixi harvest /data --keys T1w meta --sub 2 12
```

The files are saved under `/data/IXI/sourcedata/` with the names they have on
the host. Running the same command again downloads only what is missing.

## 3. Roast

```shell
pip install "brainspresso[ixi] @ git+https://github.com/balbasty/brainspresso"
bx ixi roast /data
```

`roast` converts everything that has been harvested. It accepts the same
`--sub` and `--keys` options when only part of it should be converted. The
result is a BIDS *study* dataset:

```
IXI/
├── dataset_description.json      DatasetType "study"
├── sourcedata/                   the harvested files
├── rawbids/                      the raw BIDS dataset
│   ├── dataset_description.json
│   ├── README  participants.tsv  participants.json
│   ├── sub-002/anat/sub-002_T1w.nii.gz
│   └── sub-012/anat/sub-012_T1w.nii.gz
└── .brainspresso/                brainspresso's own records
```

Use `--layout flat` on the first harvest to put the raw BIDS dataset directly
in `IXI/` instead. [Layout](../guide/layout.md) explains both options.

## 4. Compost

Once the data is in BIDS, the archives in `sourcedata/` are no longer needed:

```shell
bx ixi compost /data --dry-run    # list what would be deleted
bx ixi compost /data              # delete it, after confirmation
```

## Next steps

* Datasets that need an account, such as [OASIS](../origins/oasis/index.md)
  or [ABIDE](../origins/indi/abide1.md), take credentials on the command line,
  from environment variables, or from a prompt. See
  [Harvest](../guide/harvest.md#credentials).
* Options that you always pass can be stored in a
  [configuration file](../guide/config.md).
