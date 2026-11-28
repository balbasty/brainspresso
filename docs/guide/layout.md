# Layout on disk

Every dataset lives in its own folder below the root given on the command line:
`bx ixi harvest /data` writes to `/data/IXI/`, and `bx openneuro harvest
ds000224 /data` to `/data/OpenNeuro/ds000224/`. `--out` sets the dataset folder
directly and ignores the root.

## Study layout (default)

By default, the dataset folder is a BIDS *study* dataset
(`DatasetType: "study"`, BIDS 1.10.1 and later). The raw BIDS dataset is kept
in `rawbids/`, next to the source data and the derivatives:

```
IXI/
├── dataset_description.json      DatasetType "study"
├── sourcedata/                   harvested files (archives, folders, metadata)
├── rawbids/                      raw BIDS dataset (DatasetType "raw")
│   ├── dataset_description.json
│   ├── README  participants.tsv  participants.json  .bidsignore
│   ├── phenotype/
│   └── sub-002/anat/sub-002_T1w.nii.gz ...
├── derivatives/<pipeline>/       derivative datasets
└── .brainspresso/                manifest, pristine tables, task state
```

Each derivative dataset points back to the raw data through
`SourceDatasets: [{"URL": "../../rawbids/"}]`.

## Flat layout

With `--layout flat`, the dataset folder is the raw BIDS dataset itself, as in
most BIDS datasets found online. `sourcedata/`, `derivatives/` and
`.brainspresso/` are then subfolders of it:

```
IXI/
├── dataset_description.json      DatasetType "raw"
├── participants.tsv
├── sub-002/anat/sub-002_T1w.nii.gz ...
├── sourcedata/
├── derivatives/<pipeline>/
└── .brainspresso/
```

## Choosing a layout

The layout is recorded in `.brainspresso/manifest.json` by the first `harvest`
or `roast`. Later commands reuse it and refuse a different `--layout`. When
there is no manifest, the layout is detected from the files.

Multi-site INDI projects also accept `--layout sites`, which writes one BIDS
dataset per site. See [INDI](../origins/indi/index.md#multi-site-projects).

## The `.brainspresso/` folder

This folder holds brainspresso's own records. Do not edit it by hand.

| Path | Contents |
|------|----------|
| `manifest.json` | Layout, harvest history, and for hosted datasets the snapshot, filters, files, URLs and checksums |
| `orig/` | Untouched copies of tables that brainspresso rewrites (`participants.tsv`, `*_sessions.tsv`, `*_scans.tsv`) |
| `replaced/` | Backups of files replaced by `roast --if-exists update` |
| `locks/`, `runs/` | Lock files and records of running commands (see [Concurrency](concurrency.md)) |

## Migrating older datasets

Earlier versions of brainspresso wrote the raw data to `<DS>/rawdata/`.
`scripts/migrate_layout.py` converts such a dataset to the study or the flat
layout.
