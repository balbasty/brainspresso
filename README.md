# Brainspresso

A single application to **harvest** (download), **roast** (bidsify),
**grind** (preprocess) and **compost** (clean up) public datasets.

This is a very early work in progress.

### Funding
This application is being developed as part of the project
_"High-resolution spatiotemporal models of the brain across the lifespan
for diagnosis and decision-making"_, funded by a fellowship of the
Royal Society (NIF\R1\232460).

# Installation

```shell
pip install "brainspresso @ git+https://github.com/balbasty/brainspresso"
```

Specific datasets may require additional dependencies. To install the
dependencies to a specific dataset, use the corresponding tag. For example:
```shell
pip install "brainspresso[ixi] @ git+https://github.com/balbasty/brainspresso"
```
To install all possible dependencies, use the `all` tag:
```shell
pip install "brainspresso[all] @ git+https://github.com/balbasty/brainspresso"
```

# Usage

After installation, the `brainspresso` command is available from your shell.
Each known dataset is a specific command, with a set of subcommands.
These datasets can be listed by typing `brainspresso` (or `bx`, or
`python -m brainspresso`):
```
Usage: brainspresso COMMAND

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃     Brainspresso : Download, Bidsify and Process public neuroimagingdatasets      ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

 • Plantation : A public neuroimaging dataset supported brainspresso
 • Harvest    : Download raw data
 • Roast      : Bidsify raw data
 • Grind      : Preprocess data with standard pipelines
 • Compost    : Delete source data, or data outside a filter

╭─ Help ────────────────────────────────────────────────────────────────────────────╮
│ --help,-h  Display this message and exit.                                         │
│ --version  Display application version.                                           │
╰───────────────────────────────────────────────────────────────────────────────────╯
╭─ Plantations ─────────────────────────────────────────────────────────────────────╮
│ ixi        IXI dataset (controls)                                                 │
│ oasis1     OASIS-1 dataset (adult lifespan, dementia)                             │
│ oasis2     OASIS-2 dataset (longitudinal, dementia)                               │
│ ...                                                                               │
╰───────────────────────────────────────────────────────────────────────────────────╯
```

## Local layout

Each dataset lives in `<path>/<DATASET>/`. By default (`--layout study`),
this folder is a BIDS *study* dataset (`DatasetType: "study"`, BIDS
1.10.1+) that keeps the raw BIDS dataset in `rawbids/` (BIDS 1.11.2),
next to the source data and the derivatives:

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
│                                 (SourceDatasets: [{"URL": "../../rawbids/"}])
└── .brainspresso/                manifest, pristine tables, per-task state
```

With `--layout flat`, the dataset folder is the raw BIDS dataset itself
(classic BIDS: `IXI/sub-002/...`, `IXI/participants.tsv`, with
`sourcedata/`, `derivatives/` and `.brainspresso/` inside it). The layout
is recorded in `.brainspresso/manifest.json` by the first `harvest` or
`roast`: later commands reuse it (it is detected from the files when
there is no manifest), and fail if a different `--layout` is given.

## IXI: Information eXtraction from Images

```
Usage: brainspresso ixi COMMAND

IXI dataset (controls)

 • Project       Information eXtraction from Images (IXI)
 • Modalities    T1w, T2w, PD2, MRA, DTI
 • Populations   Controls
 • Funding       EPSRC GR/S21533/02
 • License       CC BY-SA 3.0
 • URL           https://brain-development.org/ixi-dataset/

Harvest: images from NITRC-IR (no login needed; the original tarballs of
brain-development.org with --source imperial), saved in
IXI/sourcedata/IXI-{T1,T2,PD,MRA,DTI}/ with the file names of the original tarballs.
NITRC-IR does not host IXI.xls (demographics): roast uses the copy shipped with
brainspresso if there is none in sourcedata/. Roast: see IXI/roaster.py.

╭─ Commands ───────────────────────────────────────────────────────────────────────────╮
│ compost  Delete source data, or BIDS data outside a filter.                          │
│ harvest  Download the IXI dataset.                                                   │
│ info     Show what the IXI dataset provides.                                         │
│ roast    Convert the source data of the IXI dataset to BIDS.                         │
╰──────────────────────────────────────────────────────────────────────────────────────╯
```

Every dataset has the same commands (`harvest`, `roast` when it can be
roasted, `compost`, `info`), with the same option groups; `bx <dataset>
<command> --help` lists them:

* **Keys** (`--keys`, `--exclude-keys`): data categories of the dataset
  (`bx <dataset> info` lists them). `--exclude-keys` alone keeps the
  files that match no key.
* **Selection** (`--subs`, `--exclude-subs`, `--sessions`, `--sites`,
  `--exclude-sites`): subject labels with or without `sub-` (a file
  lists one per line). Datasets with numbered subjects also accept
  numbers (`--subs 2 12` is `002 012` for IXI) and ranges (`1:10`);
  other labels are kept verbatim (`--subs 00448`).
* **Sources** (`--source`), **Credentials** (`--user`/`--password`, or
  `--token`; else environment variables or a prompt).
* **Harvest** / **Roast** / **Compost** (`--if-exists`, `--jobs`,
  `--dry-run`...), **Dataset** (`path`, `--out`, `--layout`) and
  **Logging** (`--log`, `--level`; `--level debug` prints tracebacks).

A dataset cannot be composted while it is harvested or roasted (also from
other machines that share its filesystem), and the other way round: the
second command fails at once, naming the first one (`--wait SECONDS`
waits for it). Each run leaves a record in `.brainspresso/runs/` while it
runs; compost refuses to start while records of other runs are there (a
crashed run, or a filesystem that does not share locks between machines:
`--ignore-runs` once you have checked that nothing runs).

## OpenNeuro: any dataset, or a filtered subset of it

OpenNeuro datasets are already in BIDS, so `harvest` downloads them
directly into the raw BIDS dataset of `<path>/OpenNeuro/<dataset>/` (or
`--out`): `rawbids/`, or the folder itself with `--layout flat`. The
dataset's own `sourcedata/` and `derivatives/` (`--folders`) go next to
it.
Filters select *data* files; the metadata that applies to them (sidecars,
`events.tsv`, `scans.tsv`/`sessions.tsv`, top-level files, `phenotype/`)
is added automatically, following the BIDS inheritance principle.

```shell
# What is in a dataset, and what would a selection download?
bx openneuro info ds000224
bx openneuro harvest ds000224 --datatypes anat --dry-run

# Anatomical scans of two subjects, from snapshot 1.0.4
bx openneuro harvest ds000224 /data --tag 1.0.4 --subs MSC01 MSC02 --datatypes anat

# Filter participants on participants.tsv columns
bx openneuro harvest ds000224 /data --suffixes T1w T2w --where "age>=30" "gender==M"
```

Useful options: `--sessions`, `--suffixes`, `--tasks`, `--include`/`--exclude`
(globs on paths relative to the dataset root), `--folders derivatives`,
`--jobs`, `--list`. By default, `participants.tsv`, `*_sessions.tsv` and
`*_scans.tsv` are rewritten to only list what is present locally (the
untouched copies are kept in `.brainspresso/orig/`; disable with
`--no-trim`). Each run is recorded in `.brainspresso/manifest.json`
(snapshot, filters, files, URLs and checksums).

Files are listed through the OpenNeuro API, which pins the exact snapshot
and provides SHA256 checksums for annexed files. If the API cannot be
reached, the listing falls back to the public `s3://openneuro.org` bucket,
which only holds the latest snapshot (checksums: MD5 from S3 ETags).
Use `--source graphql|s3` to choose explicitly.

## UNC-Wisconsin Rhesus Macaque Neurodevelopment Database

Longitudinal T1w, T2w and diffusion MRI of rhesus macaques (a few weeks
to 3 years old), hosted on https://data.kitware.com (Girder; no login).
The source files are NRRD: `roast` converts them to NIfTI (with
`bval`/`bvec` for DWI), and writes `participants.tsv` (species, sex, date
of birth) and per-subject `sessions.tsv` (scan date, age, weight).

```shell
pip install "brainspresso[unc-wisconsin] @ git+https://github.com/balbasty/brainspresso"
bx unc-wisconsin harvest /data --subs 001 002 --sessions 12months
bx unc-wisconsin roast /data
```

The full database is ~260 GB; `--keys` selects `raw` (`anat`, `dwi`),
`meta`, or `derivatives` (`reg`, `dti-qc`, `dti-proc`). Derivatives are
written to `derivatives/unc-wisconsin`: atlas-registered T1w/T2w
(`space-UNCEmoryWisconsin12mo`), brain masks, skull-stripped images,
the QC'd DWI series and its DTI maps (FA, MD, b0, iDWI; BEP016
`*_dwimap` names, listed in `.bidsignore`).
Please cite https://doi.org/10.3389/fnins.2017.00029.

## Bullitt: MR brain images of healthy volunteers (CASILab, UNC)

T1w (FLASH and MPRAGE), T2w and MR angiography of 109 healthy adults
(18 to 72 years), hosted on https://data.kitware.com (Girder; no login).
The source files are MetaImage (`.mha`): `roast` converts them to NIfTI,
with sidecars built from the sample DICOM headers distributed with the
data, and writes `participants.tsv` (age, sex, race, handedness, notes)
transcribed from the dataset's demographics PDF.

```shell
bx bullitt harvest /data --subs 1 2 3
bx bullitt roast /data
```

`--keys` selects `raw` (`flash`, `mprage`, `T2w`, `angio`, `dwi`),
`meta`, `derivatives` (`skullstripped`, `vessels`) or `phantoms`.
Derivatives go to `derivatives/bullitt`: skull-stripped FLASH T1w and
the segmented vascular networks (TubeTK `.tre`, listed in
`.bidsignore`). The DTI series are downloaded but not converted: they
have no gradient table, and their volumes are stacked along z.

## INDI projects on `s3://fcp-indi`, as BIDS

Curated commands for projects of the International Neuroimaging
Data-sharing Initiative mirrored on the public `fcp-indi` bucket (no
login needed). Each dataset is harvested straight to the raw BIDS
dataset of `<path>/<DATASET>/` (see [Local layout](#local-layout)), with
the same selection options as `bx openneuro
harvest` (`--subs`, `--sessions`, `--datatypes`, `--suffixes`,
`--tasks`, `--include`/`--exclude`, `--where`, `--folders derivatives`,
`--dry-run`...) plus `--sites` for multi-site datasets. Only the
selected subjects are listed, so small selections of large datasets
start immediately.

| Command        | Dataset                                              | Notes                                   |
|----------------|------------------------------------------------------|-----------------------------------------|
| `fcon1000`     | 1000 Functional Connectomes Project (33 sites)       | T1w, rest fMRI; skull-stripped T1w      |
| `nki-rs`       | NKI Rockland Sample (enhanced)                       | lifespan, multimodal, several sessions  |
| `hbn`          | Healthy Brain Network, MRI (4 sites)                 | children/adolescents; no phenotypes     |
| `sald`         | Southwest University Adult Lifespan Dataset          | T1w, rest fMRI                          |
| `slim`         | Southwest University Longitudinal Imaging Multimodal | T1w, DWI, rest fMRI; 3 time points      |
| `mpi-lemon`    | MPI-Leipzig Mind-Brain-Body: LEMON (MRI)             | MP2RAGE, SWI, DWI, rest fMRI            |
| `acpi`         | Addiction Connectome Preprocessed Initiative (3 sites) | T1w, rest fMRI                        |
| `natview`      | Naturalistic viewing, simultaneous EEG-fMRI          | EEG, eye tracking, physiology           |
| `simon`        | SIMON, one individual scanned in ~70 sessions        | `--folders derivatives`: NIAK outputs   |

```shell
bx fcon1000 info
bx fcon1000 harvest /data --sites ICBM NYU_TRT --datatypes func --dry-run
bx fcon1000 harvest /data --where "age>=60" --folders derivatives
bx hbn harvest /data --sites Site-RU --suffixes T1w --where "Age<10"
bx fcon1000 harvest /data --layout sites --sites ICBM --subs 00448
```

Multi-site datasets (`fcon1000`, `hbn`, `acpi`) can be harvested in two
ways:

* `--layout study` (default) or `--layout flat`: one BIDS dataset, sites
  merged. Labels that are not unique across sites get a site prefix
  (`AnnArbor_a/sub-04111` -> `sub-AnnArborA04111`), and
  `participants.tsv` has a `site` column.
* `--layout sites`: one dataset per site (`FCON1000/<site>/`, each a
  study or a flat dataset: `--layout sites:study` or `sites:flat`; the
  deprecated `--layout sites --site-layout <kind>` still works), with
  the labels of the bucket and site-level metadata (e.g. one
  `task-rest_bold.json` per site). This is "option 1" of the BIDS
  recommendations for multi-site studies. The parent folder is not a
  BIDS dataset: it holds the README and the merged `participants.tsv`,
  which is also what `--where` queries. `--subs` accepts both the site
  labels and the merged labels. `compost` processes each site dataset
  (`--sites`: only these).

Bucket copies are rarely valid BIDS as they are, so each dataset is
curated (see `brainspresso/datasets/INDI`): FCON1000's per-site folders
are merged with site-prefixed labels (`sub-AnnArborA04111`), runs and
typos in file names are fixed and repetition times read from the NIfTI
headers; multi-site participant tables are merged with a `site` column;
missing descriptions, READMEs and sidecars are generated; entities are
put in BIDS order; and common metadata defects (numbers stored as text,
slice timing in milliseconds, invalid JSON, sessions tables without
`session_id`, field maps whose `IntendedFor` is a tag such as `"fMRI"`...)
are repaired after download (`brainspresso/bids/repair.py`).
Each dataset's `README` lists what was changed.

## Compost: clean up a local dataset

Every dataset has a `compost` command. By default, it deletes the source
data (`<path>/<DATASET>/sourcedata/`) once it has been roasted into BIDS
(in either layout),
since the archives it contains are usually only taking up space. It
refuses to do so if no BIDS subject is found (use `--force` to override).

```shell
bx ixi compost /data --dry-run          # what would be deleted?
bx ixi compost /data --keep "*.xls"     # keep some source files
bx oasis1 compost /data --nested        # also derivatives/*/sourcedata
```

If BIDS filters are given, BIDS data that does **not** match them is
deleted instead (source data is then kept, unless `--sourcedata` is
used). The filters are the same as `bx openneuro harvest`, and describe
what is *kept*: composting with some filters leaves what harvesting with
the same filters would have downloaded. Metadata that still applies to a
kept file is kept, dataset-level files are never deleted, and
`participants.tsv`, `*_sessions.tsv` and `*_scans.tsv` are rewritten to
only list what is left.

```shell
bx ixi compost /data --exclude-subs 012 145      # delete two subjects
bx ixi compost /data --suffixes T1w T2w           # only keep T1w and T2w
bx abide1 compost /data --where "age>=18" --derivatives
bx openneuro compost ds000224 /data --datatypes anat
```

`--derivatives` applies the subject and session filters to
`derivatives/*/` too. Files to delete are confirmed interactively
(`--yes` to skip, `--list` to print them first).
