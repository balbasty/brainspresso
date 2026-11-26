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
These datasets can be listed by typing `brainspresso`:
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

## IXI: Information eXtraction from Images

```
Usage: brainspresso ixi COMMAND

IXI dataset (controls)

 • Project       Information eXtraction from Images (IXI)
 • Modalities    T1w, T2w, PD2, MRA, DTI
 • Populations   Controls
 • Funding       EPSRC GR/S21533/02
 • License       CC BY-SA 3.0

╭─ Commands ────────────────────────────────────────────────────────────────────────╮
│ harvest    Download source data for the IXI dataset.                              │
│ roast      Convert source data into a BIDS-compliant directory.                   │
│ --help,-h  Display this message and exit.                                         │
│ --version  Display application version.                                           │
╰───────────────────────────────────────────────────────────────────────────────────╯
```
```
Usage: brainspresso ixi harvest [ARGS] [OPTIONS]

Download source data for the IXI dataset.
╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path  Path to root of all datasets. An IXI folder will be created.         │
│ --keys       Modalities to download                                               │
│              [default: ('meta', 'T1w', 'T2w','PDw', 'angio', 'dwi')]              │
│ --if-exists  Behaviour if a file already exists                                   │
│              [choices: skip,overwrite,different,refresh,error] [default: skip]    │
│ --packet     Packet size to download, in bytes [default: 8.4 MB]                  │
│ --log        Path to log file                                                     │
╰───────────────────────────────────────────────────────────────────────────────────╯
```
```
Usage: brainspresso ixi roast [ARGS] [OPTIONS]

Convert source data into a BIDS-compliant directory

╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path     Path to root of all datasets. An IXI/sourcedata folder            │
│                 must exist.                                                       │
│ --keys          Only bidsify these keys                                           │
│                 [default: ('meta', 'T1w', 'T2w','PDw', 'angio', 'dwi')]           │
│ --subs          Only bidsify these subjects (all if empty) [default: ()]          │
│ --exclude-subs  Do not bidsify these subjects [default: ()]                       │
│ --json          Whether to write (only) sidecar JSON files                        │
│                 [choices:yes,no,only] [default: yes]                              │
│ --if-exists     Behaviour when a file already exists                              │
│                 [choices: skip,overwrite,different,refresh,error] [default: skip] │
│ --log           Path to log file                                                  │
╰───────────────────────────────────────────────────────────────────────────────────╯
```

## OpenNeuro: any dataset, or a filtered subset of it

OpenNeuro datasets are already in BIDS, so `harvest` downloads them
directly into a BIDS root (`<path>/OpenNeuro/<dataset>/`, or `--out`).
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

## Compost: clean up a local dataset

Every dataset has a `compost` command. By default, it deletes the source
data (`<path>/<DATASET>/sourcedata/`) once it has been roasted into BIDS,
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
