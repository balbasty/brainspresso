# Brainspresso

A single application to **harvest** (download), **roast** (bidsify)
and **grind** (preprocess) public datasets.

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
`meta`, or `derivatives` (`reg`, `dti-qc`, `dti-proc`; downloaded but
not converted yet). Please cite https://doi.org/10.3389/fnins.2017.00029.
