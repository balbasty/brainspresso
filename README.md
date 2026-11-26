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

## INDI projects on `s3://fcp-indi`, as BIDS

Curated commands for projects of the International Neuroimaging
Data-sharing Initiative mirrored on the public `fcp-indi` bucket (no
login needed). Each dataset is harvested straight to a BIDS dataset in
`<path>/<DATASET>/`, with the same selection options as `bx openneuro
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
```

Bucket copies are rarely valid BIDS as they are, so each dataset is
curated (see `brainspresso/datasets/INDI`): FCON1000's per-site folders
are merged with site-prefixed labels (`sub-AnnArborA04111`), runs and
typos in file names are fixed and repetition times read from the NIfTI
headers; multi-site participant tables are merged with a `site` column;
missing descriptions, READMEs and sidecars are generated; entities are
put in BIDS order; and common metadata defects (numbers stored as text,
slice timing in milliseconds, invalid JSON, sessions tables without
`session_id`...) are repaired after download (`brainspresso/bids/repair.py`).
Each dataset's `README` lists what was changed.
