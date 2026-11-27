# Brainspresso

A single application to **harvest** (download), **roast** (convert to BIDS),
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
┃       Brainspresso : Harvest, Roast and Grind public neuroimaging datasets        ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

 • Plantation : A public neuroimaging dataset supported brainspresso
 • Harvest    : Download raw data
 • Roast      : Convert source data to BIDS
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
* Roast `--sidecars yes|no|only` (default `yes`): `yes` writes the
  sidecars; `no` skips the JSON sidecars (bval/bvec and tables are still
  written); `only` writes the sidecars and metadata only, not the
  images. It replaces `--json yes|no|only` and `--no-json` (`--sidecars
  no`), which still work, with a deprecation warning.

A dataset cannot be composted while it is harvested or roasted (also from
other machines that share its filesystem), and the other way round: the
second command fails at once, naming the first one (`--lock-wait SECONDS`
waits for it; harvest and compost still accept the deprecated
`--wait SECONDS`). Each run leaves a record in `.brainspresso/runs/` while it
runs; compost refuses to start while records of other runs are there (a
crashed run, or a filesystem that does not share locks between machines:
`--ignore-runs` once you have checked that nothing runs).

Files are locked while they are read or written, with lock files in
`.brainspresso/locks/` (Linux, macOS and Windows; see
`brainspresso/actions/locks.py` for filesystems). `BRAINSPRESSO_LOCKS=0`
disables every lock, for a filesystem without lock support: the run
lock, the locks of files, and the lock of each partial download (two
downloads of one file could then write it at once). It is unsafe while
other commands use the dataset.

## OpenNeuro: any dataset, or a filtered subset of it

OpenNeuro datasets are already in BIDS, so `harvest` places them
in the raw BIDS dataset of `<path>/OpenNeuro/<dataset>/` (or `--out`):
`rawbids/`, or the folder itself with `--layout flat`. The dataset's own
`sourcedata/` and `derivatives/` (`--folders`) go next to it.
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
untouched copies are kept in `sourcedata/openneuro/`), and field maps
whose `IntendedFor` targets were not downloaded lose them; disable with
`--no-trim`. Each run is recorded in `.brainspresso/manifest.json`
(snapshot, filters, files, URLs and checksums, and how each file got
there: `direct`, `roasted` or `generated`).

Placement is *hybrid*: images and recordings are downloaded straight to
their place in the BIDS dataset, while metadata (sidecars, tables,
`events.tsv`, `bval`/`bvec`, README, descriptions) is downloaded to
`sourcedata/openneuro/` under its path in the snapshot (an untouched,
*pristine* copy), then **roasted** into the dataset (repaired, as for
INDI below, and trimmed to what was downloaded; `--no-repair` keeps the
content of the pristine copies, also on `roast`, or `repair = false` in
a config file). `bx openneuro roast ds000224 /data` roasts the metadata
again from those copies, without the network (after deleting or editing
a file, or with `--subs`, `--sidecars`). `--sidecars no` does not
download the JSON sidecars of the data files, `--sidecars only` only
downloads metadata (no images; tables and field maps are still trimmed to
the images selected). A harvest that runs again skips what did not
change, and a roasted file you edit is kept (`--if-exists overwrite`
replaces it). `bx openneuro roast` repairs by default, also what a
`harvest --no-repair` placed, unless a config file says `repair = false`.

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
login needed). Each dataset is harvested to the raw BIDS dataset of
`<path>/<DATASET>/` (see [Local layout](#local-layout)), with the same
selection options as `bx openneuro harvest` (`--subs`, `--sessions`,
`--datatypes`, `--suffixes`, `--tasks`, `--include`/`--exclude`,
`--where`, `--folders derivatives`, `--sidecars`, `--dry-run`...) plus
`--sites` for multi-site datasets, and with the same hybrid placement:
images go straight to their place, metadata to
`sourcedata/fcp-indi/<bucket key>` and is then roasted (repaired, filled
in, trimmed; `bx <project> roast` does it again). Only the selected
subjects are listed, so small selections of large datasets start
immediately.

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
are repaired when the metadata is roasted (`brainspresso/bids/repair.py`).
Sidecar templates listed in `templates/<project>/_fill.json` (e.g. scan
parameters from protocol printouts) add the keys that the bucket's
sidecars lack, never changing a key the bucket gives.
Each dataset's `README` lists what was changed.

## Compost: clean up a local dataset

Every dataset has a `compost` command. By default, it deletes the source
data (`<path>/<DATASET>/sourcedata/`) once it has been roasted into BIDS
(in either layout),
since the archives it contains are usually only taking up space. It
refuses to do so if no BIDS subject is found (use `--force` to override).
The pristine metadata of datasets hosted as BIDS (OpenNeuro, INDI:
`sourcedata/openneuro/`, `sourcedata/fcp-indi/`) is kept, since `roast`
reads it and it is small; `--sourcedata` deletes it too (the next
harvest downloads it again).

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
only list what is left. In a dataset hosted as BIDS, the metadata that is
left is then roasted again (field maps lose the `IntendedFor` targets
that were deleted); nothing deleted is re-created.

```shell
bx ixi compost /data --exclude-subs 012 145      # delete two subjects
bx ixi compost /data --suffixes T1w T2w           # only keep T1w and T2w
bx abide1 compost /data --where "age>=18" --derivatives
bx openneuro compost ds000224 /data --datatypes anat
```

`--derivatives` applies the subject and session filters to
`derivatives/*/` too. Files to delete are confirmed interactively
(`--yes` to skip, `--list` to print them first).

## Config files: defaults of the options

Most options can be given defaults in TOML files, so that a command line
only says what differs. The values come from, later winning:

1. the built-in defaults;
2. `~/.brainspresso/config.toml` (the user file; `$BRAINSPRESSO_CONFIG_HOME`
   replaces `~/.brainspresso`);
3. `<path>/.brainspresso/config.toml` (the root of all datasets: the `path`
   argument, else a `path` set in the user file, `--config` or the
   environment, else `$BDP_PATH`, else the current folder);
4. `<path>/<DATASET>/.brainspresso/config.toml` (the dataset folder, or
   `--out`);
5. the files of `bx --config FILE` (before the dataset name, repeatable,
   applied in order; `.yaml`/`.yml` files too if PyYAML is installed);
6. `BRAINSPRESSO_OPT_*` environment variables;
7. the command line.

Sections, the most specific winning within one file: `[defaults]` (every
command), `[<command>]` (e.g. `[roast]`, for every dataset),
`[<dataset>]` (every command of one dataset) and `[<dataset>.<command>]`.
Datasets are named as on the command line (`[abide1.roast]`,
`[unc-wisconsin]`, case-insensitive); OpenNeuro takes one more level for
an accession (`[openneuro.ds000224.harvest]`, while `[openneuro.harvest]`
applies to every accession). Keys are the option names, without `--`
(`exclude-keys` or `exclude_keys`); `no-<flag> = true` is
`<flag> = false`.

```toml
# ~/.brainspresso/config.toml
[defaults]
path = "/data/bids"        # root of all datasets
level = "info"

[harvest]
jobs = 4
if-exists = "different"

[roast]
jobs = 2

[ixi]
keys = ["T1w", "T2w"]      # harvest and roast of IXI

[abide1.roast]
exclude-keys = ["rest"]

[adhd200.harvest]
no-bids = true             # --no-bids

[openneuro.ds000224.harvest]
tag = "1.0.4"
datatypes = ["anat"]
```

The same values as environment variables: `BRAINSPRESSO_OPT_<KEY>`
(`[defaults]`), `BRAINSPRESSO_OPT_<SECTION>__<KEY>`, with `__` between the
parts of a section and `_` for `-`, case-insensitive:
`BRAINSPRESSO_OPT_HARVEST__JOBS=4`,
`BRAINSPRESSO_OPT_ABIDE1__ROAST__EXCLUDE_KEYS="rest anat"`,
`BRAINSPRESSO_OPT_NKI_RS__HARVEST__JOBS=2`. Several values are separated
by spaces; a value that holds spaces goes in quotes, which are removed
(`BRAINSPRESSO_OPT_COMPOST__WHERE='"age >= 60" sex==F'`); backslashes
and quotes inside a value are kept (`sub-0\d*`, `C:\data`, `O'Brien`).
Only whole values can be quoted: `"a b"c` is two values, `a b` and `c`.

Some options are never read from a config file: confirmations of
deletions (`--yes`, compost `--force`, `--ignore-runs`), secrets
(`--password`, `--token`: use their environment variables or the prompt)
and `--config`, nor the deprecated `--site-layout`, `--bidsify`, `--wait`
and `--json` (use `layout`, `bids`, `lock-wait` and `sidecars`). `path` and `out` go together: when the command line
gives either, neither comes from a config file. A root or dataset file
cannot set them (they locate it), and `out` only goes in the section of
one dataset. An unknown section or key, or a value of the wrong type, is
an error that names the file and the section; a value of `[defaults]` or
`[<command>]` that is not a choice of a dataset (`layout = "sites"` for
IXI) is ignored for that dataset, with a warning if it fits no dataset. `[profiles.<name>]` and `grind` tables
are reserved for the grind stage and ignored for now.

```shell
bx config show                         # the values of every file
bx config show ixi harvest --jobs 2    # every option of a command, and
                                       # where its value comes from
bx config show openneuro harvest ds000224
bx config show openneuro               # the values for every accession
bx config paths ixi                    # the files looked up, and which exist
bx config paths openneuro compost ds000224 --path /data
```
