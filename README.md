# Brainspresso

A single application to **harvest** (download), **roast** (bidsify)
and **grid** (preprocess) public datasets.

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
