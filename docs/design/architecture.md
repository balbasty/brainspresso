# Brainspresso architecture (refactor plan)

Status: **draft, under implementation** on branch `claude/refactor/datasets-core`.
This document is the reference that each refactor step follows. Update it when
a decision changes.

## 1. What brainspresso does

One command line tool that, for many public neuroimaging datasets:

| Stage       | Command          | Input → output |
|-------------|------------------|----------------|
| **harvest** | `bx <ds> harvest`| remote host(s) → `sourcedata/` (or BIDS files placed directly) |
| **roast**   | `bx <ds> roast`  | `sourcedata/` → raw BIDS dataset |
| **grind**   | `bx <ds> grind`  | raw BIDS dataset → `derivatives/<pipeline>/` |
| **compost** | `bx <ds> compost`| deletes source data, or BIDS data outside a filter |

Plus cross-dataset commands (`bx grind --datasets ...`), see §7.

### Things the design must allow (seen in merged and pending work)

* A dataset has **several sources**, possibly one per data category
  (ABIDE-I: s3 / nitrc / xnat; IXI: nitrc / imperial; ABIDE-II: images from
  S3, phenotypes from NITRC). Hosts go offline; sources must be swappable.
* Sources are **generic backends**: static URLs, S3 prefixes (fcp-indi,
  openneuro), Girder, XNAT, OpenNeuro GraphQL. They list files with size,
  mtime and digests.
* Source data comes as **tarballs, folders of files, or (nearly) BIDS trees**.
* Some datasets need **no roast** (hosted as BIDS: OpenNeuro, INDI), some can
  only be roasted from data the user obtained themselves (ADNI).
* **Derivatives** are produced by roast (distributed derivatives: FreeSurfer,
  UNC registrations, Bullitt vessels) and by grind.
* Non-MRI modalities (PET, CT, EEG), **non-human** subjects (macaques),
  multi-site datasets with several local layouts.
* Bundled/curated metadata shipped in the package (IXI.xls, Bullitt
  demographics), with `sourcedata/` taking precedence when present.

### Non-goals / things not to impose

* No fixed sequence of roast phases (meta / raw / freesurfer ...).
* No global key hierarchy; no "MRI only" assumption.
* No single source per dataset; no "source data is a tarball".
* No mandatory roast, no mandatory harvest.
* No fixed CLI signature: datasets can add positional arguments
  (OpenNeuro dataset ID), options (sites, parts, logins) or commands.
* No in-house cluster scheduler.

## 2. On-disk layout

`<root>/<DS>/` with one of two layouts, chosen with `--layout` (global
option, stored in `<DS>/.brainspresso/manifest.json` so later commands reuse it):

**study** (BIDS ≥ 1.10.1 `DatasetType: "study"`, `rawbids/` from 1.11.2):

```
<DS>/
  dataset_description.json     DatasetType "study"
  sourcedata/                  harvested files (archives, folders, pristine metadata)
  rawbids/                     raw BIDS dataset (DatasetType "raw")
    dataset_description.json  participants.tsv  README  phenotype/  sub-*/
  derivatives/<pipeline>/      derivative datasets, SourceDatasets: [{"URL": "../../rawbids/"}]
  .brainspresso/               manifest, pristine copies of edited tables
```

**flat** (classic BIDS: the raw dataset is the root):

```
<DS>/
  dataset_description.json     DatasetType "raw"
  participants.tsv  README  phenotype/  sub-*/
  sourcedata/
  derivatives/<pipeline>/      SourceDatasets: [{"URL": "../../"}]
  .brainspresso/
```

The historical `<DS>/rawdata/` layout (raw subjects in `rawdata/`, dataset
files next to it) is invalid BIDS and is **removed**, without migration (the
project is pre-release).

Layout `sites` (INDI: one BIDS dataset per site under a non-BIDS parent) is a
third layout handled by the same object.

All code obtains paths from a `Layout` object (`brainspresso/bids/layout.py`):
`layout.raw`, `layout.source`, `layout.derivative(name)`, `layout.state`,
`layout.dataset_description(kind)`. No module builds `root / "rawdata"` itself.

## 3. Filtering: `Selection` everywhere

`brainspresso.bids.select.Selection` (subjects, sessions, datatypes,
suffixes, tasks, include/exclude globs; `--where` queries on participants)
is the single filter vocabulary for harvest, roast, compost and grind.

* Roast filters **on outputs**: every roast action has a BIDS destination,
  so the runner checks `selection` against it. Datasets may *also* prune
  early for speed (`selection.keeps_subject(label)`), never required.
* Dataset-specific **keys** (`raw`, `meta`, `derivatives`, `fs`, ...) remain,
  as aliases defined per dataset that expand to selection predicates and/or
  non-BIDS categories (metadata, phenotypes, a derivative pipeline,
  source-only files). The global MRI tree in `utils/keys.py` goes away.
* Later (grind): conditions on sidecar JSON fields (`RepetitionTime`,
  `MagneticFieldStrength`) and regex/glob conditions, as prototyped in
  `balbasty/feat/wiip-grind` (`bids/conditions.py`, `bids/filter.py`),
  folded into `Selection`.

## 4. Sources and harvest

* One entry type, `RemoteEntry(path, url, size, mtime, digests, meta)`
  (today: `RemoteEntry`, `S3Object`, `GirderFile`, `XNATFile`).
* A source is an object with `list(selection, keys) -> Iterable[RemoteEntry]`
  and an optional `auth`. Implementations: `StaticURLs`, `S3Prefix`,
  `Girder`, `XNAT`, `OpenNeuro`.
* One `mirror(entries, dst, keep=, rename=, **download_opts)` builds the
  downloaders (today: `fcp.mirror`, `girder.mirror` and per-dataset loops).
* Credentials: one helper (env var, then prompt) used by every source; one
  async NITRC authentifier; `insecure: bool` instead of
  `get_opt={ssl|verify|verify_ssl: False}`.
* Each source declares the **roaster** that understands the layout it
  writes into `sourcedata/` (§5).

### Sources hosted as BIDS: hybrid placement

* Large files that are never modified (images, physio, events) are
  downloaded **directly to their final BIDS path** (after path remapping).
* Small metadata files (JSON, TSV, bval/bvec, README, descriptions) are
  downloaded to `sourcedata/` (pristine), then **roasted** (remap, repair,
  generated entries, trimmed tables) into the BIDS dataset.

This avoids doubling disk use, keeps remote size/digest checks valid for
the files placed directly, and makes repairs re-runnable (`bx <ds> roast`
exists for hosted datasets too and only touches metadata).

## 5. Roast: roasters + one action runner

* A **roaster** is any callable `roaster(ctx) -> Iterator[Action]`. It reads
  source data through `utils/archive.py` (tar, folder, later zip) and yields
  actions whose `dst` is a path inside `ctx.layout`. Roasters are free to
  organise their code as they want; there is no base class to inherit.
* `ctx` carries: `layout`, `selection`, dataset keys, `json` policy, bundled
  resource lookup (`ctx.resource(name)`: `sourcedata/` first, then the
  package templates), and per-dataset options (sites, parts...).
* One **runner** (`brainspresso/actions/runner.py`) executes the actions:
  `IfExists`, JSON yes/no/only policy (decided on the destination suffix),
  output filtering by `Selection`, progress/error/skip counters, tabular
  display, and an `Executor` (local pool only for roast: archives are read
  sequentially and actions are small).
* Common steps run after every roast, also for hosted datasets: repair
  (`bids/repair.py`), trimmed tables (`bids/tables.py`), `dataset_description`
  for the chosen layout, manifest.
* Shared action helpers: `ExtractMember(archive, member, dst)`,
  converters registry (Analyze, NRRD, MetaImage, FreeSurfer → NIfTI/GIfTI),
  `csv_to_tsv(inputs, dst, mapper)` with `TableMapper`.

## 6. Dataset registration

A dataset is a class (generalising INDI's `Project`) with overridable
methods, registered with `register(DatasetClass)`:

```python
class IXI(Dataset):
    name = "ixi"; folder = "IXI"; help = IXI_HELP
    keys = KeyAliases({...})                       # dataset-specific
    sources = {"nitrc": NitrcIXI, "imperial": ImperialIXI}
    default_source = "nitrc"
    roasters = {"nitrc": roast_folders, "imperial": roast_tarballs}
```

`register` creates the cyclopts app with default `harvest`, `roast`,
`compost` and `info` commands built from shared option groups
(`SelectionOptions`, `DownloadOptions`, `LogOptions`, `LayoutOptions`).
A dataset overrides or adds any command (OpenNeuro: dataset ID argument;
ADNI: URL list; INDI: `--layout sites`). Optional dependencies keep using
`utils/deps.requires`.

## 7. Grind

* A **pipeline** is dataset-agnostic: it consumes a raw BIDS dataset (plus
  outputs of the pipelines it requires) and writes
  `derivatives/<pipeline>/` with `GeneratedBy` provenance.
* Configuration is a pydantic model with documented fields (CLI help and
  config files generated from it), as prototyped for SPM Segment in
  `balbasty/feat/wiip-grind`. Output paths are BIDS entity templates.
* Unit of work: `(pipeline, subject[, session])`, or `group`. Pipelines
  declare `requires=(...)`; the task graph is pipelines × units. Skip/redo
  follows the `IfExists` rules on outputs. No node/connect wiring syntax.
* `BidsAppPipeline` wraps any BIDS App
  (`app bids_dir out_dir participant --participant_label X`).
* Execution backends implement `concurrent.futures.Executor`: local process
  pool; SLURM/SGE/PBS via `submitit`; Dask/Ray optional. Profiles in a user
  config file. Containers: Docker locally, Apptainer on HPC. Task status
  and provenance in the manifest so interrupted runs resume.
* `bx <ds> grind <pipeline>` applies dataset defaults/overrides (which T1w,
  species/template, excluded subjects). `bx grind <pipeline> --datasets ...
  --where ...` resolves each dataset and submits **one** task graph.
* A cross-dataset index (all subjects, with a `dataset` column) is built
  from each raw dataset's `participants.tsv`.

## 8. Implementation steps

Each step lands as commits on the refactor branch with its tests green.

| # | Step | Tier | Status |
|---|------|------|--------|
| 1 | Characterization tests for roasts without tests (COBRE, OASIS-I/II/III, ABIDE-II, ABIDE-I nitrc); known bugs as strict xfail | Low-Effort Opus | in progress |
| 2 | `Layout` (study / flat / sites) + action runner + Selection output filtering; pilot on UNC-Wisconsin and IXI; remove `rawdata/` from compost/harvest | High-Effort Opus, Fable review of contracts | |
| 3 | `RemoteEntry` + sources + `mirror` + credentials; drop request-option shim | High-Effort Opus | |
| 4 | `Dataset` + `register` (absorbs INDI `Project`, `add_compost`); shared option groups | High-Effort Opus | |
| 5 | Port remaining datasets one by one; fix known bugs | Low-Effort Opus each | |
| 6 | Tabular helpers, converter registry, archive helpers | Low-Effort Opus | |
| 7 | Grind core (pipeline model, executors), first BIDS-App pipeline, then SPM Segment from `wiip-grind` | High-Effort Opus, Fable plan | |

## 9. Open questions

* Default layout: `study` (proposed) or `flat`.
* Validator/tooling support for `DatasetType: "study"` and `rawbids/` (new
  in BIDS 1.11.2): to check; `rawbids/` itself is a plain raw dataset.
* Grind runtime for SPM (MATLAB, compiled standalone SPM, or Python port).
