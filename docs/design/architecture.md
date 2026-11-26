# Brainspresso architecture (refactor plan)

Status: **draft, under implementation** on branch `claude/refactor/datasets-core`.
This document is the reference that each refactor step follows. Update it when
a decision changes. Revision 2 folds in a contract review (§10).

## 1. What brainspresso does

One command line tool that, for many public neuroimaging datasets:

| Stage       | Command          | Input → output |
|-------------|------------------|----------------|
| **harvest** | `bx <ds> harvest`| remote host(s) → `sourcedata/` (or BIDS files placed directly) |
| **roast**   | `bx <ds> roast`  | `sourcedata/` → raw BIDS dataset (+ distributed derivatives) |
| **grind**   | `bx <ds> grind`  | raw BIDS dataset → `derivatives/<pipeline>/` |
| **compost** | `bx <ds> compost`| deletes source data, or BIDS data outside a filter |

Plus cross-dataset commands (`bx grind --datasets ...`), see §7.

### Things the design must allow (seen in merged and pending work)

* A dataset has **several sources**, possibly one per data category
  (ABIDE-I: s3 / nitrc / xnat; IXI: nitrc / imperial; ABIDE-II: images from
  S3, phenotypes from NITRC). Hosts go offline; sources must be swappable.
* Sources are **generic backends**: static URLs, S3 prefixes (fcp-indi,
  openneuro), Girder, XNAT, OpenNeuro GraphQL, Dataverse (GSP). Plausible
  next: signed S3 (HCP), datalad/git-annex, portals with their own clients
  (NDA, Synapse), data the user downloaded themselves (ADNI).
* Source data comes as **tarballs, folders of files, DICOM, or (nearly) BIDS
  trees**, sometimes mixed in one `sourcedata/`.
* Some datasets need **no roast** (hosted as BIDS: OpenNeuro, INDI), some can
  only be roasted (ADNI).
* **Derivatives** are produced by roast (FreeSurfer, UNC registrations,
  Bullitt vessels) and by grind.
* Non-MRI modalities (PET, CT, EEG, MEG, microscopy), **non-human** subjects,
  multi-site datasets with several local layouts, possibly remote storage
  (`upath`, as prototyped in `balbasty/feat/wiip-grind`).
* Bundled/curated metadata shipped in the package (IXI.xls, Bullitt
  demographics), with `sourcedata/` taking precedence when present.

### Non-goals / things not to impose

* No fixed sequence of roast phases (meta / raw / freesurfer ...).
* No global key hierarchy; no MRI-only assumption anywhere (filters included).
* No single source per dataset; no "source data is a tarball"; no "every
  source is an HTTP GET with a known size".
* No mandatory roast, no mandatory harvest.
* No fixed CLI signature: datasets can add positional arguments, options or
  commands.
* No in-house cluster scheduler; no node/connect wiring syntax.
* No `pathlib.Path`-only APIs in contracts (`os.utime`, mtime-based
  refresh) where remote paths are plausible.

## 2. On-disk layout

`<root>/<DS>/` with one of these layouts, chosen with `--layout` (stored in
`<DS>/.brainspresso/manifest.json`; later commands reuse it, detect it from
the tree when there is no manifest, and fail if `--layout` conflicts):

**study** (BIDS ≥ 1.10.1 `DatasetType: "study"`, `rawbids/` from 1.11.2):

```
<DS>/
  dataset_description.json     DatasetType "study"
  sourcedata/                  harvested files (archives, folders, pristine metadata)
  rawbids/                     raw BIDS dataset (DatasetType "raw")
    dataset_description.json  participants.tsv  README  phenotype/  sub-*/  .bidsignore
  derivatives/<pipeline>/      derivative datasets, SourceDatasets: [{"URL": "../../rawbids/"}]
  .brainspresso/               manifest, pristine tables, per-task state
```

**flat** (classic BIDS: the raw dataset is the root):

```
<DS>/
  dataset_description.json     DatasetType "raw"
  participants.tsv  README  phenotype/  sub-*/  .bidsignore
  sourcedata/
  derivatives/<pipeline>/      SourceDatasets: [{"URL": "../../"}]
  .brainspresso/
```

**sites** (INDI: one BIDS dataset per site under a non-BIDS parent) is a
*composition* of child layouts (each site is a study or flat layout; a site
target can itself be a derivative, e.g. `derivatives/niak`), with one state
directory per child and a parent holding the README and a merged
participants table.

The historical `<DS>/rawdata/` layout is invalid BIDS and is **removed**,
without migration (pre-release project).

`brainspresso/bids/layout.py` owns all paths:

* `layout.raw`, `layout.source`, `layout.state`, `layout.phenotype`,
  `layout.bidsignore`, `layout.dataset_description(kind)`;
* `layout.derivative(name)` returns a **sub-layout** (its own `source`, for
  derivatives unpacked from archives, e.g. OASIS FreeSurfer, and its own
  `.bidsignore`);
* `layout.relative(path) -> (dataset, relpath)` maps any path to the BIDS
  dataset that contains it (raw, a derivative, a site) and its path inside it.

No module builds `root / "rawdata"` (or any other fixed folder) itself.

## 3. Filtering: `Selection` everywhere

`brainspresso.bids.select.Selection` is the single filter vocabulary for
harvest, roast, compost and grind. Before anything else depends on it, it must:

* be **modality-agnostic**: today an empty selection drops PET `*_blood`,
  EEG `*_electrodes`, MEG sidecars and `channels.tsv`. Companion and
  inherited files are derived from BIDS rules (schema-driven where
  possible), not from an MRI list;
* treat **directory-format recordings** (`*.ds`, `*.ome.zarr`, `*.mefd`, ...)
  as one unit;
* offer a generic `entities={name: values}` filter instead of one field per
  entity (subjects/sessions/tasks stay as shortcuts);
* expose two operations: `select(listing)` (whole listing, adds inherited
  metadata) and a **per-path predicate** `keeps(relpath, dataset_kind)`
  that may answer "maybe" for partially known paths (§5 claims).

Dataset-specific **keys** (`raw`, `meta`, `fs`, ...) remain as per-dataset
aliases expanding to selection predicates and/or non-BIDS categories
(metadata, phenotypes, a derivative pipeline, source-only files). The global
MRI tree in `utils/keys.py` goes away.

Later (grind): conditions on sidecar JSON fields (`RepetitionTime`), regex
and glob conditions, as prototyped in `wiip-grind`
(`bids/conditions.py`, `bids/filter.py`).

## 4. Sources and harvest

* A **source** declares what it `provides` (dataset keys), the **roaster**
  that understands what it writes to `sourcedata/`, `list(selection, keys)`
  and `fetch(entries, layout, options)`. `fetch` defaults to the shared
  `mirror` (HTTP downloads through `DownloadManager`); sources with their own
  transport (datalad, NDA/Synapse clients, server-built XNAT zips) override it.
* One entry type: `RemoteEntry(path, url=None, size=None, mtime=None,
  digests={}, meta={})`. Everything but `path` is optional.
* Sources are chosen **per key**, with an ordered default list and fallback
  (ABIDE-II: `{"raw": ["s3"], "meta": ["nitrc"]}`). `--source` overrides.
* `LocalSource`: data the user put in `sourcedata/` themselves (ADNI, DUA
  data). It lists nothing and fetches nothing; roast still works.
* A dataset supplies a **label mapping** between host identifiers and BIDS
  labels (`OAS30001` ↔ `sub-0001`, INDI site codes) so that `list(selection)`
  can prune listings.
* Credentials: one helper (env var, then prompt), one async NITRC
  authentifier; `insecure: bool` instead of
  `get_opt={ssl|verify|verify_ssl: False}`.

### Sources hosted as BIDS: hybrid placement

* Files that are **never repaired or trimmed** (images, recordings) are
  downloaded directly to their final BIDS path (after path remapping).
* Files that **may be repaired or trimmed** (JSON, TSV including events,
  bval/bvec, README, descriptions) are downloaded to `sourcedata/` under
  their **remote, unremapped** path, then roasted into the BIDS dataset.
* Metadata roast runs after image placement (some repairs read siblings:
  `_phasediff`, `IntendedFor`) and again after compost.
* Compost keeps pristine metadata of hosted datasets by default
  (`--sourcedata` on a hosted dataset says the next harvest re-fetches it).
* The manifest records each file's origin: `direct`, `roasted`, `generated`.

## 5. Roast: roasters + one action runner

* A **roaster** is a plain callable `roaster(ctx) -> Iterator[Action |
  Roaster]`. It reads source data through `utils/archive.py` (tar, folder,
  zip, DICOM folder). No base class, no fixed phases. Optional
  `roaster.detect(ctx) -> bool` lets roast pick the roaster that matches what
  is in `sourcedata/` without a harvest manifest (mixed tarball/folder
  `sourcedata/` keeps working).
* **Outputs.** An action declares either `dst` (one path or a list: DWI
  nii+bval+bvec) or a **claim** (output directory + partial entities) when
  names are only known after running (dcm2niix, FreeSurfer trees,
  OASIS-III sessions parsed from member names). After running it reports its
  actual outputs. Multi-output rule: run if any output may be selected,
  write all outputs, apply `IfExists` to the set.
* `Action.kind` (`data` / `sidecar` / `table`, default from suffix) drives
  the JSON yes/no/only policy; `Action.tag` groups progress lines.
* **Runner** (`brainspresso/actions/runner.py`): consumes each action
  **before requesting the next** (actions may close over open archive
  handles and are not picklable), applies `Selection.keeps` on outputs or
  claims, `IfExists`, policies, counters and display. Parallelism is at
  **shard** level: a roaster yields sub-roasters (one per archive or
  subject chunk), which also gives progress totals (`ctx.progress(total=)`).
* **Dataset-level tables**: roasters write complete tables
  (participants, sessions, scans) to `.brainspresso/orig/`; the common trim
  step derives the visible ones from what is on disk. No table is
  accumulated across actions. A `finalize(ctx)` hook sees the finished tree
  (e.g. sites computed from a tar scan).
* Common steps after every roast, also for hosted datasets: repair, trim,
  `dataset_description` for the layout, `.bidsignore`, manifest.
* For gz tarballs, listing members is a full decompression: early pruning by
  selection (`selection.keeps_subject(label)`) is expected there.
* Shared helpers: `ExtractMember(archive, member, dst)`, converters registry
  (Analyze, NRRD, MetaImage, DICOM, FreeSurfer), `csv_to_tsv` + `TableMapper`,
  `ctx.resource(name)` (`sourcedata/` first, then package templates).

## 6. Dataset registration

A dataset is a class (generalising INDI's `Project`) with overridable
methods, registered with `register(DatasetClass)`:

```python
class IXI(Dataset):
    name = "ixi"; folder = "IXI"; help = IXI_HELP
    keys = KeyAliases({...})
    sources = {"nitrc": NitrcIXI(provides={"raw"}, roaster=roast_folders),
               "imperial": ImperialIXI(provides={"raw", "meta"},
                                       roaster=roast_tarballs),
               "bundled": Bundled(provides={"meta"})}
    defaults = {"raw": ["nitrc", "imperial"], "meta": ["bundled"]}
```

`register` creates the cyclopts app with default `harvest`, `roast`,
`compost` and `info` commands built from shared option groups
(`SelectionOptions`, `DownloadOptions`, `LogOptions`, `LayoutOptions`). A
dataset overrides or adds any command. Optional dependencies keep using
`utils/deps.requires`.

## 7. Grind

* A **pipeline** is dataset-agnostic: it consumes a raw BIDS dataset (plus
  outputs of the pipelines it requires) and writes
  `derivatives/<pipeline>/` with `GeneratedBy` provenance.
* Configuration is a pydantic model with documented fields (CLI help and
  config files generated from it), as prototyped for SPM Segment in
  `wiip-grind`. Output paths are BIDS entity templates (with `ses`).
* A pipeline is made of **stages**, each with its own unit level
  (`session`, `subject`, `group`) and declared inputs from earlier stages
  (fan-in), e.g. FreeSurfer longitudinal: cross (session) → base (subject) →
  long (session). Pipelines declare `requires=(...)` on other pipelines.
  Everything stays declarative; no wiring syntax.
* Skip/redo: per-unit **done-marker + config hash** (BIDS-App outputs
  cannot be enumerated; outputs depend on config).
* Partial failures: a group stage runs on the units that succeeded, unless
  the pipeline requires all; the failure list is recorded.
* Cross-dataset group outputs live outside any dataset, in a
  `<root>/derivatives/<pipeline>/` collection with `SourceDatasets` pointing
  at each input dataset.
* `BidsAppPipeline` wraps any BIDS App
  (`app bids_dir out_dir participant --participant_label X`).
* Execution: a thin `submit(task, resources) -> handle` protocol
  (`resources`: CPUs, memory, GPU, walltime, container), implemented over
  `concurrent.futures` (local pool), `submitit` (SLURM/SGE/PBS, job arrays,
  `afterok` dependencies) and optionally Dask/Ray. Profiles in a user config
  file. Containers: Docker locally, Apptainer on HPC.
* State: **one state file per task** under `.brainspresso/` (concurrent jobs
  never write the same file); the manifest is an index rebuilt from them.
* Roast shares reporting, the state format and the submit protocol with
  grind (local backend only), not the `Action` class.
* `bx <ds> grind <pipeline>` applies dataset defaults/overrides (which T1w,
  species/template, excluded subjects). `bx grind <pipeline> --datasets ...
  --where ...` resolves each dataset and submits **one** task graph.
* A cross-dataset index (all subjects, with a `dataset` column) is built
  from each raw dataset's `participants.tsv`.

## 8. Implementation steps

Each step lands as commits on the refactor branch with its tests green.

| # | Step | Tier | Status |
|---|------|------|--------|
| 1 | Characterization tests for roasts without tests (COBRE, OASIS-I/II/III, ABIDE-II, ABIDE-I nitrc); known bugs as strict xfail | Low-Effort Opus | done (300 passed, 16 xfailed) |
| 2a | `Selection`: modality-agnostic companions, directory formats, generic entities, per-path `keeps` | High-Effort Opus | |
| 2b | Manifest/state schema; `Layout` (study / flat / sites, sub-layouts, `relative`); runner + action outputs/claims/kind/tag + shards; tables via `.brainspresso/orig` + trim; pilot on UNC-Wisconsin, IXI, OASIS-III and one INDI project (hybrid placement); remove `rawdata/` from compost/harvest | High-Effort Opus, Fable review | |
| 3 | Sources: `RemoteEntry`, `provides`/`roaster`/`fetch`, per-key defaults, `LocalSource`, label mapping, credentials; drop request-option shim | High-Effort Opus | |
| 4 | `Dataset` + `register` (absorbs INDI `Project`, `add_compost`); shared option groups | High-Effort Opus | |
| 4g | Grind contract written down (pipelines, stages, state, submit protocol), doc + interfaces only, so that ports do not bake in assumptions | Fable plan | |
| 5 | Port remaining datasets one by one; fix known bugs | Low-Effort Opus each | |
| 6 | Tabular helpers, converter registry, archive helpers | Low-Effort Opus | |
| 7 | Grind implementation: local backend, one BIDS-App pipeline, submitit; then SPM Segment from `wiip-grind` | High-Effort Opus | |
| CI | BIDS-validator check of roasted fixtures in both layouts | Low-Effort Opus | |

### Known bugs to fix while porting (step 5)

Each one has a strict `xfail` test in `tests/test_roast_*.py`, so it shows
up as an XPASS failure once fixed (then drop the marker).

* ABIDE-II `_make_raw`: `return` instead of `continue` in the member loop.
* ABIDE-II `_run`: `rawkeys` built from `allleaves`, ignoring `keys`
  (same pattern, untested, in `ABIDE/I/bidsifier_xnat.py`).
* ABIDE-II participants mapper: stray backtick in `CURRENT_MED_STATUS`.
* ABIDE-II `make_participants` reads ABIDE-I's `Phenotypic_V1_0b.csv`
  (suspected, no xfail).
* ABIDE-I nitrc: phenotype glob misses `phenotypic_MAX_MUN.csv`.
* `participant_id` without `sub-` prefix: ABIDE-I, ABIDE-II, COBRE.
* OASIS-I: missing comma in `PARTICIPANTS_HEADER` (`'handednessage'`);
  `do_tal` looks up members without `.img` (Talairach never written);
  avg/tal `Sources` miss `_` before `run-`; empty xlsx cells written as `''`;
  relabel saved to `dst` instead of the action's path.
* OASIS-II `_raw_skip_path`: `if skip_subject(id): True` lacks `return`.
* OASIS-III: sessions written as `sub-X/sessions.tsv` instead of
  `sub-X/sub-X_sessions.tsv`; `set(*map(lower_keys, exclude_keys))` raises
  with more than one exclude key (same construct in ABIDE-I/II).
* Harvest (not covered by tests, needs network): ABIDE-II and ABIDE-I xnat
  use the sync `nitrc_authentifier` with aiohttp sessions.

## 9. Open questions

* Default layout: `study` (proposed) or `flat`.
* Validator/tooling support for `DatasetType: "study"` and `rawbids/` (new
  in BIDS 1.11.2): to check; `rawbids/` itself is a plain raw dataset.
* Grind runtime for SPM (MATLAB, compiled standalone SPM, or Python port).

## 10. Revision log

* r1: initial plan.
* r2: contract review. Selection made modality-agnostic before use (verified:
  an empty selection dropped PET/EEG/MEG companions); action claims and
  multi-output rules; sequential runner with shard-level parallelism;
  tables via `.brainspresso/orig`; per-key sources with `fetch` and
  `LocalSource`; hybrid placement classified by "may be repaired"; layout
  sub-layouts, `relative`, sites as composition; grind stages, done-markers,
  resources in the submit protocol, per-task state; step order revised.
