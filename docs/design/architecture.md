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

Plus cross-dataset commands (`bx grind --datasets ...`) and `bx
percolate PLAN`, which runs or tracks a saved roast or grind plan, see §7.

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

Implemented (step 2b-i):

* `make_layout(kind, root, **kw)` (`study`, `flat`, `sites`) and
  `resolve_layout(root, layout=None) -> Layout`: the detected layout of
  an existing dataset, else `layout`, else `DEFAULT_LAYOUT` (`"study"`,
  one constant); `LayoutError` (a `ValueError`) if `--layout` conflicts
  with the detected one. `Layout.detect(root)`: manifest first, then the
  tree (`rawbids/` or a "study" description: study; `sub-*` or another
  description at the root: flat; site datasets under a folder that is
  not a dataset: sites), else `None`. Paths are accepted as
  `os.PathLike`; no `upath` yet.
* Beyond the list above: `layout.root` (`<DS>`), `layout.bids` (root of
  the BIDS dataset the layout describes: `raw`, or the derivative root),
  `layout.derivatives`, `layout.description`, `layout.manifest`,
  `layout.tasks`, `layout.original(path)` (pristine copy of a trimmed
  table in `<state>/orig/<path relative to root>`), `layout.datasets()`
  (itself, or each site) and `layout.resolve(relpath)`, which places a
  path of a flat BIDS listing (as hosts list them: `sub-*/...`,
  `sourcedata/...`, `derivatives/<name>/...`) in the layout. Hosted-BIDS
  harvests (OpenNeuro, INDI) go through `resolve`.
* A derivative sub-layout shares the state folder of its parent; its
  `raw` is the raw dataset it derives from and `source_url` the relative
  URL to it (`../../rawbids/` or `../../`).
* `sites`: `layout.child(name)` (created with `child_kind`, CLI
  `--site-layout`, to become `--layout sites:<kind>` in step 4, §6),
  `layout.children`; the parent's `raw` is the parent
  folder itself (README, merged `participants.tsv`); compost runs on
  each site.
* `layout.write_descriptions()` (after every harvest and roast): the
  study description (`DatasetType: "study"`, name and credits from the
  raw description) and, in every existing derivative description,
  `DatasetType: "derivative"` and a first `SourceDatasets` entry with
  the local URL of the raw dataset (a `../../` entry, meaning the raw
  dataset of a flat layout, is replaced). The raw description stays the
  template copy (or the hosted one) at `layout.raw`.

**State.** `<state>/manifest.json` (one per dataset; each site of a
`sites` layout has its own, and the parent has one that records the
layout of each site):

```json
{"schema": 1,
 "layout": "study | flat | sites",
 "children": {"<site>": "study | flat"},
 "runs": [{"stage": "harvest | roast | compost | grind",
           "date": "<ISO 8601 UTC>", "...": "stage-specific fields"}],
 "files": {"<path relative to <DS>>": {"url": "...", "size": 0,
           "digests": {"md5": "..."},
           "origin": "direct | roasted | generated"}}}
```

`origin` is written by hosted harvests today (`direct` for downloads,
`generated` for inline files); `roasted` is reserved for hybrid
placement (2b-iii). Per-task state: `<state>/tasks/<task id>.json`, see
§7. Both are defined in `bids/harvest.py` (`record_run`,
`write_manifest`, `write_task_state`, `read_task_states`).

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
  metadata) and a **per-path predicate** `keeps(relpath, partial=False)
  -> bool | None` that may answer "maybe" (`None`) for claims, i.e.
  partially known paths written as `dir/` or `dir/<name fragment>` (§5),
  plus `keeps_subject(label)` for early pruning.

Implemented (step 2a): built-in companion table (checked against
`bidsschematools` in an optional test), any directory inside a datatype
folder is a recording unit, `entities=` with `subs`/`sessions`/`tasks` as
shortcuts. Not yet: links outside inheritance (`IntendedFor` field maps,
ASL `m0scan`), a CLI flag for `entities`, filters on derivative datasets
beyond subject/session.

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

Implemented (step 3):

* `sources/base.py`. `RemoteEntry(path, url=None, size=None, mtime=None,
  digests={}, meta={})` (`path` a POSIX str, `relpath`/`name`
  helpers); an entry with `meta["content"]` is a file generated from
  the listing (written, not downloaded: `nitrc_experiments.json`,
  `sub2site.json`, UNC `folders.json`). `DownloadOptions(ifexists,
  jobs, chunk_size, insecure, dry_run, path, on_error)`. `mirror(entries,
  dst, keep=, rename=, **download_opts) -> Iterator[Downloader]` and
  `run_downloads(downloaders, options)` replace `fcp.mirror`,
  `Girder.mirror` and the per-dataset Downloader loops.
* `Source` (base class; `SourceProtocol` is the structural type):
  attributes `name`, `provides`, `roaster` (a function declared on the
  class is not a method), `labels`, `folder` (sub-folder of
  `layout.source`), `insecure`, `jobs`, plus per-run options (`user`,
  `password`, `token`, `sites`...) given to `__init__`, `configure(**o)`
  (copy; unknown options and None ignored, so a command offers its
  options to every source) or `with_options(**o)` (strict). Methods:
  `list(selection=None, keys=None) -> list[RemoteEntry]`, `fetch(entries,
  layout, options=None)` (default: `downloaders()` + `run_downloads`),
  `harvest(layout, selection, keys, options)` (list + fetch; hosted-BIDS
  sources override it), `auth()` (download hook or None, resolved at
  fetch time, never at declaration), `keeps_subject(selection, host_id)`,
  `destination(layout)`, `local_path(entry)`, `resolve(layout, entry)`
  (`layout` may also be the source folder itself). `SourceUnavailable`
  (host offline, listing refused) triggers the fallback.
* Keys given to sources: `Wants(keys, tree, exclude=)`, a predicate
  `wants(key)` over the dataset tree (the `utils.keys` nested dicts of the
  unported datasets, or a `KeyAliases`): a key is wanted if it is equal,
  above or below a selected key; `Wants.restrict(group)` limits it to the
  keys of one source's group. A `KeySelection` (its `wants`) or a plain
  list of names also work.
* Label mapping: `LabelMap(to_bids, to_host=None)` with `keeps(selection,
  host_id)` and `hosts(selection)`; `numeric_labels(width, prefix)`
  (`0051456` ↔ `51456`, `OAS30001` ↔ `0001`), `prefix_labels(code)`
  (INDI `Site.labels`, used to prune the bucket listing).
* Implementations: `StaticURLs(urls={key: [url | RemoteEntry]}, keep=)`,
  `CompositeSource(*parts)` (e.g. ABIDE-I xnat: XNAT images + NITRC
  phenotypes), `LocalSource` (lists/fetches nothing, `present(layout)`),
  `s3.S3Prefix(bucket, prefix, keep=, rename=)` and `fcp.FCP(prefix)`,
  `girder.GirderSource(rtype, rid, api=)` (hooks `keep_folder`, `keep`,
  `folder_entries`; `token`), `xnat.XNAT(project, server=)` (async
  `XNATClient`; hooks `keep_experiment`, `keep_scan`, `keep_assessor`,
  `extra`; one server-built `<experiment>/<id>.tar.gz` per scan or
  assessor), `nitrc.NITRC` (static URLs behind the NITRC login,
  `insecure` and one job by default), `openneuro.OpenNeuro(dataset, tag,
  route, token)` (GraphQL with S3 fallback; fetches through
  `bids.harvest.harvest`), `dataverse.Dataverse(files={key: {name: doi}},
  server)` (token header).
* Per-key choice: `plan_sources(sources, defaults, keys, override=None)
  -> [(keys, candidates)]` (defaults per key, in order; keys without
  defaults are only harvested with `--source`, which keeps the keys the
  named sources provide) and `harvest_sources(sources, defaults, layout,
  selection, keys, options, override) -> {key: source used}` (next
  candidate on `SourceUnavailable`); `configure_sources(sources,
  **options)`; `select_roaster(sources, ctx)` (first `roaster.detect(ctx)`,
  else first roaster) for step 4's `register`.
* Credentials (`sources/credentials.py`): `get_secret(what, value, env,
  hidden=, option=)` (value, then environment variables, then a prompt
  if stdin is a terminal, else `CredentialsError` naming the variables
  and option), `get_credentials(service, user, password, env_user=,
  env_password=)`, `nitrc_credentials`; picklable async hooks
  `NitrcAuth` (NITRC form login, POST body) and `XnatAuth` (`JSESSION`
  with basic credentials). Downloads take `insecure=True`; TLS keys in
  `get_opt` raise, the `_normalize_request_options` shim is gone.
* Datasets declare `sources()` and `DEFAULTS` next to their harvest
  command (ABIDE-I `datasets/ABIDE/I/sources.py`: `{"raw": ["s3",
  "nitrc"], "meta": ["s3", "nitrc"]}`, derivatives with `--source xnat`;
  CoRR: s3 then nitrc; IXI: images nitrc then imperial, meta `bundled`
  (`LocalSource`); ABIDE-II: nitrc, `--source s3` opt-in; OASIS-III:
  xnat; UNC-Wisconsin, Bullitt: girder; COBRE: nitrc; GSP: dataverse;
  OASIS-I/II: static URLs; ADNI: user URLs or `local`). Roasters that are
  runner roasters are declared (IXI, UNC-Wisconsin, OASIS-III; imported
  on use); the others stay None until step 5.
* Not done: ABIDE-II from S3 by default. The bucket holds a re-curated
  BIDS-like copy (one dataset per site, runs, acquisitions, ETHZ for
  ETH, no SU/U_MIA, sidecars in `RawData/sidecards/`), not the tarballs
  that the roaster reads: it needs a hosted-BIDS roaster (2b-iii/step 5)
  before it can be a default. INDI projects keep their command flow
  (`Project` becomes the `Dataset` in step 4) but list through the shared
  entries and site label maps.

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

## 5. Roast: roasters on the action engine

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
* `Action.kind` is an opaque label in the action core; roast computes it
  from the names of the outputs (`bids.kinds.default_kind`: `data` /
  `sidecar` / `meta` / `table`) for its JSON yes/no/only policy;
  `Action.tag` groups progress lines.
* **Engine** (`brainspresso/actions/engine.py`, stage-agnostic, shared
  with grind): consumes each action **before requesting the next**
  (actions may close over open archive handles and are not picklable),
  asks the acceptance **policy** it is given whether each action runs
  and with which defaults, and keeps counters, display, reports and task
  states. Parallelism is at **shard** level: a roaster yields
  sub-roasters (one per archive or subject chunk), which also gives
  progress totals (`ctx.progress(total=)`). The roast layer
  (`brainspresso/roast/`) gives it a `RoastContext` and a `RoastPolicy`
  (`Selection.keeps` on outputs or claims, JSON policy, category
  folders, `IfExists` of tables).
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

Implemented (step 2b-ii; pilots UNC-Wisconsin, IXI, OASIS-III; revised by
the review fixes of 2b-ii-fix, then split into an action core and a roast
layer in 2b-iv):

* Packages (2b-iv). `brainspresso/actions/` knows nothing of BIDS or
  roast (a test checks its imports): `action.py` (`Action`, `Claim`,
  `Fail`, `IfExists`), `writers.py`, `archive.py`, `file.py` (`File`)
  and `locks.py` (its locks), `engine.py` (`Context`, `Policy`,
  `Rejected`, `Shard`, `Engine`, `LocalExecutor`, `Report`, `run`),
  `state.py` (task state files). `brainspresso/roast/`: `context.py`
  (`RoastContext`, `make_context`, `as_list`), `policy.py`
  (`RoastPolicy`), `run.py` (`run_roast`, `roast_dataset`,
  `RoastReport`), `helpers.py` (`copy_templates`, `subject_shards`).
  `bids/kinds.py`: `default_kind`, `kinds_of`.
* Acceptance policy (2b-iv): the engine calls
  `policy.accept(action, ctx, staging=) -> bool` (False drops the action
  silently; raising `Rejected(message)` counts it as an error without
  running it), then `policy.prepare(action, ctx)` (defaults: the base
  `Policy` sets `IfExists` from `ctx.ifexists`), and includes
  `policy.fingerprint(ctx)` (a JSON-able dict) in the input signature of
  shards. `staging` is inherited by the sub-actions of a staging claim.
  The policy is pickled to workers with the context. `RoastPolicy`
  implements the roast rules below (selection, kinds, JSON policy,
  tables).
* Context (2b-iv): the engine reads `ctx.root` (outputs are recorded
  relative to it), `ctx.state` (task states; None: none written, no
  skip), `ctx.ifexists`, `ctx.tag`, `ctx.source`, `ctx.stage` ("roast":
  task records and ids of anonymous tasks, `<stage>/.auto/...`) and
  calls `ctx.progress`. `RoastContext` is a dataclass subclass whose
  `root`/`state` are those of its layout.
* `Action(src, dst | None, fn, *, claim=Claim(folder | [folders],
  fragment), kind=None, tag=None, ifexists=None, staging=False,
  compare=None)`: `declared` outputs, `outputs` after running. In roast,
  `kind` defaults to `bids.kinds.default_kind` (`*.tsv`, dataset files
  such as README/`.bidsignore` and the JSON of tables or of the
  dataset: "table"; other `*.json`: "sidecar"; `*.bval`/`*.bvec`:
  "meta", non-JSON metadata as in `bids.select`; else "data"); outputs
  of several default kinds (`[nii, json]`, `[nii, bval, bvec]`) need an
  explicit `kind`, else the roast policy counts the action as an error.
  JSON policy: `--json no` drops sidecars, `--json only` drops data; tables and "meta" always pass;
  `staging=True` actions (intermediate files, e.g. an unpacked
  FreeSurfer tree in `derivatives/<fs>/sourcedata/`) and the
  sub-actions of a staging claim are exempt (there is no implicit
  exemption of `sourcedata/` any more). `ifexists=None` is the policy
  of the engine (still `different` outside it); under `skip`, tables use
  `different`, and roast sets `Action.compare` on tables: without
  size/digests a table is regenerated and replaces the existing one
  only if its content changed, so pristine tables follow new harvests
  (`compare` is opt-in in the core: actions run outside roast do not
  compare unless they set it). A claim action's `fn()` yields concrete sub-actions
  (selected one by one; it never writes files itself); a claim is
  selected if any of its paths can be; one that yields nothing is
  counted as "empty". `ExtractMember`, `ExtractMembers` (claim,
  `staging=`) in `actions/archive.py`; `Fail(message)` reports a
  missing input and is never hidden (no outputs).
* File locks (2b-iv, `actions/locks.py`): a reader (`File(path, "r")`)
  holds a **shared** lock for its whole context (any number of readers;
  every reopen inside it sees the same content), a writer an
  **exclusive** one while it writes `<dst>.tmp/<name>` and renames it
  into place (writers of one file are serialised; `a`/`r+` copy the
  file under the exclusive lock only). Acquisition polls up to a
  timeout (`timeout=`, else `$BRAINSPRESSO_LOCK_TIMEOUT`, else 10 min)
  and raises `LockTimeout` naming the file and its holder (readers, or
  a writer with its pid and host). Lock files are never created next to
  files nor deleted by their holders: inside a dataset (nearest
  ancestor with `.brainspresso/`) they are
  `<DS>/.brainspresso/locks/<h[:2]>/<h>.lock`, `h` the SHA-256 of the
  dataset-relative path, on the dataset's filesystem (also across
  cluster nodes where `fcntl` locks work, whatever the mount point);
  outside datasets (package templates, other source folders) in a
  private per-user local folder (`$BRAINSPRESSO_LOCK_DIR`, else
  `$XDG_RUNTIME_DIR/brainspresso/locks`, else
  `<tmp>/brainspresso-<uid>/locks`), which protects one user's
  processes on one machine. A reader that cannot create or open its
  lock file (read-only state folder) reads without a lock; a writer
  raises `LockError`, as does a filesystem that refuses `fcntl` locks
  (`$BRAINSPRESSO_LOCKS=0` disables locks: an unsafe escape hatch).
  POSIX locks belong to a process and closing any descriptor drops
  them all, so each process opens a lock file once and arbitrates its
  own holders (nested readers of an archive and its members, threads);
  a lock upgrade (write a file the thread reads) or a nested writer of
  one file raises `LockError` at once instead of deadlocking. Not
  handled: cross-process cycles (A reads X, writes Y; B reads Y, writes
  X) end in a `LockTimeout`; `fcntl` locks are not fair (steady readers
  can delay a writer up to its timeout); lock files accumulate (delete
  `locks/` only when nothing runs). `run_roast` creates the state
  folder first so that all locks of a roast are in the dataset.
* `run_roast(roaster, ctx, *, jobs=1, display=True | callable,
  executor=None) -> RoastReport` (counts per tag, errors, outputs,
  progress, `status`). `RoastContext(layout, selection, aliases, json,
  ifexists, options, templates, tag, source)` with `wants(key)`,
  `resource(name)` (`ctx.source`, then templates), `progress(total=,
  advance=, tag=)`, `keeps(path, partial)` (`selection` on the raw
  dataset, its subject/session filters elsewhere; `<state>/orig/` paths
  judged as the table they copy; False in the folder of an unwanted
  category, `KeyAliases(folders=)`) and `accepts(kind)`. `ctx.source`
  is the source folder of the running roaster (default `layout.source`;
  a roaster or `Shard(source=)` sets it for itself and what it yields),
  the seam for `LocalSource`/per-source folders in step 3.
  `make_context(root, aliases, select_keys=, exclude_keys=, source=,
  **filters)` builds it from CLI values; `roast_dataset(roaster, root,
  aliases, keys=, ..., **options)` is the `roast()` of a dataset.
  `ctx.tag` is the tag of the running roaster. Roaster exceptions are
  counted as errors of its tag; the rest of the roast goes on.
* Shards: any callable yielded; `Shard(fn, *args, tag=, id=, inputs=,
  advance=, unit=, source=)` is a picklable one. With `jobs > 1` every
  shard runs in the pool, through `submit(task, resources=None) ->
  Future` (`LocalExecutor`, "spawn" process pool; `resources` ignored
  locally): shards yielded in the main process are submitted, shards
  yielded in a worker are handed back and submitted when it finishes
  (a shard runs asynchronously: roasters must not rely on the outputs
  of the shards they yield). Decision (step 4 review): this stays; the
  parent never waits on its children, so a full pool cannot deadlock. Unpicklable shards run where they are
  yielded, an unpicklable `ctx` runs the roast in the main process
  (warnings). A dying worker (`BrokenProcessPool`): shards that had
  started (their state file says so, or a start marker that a worker
  of the local pool writes in `<tmp>/bx-shards-<host>-<pid>-*`: the state
  folder may be missing or not writable) are errors, never run again,
  the others and all later ones run in the main process. Other
  executors (workers on other machines) get no markers.
* After all actions and shards: the `finalize(ctx)` hook of every
  roaster that has one, once per run (also that of a shard skipped as
  unchanged), children first, the top-level one last (each may yield
  actions and shards); then, in a `finally`,
  `trim_originals(layout)` (every table under `<state>/orig/`; sessions
  and scans tables only where their folder exists) and `finish_roast`
  (keys, json policy, counts and `status`: "complete", "partial" if
  anything failed, "failed" if the engine stopped). No repair yet.
* Task state (§7): a shard with an `id`, and every shard run in a
  worker, writes `<state>/tasks/<id>.json` as it runs; the main process
  reads the progress of running shards from these files. A shard with
  `id` and `inputs` (its source archives) that is "done" with the same
  input signature (paths, sizes, mtimes; selection; JSON policy) and
  whose outputs exist is skipped under `skip` without reopening its
  archives (a shard that yielded shards never is).
* Keys: `bids/keys.py`. `KeyAliases(tree, bids={leaf: Selection
  filters}, folders={category: [folders]})`; other leaves are
  categories; groups expand to their leaves; lookup is exact, then
  case-insensitive. A group may also be a category (OASIS-III `meta`).
  `KeyAliases.selection(keys, exclude_keys, **filters) ->
  KeySelection` **classifies** paths: `key_of(path)` is the first BIDS
  key, in declared order, whose predicate keeps the path as a data file
  (specific keys first: OASIS-III `TSE` before `T2w`). Dataset files
  and sessions/scans tables follow the subject/session filters only;
  without a key filter (all BIDS keys wanted) so does every path; with
  one, a data file is kept iff its key is wanted, a metadata file iff a
  wanted key up to its class may describe it (inheritance), an
  unclassified metadata file iff a wanted key may describe it, and other
  unclassified files (`_sbref`, `_m0scan`) iff a wanted key's predicate
  without its suffix keeps them (they follow their scans), else they are
  dropped. Claims are judged on the wanted keys only: `--keys meta` or
  `--keys fs` answer False for every subject claim, so no tarball is
  opened. Same `keeps`/`keeps_subject`/`select` interface as
  `Selection`, plus `wants(key)`, `key_of`, `base`, `keys` (wanted
  leaves).
* Helpers for ports (`roast/helpers.py`): `copy_templates(ctx,
  names | {name: dst})` (a missing resource is a `Fail`) and
  `subject_shards(ctx, labels, fn, *args, inputs=)` (pruning, progress
  total, one `Shard` per subject with id `roast/subjects/sub-<label>`,
  advanced when done). Missing inputs are `Fail`s, never warnings only.

## 6. Dataset registration

Status: **implemented in step 4** (design r9, review folded in r10,
as implemented r11). It replaces INDI's `Project` command flow,
`add_compost` and the per-dataset `command.py` modules (each dataset
package now has a `dataset.py`).

### Shape

* A dataset is a `Dataset` subclass (`datasets/base.py`) that *declares*
  what it is (name, folder, keys, sources, per-key defaults, layouts,
  sites); `register` turns it into `bx <name> {harvest,roast,compost,info}`
  built from shared option groups. A dataset that needs more overrides a
  command body, edits the option groups of a command, or adds raw
  cyclopts commands in `cli(app)` (unusual commands, INDI's many
  projects, OpenNeuro's per-accession datasets all fit).
* `Dataset.run(command, layout, args)` (not meant to be overridden) is
  the one entry point of the CLI wrapper and of the Python API: it takes
  the run lock, then calls the command body. `Dataset.execute(command,
  *arguments, path=, out=, **groups)` also binds the arguments and
  resolves the folder and layout, as the command line does.

### `Dataset`: attributes (immutable class defaults)

```python
class Dataset:
    name: str                     # CLI command ("ixi", "nki-rs")
    folder: str                   # <root>/<folder>; may use arguments
                                  #   ("OpenNeuro/{dataset}")
    title: str = ""
    help: str = ""                # markdown; first line = catalog summary
    keys: KeySpec | None = None   # tree, choices, default, off of --keys
    subject_width: int | None = None   # None: labels verbatim (strip
                                  #   `sub-`); int: numbers, ranges, width
    sessions: bool = False        # SessionOptions in harvest/roast
    site_names: tuple[str, ...] = ()   # SiteOptions; names in the help
    layouts: tuple[str, ...] = ("study", "flat")   # multi-site: ALL_LAYOUTS
    template_dir: Path | None = None
    defaults: Mapping[str, Sequence[str]] = {}     # per-key sources (§4)
    roaster: Roaster | None = None   # when no source has one (staticmethod)
    credentials: Literal["login", "token"] | None = None
    harvest_jobs: int | None = None  # default of harvest --jobs
    harvest_subjects: bool = True    # harvest takes --subs (sources prune)
    has_sites: bool = False          # --sites without a list of names
    hosted: bool = False             # hosted as BIDS: filter groups
    folder_choices: tuple[str, ...] = ()   # --folders (hosted)
    download: Mapping[str, Any] = {} # fixed DownloadOptions (on_error...)
    option_defaults: Mapping[str, Mapping[str, Any]] = {}
                                  # {command: {field: default}}
    requires: Mapping[str, tuple[str, ...]] = {}   # {"roast": ("xlrd",)}
    extra: str | None = None      # pip extra that installs `requires`
    commands: tuple[str, ...] = ("harvest", "roast", "compost", "info")
    arguments: tuple[Argument, ...] = ()   # leading positionals of every
                                  #   command (OpenNeuro: accession)
```

`roast` is dropped from the default `commands` when neither a source nor
`roaster` provides one and `roast()` is not overridden (CoRR, GSP,
ADHD200). A command whose `requires` are missing (`find_spec`, no import)
is a stub: listed with "(needs pip install 'brainspresso[x]')" in its
summary, it exits 1 with the hint; it is still recorded in
`utils.deps.UNAVAILABLE`. The class body and `sources()` run at
registration: no network access and no optional import there.

### `Dataset`: methods

```python
def sources(self, args: Args | None = None) -> dict[str, Source]
                                  # fresh instances (ADNI: from --urls)
def configured_sources(self, args) -> dict   # + Source.configure(
                                  #   **self.source_options(args))
def bind(self, **arguments) -> Self          # copy for one accession
def keys_for(self, command: str) -> KeySpec | None   # default: keys
def select_sites(self, values) -> tuple[str, ...]   # case-insensitive;
                                  #   UsageError on unknown names
def groups(self, command: str) -> tuple[str, ...]   # standard groups
def options(self, command: str) -> dict[str, type[OptionGroup]]
                                  # extra groups of a command (Args.extras)
def describe(self, command: str) -> str      # summary of a command
def root(self, path=None, out=None) -> Path  # --out, else tree path/folder
def layout(self, root, layout=None) -> Layout
def labels(self, values) -> set[str]         # subject_labels(width)
def selection(self, subs=(), exclude_subs=(), sessions=(), **filters
              ) -> Selection
def wants(self, command, keys=(), exclude_keys=()) -> Wants
def select_roaster(self, ctx, override=None) -> tuple[Source | None, Roaster]
def legacy_keys(self, command, args) -> set  # legacy roasters
def check(self, command, args) -> None      # keys, sites, subjects
def chosen_sites(self, args) -> tuple       # --sites minus --exclude-sites
def run(self, command, layout, args)         # check, lock, then the body
def execute(self, command, *arguments, path=None, out=None, **groups)
# Bodies of the default commands
def harvest(self, layout, args) -> dict[str, str]  # harvest_sources(...)
def roast(self, layout, args) -> RoastReport       # make_context + run_roast
def compost(self, layout, args) -> CompostPlan | list
def info(self, layout=None, args=None) -> None     # no path, no lock
def cli(self, app: App) -> None   # add or replace commands (after defaults)
```

`Args` (a documented dataclass) has one always-populated field per
standard group (`keys`, `select`, `sessions`, `sites`, `filters`,
`hosted`, `source`, `login`, `token`, `harvest`, `roast`, `compost`,
`layout`, `log`; defaults filled in for groups a command does not show)
and `extras: dict[str, OptionGroup]` for dataset groups, whose fields
never reuse a standard name. `args.source_options()` is the union of the
groups' `source_options()` (sites, credentials, extras such as OASIS-I
`discs`), offered to every source through `Source.configure` (unknown
options ignored, §4; a test checks that each is accepted by a source).
`Args.of(ds, "harvest", select=...)` is the Python API. The default
`harvest` is `harvest_sources(self.configured_sources(args),
self.defaults, layout, selection, wants, options, override=--source)`
after `record_run` (without `defaults`: the first source, or the one of
`--source`): per-key defaults, fallback and per-source roasters are
unchanged. The default `roast` builds the context from `keys_for("roast")`
and `template_dir`, then runs the roaster of `select_roaster` with
`ctx.source` set to its source folder; `roast --source` only exists when
sources have different roasters (and for ABIDE-I's legacy roasters).
`--dry-run` creates nothing and takes no lock. `KeySpec(tree, choices,
default, off).effective(keys, exclude)`: `--keys` that covers every
choice, or the default `--keys` of a spec whose default is "all but
`off`" (groups expanded to their leaves, `KeySpec.leaves`: `all`),
counts as no `--keys` (so `--exclude-keys` alone means "all but" and,
in a roast, keeps the files of no key), and the default
excludes `off` (IXI roast: `trace` only when named in `--keys`); a
default that is a real subset (UNC-Wisconsin harvest: `raw meta`) stays
a selection. Choices come from `choices`, else from a nested-dict tree
(flattened); keys are checked (`KeySpec.check`: choices, else
`KeyAliases.expand`) before anything is written, and the hierarchy is
listed in the help of the command and in `info`. Legacy roasters
(`legacy_subjects`) get numbers: `--subs` is expanded first (files,
`start:stop`). A roast of a missing dataset folder is refused before
anything is created. The command functions return nothing (cyclopts >=
4 prints a return value; the cyclopts 3 console script would print it
and exit 1); `execute` returns it.

### Option groups (`datasets/options.py`)

Frozen dataclasses flattened into flags (`option_group(title,
sort_key)`: `@Parameter(name="*", group=...)`), help from
`Parameter(help=)`; subclassable. Signature parameters are `Group | None
= None` and `Args` fills the defaults. `OptionGroup.for_dataset(ds,
command)` returns the group, or a subclass built with
`dataclasses.make_dataclass` for per-dataset choices (`choices`: keys,
sources, layouts, folders), helps (`helps`: site names) and defaults
(`defaults`, then `option_defaults`); closures over a local `Literal`
break under `from __future__ import annotations`.

| Group | Flags | Used by |
|-------|-------|---------|
| `KeyOptions` | `--keys`, `--exclude-keys` (`keys_for(command)`) | harvest, roast |
| `SelectionOptions` | `--subs`, `--exclude-subs` | harvest, roast, compost |
| `SessionOptions` | `--sessions` | compost; harvest/roast if `sessions` |
| `SiteOptions` | `--sites`, `--exclude-sites` (str, `select_sites`) | harvest if `site_names`/`has_sites`; compost if `sites` layouts |
| `FilterOptions` | `--datatypes`, `--suffixes`, `--tasks`, `--include`, `--exclude`, `--where` | compost; hosted harvests |
| `HostedOptions` | `--folders`, `--trim`, `--list` | hosted harvests |
| `SourceOptions` | `--source` (names of `sources()`, several = ordered) | harvest; roast (several roasters) |
| `LoginOptions` / `TokenOptions` | `--user`, `--password` / `--token` | `credentials` |
| `HarvestOptions` | `--if-exists`, `--jobs`, `--packet`, `--insecure`, `--dry-run`, `--wait` | harvest |
| `RoastOptions` | `--json`, `--if-exists`, `--jobs`, `--wait` | roast |
| `CompostOptions` | `--sourcedata`, `--nested`, `--keep`, `--derivatives`, `--trim`, `--force`, `--ignore-runs`, `--yes/-y`, `--dry-run`, `--list`, `--wait` | compost |
| `LayoutOptions` | `--layout` (`layouts`), `--site-layout` (hidden) | all but info |
| `LogOptions` | `--log`, `--level` | all |

Every command but `info` also takes `path` (root of all datasets,
`BDP_PATH`) and `--out` (dataset folder). A `--subs` value that names an
existing file is read as a list of subjects (one per line). Labels that
match nothing are warned about by the hosted harvests (INDI, OpenNeuro;
an error if none matches); their format is not validated. `subject_width`
is set where subjects are numbers (IXI 3, Bullitt 3, OASIS-III 4,
ABIDE-I 5, ADHD-200 7, CoRR 0: host `0003001` is BIDS `3001`); legacy
roasters get numbers (`Dataset.numbers`).

### `register`, the catalog and `cli.main`

* `register(dataset) -> App` instantiates a class (INDI registers one
  instance per project subclass) and adds, for each command, a function
  whose `__signature__` is `arguments` + `path` + `--out` + one
  keyword-only parameter per group. Its body: `setup_filelog`,
  `bind(arguments)`, `root`, `layout`, `Args`, then `run`. Then `cli(app)`.
  Builders are a plain dict `COMMANDS`.
* `user_errors` prints `UsageError` (option validation; a `ValueError`),
  `LayoutError`, `CredentialsError`, `SourceUnavailable`, `LockError`
  and `LockTimeout` without a traceback (exit 1); other exceptions keep
  theirs. Tracebacks always go to the file log, and are printed with
  `--level debug`.
* `datasets/catalog.py` imports no dataset: `CATALOG = {name: (import
  path, summary)}` and `load(name) -> Dataset`. `scripts/make_catalog.py`
  regenerates it; a test checks that each summary is the first line of
  `help`.
* `cli.main(tokens)` registers only the dataset named by the first token
  (a stub printing the error if it fails to import), and one placeholder
  `App(name, help=summary)` per other entry, so `bx --help` imports no
  dataset and `bx ixi --help` shows the long help. No cyclopts lazy
  commands (`CommandSpec`, cyclopts >= 4, would also overwrite the
  resolved app's help with the summary). `cli.app` (module
  `__getattr__`) is the app with every dataset registered, for tests and
  scripts.
* Why `python -m brainspresso.cli` listed nothing: the package `__init__`
  imported every dataset, which registered on the module
  `brainspresso.cli`, while `-m` ran a second copy as `__main__`. Now
  datasets never import `brainspresso.cli`, `__init__` imports none, and
  `brainspresso/__main__.py` exists.
* cyclopts floor `>= 3.17`, measured (CLI tests and snapshot in
  throwaway venvs): 3.16 drops `--no-json` of a `Literal | bool` field of
  a flattened group, 3.1 cannot decorate classes with `Parameter`; 3.17,
  3.18, 3.24, 4.0 and 5.5 pass. A CI job runs at the floor.
* Later: an entry-point group for third-party datasets, merged into the
  catalog.

### Layouts

* One option: `--layout {study,flat,sites,sites:study,sites:flat}`;
  choices are the dataset's `layouts`. `parse_layout(value) -> (kind,
  child_kind)` (`bids/layout.py`); `sites` = the layout of existing
  sites, else `DEFAULT_LAYOUT` (`study`, final, §9). Manifest unchanged.
* `--site-layout X`: hidden, deprecated (warning) until the end of step
  5; with `--layout sites` it means `sites:X`, alone it is an error.
* Every INDI project keeps `--sites` and the `sites*` layouts (single-site
  ones use the child kind, as before). Compost runs site by site;
  compost `--sites`/`--exclude-sites` only apply to a sites layout
  (error otherwise); the default `roast` refuses a sites layout. A
  `--subs` selection that matches no site is refused before anything is
  downloaded (every site is listed first).

### Run lock (`actions/runlock.py`)

* `run_lock(state, command, exclusive=, wait=0)`: a `FileLock` on
  `<state>/run`, **shared** for harvest and roast, **exclusive** for
  compost, taken after `ensure_state()` and before `record_run`; compost
  takes it before planning and holds it through its confirmation.
  `--dry-run` and `info` take none. Sites: the parent and every
  existing site; a site created by a harvest takes its own shared lock
  when it is created (`INDI.harvest.harvest_sites`, with the `--wait` of
  the command), and a command on one site (`--out <DS>/<site>`, listed
  by the parent or not) also takes the lock of its parent, so a compost
  of a site waits for a whole-parent compost. The parent's lock is
  always taken **shared** by a command on a site, which takes its own
  mode only on its target (the dataset, then its sites): a compost of
  `<DS>/A` runs next to a roast of `<DS>/B` and fails during a roast of
  `<DS>/A`, and a compost of `<DS>` (exclusive on the parent and every
  site) excludes every site command. Locks are always taken parents
  first (`Dataset.lock_states`): two commands that took a site and its
  parent in opposite orders would wait on each other. All three
  commands fail at once by default (`--wait SECONDS`). Locks disabled
  (`BRAINSPRESSO_LOCKS=0`): no lock, no record.
* A shared lock that cannot be opened raises `LockError` (`FileLock`
  would go on unlocked).
* Run records: every holder writes `<state>/runs/<host>-<pid>.json`
  (command, user, host, pid, start), removed on exit; `FileLock` only
  signs exclusive holders, so the messages name holders from the
  records ("harvest running: pid N on host since ..."). Roast pool
  workers take the shared lock in the pool initializer.
* Second line of defence: compost holding the exclusive lock refuses
  (unless `--ignore-runs`; `--force` only means "delete source data
  that does not seem roasted") while a record from another host exists
  ("locks may not be shared on this filesystem, or a run crashed on
  <host>") or one from this host with a live pid; records of dead local
  pids are removed.
* Deletions and rewritten tables take no per-file lock: they run under
  the exclusive run lock (a per-file lock would leave one never-deleted
  lock file per deleted file, and `File` writers would nest with the
  writers of `bids.tables`).

### Grind seam (decided in step 4g, §7)

`grind` will be one more builder in `register.COMMANDS` (`bx <ds>
grind <pipeline>`, step 7). Decided in §7: a bound dataset is named
`<name>/<argument>` in `--datasets` (`openneuro/ds000224`, then an
optional site); a sites layout is ground site by site
(`layout.datasets()`), group outputs in the parent; grind does not take
`<state>/run` (too coarse for long jobs) but a second lock file,
`<state>/grind`, per running process: `LOCKS` in `datasets/base.py`
gives each command its lock name and mode (`"grind": ("grind",
"shared")`), which `Dataset.run` passes to `run_lock`. Ports must not assume that grind
units take the run lock (§7, "What the ports must and must not assume").

### Migration

| Dataset | harvest | roast | Notes |
|---------|---------|-------|-------|
| IXI | declared (login) | declared (source roasters) | `requires` roast xlrd, width 3 |
| UNC-Wisconsin, OASIS-III | declared (token / login) | declared | UNC sessions |
| ABIDE-I, ABIDE-II, CoRR | declared (sites, login) | ABIDE: override, legacy code (step 5) | ABIDE-I `download-nitrc/xnat` dropped |
| COBRE, Bullitt, OASIS-I/II | declared (OASIS: `discs`/`parts` groups) | override, legacy code; own `keys_for("roast")` | |
| GSP, ADHD200 | declared (ADHD200: `--bids/--no-bids` group, `--bidsify` a hidden alias) | none | |
| ADNI-I | declared + `--urls` group | none | still not in the catalog |
| INDI projects | override (`harvest_sites`/`collect`), hosted groups, jobs 8 | none | `INDIDataset` wraps a `Project` (composition: no clash with `Project.sites`/`templates()`); `info` override; `INDI/command.py` became `INDI/harvest.py` |
| OpenNeuro | override; `arguments=(dataset,)`; `--tag`, `--source` (field `route`) | none | default compost; `info` override |

### CLI compatibility

Every flag of the step-3 snapshot (`tests/data/cli_flags_step3.json`)
keeps its names, default and choices, except (listed in `test_cli.py`):

* `--layout sites:study|sites:flat` (new); `--site-layout` deprecated.
* `--sites`: free text validated by `select_sites` (was a choice list
  for ABIDE-I/II and CoRR); default `()` = all (was the list of sites).
* `--subs` and `--exclude-subs` parse as strings (same inputs accepted;
  numbers are formatted with `subject_width`); harvest takes them only
  where it did (`harvest_subjects`).
* `--exclude-keys` alone keeps files that match no key (they were
  dropped once a BIDS key was excluded: `--exclude-keys T2w` dropped IXI
  `sub-*_FLAIR.nii.gz`); files that follow an excluded key's scans go.
* ABIDE-I `download-nitrc`/`download-xnat` removed (`harvest --source`).
* Missing optional dependencies: stub commands instead of none.
* Added (additive): compost `--ignore-runs`, `info` everywhere, `--out`, `--level`, `--wait`,
  `--exclude-keys` (harvests; legacy roasts: set difference),
  `--dry-run`, `--insecure`, `--jobs` and `--source` on harvests,
  `--packet` on hosted harvests, `--exclude-sites`, `--json` on
  UNC-Wisconsin roast, compost `--sites` (sites layouts).
* `python -m brainspresso[.cli]` lists datasets; `--help` panels regrouped.

### Tests

* Toy `Dataset`: default commands, groups, `for_dataset`, stubs, `cli()`.
* Flag snapshot (names, choices, defaults) against step 3, exceptions
  listed; each dataset-group option is accepted by one of its sources.
* Subprocesses: root help of `bx`, `python -m brainspresso[.cli]` lists
  the catalog without importing datasets; `bx ixi --help` shows the long
  help; broken entry -> stub; catalog summaries match.
* Labels: zero-padded labels verbatim (fcon1000, OpenNeuro, compost);
  numeric labels with a width (IXI).
* Layouts: `sites`, `sites:flat`, alias, errors. `--exclude-keys`.
* Run lock: held by another process -> fail fast, `--wait`, records,
  stale records, `--ignore-runs`; dry runs create nothing;
  `BRAINSPRESSO_LOCKS=0`.

### Risks

* Synthesised signatures rely on cyclopts behaviour: snapshot test, run
  at the documented floor.
* Deferred imports move import errors to first use: every catalog entry
  is loaded in the package test.
* Cluster locks: NFSv3 keeps the lock of a crashed client (until the
  lock daemon notices; remedy: restart the client or clear the lock on
  the server, then delete the stale record); NFSv4 can drop the lock of
  a partitioned node; Lustre `localflock` and some FUSE mounts do not
  share locks between nodes; a stopped (SIGSTOP) job keeps its lock. Run
  records catch the cases where the lock is not shared; without `fcntl`
  compost refuses unless `BRAINSPRESSO_LOCKS=0`.
* Over-generalising: legacy roasts stay overrides, `cli()` is an escape
  hatch.

## 7. Grind

The grind **contract** (step 4g): what step 7 implements and what the
ports of step 5 may rely on. Every name below is a stub of
`brainspresso/grind/` or, for what roast shares (plans, execution
options, cluster jobs, `bx percolate`), of `brainspresso/percolate/`
(interfaces and docstrings, no behaviour).
Decisions are marked **D** or "decided" (the owner's); the last
subsection lists them and the one question left. The `wiip-grind` prototype is the reference for configs
(pydantic models with documented fields, SPM Segment); its paths
(`{sub:d}`, no `ses`), its missing execution, state and selection are
what this contract replaces.

### Model (`grind/model.py`)

* **Pipeline** (`Pipeline`): dataset-agnostic and declarative. `name`
  (also the derivative folder), `version`, `stages`, `config` (a model
  class), `requires=("<pipeline>", ...)`, default `container`. Found by
  name (`register_pipeline`, `get_pipeline`; a lazy catalog like the
  datasets').
* **Config**: a pydantic v2 model with documented fields; CLI options,
  `--config` files and hashes derive from it. The contract only needs
  the `Config` protocol (`model_dump`, `model_validate`,
  `model_json_schema`), so `grind/` imports no pydantic (step 7 adds it
  to a `grind` extra). The **effective** config of a dataset is: model
  defaults < dataset overrides < `--config` file < command line.
* **Stage** (`Stage`, abstract): `name`, `level`, `inputs`
  (`Requirement`s), `outputs` (`Output`s), `resources`, `container`,
  `fingerprint(config)` (what of the config decides its outputs;
  default: all) and `run(ctx, work)`, a producer of the action engine
  that receives a `Work` (unit, resolved inputs, output paths, scratch
  folder). Kinds: `FunctionStage` (a Python function that yields
  actions), `CommandStage` (an argv, run through the container
  runtime), `BidsAppStage`; other kinds (MATLAB batch, QC report,
  table aggregation) subclass `Stage`. There is no wiring syntax: the
  order of stages follows from their requirements.
* **Level** and **Unit**: a level is a name and the entities that key
  its units (`LEVELS`: `session` = `sub`+`ses`, `subject` = `sub`,
  `dataset` = one unit per dataset, `group` = one unit per run; more
  can be added, e.g. `run`). A `Unit` is `(level, dataset, sub, ses,
  extra)`; its `label` is `sub-01/ses-1`, `sub-01`, `dataset` or
  `group`. A dataset without sessions has one `session` unit per
  subject (no `ses`).
* **Requirement**: a named input of a stage. `query` (a `Query`:
  datatypes, suffixes, extensions, entities, reserved sidecar
  conditions), from the raw dataset (default), from an earlier `stage`
  of the pipeline, or from a `pipeline` listed in `requires`; `min` and
  `max` matches per unit (`max=1`, the default, with several candidates
  is an error unless `pick` or a dataset override decides); `need`
  ("any", "all" or a count) for fan-in. The levels give the mapping:
  same level, unit to unit; a finer upstream level fans **in** (all
  upstream units inside the unit: sessions of a subject, everything
  for `group`); a coarser one **broadcasts** (the subject's base to
  each session). Example, FreeSurfer longitudinal: `cross` (session,
  raw T1w) → `base` (subject, `cross` with `need="all"`) → `long`
  (session, `cross` and `base`).
* **Output**: `name` and a `template` relative to the derivative root.
  Templates are `str.format` strings over the unit entities, `{base}`
  and `{suffix}` of a named input, and config fields; a bracketed
  segment is dropped when one of its fields is missing
  (`sub-{sub}[/ses-{ses}]/anat/{base}_label-{label}_probseg.nii.gz`).
  Labels are strings, never `:d` (`format_output`). `claim=True`
  marks a folder whose content is only known once the stage has run.
  Templates are fixed per pipeline (decided); a config field `paths`
  stays reserved and is not read.

### Units and selection (`grind/graph.py`)

* `enumerate_units(layout, level, selection, overrides)` walks the
  `sub-*/[ses-*]` folders of `layout.raw` (what is on disk, not
  `participants.tsv`), filtered by the `Selection` of the command line
  (`--subs`, `--exclude-subs`, `--sessions`, later `entities`) and by
  the dataset's exclusions. Tables are only read for `--where` (below).
* A unit of a stage is planned if each raw requirement finds between
  `min` and `max` files; otherwise it is **blocked**, with the reason.
  Upstream requirements are resolved when the task **starts**: the plan
  only needs the upstream task to be planned, queued or done.
* **D**: the graph is static, planned before anything runs (needed
  for saved plans and scheduler dependencies). A stage may still yield
  shards of its own inside its task.

### State and skip (`grind/graph.py`, `actions/state.py`)

One task per (stage, unit): id
`grind/<pipeline>[@<variant>]/<stage>/<unit label>` (`task_id`;
`derivative_name` gives the folder), state file `<state>/tasks/<id>.json` in the state folder
of the unit's dataset (group units: that of the run's home, below). The
record is the **done marker** (decided: no other marker file, also
not inside the derivative) and uses the
schema of `actions/state.py` (schema 1, fields added, none changed):

```json
{"schema": 1, "task": "grind/fs-long/cross/sub-01/ses-1",
 "stage": "grind", "pipeline": "fs-long",
 "pipeline_stage": "cross", "variant": null, "dataset": "IXI",
 "unit": {"sub": "01", "ses": "1"},
 "status": "pending | running | done | failed | skipped",
 "config_hash": "<hex> | null",
 "started": "<ISO 8601> | null", "finished": "<ISO 8601> | null",
 "error": "<message> | null",
 "outputs": ["<path relative to <DS>>"],
 "requires": {"<upstream task id>": "<its config_hash>"},
 "version": "<pipeline version>", "container": "<image@digest> | null",
 "plan": "<plan id> | null",
 "job": {"backend": "slurm", "id": "1234_5", "host": "n1", "pid": 1},
 "staged": "none | inputs | all", "scratch": "$SLURM_TMPDIR | null"}
```

(Roast shards write `stage: "roast"`, `pipeline: null` and the fields
up to `outputs`, plus `run`, `counts`, `progress` and `children`; see
`actions.engine`. Grind adds the fields from `pipeline_stage` on.)

* `config_hash` is the task **signature** (`signature`): SHA-256 of the
  pipeline name and version, `stage.fingerprint(effective config)`, the
  image reference, the raw inputs (path relative to the dataset, size,
  mtime) and the `config_hash` of each upstream task (so a change
  propagates downstream), plus the member list of a fan-in.
* The image **digest** is not in the signature: it is recorded
  (`container`) and compared apart. `plan(..., runtime=)` asks
  `ContainerRuntime.inspect` (local store, image cache; never a pull)
  and a done task is stale only if both digests are known and differ.
  An unknown digest never makes a task stale; the plan records it as
  null (unchecked). Images are pulled only when a plan is run; a task
  then refuses a digest that differs from the plan's.
* Plan verdicts (`PlanTask.verdict`): **done** (record `done`, same
  signature, recorded outputs exist: not run); **todo** (no record, or
  `failed`/`skipped`); **queued** (recorded `pending`/`running`, by
  another plan or an earlier run: not run by this plan, whose
  dependants check its record when they start); **stale** (`done`
  with another signature); **blocked**. `--redo [STAGE]` forces a
  stage and what depends on it.
* A stale unit stops the command (`--if-stale error`); `redo`
  recomputes it, `keep` leaves it and marks dependants blocked. The
  choice is made when planning and recorded in the plan.
  A derivative folder with content but no grind record of this
  pipeline is refused: foreign outputs are never adopted as done
  units (decided).
* **Variants**: two configurations side by side use `--variant NAME`
  (letters and digits). The variant is part of the task id
  (`grind/fs-long@hires/...`), of the record (`variant`), of the plan,
  `Scope` and `GrindContext`, and of the folder
  (`derivatives/<pipeline>-<NAME>/`; refused if that is the name of a
  registered pipeline), so variants never share records. It is not
  part of `Unit`: the same unit is ground under each variant. A
  required pipeline is read in its **default** (unnamed) variant unless
  `--with <pipeline>=<variant>` says otherwise (`Scope.variants`,
  recorded in the plan); the run's own variant is never inherited. `GrindContext.required`
  returns that choice, which enters the signature through the upstream
  task ids.
* **D**: actions of a task that runs use `IfExists` "overwrite" (the
  skip decision is per unit; the roast default "skip" would keep stale
  files under a new hash).
* `pending` is written when a job is submitted, with its id; the
  task writes its own `job` (scheduler id, host, pid) when it starts,
  and `staged`/`scratch` (the option as given, never a full path).
  `skipped` means
  "requirements not met when it started" (`error` names them).
* Records are updated, not replaced: the engine keeps the fields it
  does not own and merges the extra fields of the shard
  (`Shard(record={...})`, how grind passes the `GRIND_FIELDS`), so the
  `pending` record survives the start of the task. Updates are
  read-merge-write under a short `FileLock` on the record.

### Plans and percolate (`percolate/plan.py`, `percolate/run.py`)

Owner decisions; roast and grind share all of this.

* `plan(pipeline, scope, configs, ...) -> Plan` (`grind/graph.py`):
  `PlanTask`s in dependency order (those of required pipelines
  included), each with `after` (must succeed), `after_any` (must
  finish), `resources` and its verdict. A requirement on a `pipeline`
  without `stage` depends on every terminal stage of that pipeline.
  Each dataset ends with a `finalize` task (descriptions, manifest run
  entry, failure list), `after_any` all its tasks.
* **Default: no plan file.** `bx <ds> roast` and `bx <ds> grind
  <pipeline>` plan in memory and run on the local pool. `--plan PATH`
  (both commands) writes the plan and runs nothing: the persisted
  `--dry-run`. `--detach`, and any scheduler backend (its jobs must
  read the plan), save it under `<state>/plans/` first. A local roast
  without a plan keeps today's single engine run (decided); step 7
  tests that it gives the same dataset as its plan run task by task.
* **`--wait` / `--detach`**: one option with two spellings, on roast,
  grind and percolate. Default: wait on the local backend, detach on
  SLURM, SGE and PBS; a profile may set `detach = true|false`; the
  command line wins. Detaching prints the `bx percolate <plan>
  --status` command and returns. `--wait` on a scheduler behaves like
  `sbatch --wait`: it shows progress and exits non-zero if any task
  failed. Waiting is monitoring only: it is never needed for
  correctness, and killing it never affects the jobs.
* **A plan task is a scoped re-invocation, not a serialised
  callable.** The plan records the stage (`roast`, or `grind` with
  pipeline name, version and variant), the datasets (reference, root
  and id), a snapshot of the command options and of the effective
  configs, and per task its scope (`TaskScope`: subject, session,
  dataset or group), dependencies and resources. Plan-time choices
  are in it too: `if_stale`, the variants of required pipelines, image
  references and digests. To run a task, percolate calls the planner
  of its stage (`StagePlanner.run`, found in `PLANNERS`) restricted to
  the scope:
  * grind (`GrindPlanner`): one stage on one unit. The pipeline is
    found by name (`get_pipeline`) and must have the plan's version,
    else the task refuses. So pipelines run from plans are registered,
    and stage callables (`FunctionStage.fn`, `CommandStage.command`)
    are importable as `module:qualname`; `Pipeline.validate` refuses
    lambdas and closures. The task builds its own `GrindContext`
    (`stage = "grind"`; `pipeline`: the **name**, which records pick
    up; `spec`: the `Pipeline`) and runs `stage.run` as one shard of
    the action engine, with `GrindPolicy`.
  * roast: one top-level shard that has an id (a subject, as
    `subject_shards` yields them); nested shards are found when it
    runs, as today. A `head` task runs the actions of the top-level
    roaster itself, and `finalize` the hooks, trimmed tables and
    manifest. The roaster is driven again with a scope filter (step 7,
    engine): only the shard of the scope runs.
* Every task re-checks its inputs, its requirements (upstream
  records) and its signature when it starts, and applies the failure
  policy itself. A plan that no longer matches (pipeline version,
  image digest, signature recorded in the plan) raises `PlanError`:
  reported as a **stale plan**, never followed blindly. An upstream
  record still `pending` or `running` leaves the task `pending` (not
  `skipped`): the next `bx percolate PLAN` runs it.
* **Plan files** are YAML, `<state>/plans/<timestamp>-<hash>.yaml`
  (`<root>/.brainspresso/plans/` for a collection), with `schema: 1`
  (`PLAN_SCHEMA`; format in `percolate/plan.py`). They are written
  once and never changed: state lives in the task records, which stay
  JSON (written often, atomically) and name their `plan`.
* **YAML and strings**: plans are read with `yaml.safe_load` and
  written (`write_plan`) with a `SafeDumper` subclass that quotes every
  string scalar (`plan_dumper`), so `1e3`, `01`, `no` and `on` (`YAML_PITFALLS`)
  round-trip as strings for any reader; nothing is refused when
  writing. Labels are still written with their BIDS prefix (`sub-01`),
  and `read_plan` checks that every field that must be a string is one
  (`check_strings`; a plan edited by hand with `sub: 01` is an error
  that names the field): nothing is converted back. Pinned in
  `tests/test_grind_contract.py`. PyYAML is a **core** dependency from
  step 7 (decided; roast plans need it; imported only when a plan is
  read or written).
* **`bx percolate PLAN`** (`percolate`; a top-level command, since
  the plan fixes dataset, pipeline, options and scope) runs the tasks
  that are not done. `--retry failed` also reruns `failed` and
  `skipped` tasks; `--status` prints a snapshot of the tasks from
  their records, reconciled with the scheduler (`plan_status`: a
  `pending`/`running` record whose job is gone becomes `failed`; a
  local run on another host cannot be checked and is listed as
  unknown); `--status --watch` refreshes it until the plan finishes
  (`watch_plan`: read-only, it writes no record and takes no lock;
  `--follow` is not used, it is kept for a future `--logs <task>
  --follow`); `--cancel` cancels
  the jobs and records their tasks `failed`, unknown ones included
  (`cancel_plan`). Without argument it lists the plans of the dataset
  that contains the current folder, else of `--out` (`list_plans`).
  A plan written by another brainspresso version is run with a
  warning: the pipeline version and the signatures decide. One lock per plan: all forms but `--task`, `--array`
  and a read-only `--status` hold an exclusive lock on the plan file.

### Execution and cluster jobs (`percolate/options.py`, `percolate/jobs.py`)

* **`ExecutionOptions`**: one option group on roast, grind and
  percolate, so a cluster run is one command: `--profile NAME`,
  `--backend local|slurm|sge|pbs`, `--jobs` (local), `--partition`
  (`--queue`), `--account`, `--time`, `--mem`, `--cpus`, `--gpus`,
  `--max-array`, `--max-running`, `--directive` (repeatable raw
  scheduler lines), `--container-runtime docker|apptainer|none`, `--wait`/`--detach`,
  `--path-map SRC=DST`, `--stage none|inputs|all`, `--scratch PATH`,
  `--keep-scratch-on-failure`.
* **`Profile`**: a `[profiles.<name>]` section of the config file
  (separate PR; `~/.brainspresso/config.toml`, then the root and
  dataset files, `--config FILE`, environment and the command line,
  later ones winning). Beyond the
  options: resource defaults, overrides per task id pattern, `setup`
  lines (modules, environment), the `bx` executable on the nodes,
  `detach`, `lock_wait` and image cache.
* **Fixed in the plan**: tasks, dependencies, the resources stages
  declare (`PlanTask.resources`) and those datasets override
  (`PlanTask.overrides`). **Chosen at percolate time**: backend,
  profile, runtime, limits, overrides, path map and staging (neither
  is in the signature). Precedence (`resolve_resources`): stage
  declaration < profile < dataset overrides < command line.
* **Local**: the pool of `actions.engine` (`LocalExecutor.submit`, as
  it is); each worker runs one task (`run_task`: a plan and a task id,
  nothing else is pickled) and percolate holds a task until the tasks
  in its `after` are over; a task counts `cpus` slots of `--jobs`.
* **Schedulers**: percolate creates and submits the jobs
  (`make_jobs`, `submit_jobs`). The payload of a job is plain data: a
  batch script that runs `bx percolate PLAN --task <id>`, or for an
  array `--array <job> --index <n>`. Backends are native templates
  (`Scheduler`: `script`, `submit_command` with `sbatch --parsable` or
  `qsub`, `query`, `cancel_command`) for SLURM, SGE and PBS, with
  `afterok` for `after` and `afterany` for `after_any`.
* **No submitit**: its value is pickling callables and fetching their
  results; jobs here are command lines and results are task records.
  It also only covers SLURM well. Dask and Ray are not backends.
* **Arrays**: tasks of one stage with equal resources form one array,
  split at `--max-array`. Element-wise dependencies (`Job.pairwise`,
  SLURM `aftercorr`) are only used between arrays that `make_jobs`
  built together from one ordered list of scopes and split at the same
  places; equal lengths are not enough. Anything else, and schedulers
  without them, wait for the whole array.
* **`--write-jobs DIR`** (`write_jobs`) submits nothing: it writes the
  batch scripts, `manifest.json` and a `submit.sh` that submits them
  in order, feeds captured job ids into the dependencies and logs them
  in `submitted.tsv`. Tasks write their own records when they run, so
  `--status` works after a submission by hand. `submit_jobs` is the
  same, run by percolate, which also writes `pending` records with
  the job ids.
* Scheduler dependencies are hints; correctness comes from tasks
  re-checking their requirements when they start.
* **Path translation**: login node, compute nodes and containers may
  mount the same data elsewhere. A plan holds no path but roots
  (`Plan.home`, `PlanDataset.root`, next to the reference and the
  dataset id); everything else is relative to a root. A profile
  carries an ordered path map (`[[profiles.<name>.paths]]` with `from`
  and `to`; `--path-map`), applied (`translate`) when job scripts are
  written (plan file, config file, files named by options) and when a
  job reads the plan. `resolve_roots` then checks that each translated
  root exists and that its manifest has the dataset id of the plan
  (decided: a UUID that `ensure_state` writes once in the manifest; a
  dataset without one, made earlier, is accepted with a warning), and
  fails naming the root and the map otherwise. Lock files and signatures use
  dataset-relative paths, so they do not depend on the mount point.
* **Staging** (`grind/staging.py`; grind tasks only, decided: roast
  ignores `--stage` with a warning): `--stage none` (default) runs in
  place. With `inputs` or `all`, the task resolves
  its inputs and computes its signature from the originals, then
  copies them (`stage_in`: the raw query and upstream outputs of each
  `Requirement`, with inherited metadata) into a minimal tree under
  scratch that mirrors dataset-relative paths, holding read locks only
  while copying, and gives the tool the scratch paths. With `all`,
  commands also write under scratch and `stage_out` commits what they
  wrote through the action layer (`File` under exclusive locks,
  renamed into place; one action per file, never a raw copy), so locks
  and recorded outputs stay correct; actions that already write through
  `File` write to the dataset directly. Scratch (`--scratch`, else
  `$SLURM_TMPDIR`, `$TMPDIR`, `$TMP`) is removed afterwards, unless
  the task failed and `--keep-scratch-on-failure` is set. Containers
  get the staged folders bound in place of the originals, at the same
  paths inside. For a BIDS App, stage-in builds a minimal BIDS dataset
  (`dataset_description.json` and the participant's selected files),
  which becomes its `bids_dir`.
* Engine changes of step 7, all additive and stage-agnostic
  (`actions/` imports neither `percolate/`, `grind/` nor `roast/`): a
  signature given by the shard; extra record fields merged into the
  existing record (`Shard(record=)`, `update_task_state`); a scope
  filter on top-level shards (roast tasks); an `Adopt` action (below,
  BIDS Apps); named run locks with their own records (below). No
  change to the pool, to `submit` or to how shards are collected.

### Locks

* **D** (§6): one run-lock domain per dataset *is* too coarse. Grind
  tasks read BIDS data only, never `sourcedata/`, and run for hours on
  other nodes: with the `<state>/run` lock, composting source data
  would wait for days, and a detached submitter cannot hold a lock for
  its jobs (locks belong to processes).
* So grind uses a second lock file, `<state>/grind`, with the same
  mechanism, `run_lock(..., name="grind")`: held **shared** by the
  process that plans (a local run: until it ends) and by each task
  while it runs, with the `lock_wait` of the profile;
  **exclusive** for a compost whose plan deletes BIDS data or anything
  under `derivatives/`. A compost of `sourcedata/` only does not take
  it. Grind never takes `<state>/run`; roast and harvest never take
  `<state>/grind` (a roast that rewrites an input is caught by the
  signature, and files are replaced atomically under `File` locks).
  Order: `run` before `grind`, parents before sites. Queued jobs hold
  nothing: they re-check their inputs when they start.
* **Records per lock** (step 7, `actions/runlock.py`): today every
  holder writes `<state>/runs/<host>-<pid>.json` and an exclusive
  `<state>/run` refuses while any live record exists, so grind records
  would block a compost of `sourcedata/`. Each lock name gets its own
  records folder (`run`: `<state>/runs/`, unchanged; `grind`:
  `<state>/runs-grind/`, with the task id and job), and
  `read_records`/`_check_records` only read those of the lock being
  taken. `_held` is keyed by (state, name); `held_states` and
  `hold_shared` carry (state, name) pairs, so pool workers take the
  locks their parent holds, not always `<state>/run`.
* **`Dataset.run`**: `LOCKS` (`datasets/base.py`) maps a command to a
  lock name and mode: `"grind": ("grind", "shared")`; the other
  commands keep the `run` lock. `Dataset.run` passes the name to
  `run_lock` over `lock_states(layout)` unchanged (parent shared first,
  then the dataset and its sites), so `bx <ds> grind` never takes
  `<state>/run`. Compost takes `("grind", "exclusive")` itself, after
  planning, when its plan needs it. `grind_collection` takes the locks
  of every dataset the same way, datasets sorted.
* **Tasks** hold `<state>/grind` shared in every dataset whose data or
  derivatives they read (`GrindPlanner.locks`, `GrindContext.locks`):
  their
  own dataset (and its sites parent) for unit tasks; for a group task
  the home and each site or each dataset of the collection, parents
  before sites.
* A running task holds an **exclusive** `FileLock` on
  `<state>/tasks/<id>.run` (fail at once: the unit is already running
  elsewhere; the record itself is locked only while updated), then
  writes its outputs through `File` (exclusive per file, renamed into
  place). Tools that write directly (BIDS Apps) are covered by the unit
  lock and a claim on their output folder.

### Failures

* A failed task is recorded (`failed`, `error`) and the run goes on.
* A requirement with `need="all"` blocks its task when any upstream
  unit failed or is missing; "any" (default) or a count runs it on the
  units that succeeded: `Work.members` and `Work.failed` list both,
  and the record keeps the members in `requires`.
* A blocked task is recorded `skipped`; its dependants follow the same
  rule. `GrindReport` gives counts per stage and the failure list
  (also in the manifest run entry); the exit status is non-zero if
  anything failed. Failed and skipped tasks are retried by the next run.

### BIDS Apps (`grind/bidsapp.py`)

`BidsAppPipeline` wraps any BIDS App: one `participant` stage (level
`subject`, or `session` when the app has a session flag) that runs
`<app> <bids_dir> <out_dir> participant --participant_label <sub>
[args]` in the app's container, and an optional `group` stage (level
`dataset`: an app takes one `bids_dir`). `bids_dir` is `layout.raw`
(read-only mount), `out_dir` the derivative root. Outputs cannot be
listed: the stage is one **claim action** on the `claims` templates
(default `sub-{sub}/` and `sub-{sub}.html`; with a session flag
`sub-{sub}/ses-{ses}/` only). `Pipeline.validate` refuses a claim that
does not use every entity of the level of its stage: session tasks
would overwrite each other's `sub-{sub}/`. An app that writes
per-subject files whatever the session runs at the subject level.
Once the app has exited, the claim yields one `Adopt` action per
claimed path (step 7, `actions/`): it writes nothing, fails if the
path is missing, and declares it as an output, which is how claimed
folders reach the task record. A claim that adopts nothing is an
error for grind (the engine alone would count it "empty"). Selection is
by subject (and session). Finer filters ("which T1w") go through
staging, which gives the app a minimal BIDS dataset with only the
selected files (above), or through the app's own filter file when
`BidsAppConfig.bids_filter` is set; with neither they are refused
(decided), with a message that names `--stage inputs`.

### Containers (`grind/containers.py`)

`Container` (image reference, digest, environment); `ContainerRuntime`
(`pull`, `command(container, argv, mounts=, env=, workdir=)`) with
runtimes `docker` (local default), `apptainer` (HPC; images converted
once into a cache) and `none` (tools installed on the host), chosen by
the profile. Mounts (`Mount`) derive from the translated roots, or
from the staged folders in their place; raw data is mounted read-only,
the derivative and scratch folders read-write; containers run as the calling user. The image
reference is part of the signature; the digest (`inspect`, `pull`) is
recorded, compared when known, and written in `GeneratedBy`. Python stages run
in the job's own environment (brainspresso must be importable there).

### Dataset overrides (`grind/overrides.py`)

`GrindOverrides` is data that a `Dataset` may declare as its `grind`
attribute: `exclude_subs`, `exclude` (globs never used as inputs),
`inputs` (`{requirement name or suffix: Query}` that narrows a
requirement, i.e. "which T1w"), `species`, `template`, `config`
(`{pipeline: {field: value}}`) and `resources`. The same fields may
come from `[<name>.grind]` and
`[<name>.grind.<pipeline>]` sections (`[abide1.grind.fs]`) of
the config file (separate PR), which win over the declaration. They apply in `bx <ds>
grind` and per dataset in a cross-dataset run, and enter the signature
through the effective config and the resolved inputs.

### Commands, datasets and the index (`grind/run.py`, `grind/index.py`)

* `bx <ds> grind <pipeline>` (`grind_dataset`; a builder in
  `register.COMMANDS`): selection options, the options of the
  pipeline's config (generated from its fields, over the sections of
  the config file, separate PR, which owns `--config`: there is no
  second config option), the execution options, `--redo`, `--if-stale`, `--variant`, `--with`, `--plan`,
  `--detach`, `--dry-run` (prints the plan; pulls nothing).
* **D** (§6): a sites layout is ground site by site
  (`layout.datasets()`): session, subject and dataset units, state and
  `derivatives/<pipeline>/` belong to each site; `group` units belong
  to the parent, which acts as the home of a small collection.
* `bx grind <pipeline> --datasets REF... --where EXPR...`
  (`grind_collection`) makes **one** plan over all datasets. Per-dataset
  tasks keep their ids and state in their dataset (a later `bx <ds>
  grind` sees them done); `group` tasks and outputs live in the
  **home**: `<root>/derivatives/<pipeline>/` with state in
  `<root>/.brainspresso/` (manifest `"layout": "collection"`).
* **D** (§6): a dataset reference (`DatasetRef`) is
  `<name>[/<argument>...][/<site>]`, case-insensitive: the
  dataset's CLI name (`ixi`, `abide1`, `openneuro`, `fcon1000`: the
  catalog key, also used for the sections of the config file, separate
  PR `claude/feat/config-file`), then the dataset's positional
  `arguments`, then at most one site (`openneuro/ds000224`,
  `abide1/NYU`). `*` globs match what is on disk (`openneuro/*`); a path names a BIDS dataset outside the
  catalog. The reference is the `dataset` of units, records and the
  index.
* The index (`build_index`): one row per subject (and session) on
  disk, with `dataset`, `participant_id`, `session_id` and the columns
  of each `participants.tsv`/`sessions.tsv`. `--where` takes
  `<column> <op> <value>` conditions (`== != < <= > >= in ~`), ANDed;
  a missing value never matches.

### Provenance (`grind/provenance.py`)

* `<DS>/derivatives/<pipeline>/dataset_description.json`, written when
  the run is planned and completed by `finalize`: `DatasetType`
  "derivative", `GeneratedBy` (the pipeline: `Name`, `Version`,
  `CodeURL`, `Container {Type, Tag, URI}`; then brainspresso), and
  `SourceDatasets`: the raw dataset (`DerivativeLayout.source_url`)
  and the derivative of each required pipeline.
* The home of a cross-dataset run lists every input dataset in
  `SourceDatasets` (relative URL, DOI when known).
* The effective config is saved once per signature under
  `<state>/grind/<pipeline>/` and named in the manifest run entry.
  Stages written in Python add `Sources` (BIDS URIs) to sidecars.

### What the ports (step 5) must and must not assume

Must: write valid BIDS at `layout.raw` through `Layout` and `File`
(atomic writes); `participant_id` with `sub-`, a sessions table when
there are sessions, `age`/`sex` in BIDS form where known; keep task ids
under `roast/` (`grind/` belongs to grind).

Must not: take or expect a lock on behalf of grind, or assume grind
holds `<state>/run`; add `grind` to `Dataset.commands`, define a
`grind` method or import `brainspresso.grind` (except to declare
`grind = GrindOverrides(...)`, optional, data only); name a roasted
derivative after a pipeline (`derivatives/<provider-specific name>/`);
assume one state folder per run, numeric labels, that every subject has
sessions, or that `Shard` keeps exactly today's arguments (keywords
will be added); yield subject shards without `id` and `unit` (use
`subject_shards`), or make them depend on outputs of the top-level
roaster's own actions (a roast task runs one shard alone); read or write `derivatives/<pipeline>/` of a
grind pipeline.

### Decisions (owner) and open questions

Decided, and built into the text above and the stubs: `--if-stale
error` by default, recorded in the plan; the task record is the only
done marker; the second lock file `<state>/grind`, which a compost of
BIDS data waits for; `dataset` and `group` both stay levels; `--where`
uses raw column names (a harmonised set may be added later, as new
columns); output templates are fixed per pipeline; a BIDS App with a
file-level selection needs staging or its filter file, else it is
refused; foreign outputs are never adopted; required pipelines are
read in their unnamed variant unless `--with`; PyYAML is a core
dependency; `bx percolate` alone lists the plans of the dataset of the
current folder; a plan of another brainspresso version runs with a
warning; local roast keeps its single engine run; pipeline options
come from the config file and generated options, with no second
`--config`; staging is for grind; datasets get a UUID in their
manifest; SPM runs as compiled standalone SPM in a container;
`--wait`/`--detach` and `--status --watch` as above.

Open:

1. `--wait` already exists on harvest, roast and compost as `--wait
   SECONDS` (how long to wait for the run lock, §6). The new boolean
   `--wait`/`--detach` cannot share the name. **R**: rename the lock
   option `--lock-wait SECONDS` (profile field `lock_wait`), keeping
   `--wait SECONDS` as a deprecated alias only on commands that have
   no execution options (harvest, compost). (`Profile.lock_wait`,
   `datasets/options.py` in step 7)

## 8. Implementation steps

Each step lands as commits on the refactor branch with its tests green.

| # | Step | Tier | Status |
|---|------|------|--------|
| 1 | Characterization tests for roasts without tests (COBRE, OASIS-I/II/III, ABIDE-II, ABIDE-I nitrc); known bugs as strict xfail | Low-Effort Opus | done (300 passed, 16 xfailed) |
| 2a | `Selection`: modality-agnostic companions, directory formats, generic entities, per-path `keeps` | High-Effort Opus | done (333 passed) |
| 2b-i | Manifest/state schema; `Layout` (study / flat / sites, sub-layouts, `relative`, detection); `--layout`; every writer of `rawdata/` (roasters, compost, harvest, INDI) goes through `Layout`; `rawdata/` removed | High-Effort Opus | done (421 passed, 32 xfailed: the 16 known bugs, under both layouts) |
| 2b-ii | Runner + action outputs/claims/kind/tag + shards; tables via `.brainspresso/orig` + trim + `finalize`; pilot ports UNC-Wisconsin, IXI, OASIS-III | High-Effort Opus, Fable review | done (461 passed, 28 xfailed: OASIS-III sessions name and exclude-keys fixed) |
| 2b-ii-fix | Review fixes: readers take no lock (shared and read-only sources); tables use `different` under `skip`; every shard runs in the pool (nested ones handed back), every `finalize` runs; pool failures fall back to the main process, common steps in a `finally`, run `status`; key classification (`key_of`) instead of a union; kinds (`meta` for bval/bvec, mixed kinds rejected, `Fail` never hidden), `staging`, category folders; claims yield actions only (lists of folders, "empty"); task state per shard, progress from it, input-signature skip, `submit` protocol; `ctx.source`; port helpers | High-Effort Opus | done (511 passed, 28 xfailed) |
| 2b-iv | Actions core / roast layer split (`actions/engine.py` with an acceptance `Policy`, `roast/`, `bids/kinds.py`), RW locks (`actions/locks.py`: shared readers, exclusive writers, lock files in `.brainspresso/locks/`, timeouts), docstrings (pydocstyle, numpy convention, on `actions/`, `roast/`, `bids/`) | High-Effort Opus | done (608 passed, 2 skipped, 28 xfailed, with step 3 merged) |
| 2c | Dataset module names match the stage names (`download.py` → `harvest.py`, `bidsify.py` → `roast.py`); temporary re-exports of the 2b-iv split removed | Low-Effort Opus | done |
| 2b-iii | Hybrid placement for hosted-BIDS sources; pilot on one INDI project and OpenNeuro | High-Effort Opus | |
| 3 | Sources: `RemoteEntry`, `provides`/`roaster`/`fetch`, per-key defaults, `LocalSource`, label mapping, credentials; drop request-option shim | High-Effort Opus | done (536 passed, 28 xfailed; every harvest on sources, ABIDE-II S3 opt-in only, see §4) |
| 4 | `Dataset` + `register` (absorbs INDI `Project`, `add_compost`); shared option groups; catalog; `--layout sites:<kind>`; run lock | High-Effort Opus | done (913 passed, 2 skipped, 28 xfailed, after merging #18; §6) |
| 4g | Grind contract written down (pipelines, stages, state, submit protocol), doc + interfaces only, so that ports do not bake in assumptions | Fable plan |done (§7 rewritten as the contract; stubs in `brainspresso/grind/` and `brainspresso/percolate/`, nothing implemented; `tests/test_grind_contract.py`; 1043 passed, 2 skipped, 28 xfailed) |
| 5 | Port remaining datasets one by one; fix known bugs | Low-Effort Opus each | |
| 6 | Tabular helpers, converter registry, archive helpers | Low-Effort Opus | |
| 7 | Grind implementation: local backend, plans and `percolate`, one BIDS-App pipeline, scheduler scripts; then SPM Segment from `wiip-grind` | High-Effort Opus | |
| CI | BIDS-validator check of roasted fixtures in both layouts | Low-Effort Opus | |

### Conventions

* Docstrings: every public module, class, function and method has one,
  numpy style: a one-line summary (imperative, or a noun phrase for
  properties), a blank line, then the description; `Parameters`,
  `Returns`, `Yields`, `Raises` sections for the important API and for
  functions with several arguments (arguments are at least briefly
  explained everywhere). Constructor arguments go in the class
  docstring (or in `__init__`'s, after a summary line).
* Enforced by `ruff check .` (pydocstyle `D` rules, `convention =
  "numpy"`) in `brainspresso/actions/`, `brainspresso/roast/`,
  `brainspresso/bids/` (step 2b-iv), `brainspresso/sources/` and
  `brainspresso/download/` (step 3), `brainspresso/grind/` and
  `brainspresso/percolate/` (step 4g); other packages are exempt through
  one negated `per-file-ignores` pattern in `pyproject.toml`
  (`!brainspresso/{actions,bids,download,grind,percolate,roast,sources}/**`
  and the ported modules; two
  negated patterns would exempt every file). Extend it as datasets and
  modules are ported (step 5), one package at a time.
* Imports are at the top level of modules (ruff `PLC0415`, step 4). A
  local import is kept only for an optional dependency, a genuine
  import cycle that moving code cannot break, or the start-up time of
  `bx --help` (the catalog and the CLI import one dataset), with
  `# noqa: PLC0415` and the reason (`# lazy: optional dependency`).
  Tests written before step 4 are exempt.
* Wording: the stage is "roast" (roasters); "bidsify" only survives in
  the names of the legacy roasters (`Bidsifier` classes, `bidsifier*.py`,
  rewritten in step 5) and of pre-existing helpers (`freesurfer.bidsify`).
* Layering: `actions/` (stage-agnostic) imports neither `bids/`,
  `roast/`, `percolate/` nor `grind/`; `percolate/` (plans, execution,
  stage-agnostic) builds on `actions/` and `bids/` and imports neither
  `roast/` nor `grind/`; `roast/` and `grind/` build on all three;
  BIDS knowledge lives in `bids/`.

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
* ABIDE-I XNAT (`bidsifier_xnat.py`), found in step 4 and on #18:
  FreeSurfer archives are looked up with the OASIS-3 pattern
  `OAS3{id:05d}_MR_*`, so `fs`/`fs-all` roasts find nothing; `fixstatus`
  is called without `mod` and with int counters;
  `int(label.split('_')[1])` fails on sample labels; `glob(site + '_*')`
  misses `UCLA1_...`; the `*_{id:05d}` glob misses 7-digit labels.
* ABIDE-I NITRC: `acq-hires_T1w.json` (UCLA `anat_2`) is never used.
* ~~`set(*map(lower_keys, exclude_keys))` raises with more than one
  exclude key: ABIDE-I/II~~ (fixed in OASIS-III, step 2b-ii, with its
  sessions table name; in ABIDE-I/II in step 4, where `--exclude-keys`
  lists their keys).
* IXI DWI (**fixed** on `claude/bugfix/ixi-dwi-volumes`): the bundled
  16-entry table always won over per-subject NITRC tables, and the volume
  count was never checked. `IXI-DTI.tar` has 17 3D members for IXI002
  (no duplicate entries); the old roast's `sub-002_dwi.nii.gz` has 18
  volumes, origin unexplained (being checked by matching volumes to
  members). Now: members de-duplicated, raw DWI = volumes 0..15; the
  canonical 16-entry table is written once as dataset-level
  `dwi.bval`/`dwi.bvec` (BIDS inheritance), and a per-subject
  `bval`/`bvec` only when the subject's NITRC table numerically differs
  from it; volume 16, if it correlates >= 0.95
  with the geometric/arithmetic mean of the DW volumes, goes to
  `derivatives/ixi-scanner/` as `desc-trace`/`desc-powderavg` `_dwimap`
  (key `trace`); any other layout is a counted error. Checked on synthetic
  data only; to validate on real IXI subjects of each site.
* ~~Harvest: ABIDE-II and ABIDE-I xnat use the sync `nitrc_authentifier`
  with aiohttp sessions.~~ Fixed in step 3 (async `NitrcAuth`/`XnatAuth`,
  regression tests with fake login servers in `tests/test_sources.py`).
  Also fixed there: OASIS-I `--discs 12` was never downloaded, OASIS-III
  PET experiments (FDG, PIB, AV45, AV1451) were only harvested with
  `pup`, ABIDE-I xnat sent the XNAT login to the NITRC phenotype URL.

## 9. Open questions

* ~~Default layout: `study` or `flat`.~~ Decided: `study`
  (`DEFAULT_LAYOUT`, one constant in `bids/layout.py`).
* Validator/tooling support for `DatasetType: "study"` and `rawbids/` (new
  in BIDS 1.11.2): to check; `rawbids/` itself is a plain raw dataset.
* ~~Grind runtime for SPM.~~ Decided: compiled standalone SPM in a
  container (§7).
* ABIDE-II from S3 (planned default for raw data): roast the bucket's
  BIDS-like copy as a hosted-BIDS source (INDI-style project with
  hybrid placement, 2b-iii), or keep the NITRC tarballs as the default.
  The two copies differ (runs, acquisitions, sessions), so a dataset
  harvested from both is not one BIDS dataset.

## 10. Revision log

* r1: initial plan.
* r2: contract review. Selection made modality-agnostic before use (verified:
  an empty selection dropped PET/EEG/MEG companions); action claims and
  multi-output rules; sequential runner with shard-level parallelism;
  tables via `.brainspresso/orig`; per-key sources with `fetch` and
  `LocalSource`; hybrid placement classified by "may be repaired"; layout
  sub-layouts, `relative`, sites as composition; grind stages, done-markers,
  resources in the submit protocol, per-task state; step order revised.
* r3: step 2b-i. Layout API as implemented (`resolve`, `original`,
  `bids`, `datasets`, `resolve_layout`), manifest schema 1 and per-task
  state format; INDI `--layout merged` becomes `--layout study|flat`,
  `--layout sites` gains `--site-layout`.
* r4: step 2b-ii. Keys select a union of selections (`KeySelection`);
  the JSON policy skips `sourcedata/`; tables trimmed from
  `.brainspresso/orig/` in every roast.
* r5: step 2b-ii-fix (review). Keys classify paths (first matching key)
  instead of a union; the `sourcedata/` exemption becomes
  `Action(staging=True)`; claims never write directly; tables default
  to `different`; every shard runs in the pool and every `finalize`
  runs; roast shards write the per-task state of §7 and go through the
  local `submit` protocol; `ctx.source` prepares step 3.
* r6: step 3. Sources as implemented (§4): keys reach sources as a
  `Wants` predicate; generated files are entries with `meta["content"]`;
  per-run options through `configure`; ABIDE-II keeps NITRC as its
  default (the S3 copy is not what its roaster reads).
* r7: step 2b-iv (owner review). The runner becomes a stage-agnostic
  engine (`actions/engine.py`) driven by an acceptance `Policy`; the
  roast layer moves to `brainspresso/roast/`, the BIDS kind classifier
  to `bids/kinds.py` (`Action.kind` is opaque, `Action.compare`
  opt-in); readers take shared locks again, with lock files in
  `<DS>/.brainspresso/locks/` (never next to sources, never deleted)
  and blocking acquisition with a timeout; docstrings enforced with
  pydocstyle on `actions/`, `roast/` and `bids/` (Conventions, §8).
* r8: step 2c. Dataset modules renamed after the stages
  (`download.py` -> `harvest.py`, `bidsify.py` -> `roast.py`); the
  temporary re-exports of the 2b-iv split are removed (no open branch
  used them).
* r9: step 4 design (§6, under review). `Dataset` class with declared
  attributes and overridable command bodies, `register` building the
  commands from shared option groups, a lazy catalog (`bx --help` imports
  no dataset; `python -m brainspresso.cli` lists them), one `--layout`
  option with `sites:<kind>` (`--site-layout` deprecated), `--exclude-keys`
  alone keeps files that match no key, compost takes an exclusive run
  lock that harvest and roast hold shared; default layout `study` is
  final; nested shards stay handed back to the main process (§5).
* r10: step 4 design review. `cli.main` registers only the named
  dataset (placeholders for the others, no cyclopts lazy commands, which
  lose the long help; cyclopts floor at the measured minimum); `--subs`
  verbatim unless a dataset sets `subject_width`; `--sites` free text validated by
  `select_sites`; run lock with run records, failing fast for all three
  commands, a pool-initializer lock in roast workers and stale-record
  checks; narrow `user_errors` (`UsageError`); `Args` dataclass and
  `for_dataset` groups; per-command keys; `HarvestOptions`; missing
  optional dependencies become stub commands; ABIDE-I `download-nitrc/
  xnat` dropped; grind items left open for 4g.
* r11: step 4 implemented (§6 as built). `sources()` stays raw and
  `configured_sources(args)` configures it; `groups(command)` picks the
  standard groups (`harvest_subjects`, `hosted`, `has_sites`,
  `folder_choices` declarations); `execute` is the Python API of a whole
  command; `INDIDataset` wraps a `Project`; roast `--source` only where
  sources have different roasters; tables rewritten by compost rely on
  the exclusive run lock (no per-file `File` lock); cyclopts floor 3.17
  (measured), with a CI job at the floor.
* r12: review of PR #19. Compost applies `--exclude-sites`; command
  functions return nothing; `--ignore-runs` (run records) is separate
  from `--force`; a sites harvest checks `--subs` before downloading,
  new sites take their run lock and a site command takes its parent's;
  legacy roasts expand subject files and ranges; nested key trees give
  choices, checks and a hierarchy in the help; `KeySpec.off` (IXI
  `trace`) and `effective`; options are checked and a missing dataset
  refused before anything is created; nested run-lock failures keep the
  outer count.
* r13: PR #19 round 3. Top-level imports (ruff `PLC0415`; kept local
  ones say why; `read_manifest` moved to `bids/layout.py` to break the
  cycle with `bids/harvest.py`); roast wording ("legacy roasters"
  for the `Bidsifier` classes); ADHD-200 `--bids/--no-bids` (`--bidsify`
  a hidden alias); docstring-only `roast.py` modules and ABIDE-I's CLI
  wrappers deleted; locks taken parents first, a new site's lock takes
  `--wait`, a site command always locks a sites parent; `KeySpec.leaves`
  expands groups (`all` covers everything); ABIDE-I/II `--exclude-keys`
  with several keys fixed (`set().union`), and the roast help says what
  the legacy roasters do not filter; unnamed key groups are not keys;
  bad `--subs` ranges are `UsageError`s.
* r14: round 4. A command on a site takes its parent's run lock shared
  and its own mode only on its target (compost of a site runs next to a
  roast of a sibling; a compost of the whole parent still excludes every
  site command); legacy roasters get the keys given plus the leaves
  below them (`fs` stays `fs`); deprecated flags are logged warnings;
  `--subs` names a file before a range (`C:\subs.txt`); ABIDE-I XNAT and
  NITRC roast bugs listed for step 5.
* r15: step 4g. §7 rewritten as the grind contract (model, units,
  state and skip by signature, graph and backends, failures, BIDS
  Apps, containers, overrides, cross-dataset index, provenance, what
  ports may assume, open questions); interface stubs in
  `brainspresso/grind/` (no behaviour). Decided: grind takes a second
  lock file `<state>/grind` per running process instead of
  `<state>/run`; dataset references `<name>/<argument>[/<site>]`;
  sites layouts ground site by site with group outputs in the parent;
  static graph, scheduler dependencies as hints (tasks re-check their
  requirements); stale units stop the run by default.
* r16: review of the grind contract (PR #23). Variants in task ids,
  records, `Task`, `Scope` and `GrindContext`, required pipelines read
  in their default variant unless `--with`; run records per lock name
  and `_held`/`hold_shared` keyed by (state, name); `LOCKS["grind"]`
  names the grind lock; `GrindContext.pipeline` is the name (`spec` the
  object) and records are merged, not replaced; `Executor.attach`,
  `NotReady` and job liveness for detached runs; pairwise dependencies
  only between arrays built together (`TaskArray`); the engine polls
  `Handle.done()` (`TaskLost`, `Executor.local`); per-session claims
  for BIDS Apps with a session flag, `Adopt` actions record claimed
  paths; tasks lock every dataset they read; image digests compared
  apart from the signature (`ContainerRuntime.inspect`, no pull in dry
  runs); a requirement on a pipeline depends on its terminal stages.
* r17: owner decisions on plans. Roast and grind plan in memory by
  default; `--plan PATH` writes a YAML plan (`<state>/plans/`) and
  runs nothing; `bx percolate PLAN` runs, retries, cancels or reports
  it (one lock per plan). A plan task is a scoped re-invocation
  (`StagePlanner`), not a pickled callable; plan-time choices (stale
  policy, variants, images, configs) are recorded. New stage-agnostic
  stubs in `brainspresso/percolate/` (`Plan`, `ExecutionOptions`,
  `Profile`, `Scheduler`, `Job`). Cluster jobs are native batch
  scripts that run `bx percolate PLAN --task <id>` (`--write-jobs`);
  no submitit, Dask or Ray. Path maps (`translate`, `resolve_roots`)
  and staging to node-local scratch (`grind/staging.py`). Removed:
  `grind/execution.py` (`Executor`, `Handle`, `After`, `attach`,
  `NotReady`, `TaskLost`, requeues) and `Task`/`Graph`/`TaskArray`.
  Profiles and dataset overrides also come from the config file
  (separate PR).
  Dataset references use the CLI name (`abide1/NYU`).
* r18: final owner decisions. The open questions of §7 become
  decisions; plans are written with a `SafeDumper` subclass that
  quotes every string (no writer refusal); `--wait`/`--detach` (wait
  locally, detach on schedulers, profile `detach`, CLI wins);
  `--status --watch` (read-only); `Profile.lock_wait`. One question
  left: the name clash with the existing `--wait SECONDS`.
