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
  path, summary)}` and `load(name) -> Dataset`, plus two tables for the
  config file: `ARGUMENTS` (arguments of the entries that take some)
  and `KEYS` (config keys of each command over every dataset).
  `scripts/make_catalog.py` regenerates it; a test checks that each
  summary is the first line of `help` and that it is up to date.
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

### Config file (`brainspresso/config.py`)

Defaults of the options of the dataset commands, from TOML files and
environment variables. Layers, later wins (`Loader.layers`):

1. built-in defaults (option groups, `for_dataset`, `option_defaults`);
2. the user file, `~/.brainspresso/config.toml`
   (`$BRAINSPRESSO_CONFIG_HOME` replaces the folder; the tests point it
   at an empty folder, `tests/conftest.py`);
3. the root file, `<root>/.brainspresso/config.toml`: `<root>` is the
   `path` argument, else a `path` from the user file, `--config` or the
   environment, else `$BDP_PATH`, else the current folder (as for the
   dataset folder, also with `--out`);
4. the dataset file, `<DS>/.brainspresso/config.toml` (`--out`, else
   `<root>/<folder>`, with the bound arguments; a site dataset `--out
   <DS>/<site>` reads its own, not its parent's);
5. the files of `bx --config FILE`: a global option, before the dataset
   name, repeatable, applied in order (`cli.global_options`; TOML, or
   YAML if PyYAML is importable);
6. `BRAINSPRESSO_OPT_*` variables;
7. the command line.

A later layer wins over any section of an earlier one (an env variable
for `[defaults]` beats `[ixi.harvest]` in a file). Within one layer,
the most specific section wins (`Entry.rank`): `[defaults]` <
`[<command>]` < `[<dataset>]` < `[<dataset>.<command>]`, with one more
level per argument (`[openneuro]` < `[openneuro.harvest]` <
`[openneuro.ds000224]` < `[openneuro.ds000224.harvest]`; command names
are a fixed set, so accessions and commands do not clash). Datasets are
named by their command name, case-insensitively (`[abide1]`,
`[ABIDE1]`, `[unc-wisconsin]`, `[fcon1000]`); `config.dataset_names` is
the one naming rule, and the catalog's generated `ARGUMENTS` table
(`{"openneuro": ("dataset",)}`) gives the extra levels without
importing a dataset. The same section written twice in one file
(`[ixi.harvest]` and `[IXI.harvest]`) with the same key is an error.

Keys are the long flags without `--` (`exclude-keys`, or
`exclude_keys`; OpenNeuro's `--source` is `source`, whatever the field
name), `no-<flag> = true` for a boolean flag (`no-bids`, `no-json`).
Lists are TOML arrays (a scalar is a list of one, `[]` the empty list,
which replaces a non-empty default); values become cyclopts tokens
(`config.tokens`), so lists, `Literal` choices (per dataset, from
`for_dataset`), `int | str`, `bool | None` and the deprecated aliases
(`site-layout`, `bidsify`: logged when used, as on the command line)
convert and fail as on the command line, and the error names the file
and the section (`Invalid value for --jobs from <file> [ixi.harvest]`).

Never read from a file or variable (an error naming the key and the
file): `yes`/`y`, compost `force` and `ignore-runs`, `password`,
`token`, `config`. A root or dataset file cannot set `path` or `out`
(they locate it), and `out` only goes in the section of one dataset
(not `[defaults]`, `[harvest]`, nor `[openneuro]` without an
accession). The positional arguments of a dataset (the accession) are
not keys.

Checks: every file is checked when it is read (`config.parse`):
sections (unknown ones with a did-you-mean), forbidden and location
keys, and keys against the generated `catalog.KEYS` (the config keys of
each command over every dataset, so that `[harvest] discs = ...`, an
OASIS-I option, is accepted and ignored by IXI). The sections of the
running dataset are also checked against its own options
(`config.resolve`): `[ixi.harvest] tag` is not an option of IXI's
harvest, `[ixi] discs` of no IXI command. `[profiles.<name>]` and
`grind` tables (`[grind]`, `[<dataset>.grind.<pipeline>]`) are reserved
for the execution profiles and pipelines of §7 and ignored
(`TODO(§7)` in `config.py`).

Environment: `BRAINSPRESSO_OPT_[<SECTION>__...]<KEY>`, `__` between
the parts, case-insensitive, `_` for `-` in dataset names and keys
(`BRAINSPRESSO_OPT_JOBS`: `[defaults]`; `..._HARVEST__JOBS`;
`..._ABIDE1__ROAST__EXCLUDE_KEYS="rest anat"`, split on spaces;
`..._NKI_RS__HARVEST__JOBS`; `..._OPENNEURO__DS000224__HARVEST__TAG`;
`..._ADHD200__HARVEST__NO_BIDS=1`). The dedicated prefix keeps them
apart from `BRAINSPRESSO_LOCKS`, `BRAINSPRESSO_LOCK_TIMEOUT`,
`BRAINSPRESSO_LOCK_DIR`, `BRAINSPRESSO_CONFIG_HOME`, `BDP_PATH` and the
credential variables, which keep their meaning.

Mechanism: `config.Loader` is the cyclopts config callable of the root
app (`App(config=loader)`, cyclopts >= 3.17; its first argument is a
list of apps in cyclopts 3 and the app in >= 4, and is not used). It
runs after the command line is parsed: it reads the parsed `path`,
`--out` and arguments, finds the files, resolves the values of the
command's options (`register.config_options`: one `OptionInfo` per
long flag of the flattened groups) and appends tokens only to the
arguments that have none. `cli.dataset_app` records the dataset in the
loader. `bx config show [DATASET [COMMAND [ARGS...]]]` parses the
command with `App.parse_args` (nothing runs) and prints every option,
its effective value and its origin (`built-in`, `<file> [<section>]`,
`env $VAR`, `CLI`); without a dataset, the merged sections of every
layer. `bx config paths [DATASET [ARGS]] [--path] [--out]` lists the
files looked up, whether they exist, and the variables set. `config.py`
imports neither datasets nor option groups (`bx --help` imports it;
measured about 5 ms), and `bx config` imports only the dataset it
shows. Example:

```toml
[defaults]
path = "/data/bids"
[harvest]
jobs = 4
[ixi]
keys = ["T1w", "T2w"]
[abide1.roast]
exclude-keys = ["rest"]
[openneuro.ds000224.harvest]
tag = "1.0.4"
```

Limitations: on cyclopts 3.17 to 4.0 (measured: 3.17, 3.18, 3.24, 4.0)
`--json` cannot be parsed at all, from the command line or a file
(`Cannot Union types that consume different numbers of tokens` for the
`Literal | bool` field); it works on 5.5, and its tests are skipped
below. A null value (YAML) is refused: TOML has none and "unset" is the
absence of the key.

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

### Grind seam (open items for 4g, not decisions)

`grind` would be one more builder in `register.COMMANDS` (`bx <ds>
grind <pipeline>`; `LOCKS` in `datasets/base.py` gives its lock mode). Open: naming a bound dataset in `--datasets`
(`openneuro/ds000224`); grind iterating `layout.datasets()` for sites
layouts; whether one lock domain per dataset is too coarse for long
grind jobs. Ports must not assume that grind units take the run lock.

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
* Added: `bx config show|paths`, the global `--config FILE`, and config
  files and `BRAINSPRESSO_OPT_*` variables as option defaults (Config file).

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
* Config file (`tests/test_config.py`): every layer in order, the
  most specific section, root and dataset files from the parsed path
  (`--out`, a configured `path`, `$BDP_PATH`), dataset names and
  OpenNeuro accessions, forbidden, unknown and location keys, type and
  choice errors, lists, bools, `Literal`s, deprecated aliases, repeated
  `--config`, environment variables, `bx config show|paths`, and `bx
  --help` / `bx config` importing no dataset.

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
  cannot be enumerated; outputs depend on config). Roast shards already
  do this with the task state below (`config_hash`: their input
  signature).
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
  file. Containers: Docker locally, Apptainer on HPC. The local backend
  exists (`actions.engine.LocalExecutor.submit(task, resources=None) ->
  Future`, used by roast; `task` is a picklable callable).
* State: **one state file per task** under `.brainspresso/` (concurrent jobs
  never write the same file); the manifest is an index rebuilt from them.
  Format (defined in step 2b-i, written by roast shards since 2b-ii-fix):
  `<state>/tasks/<task id>.json`, the task id URL-quoted into the file
  name, written atomically:

  ```json
  {"schema": 1, "task": "grind/fs/sub-01/ses-1",
   "stage": "roast | grind", "pipeline": "<name> | null",
   "unit": {"sub": "01", "ses": "1"},
   "status": "pending | running | done | failed | skipped",
   "config_hash": "<hex> | null",
   "started": "<ISO 8601> | null", "finished": "<ISO 8601> | null",
   "error": "<message> | null",
   "outputs": ["<path relative to <DS>>"]}
  ```

  Roast shards add `run` (id of the roast that wrote it), `counts`
  (`{tag: {outcome: n}}`), `progress` (`{tag: [total, advance]}`) and
  `children` (shards it yielded); they write it when they start, on
  every progress event, at most every 0.5 s on outcomes, and when done
  (`done` without errors, else `failed`). Shards without an id that run
  in a worker use a temporary `roast/.auto/...` id, removed once
  collected.
* Grind builds on the stage-agnostic engine (`actions.engine`, step
  2b-iv) as roast does: its own `Context` (`stage = "grind"`, a
  `pipeline` attribute, which task records pick up) and its own
  `Policy` (units to run; config hash in `fingerprint`, part of the
  input signature of shards), with the same producers and shards,
  reporting, task state, `submit` seam and `File` locks (outputs of
  concurrent units on shared storage are protected by the dataset's
  `.brainspresso/locks/`). Whether BIDS-App units are actions (a claim
  on their output folder, or a done-marker action) or plain shards is
  decided in step 4g.
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
| 2a | `Selection`: modality-agnostic companions, directory formats, generic entities, per-path `keeps` | High-Effort Opus | done (333 passed) |
| 2b-i | Manifest/state schema; `Layout` (study / flat / sites, sub-layouts, `relative`, detection); `--layout`; every writer of `rawdata/` (roasters, compost, harvest, INDI) goes through `Layout`; `rawdata/` removed | High-Effort Opus | done (421 passed, 32 xfailed: the 16 known bugs, under both layouts) |
| 2b-ii | Runner + action outputs/claims/kind/tag + shards; tables via `.brainspresso/orig` + trim + `finalize`; pilot ports UNC-Wisconsin, IXI, OASIS-III | High-Effort Opus, Fable review | done (461 passed, 28 xfailed: OASIS-III sessions name and exclude-keys fixed) |
| 2b-ii-fix | Review fixes: readers take no lock (shared and read-only sources); tables use `different` under `skip`; every shard runs in the pool (nested ones handed back), every `finalize` runs; pool failures fall back to the main process, common steps in a `finally`, run `status`; key classification (`key_of`) instead of a union; kinds (`meta` for bval/bvec, mixed kinds rejected, `Fail` never hidden), `staging`, category folders; claims yield actions only (lists of folders, "empty"); task state per shard, progress from it, input-signature skip, `submit` protocol; `ctx.source`; port helpers | High-Effort Opus | done (511 passed, 28 xfailed) |
| 2b-iv | Actions core / roast layer split (`actions/engine.py` with an acceptance `Policy`, `roast/`, `bids/kinds.py`), RW locks (`actions/locks.py`: shared readers, exclusive writers, lock files in `.brainspresso/locks/`, timeouts), docstrings (pydocstyle, numpy convention, on `actions/`, `roast/`, `bids/`) | High-Effort Opus | done (608 passed, 2 skipped, 28 xfailed, with step 3 merged) |
| 2c | Dataset module names match the stage names (`download.py` → `harvest.py`, `bidsify.py` → `roast.py`); temporary re-exports of the 2b-iv split removed | Low-Effort Opus | done |
| 2b-iii | Hybrid placement for hosted-BIDS sources; pilot on one INDI project and OpenNeuro | High-Effort Opus | |
| 3 | Sources: `RemoteEntry`, `provides`/`roaster`/`fetch`, per-key defaults, `LocalSource`, label mapping, credentials; drop request-option shim | High-Effort Opus | done (536 passed, 28 xfailed; every harvest on sources, ABIDE-II S3 opt-in only, see §4) |
| 4 | `Dataset` + `register` (absorbs INDI `Project`, `add_compost`); shared option groups; catalog; `--layout sites:<kind>`; run lock | High-Effort Opus | done (913 passed, 2 skipped, 28 xfailed, after merging #18; §6) |
| 4g | Grind contract written down (pipelines, stages, state, submit protocol), doc + interfaces only, so that ports do not bake in assumptions | Fable plan | |
| 5 | Port remaining datasets one by one; fix known bugs | Low-Effort Opus each | |
| 6 | Tabular helpers, converter registry, archive helpers | Low-Effort Opus | |
| 7 | Grind implementation: local backend, one BIDS-App pipeline, submitit; then SPM Segment from `wiip-grind` | High-Effort Opus | |
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
  `brainspresso/download/` (step 3); other packages are exempt through
  one negated `per-file-ignores` pattern in `pyproject.toml`
  (`!brainspresso/{actions,bids,download,roast,sources}/**`; two
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
* Layering: `actions/` (stage-agnostic) imports neither `bids/` nor
  `roast/`; `roast/` builds on both; BIDS knowledge lives in `bids/`.

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
* Grind runtime for SPM (MATLAB, compiled standalone SPM, or Python port).
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
* r15: config file (§6, "Config file"). Layered defaults of the options
  (user, root, dataset, `--config`, `BRAINSPRESSO_OPT_*`, command line)
  read by a cyclopts config callable after parsing; sections
  `[defaults]`, `[<command>]`, `[<dataset>]`, `[<dataset>.<command>]`
  named by the command names (owner decision, over module paths and
  folder names; one naming function), an accession level for OpenNeuro;
  confirmations, secrets and `--config` never configurable; `bx config
  show|paths`; catalog tables `ARGUMENTS` and `KEYS`.
