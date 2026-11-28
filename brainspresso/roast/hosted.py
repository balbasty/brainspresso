"""
Hybrid placement of datasets hosted as BIDS: harvest and metadata roast.

```python
harvest_hosted(entries, layout, selection, folder="openneuro")
roast_hosted(layout)                    # `bx <ds> roast`, on its own
```

`harvest_hosted` is the harvest of a source hosted as BIDS (OpenNeuro,
the INDI projects): it selects the entries of the listing, downloads
the files that are never repaired or trimmed (`bids.hosted.placement`:
"direct") to their final path, the metadata to `sourcedata/<folder>/`
under their remote, unremapped path (pristine copies, refreshed against
the listing whatever `--if-exists` says), writes the files that the
listing generates (`InlineEntry`) to `<state>/generated/`, and merges
every file into the manifest with its origin (`direct`, `roasted`,
`generated`) and, for roasted and generated files, its `source` (the
*base*). It then runs `roast_hosted` on what it placed.

`roast_hosted` is the **metadata roast**. The model (`docs/design/
architecture.md`, §4, "State model"):

* every visible metadata file, tables included, is
  `render(base, options, presence)` (`bids.hosted.render`: repair,
  fill-in, paths remapped like the images, `IntendedFor` and table rows
  trimmed to what is present), written by one `RoastMetadata` action
  (`actions.File`: shared locks on its inputs, an exclusive lock and an
  atomic rename on its output);
* a missing base freezes the file: it is left as it is (one info line),
  and only a missing visible file too is an error (`Fail`);
* a visible file is edited iff its SHA-256 differs from the recorded
  one; an edit is kept under `update` (an edited field map keeps the
  user's keys and takes `IntendedFor` from the render); without a
  record (an older dataset), a file equal to the render is adopted,
  any other follows `bids.harvest.MIGRATION_RULE` (re-exported here);
* only `_record` writes the hashes, of the files that hold the render.

It runs after the images are placed (repairs read siblings:
`_phasediff`, `IntendedFor`), on its own (`bx <ds> roast`: everything
the manifest records, from the bases), and after a compost of BIDS data
(`existing_only`: only the files still there; compost never re-creates
a file).

Rules (`docs/design/architecture.md`, §4):

* `--sidecars yes|no|only` is the placement policy (`bids.kinds.
  accepts`): at harvest, "no" does not fetch the JSON sidecars of data
  files, "only" does not fetch data files (images, recordings); in a
  roast, it drops the actions of the kinds it excludes (files on disk
  are left as they are).
* `--repair/--no-repair` (default on): recorded per file, in the shard
  config; a roast with the default repairs the files it writes (also
  what a harvest with `--no-repair` placed, under `--if-exists
  update`).
* Selection: a harvest roasts what it selected and the files of the
  subjects it touched; a roast takes `--subs`, `--exclude-subs`,
  `--sessions` (files that describe no selected data are left alone).
* `--if-exists`: governs the visible files only. "skip" (default) is
  pure: an existing file is left alone (not rendered, no log line, no
  orphan removed), but for the trimmed tables and the field maps whose
  `IntendedFor` is trimmed, which follow "update"; a missing file is
  written. Under "update" a file is rendered and replaces the visible
  one if its content changed and it is not edited (the edit test
  above); "different" replaces edits too, "overwrite" rewrites, "error"
  refuses an existing file, "refresh" compares mtimes with the base.
* Re-runs: one shard per subject (`roast/hosted/sub-<label>`), whose
  input signature is its bases, fill-in templates, the files of the
  subject on disk, `.bidsignore` and `Shard.config` (per-file options,
  `RENDER_VERSION`), skipped under "skip" when unchanged.
"""
from __future__ import annotations

import functools
import os
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator

from humanize import naturalsize

from brainspresso.actions.action import Action, Fail, IfExists
from brainspresso.actions.engine import Shard
from brainspresso.actions.writers import Unlink
from brainspresso.bids import harvest
from brainspresso.bids.harvest import (
    MIGRATION_RULE,
    InlineEntry,
    _filters,
    manifest_files,
    record_run,
    select_entries,
    sha256,
    update_files,
    write_inline,
)
from brainspresso.bids.hosted import (
    FillPlan,
    FillTemplate,
    is_data,
    placement,
    plan_fill,
    render,
)
from brainspresso.bids.keys import KeySelection
from brainspresso.bids.kinds import accepts, default_kind
from brainspresso.bids.layout import (
    DERIVATIVES_DIR,
    DESCRIPTION,
    Layout,
    SitesLayout,
    complete_description,
    description_bytes,
    resolve_layout,
)
from brainspresso.bids.repair import (
    dumps,
    loads,
    read_sibling,
    repair_sidecar,
)
from brainspresso.bids.select import OPTIONAL_FOLDERS, Selection
from brainspresso.bids.tables import is_trimmable
from brainspresso.download import Downloader, DownloadManager
from brainspresso.roast.context import RoastContext
from brainspresso.roast.policy import RoastPolicy
from brainspresso.roast.run import RoastReport, run_roast
from brainspresso.sources.base import write_content

lg = getLogger(__name__)

__all__ = ["MIGRATION_RULE", "RENDER_VERSION", "RoastMetadata",
           "harvest_hosted", "hosted_files", "roast_hosted",
           "roast_metadata"]

#: Origins of the files that a metadata roast writes
PLACED = ("roasted", "generated")
#: Progress group (and task ids: `roast/hosted/sub-<label>`)
TAG = "hosted"
#: Version of what `render` writes: bump it when the repairs or the
#: rendering change, so that unchanged shards are roasted again
RENDER_VERSION = 1


# ----------------------------------------------------------------------
#   Harvest
# ----------------------------------------------------------------------

def _rel(layout: Layout, path: Path) -> str:
    return Path(path).relative_to(layout.root).as_posix()


def _remote_fields(entry, rel: str) -> dict:
    """
    Remote path of an entry, for the manifest (cleared fields: None).

    `remote_root` is the folder of the remote BIDS dataset that holds it
    (INDI: its site), recorded on every entry that has one, renamed or
    not: the paths that its sidecars and tables give are relative to it.
    `remote` is its path there, recorded where it differs from the local
    one. None for a listing without remote roots (OpenNeuro: placed
    under its remote names).
    """
    meta = getattr(entry, "meta", None) or {}
    remote = meta.get("remote")
    root = str(meta.get("remote_root") or "").strip("/")
    if remote is None or not root:
        return {"remote_root": None, "remote": None}
    if remote.startswith(root + "/"):
        remote = remote[len(root) + 1:]
    return {"remote_root": root, "remote": remote if remote != rel else None}


def harvest_hosted(
    entries: list,
    root: str | os.PathLike | Layout,
    selection: Selection,
    *,
    folder: str,
    where: Iterable[str] = (),
    trim: bool = True,
    if_exists: IfExists.Choice = "skip",
    jobs: int = 8,
    dry_run: bool = False,
    listing: bool = False,
    provenance: dict | None = None,
    repair: bool = True,
    layout: str | None = None,
    insecure: bool = False,
    chunk_size: int | None = None,
    check_subjects: bool = True,
    sidecars: str = "yes",
    fill: Iterable[FillTemplate] = (),
    site: str | None = None,
    display: bool = True,
) -> list:
    """
    Harvest a dataset hosted as BIDS, with hybrid placement.

    Parameters
    ----------
    entries : list[RemoteEntry | InlineEntry]
        Full listing (flat BIDS view, after remapping); `entry.meta`
        may give the `remote` path (default: `entry.path`), the
        `remote_root` and the `site` of an entry
    root : path | Layout
        Dataset folder, or its layout (study or flat)
    selection : Selection
        Filters
    folder : str
        Sub-folder of `sourcedata/` for the pristine copies (e.g.
        "openneuro", "fcp-indi")
    where : list[str]
        Queries on participants.tsv columns
    trim : bool
        Trim tables and `IntendedFor` to what is present (recorded per
        file: a later roast keeps the choice)
    if_exists : {"skip", "overwrite", "different", "refresh", "error"}
        Downloads of data files, generated files placed directly (left
        alone under "skip" once they exist) and the visible files of the
        roast (`roast_hosted`); pristine copies and generated bases are
        always refreshed against the listing
    jobs : int
        Concurrent downloads
    dry_run, listing : bool
        Print what would be (or is) fetched
    provenance : dict
        Fields of the harvest run in the manifest
    repair : bool
        Repair the metadata (`bids.repair`), recorded per file
    layout : {"study", "flat"}, optional
        Layout of a new dataset
    insecure : bool
        Do not verify TLS certificates
    chunk_size : int, optional
        Download chunk size, in bytes
    check_subjects : bool
        Check `selection.subs` against the listing
    sidecars : {"yes", "no", "only"}
        Placement policy (see the module)
    fill : list[FillTemplate]
        Fill-in templates of the dataset (`bids.hosted`)
    site : str | None
        Site that this dataset is (one dataset per site)
    display : bool
        Show the progress table of the metadata roast

    Returns
    -------
    list
        The entries fetched (or that would be, in a dry run)
    """
    sidecars = str(sidecars or "yes")
    chosen, selection = select_entries(
        entries, selection, where=where, listing=False,
        check_subjects=check_subjects,
    )

    def fetched(e):
        if placement(e.path) == "roasted":
            return accepts(sidecars, default_kind(e.path))
        return not (sidecars == "only" and is_data(e.path))

    selected = [e for e in chosen if fetched(e)]
    if len(selected) < len(chosen):
        print(f"Not fetched (--sidecars {sidecars}): "
              f"{len(chosen) - len(selected)} files")
    if listing or dry_run:
        for e in selected:
            size = naturalsize(e.size) if e.size is not None else "?"
            where_to = "sourcedata" if placement(e.path) == "roasted" \
                and not isinstance(e, InlineEntry) else "direct"
            print(f"{size:>10}  {e.path}  [{where_to}]")
    if dry_run or not selected:
        return selected

    layout = resolve_layout(root, layout)
    if isinstance(layout, SitesLayout):
        raise ValueError(f"{layout.root} holds one dataset per site: "
                         f"harvest each site")
    layout.ensure_state()
    # entries are merged into the manifest (None clears a field); only
    # the roast writes the hashes of roasted files (`_record`)
    files: dict[str, dict] = {}
    placed: set[str] = set()
    downloads: dict[Path, Downloader] = {}
    raw = Path(layout.raw)
    present: list[str] = []
    nb_generated = 0
    flags = {"repair": True if repair else None,
             "trim": None if trim else False}

    def download(e, dst, ifexists):
        if dst not in downloads:
            downloads[dst] = Downloader(
                e.url, dst, size=e.size, mtime=e.mtime,
                digests=e.digests or None, ifexists=ifexists,
                insecure=insecure,
                **({"chunk_size": chunk_size} if chunk_size else {}),
            )

    unchanged: dict[Path, tuple] = {}
    touched: set[str] = set()
    fetched_paths = {e.path for e in selected}
    remotes: dict[str, dict] = {}
    for e in chosen:
        local = layout.resolve(e.path)
        dataset, rel = layout.relative(local)
        key = _rel(layout, local)
        if Path(dataset) == raw:
            present.append(rel)
            remotes[key] = _remote_fields(e, rel)
        if _subject(rel):
            touched.add(_subject(rel))
        if e.path not in fetched_paths and is_data(e.path):
            # selected, not fetched (`--sidecars only`): it counts as
            # present when tables and `IntendedFor` are trimmed
            if not local.exists():          # (fetched by an earlier run)
                meta = getattr(e, "meta", None) or {}
                files[key] = {"url": e.url, "size": e.size,
                              "origin": "direct", "fetched": False,
                              "site": meta.get("site"),
                              **remotes.get(key, {})}
    for e in selected:
        local = layout.resolve(e.path)
        key = _rel(layout, local)
        meta = getattr(e, "meta", None) or {}
        extra = {"site": meta.get("site"), **remotes.get(key, {})}
        roasted = placement(e.path) == "roasted"
        if isinstance(e, InlineEntry):
            if roasted:
                base = layout.generated(local)
                nb_generated += write_content(base, e.content, "overwrite")
                files[key] = {"origin": "generated",
                              "source": _rel(layout, base),
                              "trim": flags["trim"], "repair": None,
                              # (earlier versions of its template: an
                              # unedited copy of one is ours, `_state`)
                              "previous": sorted(getattr(e, "previous", ())
                                                 or ()) or None,
                              **extra}
                placed.add(key)
            else:
                # placed directly: left alone under `skip` once it exists
                nb_generated += write_inline(e, local, if_exists)
                files[key] = {"origin": "generated", "size": e.size,
                              **extra}
                if local.is_file() and local.read_bytes() == e.content:
                    files[key]["sha256"] = sha256(e.content)
            continue
        remote = {"url": e.url, "size": e.size,
                  "digests": e.digests or None}
        if roasted:
            path = PurePosixPath(str(meta.get("remote") or e.path))
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"Invalid remote path: {path}")
            pristine = layout.source.joinpath(folder, *path.parts)
            # pristine copies follow the listing whatever `--if-exists`
            # says (it governs the visible files): by digest, else by
            # size and mtime, else downloaded again (`unchanged` keeps
            # the mtime of an identical copy: shard signatures)
            policy = "different" if e.digests else \
                "refresh" if e.mtime is not None and e.size is not None \
                else "different"
            if pristine.is_file() and pristine not in unchanged:
                unchanged[pristine] = (sha256(pristine.read_bytes()),
                                       pristine.stat().st_mtime_ns)
            download(e, pristine, policy)
            files[_rel(layout, pristine)] = {**remote, "origin": "direct"}
            # (a None clears what an older, direct entry had)
            files[key] = {"origin": "roasted",
                          "source": _rel(layout, pristine),
                          "url": e.url, "size": None, "digests": None,
                          **flags, **extra}
            placed.add(key)
        else:
            download(e, local, if_exists)
            files[key] = {**remote, "origin": "direct", "fetched": None,
                          **extra}
    if nb_generated:
        lg.info(f"Wrote {nb_generated} generated files")
    # (one manager per `IfExists`: a manager imposes its own)
    groups: dict[str, list] = {}
    for downloader in downloads.values():
        groups.setdefault(downloader.ifexists, []).append(downloader)
    for policy, group in groups.items():
        DownloadManager(group, ifexists=policy, jobs=jobs,
                        path="short").run()
    for path, (digest, mtime) in unchanged.items():
        # downloaded again, identical: its old mtime back
        try:
            if path.stat().st_mtime_ns != mtime and \
                    sha256(path.read_bytes()) == digest:
                os.utime(path, ns=(path.stat().st_atime_ns, mtime))
        except OSError:
            pass
    record_run(layout, "harvest", {
        **(provenance or {}),
        "filters": _filters(selection),
        "where": list(where or []),
        "sidecars": sidecars,
        "nb_files": len(selected),
    }, files=files, merge=True)
    report = roast_hosted(
        layout, fill=fill, site=site, paths=placed, subjects=touched,
        present=present, json=sidecars, repair=repair, ifexists=if_exists,
        display=display, info={"trigger": "harvest"},
    )
    if report.status != "complete":
        lg.warning(f"Metadata roast of {layout.root}: {report.status} "
                   f"({report.summary()})")
    return selected


# ----------------------------------------------------------------------
#   Metadata roast
# ----------------------------------------------------------------------

def hosted_files(layout: Layout) -> dict[str, dict]:
    """
    Roasted and generated files of a hosted dataset (from its manifest).

    Parameters
    ----------
    layout : Layout

    Returns
    -------
    dict[str, dict]
        Manifest entries with a `source`, by path relative to
        `layout.root`
    """
    return {
        key: entry for key, entry in manifest_files(layout).items()
        if entry.get("origin") in PLACED and entry.get("source")
    }


def _subject(rel: str) -> str | None:
    head = rel.split("/", 1)[0]
    return head[4:] if head.startswith("sub-") and "/" in rel else None


def _sibling_reader(folder: Path, sources: dict[str, Path]):
    """Siblings of a roasted sidecar: their pristine copy, else on disk."""
    def read(name: str) -> dict | None:
        path = sources.get(name, folder / name)
        meta = read_sibling(path) if Path(path).exists() else None
        return repair_sidecar(meta, name) if meta is not None else None
    return read


def _read_json(path: Path) -> dict | None:
    try:
        meta = loads(Path(path).read_text(encoding="utf-8",
                                          errors="replace"))
    except (OSError, ValueError):
        return None
    return meta if isinstance(meta, dict) else None


def _read_json_bytes(data: bytes) -> dict | None:
    try:
        meta = loads(data.decode("utf-8", errors="replace"))
    except ValueError:
        return None
    return meta if isinstance(meta, dict) else None


def _plan(
    layout: Layout,
    files: dict[str, dict],
    *,
    fill: Iterable[FillTemplate] = (),
    site: str | None = None,
    paths: Iterable[str] | None = None,
    subjects: Iterable[str] | None = None,
    present: Iterable[str] = (),
    existing_only: bool = False,
    every: dict[str, dict] | None = None,
    repair: bool | None = None,
    selection: Selection | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Items of a metadata roast (picklable dictionaries).

    Returns
    -------
    items : list[dict]
        Files to roast (`_item`)
    orphans : list[dict]
        Fill-in sidecars written by earlier roasts whose templates no
        longer apply (removed if unedited)
    """
    every = manifest_files(layout) if every is None else every
    raw = Path(layout.raw)
    root = Path(layout.root)

    def where(key):
        dataset, rel = layout.relative(root / key)
        return Path(dataset), rel

    located = {key: where(key) for key in files}
    raw_placed = {rel: key for key, (d, rel) in located.items() if d == raw}
    present = set(present)
    subject_sites = {}
    # selected data not fetched (`--sidecars only`), per dataset and
    # subject ("*": all): present when tables and `IntendedFor` are trimmed
    unfetched: dict[tuple, list[str]] = {}
    # remapped paths, per remote dataset and remote subject folder
    remapped: dict[tuple, dict[str, str]] = {}
    for key, entry in every.items():
        try:
            dataset, rel = where(key)
        except ValueError:
            continue
        if entry.get("fetched") is False:
            unfetched.setdefault((dataset, _subject(rel)), []).append(rel)
            unfetched.setdefault((dataset, "*"), []).append(rel)
        if dataset != raw:
            continue
        present.add(rel)
        label = _subject(rel)
        if label and entry.get("site"):
            subject_sites.setdefault(label, entry["site"])
        if entry.get("remote_root"):
            remote = str(entry.get("remote") or rel)
            remapped.setdefault(
                (entry["remote_root"], remote.split("/")[0]), {}
            )[remote] = rel

    def read(rel):
        # keys of a top-level sidecar as roasted: the base (repaired as
        # this roast repairs), else the visible file (base composted);
        # None if neither can be read
        key = raw_placed[rel]
        entry = files[key]
        meta = _read_json(root / entry["source"])
        if meta is not None:
            fix = entry.get("repair") if repair is None else repair
            if fix and entry.get("origin") == "roasted":
                meta = repair_sidecar(meta, PurePosixPath(rel).name)
            return meta
        return _read_json(root / key)

    plans = plan_fill(
        list(fill), present=present, placements=raw_placed, read=read,
        site=site, subject_sites=subject_sites if site is None else None,
    ) if fill else {}

    context = {"files": files, "unfetched": unfetched, "remapped": remapped}
    items = []
    for key in sorted(files):
        entry = files[key]
        dataset, rel = located[key]
        plan = plans.pop(rel, None) if dataset == raw else None
        items.append(_item(layout, key, root / key, dataset, rel, entry,
                           plan, context))
    for rel, plan in sorted(plans.items()):
        # a new fill-in sidecar: its record (`sha256`) if it has one
        target = raw / rel
        key = _rel(layout, target)
        entry = {**every.get(key, {}), "origin": "generated"}
        entry.pop("source", None)
        items.append(_item(layout, key, target, raw, rel, entry, plan,
                           context))
    if repair is not None:
        for item in items:
            if item["origin"] == "roasted":
                item["repair"] = bool(repair)
                item["intended"] = item["fmap"] and (item["repair"]
                                                     or item["trim"])
    planned = {i["key"] for i in items}
    orphans = [
        {"key": key, "dst": str(root / key), "sha256": entry.get("sha256"),
         "subject": _subject(where(key)[1])}
        for key, entry in sorted(every.items())
        if entry.get("origin") == "generated" and not entry.get("source")
        and entry.get("fill") and key not in planned
    ]

    if paths is not None:
        # a harvest: what it placed, and every file of the subjects it
        # touched (new images re-trim `IntendedFor` and scans tables)
        paths = set(paths)
        touched = {_subject(located[k][1]) for k in paths if k in located}
        touched |= set(subjects or ())
        items = [i for i in items if i["key"] in paths or (
            i["subject"] in touched) or (
            i["source"] is None and i["subject"] is None)]
        orphans = [o for o in orphans
                   if o["subject"] is None or o["subject"] in touched]
    if selection is not None and (selection.sessions or selection.subs
                                  or selection.exclude_subs):
        # files of subjects that describe no selected data are left alone
        listed = sorted(present | {i["rel"] for i in items
                                   if i["dataset"] == str(raw)})
        kept = set(Selection(subs=selection.subs,
                             exclude_subs=selection.exclude_subs,
                             sessions=selection.sessions).select(listed))
        items = [i for i in items if i["dataset"] != str(raw)
                 or i["subject"] is None or i["rel"] in kept]
    if existing_only:
        items = [i for i in items if Path(i["target"]).exists()]
    return items, orphans


def _item(layout, key, target, dataset, rel, entry, plan, context) -> dict:
    root = Path(layout.root)
    files = context["files"]
    name = target.name
    trim = entry.get("trim", True) is not False
    repair = bool(entry.get("repair", False))
    trimmable = is_trimmable(rel) and dataset == Path(layout.raw)
    sibling = None
    if name.endswith("_phasediff.json"):        # (used if repaired)
        magnitude = name.replace("_phasediff.json", "_magnitude1.json")
        other = PurePosixPath(key).with_name(magnitude).as_posix()
        sibling = str(root / files[other]["source"]) if other in files \
            else str(target.with_name(magnitude))
    source = entry.get("source")
    fmap = name.endswith(".json") and target.parent.name == "fmap"
    intended = fmap and (repair or trim)
    previous = sorted({*(plan.previous if plan else ()),
                       *(entry.get("previous") or ())})
    # paths of the file in the remote layout (remapped labels, runs...)
    relabel = None
    if (fmap or name.endswith("_scans.tsv")) and entry.get("remote_root"):
        remote = str(entry.get("remote") or rel)
        mapping = context["remapped"].get(
            (entry["remote_root"], remote.split("/")[0]), {})
        if any(k != v for k, v in mapping.items()):
            if fmap:
                relabel = [mapping, remote.split("/")[0], rel.split("/")[0]]
            else:
                relabel = [mapping, PurePosixPath(remote).parent.as_posix(),
                           PurePosixPath(rel).parent.as_posix()]
    # the description of a derivative dataset: completed in the render
    # (`SourceDatasets`), so that the file equals it (edits by hash)
    source_url = _derivative_url(layout, target)
    # files that count as present without being on disk (unfetched)
    if trimmable and rel == "participants.tsv":
        present = context["unfetched"].get((dataset, "*"), [])
    elif intended or trimmable:
        present = context["unfetched"].get((dataset, _subject(rel)), [])
    else:
        present = []
    return {
        "key": key,
        "origin": entry.get("origin", "roasted"),
        "source": str(root / source) if source else None,
        "target": str(target),
        "dataset": str(dataset),
        "rel": rel,
        "repair": repair,
        "trim": trim,
        "intended": intended,
        "fmap": fmap,
        "rows": trimmable and trim,
        "table": trimmable,
        "sibling": sibling,
        "fill": plan,
        "subject": _subject(rel),
        "sha256": entry.get("sha256"),
        "previous": previous,
        "present": present,
        "relabel": relabel,
        "source_url": source_url,
    }


def _derivative_url(layout: Layout, target: Path) -> str | None:
    """
    `SourceDatasets` URL of a derivative description, at any depth.

    `derivatives/<a>/dataset_description.json`,
    `derivatives/<a>/derivatives/<b>/dataset_description.json`...: the
    `source_url` of the `DerivativeLayout` that owns it (it need not be
    on disk yet). None for any other file.
    """
    target = Path(target)
    if target.name != DESCRIPTION:
        return None
    try:
        parts = target.parent.relative_to(layout.derivatives).parts
    except ValueError:
        return None
    if not parts or len(parts) % 2 == 0 or any(
            p != DERIVATIVES_DIR for p in parts[1::2]):
        return None
    owner = layout
    for name in parts[::2]:
        owner = owner.derivative(name)
    return owner.source_url


def _here(folder: Path, dataset: Path, present: Iterable[str]):
    """Presence of a path relative to `folder`: on disk, or unfetched."""
    try:
        prefix = folder.relative_to(dataset).as_posix()
    except ValueError:
        prefix = "."
    prefix = "" if prefix == "." else prefix + "/"
    files = {p[len(prefix):] for p in present if p.startswith(prefix)}
    dirs = {"/".join(p.split("/")[:i]) for p in files
            for i in range(1, p.count("/") + 1)}

    def here(path: str, is_folder: bool) -> bool:
        found = (folder / path).is_dir() if is_folder \
            else (folder / path).exists()
        return found or path in (dirs if is_folder else files)
    return here


class RoastMetadata(Action):
    """
    Write a roasted metadata file (`bids.hosted.render`).

    Its inputs (base, sibling, fill-in templates) are read under shared
    locks, the output written under an exclusive lock, to a temporary
    file renamed into place (`actions.Action`).

    Parameters
    ----------
    item : dict
        What to roast (`roast.hosted._item`)
    ifexists : {"skip", "update", "different", "overwrite", "refresh", "error"}
        Behaviour if the output exists (default: that of the roast)
    tag : str
        Progress group
    keep : bool
        The output is an edited field map: keep its keys, take
        `IntendedFor` from the render
    backup : path, optional
        Copy the existing output there before it is replaced (the
        migration rule "replace"); nothing is copied unless the action
        writes
    """

    def __init__(self, item: dict, *, ifexists=None, tag=None,
                 keep: bool = False, backup=None):
        self.item = item
        self.keep = keep
        fill: FillPlan | None = item.get("fill")
        src = [p for p in (item["source"], item["sibling"]) if p]
        src += fill.paths if fill else []
        super().__init__(
            src=[p for p in src if Path(p).exists()], dst=item["target"],
            action=self.write, mode="wb", input="file", ifexists=ifexists,
            tag=tag, compare=True,
        )
        # (copied by the action core before it replaces the file)
        self.backup = Path(backup) if backup is not None else None

    def render(self) -> bytes:
        """Content of the output."""
        item = self.item
        source = item["source"]
        content = Path(source).read_bytes() if source else None
        target = Path(item["target"])
        sibling = None
        if item["sibling"]:
            # by the local name of the sibling (its pristine copy may
            # have another name: remapped labels)
            name = target.name.replace("_phasediff.json", "_magnitude1.json")
            sibling = _sibling_reader(target.parent, {
                name: Path(item["sibling"])})
        intended = (item["dataset"], item["rel"], item["trim"],
                    item["present"]) if item["intended"] else None
        rows = _here(target.parent, Path(item["dataset"]), item["present"]) \
            if item["rows"] else None
        out = render(content, target.name, repair=item["repair"],
                     fill=item["fill"], intended=intended, sibling=sibling,
                     relabel=item.get("relabel"), rows=rows)
        if item.get("source_url") is not None:
            # (as `write_descriptions` completes it: equal if complete)
            desc = _read_json_bytes(out)
            if desc is not None:
                done = complete_description(desc, item["source_url"])
                if done != desc:
                    out = description_bytes(done)
        if self.keep:
            # an edited field map: the user's keys, the render's targets
            mine = _read_json(target) or {}
            ours = _read_json_bytes(out) or {}
            kept = {k: v for k, v in mine.items() if k != "IntendedFor"}
            if "IntendedFor" in ours:
                kept["IntendedFor"] = ours["IntendedFor"]
            return dumps(kept).encode("utf-8") \
                if kept != mine else target.read_bytes()
        return out

    def write(self, file) -> None:
        """Write the output (the actor of the action)."""
        file.write(self.render())


def _state(item: dict) -> str:
    """
    State of the visible file of an item, by hash only.

    Returns
    -------
    {"absent", "ours", "edited", "unknown"}
        "ours": its hash is the recorded one (or that of a known earlier
        version of its template); "edited": another hash; "unknown": no
        hash recorded (an older dataset)
    """
    dst = Path(item["target"])
    if not dst.is_file():
        return "absent"
    digest = sha256(dst.read_bytes())
    if digest in {item["sha256"], *item["previous"]} - {None}:
        return "ours"
    return "edited" if item["sha256"] else "unknown"


def _migrate(ctx: RoastContext, item: dict, rule: str
             ) -> tuple[bool, Path | None]:
    """
    Apply the migration rule to a file without a recorded hash.

    Called when the file differs from the render. "keep": it is kept,
    with a warning; "replace": it is copied to `<state>/replaced/<path>`
    as the action writes it (which replaces it atomically; if no action
    writes, it stays as it is), with a warning naming the backup.

    Returns
    -------
    write : bool
        Whether the file is to be written
    backup : Path | None
        Where the action copies it first
    """
    dst = Path(item["target"])
    if rule == "replace":
        # (`Action.backup`: the copy and its warning)
        return True, ctx.layout.replaced(dst)
    lg.warning(f"{dst} differs from what brainspresso writes and has no "
               f"recorded hash (an older harvest, or edited): kept "
               f"(--if-exists different or overwrite replaces it)")
    return False, None


def _trimmed(item: dict) -> bool:
    """
    Tell a derived file, which follows `update` under `skip`.

    A participants, sessions or scans table of the raw dataset (trimmed
    to what is present, or restored whole under `--no-trim`), or a field
    map whose `IntendedFor` is trimmed or repaired.
    """
    return bool(item.get("table") or item["intended"])


def _actions(ctx: RoastContext, item: dict) -> Iterator[Action]:
    source, dst = item["source"], Path(item["target"])
    mode = IfExists.from_any(ctx.ifexists)
    if mode is IfExists.SKIP and not _trimmed(item) and dst.exists():
        # pure skip: left alone, never rendered (also a frozen file); a
        # skipped action makes it an output of the shard, so that
        # deleting it runs the shard again (`_record` ignores it)
        yield RoastMetadata(item, ifexists="skip", tag=ctx.tag)
        return
    # (`update`, and the trimmed files under `skip`: the edit test)
    update = mode is IfExists.UPDATE or mode is IfExists.SKIP
    ifexists = "different" if update else None
    if source is not None and not Path(source).exists():
        # a missing base freezes the file
        if dst.exists():
            lg.info(f"{item['key']}: no base ({source}): left as it is")
            return
        yield Fail(f"{item['key']}: its base {source} is missing "
                   f"(harvest again)", name=dst.name, tag=ctx.tag)
        return
    state = item["state"] if update else "ours"
    if state == "edited":
        if item["fmap"] and item["intended"]:
            action = RoastMetadata(item, ifexists=ifexists, tag=ctx.tag,
                                   keep=True)
            try:
                same = action.render() == dst.read_bytes()
            except (OSError, ValueError):
                same = False
            if not same:
                # (compared first: one that holds the merge is no edit
                # to warn about, e.g. a stale hash)
                lg.warning(f"{dst} was edited since brainspresso wrote it: "
                           f"kept, but its IntendedFor is taken from the "
                           f"pristine copy (yours is replaced; --if-exists "
                           f"different or overwrite replaces the whole "
                           f"file)")
            yield action
            return
        # compared first: a file that holds the render is adopted (a
        # stale hash is no edit), any other kept with the warning
        action = RoastMetadata(item, ifexists=ifexists, tag=ctx.tag)
        action.keep_edit = (
            f"{dst} was edited since brainspresso wrote it: kept "
            f"(--if-exists different or overwrite replaces it)")
        yield action
        return
    backup = None
    if state == "unknown":
        try:
            same = RoastMetadata(item).render() == dst.read_bytes()
        except (OSError, ValueError):
            same = False
        if not same:
            write, backup = _migrate(ctx, item, harvest.MIGRATION_RULE)
            if not write:
                return
    yield RoastMetadata(item, ifexists=ifexists, tag=ctx.tag, backup=backup)


def _orphan_actions(ctx: RoastContext, orphan: dict) -> Iterator[Action]:
    dst = Path(orphan["dst"])
    if not dst.exists():
        return
    if orphan["sha256"] and sha256(dst.read_bytes()) == orphan["sha256"]:
        yield Unlink(dst, tag=ctx.tag)
    else:
        lg.warning(f"{dst}: its fill-in template no longer applies, but "
                   f"it was edited: kept")


def roast_subject(ctx: RoastContext, label: str, items: list[dict]):
    """
    Roast the metadata of one subject (a shard).

    Parameters
    ----------
    ctx : RoastContext
    label : str
        Subject label
    items : list[dict]
        Its files (`_item`)

    Yields
    ------
    Action
    """
    for item in items:
        yield from _actions(ctx, item)


def _inputs(items: list[dict], label: str) -> list[str]:
    """Input signature of a subject: bases, templates, files on disk."""
    outputs = {os.path.normcase(i["target"]) for i in items}
    inputs = set()
    for item in items:
        for path in (item["source"], item["sibling"]):
            if path:
                inputs.add(path)
        if item["fill"]:
            inputs.update(item["fill"].paths)
    for dataset in {i["dataset"] for i in items}:
        # (free-text `IntendedFor` tags ignore what it lists)
        inputs.add(os.path.join(dataset, ".bidsignore"))
        folder = Path(dataset) / f"sub-{label}"
        for dirpath, dirnames, filenames in os.walk(folder):
            dirnames[:] = [d for d in dirnames if not d.endswith(".tmp")]
            for name in filenames:
                path = os.path.join(dirpath, name)
                if os.path.normcase(path) not in outputs:
                    inputs.add(path)
    return sorted(inputs)


def _config(items: list[dict]) -> dict:
    """
    Describe what else decides the outputs of a subject (`Shard.config`).

    `RENDER_VERSION`, and the options of each file (`repair`, `trim`,
    fill-in templates with their keys and the keys excluded from them,
    the unfetched files that count as present, the remapped paths); not
    its paths (inputs and outputs are signed as such), nor the hashes the
    roast records.
    """
    skipped = ("sha256", "fill", "state", "source", "target", "dataset",
               "sibling", "left", "table")
    out = []
    for item in items:
        fill = item["fill"]
        out.append({
            **{k: v for k, v in item.items() if k not in skipped},
            "fill": None if fill is None else {
                "layers": [[t.id, t.content] for t in fill.layers],
                "exclude": sorted(fill.exclude),
            },
        })
    return {"render": RENDER_VERSION, "items": out}


def roast_metadata(ctx: RoastContext, items: Iterable[dict] = (),
                   orphans: Iterable[dict] = ()):
    """
    Roaster of the metadata of a hosted dataset.

    Top-level files are roasted first (and fill-in sidecars whose
    templates no longer apply removed), then one shard per subject.

    Parameters
    ----------
    ctx : RoastContext
    items : list[dict]
        Files to roast (`_plan`)
    orphans : list[dict]
        Fill-in sidecars to remove (`_plan`)

    Yields
    ------
    Action | Shard
    """
    for orphan in orphans:
        if orphan.get("handled", True):
            yield from _orphan_actions(ctx, orphan)
    subjects: dict[str, list[dict]] = {}
    for item in items:
        label = item["subject"]
        if label is None:
            yield from _actions(ctx, item)
        else:
            subjects.setdefault(label, []).append(item)
    labels = [label for label in sorted(subjects)
              if ctx.selection.keeps_subject(label)]
    ctx.progress(total=len(labels), tag=TAG)
    for label in labels:
        group = subjects[label]
        yield Shard(roast_subject, label, group, tag=TAG, advance=TAG,
                    id=f"roast/{TAG}/sub-{label}", unit={"sub": label},
                    inputs=_inputs(group, label), config=_config(group))


def roast_hosted(
    layout: str | os.PathLike | Layout,
    *,
    fill: Iterable[FillTemplate] = (),
    site: str | None = None,
    selection: Selection | None = None,
    paths: Iterable[str] | None = None,
    subjects: Iterable[str] | None = None,
    present: Iterable[str] = (),
    existing_only: bool = False,
    json: str = "yes",
    repair: bool | None = None,
    tables: bool = True,
    ifexists: IfExists.Choice = "skip",
    jobs: int = 1,
    display: bool = True,
    info: dict | None = None,
) -> RoastReport:
    """
    Roast the metadata of a hosted dataset from its bases.

    See the module docstring.

    Parameters
    ----------
    layout : path | Layout
        The dataset (study or flat; a site of a sites layout)
    fill : list[FillTemplate]
        Fill-in templates of the dataset
    site : str | None
        Site that this dataset is (one dataset per site)
    selection : Selection, optional
        Subject and session filters
    paths : list[str], optional
        Only these files (relative to the dataset folder), and every
        file of their subjects and of `subjects` (a harvest)
    subjects : list[str], optional
        With `paths`: subjects whose files are roasted again (their
        images changed)
    present : list[str]
        Files the harvest selected but may not have fetched, relative to
        the raw dataset (where fill-in sidecars apply)
    existing_only : bool
        Only rewrite the files that exist (after a compost)
    json : {"yes", "no", "only"}
        Sidecar policy
    repair : bool | None
        Repair the hosted metadata (`--repair/--no-repair`; recorded per
        file); None: as each file was harvested
    tables : bool
        Roast the participants, sessions and scans tables (False: left
        as they are, `compost --no-trim`)
    ifexists : {"skip", "update", "different", "overwrite", "refresh", "error"}
        Default `IfExists` of the visible files: under "skip" an existing
        file is left alone, but for the trimmed tables and the field maps
        whose `IntendedFor` is trimmed, which follow "update"
    jobs : int
        Processes (subject shards)
    display : bool
        Progress table
    info : dict
        Extra fields of the roast run in the manifest

    Returns
    -------
    RoastReport
    """
    layout = resolve_layout(layout)
    every = manifest_files(layout)
    files = {k: v for k, v in every.items()
             if v.get("origin") in PLACED and v.get("source")}
    items, orphans = _plan(
        layout, files, fill=fill, site=site, paths=paths,
        subjects=subjects, present=present, existing_only=existing_only,
        every=every, repair=repair, selection=selection)
    if not tables:
        items = [i for i in items if not is_trimmable(i["rel"])]
    skip = IfExists.from_any(ifexists) is IfExists.SKIP
    for item in items:
        exists = Path(item["target"]).exists()
        frozen = item["source"] is not None and \
            not Path(item["source"]).exists()
        if skip and not _trimmed(item):
            # pure skip: an existing file is left alone (not even hashed)
            item["state"], item["kept"], item["left"] = None, None, exists
            continue
        item["state"] = _state(item)        # (before anything changes)
        # a file the shard leaves as it is (an edit, a file without a
        # hash, a frozen one) is no recorded output: whether it exists
        # is in the shard config, so that deleting it runs the shard
        item["kept"] = exists \
            if frozen or item["state"] in ("edited", "unknown") else None
    base = selection or Selection()
    ctx = RoastContext(
        layout=layout,
        selection=KeySelection(base=Selection(
            subs=base.subs, exclude_subs=base.exclude_subs,
            sessions=base.sessions, folders=OPTIONAL_FOLDERS,
        )),
        json=json, ifexists=ifexists, tag=TAG,
    )
    for orphan in orphans:
        # what this roast handles: in the selection, under the policy
        # (not under `skip`: a removal touches an existing file)
        orphan["handled"] = not skip and ctx.accepts("sidecar") and (
            orphan["subject"] is None
            or ctx.selection.keeps_subject(orphan["subject"]))
    roaster = functools.partial(roast_metadata, items=items,
                                orphans=orphans)
    roaster.__name__ = "roast_metadata"
    # (tables are trimmed as they are rendered: no `trim_originals`;
    # `_record` writes the hashes: no "converted" entries)
    report = run_roast(roaster, ctx, jobs=jobs, display=display,
                       info={"hosted": True, **(info or {})}, trim=False,
                       policy=RoastPolicy(track=False))
    _record(layout, items, orphans, report, ctx, repair)
    return report


def _record(layout, items, orphans, report, ctx, repair) -> None:
    """
    Record what a roast wrote: hashes, fill-in templates, removals.

    The one writer of the hashes of roasted files: that of a file which
    holds what this roast writes; never that of an edit kept, nor of
    the outputs a skipped shard replays.
    """
    written = {os.path.normcase(os.path.abspath(p)) for p in report.outputs}
    recorded = {}
    for item in items:
        dst = Path(item["target"])
        if item.get("left") or not dst.is_file() or \
                os.path.normcase(os.path.abspath(dst)) not in written:
            continue                # (left alone under `skip`: no hash)
        data = dst.read_bytes()
        # whatever its state was: ours iff it holds the render (an edit
        # replaced by `--if-exists overwrite` is ours; an edited field
        # map merged with the render, or a replayed output, is not)
        try:
            if RoastMetadata(item).render() != data:
                continue
        except (OSError, ValueError):
            continue
        entry = recorded[item["key"]] = {"sha256": sha256(data)}
        # (None removes a stale list)
        entry["fill"] = item["fill"].ids if item["fill"] else None
        if item["fill"] and item["source"] is None:
            entry["origin"] = "generated"
    if repair is not None and report.status != "failed":
        for item in items:
            if item["origin"] == "roasted" and (
                    item["subject"] is None
                    or ctx.selection.keeps_subject(item["subject"])):
                recorded.setdefault(item["key"], {})["repair"] = \
                    True if repair else None
    deleted = []
    for orphan in orphans:
        if not Path(orphan["dst"]).exists():
            deleted.append(orphan["key"])
        elif orphan.get("handled"):
            recorded[orphan["key"]] = {"fill": None}    # (a kept edit)
    if recorded or deleted:
        update_files(layout, recorded, deleted=deleted)
