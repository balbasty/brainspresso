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
under their remote, unremapped path (pristine copies), writes the files
that the listing generates (`InlineEntry`) to `<state>/generated/`, and
records every file in the manifest with its origin (`direct`,
`roasted`, `generated`) and, for roasted and generated files, its
`source`. It then runs `roast_hosted` on what it placed.

`roast_hosted` is the **metadata roast**: one action per roasted or
generated file (`bids.hosted.render`: `bids.repair` if the file was
harvested with `repair`, fill-in templates, `IntendedFor` of field maps
trimmed to the local images), written through the action engine
(`actions.File`: shared locks on the pristine copies, exclusive locks
and atomic writes on the outputs); participants, sessions and scans
tables go to `<state>/orig/` and are trimmed to what is on disk
(`trim_originals`), as in every roast. It runs after the images are
placed (repairs read siblings: `_phasediff`, `IntendedFor`), on its own
(`bx <ds> roast`: everything the manifest records, re-roasted from
`sourcedata/`), and after a compost of BIDS data (`existing_only`:
only the files still there, so that `IntendedFor` and tables are
trimmed again; compost never re-creates a file).

Rules (`docs/design/architecture.md`, §4):

* `--sidecars yes|no|only` is the placement policy (`bids.kinds.
  accepts`): at harvest, "no" does not fetch the JSON sidecars of data
  files, "only" does not fetch data files (images, recordings); in a
  roast, it drops the actions of the kinds it excludes (files on disk
  are left as they are).
* Selection: a harvest roasts what it selected; a roast takes `--subs`,
  `--exclude-subs`, `--sessions` (subject shards are pruned, top-level
  files always roasted).
* `--if-exists`: under "skip" (default) a roasted file is regenerated
  and replaces the existing one only if its content changed (its inputs
  are small; a new pristine copy or fill-in template must reach the
  dataset); "overwrite" rewrites, "error" refuses an existing file,
  "refresh" compares mtimes with the pristine copy.
* Edits: the manifest records the SHA-256 of every file a roast wrote;
  under "skip", a file that no longer has it (edited by hand) is kept,
  with a warning ("different" or "overwrite" replaces it).
* Re-runs: one shard per subject (`roast/hosted/sub-<label>`), whose
  input signature is its pristine copies, fill-in templates and the
  files of the subject on disk (images: `IntendedFor`), skipped under
  "skip" when unchanged.
* A pristine copy that is missing (composted with `--sourcedata`) is
  not an error if the roasted file exists (it is left as it is); else it
  is (harvest again).
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
from brainspresso.bids.harvest import (
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
from brainspresso.bids.layout import Layout, SitesLayout, resolve_layout
from brainspresso.bids.repair import loads, read_sibling, repair_sidecar
from brainspresso.bids.select import OPTIONAL_FOLDERS, Selection
from brainspresso.bids.tables import is_trimmable
from brainspresso.download import Downloader, DownloadManager
from brainspresso.roast.context import RoastContext
from brainspresso.roast.run import RoastReport, run_roast
from brainspresso.sources.base import write_content

lg = getLogger(__name__)

__all__ = ["RoastMetadata", "harvest_hosted", "hosted_files",
           "roast_hosted", "roast_metadata"]

#: Origins of the files that a metadata roast writes
PLACED = ("roasted", "generated")
#: Progress group (and task ids: `roast/hosted/sub-<label>`)
TAG = "hosted"


# ----------------------------------------------------------------------
#   Harvest
# ----------------------------------------------------------------------

def _rel(layout: Layout, path: Path) -> str:
    return Path(path).relative_to(layout.root).as_posix()


def _remote_fields(entry, rel: str) -> dict:
    """
    Remote path of an entry, where it differs from its local one.

    `remote_root` is the folder of the remote BIDS dataset that holds it
    (INDI: its site), recorded on every entry that has one, renamed or
    not: the paths that its sidecars and tables give are relative to it.
    `remote` is its path there, recorded where it differs from the local
    one. Nothing for a listing without remote roots (OpenNeuro: placed
    under its remote names).
    """
    meta = getattr(entry, "meta", None) or {}
    remote = meta.get("remote")
    root = str(meta.get("remote_root") or "").strip("/")
    if remote is None or not root:
        return {}
    if remote.startswith(root + "/"):
        remote = remote[len(root) + 1:]
    return {"remote_root": root,
            **({"remote": remote} if remote != rel else {})}


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
    repair: bool = False,
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
        may give the `remote` path (default: `entry.path`) and the
        `site` of an entry
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
        Trim tables and `IntendedFor` to what is on disk (recorded per
        file: a later roast keeps the choice)
    if_exists : {"skip", "overwrite", "different", "refresh", "error"}
        Downloads, and the metadata roast (see the module)
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
    # SHA-256 of the files the earlier roasts wrote (an edited file is
    # kept, see `_actions`)
    before = manifest_files(layout)
    known = {k: v["sha256"] for k, v in before.items()
             if isinstance(v.get("sha256"), str)}
    files: dict[str, dict] = {}
    placed: set[str] = set()
    downloads: dict[Path, Downloader] = {}
    raw = Path(layout.raw)
    present: list[str] = []
    nb_generated = 0

    def download(e, dst):
        if dst not in downloads:
            downloads[dst] = Downloader(
                e.url, dst, size=e.size, mtime=e.mtime,
                digests=e.digests or None, ifexists=if_exists,
                insecure=insecure,
                **({"chunk_size": chunk_size} if chunk_size else {}),
            )

    touched: set[str] = set()
    fetched_paths = {e.path for e in selected}
    remotes: dict[str, dict] = {}       # remote paths, where remapped
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
                              **({"site": meta["site"]}
                                 if meta.get("site") else {}),
                              **remotes.get(key, {})}
    for e in selected:
        local = layout.resolve(e.path)
        key = _rel(layout, local)
        meta = getattr(e, "meta", None) or {}
        extra = {"site": meta["site"]} if meta.get("site") else {}
        extra.update(remotes.get(key, {}))
        roasted = placement(e.path) == "roasted"
        if isinstance(e, InlineEntry):
            if roasted:
                base = layout.generated(local)
                nb_generated += write_content(base, e.content, "overwrite")
                files[key] = {"origin": "generated",
                              "source": _rel(layout, base),
                              **({"trim": False} if not trim else {}),
                              **extra}
                # earlier versions of a template, unedited: ours
                previous = set(getattr(e, "previous", ()) or ())
                if previous and local.is_file() and \
                        sha256(local.read_bytes()) in previous:
                    known[key] = sha256(local.read_bytes())
                placed.add(key)
            else:
                # placed directly: refreshed if unedited (#26)
                nb_generated += write_inline(
                    e, local, if_exists, before.get(key, {}).get("sha256"))
                files[key] = {"origin": "generated", "size": e.size,
                              **extra}
                if local.is_file() and local.read_bytes() == e.content:
                    files[key]["sha256"] = sha256(e.content)
                elif key in known:
                    files[key]["sha256"] = known[key]   # (an edit, kept)
            continue
        remote = {"url": e.url, "size": e.size,
                  **({"digests": e.digests} if e.digests else {})}
        if roasted:
            path = PurePosixPath(str(meta.get("remote") or e.path))
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"Invalid remote path: {path}")
            pristine = layout.source.joinpath(folder, *path.parts)
            download(e, pristine)
            files[_rel(layout, pristine)] = {**remote, "origin": "direct"}
            files[key] = {"origin": "roasted",
                          "source": _rel(layout, pristine),
                          "url": e.url,
                          **({"repair": True} if repair else {}),
                          **({"trim": False} if not trim else {}),
                          **extra}
            placed.add(key)
        else:
            download(e, local)
            files[key] = {**remote, "origin": "direct", **extra}
    for key in placed:
        if key in known:
            files[key]["sha256"] = known[key]
    if nb_generated:
        lg.info(f"Wrote {nb_generated} generated files")
    if downloads:
        DownloadManager(list(downloads.values()), ifexists=if_exists,
                        jobs=jobs, path="short").run()
    record_run(layout, "harvest", {
        **(provenance or {}),
        "filters": _filters(selection),
        "where": list(where or []),
        "sidecars": sidecars,
        "nb_files": len(selected),
    }, files=files)
    report = roast_hosted(
        layout, fill=fill, site=site, paths=placed, subjects=touched,
        present=present, json=sidecars, ifexists=if_exists,
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
    # subject: present for `IntendedFor`
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
        entry = files[raw_placed[rel]]
        source = root / entry["source"]
        try:
            meta = loads(source.read_text(encoding="utf-8",
                                          errors="replace"))
        except (OSError, ValueError):
            return None
        if not isinstance(meta, dict):
            return None
        # the keys of the roasted file: repairs may rename or add some
        return repair_sidecar(meta, PurePosixPath(rel).name) \
            if entry.get("repair") else meta

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
    if existing_only:
        items = [i for i in items if Path(i["dst"]).exists()
                 or (i["table"] and Path(i["target"]).exists())]
    return items, orphans


def _item(layout, key, target, dataset, rel, entry, plan, context) -> dict:
    root = Path(layout.root)
    files = context["files"]
    name = target.name
    trim = entry.get("trim", True)
    repair = bool(entry.get("repair", False))
    trimmable = is_trimmable(rel) and dataset == Path(layout.raw)
    table = trim and trimmable
    dst = layout.original(target) if table else target
    sibling = None
    if name.endswith("_phasediff.json"):        # (used if repaired)
        magnitude = name.replace("_phasediff.json", "_magnitude1.json")
        other = PurePosixPath(key).with_name(magnitude).as_posix()
        sibling = str(root / files[other]["source"]) if other in files \
            else str(target.with_name(magnitude))
    source = entry.get("source")
    fmap = name.endswith(".json") and target.parent.name == "fmap"
    intended = fmap and (repair or trim)
    previous = sorted(plan.previous) if plan else []
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
    return {
        "key": key,
        "origin": entry.get("origin", "roasted"),
        "source": str(root / source) if source else None,
        "dst": str(dst),
        "target": str(target),
        "dataset": str(dataset),
        "rel": rel,
        "repair": repair,
        "trim": trim,
        "intended": intended,
        "fmap": fmap,
        "sibling": sibling,
        "fill": plan,
        "subject": _subject(rel),
        "table": table,
        # a full table that a roast without trimming starts from when the
        # pristine copy is gone (it is kept: no row is ever lost)
        "orig": str(layout.original(target)) if trimmable and not table
        else None,
        "sha256": entry.get("sha256"),
        "previous": previous,
        "present": context["unfetched"].get((dataset, _subject(rel)), [])
        if intended else [],
        "relabel": relabel,
    }


class RoastMetadata(Action):
    """
    Write a roasted metadata file (`bids.hosted.render`).

    Its sources (pristine copy, sibling, fill-in templates) are read
    under shared locks, the output written under an exclusive lock, to a
    temporary file renamed into place (`actions.Action`).

    Parameters
    ----------
    item : dict
        What to roast (`roast.hosted._item`)
    ifexists : {"skip", "overwrite", "different", "refresh", "error"}
        Behaviour if the output exists (default: that of the roast)
    tag : str
        Progress group
    """

    def __init__(self, item: dict, *, ifexists=None, tag=None):
        self.item = item
        fill: FillPlan | None = item.get("fill")
        src = [p for p in (item["source"], item["sibling"]) if p]
        src += fill.paths if fill else []
        super().__init__(
            src=[p for p in src if Path(p).exists()], dst=item["dst"],
            action=self.write, mode="wb", input="file", ifexists=ifexists,
            tag=tag, compare=True,
        )

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
        return render(content, target.name, repair=item["repair"],
                      fill=item["fill"], intended=intended, sibling=sibling,
                      relabel=item.get("relabel"))

    def write(self, file) -> None:
        """Write the output (the actor of the action)."""
        file.write(self.render())


def _renders(item: dict, data: bytes) -> bool:
    """Whether `data` is what an item renders (False if it cannot)."""
    try:
        return RoastMetadata(item).render() == data
    except (OSError, ValueError) as e:
        lg.debug(f"{item['dst']}: cannot render ({e})")
        return False


def _edited(item: dict) -> bool:
    """
    Whether the output of an item was edited since brainspresso wrote it.

    Its SHA-256 is neither the one recorded in the manifest nor that of
    a known earlier version of its template, and it is not what this
    roast writes. Without a record (a dataset harvested before hashes
    were recorded), a file that is what an earlier harvest wrote (the
    pristine copy repaired and trimmed, without fill-in) is ours too.
    """
    dst = Path(item["dst"])
    if item["table"] or not dst.is_file():
        return False
    data = dst.read_bytes()
    if sha256(data) in {item["sha256"], *item["previous"]} - {None}:
        return False
    if _renders(item, data):
        return False
    if item["sha256"] is None:
        # what an older harvest may have written: no fill-in, paths not
        # remapped, or neither
        for older in ({"fill": None}, {"relabel": None},
                      {"fill": None, "relabel": None}):
            if _renders({**item, **older}, data):
                return False
    return True


def _in_place(item: dict) -> dict:
    """Make an item that trims `IntendedFor` of the field map on disk."""
    return {**item, "source": item["dst"], "repair": False, "fill": None,
            "sibling": None, "relabel": None}


def _actions(ctx: RoastContext, item: dict) -> Iterator[Action]:
    source, dst = item["source"], Path(item["dst"])
    skip = IfExists.from_any(ctx.ifexists) is IfExists.SKIP
    ifexists = "different" if skip else None
    missing = source is not None and not Path(source).exists()
    if missing and item["orig"] and Path(item["orig"]).exists():
        # not trimmed, no pristine copy: the full table in `orig/`
        item = {**item, "source": item["orig"], "repair": False,
                "relabel": None}
        missing = False
    if missing and not dst.exists():
        if item["table"] and Path(item["target"]).exists():
            # (an older harvest: neither `orig/` nor a pristine copy)
            lg.info(f"{item['key']}: no pristine copy, left as it is")
            return
        yield Fail(f"{item['key']}: its pristine copy {source} is "
                   f"missing (harvest again)", name=dst.name, tag=ctx.tag)
        return
    edited = item.get("edited")
    if edited is None:
        edited = skip and not missing and _edited(item)
    if missing or edited:
        if item["intended"] and item["trim"]:
            # trim `IntendedFor` of the file on disk, keep the rest
            if edited:
                lg.warning(f"{dst} was edited since brainspresso wrote "
                           f"it: kept, its IntendedFor trimmed "
                           f"(--if-exists different or overwrite "
                           f"replaces it)")
            yield RoastMetadata(_in_place(item), ifexists=ifexists,
                                tag=ctx.tag)
        elif edited:
            lg.warning(f"{dst} was edited since brainspresso wrote it: "
                       f"kept (--if-exists different or overwrite "
                       f"replaces it)")
        else:
            lg.debug(f"{item['key']}: no pristine copy ({source}), "
                     f"left as it is")
        return
    yield RoastMetadata(item, ifexists=ifexists, tag=ctx.tag)


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
    """
    Input signature of a subject: sources, templates, files on disk.

    The outputs are not inputs, but for those whose pristine copy is
    missing (left as they are): their deletion must run the shard again.
    """
    outputs = {os.path.normcase(i[k]) for i in items for k in ("dst",
                                                                "target")
               if i["source"] is None or os.path.exists(i["source"])}
    inputs = set()
    for item in items:
        for path in (item["source"], item["sibling"]):
            if path:
                inputs.add(path)
        if item["fill"]:
            inputs.update(item["fill"].paths)
    for dataset in {i["dataset"] for i in items}:
        folder = Path(dataset) / f"sub-{label}"
        for dirpath, dirnames, filenames in os.walk(folder):
            dirnames[:] = [d for d in dirnames if not d.endswith(".tmp")]
            for name in filenames:
                path = os.path.join(dirpath, name)
                if os.path.normcase(path) not in outputs:
                    inputs.add(path)
    return sorted(inputs)


def _config(items: list[dict]) -> list:
    """
    Describe what else decides the outputs of a subject (`Shard.config`).

    The options of each file (`repair`, `trim`, fill-in templates with
    their keys and the keys excluded from them, the unfetched files that
    count as present, the remapped paths); not its paths (inputs and
    outputs are signed as such), nor the hashes the roast records.
    """
    skipped = ("sha256", "fill", "edited", "source", "dst", "target",
               "dataset", "sibling", "orig")
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
    return out


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
    ifexists: IfExists.Choice = "skip",
    jobs: int = 1,
    display: bool = True,
    info: dict | None = None,
) -> RoastReport:
    """
    Roast the metadata of a hosted dataset from `sourcedata/`.

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
    ifexists : {"skip", "overwrite", "different", "refresh", "error"}
        Default `IfExists`
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
        every=every)
    if repair is not None:
        # (in the shard config: a change roasts the files again)
        for item in items:
            if item["origin"] == "roasted":
                item["repair"] = bool(repair)
                item["intended"] = item["fmap"] and (item["repair"]
                                                     or item["trim"])
    skip = IfExists.from_any(ifexists) is IfExists.SKIP
    for item in items:
        if item["source"] is not None and \
                not Path(item["source"]).exists() and \
                not (item["orig"] and Path(item["orig"]).exists()):
            # (no pristine copy: decided here, before the file changes)
            item["edited"] = skip and _edited(item)
    unfetched = sorted(k for k, v in every.items()
                       if v.get("fetched") is False)
    untrimmed = sorted(
        k for k, v in every.items() if v.get("trim") is False and (
            is_trimmable(k) or k.endswith("/participants.tsv")))
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
        orphan["handled"] = ctx.accepts("sidecar") and (
            orphan["subject"] is None
            or ctx.selection.keeps_subject(orphan["subject"]))
    roaster = functools.partial(roast_metadata, items=items,
                                orphans=orphans)
    roaster.__name__ = "roast_metadata"
    report = run_roast(roaster, ctx, jobs=jobs, display=display,
                       info={"hosted": True, **(info or {})},
                       present=unfetched, untrimmed=untrimmed)
    _record(layout, items, orphans, report)
    if repair is not None:
        flags = {
            i["key"]: {"repair": True if repair else None} for i in items
            if i["origin"] == "roasted" and (
                i["subject"] is None
                or ctx.selection.keeps_subject(i["subject"]))
        }
        if flags:
            update_files(layout, flags)
    return report


def _record(layout, items, orphans, report) -> None:
    """
    Record what a roast wrote: hashes, fill-in templates, removals.

    A hash is only recorded for a file that holds what this roast writes
    (or, without a pristine copy, an unedited field map trimmed in
    place): never for an edit kept, nor for the outputs of a skipped
    shard that are no longer what it writes.
    """
    written = {os.path.normcase(os.path.abspath(p)) for p in report.outputs}

    def done(path):
        return os.path.normcase(os.path.abspath(path)) in written

    recorded = {}
    for item in items:
        dst, target = Path(item["dst"]), Path(item["target"])
        if not done(dst):
            continue
        if item["table"]:
            # the visible table, trimmed from the pristine one
            if target.is_file():
                recorded[item["key"]] = {
                    "sha256": sha256(target.read_bytes())}
            continue
        if not dst.is_file():
            continue
        data = dst.read_bytes()
        missing = item["source"] is not None and \
            not Path(item["source"]).exists() and \
            not (item["orig"] and Path(item["orig"]).exists())
        if missing:
            # trimmed in place: its hash if unedited; its fill-in list
            # untouched (nothing merged)
            if not item.get("edited"):
                recorded[item["key"]] = {"sha256": sha256(data)}
            continue
        source = item["source"]
        if not _renders(item, data):
            # (a no-trim table without pristine copy: from `orig/`)
            if not (item["orig"] and source and not Path(source).exists()
                    and _renders({**item, "source": item["orig"],
                                  "repair": False, "relabel": None}, data)):
                continue
        entry = recorded[item["key"]] = {"sha256": sha256(data)}
        # (None removes a stale list)
        entry["fill"] = item["fill"].ids if item["fill"] else None
        if item["fill"] and item["source"] is None:
            entry["origin"] = "generated"
    deleted = []
    for orphan in orphans:
        if not Path(orphan["dst"]).exists():
            deleted.append(orphan["key"])
        elif orphan.get("handled"):
            recorded[orphan["key"]] = {"fill": None}    # (a kept edit)
    if recorded or deleted:
        update_files(layout, recorded, deleted=deleted)
