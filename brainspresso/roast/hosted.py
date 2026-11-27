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
    for e in chosen:
        local = layout.resolve(e.path)
        dataset, rel = layout.relative(local)
        if Path(dataset) == raw:
            present.append(rel)
        if _subject(rel):
            touched.add(_subject(rel))
        if e.path not in fetched_paths and is_data(e.path):
            # selected, not fetched (`--sidecars only`): it counts as
            # present when tables and `IntendedFor` are trimmed
            key = _rel(layout, local)
            if not local.exists():          # (fetched by an earlier run)
                meta = getattr(e, "meta", None) or {}
                files[key] = {"url": e.url, "size": e.size,
                              "origin": "direct", "fetched": False,
                              **({"site": meta["site"]}
                                 if meta.get("site") else {})}
    for e in selected:
        local = layout.resolve(e.path)
        key = _rel(layout, local)
        meta = getattr(e, "meta", None) or {}
        extra = {"site": meta["site"]} if meta.get("site") else {}
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
                nb_generated += write_inline(e, local, if_exists)
                files[key] = {"origin": "generated", "size": e.size,
                              **extra}
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
) -> list[dict]:
    """Items of a metadata roast (picklable dictionaries)."""
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

    items = []
    for key in sorted(files):
        entry = files[key]
        dataset, rel = located[key]
        target = root / key
        plan = plans.pop(rel, None) if dataset == raw else None
        items.append(_item(layout, key, target, dataset, rel, entry,
                           files, plan, unfetched))
    for rel, plan in sorted(plans.items()):
        # a new fill-in sidecar: its record (`sha256`) if it has one
        target = raw / rel
        key = _rel(layout, target)
        entry = {**every.get(key, {}), "origin": "generated"}
        entry.pop("source", None)
        items.append(_item(layout, key, target, raw, rel, entry, files,
                           plan, unfetched))

    if paths is not None:
        # a harvest: what it placed, and every file of the subjects it
        # touched (new images re-trim `IntendedFor` and scans tables)
        paths = set(paths)
        touched = {_subject(located[k][1]) for k in paths if k in located}
        touched |= set(subjects or ())
        items = [i for i in items if i["key"] in paths or (
            i["subject"] in touched) or (
            i["source"] is None and i["subject"] is None)]
    if existing_only:
        items = [i for i in items if Path(i["dst"]).exists()]
    return items


def _item(layout, key, target, dataset, rel, entry, files, plan,
          unfetched) -> dict:
    root = Path(layout.root)
    name = target.name
    trim = entry.get("trim", True)
    repair = bool(entry.get("repair", False))
    table = trim and is_trimmable(rel) and dataset == Path(layout.raw)
    dst = layout.original(target) if table else target
    sibling = None
    if repair and name.endswith("_phasediff.json"):
        magnitude = name.replace("_phasediff.json", "_magnitude1.json")
        other = PurePosixPath(key).with_name(magnitude).as_posix()
        sibling = str(root / files[other]["source"]) if other in files \
            else str(target.with_name(magnitude))
    source = entry.get("source")
    intended = name.endswith(".json") and target.parent.name == "fmap" \
        and (repair or trim)
    previous = sorted(plan.previous) if plan else []
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
        "sibling": sibling,
        "fill": plan,
        "subject": _subject(rel),
        "table": table,
        "sha256": entry.get("sha256"),
        "previous": previous,
        "present": unfetched.get((dataset, _subject(rel)), [])
        if intended else [],
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
                      fill=item["fill"], intended=intended, sibling=sibling)

    def write(self, file) -> None:
        """Write the output (the actor of the action)."""
        file.write(self.render())


def _edited(item: dict) -> bool:
    """
    Whether the output of an item was edited since brainspresso wrote it.

    Its SHA-256 is neither the one recorded in the manifest nor that of
    a known earlier version of its template. Without a record (a dataset
    harvested before hashes were recorded), a file that equals what the
    roast writes is ours (its hash is then recorded); any other is an
    edit.
    """
    dst = Path(item["dst"])
    if item["table"] or not dst.is_file():
        return False
    data = dst.read_bytes()
    if sha256(data) in {item["sha256"], *item["previous"]} - {None}:
        return False
    try:
        return RoastMetadata(item).render() != data
    except (OSError, ValueError) as e:
        lg.debug(f"{dst}: cannot render ({e})")
        return True


def _actions(ctx: RoastContext, item: dict) -> Iterator[Action]:
    source, dst = item["source"], Path(item["dst"])
    skip = IfExists.from_any(ctx.ifexists) is IfExists.SKIP
    if source is not None and not Path(source).exists():
        if dst.exists():
            if item["intended"] and item["trim"]:
                # no pristine copy: trim the field map on disk itself
                yield RoastMetadata({**item, "source": str(dst),
                                     "repair": False, "fill": None,
                                     "sibling": None},
                                    ifexists="different" if skip else None,
                                    tag=ctx.tag)
                return
            lg.debug(f"{item['key']}: no pristine copy ({source}), "
                     f"left as it is")
            return
        yield Fail(f"{item['key']}: its pristine copy {source} is "
                   f"missing (harvest again)", name=dst.name, tag=ctx.tag)
        return
    if skip and _edited(item):
        lg.warning(f"{dst} was edited since brainspresso wrote it: kept "
                   f"(--if-exists different or overwrite replaces it)")
        return
    yield RoastMetadata(item, ifexists="different" if skip else None,
                        tag=ctx.tag)


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


def roast_metadata(ctx: RoastContext, items: Iterable[dict] = ()):
    """
    Roaster of the metadata of a hosted dataset.

    Top-level files are roasted first, then one shard per subject.

    Parameters
    ----------
    ctx : RoastContext
    items : list[dict]
        Files to roast (`_plan`)

    Yields
    ------
    Action | Shard
    """
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
                    inputs=_inputs(group, label))


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
    items = _plan(layout, files, fill=fill, site=site, paths=paths,
                  subjects=subjects, present=present,
                  existing_only=existing_only, every=every)
    unfetched = sorted(k for k, v in every.items()
                       if v.get("fetched") is False)
    base = selection or Selection()
    ctx = RoastContext(
        layout=layout,
        selection=KeySelection(base=Selection(
            subs=base.subs, exclude_subs=base.exclude_subs,
            sessions=base.sessions, folders=OPTIONAL_FOLDERS,
        )),
        json=json, ifexists=ifexists, tag=TAG,
    )
    roaster = functools.partial(roast_metadata, items=items)
    roaster.__name__ = "roast_metadata"
    report = run_roast(roaster, ctx, jobs=jobs, display=display,
                       info={"hosted": True, **(info or {})},
                       present=unfetched)
    # SHA-256 of what this roast wrote (or found identical), and the
    # fill-in templates merged into it
    written = {os.path.normcase(os.path.abspath(p)) for p in report.outputs}
    recorded = {}
    for item in items:
        dst = Path(item["dst"])
        if os.path.normcase(os.path.abspath(dst)) not in written:
            continue
        entry = recorded[item["key"]] = {}
        if not item["table"] and dst.is_file():
            entry["sha256"] = sha256(dst.read_bytes())
        # (None removes a stale list)
        entry["fill"] = item["fill"].ids if item["fill"] else None
        if item["fill"] and item["source"] is None:
            entry["origin"] = "generated"
    if recorded:
        update_files(layout, recorded)
    return report
