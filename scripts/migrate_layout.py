#!/usr/bin/env python
"""
Migrate datasets roasted with the old `rawdata/` layout to `study`/`flat`.

Old layout (written by every bidsifier before the layout refactor)::

    <DS>/
      dataset_description.json  README  participants.{tsv,json}
      sessions.json  phenotype/              raw-dataset files at the root
      rawdata/   sub-*/  [phenotype/]  [dwi.bval dwi.bvec]
                 [dataset_description.json]  (OASIS-III duplicate)
      sourcedata/
      derivatives/<pipeline>/

Usage::

    python scripts/migrate_layout.py PATH [PATH ...] --layout {study,flat}
        [--apply] [--recursive]

Without `--apply` the planned moves are printed and nothing is changed.

File placement
--------------
study
    Contents of `rawdata/` and the raw-dataset files of the root
    (`dataset_description.json`, `README*`, `CHANGES`, `LICENSE*`,
    `participants.*`, `sessions.*`, other top-level `*.json`/`*.tsv`,
    `phenotype/`, `.bidsignore`) move into `<DS>/rawbids/`. A study
    `dataset_description.json` is written at `<DS>/`.
flat
    Contents of `rawdata/` move up to `<DS>/`; root files stay.

In both layouts `sourcedata/`, `derivatives/` and `.brainspresso/` stay,
the emptied `rawdata/` is removed, derivative descriptions get the raw
dataset as first `SourceDatasets` entry (`../../rawbids/` or `../../`,
stale `../../rawdata/` entries are dropped), and the layout is recorded
in `<DS>/.brainspresso/manifest.json`. Moves are renames (same
filesystem, no copies) and are logged to `<DS>/.brainspresso/migrate.log`.

Duplicate `dataset_description.json` (in `rawdata/` and at the root):
if byte-identical, the `rawdata/` copy is deleted; otherwise the root
one is kept (it is the one that old bidsifiers wrote from the template
for the whole dataset) and the `rawdata/` copy is saved as
`.brainspresso/orig/rawdata_dataset_description.json`, with a warning.

Refusals (non-zero exit, nothing changed in that dataset): no
`rawdata/`; `rawbids/` or `sub-*` at the root, or a layout already
recorded in the manifest (already migrated / mixed); any move target
that already exists (other than the duplicate description above); a
root entry that is neither a known raw-dataset file nor a folder that
stays (the script does not guess where it belongs).

Library use: when `brainspresso.bids.layout` and `brainspresso.bids.harvest`
are importable and provide `make_layout`/`record_run`, they write the
descriptions and the manifest; otherwise a stdlib implementation of the
same schema (manifest schema 1) is used. The one used is printed.
"""
from __future__ import annotations

import argparse
import datetime
import filecmp
import fnmatch
import json
import posixpath
import sys
from pathlib import Path

RAW_OLD = "rawdata"
RAW_NEW = "rawbids"
STATE = ".brainspresso"
DESCRIPTION = "dataset_description.json"
DUP_SAVE = "orig/rawdata_dataset_description.json"
STAYING = {RAW_OLD, "sourcedata", "derivatives", STATE}
# Root entries that belong to the raw dataset (study: moved to rawbids/)
RAW_FILE_PATTERNS = (
    DESCRIPTION, "README*", "CHANGES*", "LICENSE*", "participants.*",
    "sessions.*", "*.json", "*.tsv", ".bidsignore",
)
RAW_DIRS = ("phenotype",)
BIDS_VERSION = "1.11.2"
STUDY_FIELDS = (
    "License", "Authors", "Acknowledgements", "HowToAcknowledge",
    "Funding", "EthicsApprovals", "ReferencesAndLinks", "DatasetDOI",
)

try:  # pragma: no cover - depends on the installed brainspresso
    from brainspresso.bids.harvest import record_run as _record_run
    from brainspresso.bids.layout import make_layout as _make_layout
    BACKEND = "brainspresso"
except ImportError:  # pragma: no cover
    _record_run = _make_layout = None
    BACKEND = "stdlib"


class Refusal(Exception):
    """Dataset cannot be migrated safely; nothing was changed."""


def _is_raw_root_entry(path: Path) -> bool:
    if path.is_dir():
        return path.name in RAW_DIRS
    return any(fnmatch.fnmatch(path.name, p) for p in RAW_FILE_PATTERNS)


def plan(ds: Path, layout: str) -> tuple[list, list, list]:
    """
    Plan the migration of one dataset, checking every conflict.

    Parameters
    ----------
    ds : Path
        Dataset folder (old layout).
    layout : {"study", "flat"}
        Target layout.

    Returns
    -------
    moves : list[tuple[Path, Path]]
        Renames (source, destination), in order.
    drops : list[Path]
        Identical duplicates to delete.
    warnings : list[str]
        Messages to show.

    Raises
    ------
    Refusal
        If the dataset is not in the old layout or a conflict exists.
    """
    old = ds / RAW_OLD
    if not old.is_dir():
        raise Refusal(f"{ds}: no {RAW_OLD}/ folder (not an old layout)")
    if (ds / RAW_NEW).exists():
        raise Refusal(f"{ds}: {RAW_NEW}/ exists (already migrated or "
                      f"mixed layout)")
    subs = sorted(p.name for p in ds.glob("sub-*"))
    if subs:
        raise Refusal(f"{ds}: {subs[0]} at the root (already migrated or "
                      f"mixed layout)")
    manifest = _read_json(ds / STATE / "manifest.json")
    if manifest and manifest.get("layout"):
        raise Refusal(f"{ds}: layout {manifest['layout']!r} already "
                      f"recorded in {STATE}/manifest.json")

    root_raw, unknown = [], []
    for p in sorted(ds.iterdir()):
        if p.name in STAYING:
            continue
        if _is_raw_root_entry(p):
            root_raw.append(p)
        elif not p.name.startswith("."):
            unknown.append(p.name)
    if unknown:
        raise Refusal(f"{ds}: unexpected entries at the root, move them "
                      f"by hand first: {unknown}")

    dest = ds / RAW_NEW if layout == "study" else ds
    moves, drops, warnings = [], [], []
    targets = {}
    if layout == "study":
        for p in root_raw:
            moves.append((p, dest / p.name))
            targets[p.name] = p
    for p in sorted(old.iterdir()):
        target = dest / p.name
        twin = targets.get(p.name) or (
            target if layout == "flat" and target.exists() else None)
        if twin is None:
            moves.append((p, target))
            continue
        if p.name == DESCRIPTION and p.is_file() and twin.is_file():
            if filecmp.cmp(p, twin, shallow=False):
                drops.append(p)
            else:
                save = ds / STATE / DUP_SAVE
                if save.exists():
                    raise Refusal(f"{ds}: {save} already exists")
                moves.append((p, save))
                warnings.append(
                    f"{ds}: {RAW_OLD}/{DESCRIPTION} differs from the root "
                    f"one; keeping the root one, saved the other as "
                    f"{STATE}/{DUP_SAVE}")
            continue
        raise Refusal(f"{ds}: {RAW_OLD}/{p.name} conflicts with "
                      f"{twin.relative_to(ds)}")
    return moves, drops, warnings


def _read_json(path: Path) -> dict | None:
    try:
        out = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return out if isinstance(out, dict) else None


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n")


def _strip_rawdata_urls(ds: Path) -> None:
    """Drop stale `SourceDatasets` URLs to `../../rawdata/`."""
    for desc_path in sorted((ds / "derivatives").glob(f"*/{DESCRIPTION}")):
        desc = _read_json(desc_path)
        if not desc or not isinstance(desc.get("SourceDatasets"), list):
            continue
        keep = [s for s in desc["SourceDatasets"] if not (
            isinstance(s, dict) and isinstance(s.get("URL"), str)
            and posixpath.normpath(s["URL"]) == "../../" + RAW_OLD)]
        if keep != desc["SourceDatasets"]:
            desc["SourceDatasets"] = keep
            _write_json(desc_path, desc)


def _finalize_stdlib(ds: Path, layout: str, info: dict) -> None:
    raw = ds / RAW_NEW if layout == "study" else ds
    url = "../../rawbids/" if layout == "study" else "../../"
    if layout == "study":
        rawdesc = _read_json(raw / DESCRIPTION) or {}
        study = {"Name": rawdesc.get("Name") or ds.name,
                 "BIDSVersion": BIDS_VERSION, "DatasetType": "study"}
        study.update({k: rawdesc[k] for k in STUDY_FIELDS if k in rawdesc})
        _write_json(ds / DESCRIPTION, study)
    for desc_path in sorted((ds / "derivatives").glob(f"*/{DESCRIPTION}")):
        desc = _read_json(desc_path)
        if desc is None:
            continue
        desc.setdefault("DatasetType", "derivative")
        sources = [s for s in desc.get("SourceDatasets") or []
                   if not (isinstance(s, dict) and s.get("URL") == url)]
        desc["SourceDatasets"] = [{"URL": url}] + sources
        _write_json(desc_path, desc)
    path = ds / STATE / "manifest.json"
    manifest = _read_json(path) or {}
    manifest["schema"] = 1
    manifest["layout"] = layout
    manifest.setdefault("runs", []).append({
        "stage": "migrate",
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        **info,
    })
    manifest.setdefault("files", {})
    _write_json(path, manifest)


def _finalize(ds: Path, layout: str, info: dict) -> None:
    _strip_rawdata_urls(ds)
    if BACKEND == "stdlib":
        return _finalize_stdlib(ds, layout, info)
    lay = _make_layout(layout, ds)
    lay.write_descriptions()
    _record_run(lay, "migrate", info)


def migrate(ds: Path, layout: str, apply: bool = False,
            out=sys.stdout) -> list:
    """
    Migrate one dataset (dry run unless `apply`).

    Parameters
    ----------
    ds : Path
        Dataset folder.
    layout : {"study", "flat"}
        Target layout.
    apply : bool
        Perform the moves (default: only print them).
    out : file
        Where to print the plan.

    Returns
    -------
    moves : list[tuple[Path, Path]]
        Planned (or performed) moves.

    Raises
    ------
    Refusal
        Nothing was changed.
    """
    ds = Path(ds).absolute()
    moves, drops, warnings = plan(ds, layout)
    tag = "" if apply else "[dry run] "
    for w in warnings:
        print(f"WARNING: {w}", file=sys.stderr)
    for src, dst in moves:
        print(f"{tag}move {src.relative_to(ds)} -> {dst.relative_to(ds)}",
              file=out)
    for p in drops:
        print(f"{tag}delete {p.relative_to(ds)} (identical duplicate)",
              file=out)
    print(f"{tag}remove {RAW_OLD}/, write descriptions and manifest "
          f"(layout {layout!r})", file=out)
    if not apply:
        return moves
    (ds / STATE).mkdir(exist_ok=True)
    with open(ds / STATE / "migrate.log", "a") as log:
        stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        log.write(f"# {stamp} migrate to {layout} (backend {BACKEND})\n")
        for w in warnings:
            log.write(f"WARNING {w}\n")
        for src, dst in moves:
            dst.parent.mkdir(parents=True, exist_ok=True)
            src.replace(dst)
            log.write(f"move {src.relative_to(ds)} -> "
                      f"{dst.relative_to(ds)}\n")
        for p in drops:
            p.unlink()
            log.write(f"delete {p.relative_to(ds)} (identical)\n")
        (ds / RAW_OLD).rmdir()
        log.write(f"rmdir {RAW_OLD}\n")
    _finalize(ds, layout, {"from": "rawdata", "moves": len(moves)})
    return moves


def _datasets(paths, recursive: bool) -> list[Path]:
    out = []
    for p in map(Path, paths):
        if recursive:
            out += sorted(c for c in p.iterdir()
                          if c.is_dir() and (c / RAW_OLD).is_dir())
        else:
            out.append(p)
    return out


def main(argv=None) -> int:
    """Command-line entry point (returns the exit code)."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("paths", nargs="+", metavar="PATH")
    parser.add_argument("--layout", required=True, choices=["study", "flat"])
    parser.add_argument("--apply", action="store_true",
                        help="Perform the moves (default: dry run)")
    parser.add_argument("--recursive", action="store_true",
                        help="PATH holds datasets (subfolders with rawdata/)")
    args = parser.parse_args(argv)
    print(f"Using {BACKEND} backend for descriptions and manifest")
    datasets = _datasets(args.paths, args.recursive)
    if not datasets:
        print("ERROR: no dataset found", file=sys.stderr)
        return 1
    failed = 0
    for ds in datasets:
        try:
            migrate(ds, args.layout, args.apply)
        except Refusal as e:
            print(f"REFUSED: {e}", file=sys.stderr)
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
