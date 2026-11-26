#!/usr/bin/env python
"""Regenerate dataset sidecar templates from protocol printouts.

Maintainer tool: requires `protocol2bids` (``pip install .[maint]``).
It is never needed at runtime.

Usage
-----
    python scripts/make_sidecars.py DATASET [--check] [--only PATTERN]

DATASET is one of ABIDE1, ABIDE2, CoRR
(see `printouts/<DATASET>/sidecars.toml`).

For each entry of the conversion table, the printout is converted with
protocol2bids, `defaults` and `assigns` are applied, and the sidecar is
written to `brainspresso/datasets/<DS>/templates/<output>`. Provenance is
written to `printouts/<DATASET>/sidecars.lock.json` and a diff report to
`printouts/<DATASET>/report.md`.

With `--check`, nothing is written and the exit code is non-zero if
regenerating would change any sidecar or the lock file.
"""
import argparse
import fnmatch
import hashlib
import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PRINTOUTS = ROOT / 'printouts'
TEMPLATES = {
    'ABIDE1': 'brainspresso/datasets/ABIDE/I/templates',
    'ABIDE2': 'brainspresso/datasets/ABIDE/II/templates',
    'CoRR': 'brainspresso/datasets/CoRR/templates',
}
# template JSON files that are not acquisition sidecars
NON_SIDECARS = ('dataset_description.json', 'participants.json')
ENTRY_KEYS = {'output', 'printout', 'select', 'nii', 'defaults', 'assigns',
              'manual', 'note'}


class TableError(ValueError):
    pass


# ---------------------------------------------------------------------------
# table
# ---------------------------------------------------------------------------

def template_dir(dataset, root=ROOT):
    return Path(root) / TEMPLATES[dataset]


def template_sidecars(dataset, root=ROOT):
    """List acquisition sidecars currently in a dataset's templates."""
    tpl = template_dir(dataset, root)
    return sorted(
        p.relative_to(tpl).as_posix() for p in tpl.rglob('*.json')
        if p.name not in NON_SIDECARS and 'phenotype' not in p.parts
    )


def load_table(dataset, root=ROOT):
    """Load and validate `printouts/<dataset>/sidecars.toml`."""
    path = Path(root) / 'printouts' / dataset / 'sidecars.toml'
    with open(path, 'rb') as f:
        table = tomllib.load(f)
    entries = table.get('sidecar', [])
    seen = set()
    for e in entries:
        unknown = set(e) - ENTRY_KEYS
        if unknown:
            raise TableError(f'{path}: unknown keys {sorted(unknown)} in {e}')
        if 'output' not in e:
            raise TableError(f'{path}: entry without output: {e}')
        if e['output'] in seen:
            raise TableError(f'{path}: duplicate output {e["output"]}')
        seen.add(e['output'])
        if e.get('manual'):
            if 'printout' in e:
                raise TableError(f'{path}: manual entry with printout: {e}')
            continue
        if 'printout' not in e:
            raise TableError(f'{path}: {e["output"]}: no printout '
                             '(use manual = true)')
        for key in ('printout', 'nii'):
            if key in e and not (path.parent / e[key]).is_file():
                raise TableError(f'{path}: {e["output"]}: missing {key} '
                                 f'{e[key]}')
        for key in ('defaults', 'assigns'):
            if not isinstance(e.get(key, {}), dict):
                raise TableError(f'{path}: {e["output"]}: {key} must be a '
                                 'table')
    return entries


def uncovered(dataset, entries, root=ROOT):
    """Template sidecars not listed in the table."""
    listed = {e['output'] for e in entries}
    return [p for p in template_sidecars(dataset, root) if p not in listed]


# ---------------------------------------------------------------------------
# protocol2bids
# ---------------------------------------------------------------------------

def protocol2bids_version():
    """Version and commit of the installed protocol2bids."""
    import importlib.metadata as md
    try:
        dist = md.distribution('protocol2bids')
    except md.PackageNotFoundError:
        return {'version': None, 'commit': None}
    commit = None
    try:
        direct = json.loads(dist.read_text('direct_url.json') or '{}')
        commit = direct.get('vcs_info', {}).get('commit_id')
    except Exception:
        pass
    return {'version': dist.version, 'commit': commit}


def convert(printout, select=None, nii=None):
    """Convert one protocol of a printout to a BIDS sidecar (dict)."""
    from protocol2bids import protocol_to_bids, read_protocols
    prots = read_protocols(printout)
    if select is not None:
        sel = str(select)
        if sel.isdigit():
            prots = [prots[int(sel)]]
        else:
            prots = [p for p in prots if fnmatch.fnmatch(p.name, sel)]
    if len(prots) != 1:
        raise TableError(f'{printout}: selector {select!r} matches '
                         f'{len(prots)} protocols')
    return protocol_to_bids(prots[0], nii=nii, extra=False)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def dumps(obj):
    text = json.dumps(obj, indent=4, ensure_ascii=False, default=_jsonable)
    return text + '\n'


def _jsonable(x):
    if hasattr(x, 'tolist'):
        return x.tolist()
    if hasattr(x, 'item'):
        return x.item()
    return str(x)


def diff(old, new):
    """Return (added, removed, changed) between two sidecars."""
    added = {k: new[k] for k in new if k not in old}
    removed = {k: old[k] for k in old if k not in new}
    changed = {k: (old[k], new[k]) for k in new
               if k in old and old[k] != new[k]}
    return added, removed, changed


def committed_text(path, root=ROOT):
    """Content of `path` at git HEAD (None if unavailable)."""
    import subprocess
    try:
        rel = Path(path).resolve().relative_to(Path(root).resolve())
        res = subprocess.run(
            ['git', 'show', f'HEAD:{rel.as_posix()}'], cwd=root,
            capture_output=True, text=True, check=False)
    except (OSError, ValueError):
        return None
    return res.stdout if res.returncode == 0 else None


def _fmt(v):
    return json.dumps(v, ensure_ascii=False, default=_jsonable)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def run(dataset, check=False, only=None, root=ROOT, out=sys.stdout):
    root = Path(root)
    pdir = root / 'printouts' / dataset
    tpl = template_dir(dataset, root)
    entries = load_table(dataset, root)
    missing = uncovered(dataset, entries, root)
    if missing:
        raise TableError(f'{dataset}: template sidecars not in table: '
                         f'{missing}')

    lock_path = pdir / 'sidecars.lock.json'
    old_lock = (json.loads(lock_path.read_text())
                if lock_path.exists() else {})
    lock = {'protocol2bids': protocol2bids_version(), 'sidecars': {}}
    report = []
    failures = []
    dirty = []

    for e in entries:
        output = e['output']
        if only and not fnmatch.fnmatch(output, only):
            if output in old_lock.get('sidecars', {}):
                lock['sidecars'][output] = old_lock['sidecars'][output]
            continue
        if e.get('manual'):
            lock['sidecars'][output] = {'manual': True}
            report.append((output, 'manual', None))
            continue
        printout = pdir / e['printout']
        nii = pdir / e['nii'] if 'nii' in e else None
        try:
            sidecar = convert(printout, e.get('select'), nii)
        except Exception as exc:
            failures.append(output)
            report.append((output, 'error', f'{type(exc).__name__}: {exc}'))
            if output in old_lock.get('sidecars', {}):
                lock['sidecars'][output] = old_lock['sidecars'][output]
            continue
        sidecar = {**e.get('defaults', {}), **sidecar, **e.get('assigns', {})}
        entry = {'printout': e['printout'], 'sha256': sha256(printout)}
        if 'select' in e:
            entry['select'] = str(e['select'])
        if nii is not None:
            entry['nii'] = e['nii']
            entry['nii_sha256'] = sha256(nii)
        lock['sidecars'][output] = entry

        dst = tpl / output
        text = dumps(sidecar)
        old_text = dst.read_text() if dst.exists() else None
        committed = committed_text(dst, root)
        if committed is None:
            committed = old_text
        old = json.loads(committed) if committed else {}
        # roundtrip so that the comparison is done on json values
        new = json.loads(text)
        report.append((output, 'ok', diff(old, new)))
        if old_text != text:
            dirty.append(output)
            if not check:
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_text(text)

    lock_text = dumps(lock)
    old_lock_text = lock_path.read_text() if lock_path.exists() else None
    if old_lock_text != lock_text:
        dirty.append(lock_path.name)

    md = render_report(dataset, lock['protocol2bids'], report, pdir)
    print(md, file=out)
    if check:
        if dirty:
            print(f'--check: would change {dirty}', file=out)
        return 1 if (dirty or failures) else 0
    lock_path.write_text(lock_text)
    (pdir / 'report.md').write_text(md)
    return 1 if failures else 0


def render_report(dataset, version, report, pdir):
    lines = [f'# {dataset} sidecar regeneration report', '']
    known = pdir / 'known_issues.md'
    if known.exists():
        lines += [known.read_text().rstrip(), '']
    lines += [
        f'protocol2bids {version["version"]} (commit {version["commit"]})',
        '',
        'Generated by `scripts/make_sidecars.py`; diffs are against the '
        'sidecar at git HEAD when the script was run.', '',
    ]
    for output, status, info in report:
        lines.append(f'## {output}')
        lines.append('')
        if status == 'manual':
            lines += ['manual (no printout): kept as is', '']
            continue
        if status == 'error':
            lines += [f'**ERROR**: {info}', '']
            continue
        added, removed, changed = info
        if not (added or removed or changed):
            lines += ['unchanged', '']
            continue
        for k, v in added.items():
            lines.append(f'- added `{k}`: {_fmt(v)}')
        for k, v in removed.items():
            lines.append(f'- removed `{k}`: {_fmt(v)}')
        for k, (a, b) in changed.items():
            lines.append(f'- changed `{k}`: {_fmt(a)} -> {_fmt(b)}')
        lines.append('')
    return '\n'.join(lines)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('dataset', choices=sorted(TEMPLATES))
    p.add_argument('--check', action='store_true',
                   help='exit non-zero if regenerating would change files')
    p.add_argument('--only', help='glob on output paths (e.g. "BNI/*")')
    a = p.parse_args(argv)
    return run(a.dataset, check=a.check, only=a.only)


if __name__ == '__main__':
    sys.exit(main())
