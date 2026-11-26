#!/usr/bin/env python
"""Regenerate dataset sidecar templates from protocol printouts.

Maintainer tool: requires `protocol2bids`
(``pip install -r requirements-maint.txt``).
It is never needed at runtime.

Usage
-----
    python scripts/make_sidecars.py DATASET [--check] [--only PATTERN]
                                    [--baseline REF]

DATASET is one of ABIDE1, ABIDE2, CoRR
(see `printouts/<DATASET>/sidecars.toml`).

For each entry of the conversion table, the printout is converted with
protocol2bids, `defaults` and `assigns` are applied, and the sidecar is
written to `brainspresso/datasets/<DS>/templates/<output>`. Provenance is
written to `printouts/<DATASET>/sidecars.lock.json` and a diff report to
`printouts/<DATASET>/report.md`.

With `--check`, nothing is written and the exit code is non-zero if
regenerating would change any sidecar or the lock file.

`report.md` lists, per sidecar, the keys added, removed and changed
compared to the sidecar at git `--baseline` (default `HEAD`; an unknown
ref is an error).

With `--only PATTERN`, only the matching entries are regenerated; the lock
entries and report sections of the other sidecars are kept as they are.

An entry may set `fill_from` (and `fill_select`) to fill the keys missing
from the sidecar from another printout of the same acquisition (e.g. the
site scan table), as `p2b --fill-from` does.

An entry may set `previous` (path relative to the templates) to diff a new
sidecar against an older template at another path (e.g. a per-site template
replaced by per-sample ones); it must exist at the baseline.

A `manual = true` entry (no printout) is kept as is and takes no conversion
key (`printout`, `select`, `nii`, `defaults`, `assigns`, `fill_from`,
`fill_select`).
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
              'fill_from', 'fill_select', 'manual', 'note', 'previous'}
# keys that drive a conversion (meaningless, hence rejected, if manual)
CONVERSION_KEYS = ENTRY_KEYS - {'output', 'manual', 'note', 'previous'}


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
            ignored = sorted(CONVERSION_KEYS & set(e))
            if ignored:
                raise TableError(f'{path}: {e["output"]}: manual entry with '
                                 f'conversion keys {ignored}')
            continue
        if 'printout' not in e:
            raise TableError(f'{path}: {e["output"]}: no printout '
                             '(use manual = true)')
        if 'fill_select' in e and 'fill_from' not in e:
            raise TableError(f'{path}: {e["output"]}: fill_select without '
                             'fill_from')
        for key in ('printout', 'nii', 'fill_from'):
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


def _protocol2bids():
    """Import protocol2bids, with a clear error if it is not installed."""
    try:
        import protocol2bids
    except ImportError as exc:
        raise ImportError(
            'make_sidecars.py needs protocol2bids (maintainers only): '
            'pip install -r requirements-maint.txt') from exc
    return protocol2bids.protocol_to_bids, protocol2bids.read_protocols


def convert(printout, select=None, nii=None):
    """Convert one protocol of a printout to a BIDS sidecar (dict)."""
    protocol_to_bids, read_protocols = _protocol2bids()
    prots = _select(read_protocols(printout), printout, select)
    return protocol_to_bids(prots[0], nii=nii, extra=False)


def fill(printout, select=None):
    """Sidecar used to fill missing keys (as `p2b --fill-from`).

    Like the protocol2bids CLI, the fill document is converted without
    NIfTI header and defaults to its first protocol; its keys never
    override the converted printout, `defaults` or `assigns`.
    """
    protocol_to_bids, read_protocols = _protocol2bids()
    prots = _select(read_protocols(printout), printout,
                    0 if select is None else select)
    return protocol_to_bids(prots[0], extra=False)


def _select(prots, printout, select):
    if select is not None:
        sel = str(select)
        if sel.isdigit():
            prots = [prots[int(sel)]]
        else:
            prots = [p for p in prots if fnmatch.fnmatch(p.name, sel)]
    if len(prots) != 1:
        raise TableError(f'{printout}: selector {select!r} matches '
                         f'{len(prots)} protocols')
    return prots


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


def verify_ref(ref, root=ROOT):
    """Check that git `ref` names a commit of the repository at `root`.

    Returns False if `ref` is `HEAD` and `root` is not a git checkout
    (the working copy is then the baseline); raises `TableError` for any
    other ref that does not resolve.
    """
    import subprocess
    try:
        res = subprocess.run(
            ['git', 'rev-parse', '--verify', '--quiet', f'{ref}^{{commit}}'],
            cwd=root, capture_output=True, text=True, check=False)
        ok = res.returncode == 0
    except OSError:
        ok = False
    if not ok and ref != 'HEAD':
        raise TableError(f'--baseline {ref!r}: not a git commit')
    return ok


def report_sections(text):
    """Body of each `## <output>` section of a report."""
    sections, current = {}, None
    for line in text.splitlines():
        if line.startswith('## '):
            current = line[3:]
            sections[current] = []
        elif current is not None:
            sections[current].append(line)
    return {k: '\n'.join(v).strip('\n') for k, v in sections.items()}


def committed_text(path, root=ROOT, ref='HEAD'):
    """Content of `path` at git `ref` (None if unavailable)."""
    import subprocess
    try:
        rel = Path(path).resolve().relative_to(Path(root).resolve())
        res = subprocess.run(
            ['git', 'show', f'{ref}:{rel.as_posix()}'], cwd=root,
            capture_output=True, text=True, check=False)
    except (OSError, ValueError):
        return None
    return res.stdout if res.returncode == 0 else None


def _fmt(v):
    return json.dumps(v, ensure_ascii=False, default=_jsonable)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def run(dataset, check=False, only=None, root=ROOT, out=sys.stdout,
        baseline='HEAD'):
    root = Path(root)
    pdir = root / 'printouts' / dataset
    tpl = template_dir(dataset, root)
    entries = load_table(dataset, root)
    missing = uncovered(dataset, entries, root)
    if missing:
        raise TableError(f'{dataset}: template sidecars not in table: '
                         f'{missing}')

    in_git = verify_ref(baseline, root)

    lock_path = pdir / 'sidecars.lock.json'
    report_path = pdir / 'report.md'
    old_lock = (json.loads(lock_path.read_text())
                if lock_path.exists() else {})
    old_locked = old_lock.get('sidecars', {})
    # with --only, entries outside the filter keep their lock entry and
    # report section (merge, don't replace)
    old_sections = (report_sections(report_path.read_text())
                    if only and report_path.exists() else {})
    lock = {'protocol2bids': protocol2bids_version(), 'sidecars': {}}
    report = []
    failures = []
    dirty = []

    for e in entries:
        output = e['output']
        if e.get('manual'):
            lock['sidecars'][output] = {'manual': True}
            report.append((output, 'manual', None))
            continue
        if only and not fnmatch.fnmatch(output, only):
            if output in old_locked:
                lock['sidecars'][output] = old_locked[output]
            report.append((output, 'kept', old_sections.get(output)))
            continue
        printout = pdir / e['printout']
        nii = pdir / e['nii'] if 'nii' in e else None
        fill_from = pdir / e['fill_from'] if 'fill_from' in e else None
        try:
            sidecar = convert(printout, e.get('select'), nii)
            filled = (fill(fill_from, e.get('fill_select'))
                      if fill_from else {})
        except Exception as exc:
            failures.append(output)
            report.append((output, 'error', f'{type(exc).__name__}: {exc}'))
            if output in old_locked:
                lock['sidecars'][output] = old_locked[output]
            continue
        sidecar = {**filled, **e.get('defaults', {}), **sidecar,
                   **e.get('assigns', {})}
        entry = {'printout': e['printout'], 'sha256': sha256(printout)}
        if 'select' in e:
            entry['select'] = str(e['select'])
        if nii is not None:
            entry['nii'] = e['nii']
            entry['nii_sha256'] = sha256(nii)
        if fill_from is not None:
            entry['fill_from'] = e['fill_from']
            entry['fill_sha256'] = sha256(fill_from)
            if 'fill_select' in e:
                entry['fill_select'] = str(e['fill_select'])

        dst = tpl / output
        text = dumps(sidecar)
        old_text = dst.read_text() if dst.exists() else None
        try:
            old = json.loads(baseline_text(e, tpl, root, baseline, in_git)
                             or '{}')
        except json.JSONDecodeError as exc:
            failures.append(output)
            report.append((output, 'error', f'baseline {baseline}: '
                           f'{type(exc).__name__}: {exc}'))
            if output in old_locked:
                lock['sidecars'][output] = old_locked[output]
            continue
        lock['sidecars'][output] = entry
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

    md = render_report(dataset, lock['protocol2bids'], report, baseline)
    print(md, file=out)
    if check:
        if dirty:
            print(f'--check: would change {dirty}', file=out)
        return 1 if (dirty or failures) else 0
    lock_path.write_text(lock_text)
    report_path.write_text(md)
    return 1 if failures else 0


def baseline_text(e, tpl, root, baseline, in_git):
    """Text of the sidecar an entry is diffed against (None: new).

    The sidecar at git `baseline`, else its `previous` template there.
    Without git (`HEAD` outside a checkout), the working copy. A
    `previous` that does not exist at the baseline is an error.
    """
    def read(path):
        if in_git:
            return committed_text(path, root, baseline)
        return path.read_text() if path.exists() else None

    text = read(tpl / e['output'])
    if text is None and 'previous' in e:
        text = read(tpl / e['previous'])
        if text is None:
            raise TableError(f'{e["output"]}: previous {e["previous"]} '
                             f'not found at baseline {baseline}')
    if text is None and baseline == 'HEAD':
        # not committed yet: diff against the working copy
        path = tpl / e['output']
        text = path.read_text() if path.exists() else None
    return text


def render_report(dataset, version, report, baseline='HEAD'):
    lines = [f'# {dataset} sidecar regeneration report', '']
    lines += [
        f'protocol2bids {version["version"]} (commit {version["commit"]})',
        '',
        'Generated by `scripts/make_sidecars.py`; diffs are against the '
        f'sidecar at git `{baseline}` when the script was run.', '',
    ]
    for output, status, info in report:
        lines.append(f'## {output}')
        lines.append('')
        if status == 'manual':
            lines += ['manual (no printout): kept as is', '']
            continue
        if status == 'kept':
            lines += [info or 'not regenerated (outside `--only`)', '']
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
    p.add_argument('--baseline', default='HEAD',
                   help='git ref of the previous templates to diff against '
                        '(default: HEAD)')
    a = p.parse_args(argv)
    return run(a.dataset, check=a.check, only=a.only, baseline=a.baseline)


if __name__ == '__main__':
    sys.exit(main())
