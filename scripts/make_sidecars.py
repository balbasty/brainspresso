#!/usr/bin/env python
"""Regenerate dataset sidecar templates from protocol printouts.

Maintainer tool: requires `protocol2bids` (and `tomli` on Python 3.10)
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
written to `brainspresso/datasets/<DS>/templates/<output>`. Provenance
(including, per sidecar, the protocol2bids version and commit) is written
to `printouts/<DATASET>/sidecars.lock.json` and a diff report to
`printouts/<DATASET>/report.md`.

With `--check`, nothing is written and the exit code is non-zero if
regenerating would change any sidecar or the lock file.

`report.md` lists, per sidecar, the keys added, removed and changed
compared to the sidecar at git `--baseline` (default `HEAD`; an unknown
ref is an error); each section names its protocol2bids version and
baseline. An unreadable baseline sidecar only makes its diff unavailable.

With `--only PATTERN`, only the matching entries are regenerated, plus the
`common` entries built from a matching sidecar; the lock entries and report
sections of the other sidecars are kept as they are.

An entry may set `fill_from` (and `fill_select`) to fill the keys missing
from the sidecar from another printout of the same acquisition (e.g. the
site scan table), as `p2b --fill-from` does.

An entry may set `previous` (path relative to the templates) to diff a new
sidecar against an older template at another path (e.g. a per-site template
replaced by per-sample ones); it must exist at a git baseline, which is
checked for every entry before anything is written.

An entry may set `common` (a list of other outputs) instead of a printout:
its sidecar holds the keys whose values agree across those sidecars (e.g.
a site template used when the data does not name the sample).

A `manual = true` entry (no printout) is kept as is and takes no conversion
key (`printout`, `select`, `nii`, `defaults`, `assigns`, `fill_from`,
`fill_select`).
"""
import argparse
import fnmatch
import hashlib
import importlib.metadata as md
import json
import subprocess
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:     # Python < 3.11
    import tomli as tomllib
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
              'fill_from', 'fill_select', 'manual', 'common', 'note',
              'previous'}
# keys that drive a conversion (meaningless, hence rejected, if manual
# or common)
CONVERSION_KEYS = ENTRY_KEYS - {'output', 'manual', 'common', 'note',
                                'previous'}


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
        if e.get('manual') and 'common' in e:
            raise TableError(f'{path}: {e["output"]}: manual and common')
        if e.get('manual') or 'common' in e:
            kind = 'manual' if e.get('manual') else 'common'
            ignored = sorted((CONVERSION_KEYS | {'manual'}) & set(e)
                             - {kind})
            if ignored:
                raise TableError(f'{path}: {e["output"]}: {kind} entry with '
                                 f'keys {ignored}')
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
    converted = {e['output'] for e in entries
                 if not e.get('manual') and 'common' not in e}
    for e in entries:
        if 'common' in e:
            srcs = e['common']
            if (not isinstance(srcs, list)
                    or not all(isinstance(x, str) for x in srcs)
                    or len(set(srcs)) < 2 or not set(srcs) <= converted):
                raise TableError(f'{path}: {e["output"]}: common must list '
                                 'two or more distinct converted outputs')
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
        import protocol2bids  # noqa: PLC0415  lazy: optional (maintainers)
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

def _canon(v):
    return json.dumps(v, sort_keys=True, default=_jsonable)


def common_keys(sidecars):
    """Keys (and values) shared by all sidecars, in the first one's order
    (values compared as canonical JSON: `false` != `0`, `90` != `90.0`)."""
    first, *rest = sidecars
    return {k: v for k, v in first.items()
            if all(k in o and _canon(o[k]) == _canon(v) for o in rest)}


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
    version = protocol2bids_version()
    if version.get('version') is None:
        version = None      # not installed: no version recorded

    lock_path = pdir / 'sidecars.lock.json'
    report_path = pdir / 'report.md'
    old_lock = (json.loads(lock_path.read_text())
                if lock_path.exists() else {})
    old_locked = old_lock.get('sidecars', {})
    # with --only, entries outside the filter keep their lock entry and
    # report section (merge, don't replace)
    old_sections = (report_sections(report_path.read_text())
                    if only and report_path.exists() else {})

    def selected(e):
        if only is None:
            return True
        outputs = [e['output'], *e.get('common', [])]
        return any(fnmatch.fnmatch(o, only) for o in outputs)

    todo = [e for e in entries if not e.get('manual') and selected(e)]
    # baselines first: nothing is written if one is missing
    baselines, errors = {}, []
    for e in todo:
        try:
            baselines[e['output']] = baseline_text(e, tpl, root, baseline,
                                                   in_git)
        except TableError as exc:
            errors.append(str(exc))
    if errors:
        raise TableError('; '.join(errors))

    locked = {}
    report = {}
    failures = []
    dirty = []
    texts = {}      # regenerated sidecars (text)

    def keep(output):
        if output in old_locked:
            rec = dict(old_locked[output])
            if 'protocol2bids' not in rec and 'protocol2bids' in old_lock:
                rec['protocol2bids'] = old_lock['protocol2bids']
            locked[output] = rec

    def write(e, sidecar, entry, prov):
        output = e['output']
        dst = tpl / output
        text = dumps(sidecar)
        texts[output] = text
        if prov is not None:
            entry['protocol2bids'] = prov
        locked[output] = entry
        try:
            old = json.loads(baselines[output] or '{}')
            if not isinstance(old, dict):
                raise ValueError(f'not an object: {type(old).__name__}')
        except ValueError as exc:   # includes json.JSONDecodeError
            report[output] = ('nodiff', f'{type(exc).__name__}: {exc}', prov)
        else:
            # roundtrip so that the comparison is done on json values
            report[output] = ('ok', diff(old, json.loads(text)), prov)
        old_text = dst.read_text() if dst.exists() else None
        if old_text != text:
            dirty.append(output)
            if not check:
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_text(text)

    def fail(output, exc, prov=None):
        failures.append(output)
        report[output] = ('error', f'{type(exc).__name__}: {exc}', prov)
        keep(output)

    for e in entries:
        output = e['output']
        if e.get('manual'):
            locked[output] = {'manual': True}
            report[output] = ('manual', None, None)
        elif not selected(e):
            keep(output)
            report[output] = ('kept', old_sections.get(output), None)
        elif 'common' in e:
            continue    # once the sidecars it is made of are known
        else:
            printout = pdir / e['printout']
            nii = pdir / e['nii'] if 'nii' in e else None
            fill_from = pdir / e['fill_from'] if 'fill_from' in e else None
            try:
                sidecar = convert(printout, e.get('select'), nii)
                filled = (fill(fill_from, e.get('fill_select'))
                          if fill_from else {})
            except Exception as exc:
                fail(output, exc, version)
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
            write(e, sidecar, entry, version)

    for e in entries:
        if 'common' not in e or e.get('manual') or not selected(e):
            continue
        # provenance: that of its sources
        prov = lock_version({src: locked.get(src, {})
                             for src in e['common']})
        try:
            sources = []
            for src in e['common']:
                if src in failures:
                    raise TableError(f'{src} failed')
                text = texts.get(src)
                if text is None:
                    text = (tpl / src).read_text()
                sources.append(json.loads(text))
        except (OSError, ValueError) as exc:
            fail(e['output'], exc, prov)
            continue
        write(e, common_keys(sources), {'common': list(e['common'])}, prov)

    lock = {'protocol2bids': lock_version(locked), 'sidecars': {
        e['output']: locked[e['output']] for e in entries
        if e['output'] in locked}}
    lock_text = dumps(lock)
    old_lock_text = lock_path.read_text() if lock_path.exists() else None
    if old_lock_text != lock_text:
        dirty.append(lock_path.name)

    md = render_report(dataset, [
        (e['output'], *report[e['output']]) for e in entries], baseline)
    print(md, file=out)
    if check:
        if dirty:
            print(f'--check: would change {dirty}', file=out)
        return 1 if (dirty or failures) else 0
    lock_path.write_text(lock_text)
    report_path.write_text(md)
    return 1 if failures else 0


def lock_version(locked):
    """protocol2bids of all lock entries, or "mixed" (None if none)."""
    versions = {_canon(rec['protocol2bids']): rec['protocol2bids']
                for rec in locked.values() if 'protocol2bids' in rec}
    if len(versions) == 1:
        return dict(next(iter(versions.values())))
    return 'mixed' if versions else None


def baseline_text(e, tpl, root, baseline, in_git):
    """Text of the sidecar an entry is diffed against (None: new).

    The sidecar at git `baseline`, else its `previous` template there
    (a `previous` missing at a git baseline is an error). Without git
    (`HEAD` outside a checkout), or for a sidecar not committed yet, the
    working copy (where a missing `previous` is not an error: it has
    usually been deleted).
    """
    def read(path):
        if in_git:
            return committed_text(path, root, baseline)
        return path.read_text() if path.exists() else None

    text = read(tpl / e['output'])
    if text is None and 'previous' in e:
        text = read(tpl / e['previous'])
        if text is None and in_git:
            raise TableError(f'{e["output"]}: previous {e["previous"]} '
                             f'not found at baseline {baseline}')
    if text is None and baseline == 'HEAD':
        # not committed yet: diff against the working copy
        path = tpl / e['output']
        text = path.read_text() if path.exists() else None
    return text


def _short(version):
    if not isinstance(version, dict):
        return str(version)     # "mixed"
    commit = version.get('commit')
    return (f'{version["version"]} (commit {commit[:7]})' if commit
            else str(version['version']))


def render_report(dataset, report, baseline='HEAD'):
    lines = [f'# {dataset} sidecar regeneration report', '']
    lines += [
        'Generated by `scripts/make_sidecars.py`. Each section gives the '
        'protocol2bids version it was converted with and the git ref of '
        'the sidecar it is diffed against (`--baseline`).', '',
    ]
    for output, status, info, version in report:
        prov = (f'protocol2bids {_short(version)}; baseline `{baseline}`'
                if version else f'baseline `{baseline}`')
        lines.append(f'## {output}')
        lines.append('')
        if status == 'manual':
            lines += ['manual (no printout): kept as is', '']
            continue
        if status == 'kept':
            lines += [info or 'not regenerated (outside `--only`)', '']
            continue
        lines += [prov, '']
        if status == 'error':
            lines += [f'**ERROR**: {info}', '']
            continue
        if status == 'nodiff':
            lines += [f'diff unavailable: unreadable baseline ({info})', '']
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
