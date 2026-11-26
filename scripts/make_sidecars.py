#!/usr/bin/env python
"""Regenerate dataset sidecar templates from protocol printouts.

Maintainer tool: requires `protocol2bids` (``pip install .[maint]``).
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

Every difference with the previous template is classified automatically
(see `classify`) and written to `printouts/<DATASET>/known_issues.md`:

- renames (`ManufacturerModelName` -> `ManufacturersModelName`, the old key
  was misspelt);
- (a) old-template errors corrected by protocol2bids: the old value is not
  a valid BIDS value (e.g. `PhaseEncodingDirection: "AP"`, a text
  `SliceTiming`, `SliceTiming` in an anatomical sidecar, an implausible
  `EffectiveEchoSpacing`, `RepetitionTimeExcitation` in a bold sidecar);
- (b) suspected protocol2bids issues: the new value is invalid, or a valid
  old value was dropped or changed.

An entry may set `previous` (path relative to the templates) to diff a new
sidecar against an older template at another path (e.g. a per-site template
replaced by per-sample ones).
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
              'manual', 'note', 'previous'}


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
# classification of differences
# ---------------------------------------------------------------------------

RENAMES = {'ManufacturerModelName': 'ManufacturersModelName'}
DIRECTIONS = {'i', 'j', 'k', 'i-', 'j-', 'k-'}
# plausible ranges (seconds, degrees, tesla)
RANGES = {
    'RepetitionTime': (1e-3, 20),
    'RepetitionTimeExcitation': (1e-3, 20),
    'RepetitionTimePreparation': (1e-3, 20),
    'EchoTime': (1e-4, 0.5),
    'InversionTime': (1e-3, 5),
    'FlipAngle': (0, 180),
    'EffectiveEchoSpacing': (1e-5, 1e-2),
    'TotalReadoutTime': (1e-3, 0.5),
    'DwellTime': (1e-7, 1e-3),
    'MagneticFieldStrength': (0.1, 12),
    'SliceThickness': (0.1, 20),
    'ParallelReductionFactorInPlane': (1, 16),
    'MultibandAccelerationFactor': (1, 16),
}
ENUMS = {
    'MRAcquisitionType': {'2D', '3D'},
    'PhaseEncodingDirection': DIRECTIONS,
    'SliceEncodingDirection': DIRECTIONS,
}
# MRI sidecar keys of the BIDS specification (1.10); dropping another key
# (dcm2niix/vendor extras of the old templates) is reported separately
BIDS_KEYS = set('''
Manufacturer ManufacturersModelName DeviceSerialNumber StationName
SoftwareVersions HardcopyDeviceSoftwareVersion MagneticFieldStrength
ReceiveCoilName ReceiveCoilActiveElements GradientSetType
MRTransmitCoilSequence MatrixCoilMode CoilCombinationMethod
PulseSequenceType ScanningSequence SequenceVariant ScanOptions
SequenceName PulseSequenceDetails NonlinearGradientCorrection
MRAcquisitionType MTState MTOffsetFrequency MTPulseBandwidth
MTNumberOfPulses MTPulseShape MTPulseDuration SpoilingState SpoilingType
SpoilingRFPhaseIncrement SpoilingGradientMoment SpoilingGradientDuration
NumberShots ParallelReductionFactorInPlane ParallelAcquisitionTechnique
ParallelReductionFactorOutOfPlane PartialFourier PartialFourierDirection
PhaseEncodingDirection EffectiveEchoSpacing TotalReadoutTime MixingTime
EchoTime InversionTime SliceTiming SliceEncodingDirection DwellTime
FlipAngle NegativeContrast AnatomicalLandmarkCoordinates
InstitutionName InstitutionAddress InstitutionalDepartmentName
ContrastBolusIngredient RepetitionTime RepetitionTimeExcitation
RepetitionTimePreparation VolumeTiming TaskName
NumberOfVolumesDiscardedByScanner
NumberOfVolumesDiscardedByUser DelayTime AcquisitionDuration
DelayAfterTrigger Instructions TaskDescription CogAtlasID CogPOID
MultibandAccelerationFactor B0FieldIdentifier B0FieldSource IntendedFor
Units EchoTime1 EchoTime2 MultipartID SpacingBetweenSlices SliceThickness
PhaseEncodingSteps ImagingFrequency
'''.split())
ANAT_SUFFIXES = ('T1w', 'T2w', 'FLAIR', 'PDw', 'T2starw')
NOT_IN_ANAT = ('SliceTiming',)
NOT_IN_FUNC = ('RepetitionTimeExcitation',)


def _is_num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _suffix(output):
    return Path(output).stem.split('_')[-1]


def invalid(output, key, value):
    """Reason why `value` is not a valid BIDS value for `key` (or None)."""
    suffix = _suffix(output)
    if suffix in ANAT_SUFFIXES and key in NOT_IN_ANAT:
        return f'`{key}` in an anatomical ({suffix}) sidecar'
    if suffix in ('bold', 'dwi') and key in NOT_IN_FUNC:
        return f'`{key}` in a {suffix} sidecar'
    if key in ENUMS:
        if value not in ENUMS[key]:
            return f'`{key}` {_fmt(value)} not in {sorted(ENUMS[key])}'
        return None
    if key == 'SliceTiming':
        if not (isinstance(value, list) and value
                and all(_is_num(v) for v in value)):
            return f'`SliceTiming` {_fmt(value)} is not an array of numbers'
        if min(value) < 0 or max(value) > RANGES['RepetitionTime'][1]:
            return '`SliceTiming` values out of range'
        return None
    if key in RANGES:
        lo, hi = RANGES[key]
        if not _is_num(value):
            return f'`{key}` {_fmt(value)} is not a number'
        if not lo <= value <= hi:
            return f'`{key}` {_fmt(value)} outside plausible [{lo}, {hi}]'
    return None


def classify(output, old, new):
    """Classify the differences between an old and a new sidecar.

    Returns a list of (category, title, detail), with category one of
    'rename', 'fixed' (old-template error corrected), 'suspect'
    (suspected protocol2bids issue), 'nonbids' (non-BIDS key of the old
    template).
    """
    out = []
    added, removed, changed = diff(old, new)
    for a, b in RENAMES.items():
        if a in removed and b in new:
            out.append(('rename', f'`{a}` -> `{b}`',
                        f'{_fmt(removed.pop(a))}'))
            added.pop(b, None)
    for k, v in removed.items():
        why = invalid(output, k, v)
        if k in RENAMES:
            out.append(('suspect', f'`{RENAMES[k]}` not produced',
                        f'old (misspelt) `{k}` was {_fmt(v)}'))
        elif k not in BIDS_KEYS:
            out.append(('nonbids', f'`{k}` dropped', f'was {_fmt(v)}'))
        elif why:
            out.append(('fixed', f'invalid `{k}` dropped', why))
        else:
            out.append(('suspect', f'`{k}` dropped', f'was {_fmt(v)}'))
    for k, (a, b) in changed.items():
        why_old, why_new = invalid(output, k, a), invalid(output, k, b)
        if k not in BIDS_KEYS:
            out.append(('nonbids', f'`{k}` differs',
                        f'{_fmt(a)} -> {_fmt(b)}'))
        elif why_new:
            out.append(('suspect', f'invalid `{k}` produced', why_new))
        elif why_old:
            out.append(('fixed', f'invalid `{k}` corrected',
                        f'{why_old}; now {_fmt(b)}'))
        else:
            out.append(('suspect', f'`{k}` differs',
                        f'{_fmt(a)} -> {_fmt(b)}'))
    for k, v in added.items():
        why = invalid(output, k, v)
        if why:
            out.append(('suspect', f'invalid `{k}` produced', why))
    for key in ('RepetitionTime',):
        if key not in new and not any(
                k.startswith(key) for k in new):
            out.append(('suspect', 'no repetition time produced', ''))
    return out


def render_issues(dataset, issues):
    """Render the classified differences (known_issues.md)."""
    sections = [
        ('rename', 'Renamed keys (fixes)',
         'The old templates used a misspelt key; protocol2bids writes the '
         'BIDS name. Not a loss.'),
        ('fixed', '(a) Old-template errors corrected by protocol2bids',
         'The old (hand-parsed) value was not a valid BIDS value.'),
        ('suspect', '(b) Suspected protocol2bids issues',
         'The new value is invalid, or a valid old value was dropped or '
         'changed. Values were NOT hand-fixed: fix protocol2bids (or add '
         '`defaults`/`assigns` in `sidecars.toml`) and re-run.'),
        ('nonbids', 'Non-BIDS keys of the old templates (dropped or changed)',
         'Vendor/dcm2niix keys that are not BIDS sidecar fields; listed '
         'for reference only.'),
    ]
    lines = [f'# {dataset}: differences with the previous templates', '',
             'Generated by `scripts/make_sidecars.py` (do not edit): '
             'differences between the regenerated sidecars and the previous '
             'templates, classified by rules on value validity (BIDS enums, '
             '`i`/`j`/`k` directions, numeric `SliceTiming`, plausible '
             'ranges).', '']
    for cat, title, intro in sections:
        lines += [f'## {title}', '', intro, '']
        groups = {}
        for output, e, c, t, detail in issues:
            if c == cat:
                groups.setdefault(t, []).append((output, e, detail))
        if not groups:
            lines += ['None.', '']
        for t, items in sorted(groups.items(), key=lambda x: -len(x[1])):
            lines += [f'### {t} ({len(items)})', '']
            for output, e, detail in items:
                src = f'`{e["printout"]}`'
                if 'select' in e:
                    src += f' (select `{e["select"]}`)'
                lines.append(f'- `{output}` <- {src}'
                             + (f': {detail}' if detail else ''))
            lines.append('')
    return '\n'.join(lines)


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

    lock_path = pdir / 'sidecars.lock.json'
    old_lock = (json.loads(lock_path.read_text())
                if lock_path.exists() else {})
    lock = {'protocol2bids': protocol2bids_version(), 'sidecars': {}}
    report = []
    issues = []
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
        committed = committed_text(dst, root, baseline)
        if committed is None and 'previous' in e:
            committed = committed_text(tpl / e['previous'], root, baseline)
        if committed is None and baseline == 'HEAD':
            committed = old_text
        old = json.loads(committed) if committed else {}
        # roundtrip so that the comparison is done on json values
        new = json.loads(text)
        report.append((output, 'ok', diff(old, new)))
        issues += [(output, e, *i) for i in classify(output, old, new)]
        if old_text != text:
            dirty.append(output)
            if not check:
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_text(text)

    lock_text = dumps(lock)
    old_lock_text = lock_path.read_text() if lock_path.exists() else None
    if old_lock_text != lock_text:
        dirty.append(lock_path.name)

    known = render_issues(dataset, issues)
    md = render_report(dataset, lock['protocol2bids'], report, known,
                       baseline)
    print(md, file=out)
    if check:
        if dirty:
            print(f'--check: would change {dirty}', file=out)
        return 1 if (dirty or failures) else 0
    lock_path.write_text(lock_text)
    if not only:
        (pdir / 'known_issues.md').write_text(known)
    (pdir / 'report.md').write_text(md)
    return 1 if failures else 0


def render_report(dataset, version, report, known, baseline='HEAD'):
    lines = [f'# {dataset} sidecar regeneration report', '']
    lines += [known.rstrip(), '']
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
