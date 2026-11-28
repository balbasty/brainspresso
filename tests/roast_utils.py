"""
Shared helpers for roast characterization tests.

All assertions about the raw BIDS dataset go through `raw_tree` /
`raw_file`, which resolve the raw dataset with `Layout.detect`
(`<DS>/rawbids/` for a study, `<DS>/` for a flat dataset), so that the
same assertions hold in both layouts. Fixtures parametrized with
`LAYOUTS` run each test under both.
"""
import gzip
import io
import shutil
import tarfile
from pathlib import Path

import nibabel as nib
import numpy as np

from brainspresso.actions.state import TASKS_DIR, read_task_states
from brainspresso.bids.compost import compost
from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import STATE_DIR, Layout, make_layout
from brainspresso.bids.select import Selection

LAYOUTS = ("study", "flat")
_EXCLUDED = ("derivatives", "sourcedata", STATE_DIR)


def init_layout(ds_root: Path, kind: str) -> Layout:
    """Record a layout in the manifest, as `harvest --layout` does"""
    layout = make_layout(kind, ds_root)
    record_run(layout, "harvest", {"test": True})
    return layout


def layout_of(ds_root: Path) -> Layout:
    """Layout of a roasted dataset (it must be detectable)"""
    layout = Layout.detect(ds_root)
    assert layout is not None, f"no layout detected in {ds_root}"
    return layout


def readme_counts(ds_root: Path, state: str) -> dict:
    """Counts of the study README (`roast.run.with_study_readme`): a
    `study` tag in a study layout, nothing in a flat one"""
    if layout_of(ds_root).kind != "study":
        return {}
    return {"study": {state: 1}}


def raw_tree(ds_root: Path) -> set[str]:
    """Files of the raw BIDS dataset, relative to the raw dataset root."""
    layout = Layout.detect(ds_root)
    if layout is None or not layout.raw.is_dir():
        return set()
    raw = layout.raw
    out = set()
    for p in raw.rglob("*"):
        if not p.is_file():
            continue
        parts = p.relative_to(raw).parts
        if parts[0] in _EXCLUDED:
            continue
        out.add("/".join(parts))
    return out


def raw_file(ds_root: Path, rel: str) -> Path:
    """Path to a file of the raw BIDS dataset, given its relative name."""
    return layout_of(ds_root).raw / rel


def nii_bytes(shape=(2, 2, 2), gz=True, value=1, tr=None) -> bytes:
    """A tiny NIfTI image (`tr`: a 4D image with this TR, in seconds)."""
    img = nib.Nifti1Image(
        np.full(shape, value, dtype=np.int16), np.diag([2., 2., 2., 1.])
    )
    if tr is not None:
        img.header.set_xyzt_units("mm", "sec")
        img.header.set_zooms((2., 2., 2., tr))
    data = img.to_bytes()
    return gzip.compress(data) if gz else data


def analyze_pair(shape=(2, 2, 2), data=None):
    """Return (hdr_bytes, img_bytes) of a tiny Analyze image."""
    if data is None:
        data = np.arange(np.prod(shape), dtype=np.int16).reshape(shape)
    img = nib.AnalyzeImage(data, np.eye(4))
    hdr = io.BytesIO()
    imgb = io.BytesIO()
    img.to_file_map({
        "header": nib.FileHolder(fileobj=hdr),
        "image": nib.FileHolder(fileobj=imgb),
    })
    return hdr.getvalue(), imgb.getvalue()


def make_tar(path: Path, members: dict, mode="w:gz", dirs=()):
    """Write a tarball. `members` maps member name -> bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, mode) as tar:
        for d in dirs:
            info = tarfile.TarInfo(d)
            info.type = tarfile.DIRTYPE
            tar.addfile(info)
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return path


def read_tsv(path: Path) -> list[list[str]]:
    return [
        line.split("\t")
        for line in Path(path).read_text().splitlines() if line
    ]


# ----------------------------------------------------------------------
#   Scope of shards (`roast.scope`)
# ----------------------------------------------------------------------

def scoped_records(ds_root: Path) -> dict[str, dict]:
    """
    Task records of a roast; fail on a scoped one whose audit failed.

    A record with `scopes == []` is that of a shard whose outputs were
    found outside its declared scope (`RoastPolicy.audit`): it is never
    skipped by its scope, and its roaster's declaration is wrong.
    """
    states = read_task_states(layout_of(ds_root).state)
    for task, record in states.items():
        if record.get("status") == "done" and "scopes" in record:
            assert record["scopes"], f"{task}: outputs outside its scope"
    return states


def open_spy(monkeypatch, module, name: str = "TarArchive") -> list[str]:
    """Record the names of the archives that a roaster module opens."""
    opened = []
    real = getattr(module, name)

    def opener(path, *args, **kwargs):
        opened.append(Path(path).name)
        return real(path, *args, **kwargs)

    monkeypatch.setattr(module, name, opener)
    return opened


#: Scenarios of `check_scope` (a fresh dataset each)
SCOPE_SCENARIOS = ("narrow", "widen", "back", "seen", "compost")


def check_scope(scenario: str, ds_root: Path, roast, opened: list, *,
                a, b, label_a: str, label_b: str, archives: dict,
                keys: set, key: str, absent=None):
    """
    Check that a roaster's shards are skipped by their scope.

    Parameters
    ----------
    scenario : {"narrow", "widen", "back", "seen", "compost"}
        - narrow: a full roast, then `--subs a`, then `--exclude-keys
          meta`: no archive is opened again;
        - widen: `--subs a` then `--subs a b`: only the archives that
          hold `b` are opened again, and `b` is written; a subject held
          by no archive (`absent`) opens nothing;
        - back: `--subs a`, `--subs b`, `--subs a`: the third opens
          nothing;
        - seen: after a roast of `a` and `key` only, every scoped record
          has seen every subject of its archive;
        - compost: compost `b`, then `--exclude-subs b` opens nothing,
          then a full roast opens the archives of `b` and writes it
          again
    ds_root : Path
        Dataset folder
    roast : callable(**options) -> report
        Roast of the dataset (`subs=`, `exclude_subs=`, `keys=`,
        `exclude_keys=`)
    opened : list[str]
        Names of the archives opened (`open_spy`)
    a, b : subject values of `subs=`
        In different archives, if the dataset has several
    label_a, label_b : str
        Their labels
    archives : dict[str, (set[str], task id)]
        Archive name -> (labels of the subjects it holds, task id of its
        shard)
    keys : set[str]
        Keys of a full roast (meta and the image keys)
    key : str
        One image key that `a` has
    absent : subject value of `subs=`, optional
        A subject that no archive holds
    """
    def holding(label):
        return sorted(n for n, (labels, _) in archives.items()
                      if label in labels)

    def written(label):
        return any(p.startswith(f"sub-{label}/") for p in raw_tree(ds_root))

    def check(report):
        assert not report.errors, report.errors
        scoped_records(ds_root)

    if scenario == "narrow":
        check(roast(keys=keys))
        assert sorted(opened) == sorted(archives)
        before = raw_tree(ds_root)
        check(roast(keys=keys, subs=[a]))
        check(roast(keys=keys, exclude_keys={"meta"}))
        check(roast(keys={key}, subs=[a, b]))
        assert sorted(opened) == sorted(archives)
        assert raw_tree(ds_root) == before
    elif scenario == "widen":
        check(roast(keys=keys, subs=[a]))
        assert written(label_a) and not written(label_b)
        del opened[:]
        check(roast(keys=keys, subs=[a, b]))
        assert sorted(opened) == holding(label_b)
        assert written(label_b)
        if absent is not None:
            del opened[:]
            check(roast(keys=keys, subs=[a, b, absent]))
            assert opened == []
    elif scenario == "back":
        check(roast(keys=keys, subs=[a]))
        check(roast(keys=keys, subs=[b]))
        del opened[:]
        check(roast(keys=keys, subs=[a]))
        assert opened == []
    elif scenario == "seen":
        check(roast(keys={key}, subs=[a]))
        records = scoped_records(ds_root)
        for name, (labels, task) in archives.items():
            if label_a in labels:
                assert records[task]["seen"] == {"sub": sorted(labels)}
    elif scenario == "compost":
        check(roast(keys=keys))
        files = {p for p in raw_tree(ds_root)
                 if p.startswith(f"sub-{label_b}/")}
        assert files
        compost(ds_root, selection=Selection(exclude_subs=[label_b]),
                yes=True)
        assert not written(label_b)
        del opened[:]
        check(roast(keys=keys, exclude_subs=[b]))
        assert opened == [] and not written(label_b)
        check(roast(keys=keys))
        assert sorted(opened) == holding(label_b)
        assert files <= raw_tree(ds_root)
    else:
        raise ValueError(scenario)


def check_keys_rerun(ds_root: Path, roast, keys: set, shards: dict):
    """
    Check that adding a key that a shard reads re-runs it.

    `RoastPolicy.audit` cannot tell a key missing from a declaration when
    it has no folder of its own, or shares one (OASIS-I `avg`, `tal`,
    `fsl`): for each key, a roast without it, then with it, must re-run
    every shard that reads it (from no record: the task records are
    removed first).

    Parameters
    ----------
    ds_root : Path
        Dataset folder
    roast : callable(**options) -> report
        Roast of the dataset (`keys=`)
    keys : set[str]
        Keys of a full roast
    shards : dict[str, iterable[str]]
        Task id prefix -> the keys its shards read (`ctx.wants`, or the
        BIDS keys of their outputs)
    """
    state = layout_of(ds_root).state
    for prefix, read in shards.items():
        for key in read:
            shutil.rmtree(state / TASKS_DIR, ignore_errors=True)
            report = roast(keys=set(keys) - {key})
            assert not report.errors, report.errors
            before = {task: record.get("run") for task, record
                      in read_task_states(state).items()}
            report = roast(keys=set(keys))
            assert not report.errors, report.errors
            after = scoped_records(ds_root)
            tasks = [task for task in after if task.startswith(prefix)]
            assert tasks, prefix
            for task in tasks:
                assert after[task]["run"] != before.get(task), \
                    f"{task} skipped when {key!r} is added"
