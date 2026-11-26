"""
Reader/writer locks of `actions.File` (`actions.locks`), across processes.

Processes are started with the "spawn" method, as the engine does.
"""
import hashlib
import multiprocessing
import os
import threading
import time
from pathlib import Path

import pytest

from brainspresso.actions import (
    CopyBytes, File, Files, LockError, LockTimeout, Unlink, WrapAction,
)
from brainspresso.actions.engine import Context, Shard, run
from brainspresso.actions.locks import (
    LOCAL_DIR_ENV, STATE_DIR, TIMEOUT_ENV, FileLock, find_dataset,
    forget_datasets, lock_file,
)
from brainspresso.bids.layout import STATE_DIR as LAYOUT_STATE_DIR

SPAWN = multiprocessing.get_context("spawn")
# generous bound for things that must happen (slow CI machines)
SOON = 60


@pytest.fixture(autouse=True)
def local_locks(tmp_path, monkeypatch):
    """Locks of files outside datasets in a folder of the test."""
    monkeypatch.setenv(LOCAL_DIR_ENV, str(tmp_path / "local-locks"))
    monkeypatch.delenv(TIMEOUT_ENV, raising=False)


def dataset(root: Path) -> Path:
    """A dataset folder (it has a state folder)."""
    (root / STATE_DIR).mkdir(parents=True)
    return root


def listing(folder: Path) -> list[str]:
    return sorted(p.relative_to(folder).as_posix()
                  for p in folder.rglob("*"))


# ----------------------------------------------------------------------
#   Where lock files live
# ----------------------------------------------------------------------

def test_state_dir_is_that_of_layouts():
    assert STATE_DIR == LAYOUT_STATE_DIR


def test_lock_file_inside_a_dataset(tmp_path):
    ds = dataset(tmp_path / "DS")
    path = ds / "rawbids" / "sub-01" / "anat" / "sub-01_T1w.nii.gz"
    lockfile, found = lock_file(path)
    assert found == ds
    digest = hashlib.sha256(
        b"rawbids/sub-01/anat/sub-01_T1w.nii.gz").hexdigest()
    assert lockfile == ds / STATE_DIR / "locks" / digest[:2] / \
        f"{digest}.lock"
    # same lock file through a symbolic link to the dataset
    (tmp_path / "link").symlink_to(ds)
    assert lock_file(tmp_path / "link" / path.relative_to(ds))[0] == lockfile
    # files of the state folder too
    assert lock_file(ds / STATE_DIR / "orig" / "participants.tsv")[1] == ds


def test_lock_file_of_nested_datasets(tmp_path):
    """Sites: the nearest dataset wins."""
    parent = dataset(tmp_path / "FCON")
    site = dataset(parent / "ICBM")
    assert find_dataset(site / "sub-01" / "x.nii") == site
    assert find_dataset(parent / "README") == parent


def test_lock_file_outside_datasets(tmp_path):
    lockfile, found = lock_file(tmp_path / "templates" / "README")
    assert found is None
    assert tmp_path / "local-locks" in lockfile.parents


# ----------------------------------------------------------------------
#   Many concurrent readers (shards in a spawn pool)
# ----------------------------------------------------------------------

def read_many(ctx, shard, src, n):
    for j in range(n):
        yield CopyBytes(src, ctx.root / "out" / f"{shard}-{j}.json")


def read_shared(ctx):
    for shard in range(8):
        yield Shard(read_many, shard, ctx.options["src"], 40,
                    id=f"test/read-{shard}")


class OptionsContext(Context):
    def __init__(self, *args, options=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.options = dict(options or {})


@pytest.mark.parametrize("where", ["dataset", "outside"])
def test_concurrent_readers_of_one_file(tmp_path, where):
    ds = dataset(tmp_path / "DS")
    folder = ds / "sourcedata" if where == "dataset" else tmp_path / "pkg"
    folder.mkdir(parents=True)
    src = folder / "template.json"
    src.write_text('{"Name": "shared"}')
    before = listing(folder)
    ctx = OptionsContext(ds, state=ds / STATE_DIR, options={"src": src})
    report = run(read_shared, ctx, jobs=4, display=False)
    assert report.errors == []
    assert report.counts["run"] == {"done": 8 * 40}
    assert len(list((ds / "out").glob("*.json"))) == 8 * 40
    # nothing created next to the source
    assert listing(folder) == before
    if where == "dataset":
        assert lock_file(src)[0].exists()       # never deleted


# ----------------------------------------------------------------------
#   Writers and readers in other processes
# ----------------------------------------------------------------------

def write_file(path, text, started, acquired, mode="w", timeout=None):
    started.set()
    with File(path, mode, timeout=timeout) as f:
        acquired.set()
        with f.open() as fobj:
            fobj.write(text)


def read_file(path, started, acquired, seen, hold=0.0):
    started.set()
    with File(path, "r") as f:
        acquired.set()
        with f.open() as fobj:
            seen.put(fobj.read())
        time.sleep(hold)


def start(target, *args, **kwargs):
    proc = SPAWN.Process(target=target, args=args, kwargs=kwargs)
    proc.start()
    return proc


def events(n=2):
    return [SPAWN.Event() for _ in range(n)]


def test_writer_waits_for_reader(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("old")
    started, acquired = events()
    with File(path, "r"):
        proc = start(write_file, path, "new", started, acquired)
        assert started.wait(SOON)
        # the writer cannot take the file while it is read
        assert not acquired.wait(1.0)
        assert path.read_text() == "old"
    assert acquired.wait(SOON)
    proc.join(SOON)
    assert proc.exitcode == 0
    assert path.read_text() == "new"


def test_reader_reopening_sees_the_same_content(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("version 1")
    started, acquired = events()
    with File(path, "rt") as f:
        with f.open() as fobj:
            first = fobj.read()
        proc = start(write_file, path, "version 2", started, acquired)
        assert started.wait(SOON)
        time.sleep(0.5)
        with f.open() as fobj:
            second = fobj.read()
        assert first == second == "version 1"
        assert not acquired.is_set()        # the writer still waits
    assert acquired.wait(SOON)
    proc.join(SOON)
    assert proc.exitcode == 0
    assert path.read_text() == "version 2"


def test_readers_wait_for_writer(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("old")
    seen = SPAWN.Queue()
    started, acquired = events()
    with File(path, "w") as f:
        f.safename.write_text("new")
        proc = start(read_file, path, started, acquired, seen)
        assert started.wait(SOON)
        assert not acquired.wait(1.0)
    assert acquired.wait(SOON)
    assert seen.get(timeout=SOON) == "new"      # never the old one
    proc.join(SOON)
    assert proc.exitcode == 0


def test_concurrent_readers_do_not_block_each_other(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("shared")
    seen = SPAWN.Queue()
    started, acquired = events()
    with File(path, "r"):
        proc = start(read_file, path, started, acquired, seen)
        assert acquired.wait(SOON)
        assert seen.get(timeout=SOON) == "shared"
    proc.join(SOON)
    assert proc.exitcode == 0


def append_many(path, marker, n, intervals):
    for _ in range(n):
        with File(path, "a") as f:
            begin = time.time()
            with f.open("at") as fobj:
                fobj.write(f"{marker}\n")
            time.sleep(0.005)
            end = time.time()
        intervals.put((begin, end))


def test_writers_of_one_file_are_serialised(tmp_path):
    path = tmp_path / "log.txt"
    path.write_text("")
    intervals = SPAWN.Queue()
    procs = [start(append_many, path, marker, 20, intervals)
             for marker in ("a", "b", "c")]
    spans = sorted(intervals.get(timeout=SOON) for _ in range(60))
    for proc in procs:
        proc.join(SOON)
        assert proc.exitcode == 0
    lines = path.read_text().splitlines()
    # read-modify-write under the exclusive lock: no update is lost
    assert sorted(lines) == ["a"] * 20 + ["b"] * 20 + ["c"] * 20
    # and no two writers ever held the file at once
    for (_, end), (begin, _) in zip(spans, spans[1:]):
        assert begin >= end
    assert not (tmp_path / "log.txt.tmp").exists()


def try_lock(path, mode, result, timeout=None, env_timeout=None):
    if env_timeout is not None:
        os.environ[TIMEOUT_ENV] = str(env_timeout)
    tic = time.monotonic()
    try:
        with File(path, mode, timeout=timeout):
            result.put(("locked", "", time.monotonic() - tic))
    except Exception as e:
        result.put((type(e).__name__, str(e), time.monotonic() - tic))


def test_timeout_names_the_file_and_a_writer(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("x")
    result = SPAWN.Queue()
    with File(path, "w"):
        proc = start(try_lock, path, "r", result, timeout=0.5)
        kind, message, elapsed = result.get(timeout=SOON)
    proc.join(SOON)
    assert kind == "LockTimeout"
    assert str(path) in message and "read (shared)" in message
    assert f"held by a writer (pid {os.getpid()} on" in message
    assert elapsed >= 0.5


def test_timeout_names_readers(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("x")
    result = SPAWN.Queue()
    with File(path, "r"):
        # timeout from the environment
        proc = start(try_lock, path, "w", result, env_timeout=0.3)
        kind, message, elapsed = result.get(timeout=SOON)
    proc.join(SOON)
    assert kind == "LockTimeout"
    assert str(path) in message and "write (exclusive)" in message
    assert "held by readers" in message
    assert elapsed >= 0.3
    assert path.read_text() == "x"


# ----------------------------------------------------------------------
#   Read-only sources, in-process rules
# ----------------------------------------------------------------------

@pytest.mark.parametrize("where", ["dataset", "outside"])
def test_read_only_source_folder(tmp_path, where):
    ds = dataset(tmp_path / "DS")
    folder = ds / "sourcedata" if where == "dataset" else tmp_path / "ro"
    folder.mkdir(parents=True)
    src = folder / "T1w.json"
    src.write_text('{"a": 1}')
    folder.chmod(0o555)
    try:
        before = listing(folder)
        CopyBytes(src, ds / "out" / "T1w.json", ifexists="overwrite")()
        assert (ds / "out" / "T1w.json").read_text() == '{"a": 1}'
        assert listing(folder) == before
    finally:
        folder.chmod(0o755)


def test_reader_without_lock_folder(tmp_path):
    """A read-only state folder: readers go on, writers refuse."""
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root ignores permissions")
    ds = dataset(tmp_path / "DS")
    src = ds / "sourcedata" / "x.txt"
    src.parent.mkdir()
    src.write_text("x")
    (ds / STATE_DIR).chmod(0o555)
    try:
        with File(src, "r") as f:
            assert f.lock is not None and not f.lock.locked
            assert f.safename.read_text() == "x"
        with pytest.raises(LockError, match="for writing"):
            with File(ds / "out.txt", "w"):
                pass
    finally:
        (ds / STATE_DIR).chmod(0o755)


def test_nested_readers_and_lock_file_kept(tmp_path):
    ds = dataset(tmp_path / "DS")
    path = ds / "archive.tar"
    path.write_bytes(b"tar")
    with File(path, "r") as outer:
        with File(path, "r") as inner:          # (claim, then member)
            assert inner.lock.locked
        assert outer.lock.locked
        assert lock_file(path)[0].exists()
        # the outer reader still holds the file
        lock = FileLock(path, exclusive=True, timeout=0.2)
        thread = threading.Thread(target=lambda: errors.append(
            _catch(lock.acquire)))
        errors = []
        thread.start()
        thread.join()
        assert errors and isinstance(errors[0], LockTimeout)
        assert "readers in this process" in str(errors[0])
    assert lock_file(path)[0].exists()
    with File(path, "w"):                       # free again
        pass


def _catch(fn):
    try:
        fn()
    except Exception as e:
        return e
    return None


def test_upgrade_and_nested_writers_raise(tmp_path):
    path = tmp_path / "x.txt"
    path.write_text("x")
    with File(path, "r"):
        with pytest.raises(LockError, match="upgrade"):
            with File(path, "w"):
                pass
    with File(path, "w"):
        with pytest.raises(LockError, match="already being written"):
            with File(path, "a"):
                pass
    with File(path) as f:                       # deferred protection
        with f.open("rb") as fobj:
            assert fobj.read() == b"x"
        with pytest.raises(LockError, match="upgrade"):
            with f.open("wb"):
                pass
    with File(path, "w") as f:                  # reader inside its writer
        with File(path, "r"):
            pass


def test_threads_of_one_process(tmp_path):
    """A writer thread waits for a reader thread of the same process."""
    path = tmp_path / "x.txt"
    path.write_text("old")
    acquired = threading.Event()

    def writer():
        with File(path, "w") as f:
            acquired.set()
            f.safename.write_text("new")

    with File(path, "r"):
        thread = threading.Thread(target=writer)
        thread.start()
        assert not acquired.wait(0.5)
        assert path.read_text() == "old"
    thread.join(SOON)
    assert acquired.is_set() and path.read_text() == "new"


def test_failed_writer_leaves_the_file(tmp_path):
    path = tmp_path / "x.txt"
    path.write_text("old")
    with pytest.raises(RuntimeError):
        with File(path, "w") as f:
            f.safename.write_text("partial")
            raise RuntimeError("interrupted")
    assert path.read_text() == "old"
    assert not (tmp_path / "x.txt.tmp").exists()
    with File(path, "w", timeout=1):            # the lock was released
        pass


# ----------------------------------------------------------------------
#   Cleanup errors, updates in place, deletions
# ----------------------------------------------------------------------

class _FailsOnExit:
    """A member of `Files` whose cleanup fails."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        raise OSError("cleanup failed")


class _FailsOnEnter:
    def __enter__(self):
        raise LockError("cannot enter")

    def __exit__(self, *exc):
        pass


def test_files_cleanup_errors_do_not_mask_the_exception(tmp_path, caplog):
    path = tmp_path / "x.txt"
    path.write_text("old")
    # exiting because of an exception: it propagates, cleanup is logged
    with pytest.raises(ValueError, match="original"):
        with Files(File(path, "w"), _FailsOnExit()):
            raise ValueError("original")
    assert "cleanup failed" in caplog.text
    assert path.read_text() == "old"
    with File(path, "w", timeout=1):            # every file was exited
        pass
    # the cleanup of a failed `__enter__` does not mask its error either
    files = Files(File(path, "r"), _FailsOnExit(), _FailsOnEnter())
    with pytest.raises(LockError, match="cannot enter"):
        files.__enter__()
    with File(path, "w", timeout=1):
        pass
    # a clean exit raises the cleanup error
    with pytest.raises(OSError, match="cleanup failed"):
        with Files(File(path, "r"), _FailsOnExit()):
            pass


def test_action_that_updates_its_source_in_place(tmp_path):
    path = tmp_path / "x.txt"
    path.write_text("old")

    def fix(src, fobj):
        fobj.write(Path(src).read_text().upper())

    action = WrapAction(path, path, fix, mode="t", ifexists="overwrite")
    statuses = list(action)
    assert not [s for s in statuses if s.get("status") == "error"]
    assert {"status": "done"} in statuses
    assert path.read_text() == "OLD"
    assert action.outputs == [path]
    # through a symbolic link of its folder too
    path.write_text("old")
    (tmp_path / "link").symlink_to(tmp_path)
    action = WrapAction(tmp_path / "link" / "x.txt", path, fix, mode="t",
                        ifexists="overwrite")
    statuses = list(action)
    assert {"status": "done"} in statuses, statuses
    assert path.read_text() == "OLD"
    with File(path, "w", timeout=1):            # no lock left behind
        pass


def test_unlink_deletes_its_destination_under_its_lock(tmp_path):
    path = tmp_path / "x.txt"
    path.write_text("old")
    done = threading.Event()
    statuses = []

    def unlink():
        statuses.extend(Unlink(path))
        done.set()

    with File(path, "r"):
        thread = threading.Thread(target=unlink)
        thread.start()
        # it waits for the reader
        assert not done.wait(0.5)
        assert path.read_text() == "old"
    thread.join(SOON)
    assert statuses == [{"status": "done"}]
    assert not path.exists()
    assert not (tmp_path / "x.txt.tmp").exists()
    assert list(Unlink(path)) == [{"status": "done"}]       # missing_ok
    statuses = list(Unlink(path, missing_ok=False))
    assert statuses[0]["status"] == "error"


# ----------------------------------------------------------------------
#   Downloads replace files under the lock of their destination
# ----------------------------------------------------------------------

def download_file(path, text, finishing, done):
    import asyncio
    from brainspresso.download.incomplete import IncompleteFile

    async def go():
        async with IncompleteFile(path) as partial:
            await partial.append(text.encode())
            finishing.set()             # the rename comes next

    asyncio.run(go())
    done.set()


@pytest.mark.parametrize("where", ["dataset", "outside"])
def test_download_waits_for_reader_before_replacing(tmp_path, where):
    root = dataset(tmp_path / "DS") if where == "dataset" else tmp_path
    path = root / "sourcedata" / "data.txt"
    path.parent.mkdir(parents=True)
    path.write_text("old")
    finishing, done = events()
    with File(path, "r") as f:
        proc = start(download_file, path, "new", finishing, done)
        assert finishing.wait(SOON)
        # the complete download waits until the file is not read
        assert not done.wait(1.0)
        with f.open("rt") as fobj:
            assert fobj.read() == "old"
    assert done.wait(SOON)
    proc.join(SOON)
    assert proc.exitcode == 0
    assert path.read_text() == "new"
    assert sorted(p.name for p in path.parent.iterdir()) == ["data.txt"]


def test_download_package_imports_no_stage_layer():
    import ast
    import brainspresso.download as download_pkg
    for path in sorted(Path(download_pkg.__file__).parent.glob("*.py")):
        tree = ast.parse(path.read_text())
        modules = {n.module for n in ast.walk(tree)
                   if isinstance(n, ast.ImportFrom) and n.module}
        modules |= {a.name for n in ast.walk(tree)
                    if isinstance(n, ast.Import) for a in n.names}
        bad = {m for m in modules if m.startswith(
            ("brainspresso.bids", "brainspresso.roast"))}
        assert not bad, (path.name, bad)


# ----------------------------------------------------------------------
#   The state folder exists before any lock is taken
# ----------------------------------------------------------------------

@pytest.fixture
def locks_seen(monkeypatch):
    """(path, dataset found) of every `FileLock.acquire`."""
    seen = []
    acquire = FileLock.acquire

    def spy(self):
        seen.append((self.path, find_dataset(self.path)))
        return acquire(self)

    monkeypatch.setattr(FileLock, "acquire", spy)
    return seen


@pytest.mark.parametrize("kind", ["study", "flat"])
def test_harvest_creates_the_state_folder_before_locks(
    tmp_path, monkeypatch, locks_seen, kind,
):
    import asyncio
    from brainspresso.bids import harvest as harvest_module
    from brainspresso.bids.select import Selection
    from brainspresso.download.incomplete import IncompleteFile
    from brainspresso.sources.base import RemoteEntry

    class Manager:
        """Downloads without network: through `IncompleteFile`."""

        def __init__(self, downloaders, **kwargs):
            self.downloaders = list(downloaders)

        def run(self):
            async def fetch(dst):
                async with IncompleteFile(dst) as partial:
                    await partial.append(b"data")
            for dl in self.downloaders:
                asyncio.run(fetch(dl.dst))

    monkeypatch.setattr(harvest_module, "DownloadManager", Manager)
    root = tmp_path / "DS"
    entries = [
        RemoteEntry(f"sub-0{i}/anat/sub-0{i}_T1w.nii.gz",
                    url=f"http://localhost/{i}", size=4)
        for i in (1, 2)
    ]
    harvest_module.harvest(entries, root, Selection(), layout=kind,
                           trim=False)
    assert len(locks_seen) == 2
    assert all(found == root for _, found in locks_seen)


def test_record_run_creates_the_state_folder(tmp_path, locks_seen):
    from brainspresso.bids.harvest import record_run
    from brainspresso.bids.layout import make_layout
    layout = make_layout("study", tmp_path / "DS")
    record_run(layout, "harvest", {"dataset": "DS"})    # before downloads
    with File(layout.source / "x.tgz", "w"):
        pass
    assert locks_seen == [(layout.source / "x.tgz", layout.root)]


def test_ensure_state_of_a_sites_layout(tmp_path):
    from brainspresso.bids.layout import SitesLayout
    root = tmp_path / "DS"
    (root / "site-a").mkdir(parents=True)
    layout = SitesLayout(root, {"site-a": "flat", "site-b": "flat"})
    layout.ensure_state()
    assert (root / STATE_DIR).is_dir()
    assert (root / "site-a" / STATE_DIR).is_dir()
    assert not (root / "site-b").exists()       # not created for nothing


# ----------------------------------------------------------------------
#   The dataset of a folder is looked up once
# ----------------------------------------------------------------------

def test_dataset_of_a_folder_is_looked_up_once(tmp_path, monkeypatch):
    import brainspresso.actions.locks as locks
    ds = dataset(tmp_path / "DS")
    folders = [ds / "rawbids" / "sub-01" / "anat",
               ds / "rawbids" / "sub-02" / "anat"]
    forget_datasets()
    looked = []
    is_state_dir = locks._is_state_dir
    monkeypatch.setattr(locks, "_is_state_dir",
                        lambda f: looked.append(f) or is_state_dir(f))
    for folder in folders:
        for i in range(20):
            with FileLock(folder / f"sub_{i}.nii.gz"):
                pass
            assert lock_file(folder / f"sub_{i}.json")[1] == ds
    real = [Path(os.path.realpath(f)) for f in folders]
    # one walk up to the dataset per folder
    assert looked == [real[0], *real[0].parents[:3],
                      real[1], *real[1].parents[:3]]


def test_state_folder_created_later_is_found(tmp_path):
    from brainspresso.bids.layout import FlatLayout
    path = tmp_path / "DS" / "site-a" / "sub-01" / "anat" / "x.nii.gz"
    forget_datasets()
    assert find_dataset(path) is None           # (a miss is not cached)
    (tmp_path / "DS" / STATE_DIR).mkdir(parents=True)
    assert find_dataset(path) == tmp_path / "DS"
    # a nearer one, created by a layout, replaces the cached one
    FlatLayout(tmp_path / "DS" / "site-a").ensure_state()
    assert find_dataset(path) == tmp_path / "DS" / "site-a"
    assert lock_file(path)[0].is_relative_to(
        tmp_path / "DS" / "site-a" / STATE_DIR)
