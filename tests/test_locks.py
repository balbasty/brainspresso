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

from brainspresso.actions import CopyBytes, File, LockError, LockTimeout
from brainspresso.actions.engine import Context, Shard, run
from brainspresso.actions.locks import (
    LOCAL_DIR_ENV, STATE_DIR, TIMEOUT_ENV, FileLock, find_dataset, lock_file,
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
