"""
Lock-file identity (registry, layer A) and OS primitives (layer B) of
`actions.locks`: the tests of the design note, on every platform.
"""
import errno
import multiprocessing
import os
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from lock_probe import (
    conflicts_with_child,
    ofd_getlk,
    second_descriptor_conflicts,
)

from brainspresso.actions import locks
from brainspresso.actions.locks import (
    LOCAL_DIR_ENV,
    STATE_DIR,
    TIMEOUT_ENV,
    FileLock,
    LockError,
    LockTimeout,
    forget_datasets,
    lock_file,
)

try:
    import fcntl
except ImportError:                     # (Windows)
    fcntl = None

SPAWN = multiprocessing.get_context("spawn")
# (scripts run with `-c` import brainspresso from the checkout)
REPO = Path(__file__).resolve().parents[1]
SOON = 60
LINUX = sys.platform.startswith("linux")
WINDOWS = sys.platform == "win32"
MACOS = sys.platform == "darwin"
# per-descriptor primitive (two entries of one file exclude each other)
PER_DESCRIPTOR = LINUX or WINDOWS


@pytest.fixture(autouse=True)
def local_locks(tmp_path, monkeypatch):
    monkeypatch.setenv(LOCAL_DIR_ENV, str(tmp_path / "local-locks"))
    monkeypatch.delenv(TIMEOUT_ENV, raising=False)
    forget_datasets()
    yield
    forget_datasets()
    assert not locks._entries(), "an entry outlived its holders"


def dataset(root: Path) -> Path:
    (root / STATE_DIR).mkdir(parents=True)
    return root


@pytest.fixture
def spellings(tmp_path, monkeypatch):
    """A dataset and a second spelling of its folder (a symbolic link
    that `_real` does not resolve, as a case alias on macOS)."""
    ds = dataset(tmp_path / "DS")
    other = tmp_path / "ds-alias"
    other.symlink_to(ds, target_is_directory=True)
    monkeypatch.setattr(locks, "_real", lambda p: Path(os.path.abspath(p)))
    return ds, other


def in_thread(fn, *args):
    out = {}

    def run():
        try:
            out["value"] = fn(*args)
        except BaseException as e:
            out["error"] = e

    thread = threading.Thread(target=run)
    thread.start()
    return thread, out


def entry_of(lock: FileLock):
    return lock._entry


# ----------------------------------------------------------------------
#   Cross-process helpers (spawn)
# ----------------------------------------------------------------------

def try_lock(path, exclusive, timeout, result, real=False):
    if real:                            # (the parent patched `_real`)
        locks._real = lambda p: Path(os.path.abspath(p))
    try:
        with FileLock(path, exclusive=exclusive, timeout=timeout):
            result.put("locked")
    except LockTimeout as e:
        result.put(f"timeout: {e.holder}")


def other_process_gets(path, exclusive=True, timeout=0.3, real=False):
    result = SPAWN.Queue()
    proc = SPAWN.Process(target=try_lock,
                         args=(path, exclusive, timeout, result, real))
    proc.start()
    out = result.get(timeout=SOON)
    proc.join(SOON)
    assert proc.exitcode == 0
    return out == "locked"


# ----------------------------------------------------------------------
#   Threads
# ----------------------------------------------------------------------

def test_threads_creating_one_lock_file_share_one_entry(tmp_path,
                                                         monkeypatch):
    ds = dataset(tmp_path / "DS")
    path = ds / "sub-01" / "x.nii"
    opened, closed = [], []
    open_lockfile, close = locks._open_lockfile, os.close

    def spy_open(p):
        fd, writable = open_lockfile(p)
        opened.append(fd)
        return fd, writable

    def spy_close(fd):
        closed.append(fd)
        return close(fd)

    monkeypatch.setattr(locks, "_open_lockfile", spy_open)
    monkeypatch.setattr(os, "close", spy_close)
    barrier = threading.Barrier(8)
    held = []

    def read():
        barrier.wait(SOON)
        held.append(FileLock(path).acquire())

    threads = [in_thread(read)[0] for _ in range(8)]
    for thread in threads:
        thread.join(SOON)
    assert len(held) == 8
    assert len({id(entry_of(lock)) for lock in held}) == 1
    assert len(locks._entries()) == 1
    for lock in held:
        lock.release()
    monkeypatch.undo()
    # every descriptor opened (the entry's and the parked ones) is closed
    assert opened and set(opened) <= set(closed)


def test_close_and_reopen_race_keeps_exclusion(tmp_path, monkeypatch):
    """I2: an entry that closes and a new one never both hold a lock."""
    if not WINDOWS:                     # (classic fcntl: per process)
        monkeypatch.setattr(locks, "_MECHANISMS", (locks._ClassicLock(),))
    path = dataset(tmp_path / "DS") / "counter"
    path.write_text("0")
    errors = []

    def work():
        try:
            for _ in range(40):
                with FileLock(path, exclusive=True, timeout=SOON):
                    n = int(path.read_text())
                    path.write_text(str(n + 1))
        except BaseException as e:
            errors.append(e)

    threads = [in_thread(work)[0] for _ in range(8)]
    for thread in threads:
        thread.join(SOON)
    assert not errors
    assert int(path.read_text()) == 8 * 40


def test_waiter_on_a_retired_entry_restarts(tmp_path, monkeypatch):
    path = dataset(tmp_path / "DS") / "x.nii"
    lock = FileLock(path).acquire()
    old = entry_of(lock)
    closing, proceed = threading.Event(), threading.Event()
    close = os.close

    def slow_close(fd):
        if fd == old.fd:
            closing.set()
            proceed.wait(SOON)
        return close(fd)

    monkeypatch.setattr(os, "close", slow_close)
    releaser, _ = in_thread(lock.release)
    assert closing.wait(SOON)           # retired, its descriptor closing
    assert old.retired and not old.gone.is_set()
    waiter, out = in_thread(lambda: FileLock(path, timeout=SOON).acquire())
    time.sleep(0.3)
    assert waiter.is_alive()            # it waits for `gone`
    proceed.set()
    releaser.join(SOON)
    waiter.join(SOON)
    new = out["value"]
    assert new.locked and entry_of(new) is not old
    new.release()


# (nested readers, upgrades and nested writers: test_locks.py)


# ----------------------------------------------------------------------
#   Aliases
# ----------------------------------------------------------------------

def test_two_spellings_one_entry_and_one_spare(spellings):
    ds, other = spellings
    a = FileLock(ds / "x.nii").acquire()
    b = FileLock(other / "x.nii").acquire()
    try:
        assert a.lockfile != b.lockfile                 # two path keys
        assert entry_of(a) is entry_of(b)
        assert len(entry_of(a).spares) == 1
        assert len(locks._entries()) == 1
        # an upgrade through the other spelling is detected
        with pytest.raises(LockError, match="upgrade"):
            FileLock(other / "x.nii", exclusive=True).acquire()
    finally:
        b.release()
        a.release()


def test_case_alias_merges(tmp_path, monkeypatch):
    """Two spellings that differ only by case (caseless filesystem)."""
    ds = dataset(tmp_path / "DS")
    if MACOS or WINDOWS:                # (their default filesystems)
        other = tmp_path / "ds"
        assert other.is_dir()
        monkeypatch.setattr(locks, "_real",
                            lambda p: Path(os.path.abspath(p)))
    else:                               # simulated: a link, case only
        other = tmp_path / "ds"
        other.symlink_to(ds, target_is_directory=True)
        monkeypatch.setattr(locks, "_real",
                            lambda p: Path(os.path.abspath(p)))
        # (realpath of a caseless filesystem keeps the case given)
        monkeypatch.setattr(locks, "_alias_key", lambda p: os.path.abspath(
            p).casefold())
    a = FileLock(ds / "x.nii").acquire()
    b = FileLock(other / "x.nii").acquire()
    try:
        assert entry_of(a) is entry_of(b)
        assert len(locks._entries()) == 1
    finally:
        b.release()
        a.release()


def test_without_identity_two_entries_still_exclude(spellings, monkeypatch):
    """No inode: two entries; per-descriptor locks still exclude them."""
    ds, other = spellings
    monkeypatch.setattr(locks, "_ident", lambda fd, path: None)
    a = FileLock(ds / "x.nii", exclusive=True).acquire()
    try:
        thread, out = in_thread(
            lambda: FileLock(other / "x.nii", exclusive=True,
                             timeout=0.3).acquire())
        thread.join(SOON)
        if PER_DESCRIPTOR:
            assert isinstance(out.get("error"), LockTimeout)
        else:       # (macOS: classic locks belong to the process; the
            # documented double failure, no exclusion in the process)
            assert out["value"].locked
            out["value"].release()
    finally:
        a.release()


@pytest.mark.parametrize("fake", [False, True])
def test_same_relative_path_in_two_datasets_two_entries(tmp_path,
                                                        monkeypatch, fake):
    if fake:                            # equal (st_dev, st_ino)
        monkeypatch.setattr(locks, "_file_id", lambda fd: (1, 42))
        monkeypatch.setattr(locks, "_path_id", lambda path: (1, 42))
    a, b = dataset(tmp_path / "A"), dataset(tmp_path / "B")
    la = FileLock(a / "x.nii", exclusive=True).acquire()
    lb = FileLock(b / "x.nii", exclusive=True).acquire()
    try:
        assert la.lockfile.name == lb.lockfile.name
        assert entry_of(la) is not entry_of(lb)
        assert len(locks._entries()) == 2
        assert entry_of(la).ident[:2] == entry_of(lb).ident[:2] or not fake
        assert not other_process_gets(b / "x.nii")
    finally:
        la.release()
        lb.release()
    assert other_process_gets(b / "x.nii")


@pytest.mark.skipif(WINDOWS, reason=(
    "Windows cannot delete a lock file that a handle keeps open"))
def test_live_entry_by_identity_wins_over_a_retired_one_by_path(
    spellings, monkeypatch,
):
    """
    Amendment B. A joiner through path p finds `by_path[p]` retired (its
    file deleted, its descriptor closing) and a live entry of the file
    now at p, made through another spelling: it parks its descriptor in
    the live entry, and closes no descriptor of the live file.
    """
    ds, other = spellings
    p_path, alias = ds / "x.nii", other / "x.nii"
    monkeypatch.setattr(locks, "_MECHANISMS", (locks._ClassicLock(),))
    # 1. T3 checks `by_path[p]` (missing) and is paused before it opens
    paused, resume = threading.Event(), threading.Event()
    open_lockfile = locks._open_lockfile

    def hooked_open(p):
        if threading.current_thread().name == "T3":
            paused.set()
            resume.wait(SOON)
        return open_lockfile(p)

    monkeypatch.setattr(locks, "_open_lockfile", hooked_open)
    t3 = threading.Thread(name="T3", target=lambda: results.update(
        t3=FileLock(p_path, timeout=SOON).acquire()))
    results = {}
    t3.start()
    assert paused.wait(SOON)
    # 2. an entry of p (file X) is made, released, and its close blocked
    lock = FileLock(p_path).acquire()
    old = entry_of(lock)
    closing, proceed = threading.Event(), threading.Event()
    closed = []
    close = os.close

    def hooked_close(fd):
        if fd == old.fd:
            closing.set()
            proceed.wait(SOON)
        closed.append(fd)
        return close(fd)

    monkeypatch.setattr(os, "close", hooked_close)
    releaser, _ = in_thread(lock.release)
    assert closing.wait(SOON) and old.retired
    # 3. X is deleted; Y is made at the same place, through the alias
    lockfile = lock_file(p_path)[0]
    os.unlink(lockfile)
    live = FileLock(alias, exclusive=True).acquire()
    try:
        assert entry_of(live) is not old
        live_fd = entry_of(live).fd
        # 4. T3 resumes: opens Y through p, finds both entries
        resume.set()
        time.sleep(0.5)
        assert live_fd not in closed
        # the live entry kept its lock (closing a descriptor of Y would
        # have dropped this process's classic locks on it)
        assert not other_process_gets(p_path, exclusive=False, real=True)
    finally:
        proceed.set()
        releaser.join(SOON)
        live.release()
        t3.join(SOON)
    assert results["t3"].locked
    assert entry_of(results["t3"]) is not old
    results["t3"].release()


# ----------------------------------------------------------------------
#   Staleness
# ----------------------------------------------------------------------

def test_lock_file_deleted_between_holds_is_recreated(tmp_path):
    path = dataset(tmp_path / "DS") / "x.nii"
    with FileLock(path, exclusive=True) as lock:
        lockfile = lock.lockfile
    os.unlink(lockfile)
    with FileLock(path, exclusive=True) as lock:
        assert lockfile.exists()
        assert not other_process_gets(path)


def test_lock_file_deleted_while_a_thread_waits(tmp_path):
    path = dataset(tmp_path / "DS") / "x.nii"
    holder = FileLock(path, exclusive=True).acquire()
    waiter, out = in_thread(
        lambda: FileLock(path, exclusive=True, timeout=SOON).acquire())
    time.sleep(0.2)
    if WINDOWS:     # (a lock file kept open by a handle is not deleted)
        with pytest.raises(PermissionError):
            os.unlink(holder.lockfile)
        holder.release()
        waiter.join(SOON)
        out["value"].release()
        return
    os.unlink(holder.lockfile)
    holder.release()
    waiter.join(SOON)
    lock = out["value"]
    try:
        # it ended on the new file, which excludes other processes
        assert lock.lockfile.exists()
        st = os.stat(lock.lockfile)
        assert entry_of(lock).ident[:2] == (st.st_dev, st.st_ino)
        assert not other_process_gets(path)
    finally:
        lock.release()


def test_state_folder_replaced_while_held(tmp_path):
    ds = dataset(tmp_path / "DS")
    path = ds / "x.nii"
    holder = FileLock(path).acquire()
    if WINDOWS:     # (a folder with an open file cannot be renamed)
        with pytest.raises(PermissionError):
            (ds / STATE_DIR).rename(ds / ".old")
        holder.release()
        return
    (ds / STATE_DIR).rename(ds / ".old")
    (ds / STATE_DIR).mkdir()
    forget_datasets()
    holder.release()
    with FileLock(path, exclusive=True, timeout=5) as lock:
        assert lock.lockfile.is_relative_to(ds / STATE_DIR)
        st = os.stat(lock.lockfile)
        assert entry_of(lock).ident[:2] == (st.st_dev, st.st_ino)
        assert not other_process_gets(path)


def test_stat_error_keeps_the_lock(tmp_path, monkeypatch):
    path = dataset(tmp_path / "DS") / "x.nii"

    def eio(p):
        raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr(locks, "_path_id", eio)
    with FileLock(path, exclusive=True) as lock:
        assert lock.locked and not entry_of(lock).retired


def test_replaced_lock_file_is_locked_again_then_accepted(tmp_path,
                                                          monkeypatch,
                                                          caplog):
    path = dataset(tmp_path / "DS") / "x.nii"
    joins = []
    join = FileLock._join

    def spy(self, *args):
        joins.append(args[0])
        return join(self, *args)

    monkeypatch.setattr(FileLock, "_join", spy)
    if WINDOWS:     # (no verification: Windows keeps open files)
        with FileLock(path, exclusive=True):
            pass
        assert len(joins) == 1
        return
    # once replaced: locked again (two rounds)
    replaced = iter([True])
    path_id = locks._path_id
    monkeypatch.setattr(locks, "_path_id", lambda p: (
        (0, 0) if next(replaced, False) else path_id(p)))
    with FileLock(path, exclusive=True) as lock:
        assert lock.locked
    assert len(joins) == 2
    # always replaced: accepted after `_ROUNDS` rounds, with a warning
    joins.clear()
    monkeypatch.setattr(locks, "_path_id", lambda p: (0, 0))
    with FileLock(path, exclusive=True) as lock:
        assert lock.locked
    assert len(joins) == locks._ROUNDS
    assert "was replaced while it was locked" in caplog.text


# ----------------------------------------------------------------------
#   Mechanisms
# ----------------------------------------------------------------------

def test_probe_second_descriptor(tmp_path):
    assert second_descriptor_conflicts(tmp_path) is PER_DESCRIPTOR


def test_chain_of_mechanisms():
    names = [m.name for m in locks._MECHANISMS]
    if LINUX:
        assert names == ["ofd", "fcntl"]
    elif WINDOWS:
        assert names == ["LockFileEx"]
    else:
        assert names == ["fcntl", "flock"]


@pytest.mark.skipif(not LINUX, reason="OFD locks: Linux only")
def test_ofd_struct_flock_layout(tmp_path):
    """`F_OFD_GETLK` reads back our `struct flock`: layout verified."""
    got = ofd_getlk(tmp_path)
    assert got["l_type"] == got["F_WRLCK"]
    assert (got["l_whence"], got["l_start"], got["l_len"]) == (0, 0, 0)
    assert got["l_pid"] == -1               # (an OFD lock)


@pytest.mark.skipif(not LINUX, reason="OFD locks: Linux only")
def test_ofd_without_named_constant_uses_37(tmp_path, monkeypatch):
    monkeypatch.delattr(fcntl, "F_OFD_SETLK", raising=False)
    commands = []
    real = fcntl.fcntl

    def spy(fd, cmd, *args):
        commands.append(cmd)
        return real(fd, cmd, *args)

    monkeypatch.setattr(fcntl, "fcntl", spy)
    with FileLock(tmp_path / "x.nii", exclusive=True):
        pass
    assert 37 in commands


def test_refused_primitive_falls_back_to_the_next(tmp_path, monkeypatch):
    path = dataset(tmp_path / "DS") / "x.nii"
    first = type(locks._MECHANISMS[0])

    def refused(self, fd, exclusive):
        raise OSError(errno.EINVAL, "Invalid argument")

    monkeypatch.setattr(first, "try_lock", refused)
    if WINDOWS:                         # (no other primitive)
        with pytest.raises(LockError, match="does the filesystem"):
            FileLock(path, exclusive=True).acquire()
        return
    with FileLock(path, exclusive=True) as lock:
        assert entry_of(lock).mech == 1
        if not MACOS:   # (macOS: flock vs fcntl across processes is
            # what test_flock_and_lockf_of_two_processes reports)
            assert not other_process_gets(path)


def test_downgrade_failure_keeps_it_exclusive(tmp_path, monkeypatch):
    path = dataset(tmp_path / "DS") / "x.nii"
    mechanism = type(locks._MECHANISMS[0])

    def fails(self, fd):
        raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr(mechanism, "downgrade", fails)
    writer = FileLock(path, exclusive=True).acquire()
    reader = FileLock(path).acquire()
    writer.release()                    # (does not raise)
    try:
        assert entry_of(reader).mode == "exclusive"
        assert not other_process_gets(path, exclusive=False)
    finally:
        reader.release()
    assert other_process_gets(path, exclusive=False)


def test_no_filesystem_call_under_the_registry_lock(spellings, monkeypatch):
    ds, other = spellings
    calls = []

    def guard(name, fn):
        def wrapper(*args, **kwargs):
            calls.append(name)
            assert not locks._registry_lock.locked(), name
            return fn(*args, **kwargs)
        return wrapper

    for name in ("open", "close", "stat", "fstat", "utime", "mkdir"):
        monkeypatch.setattr(os, name, guard(name, getattr(os, name)))
    monkeypatch.setattr(os.path, "realpath",
                        guard("realpath", os.path.realpath))
    for spelling in (ds, other, ds):
        with FileLock(spelling / "x.nii"):
            with FileLock(other / "x.nii"):
                pass
        with FileLock(spelling / "y.nii", exclusive=True):
            pass
    assert {"open", "close", "fstat", "realpath"} <= set(calls)


# ----------------------------------------------------------------------
#   Across processes
# ----------------------------------------------------------------------

OLD_CLIENT = """
import os, sys, time
path, mode = sys.argv[1], sys.argv[2]
fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0))
if os.name == "nt":     # (fasteners: LockFileEx on byte 0)
    import ctypes, msvcrt
    class OV(ctypes.Structure):
        _fields_ = [("a", ctypes.c_void_p), ("b", ctypes.c_void_p),
                    ("c", ctypes.c_uint32), ("d", ctypes.c_uint32),
                    ("e", ctypes.c_void_p)]
    k = ctypes.WinDLL("kernel32", use_last_error=True)
    ok = k.LockFileEx(ctypes.c_void_p(msvcrt.get_osfhandle(fd)),
                      0x1 | 0x2, 0, 1, 0, ctypes.byref(OV()))
else:                   # (earlier brainspresso: lockf on the whole file)
    import fcntl
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ok = True
    except OSError:
        ok = False
print("locked" if ok else "busy", flush=True)
if mode == "hold":
    sys.stdin.read()
"""


def old_client(lockfile, mode):
    return subprocess.Popen([sys.executable, "-c", OLD_CLIENT,
                             str(lockfile), mode],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            text=True)


def test_earlier_versions_exclude_and_are_excluded(tmp_path):
    path = dataset(tmp_path / "DS") / "x.nii"
    lockfile = lock_file(path)[0]
    lockfile.parent.mkdir(parents=True)
    # an earlier client holds the lock: this version waits
    proc = old_client(lockfile, "hold")
    try:
        assert proc.stdout.readline().strip() == "locked"
        with pytest.raises(LockTimeout):
            FileLock(path, timeout=0.3).acquire()
    finally:
        proc.stdin.close()
        proc.wait(SOON)
    # this version holds the lock: an earlier client cannot take it
    with FileLock(path):
        proc = old_client(lockfile, "try")
        assert proc.stdout.readline().strip() == "busy"
        proc.wait(SOON)


def forked_child(path, parent_holds: bool) -> int:
    """In a forked child: 0 if it behaves (see the test)."""
    if locks._entries():
        return 1
    try:
        with FileLock(path, exclusive=True, timeout=0.3):
            return 2 if parent_holds else 0
    except LockTimeout:
        return 0 if parent_holds else 3


@pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_forked_child(tmp_path):
    path = dataset(tmp_path / "DS") / "x.nii"

    def fork(parent_holds):
        pid = os.fork()
        if pid == 0:                    # pragma: no cover (child)
            os._exit(forked_child(path, parent_holds))
        return os.waitstatus_to_exitcode(os.waitpid(pid, 0)[1])

    with FileLock(path, exclusive=True):
        # an empty registry; the parent's lock excludes the child
        assert fork(parent_holds=True) == 0
        # and survives the child's exit
        assert not other_process_gets(path)
    # the child can lock once the parent released
    assert fork(parent_holds=False) == 0


CRASH = """
import os, sys
from brainspresso.actions.locks import FileLock
FileLock(sys.argv[1], exclusive=True).acquire()
print("locked", flush=True)
sys.stdin.read()
"""


def test_crashed_holder_frees_the_lock(tmp_path):
    path = dataset(tmp_path / "DS") / "x.nii"
    proc = subprocess.Popen([sys.executable, "-c", CRASH, str(path)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            text=True, cwd=REPO)
    assert proc.stdout.readline().strip() == "locked"
    with pytest.raises(LockTimeout):
        FileLock(path, timeout=0.2).acquire()
    proc.kill()
    proc.wait(SOON)
    with FileLock(path, exclusive=True, timeout=SOON) as lock:
        assert lock.locked


# ----------------------------------------------------------------------
#   Platforms
# ----------------------------------------------------------------------

def test_signature_after_byte_0(tmp_path):
    path = dataset(tmp_path / "DS") / "x.nii"
    proc = subprocess.Popen([sys.executable, "-c", CRASH, str(path)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            text=True, cwd=REPO)
    try:
        assert proc.stdout.readline().strip() == "locked"
        lockfile = lock_file(path)[0]
        with open(lockfile, "rb") as f:
            f.seek(1)                   # (byte 0 is locked on Windows)
            assert f.read().decode().strip().startswith(f"pid {proc.pid}")
        with pytest.raises(LockTimeout, match=f"pid {proc.pid}"):
            FileLock(path, timeout=0.2).acquire()
        if WINDOWS:     # (a held lock file cannot be deleted)
            with pytest.raises(PermissionError):
                os.unlink(lockfile)
    finally:
        proc.stdin.close()
        proc.wait(SOON)


@pytest.mark.skipif(fcntl is None, reason="no flock on Windows")
def test_flock_and_lockf_of_two_processes(tmp_path):
    """Informative (printed): they conflict on macOS, not on Linux."""
    a = conflicts_with_child(tmp_path, "flock", locks._ClassicLock())
    b = conflicts_with_child(tmp_path, "lockf", locks._FlockLock())
    print(f"{sys.platform}: child flock excludes lockf: {a}; "
          f"child lockf excludes flock: {b}")


def stress_worker(root, spellings, files, rounds, seed, errors):
    locks._real = lambda p: Path(os.path.abspath(p))
    rng = random.Random(seed)

    def thread(k):
        for i in range(rounds):
            name = rng.choice(files)
            base = Path(rng.choice(spellings))
            data = base / name
            try:
                if rng.random() < 0.4:
                    with FileLock(data, exclusive=True, timeout=SOON):
                        lines = data.read_text().split()
                        n = int(lines[0]) + 1
                        with open(data, "w") as f:  # (not atomic)
                            f.write(f"{n}\n")
                            f.flush()
                            time.sleep(0.001)
                            f.write(f"{n}\n")
                        with FileLock(base / f"{name}.count",
                                      exclusive=True, timeout=SOON):
                            c = base / f"{name}.count"
                            c.write_text(str(int(c.read_text()) + 1))
                else:
                    with FileLock(data, timeout=SOON):
                        lines = data.read_text().split()
                        if len(lines) != 2 or lines[0] != lines[1]:
                            errors.put(f"torn read of {name}: {lines}")
            except Exception as e:
                errors.put(f"{type(e).__name__}: {e}")

    threads = [threading.Thread(target=thread, args=(k,)) for k in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def test_stress_processes_threads_spellings(spellings):
    ds, other = spellings
    files = ["a.txt", "b.txt", "c.txt"]
    for name in files:
        (ds / name).write_text("0\n0\n")
        (ds / f"{name}.count").write_text("0")
    errors = SPAWN.Queue()
    procs = [SPAWN.Process(target=stress_worker, args=(
        str(ds), [str(ds), str(other)], files, 30, seed, errors))
        for seed in range(4)]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(SOON * 3)
        assert proc.exitcode == 0
    found = []
    while not errors.empty():
        found.append(errors.get())
    assert not found, found[:5]
    for name in files:
        n = int((ds / name).read_text().split()[0])
        assert n == int((ds / f"{name}.count").read_text())
