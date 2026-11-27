"""
Probes of the OS lock primitives of `actions.locks` on this platform.

Used by the tests (`test_locks_identity.py`), and run by CI on each OS
(`python tests/lock_probe.py`) to report what was verified there:

* whether a second descriptor of one file, in one process, conflicts
  with the first (per-descriptor locks: Linux OFD, Windows `LockFileEx`;
  not classic `fcntl` locks, which belong to the process);
* Linux: the layout of `struct flock` used for `F_OFD_SETLK`, read back
  with `F_OFD_GETLK` (whose `l_pid` is -1 for an OFD lock);
* Linux: OFD and classic `lockf` locks of two processes conflict (one
  lock space: older brainspresso versions and this one exclude each
  other);
* POSIX: whether `flock` and `lockf` locks of two processes conflict
  (informative: they do on BSD/macOS, not on local Linux filesystems).
"""
import json
import os
import platform
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    import fcntl
except ImportError:                     # (Windows)
    fcntl = None

from brainspresso.actions import locks

# A child that holds a lock on a file until its stdin closes
HOLD = """
import sys, time
path, kind = sys.argv[1], sys.argv[2]
fd = __import__("os").open(path, __import__("os").O_RDWR)
if kind == "lockf":
    import fcntl
    fcntl.lockf(fd, fcntl.LOCK_EX)
elif kind == "flock":
    import fcntl
    fcntl.flock(fd, fcntl.LOCK_EX)
elif kind == "ofd":
    import fcntl, struct
    data = struct.pack("hhqqi", fcntl.F_WRLCK, 0, 0, 0, 0)
    data += bytes(struct.calcsize("hhqqi0q") - len(data))
    fcntl.fcntl(fd, getattr(fcntl, "F_OFD_SETLK", 37), data)
print("ready", flush=True)
sys.stdin.read()
"""


def _held_by_child(path, kind):
    proc = subprocess.Popen([sys.executable, "-c", HOLD, str(path), kind],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            text=True)
    assert proc.stdout.readline().strip() == "ready"
    return proc


def _release(proc):
    proc.stdin.close()
    proc.wait(30)


def _new_file(folder) -> Path:
    path = Path(folder) / f"probe-{time.monotonic_ns()}.lock"
    path.write_bytes(b"")
    return path


def second_descriptor_conflicts(folder) -> bool:
    """Whether the first mechanism excludes a second descriptor."""
    path = _new_file(folder)
    mechanism = locks._MECHANISMS[0]
    fd1 = os.open(path, os.O_RDWR | getattr(os, "O_BINARY", 0))
    fd2 = os.open(path, os.O_RDWR | getattr(os, "O_BINARY", 0))
    try:
        assert mechanism.try_lock(fd1, True)
        conflicts = not mechanism.try_lock(fd2, True)
        if not conflicts:
            mechanism.unlock(fd2)
        mechanism.unlock(fd1)
        return conflicts
    finally:
        os.close(fd2)
        os.close(fd1)


def ofd_getlk(folder) -> dict:
    """Linux: an OFD write lock as `F_OFD_GETLK` reads it back."""
    path = _new_file(folder)
    ofd = locks._OFDLock()
    fd1 = os.open(path, os.O_RDWR)
    fd2 = os.open(path, os.O_RDWR)
    try:
        assert ofd.try_lock(fd1, True)
        query = ofd.flock(fcntl.F_WRLCK)
        answer = fcntl.fcntl(fd2, ofd.command(get=True), query)
        l_type, l_whence, l_start, l_len, l_pid = struct.unpack(
            locks._FLOCK, answer[:struct.calcsize(locks._FLOCK)])
        ofd.unlock(fd1)
        return {"l_type": l_type, "l_whence": l_whence, "l_start": l_start,
                "l_len": l_len, "l_pid": l_pid, "size": len(answer),
                "F_WRLCK": fcntl.F_WRLCK}
    finally:
        os.close(fd2)
        os.close(fd1)


def conflicts_with_child(folder, child: str, mine) -> bool:
    """Whether a lock of a child (`child` primitive) excludes `mine`."""
    path = _new_file(folder)
    proc = _held_by_child(path, child)
    fd = os.open(path, os.O_RDWR)
    try:
        got = mine.try_lock(fd, True)
        if got:
            mine.unlock(fd)
        return not got
    finally:
        os.close(fd)
        _release(proc)


def report(folder) -> dict:
    """What was verified on this platform."""
    out = {
        "platform": sys.platform, "machine": platform.machine(),
        "python": platform.python_version(),
        "mechanisms": [m.name for m in locks._MECHANISMS],
        "second_descriptor_conflicts": second_descriptor_conflicts(folder),
    }
    if sys.platform.startswith("linux"):
        out["F_OFD_SETLK_named"] = hasattr(fcntl, "F_OFD_SETLK")
        out["ofd_getlk"] = ofd_getlk(folder)
        out["child_lockf_excludes_ofd"] = conflicts_with_child(
            folder, "lockf", locks._OFDLock())
        out["child_ofd_excludes_lockf"] = conflicts_with_child(
            folder, "ofd", locks._ClassicLock())
    if fcntl is not None:
        out["child_flock_excludes_lockf"] = conflicts_with_child(
            folder, "flock", locks._ClassicLock())
        out["child_lockf_excludes_flock"] = conflicts_with_child(
            folder, "lockf", locks._FlockLock())
    return out


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        print(json.dumps(report(tmp), indent=2))
