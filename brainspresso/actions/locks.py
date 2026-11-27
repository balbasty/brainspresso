"""
Inter-process reader/writer locks on the files that actions read and write.

A file is locked through a separate **lock file**, never through the file
itself, and lock files are never deleted by the processes that hold them
(deleting a lock file that another process is waiting on lets two
processes "hold" it at once):

* inside a dataset (the nearest ancestor folder that has a
  `.brainspresso/` state folder, `is_state_folder`: one that holds
  lock files, a manifest, run records..., not only a config file), the
  lock file is
  `<DS>/.brainspresso/locks/<h[:2]>/<h>.lock`, with `h` the SHA-256 of
  the path of the file relative to `<DS>`. It is on the filesystem of
  the dataset, so it also protects processes on other machines that
  share it (cluster nodes), wherever the filesystem shares the locks
  (see "Platforms" below), and whatever path the dataset is mounted at.
* outside any dataset (package templates, a source folder given on the
  command line, files written by legacy code), the lock file is in a
  private per-user folder on the local machine
  (`$BRAINSPRESSO_LOCK_DIR`, else `$XDG_RUNTIME_DIR/brainspresso/locks`,
  else `<tmp>/brainspresso-<uid>/locks`, `<tmp>` being the per-user
  temporary folder on Windows and macOS), with `h` the SHA-256 of the
  resolved absolute path. It protects the processes of one user on one
  machine only (other users and other machines use other folders).

The path is NFC-normalised and casefolded before it is hashed, on every
platform: Windows and macOS filesystems ignore case (and macOS Unicode
normalisation) by default, so two spellings of one file must share its
lock, and Linux, macOS and Windows clients of one shared dataset must
agree on it. Two files whose names differ only by case, on a
case-sensitive filesystem, share one lock file too: they exclude each
other needlessly, which is safe. (Lock files of earlier versions were
named from the path as given: during an upgrade, runs of an older and
a newer version do not exclude each other.)

Nothing is ever created next to the file: a source may be shared or
read-only.

The lock file of a file therefore moves when a `.brainspresso/` folder
appears in one of its ancestors: two processes that lock the same file
before and after it appears do not exclude each other. So every command
that writes into a dataset creates its state folder *before* it takes
any lock (`bids.layout.Layout.ensure_state`, called by `record_run`
before the downloads of every harvest command, by `bids.harvest.harvest`
and `run_roast` before anything is read or written, by `compost` and by
the legacy roasters (`Bidsifier` classes) before their actions run).

The dataset of a folder is cached once found (`find_dataset`; folders
outside any dataset are looked up each time, so a state folder created
later is seen). `forget_datasets` (called by `Layout.ensure_state` when
it creates a state folder) drops the cache, for a state folder created
inside a dataset that is already cached (a site of a "sites" layout).

A **reader** holds a *shared* lock (any number of readers at once), a
**writer** an *exclusive* lock (no reader and no other writer).
Acquisition waits, polling with a growing delay, up to a timeout
(`DEFAULT_TIMEOUT`, or `$BRAINSPRESSO_LOCK_TIMEOUT` in seconds, or the
`timeout` argument), then raises `LockTimeout`, whose message names the
file and what holds it (readers or a writer, with the process id and
host of a writer, which it writes in the lock file once it holds it).

Design: two layers
------------------
Under-exclusion needs both layers to fail.

**Layer A, the registry of a process** (`_Entry`): one entry per lock
file, which owns its descriptor for its whole life (never reopened or
repaired), and arbitrates the holders of the process (nested readers of
an archive and its members, threads; a lock upgrade or a nested writer
of one file raises `LockError` at once instead of deadlocking).

* Entries are found by path (`str(lockfile)`, as given: not casefolded,
  which would merge `/d/DS` and `/d/ds` on a case-sensitive
  filesystem) and by identity, `(st_dev, st_ino, alias key)` from
  `fstat` of a descriptor already held (never from `stat` of a path),
  the alias key being the resolved path, NFC-normalised and casefolded.
  So two spellings of one lock file share one entry: a symbolic link
  resolves through `realpath`, a case alias (macOS, Windows) through
  the casefolding; two distinct files never merge, even with equal
  (st_dev, st_ino) (two datasets with the same relative path have lock
  files of one name). No identity where `st_ino` is 0.
* A descriptor opened through another spelling is parked in the entry
  (`spares`), never closed while the entry lives (closing it would drop
  the process's classic POSIX locks). The lookup by identity comes
  first: a live entry of the file wins over a retired entry of the same
  path (a lock file replaced meanwhile), and a new descriptor is closed
  only against a retired entry, which holds no OS lock, and when no live
  entry has its file.
* Opening, creating, `fstat`, `stat` and closing happen outside the
  registry lock (a leaf lock: dictionaries and counters only).
* The last holder retires the entry, closes its descriptors, then
  removes its keys (each one only if it still names this entry) and
  sets its `gone` event: a thread that finds a retired entry waits for
  it, so no new entry of that file locks before the old one closed.
* Lock, then verify (POSIX): after the OS lock is taken, the path must
  still name the entry's file (`stat`); if it was deleted or replaced,
  the entry is unlocked, retired, and the lock is taken again (at most
  `_ROUNDS` times, then accepted with a warning). Other `stat` errors
  (EIO...) keep the lock.

**Layer B, the OS primitive** (`_MECHANISMS`, a chain: the next one is
used if a primitive is refused with EINVAL, ENOTSUP or ENOSYS):

* Linux: open file description locks (`F_OFD_SETLK`, 37 where `fcntl`
  does not name it), then classic `lockf`. OFD locks belong to the
  descriptor and share the lock space of classic `fcntl` locks: they
  exclude earlier brainspresso versions and are excluded by them, and
  two entries of one file (an identity split) still exclude each other.
* macOS: classic `fcntl` (`lockf`) locks, then `flock` where `fcntl`
  locks are refused (a `flock` lock is not downgraded: it stays
  exclusive until the last reader leaves).
* Windows: `LockFileEx` on byte 0 of the lock file (through `ctypes`),
  owned by the handle and mandatory on that byte: the signature of a
  writer follows it, at byte 1. A downgrade stacks a shared lock on the
  exclusive one, then one unlock removes the exclusive lock. No
  verification: an open lock file cannot be deleted there.

A writer released inside readers of its thread downgrades the OS lock
without a gap where the primitive can (OFD and `lockf` convert it in
place); otherwise, or if the downgrade fails, the lock stays exclusive
until the last reader leaves (over-exclusion; `release` does not raise
for it). The OS releases the locks of a process that dies (Windows:
possibly after a short delay); a forked child closes its copies of the
descriptors without unlocking (OFD locks are shared with the child
until then).

Platforms and filesystems
-------------------------
Lock files keep their names and locations, and mixed runs of earlier
commits and this one exclude each other on all three platforms (same
lock space; same byte on Windows; whole file on POSIX).

* Linux local filesystems (ext4, xfs, btrfs): OFD locks.
* Linux NFS: OFD (else classic) locks are shared between clients with
  NFSv4, and NFSv3 with `lockd` (not `nolock` or `local_lock=all`
  mounts; a crashed client's lock stays until the lock manager notices).
  Lustre shares them with `-o flock` (`localflock`: node-local;
  `noflock`: `LockError`); GPFS, Ceph, GFS2: filesystem-dependent; CIFS:
  shared unless mounted `nobrl`; FUSE: often node-local. None of these
  is verified; run records (`actions.runlock`) are the second line of
  defence of compost.
* macOS (APFS, HFS+): classic locks, per process; case aliases merge in
  layer A. macOS NFS: shared; smbfs: not verified.
* Windows (NTFS, SMB shares): `LockFileEx`, enforced by an SMB server
  between clients.

Disabling locks
---------------
Locks are disabled only explicitly: `$BRAINSPRESSO_LOCKS` set to "0",
"no", "off" or "false" (an escape hatch for filesystems without lock
support; unsafe with concurrent writers). It disables every lock: the
run lock, the locks of files, and also the lock of each partial
download (`download.incomplete`, a `FileLock` since fasteners was
dropped), so two downloads of one file could then write it at once.
A platform with neither `fcntl` nor the Windows API raises `LockError`
instead of running unlocked.

A reader whose lock file can neither be created nor opened (e.g. a
dataset whose `.brainspresso/` is read-only for this user) reads without
a lock (a debug message says so). A writer whose lock file cannot be
created, and any holder on a filesystem that refuses locks (`ENOLCK`,
e.g. NFS without a lock manager), raise `LockError` instead of writing
unprotected.

Limitations
-----------
* Between processes, cycles (A reads X and waits to write Y while B
  reads Y and waits to write X) are not detected: they end with a
  `LockTimeout`. The locks are not fair: a steady stream of readers can
  delay a writer up to its timeout.
* Double failure: a per-process primitive (macOS, Linux before 3.15, a
  filesystem that ignores lock owners) and an identity split (two
  mounts of one export at different mount points in one process, or
  `st_ino` 0) lose the exclusion in the process. Without merged
  aliases, on a per-descriptor primitive, an upgrade through the other
  spelling ends in a `LockTimeout` instead of an immediate `LockError`.
* Two distinct lock files merge (in one process) only with equal
  (st_dev, st_ino) *and* paths that differ only by case, which needs a
  case-sensitive filesystem whose inodes are not unique.
* Replacing a lock file or the state folder while a lock is held splits
  the exclusion between processes (in the process, its holders keep
  excluding each other; the next OS acquisition retires the entry).
  Temporary-file cleaners: each OS acquisition refreshes the times of
  the lock file, but a lock held longer than the cleaning age can still
  be split.
* Lock files accumulate (one per file ever locked). Deleting the
  `locks/` folder is safe only while no brainspresso process uses the
  dataset.
* Lock files are created with mode 0o666 less the umask (POSIX): in a
  dataset shared by several users, every writer needs write access to
  `.brainspresso/locks/` and its files (e.g. umask 002 and a common
  group); a writer that cannot open its lock file for writing raises
  `LockError`.
"""
from __future__ import annotations

import ctypes
import errno
import functools
import hashlib
import os
import socket
import struct
import sys
import tempfile
import threading
import time
import unicodedata
from collections import Counter, OrderedDict
from logging import getLogger
from pathlib import Path

try:
    import fcntl
except ImportError:                     # pragma: no cover (Windows)
    fcntl = None
if os.name == "nt":                     # pragma: no cover (Windows)
    import msvcrt

lg = getLogger(__name__)

__all__ = [
    "DEFAULT_TIMEOUT", "FileLock", "LockError", "LockTimeout",
    "find_dataset", "forget_datasets", "is_state_folder", "lock_file",
    "lock_timeout", "locks_enabled",
]

#: State folder of a dataset (`bids.layout.STATE_DIR`)
STATE_DIR = ".brainspresso"
#: Folder of the lock files, in the state folder of a dataset
LOCKS_DIR = "locks"
#: Entries that make a `.brainspresso/` folder a state folder: lock
#: files, manifest (`bids.layout.MANIFEST`), run lock and records
#: (`actions.runlock`), task states, pristine tables. Anything else (a
#: config file, `config.toml`, with its backup or swap files) does not:
#: `~/.brainspresso/` and `<root>/.brainspresso/` are config folders
STATE_ENTRIES = frozenset({LOCKS_DIR, "manifest.json", "run", "runs",
                           "tasks", "orig"})
#: Folder of the user config file instead of `~/.brainspresso`
#: (`brainspresso.config.HOME_ENV`): never a state folder
CONFIG_HOME_ENV = "BRAINSPRESSO_CONFIG_HOME"
#: Seconds to wait for a lock before `LockTimeout`
DEFAULT_TIMEOUT = 600.0
#: Environment variable that overrides `DEFAULT_TIMEOUT` (seconds)
TIMEOUT_ENV = "BRAINSPRESSO_LOCK_TIMEOUT"
#: Environment variable that sets the folder of locks outside datasets
LOCAL_DIR_ENV = "BRAINSPRESSO_LOCK_DIR"
#: Environment variable that disables locks ("0", "no", "off", "false")
ENABLE_ENV = "BRAINSPRESSO_LOCKS"

# Delays between attempts to take a lock held by another process
_FIRST_DELAY = 0.005
_MAX_DELAY = 0.25
_WINDOWS = os.name == "nt"


class LockError(RuntimeError):
    """A lock that cannot be taken (unsupported, upgrade, unwritable)."""


class LockTimeout(LockError, TimeoutError):
    """
    A lock that could not be taken before the timeout.

    Parameters
    ----------
    path : Path
        File to lock
    lockfile : Path
        Its lock file
    exclusive : bool
        Whether an exclusive (write) lock was requested
    holder : str
        What holds the file ("a writer (pid 12 on node1)", "readers")
    timeout : float
        Seconds waited
    """

    def __init__(self, path, lockfile, exclusive, holder, timeout):
        self.path, self.lockfile = Path(path), Path(lockfile)
        self.exclusive, self.holder = exclusive, holder
        self.timeout = timeout
        want = "write (exclusive)" if exclusive else "read (shared)"
        super().__init__(
            f"Timed out after {timeout:g} s waiting for a {want} lock on "
            f"{path}: it is held by {holder} (lock file: {lockfile})"
        )


def locks_enabled() -> bool:
    """
    Whether files are locked at all.

    Only an explicit `$BRAINSPRESSO_LOCKS` ("0", "no", "off" or "false")
    disables locks: a platform without a lock backend does not (taking a
    lock there raises `LockError`, see `FileLock.acquire`).

    Returns
    -------
    bool
        False only when `$BRAINSPRESSO_LOCKS` disables them
    """
    value = os.environ.get(ENABLE_ENV, "").strip().lower()
    return value not in ("0", "no", "off", "false")


def lock_timeout(timeout: float | None = None) -> float:
    """
    Timeout of lock acquisition.

    Parameters
    ----------
    timeout : float | None
        Seconds; None: `$BRAINSPRESSO_LOCK_TIMEOUT`, else
        `DEFAULT_TIMEOUT`

    Returns
    -------
    float
        Seconds
    """
    if timeout is not None:
        return float(timeout)
    value = os.environ.get(TIMEOUT_ENV)
    if value:
        try:
            return float(value)
        except ValueError:
            lg.warning(f"Invalid ${TIMEOUT_ENV}={value!r}: ignored")
    return DEFAULT_TIMEOUT


def _real(path: str | os.PathLike) -> Path:
    """Absolute path, with the symbolic links of its folder resolved."""
    path = Path(os.path.abspath(path))
    return Path(os.path.realpath(path.parent)) / path.name


def is_state_folder(folder: str | os.PathLike) -> bool:
    """
    Whether a folder is a dataset: it has a state folder.

    A state folder is a `.brainspresso/` folder that holds one of the
    `STATE_ENTRIES` (`Layout.ensure_state` creates `locks/` in it), or
    nothing (one created by an older version, or by hand before a run
    lock: its lock files must not move once they exist). One that holds
    other files only (a config file, its backup or swap files,
    `.DS_Store`: the user's `~`, the root of all datasets) is not, and
    the user config folder (`~/.brainspresso`, or
    `$BRAINSPRESSO_CONFIG_HOME`) never is.

    Not cached: a state folder created by another process is seen at
    once (two processes must never lock one file through two lock
    files).

    Parameters
    ----------
    folder : str | PathLike

    Returns
    -------
    bool
    """
    state = os.path.join(folder, STATE_DIR)
    try:
        names = os.listdir(state)
    except OSError:                 # (none, or not a folder)
        return False
    if names and not any(name in STATE_ENTRIES for name in names):
        return False
    return os.path.realpath(state) != _config_home(
        os.environ.get(CONFIG_HOME_ENV, ""), os.path.expanduser("~"))


@functools.lru_cache(maxsize=8)
def _config_home(env: str, home: str) -> str:
    """Resolve the user config folder (for one value of its variable)."""
    folder = os.path.expanduser(env) if env else os.path.join(
        home, STATE_DIR)
    return os.path.realpath(folder)


def _is_state_dir(folder: Path) -> bool:
    """Whether a folder has a state folder (`is_state_folder`)."""
    return is_state_folder(folder)


# Dataset of each (resolved) folder that holds locked files. Only hits
# are cached: a folder outside any dataset is looked up again each time,
# so a state folder created later is found. A hit can only go stale if a
# *nearer* state folder is created later (a site of a "sites" layout
# under a dataset whose locks were already taken), which
# `forget_datasets` handles in this process (`bids.layout.Layout.
# ensure_state` calls it), or if the state folder is deleted while
# processes use it.
_DATASET_CACHE_SIZE = 4096
_datasets: "OrderedDict[Path, Path]" = OrderedDict()
_datasets_lock = threading.Lock()


_generation = 0


def forget_datasets() -> None:
    """Forget the cached dataset of every folder (`find_dataset`)."""
    global _generation
    with _datasets_lock:
        _datasets.clear()
        _generation += 1


def _dataset_of(folder: Path) -> Path | None:
    """Nearest ancestor-or-self of a resolved folder with a state folder."""
    with _datasets_lock:
        found = _datasets.get(folder)
        if found is not None:
            _datasets.move_to_end(folder)
            return found
        generation = _generation
    for parent in (folder, *folder.parents):
        if _is_state_dir(parent):
            found = parent
            break
    else:
        return None
    with _datasets_lock:
        # (not if `forget_datasets` ran meanwhile: a nearer state folder
        # may have appeared)
        if generation == _generation:
            _datasets[folder] = found
            _datasets.move_to_end(folder)
            while len(_datasets) > _DATASET_CACHE_SIZE:
                _datasets.popitem(last=False)
    return found


def find_dataset(path: str | os.PathLike) -> Path | None:
    """
    Dataset that contains a file.

    The dataset of each folder is cached once found (see
    `forget_datasets`); folders outside any dataset are not cached.

    Parameters
    ----------
    path : str | PathLike
        A file (or folder), which need not exist

    Returns
    -------
    Path | None
        The nearest ancestor of `path` (symbolic links resolved) that has
        a `.brainspresso/` folder; None if there is none
    """
    return _dataset_of(_real(path).parent)


def _local_dir() -> Path:
    """Private folder of the locks of files outside datasets."""
    env = os.environ.get(LOCAL_DIR_ENV)
    if env:
        return Path(env)
    runtime = os.environ.get("XDG_RUNTIME_DIR")
    if runtime and os.path.isdir(runtime):
        return Path(runtime) / "brainspresso" / LOCKS_DIR
    uid = os.getuid() if hasattr(os, "getuid") else "user"
    return Path(tempfile.gettempdir()) / f"brainspresso-{uid}" / LOCKS_DIR


def _private(folder: Path) -> None:
    """Create the folder of local locks, refusing one of another user."""
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not hasattr(os, "getuid") or os.environ.get(LOCAL_DIR_ENV):
        return                      # (a folder chosen by the user)
    for path in (folder.parent, folder):
        if os.path.islink(path) or os.lstat(path).st_uid != os.getuid():
            raise LockError(
                f"{path} (folder of local locks) is not a folder owned by "
                f"this user: set ${LOCAL_DIR_ENV}"
            )


def _normalise(key: str) -> str:
    """
    Lock key of a path: NFC-normalised and casefolded.

    On every platform, so that the clients of one shared dataset (Linux,
    macOS and Windows machines on NFS or SMB) agree on its lock files;
    files whose names differ only by case share one (safe).

    Parameters
    ----------
    key : str
        Path (relative to the dataset, or absolute)

    Returns
    -------
    str
        Its key
    """
    return unicodedata.normalize("NFC", key).casefold()


def lock_file(path: str | os.PathLike) -> tuple[Path, Path | None]:
    """
    Lock file of a file.

    Parameters
    ----------
    path : str | PathLike
        File to lock (need not exist)

    Returns
    -------
    lockfile : Path
        `<DS>/.brainspresso/locks/<h[:2]>/<h>.lock` inside a dataset,
        else a file of the local lock folder (see the module docstring)
    dataset : Path | None
        The dataset that contains `path`, if any
    """
    real = _real(path)
    dataset = _dataset_of(real.parent)
    if dataset is not None:
        key = real.relative_to(dataset).as_posix()
        folder = dataset / STATE_DIR / LOCKS_DIR
    else:
        key = str(real)
        folder = _local_dir()
    digest = hashlib.sha256(_normalise(key).encode()).hexdigest()
    return folder / digest[:2] / f"{digest}.lock", dataset


# ----------------------------------------------------------------------
#   Layer B: the OS lock of a lock-file descriptor
# ----------------------------------------------------------------------

# errno of a lock held by another holder
_BUSY = (errno.EACCES, errno.EAGAIN)
# errno of a primitive that this filesystem or kernel does not offer:
# the next mechanism of the chain is used
_UNSUPPORTED = tuple({
    errno.EINVAL, errno.ENOSYS,
    getattr(errno, "ENOTSUP", errno.EINVAL),
    getattr(errno, "EOPNOTSUPP", errno.EINVAL),
})
# `struct flock` of Linux (`F_OFD_SETLK`): l_type, l_whence, l_start,
# l_len, l_pid, padded to its alignment (checked by the tests: F_OFD_GETLK
# reads it back)
_FLOCK = "hhqqi"
_FLOCK_SIZE = struct.calcsize(_FLOCK + "0q")
# Offset of the signature of a writer: byte 0 holds the lock on Windows,
# where it cannot be read while it is locked exclusively; the signature
# is written after a newline everywhere, and read from byte 1 there
_SIGNATURE_AT = 1 if _WINDOWS else 0


def _describe(error: OSError) -> str:
    """Text of an OS error."""
    return error.strerror or str(error) or type(error).__name__


class _Mechanism:
    """
    A non-blocking shared/exclusive lock primitive on a descriptor.

    `try_lock` returns False if another holder has the lock (and raises
    `OSError` if the primitive cannot be used); `downgrade` turns an
    exclusive lock into a shared one without a gap, or returns False.
    """

    name = "?"

    def try_lock(self, fd: int, exclusive: bool) -> bool:
        """Take the lock if it is free (False: held by another)."""
        raise NotImplementedError

    def unlock(self, fd: int) -> None:
        """Release the lock."""
        raise NotImplementedError

    def downgrade(self, fd: int) -> bool:
        """Exclusive to shared without a gap (False: not supported)."""
        return False


def _busy(error: OSError) -> bool:
    return error.errno in _BUSY


class _OFDLock(_Mechanism):
    """
    Linux open file description locks (`F_OFD_SETLK`).

    Owned by the open file description (a descriptor here), and in the
    lock space of classic `fcntl` locks: they conflict with those of
    other processes (older brainspresso versions included) and with
    those of other descriptors of this process. Over the whole file.
    """

    name = "ofd"

    @staticmethod
    def command(get: bool = False) -> int:
        """`F_OFD_SETLK` (37) or `F_OFD_GETLK` (36), also if not named."""
        if get:
            return getattr(fcntl, "F_OFD_GETLK", 36)
        return getattr(fcntl, "F_OFD_SETLK", 37)

    @staticmethod
    def flock(kind: int) -> bytes:
        """Pack a `struct flock` over the whole file (l_pid 0, for OFD)."""
        data = struct.pack(_FLOCK, kind, os.SEEK_SET, 0, 0, 0)
        return data + bytes(_FLOCK_SIZE - len(data))

    def _set(self, fd: int, kind: int) -> None:
        fcntl.fcntl(fd, self.command(), self.flock(kind))

    def try_lock(self, fd: int, exclusive: bool) -> bool:
        """Take the lock if it is free (False: held by another)."""
        try:
            self._set(fd, fcntl.F_WRLCK if exclusive else fcntl.F_RDLCK)
        except OSError as e:
            if _busy(e):
                return False
            raise
        return True

    def unlock(self, fd: int) -> None:
        """Release the lock."""
        self._set(fd, fcntl.F_UNLCK)

    def downgrade(self, fd: int) -> bool:
        """Exclusive to shared: the kernel converts the lock in place."""
        try:
            self._set(fd, fcntl.F_RDLCK)
        except OSError:
            return False
        return True


class _ClassicLock(_Mechanism):
    """
    Classic POSIX record locks (`fcntl.lockf`) over the whole file.

    Owned by the process: closing *any* descriptor of the file drops
    them all (hence one entry per lock file, and parked spares).
    """

    name = "fcntl"

    def try_lock(self, fd: int, exclusive: bool) -> bool:
        """Take the lock if it is free (False: held by another)."""
        flag = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        try:
            fcntl.lockf(fd, flag | fcntl.LOCK_NB)
        except OSError as e:
            if _busy(e):
                return False
            raise
        return True

    def unlock(self, fd: int) -> None:
        """Release the lock."""
        fcntl.lockf(fd, fcntl.LOCK_UN)

    def downgrade(self, fd: int) -> bool:
        """Exclusive to shared: `lockf` converts the lock in place."""
        try:
            fcntl.lockf(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError:
            return False
        return True


class _FlockLock(_Mechanism):
    """
    BSD `flock` locks: only where `fcntl` locks are refused (macOS).

    A downgrade is not atomic with `flock`: the lock stays exclusive
    until the last reader of the process leaves.
    """

    name = "flock"

    def try_lock(self, fd: int, exclusive: bool) -> bool:
        """Take the lock if it is free (False: held by another)."""
        flag = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        try:
            fcntl.flock(fd, flag | fcntl.LOCK_NB)
        except OSError as e:
            if e.errno in (*_BUSY, getattr(errno, "EWOULDBLOCK", 0)):
                return False
            raise
        return True

    def unlock(self, fd: int) -> None:
        """Release the lock."""
        fcntl.flock(fd, fcntl.LOCK_UN)


class _OVERLAPPED(ctypes.Structure):
    _fields_ = [("Internal", ctypes.c_void_p),
                ("InternalHigh", ctypes.c_void_p),
                ("Offset", ctypes.c_uint32),
                ("OffsetHigh", ctypes.c_uint32),
                ("hEvent", ctypes.c_void_p)]


class _WindowsLock(_Mechanism):           # pragma: no cover (Windows)
    """
    `LockFileEx` on byte 0 of the lock file (through `ctypes`).

    Owned by the file handle, mandatory on that byte. A downgrade stacks
    a shared lock on the exclusive one (same handle), then one unlock
    removes the exclusive lock.
    """

    name = "LockFileEx"
    _FAIL_IMMEDIATELY, _EXCLUSIVE = 0x1, 0x2
    _ERROR_LOCK_VIOLATION = 33

    def __init__(self):
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle, dword = ctypes.c_void_p, ctypes.c_uint32
        overlapped = ctypes.POINTER(_OVERLAPPED)
        self._lock = kernel32.LockFileEx
        self._lock.argtypes = [handle, dword, dword, dword, dword,
                               overlapped]
        self._lock.restype = ctypes.c_int
        self._unlock = kernel32.UnlockFileEx
        self._unlock.argtypes = [handle, dword, dword, dword, overlapped]
        self._unlock.restype = ctypes.c_int

    def try_lock(self, fd: int, exclusive: bool) -> bool:
        """Take the lock if it is free (False: held by another)."""
        flags = self._FAIL_IMMEDIATELY | (self._EXCLUSIVE if exclusive
                                          else 0)
        overlapped = _OVERLAPPED()
        if self._lock(msvcrt.get_osfhandle(fd), flags, 0, 1, 0,
                      ctypes.byref(overlapped)):
            return True
        code = ctypes.get_last_error()
        if code == self._ERROR_LOCK_VIOLATION:
            return False
        raise ctypes.WinError(code)

    def unlock(self, fd: int) -> None:
        """Release the lock (the exclusive one first, if both)."""
        overlapped = _OVERLAPPED()
        if not self._unlock(msvcrt.get_osfhandle(fd), 0, 1, 0,
                            ctypes.byref(overlapped)):
            raise ctypes.WinError(ctypes.get_last_error())

    def downgrade(self, fd: int) -> bool:
        """Stack a shared lock, then remove the exclusive one."""
        try:
            if not self.try_lock(fd, False):
                return False
            self.unlock(fd)
        except OSError:
            return False
        return True


def _default_mechanisms() -> tuple[_Mechanism, ...]:
    """Return the chain of lock primitives of this platform."""
    if _WINDOWS:                            # pragma: no cover (Windows)
        return (_WindowsLock(),)
    if fcntl is None:                       # pragma: no cover
        return ()
    if sys.platform.startswith("linux"):
        return (_OFDLock(), _ClassicLock())
    return (_ClassicLock(), _FlockLock())


#: Lock primitives, tried in this order (`_Entry.mech` indexes it)
_MECHANISMS: tuple[_Mechanism, ...] = _default_mechanisms()


def _check_backend() -> None:
    """Raise `LockError` if no OS lock can be taken on this platform."""
    if not _MECHANISMS:
        raise LockError(
            f"No file lock backend: this platform has neither `fcntl` "
            f"nor the Windows API. Set ${ENABLE_ENV}=0 to run without "
            f"locks (unsafe with concurrent writers)."
        )


# ----------------------------------------------------------------------
#   Layer A: one entry per lock file in a process
# ----------------------------------------------------------------------

def _file_id(fd: int) -> tuple[int, int]:
    """(st_dev, st_ino) of an open descriptor."""
    st = os.fstat(fd)
    return st.st_dev, st.st_ino


def _path_id(path: str) -> tuple[int, int]:
    """(st_dev, st_ino) of a path (raises OSError)."""
    st = os.stat(path)
    return st.st_dev, st.st_ino


def _alias_key(path: str) -> str:
    """Return the resolved path, NFC-normalised and casefolded."""
    return unicodedata.normalize("NFC", os.path.realpath(path)).casefold()


def _ident(fd: int, path: str) -> tuple | None:
    """
    Identity of an open lock file: (st_dev, st_ino, alias key).

    None where the filesystem gives no inode (`st_ino` 0). The alias key
    makes two distinct files with equal (st_dev, st_ino) distinct, unless
    their paths differ only by case (see the module docstring).
    """
    dev, ino = _file_id(fd)
    if not ino:
        return None
    return dev, ino, _alias_key(path)


def _open_lockfile(path: Path) -> tuple[int, bool]:
    """
    Open (and create) a lock file: (descriptor, writable).

    Read-only where it cannot be written (enough for a reader). Its
    folder is created first; a folder removed meanwhile is retried.

    Raises
    ------
    OSError
        If it can be neither created nor opened
    """
    flags = getattr(os, "O_BINARY", 0)
    for attempt in range(3):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            return os.open(path, os.O_RDWR | os.O_CREAT | flags, 0o666), True
        except FileNotFoundError:
            if attempt == 2:
                raise
        except OSError:
            return os.open(path, os.O_RDONLY | flags), False
    raise AssertionError("not reached")      # pragma: no cover


def _touch(fd: int) -> None:
    """Refresh the times of a lock file (temporary-file cleaners)."""
    if os.utime in os.supports_fd:
        try:
            os.utime(fd)
        except OSError:
            pass


class _Entry:
    """
    A lock file opened by this process, and its holders in the process.

    The entry owns its descriptor for its whole life (never reopened):
    descriptors of the same file opened through other spellings are
    parked in `spares`, and all are closed only once the entry is
    retired and has no user. The OS lock (`mode`) is shared while only
    readers hold it, exclusive while a writer does; `busy` while a
    thread waits for the OS lock.
    """

    def __init__(self, fd: int, writable: bool, ident: tuple | None,
                 path: str):
        self.fd, self.writable, self.ident = fd, writable, ident
        self.lockfile = Path(path)
        self.paths = {path}
        self.spares: list[int] = []
        self.mech = 0                       # in `_MECHANISMS`
        self.cond = threading.Condition()
        self.mode: str | None = None        # None | "shared" | "exclusive"
        self.readers: Counter = Counter()   # thread -> number of holds
        self.writer: int | None = None      # thread
        self.busy = False
        self.users = 0                      # holders and waiters
        self.retired = False
        self.gone = threading.Event()       # descriptors closed

    def compatible(self, exclusive: bool, me: int) -> bool:
        """Whether this thread may take the lock, given the others."""
        if self.busy:
            return False
        if exclusive:
            return self.writer is None and not +self.readers
        return self.writer is None or self.writer == me

    def local_holder(self) -> str:
        """Describe what holds the lock in this process."""
        if self.writer is not None:
            return f"a writer in this process (thread {self.writer})"
        if +self.readers:
            return "readers in this process"
        return "another thread of this process, waiting for it"


# The registry (layer A), under `_registry_lock` (R): a leaf lock, under
# which only dictionaries and counters change (never a filesystem call,
# never another lock); `_Entry.cond` may take it, never the reverse.
_by_path: dict[str, _Entry] = {}        # str(lockfile) -> entry
_by_id: dict[tuple, _Entry] = {}        # `_ident` -> entry
_registry_lock = threading.Lock()
# Descriptors whose `fstat` failed: never closed (a live entry may hold
# their file, and closing one drops its classic POSIX locks)
_leaked: list[int] = []
# Rounds of lock-then-verify before a lock is accepted as it is
_ROUNDS = 3
# Seconds that a thread waits for a retired entry to close its
# descriptors (in-process, short), even past a zero timeout
_GONE_GRACE = 10.0
_RETIRED = object()


def _entries() -> set[_Entry]:
    """Entries of the registry (tests)."""
    with _registry_lock:
        return {*_by_path.values(), *_by_id.values()}


def _after_fork() -> None:              # pragma: no cover
    # The child does not hold the locks of its parent: close its copies
    # of the descriptors *without unlocking* (an unlock would release
    # the OFD locks that it shares with the parent), and forget them
    global _registry_lock
    for entry in {*_by_path.values(), *_by_id.values()}:
        for fd in (entry.fd, *entry.spares):
            try:
                os.close(fd)
            except OSError:
                pass
    _by_path.clear()
    _by_id.clear()
    _registry_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


class FileLock:
    """
    Shared (reader) or exclusive (writer) lock on a file.

    ```python
    with FileLock(path):                     # shared
        data = Path(path).read_bytes()
    with FileLock(path, exclusive=True):     # exclusive
        ...
    ```

    Parameters
    ----------
    path : str | PathLike
        File to lock (need not exist)
    exclusive : bool
        Exclusive (writer) lock instead of a shared (reader) one
    timeout : float | None
        Seconds to wait (default: `lock_timeout()`)

    Attributes
    ----------
    lockfile : Path | None
        Lock file (None if locks are disabled)
    locked : bool
        Whether the lock is held (False for a reader that could not
        create its lock file, or with locks disabled)

    Notes
    -----
    `timeout` bounds the wait for other holders, in this process and in
    others. A thread that finds the entry of its lock file retired (its
    last holder is closing its descriptors) waits for that close, at
    least `_GONE_GRACE` seconds (10), even with `timeout=0`: a new
    descriptor of the file may only lock once the old one is closed
    (invariant I2), and that close is local and short, not contention;
    a zero timeout (`run_lock(wait=0)`, fail fast) must not fail
    because another thread of the process is releasing the same file.
    Only if the close takes longer does it raise `LockTimeout`.
    """

    def __init__(
        self,
        path: str | os.PathLike,
        exclusive: bool = False,
        timeout: float | None = None,
    ):
        self.path = Path(path)
        self.exclusive = exclusive
        self.timeout = timeout
        self.lockfile: Path | None = None
        self.locked = False
        self._entry: _Entry | None = None
        self._thread: int | None = None

    @property
    def kind(self) -> str:
        """Kind of holder: "writer" or "reader"."""
        return "writer" if self.exclusive else "reader"

    def acquire(self) -> "FileLock":
        """
        Take the lock, waiting up to the timeout.

        Returns
        -------
        FileLock
            self

        Raises
        ------
        LockTimeout
            If the lock is still held by others after the timeout
        LockError
            If the lock cannot be taken at all (see the module docstring),
            or if no lock backend is available while locks are enabled
        """
        if self._entry is not None:
            raise LockError(f"The {self.kind} lock of {self.path} is "
                            f"already held by this object")
        if not locks_enabled():
            return self
        _check_backend()
        timeout = lock_timeout(self.timeout)
        deadline = time.monotonic() + timeout
        lockfile, dataset = lock_file(self.path)
        if dataset is None:
            try:
                _private(_local_dir())
            except OSError as e:
                return self._unlocked(f"no local lock folder ({e})")
        self.lockfile = lockfile
        path = str(lockfile)
        for round_ in range(_ROUNDS):
            entry = self._join(path, deadline, timeout)
            if entry is None:
                return self._unlocked(f"cannot create {lockfile}")
            verify = round_ < _ROUNDS - 1
            if not verify:
                lg.warning(f"{lockfile} was replaced while it was locked, "
                           f"{_ROUNDS - 1} times: locked as it is now")
            try:
                took = self._take(entry, path, deadline, timeout, verify)
            except BaseException:
                self._leave(entry)
                raise
            if took is _RETIRED:
                lg.debug(f"{lockfile} was replaced: locking it again")
                self._leave(entry)
                continue
            self._entry, self.locked = entry, True
            return self
        raise AssertionError("not reached")      # pragma: no cover

    def _unlocked(self, why: str) -> "FileLock":
        """Go on without a lock (a reader), or raise (a writer)."""
        if self.exclusive:
            raise LockError(f"Cannot lock {self.path} for writing: {why}")
        lg.debug(f"{self.path} is read without a lock: {why}")
        return self

    def _wait_gone(self, entry: _Entry, deadline: float,
                   timeout: float) -> None:
        """Wait until a retired entry has closed its descriptors."""
        remaining = max(deadline - time.monotonic(), _GONE_GRACE)
        if not entry.gone.wait(remaining):
            raise LockTimeout(self.path, entry.lockfile, self.exclusive,
                              "a lock file being closed in this process",
                              timeout)

    def _join(self, path: str, deadline: float,
              timeout: float) -> _Entry | None:
        """
        Find or make the entry of a lock file, counting this holder.

        Returns
        -------
        _Entry | None
            None: a reader whose lock file can be neither created nor
            opened

        Raises
        ------
        LockError
            For a writer whose lock file cannot be created, or if the
            identity of a new descriptor cannot be read
        """
        while True:
            with _registry_lock:
                entry = _by_path.get(path)
                if entry is not None and not entry.retired:
                    entry.users += 1
                    return entry
            if entry is not None:
                self._wait_gone(entry, deadline, timeout)
                continue
            # (no lock held: opening, creating and `fstat` are I/O)
            try:
                fd, writable = _open_lockfile(Path(path))
            except OSError as e:
                if self.exclusive:
                    raise LockError(f"Cannot lock {self.path} for "
                                    f"writing: cannot create {path} ({e})")
                return None
            try:
                ident = _ident(fd, path)
            except OSError as e:
                _leaked.append(fd)          # (I4: never closed)
                raise LockError(f"Cannot lock {self.path}: cannot read "
                                f"the identity of {path} ({e})") from e
            with _registry_lock:
                by_id = _by_id.get(ident) if ident is not None else None
                by_path = _by_path.get(path)
                # a live entry of this file (by identity first: a retired
                # entry of this path must not hide it), else of this path
                for entry in (by_id, by_path):
                    if entry is not None and not entry.retired:
                        entry.spares.append(fd)
                        entry.paths.add(path)
                        _by_path[path] = entry
                        entry.users += 1
                        return entry
                retired = by_id or by_path
                if retired is None:
                    entry = _Entry(fd, writable, ident, path)
                    _by_path[path] = entry
                    if ident is not None:
                        _by_id[ident] = entry
                    entry.users = 1
                    return entry
            # (a retired entry holds no OS lock, and no live entry has
            # this file: closing this descriptor drops no lock)
            os.close(fd)
            self._wait_gone(retired, deadline, timeout)

    def _take(self, entry: _Entry, path: str, deadline: float,
              timeout: float, verify: bool):
        """Take the lock of `entry` (`_RETIRED`: join again)."""
        me = threading.get_ident()
        with entry.cond:
            if self.exclusive and entry.writer == me:
                raise LockError(
                    f"{self.path} is already being written by this thread "
                    f"(nested writers of one file would deadlock)"
                )
            if self.exclusive and entry.readers[me]:
                raise LockError(
                    f"Cannot write {self.path} while this thread reads it "
                    f"(lock upgrades are not supported: they deadlock)"
                )
            while not entry.retired and \
                    not entry.compatible(self.exclusive, me):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LockTimeout(self.path, entry.lockfile,
                                      self.exclusive, entry.local_holder(),
                                      timeout)
                entry.cond.wait(remaining)
            if entry.retired:
                return _RETIRED
            if self.exclusive and not entry.writable:
                raise LockError(f"Cannot lock {self.path} for writing: "
                                f"{entry.lockfile} is read-only")
            if self.exclusive or entry.mode is None:
                entry.busy = True
                try:
                    self._take_os(entry, deadline, timeout)
                    if verify and not _WINDOWS and entry.ident is not None \
                            and entry.mode is None \
                            and self._stale(entry, path):
                        # lock, then verify: the path names another file
                        # now (replaced or deleted): retire the entry
                        _MECHANISMS[entry.mech].unlock(entry.fd)
                        with _registry_lock:
                            entry.retired = True
                        return _RETIRED
                    _touch(entry.fd)
                    if self.exclusive:
                        self._sign(entry)
                finally:
                    entry.busy = False
                    entry.cond.notify_all()
            if self.exclusive:
                entry.writer, entry.mode = me, "exclusive"
            else:
                entry.readers[me] += 1
                entry.mode = entry.mode or "shared"
            self._thread = me
        return True

    @staticmethod
    def _stale(entry: _Entry, path: str) -> bool:
        """Whether `path` no longer names the file of `entry`."""
        try:
            found = _path_id(path)
        except OSError as e:
            # gone; other errors (EIO...) keep the lock
            return e.errno in (errno.ENOENT, errno.ENOTDIR,
                               getattr(errno, "ESTALE", errno.ENOENT))
        return found != entry.ident[:2]

    def _take_os(self, entry: _Entry, deadline: float,
                 timeout: float) -> None:
        """Take the OS lock, polling (with `entry.cond` held)."""
        delay = _FIRST_DELAY
        while True:
            mechanism = _MECHANISMS[entry.mech]
            try:
                if mechanism.try_lock(entry.fd, self.exclusive):
                    return
            except OSError as e:
                if e.errno in _UNSUPPORTED and entry.mode is None \
                        and entry.mech + 1 < len(_MECHANISMS):
                    lg.debug(f"{entry.lockfile}: {mechanism.name} locks "
                             f"refused ({_describe(e)}): "
                             f"{_MECHANISMS[entry.mech + 1].name} locks")
                    entry.mech += 1
                    continue
                raise LockError(
                    f"Cannot lock {self.path} ({entry.lockfile}): "
                    f"{_describe(e)} (does the filesystem support file "
                    f"locks? ${ENABLE_ENV}=0 disables locks)"
                ) from e
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LockTimeout(self.path, entry.lockfile, self.exclusive,
                                  self._os_holder(entry), timeout)
            entry.cond.wait(min(delay, remaining))
            delay = min(2 * delay, _MAX_DELAY)

    @staticmethod
    def _sign(entry: _Entry) -> None:
        """Write who holds the exclusive lock in its lock file."""
        info = f"\npid {os.getpid()} on {socket.gethostname()}\n".encode()
        try:
            os.lseek(entry.fd, 0, os.SEEK_SET)
            os.ftruncate(entry.fd, 0)
            os.write(entry.fd, info)
        except OSError:
            pass

    def _os_holder(self, entry: _Entry) -> str:
        """Describe what holds an OS lock that this process lacks."""
        mechanism = _MECHANISMS[entry.mech]
        if self.exclusive:
            # (this entry holds no OS lock: see `_take`)
            try:
                shared = mechanism.try_lock(entry.fd, False)
            except OSError:
                shared = False
            if shared:
                mechanism.unlock(entry.fd)
                return "readers (in other processes)"
        try:
            os.lseek(entry.fd, _SIGNATURE_AT, os.SEEK_SET)
            info = os.read(entry.fd, 256).decode(errors="replace").strip()
        except OSError:
            info = ""
        return f"a writer ({info})" if info else "a writer"

    def release(self) -> None:
        """
        Release the lock (no-op if it is not held).

        A writer released inside readers of its thread downgrades the OS
        lock to a shared one where the primitive can (else it stays
        exclusive until the last reader leaves; this never raises).
        """
        entry, self._entry = self._entry, None
        if entry is None:
            return
        self.locked = False
        try:
            with entry.cond:
                if self.exclusive:
                    entry.writer = None
                else:
                    entry.readers[self._thread] -= 1
                    entry.readers = +entry.readers
                try:
                    if entry.writer is None:
                        if +entry.readers:
                            if entry.mode == "exclusive" \
                                    and self._downgrade(entry):
                                entry.mode = "shared"
                        else:
                            try:
                                _MECHANISMS[entry.mech].unlock(entry.fd)
                            finally:
                                entry.mode = None
                finally:
                    entry.cond.notify_all()
        finally:
            self._leave(entry)

    @staticmethod
    def _downgrade(entry: _Entry) -> bool:
        """Downgrade the OS lock of an entry (False: it stays exclusive)."""
        try:
            return bool(_MECHANISMS[entry.mech].downgrade(entry.fd))
        except OSError as e:
            lg.debug(f"{entry.lockfile}: downgrade failed ({e}): the lock "
                     f"stays exclusive until its last reader leaves")
            return False

    @staticmethod
    def _leave(entry: _Entry) -> None:
        """Count a holder out; the last one retires and closes the entry."""
        with _registry_lock:
            entry.users -= 1
            last = entry.users <= 0
            if last:
                entry.retired = True
        if not last:
            return
        # (no lock held; the entry holds no OS lock: I1)
        for fd in (entry.fd, *entry.spares):
            try:
                os.close(fd)
            except OSError:
                pass
        with _registry_lock:
            for path in entry.paths:
                if _by_path.get(path) is entry:
                    del _by_path[path]
            if entry.ident is not None and _by_id.get(entry.ident) is entry:
                del _by_id[entry.ident]
        entry.gone.set()

    def __enter__(self) -> "FileLock":
        """Take the lock (`acquire`)."""
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Release the lock (`release`)."""
        self.release()

    def __repr__(self) -> str:
        """Representation, with the kind of holder."""
        return f"FileLock({str(self.path)!r}, {self.kind})"
