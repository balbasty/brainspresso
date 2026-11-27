"""
Inter-process reader/writer locks on the files that actions read and write.

A file is locked through a separate **lock file**, never through the file
itself, and lock files are never deleted by the processes that hold them
(deleting a lock file that another process is waiting on lets two
processes "hold" it at once):

* inside a dataset (the nearest ancestor folder that has a
  `.brainspresso/` state folder), the lock file is
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

On Windows and macOS, whose filesystems ignore case by default, the
path is lowercased (and NFC-normalised) before it is hashed, so that two
spellings of one file share its lock (two files whose names differ only
by case, on a case-sensitive volume, share one too: they exclude each
other needlessly, which is safe).

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

Platforms
---------
The OS lock is the reader/writer lock mechanism of `fasteners`
(`_OSLock` wraps it, see `FASTENERS_REQUIREMENT`):

* POSIX (Linux, macOS): `fcntl.lockf` record locks on the whole lock
  file. They are advisory (only brainspresso's own processes honour
  them), they belong to a *process*, not to a file descriptor, and
  closing *any* descriptor of the lock file releases them all.
* Windows: `LockFileEx` locks on the first byte of the lock file (that
  byte cannot be read by other processes while it is locked exclusively,
  so the signature of a writer follows it). They belong to a *file
  handle* and are mandatory on that byte. The OS releases them when the
  handle is closed or the process ends (after a crash, possibly with a
  short delay).

On both, a process therefore opens each lock file once, and the holders
of one lock file in a process (nested readers, threads) share that
handle (`_Entry`), with the rules between them enforced in the process
(a second handle would see the process's own locks as foreign on
Windows, and drop them on POSIX when closed).

Network filesystems: on NFS, `fcntl` locks are shared between clients
with NFSv4, and with NFSv3 only through a lock manager (`lockd`; mounts
with `nolock`, or `local_lock=all`, keep them local to each client);
Lustre and GPFS share them with locking enabled (not with
`localflock`). On SMB shares, `LockFileEx` locks are enforced by the
server, so they hold between Windows clients; POSIX clients of the same
share may map their `fcntl` locks to SMB byte-range locks (Linux
`cifs` does, unless mounted with `nobrl`; macOS `smbfs` is not
checked), and a Samba server maps SMB locks to its local `fcntl` locks
(`posix locking = yes`, its default). Whether locks
hold across machines cannot be checked from here (some FUSE or object
stores accept locks without sharing them); run records
(`actions.runlock`) are the second line of defence of compost.

Disabling locks
---------------
Locks are disabled only explicitly: `$BRAINSPRESSO_LOCKS` set to "0",
"no", "off" or "false" (an escape hatch for filesystems without lock
support; unsafe with concurrent writers). Otherwise, a missing lock
backend (an unsupported `fasteners`) raises `LockError` instead of
running unlocked.

A reader whose lock file can neither be created nor opened (e.g. a
dataset whose `.brainspresso/` is read-only for this user) reads without
a lock (a debug message says so). A writer whose lock file cannot be
created, and any holder on a filesystem that refuses locks (`ENOLCK`,
e.g. NFS without a lock manager), raise `LockError` instead of writing
unprotected.

Limitations
-----------
* A thread that reads a file cannot also write it (lock upgrade), and a
  thread cannot write a file twice at once: both raise `LockError` at
  once (they would deadlock). Between processes, cycles (A reads X and
  waits to write Y while B reads Y and waits to write X) are not
  detected: they end with a `LockTimeout`.
* The locks are not fair: a steady stream of readers can delay a writer
  up to its timeout.
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
import hashlib
import os
import socket
import sys
import tempfile
import threading
import time
import unicodedata
from collections import Counter, OrderedDict
from logging import getLogger
from pathlib import Path

try:
    # The non-blocking shared/exclusive try-lock of `fasteners` (`lockf`
    # on POSIX, `LockFileEx` through its ctypes shim on Windows). Its
    # public `InterProcessReaderWriterLock` closes the file on release
    # and cannot downgrade a lock, so `_OSLock` wraps the mechanism.
    from fasteners.process_mechanism import (
        _interprocess_reader_writer_mechanism as _MECHANISM,
    )
except ImportError as _e:       # pragma: no cover (unsupported fasteners)
    _MECHANISM, _NO_BACKEND = None, str(_e)
else:
    _NO_BACKEND = ""

lg = getLogger(__name__)

__all__ = [
    "DEFAULT_TIMEOUT", "FileLock", "LockError", "LockTimeout",
    "find_dataset", "forget_datasets", "lock_file", "lock_timeout",
    "locks_enabled",
]

#: State folder of a dataset (`bids.layout.STATE_DIR`)
STATE_DIR = ".brainspresso"
#: Folder of the lock files, in the state folder of a dataset
LOCKS_DIR = "locks"
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
#: The versions of `fasteners` whose (private) lock mechanism `_OSLock`
#: uses (also in `setup.cfg`)
FASTENERS_REQUIREMENT = "fasteners >= 0.18, < 0.21"

_WINDOWS = os.name == "nt"
# Filesystems that ignore case (and, on macOS, Unicode normalisation) by
# default: lock keys are normalised (see the module docstring)
_CASELESS = sys.platform in ("win32", "darwin")


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


def _check_backend() -> None:
    """Raise `LockError` if no OS lock can be taken on this platform."""
    if _MECHANISM is None:
        raise LockError(
            f"No file lock backend: the reader/writer lock mechanism of "
            f"fasteners cannot be imported ({_NO_BACKEND}). Install "
            f"`{FASTENERS_REQUIREMENT}`, or set ${ENABLE_ENV}=0 to run "
            f"without locks (unsafe with concurrent writers)."
        )


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


def _is_state_dir(folder: Path) -> bool:
    """Whether a folder has a state folder (one `stat`)."""
    return os.path.isdir(os.path.join(folder, STATE_DIR))


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


def forget_datasets() -> None:
    """Forget the cached dataset of every folder (`find_dataset`)."""
    with _datasets_lock:
        _datasets.clear()


def _dataset_of(folder: Path) -> Path | None:
    """Nearest ancestor-or-self of a resolved folder with a state folder."""
    with _datasets_lock:
        found = _datasets.get(folder)
        if found is not None:
            _datasets.move_to_end(folder)
            return found
    for parent in (folder, *folder.parents):
        if _is_state_dir(parent):
            found = parent
            break
    else:
        return None
    with _datasets_lock:
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
    if _CASELESS:
        key = unicodedata.normalize("NFC", key).lower()
    digest = hashlib.sha256(key.encode()).hexdigest()
    return folder / digest[:2] / f"{digest}.lock", dataset


# ----------------------------------------------------------------------
#   The OS lock of a lock file (fasteners)
# ----------------------------------------------------------------------

def _describe(error: OSError) -> str:
    """Text of an OS error (Windows codes of `fasteners` included)."""
    if error.strerror:
        return error.strerror
    code = error.args[0] if len(error.args) == 1 else None
    format_error = getattr(ctypes, "FormatError", None)     # (Windows)
    if isinstance(code, int) and format_error is not None:
        return f"{format_error(code).strip()} (error {code})"
    return str(error) or type(error).__name__


class _OSLock:
    """
    The OS lock of one lock file, through one open file.

    A small adapter of the reader/writer mechanism of `fasteners`
    (`FASTENERS_REQUIREMENT`), which takes a shared or exclusive lock
    without blocking (`lockf` from offset 0 to the end on POSIX,
    `LockFileEx` on byte 0 on Windows, both through the open file). A
    process has at most one per lock file (`_Entry`).

    Parameters
    ----------
    path : Path
        Lock file (created if `writable`)
    writable : bool
        Open (and create) it for reading and writing, else read-only
        (enough for a shared lock)

    Raises
    ------
    OSError
        If it cannot be opened
    """

    # Offset of the signature of a writer: byte 0 holds the lock on
    # Windows, where it cannot be read while it is locked exclusively
    _SIGNATURE_AT = 1 if _WINDOWS else 0

    def __init__(self, path: Path, writable: bool):
        flags = os.O_RDWR | os.O_CREAT if writable else os.O_RDONLY
        fd = os.open(path, flags | getattr(os, "O_BINARY", 0), 0o666)
        try:
            self.file = os.fdopen(fd, "r+b" if writable else "rb",
                                  buffering=0)
        except BaseException:
            os.close(fd)
            raise
        self.writable = writable

    def try_lock(self, exclusive: bool) -> bool:
        """
        Take the lock if it is free, without waiting.

        Parameters
        ----------
        exclusive : bool
            Exclusive lock, else shared

        Returns
        -------
        bool
            False if another process holds it (incompatibly)

        Raises
        ------
        OSError
            If the lock cannot be taken at all (e.g. `ENOLCK`)
        """
        self.file.seek(0)               # (`lockf` locks from the offset)
        return bool(_MECHANISM.trylock(self.file, exclusive))

    def downgrade(self) -> None:
        """Turn the exclusive lock into a shared one, without a gap."""
        self.file.seek(0)
        # POSIX: `lockf` converts the lock in place. Windows: a shared
        # lock on the same handle stacks on the exclusive one, which the
        # next unlock removes (first the exclusive, then the shared one).
        if not _MECHANISM.trylock(self.file, False):
            raise OSError(errno.EAGAIN, "cannot downgrade the lock")
        if _WINDOWS:
            _MECHANISM.unlock(self.file)

    def unlock(self) -> None:
        """Release the lock."""
        self.file.seek(0)
        _MECHANISM.unlock(self.file)

    def sign(self, info: str) -> None:
        """
        Write who holds the exclusive lock in the lock file.

        Parameters
        ----------
        info : str
            One line ("pid 12 on node1")
        """
        try:
            self.file.seek(0)
            self.file.truncate()
            self.file.write(b"\n" + info.encode() + b"\n")
            self.file.seek(0)
        except OSError:
            pass

    def signature(self) -> str:
        """
        Read the signature of the last writer.

        Returns
        -------
        str
            Its line ("" if there is none, or it cannot be read)
        """
        try:
            self.file.seek(self._SIGNATURE_AT)
            info = self.file.read(256) or b""
            self.file.seek(0)
        except OSError:
            return ""
        return info.decode(errors="replace").strip()

    def close(self) -> None:
        """Close the file (which releases its locks)."""
        self.file.close()


# ----------------------------------------------------------------------
#   One open lock file per process
# ----------------------------------------------------------------------

class _Entry:
    """
    A lock file opened by this process, and its holders in the process.

    The OS lock (`mode`) is shared while only readers hold it, exclusive
    while a writer does; `busy` while a thread waits for the OS lock.
    """

    def __init__(self, lockfile: Path):
        self.lockfile = lockfile
        self.cond = threading.Condition()
        self.handle: _OSLock | None = None  # the open lock file
        self.mode: str | None = None        # None | "shared" | "exclusive"
        self.readers: Counter = Counter()   # thread -> number of holds
        self.writer: int | None = None      # thread
        self.busy = False
        self.users = 0                      # holders and waiters

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


_registry: dict[Path, _Entry] = {}
_registry_lock = threading.Lock()


def _after_fork() -> None:              # pragma: no cover
    # POSIX locks are not inherited: forget those of the parent (closing
    # the inherited descriptors in the child leaves the parent's locks)
    global _registry_lock
    for entry in _registry.values():
        if entry.handle is not None:
            try:
                entry.handle.close()
            except OSError:
                pass
    _registry.clear()
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
        with _registry_lock:
            entry = _registry.get(lockfile)
            if entry is None:
                entry = _registry[lockfile] = _Entry(lockfile)
            entry.users += 1
        try:
            took = self._take(entry, deadline, timeout)
        except BaseException:
            self._leave(entry)
            raise
        if not took:
            self._leave(entry)
            return self._unlocked(f"cannot open {lockfile}")
        self._entry, self.locked = entry, True
        return self

    def _unlocked(self, why: str) -> "FileLock":
        """Go on without a lock (a reader), or raise (a writer)."""
        if self.exclusive:
            raise LockError(f"Cannot lock {self.path} for writing: {why}")
        lg.debug(f"{self.path} is read without a lock: {why}")
        return self

    def _take(self, entry: _Entry, deadline: float, timeout: float) -> bool:
        """Take the lock of `entry` (False: no lock file for a reader)."""
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
            while not entry.compatible(self.exclusive, me):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LockTimeout(self.path, entry.lockfile,
                                      self.exclusive, entry.local_holder(),
                                      timeout)
                entry.cond.wait(remaining)
            if self.exclusive or entry.mode is None:
                entry.busy = True
                try:
                    if not self._take_os(entry, deadline, timeout):
                        return False
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

    def _open(self, entry: _Entry) -> bool:
        """Open the lock file of `entry` (False: a reader cannot)."""
        if entry.handle is not None:
            if self.exclusive and not entry.handle.writable:
                raise LockError(f"Cannot lock {self.path} for writing: "
                                f"{entry.lockfile} is read-only")
            return True
        try:
            entry.lockfile.parent.mkdir(parents=True, exist_ok=True)
            entry.handle = _OSLock(entry.lockfile, writable=True)
            return True
        except OSError as e:
            if self.exclusive:
                raise LockError(f"Cannot lock {self.path} for writing: "
                                f"cannot create {entry.lockfile} ({e})")
        try:
            entry.handle = _OSLock(entry.lockfile, writable=False)
            return True
        except OSError:
            return False

    def _take_os(self, entry: _Entry, deadline: float,
                 timeout: float) -> bool:
        """Take the OS lock, polling (with `entry.cond` held)."""
        if not self._open(entry):
            return False
        delay = _FIRST_DELAY
        while True:
            try:
                if entry.handle.try_lock(self.exclusive):
                    break
            except OSError as e:
                self._close(entry)
                raise LockError(
                    f"Cannot lock {self.path} ({entry.lockfile}): "
                    f"{_describe(e)} (does the filesystem support file "
                    f"locks? ${ENABLE_ENV}=0 disables locks)"
                ) from e
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                holder = self._os_holder(entry)
                self._close(entry)
                raise LockTimeout(self.path, entry.lockfile, self.exclusive,
                                  holder, timeout)
            entry.cond.wait(min(delay, remaining))
            delay = min(2 * delay, _MAX_DELAY)
        if self.exclusive:
            entry.handle.sign(f"pid {os.getpid()} on {socket.gethostname()}")
        return True

    def _os_holder(self, entry: _Entry) -> str:
        """Describe what holds an OS lock that this process lacks."""
        if self.exclusive:
            # (this process holds no OS lock on the file: see `_take`)
            try:
                shared = entry.handle.try_lock(False)
            except OSError:
                shared = False
            if shared:
                entry.handle.unlock()
                return "readers (in other processes)"
        info = entry.handle.signature()
        return f"a writer ({info})" if info else "a writer"

    @staticmethod
    def _close(entry: _Entry) -> None:
        """Close the lock file of an entry that holds no OS lock."""
        if entry.handle is not None and entry.mode is None:
            try:
                entry.handle.close()
            finally:
                entry.handle = None

    def release(self) -> None:
        """Release the lock (no-op if it is not held)."""
        entry, self._entry = self._entry, None
        if entry is None:
            return
        self.locked = False
        with entry.cond:
            if self.exclusive:
                entry.writer = None
            else:
                entry.readers[self._thread] -= 1
                entry.readers = +entry.readers
            try:
                if entry.writer is None:
                    if +entry.readers:
                        if entry.mode == "exclusive":
                            entry.handle.downgrade()
                            entry.mode = "shared"
                    else:
                        try:
                            entry.handle.unlock()
                        finally:
                            entry.mode = None
                            self._close(entry)
            finally:
                entry.cond.notify_all()
        self._leave(entry)

    @staticmethod
    def _leave(entry: _Entry) -> None:
        """Forget an entry that has no holders and no waiters."""
        with _registry_lock:
            entry.users -= 1
            if entry.users <= 0 and _registry.get(entry.lockfile) is entry:
                del _registry[entry.lockfile]

    def __enter__(self) -> "FileLock":
        """Take the lock (`acquire`)."""
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Release the lock (`release`)."""
        self.release()

    def __repr__(self) -> str:
        """Representation, with the kind of holder."""
        return f"FileLock({str(self.path)!r}, {self.kind})"
