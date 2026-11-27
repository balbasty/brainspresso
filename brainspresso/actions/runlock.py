"""
Run lock of a dataset: harvests and roasts against compost.

Every harvest and roast of a dataset holds a **shared** lock on
`<state>/run` (a `FileLock`, whose lock file is in
`<state>/locks/`) for the whole run; compost holds it **exclusively**,
from before it plans until it has deleted (through its confirmation
prompt). So compost never deletes files that a running harvest or roast
writes or reads, also on other cluster nodes where the filesystem
shares `fcntl` locks, and nothing starts while compost runs. By default
a command that cannot take the lock fails at once (`wait=0`).

`FileLock` only signs exclusive holders. So every holder also writes a
**run record**, `<state>/runs/<host>-<pid>.json` (command, user, host,
pid, start), removed when it exits; the messages name the holders from
these records ("harvest running: pid 12 on node1 since ...").

Run records are also a second line of defence where locks are not
shared between machines (Lustre `localflock`, some FUSE mounts) or
outlive their process (NFSv3 after a client crash): compost, once it
holds the exclusive lock, refuses while a record from another host
exists, or one from this host whose process is alive, unless `force`
(`compost --ignore-runs`). Records of dead processes of this host are
removed.

With locks disabled (`$BRAINSPRESSO_LOCKS=0`, or no `fcntl`), nothing is
locked and no record is written.

Roast workers take the shared lock too: `held_states()` lists the state
folders whose run lock this process holds, and `hold_shared(states)`
(the initializer of the local process pool) takes them in a worker.
"""
from __future__ import annotations

import contextlib
import datetime
import getpass
import json
import os
import socket
from pathlib import Path
from typing import Iterable, Iterator

from brainspresso.actions.locks import (
    FileLock,
    LockError,
    LockTimeout,
    locks_enabled,
)

RUN_FILE = "run"
RECORDS_DIR = "runs"

# state folders whose run lock this process holds: {state: count}
_held: dict[str, int] = {}
# locks taken by `hold_shared` in a worker (kept until it exits)
_worker_locks: list[FileLock] = []


class RunLocked(LockTimeout):
    """The run lock of a dataset is held by another command."""

    def __init__(self, error: LockTimeout, message: str):
        LockError.__init__(self, message)
        self.path, self.lockfile = error.path, error.lockfile
        self.exclusive, self.holder = error.exclusive, error.holder
        self.timeout = error.timeout


def _record_name() -> str:
    """Name of the run record of this process."""
    return f"{socket.gethostname()}-{os.getpid()}.json"


def read_records(state: str | os.PathLike) -> list[dict]:
    """
    Run records of a state folder.

    Parameters
    ----------
    state : path
        State folder of a dataset (`<DS>/.brainspresso`)

    Returns
    -------
    list[dict]
        One record per process that holds (or held, if it crashed) the
        run lock: `command`, `user`, `host`, `pid`, `started`, `file`
    """
    out = []
    folder = Path(state) / RECORDS_DIR
    for path in sorted(folder.glob("*.json")) if folder.is_dir() else ():
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        record["file"] = str(path)
        out.append(record)
    return out


def describe(record: dict) -> str:
    """One line about a run record ("harvest running: pid 12 on n1")."""
    return (f"{record.get('command', '?')} running: pid "
            f"{record.get('pid', '?')} on {record.get('host', '?')} "
            f"(user {record.get('user', '?')}, since "
            f"{record.get('started', '?')})")


def pid_alive(pid: int) -> bool:
    """
    Tell whether a process of this host is alive.

    POSIX only, as the locks (`fcntl`): Windows is not supported, and
    there `os.kill(pid, 0)` is no probe, so every pid counts as alive.

    Parameters
    ----------
    pid : int
        Its process id

    Returns
    -------
    bool
        False only if no such process exists (one that cannot be
        signalled, or an invalid pid, counts as alive)
    """
    if os.name == "nt":         # `os.kill(pid, 0)` would terminate it
        return True
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except (PermissionError, ValueError, TypeError, OverflowError):
        return True
    return True


def _write_record(state: Path, command: str) -> Path:
    """Write the run record of this process."""
    folder = state / RECORDS_DIR
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / _record_name()
    try:
        user = getpass.getuser()
    except Exception:           # (no user name in some containers)
        user = str(os.getuid()) if hasattr(os, "getuid") else "?"
    record = {
        "command": command, "user": user, "host": socket.gethostname(),
        "pid": os.getpid(),
        "started": datetime.datetime.now(datetime.timezone.utc)
        .isoformat(timespec="seconds"),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(record))
    os.replace(tmp, path)
    return path


def _check_records(state: Path, force: bool) -> None:
    """Refuse an exclusive run while other runs seem to be alive."""
    host, me = socket.gethostname(), os.getpid()
    others = []
    for record in read_records(state):
        if record.get("host") == host:
            if record.get("pid") == me:
                continue
            if not pid_alive(record.get("pid")):
                with contextlib.suppress(OSError):
                    os.unlink(record["file"])
                continue
            others.append((record, "its process is alive"))
        else:
            others.append((record, "on another host"))
    if not others or force:
        return
    lines = "\n".join(f"  {describe(r)} [{why}]" for r, why in others)
    raise LockError(
        f"{state.parent} has run records of other commands although "
        f"its run lock is free:\n{lines}\nEither locks are not shared "
        f"between machines on this filesystem (then wait for these "
        f"runs to finish), or a run crashed on the host named above "
        f"(then delete its record in {state / RECORDS_DIR}). Use "
        f"--ignore-runs to go on anyway."
    )


def _locked_message(state: Path, command: str, exclusive: bool) -> str:
    """Message of a run lock held by others."""
    records = [r for r in read_records(state)
               if not (r.get("host") == socket.gethostname()
                       and r.get("pid") == os.getpid())]
    what = "compost" if not exclusive else "harvest or roast"
    lines = "\n".join(f"  {describe(r)}" for r in records) or (
        f"  (no run record: a {what} of an older version, or one that "
        f"is starting)")
    return (f"Cannot {command} {state.parent}: the dataset is in use\n"
            f"{lines}\nWait for it to finish, or use --lock-wait SECONDS.")


@contextlib.contextmanager
def run_lock(
    states: str | os.PathLike | Iterable[str | os.PathLike],
    command: str,
    *,
    exclusive: bool = False,
    wait: float = 0,
    force: bool = False,
) -> Iterator[list[FileLock]]:
    """
    Hold the run lock of one or several datasets.

    The state folders must exist (`Layout.ensure_state`).

    Parameters
    ----------
    states : path | list[path]
        State folders (`<DS>/.brainspresso`; a sites layout: the parent
        and each site)
    command : str
        Command name, for the run records and messages
    exclusive : bool
        Exclusive lock (compost) instead of a shared one
    wait : float
        Seconds to wait for the lock (0: fail at once)
    force : bool
        Exclusive lock: go on despite run records of other runs

    Yields
    ------
    list[FileLock]
        The locks held

    Raises
    ------
    RunLocked
        If another command holds the lock after `wait` seconds
    LockError
        If the lock cannot be taken (e.g. a filesystem without `fcntl`
        locks), or if records of other runs exist (exclusive lock)
    """
    if isinstance(states, (str, os.PathLike)):
        states = [states]
    states = [Path(s) for s in states]
    if not locks_enabled():
        yield []
        return
    locks, records, counted = [], [], []
    try:
        for state in states:
            if not state.is_dir():
                raise LockError(f"No state folder {state}: cannot lock it")
            lock = FileLock(state / RUN_FILE, exclusive=exclusive,
                            timeout=wait)
            try:
                lock.acquire()
            except LockTimeout as e:
                raise RunLocked(
                    e, _locked_message(state, command, exclusive)
                ) from e
            locks.append(lock)
            if not lock.locked:
                raise LockError(
                    f"Cannot take the run lock of {state.parent} "
                    f"({state / RUN_FILE}): its lock file cannot be "
                    f"opened (read-only state folder?). "
                    f"$BRAINSPRESSO_LOCKS=0 disables locks (unsafe)."
                )
            if exclusive:
                _check_records(state, force)
            key = str(state)
            if not _held.get(key):
                records.append(_write_record(state, command))
            _held[key] = _held.get(key, 0) + 1
            counted.append(key)
        yield locks
    finally:
        # only the counts this call incremented (a failure between
        # `acquire` and the count must not drop an outer holder's)
        for key in reversed(counted):
            _held[key] -= 1
            if not _held[key]:
                del _held[key]
        for lock in reversed(locks):
            lock.release()
        for path in records:
            with contextlib.suppress(OSError):
                path.unlink()


def held_states() -> list[str]:
    """
    State folders whose run lock this process holds.

    Returns
    -------
    list[str]
        Paths, for `hold_shared` in worker processes
    """
    return sorted(_held)


def hold_shared(states: Iterable[str]) -> None:
    """
    Take shared run locks for the life of this (worker) process.

    The initializer of the local process pool: a worker that outlives
    the main process still holds the lock. Failures are ignored (the
    main process holds the lock).

    Parameters
    ----------
    states : list[str]
        State folders (`held_states()` of the main process)
    """
    for state in states:
        lock = FileLock(Path(state) / RUN_FILE, timeout=0)
        try:
            lock.acquire()
        except LockError:
            continue
        _worker_locks.append(lock)
