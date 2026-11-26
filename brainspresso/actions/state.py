"""
Per-task state files: `<state>/tasks/<task id>.json`.

One file per task (roast shard, grind unit...), so that concurrent jobs
never write the same file; the manifest of a dataset is an index that
can be rebuilt from them. Task ids are free-form (e.g.
"grind/fs/sub-01/ses-1") and URL-quoted into file names. The format is
described in `docs/design/architecture.md` (§7) and `bids.harvest`.
"""
from __future__ import annotations

import datetime
import json
import os
import secrets
import time
from logging import getLogger
from pathlib import Path
from urllib.parse import quote, unquote

lg = getLogger(__name__)

__all__ = [
    "TASKS_DIR", "TASK_SCHEMA", "now", "read_task", "read_task_states",
    "task_path", "write_task_state",
]

#: Folder of the task state files, in the state folder
#: (`bids.layout.TASKS_DIR`)
TASKS_DIR = "tasks"
#: Version of the format of task state files
TASK_SCHEMA = 1
#: Seconds after which a temporary file of the tasks folder is stale (a
#: write takes milliseconds: its process was killed before the rename;
#: a day, for the clocks of machines that share the folder)
STALE_TMP_AGE = 24 * 3600

# tasks folders swept for stale temporary files by this process
_swept: set[str] = set()


def now() -> str:
    """
    Return the current time.

    Returns
    -------
    str
        UTC time, ISO 8601
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def task_path(state: str | os.PathLike, task: str) -> Path:
    """
    Path of the state file of a task.

    Parameters
    ----------
    state : str | PathLike
        State folder (`<DS>/.brainspresso`)
    task : str
        Task id

    Returns
    -------
    Path
        `<state>/tasks/<quoted id>.json`
    """
    return Path(state) / TASKS_DIR / (quote(task, safe="") + ".json")


def write_task_state(
    state: str | os.PathLike, task: str, record: dict
) -> Path:
    """
    Write the state file of a task (atomically).

    Parameters
    ----------
    state : str | PathLike
        State folder
    task : str
        Task id
    record : dict
        Content (`schema` and `task` are added)

    Returns
    -------
    Path
        The state file

    Raises
    ------
    OSError
        If it cannot be written
    """
    path = task_path(state, task)
    path.parent.mkdir(parents=True, exist_ok=True)
    _sweep(path.parent)
    record = {"schema": TASK_SCHEMA, "task": task, **record}
    # unique temporary file: another process (a worker, another run)
    # may write the state of the same task at the same time; short: a
    # long task id plus a suffix could exceed the limit of file names
    tmp = path.with_name(f".{os.getpid()}-{secrets.token_hex(4)}.tmp")
    try:
        tmp.write_text(json.dumps(record, indent=2, default=str))
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return path


def _sweep(folder: Path) -> None:
    """
    Remove the stale temporary files of a tasks folder (once a process).

    A temporary file is stale when it is older than `STALE_TMP_AGE`.
    Its age, not its pid, decides: the folder may be shared by
    processes of other machines. Those of this process are kept.

    Parameters
    ----------
    folder : Path
        Tasks folder
    """
    if str(folder) in _swept:
        return
    _swept.add(str(folder))
    limit = time.time() - STALE_TMP_AGE
    mine = f".{os.getpid()}-"
    try:
        for path in folder.glob("*.tmp"):
            if path.name.startswith(mine):        # never our own
                continue
            try:
                if path.is_file() and path.stat().st_mtime < limit:
                    path.unlink()
            except OSError:
                continue
    except OSError as e:
        lg.debug(f"Could not sweep {folder}: {e}")


def read_task(state: str | os.PathLike, task: str) -> dict | None:
    """
    Read the state file of a task.

    Parameters
    ----------
    state : str | PathLike
        State folder
    task : str
        Task id

    Returns
    -------
    dict | None
        Its content (None if missing or invalid)
    """
    try:
        return json.loads(task_path(state, task).read_text())
    except (OSError, ValueError):
        return None


def read_task_states(state: str | os.PathLike) -> dict[str, dict]:
    """
    Read the state of all tasks.

    Parameters
    ----------
    state : str | PathLike
        State folder

    Returns
    -------
    dict[str, dict]
        `{task id: record}`
    """
    folder = Path(state) / TASKS_DIR
    out = {}
    if not folder.is_dir():
        return out
    for path in sorted(folder.glob("*.json")):
        try:
            record = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            lg.warning(f"Could not parse {path}")
            continue
        out[record.get("task") or unquote(path.stem)] = record
    return out
