"""Run lock of a dataset (`actions.runlock`): harvest/roast vs compost."""
import json
import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from brainspresso.actions import locks, runlock
from brainspresso.actions.locks import LockError
from brainspresso.actions.runlock import (
    RunLocked,
    held_states,
    hold_shared,
    read_records,
    run_lock,
)
from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import make_layout
from brainspresso.cli import main
from brainspresso.datasets.base import Dataset

HOLD = """
import sys, time
from brainspresso.actions.runlock import run_lock
with run_lock(sys.argv[1], sys.argv[2], exclusive=sys.argv[3] == "1"):
    print("ready", flush=True)
    time.sleep(60)
"""


@pytest.fixture
def state(tmp_path):
    folder = tmp_path / "DS" / ".brainspresso"
    folder.mkdir(parents=True)
    return folder


@pytest.fixture
def holder(state):
    """Another process that holds the run lock."""
    procs = []

    def start(command="harvest", exclusive=False):
        proc = subprocess.Popen(
            [sys.executable, "-c", HOLD, str(state), command,
             "1" if exclusive else "0"],
            stdout=subprocess.PIPE, text=True,
            cwd=Path(__file__).parents[1],
        )
        procs.append(proc)
        assert proc.stdout.readline().strip() == "ready"
        return proc

    yield start
    for proc in procs:
        proc.kill()
        proc.wait()


def test_shared_runs_together(state, holder):
    holder("roast")
    with run_lock(state, "harvest"):
        records = read_records(state)
        assert {r["command"] for r in records} == {"roast", "harvest"}
        assert str(state) in held_states()
    assert [r["command"] for r in read_records(state)] == ["roast"]
    assert str(state) not in held_states()


def test_compost_fails_fast(state, holder):
    proc = holder("harvest")
    with pytest.raises(RunLocked) as e:
        with run_lock(state, "compost", exclusive=True):
            pass
    assert f"harvest running: pid {proc.pid}" in str(e.value)
    with pytest.raises(RunLocked):
        with run_lock(state, "compost", exclusive=True, wait=0.2):
            pass
    proc.kill()
    proc.wait()
    with run_lock(state, "compost", exclusive=True, force=True):
        pass


def test_harvest_waits_for_compost(state, holder):
    proc = holder("compost", exclusive=True)
    with pytest.raises(RunLocked, match=f"compost running: pid {proc.pid}"):
        with run_lock(state, "harvest"):
            pass


def _record(state, host, pid, command="harvest"):
    folder = state / runlock.RECORDS_DIR
    folder.mkdir(exist_ok=True)
    path = folder / f"{host}-{pid}.json"
    path.write_text(json.dumps({"command": command, "host": host,
                                "pid": pid, "user": "u", "started": "t"}))
    return path


def test_stale_records(state):
    # a record from another host: locks may not be shared, or a crash
    other = _record(state, "elsewhere", 12)
    with pytest.raises(LockError, match="not shared between machines"):
        with run_lock(state, "compost", exclusive=True):
            pass
    with run_lock(state, "compost", exclusive=True, force=True):
        pass
    other.unlink()
    # a record of a dead process of this host is removed
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    dead = _record(state, socket.gethostname(), proc.pid)
    with run_lock(state, "compost", exclusive=True):
        pass
    assert not dead.exists()
    # ... a live one is not
    _record(state, socket.gethostname(), os.getppid())
    with pytest.raises(LockError, match="process is alive"):
        with run_lock(state, "compost", exclusive=True):
            pass


def test_locks_disabled(state, monkeypatch):
    monkeypatch.setenv("BRAINSPRESSO_LOCKS", "0")
    with run_lock(state, "harvest") as locks:
        assert locks == []
        assert read_records(state) == []


def test_unopenable_shared_lock(state, monkeypatch):

    def acquire(self):
        return self             # (a reader that could not open its file)

    monkeypatch.setattr(locks.FileLock, "acquire", acquire)
    with pytest.raises(LockError, match="cannot be opened"):
        with run_lock(state, "harvest"):
            pass


def test_hold_shared(state):
    hold_shared([str(state)])
    try:
        assert runlock._worker_locks[-1].locked
    finally:
        runlock._worker_locks.pop().release()


def test_compost_command_fails_while_harvest_runs(tmp_path, holder, state,
                                                  capsys):
    holder("harvest")
    with pytest.raises(SystemExit) as e:
        main(["openneuro", "compost", "ds1", "--out", str(state.parent),
              "--yes"])
    assert e.value.code == 1
    assert "harvest running" in capsys.readouterr().err


def test_nested_failure_keeps_outer_count(state, monkeypatch):
    with run_lock(state, "harvest"):
        assert held_states() == [str(state)]
        # an inner lock that fails after it is listed (unopenable)
        monkeypatch.setattr(locks.FileLock, "acquire", lambda self: self)
        with pytest.raises(LockError, match="cannot be opened"):
            with run_lock(state, "roast"):
                raise AssertionError("not reached")
        monkeypatch.undo()
        assert held_states() == [str(state)]
        assert runlock._held[str(state)] == 1
    assert held_states() == []


ORDERED = """
import sys, time
from brainspresso.actions.runlock import run_lock
parent, child = sys.argv[1], sys.argv[2]
with run_lock(parent, "compost", exclusive=True):
    print("parent", flush=True)
    time.sleep(1.0)
    with run_lock(child, "compost", exclusive=True, wait=5):
        print("child", flush=True)
"""


def test_parent_before_child(tmp_path):
    # A locks the parent then a site; B (a compost of the site) locks in
    # the same order (`Dataset.lock_states`), so neither waits forever
    root = tmp_path / "DS"
    sites = make_layout("sites", root, child_kind="study")
    record_run(sites.child("A"), "harvest")
    record_run(sites, "harvest")
    site = make_layout("study", root / "A")
    states = Dataset.lock_states(site)
    assert states == [root / ".brainspresso", root / "A" / ".brainspresso"]
    proc = subprocess.Popen(
        [sys.executable, "-c", ORDERED, str(states[0]), str(states[1])],
        stdout=subprocess.PIPE, text=True, cwd=Path(__file__).parents[1])
    try:
        assert proc.stdout.readline().strip() == "parent"
        with run_lock(states, "compost", exclusive=True, wait=5):
            pass
        assert proc.stdout.readline().strip() == "child"
        assert proc.wait(timeout=10) == 0
    finally:
        proc.kill()
        proc.wait()
