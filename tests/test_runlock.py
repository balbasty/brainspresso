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
    pid_alive,
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

HOLD_ALL = """
import sys, time
from brainspresso.actions.runlock import run_lock
with run_lock(sys.argv[2:], sys.argv[1]):
    print("ready", flush=True)
    time.sleep(60)
"""


@pytest.fixture
def two_sites(tmp_path):
    """`DS/` with one dataset per site, `A` and `B`, each with subjects."""
    root = tmp_path / "DS"
    sites = make_layout("sites", root, child_kind="flat")
    for name in ("A", "B"):
        child = sites.child(name)
        for sub in ("sub-01", "sub-02"):
            (Path(child.raw) / sub / "anat").mkdir(parents=True)
            (Path(child.raw) / sub / "anat" / f"{sub}_T1w.nii.gz"
             ).write_bytes(b"x")
        (Path(child.raw) / "dataset_description.json").write_text(
            '{"Name": "x", "BIDSVersion": "1.10.0"}')
        record_run(child, "harvest")
    record_run(sites, "harvest")
    return root


def _compost_site(root, name, *extra):
    main(["openneuro", "compost", "ds1", "--out", str(root / name),
          "--exclude-subs", "01", "--yes", *extra])


def _hold(command, *states):
    proc = subprocess.Popen(
        [sys.executable, "-c", HOLD_ALL, command, *map(str, states)],
        stdout=subprocess.PIPE, text=True, cwd=Path(__file__).parents[1])
    assert proc.stdout.readline().strip() == "ready"
    return proc


def test_parent_before_child(two_sites):
    # A compost of the whole parent locks it, then a site; a compost of
    # the site waits for the parent (shared) first, so neither waits
    # forever
    root = two_sites
    parents, targets = Dataset.lock_states(make_layout("flat", root / "A"))
    assert parents == [root / ".brainspresso"]
    proc = subprocess.Popen(
        [sys.executable, "-c", ORDERED, str(parents[0]), str(targets[0])],
        stdout=subprocess.PIPE, text=True, cwd=Path(__file__).parents[1])
    try:
        assert proc.stdout.readline().strip() == "parent"
        try:
            _compost_site(root, "A", "--wait", "5")
        except SystemExit as e:
            assert not e.code
        assert proc.stdout.readline().strip() == "child"
        assert proc.wait(timeout=10) == 0
    finally:
        proc.kill()
        proc.wait()
    assert not (root / "A" / "sub-01").exists()


def test_compost_site_next_to_roast_of_sibling(two_sites, capsys):
    root = two_sites
    # a roast of B: the parent and B, shared
    proc = _hold("roast", root / ".brainspresso", root / "B" / ".brainspresso")
    try:
        try:
            _compost_site(root, "A")
        except SystemExit as e:
            assert not e.code
        assert not (root / "A" / "sub-01").exists()
    finally:
        proc.kill()
        proc.wait()
    # a roast of A: the compost of A fails
    proc = _hold("roast", root / ".brainspresso", root / "A" / ".brainspresso")
    try:
        with pytest.raises(SystemExit) as e:
            main(["openneuro", "compost", "ds1", "--out", str(root / "A"),
                  "--exclude-subs", "02", "--yes"])
        assert e.value.code == 1
        assert "roast running" in capsys.readouterr().err
        assert (root / "A" / "sub-02").exists()
    finally:
        proc.kill()
        proc.wait()
    # a compost of the whole parent fails while any site is roasted
    proc = _hold("roast", root / ".brainspresso", root / "B" / ".brainspresso")
    try:
        with pytest.raises(SystemExit):
            main(["openneuro", "compost", "ds1", "--out", str(root),
                  "--exclude-subs", "02", "--yes"])
        assert "roast running" in capsys.readouterr().err
    finally:
        proc.kill()
        proc.wait()


# ----------------------------------------------------------------------
#   Every platform
# ----------------------------------------------------------------------

def test_pid_alive_live_and_dead_processes():
    assert pid_alive(os.getpid())
    assert pid_alive(str(os.getpid()))          # (as in a JSON record)
    proc = subprocess.Popen([sys.executable, "-c",
                             "import time; time.sleep(60)"])
    try:
        assert pid_alive(proc.pid)
    finally:
        proc.kill()
        proc.wait()
    # dead: on Windows, its process object outlives it while `proc`
    # holds a handle, with an exit code (not STILL_ACTIVE)
    assert not pid_alive(proc.pid)
    pid = proc.pid
    del proc
    assert not pid_alive(pid)
    # invalid pids count as alive (never delete a record because of one)
    for pid in (None, "x", -1, 2 ** 64):
        assert pid_alive(pid)


def test_run_lock_without_backend_raises(state, monkeypatch):
    monkeypatch.delenv("BRAINSPRESSO_LOCKS", raising=False)
    monkeypatch.setattr(locks, "_MECHANISM", None)
    with pytest.raises(LockError, match="BRAINSPRESSO_LOCKS=0"):
        with run_lock(state, "harvest"):
            raise AssertionError("not reached")
    assert read_records(state) == []
    monkeypatch.setenv("BRAINSPRESSO_LOCKS", "0")
    with run_lock(state, "harvest") as held:
        assert held == []
