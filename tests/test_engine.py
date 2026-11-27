"""Stage-agnostic engine (`actions.engine`): context, policy, shards."""
import ast
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import Future
from pathlib import Path

import pytest

import brainspresso.actions as actions_pkg
import brainspresso.actions.engine as engine_module
import brainspresso.actions.state as state_module
from brainspresso.actions import (
    Action,
    CopyJSON,
    Fail,
    IfExists,
    WriteJSON,
    WriteText,
    WriteTSV,
)
from brainspresso.actions.engine import (
    Context,
    Policy,
    Rejected,
    Shard,
    run,
)
from brainspresso.actions.state import read_task_states, write_task_state

ACTIONS = Path(actions_pkg.__file__).parent


def _imports(path: Path) -> set[str]:
    """Modules imported by a module (except in a lazy `__getattr__`)."""
    found = set()

    def visit(node):
        if isinstance(node, ast.FunctionDef) and node.name == "__getattr__":
            return
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(ast.parse(path.read_text()))
    return found


def test_actions_core_knows_nothing_of_bids_or_roast():
    offenders = {}
    for path in sorted(ACTIONS.glob("*.py")):
        bad = {m for m in _imports(path)
               if m.startswith(("brainspresso.bids", "brainspresso.roast",
                                "brainspresso.datasets"))}
        if bad:
            offenders[path.name] = sorted(bad)
    assert offenders == {}


def test_actions_core_has_no_bids_constants():
    for path in sorted(ACTIONS.glob("*.py")):
        source, name = path.read_text(), path.name
        for word in ("participants", "dataset_description", "phenotype",
                     "'sidecar'", '"sidecar"'):
            assert word not in source, (name, word)


# ----------------------------------------------------------------------
#   A generic run (no BIDS, no roast)
# ----------------------------------------------------------------------

def unit(ctx, i):
    yield WriteText(f"unit {i}", ctx.root / "out" / f"unit-{i}.txt",
                    tag="units")
    ctx.progress(advance=1, tag="units")


def produce(ctx):
    ctx.progress(total=3, tag="units")
    yield WriteText("top", ctx.root / "out" / "top.txt", kind="text")
    for i in range(3):
        yield Shard(unit, i, id=f"demo/unit-{i}", unit={"unit": str(i)},
                    tag="units")


@pytest.mark.parametrize("jobs", [1, 2])
def test_generic_run(tmp_path, jobs):
    ctx = Context(tmp_path, state=tmp_path / "state", stage="demo",
                  tag="top")
    report = run(produce, ctx, jobs=jobs, display=False)
    assert report.status == "complete" and not report.errors
    assert report.summary() == {"top": {"done": 1}, "units": {"done": 3}}
    assert report.progress["units"] == (3, 3)
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == [
        "top.txt", "unit-0.txt", "unit-1.txt", "unit-2.txt"]
    states = read_task_states(tmp_path / "state")
    assert set(states) == {f"demo/unit-{i}" for i in range(3)}
    record = states["demo/unit-1"]
    assert record["stage"] == "demo" and record["status"] == "done"
    assert record["unit"] == {"unit": "1"}
    assert record["outputs"] == ["out/unit-1.txt"]


def test_generic_run_without_state(tmp_path):
    ctx = Context(tmp_path / "root")
    report = run(produce, ctx, display=False)
    assert report.status == "complete"
    assert sorted(p.name for p in (tmp_path / "root").iterdir()) == ["out"]


class OnlyTxt(Policy):
    """Accept `.txt` outputs, reject `.bad`, force "overwrite"."""

    def accept(self, action, ctx, *, staging=False):
        names = [p.name for p in action.declared]
        if any(n.endswith(".bad") for n in names):
            raise Rejected("bad outputs")
        return not names or all(n.endswith(".txt") for n in names)

    def prepare(self, action, ctx):
        action.ifexists = IfExists.from_any("overwrite")
        seen.append(action.declared[0].name if action.declared else None)

    def fingerprint(self, ctx):
        return {"only": "txt"}


seen = []


def test_policy_decides(tmp_path):
    seen.clear()
    out = tmp_path / "out"

    def producer(ctx):
        yield WriteText("a", out / "a.txt")
        yield WriteText("b", out / "b.dat")
        yield WriteText("c", out / "c.bad")
        yield Fail("missing input", name="d")

    report = run(producer, Context(tmp_path), policy=OnlyTxt(),
                 display=False)
    assert sorted(p.name for p in out.iterdir()) == ["a.txt"]
    assert report.counts["run"] == {"done": 1, "error": 2}
    assert sorted(e[2] for e in report.errors) == [
        "bad outputs", "missing input"]
    assert seen == ["a.txt", None]


def test_default_policy_uses_ifexists_of_context(tmp_path):
    dst = tmp_path / "x.txt"
    dst.write_text("old")

    def producer(ctx):
        yield WriteText("new", dst)

    report = run(producer, Context(tmp_path, ifexists="skip"),
                 display=False)
    assert report.counts["run"] == {"skipped": 1}
    assert dst.read_text() == "old"
    report = run(producer, Context(tmp_path, ifexists="overwrite"),
                 display=False)
    assert report.counts["run"] == {"done": 1}
    assert dst.read_text() == "new"


def test_compare_is_opt_in(tmp_path):
    """`compare` regenerates and compares under "different"."""
    dst = tmp_path / "t.tsv"
    dst.write_text("old")

    def make(**kw):
        return Action([], dst, lambda f: f.write(b"new"), **kw)

    action = make(ifexists="different")
    action()
    assert dst.read_text() == "old"         # exists, no size: skipped
    make(ifexists="different", compare=True)()
    assert dst.read_text() == "new"         # regenerated: different
    action = make(ifexists="different", compare=True)
    statuses = list(action)
    assert {"status": "skipped", "message": "identical"} in statuses


def test_table_writers_compare_by_default(tmp_path):
    """`WriteTSV` compares without a layer; sidecars opt in."""
    dst = tmp_path / "t.tsv"

    def write(value, **kw):
        return list(WriteTSV([["a"], [value]], dst, ifexists="different",
                             **kw))

    write("1")
    first = dst.stat()
    assert {"status": "skipped", "message": "identical"} in write("1")
    assert dst.stat().st_ino == first.st_ino            # untouched
    write("2")
    assert dst.read_text().split() == ["a", "2"]        # regenerated
    assert write("3", compare=False)[-1]["status"] == "skipped"
    assert dst.read_text().split() == ["a", "2"]
    json = WriteJSON({}, tmp_path / "x.json")
    assert not json.compare and not json.compare_given
    assert WriteJSON({}, tmp_path / "x.json", compare=True).compare
    assert CopyJSON(tmp_path / "x.json", tmp_path / "y.json",
                    compare=True).compare


def test_shard_signature_includes_policy_fingerprint(tmp_path):
    src = tmp_path / "src.txt"
    src.write_text("input")

    def shard(ctx):
        yield WriteText("x", ctx.root / "out" / "x.txt")

    def producer(ctx):
        yield Shard(shard, id="demo/x", inputs=[src])

    ctx = Context(tmp_path, state=tmp_path / "state")
    run(producer, ctx, display=False)
    first = json.loads(next((tmp_path / "state").rglob("*.json"))
                       .read_text())["config_hash"]
    report = run(producer, ctx, display=False)          # unchanged: skip
    assert report.counts["run"] == {"skipped": 1}
    report = run(producer, ctx, policy=OnlyTxt(), display=False)
    second = json.loads(next((tmp_path / "state").rglob("*.json"))
                        .read_text())["config_hash"]
    assert first != second


# ----------------------------------------------------------------------
#   Pool failures, finalize, children, task state writes
# ----------------------------------------------------------------------

def crash_in_worker(ctx, i, main_pid):
    if os.getpid() != main_pid:
        if i == 1:
            os._exit(1)         # a worker dies (segfault, OOM killer...)
        # held until the broken pool kills its worker
        time.sleep(60)
    elif i == 1:
        # run again in the main process, it would crash it as well
        (ctx.root / "rerun").touch()
        return
    yield WriteText(str(os.getpid()), ctx.root / "out" / f"{i}.txt",
                    tag="units")


@pytest.mark.parametrize("state", ["none", "unwritable"])
@pytest.mark.parametrize("pause", [0, 0.4])
def test_crashed_shard_without_state_is_an_error(tmp_path, monkeypatch,
                                                 pause, state):
    markers = tmp_path / "tmp"
    markers.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(markers))
    seen = []
    mkdtemp = tempfile.mkdtemp

    def spy(*args, **kwargs):
        seen.append(mkdtemp(*args, **kwargs))
        return seen[-1]

    monkeypatch.setattr(tempfile, "mkdtemp", spy)
    main = os.getpid()

    def producer(ctx):
        for i in range(6):
            yield Shard(crash_in_worker, i, main, tag="units")
            # (pause: the pool breaks while the producer yields shards)
            time.sleep(pause)

    if state == "unwritable":
        # no state file can be written, from before the shards start
        # (a file in the place of the tasks folder: even root fails)
        (tmp_path / "state").mkdir()
        (tmp_path / "state" / "tasks").write_text("")
        ctx = Context(tmp_path / "root", state=tmp_path / "state")
    else:
        ctx = Context(tmp_path / "root")
    report = run(producer, ctx, jobs=2, display=False)
    assert report.status == "partial"
    # the shard that crashed its worker is not run again
    assert not (tmp_path / "root" / "rerun").exists()
    died = [e for e in report.errors if "worker process died" in e[2]]
    assert len(report.errors) == len(died)
    assert f"crash_in_worker(1, {main})" in {e[1] for e in died}
    assert report.counts["units"]["done"] + len(died) == 6
    # shards that had not started ran in the main process
    assert (tmp_path / "root" / "out" / "5.txt").read_text() == str(main)
    # no start marker left (other files: lock files outside datasets)
    assert any("bx-shards-" in path for path in seen)
    assert not list(markers.glob("bx-shards-*"))


def leaf(ctx, name):
    yield WriteText(name, ctx.root / "out" / f"{name}.txt")


def leaf_finalize(ctx, name):
    with open(ctx.root / "finalized.txt", "a") as f:
        f.write(f"{name}\n")


leaf.finalize = leaf_finalize


@pytest.mark.parametrize("jobs", [1, 2])
def test_finalize_of_a_skipped_shard_runs(tmp_path, jobs):
    src = tmp_path / "src.txt"
    src.write_text("input")

    def producer(ctx):
        yield Shard(leaf, "a", id="demo/a", inputs=[src])

    ctx = Context(tmp_path, state=tmp_path / "state")
    report = run(producer, ctx, jobs=jobs, display=False)
    assert report.counts["run"] == {"done": 1}
    report = run(producer, ctx, jobs=jobs, display=False)
    assert report.counts["run"] == {"skipped": 1}
    # once per run, run or skipped
    assert (tmp_path / "finalized.txt").read_text() == "a\na\n"


def parent(ctx):
    for name in ("x", "y"):
        yield Shard(leaf, name)


@pytest.mark.parametrize("jobs", [1, 2])
def test_children_of_a_shard_are_counted_once(tmp_path, jobs):
    def producer(ctx):
        yield Shard(parent, id="demo/parent")

    ctx = Context(tmp_path, state=tmp_path / "state")
    report = run(producer, ctx, jobs=jobs, display=False)
    assert report.status == "complete"
    assert read_task_states(tmp_path / "state")["demo/parent"][
        "children"] == 2


def test_concurrent_writes_of_a_task_state(tmp_path):
    errors = []

    def write(n):
        try:
            for i in range(200):
                write_task_state(tmp_path, "demo/x", {"n": n, "i": i})
        except OSError as e:
            errors.append(e)

    threads = [threading.Thread(target=write, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert errors == []
    assert set(read_task_states(tmp_path)) == {"demo/x"}
    assert [p.name for p in (tmp_path / "tasks").iterdir()] == \
        ["demo%2Fx.json"]


def test_worker_death_with_an_unwritable_state(tmp_path, monkeypatch,
                                               caplog):
    ctx = Context(tmp_path, state=tmp_path / "state")
    reporter = engine_module._MainReporter(root=tmp_path)
    engine = engine_module.Engine(ctx, reporter, run="r1")
    write_task_state(ctx.state, "demo/a", {"run": "r1"})

    def fail(*args, **kwargs):
        raise OSError("read-only state folder")

    monkeypatch.setattr(engine_module, "write_task_state", fail)
    pending = engine_module._Pending(Shard(leaf, "a"), "run", "demo/a",
                                     False)
    engine.broken(pending)
    assert [e[2] for e in reporter.report.errors] == [
        "the worker process died while this shard ran (it, or a shard "
        "running at the same time, crashed it)"]
    assert "Could not write the state of demo/a" in caplog.text
    assert not (tmp_path / "out").exists()          # not run again


def test_handed_back_children_are_counted_on_their_parent(tmp_path):
    out = tmp_path / "out"

    def local(ctx):
        # unpicklable (a closure): runs in the main process, while the
        # worker of `parent` hands back its children (run in the pool)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            yield WriteText("busy", ctx.root / "busy.txt")
            if (out / "x.txt").exists() and (out / "y.txt").exists():
                return
            time.sleep(0.05)
        raise TimeoutError("the children did not run")

    def producer(ctx):
        yield Shard(parent, id="demo/parent")
        yield Shard(local, id="demo/local")

    ctx = Context(tmp_path, state=tmp_path / "state")
    report = run(producer, ctx, jobs=2, display=False)
    assert not report.errors
    states = read_task_states(tmp_path / "state")
    assert states["demo/parent"]["children"] == 2
    assert states["demo/local"]["children"] == 0


class InlineExecutor:
    """An executor of the submit protocol that is not the local pool."""

    def __init__(self):
        self.tasks = []

    def submit(self, task, resources=None):
        self.tasks.append(task)
        future = Future()
        future.set_result(task())
        return future

    def shutdown(self):
        pass


def test_other_executors_get_no_start_markers(tmp_path, caplog):
    executor = InlineExecutor()

    def producer(ctx):
        for name in ("a", "b"):
            yield Shard(leaf, name)

    report = run(producer, Context(tmp_path), executor=executor,
                 display=False)
    assert report.summary() == {"run": {"done": 2}}
    assert [task.started for task in executor.tasks] == [None, None]
    assert "Could not mark" not in caplog.text


def test_stale_start_markers_are_removed(tmp_path, monkeypatch):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    proc = subprocess.run([sys.executable, "-c",
                           "import os; print(os.getpid())"],
                          capture_output=True, text=True, check=True)
    dead = int(proc.stdout)
    host = engine_module._host()
    stale = tmp_path / f"bx-shards-{host}-{dead}-abc"
    alive = tmp_path / f"bx-shards-{host}-{os.getppid()}-def"
    other = tmp_path / f"bx-shards-another_host-{dead}-abc"
    old = tmp_path / f"bx-shards-{dead}-abc"       # (older format)
    for folder in (stale, alive, other, old):
        folder.mkdir()
        (folder / "1.started").touch()
    folder = engine_module._marker_folder()
    assert folder.name.startswith(f"bx-shards-{host}-{os.getpid()}-")
    assert not stale.exists() and folder.is_dir()
    # only the dead pids of this host
    assert alive.exists() and other.exists() and old.exists()


@pytest.mark.parametrize("state", [None, "state"])
def test_marker_failures_warn_only_without_state(tmp_path, monkeypatch,
                                                 caplog, state):
    (tmp_path / "file").write_text("")
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "file"))
    if state is not None:
        state = tmp_path / state
        state.mkdir()
    with caplog.at_level("DEBUG", logger="brainspresso.actions.engine"):
        assert engine_module._marker_folder(state) is None
    [record] = [r for r in caplog.records if "start markers" in r.message]
    assert record.levelname == ("WARNING" if state is None else "DEBUG")


def test_stale_task_state_temporary_files_are_removed(tmp_path,
                                                      monkeypatch):
    monkeypatch.setattr(state_module, "_swept", set())
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    stale, fresh = tasks / ".1-dead.tmp", tasks / ".2-live.tmp"
    old = tasks / "demo%2Fx.json.tmp"      # (name of an older version)
    mine = tasks / f".{os.getpid()}-mine.tmp"
    for path in (stale, fresh, old, mine):
        path.write_text("{")
    past = time.time() - state_module.STALE_TMP_AGE - 60
    for path in (stale, old, mine):
        os.utime(path, (past, past))
    write_task_state(tmp_path, "demo/x", {})
    assert sorted(p.name for p in tasks.iterdir()) == sorted([
        mine.name, ".2-live.tmp", "demo%2Fx.json"])
    assert state_module.STALE_TMP_AGE >= 24 * 3600


def test_task_state_of_a_long_task_id(tmp_path):
    task = "x" * 245              # the longest file name: 250 characters
    path = write_task_state(tmp_path, task, {"status": "done"})
    assert len(path.name) == 250
    assert read_task_states(tmp_path)[task]["status"] == "done"


def finalized_parent(ctx):
    yield from ()


# an unpicklable hook: it runs in the worker of its shard
finalized_parent.finalize = lambda ctx: iter([Shard(leaf, "z")])


def test_children_of_a_finalize_run_in_a_worker_are_counted(tmp_path):
    def producer(ctx):
        yield Shard(finalized_parent, id="demo/fp")

    ctx = Context(tmp_path, state=tmp_path / "state")
    report = run(producer, ctx, jobs=2, display=False)
    assert not report.errors
    assert (tmp_path / "out" / "z.txt").exists()
    assert read_task_states(tmp_path / "state")["demo/fp"]["children"] == 1


# ----------------------------------------------------------------------
#   Scope of shards (`Shard(scope=)`, `Policy.scope/covers/expects/audit`)
# ----------------------------------------------------------------------

class ToyContext(Context):
    """A run that wants some units (`wanted`)."""

    def __init__(self, root, wanted, **kwargs):
        super().__init__(root, state=Path(root) / "state", **kwargs)
        self.wanted = set(wanted)


class ToyPolicy(Policy):
    """
    A scope: the wanted units. Covered if every unit that the shard saw
    and the new run wants was wanted (or, without `seen`, by inclusion);
    outputs `out/<unit>.txt` are expected if their unit is wanted.
    """

    def fingerprint(self, ctx):
        return {"wanted": sorted(ctx.wanted)}

    def scope(self, ctx, shard):
        if getattr(shard, "scope", None) is None:
            return None
        return {"wanted": sorted(ctx.wanted)}

    def covers(self, old, new, seen):
        if "u" in seen:
            return all(u in old["wanted"] for u in seen["u"]
                       if u in new["wanted"])
        return set(new["wanted"]) <= set(old["wanted"])

    def expects(self, path, ctx):
        return Path(path).stem in ctx.wanted

    def audit(self, ctx, scope, outputs, seen):
        for output in outputs:
            if Path(output).stem not in seen.get("u", ()):
                return f"{output}: unit not seen"
        return None


def toy_shard(ctx, units, unseen=(), fail=None, error=False):
    for unit in units:
        if unit not in unseen:
            ctx.seen("u", unit)
        if unit in ctx.wanted:
            yield WriteText(unit, ctx.root / "out" / f"{unit}.txt")
        if unit == fail:
            yield Fail("missing input")
        if error and unit == units[-1]:
            raise ValueError("boom")


calls = []


def counted_shard(ctx, *args, **kwargs):
    calls.append(args[0])
    yield from toy_shard(ctx, *args, **kwargs)


def toy_producer(units=("a", "b"), scoped=True, **kwargs):
    def producer(ctx):
        yield Shard(counted_shard, list(units), id="demo/toy",
                    inputs=[ctx.root / "src.txt"],
                    scope={"toy": True} if scoped else None, **kwargs)
    return producer


def toy_run(tmp_path, wanted, *, jobs=1, **kwargs):
    if not (tmp_path / "src.txt").exists():
        (tmp_path / "src.txt").write_text("input")
    ctx = ToyContext(tmp_path, wanted)
    return run(toy_producer(**kwargs), ctx, policy=ToyPolicy(), jobs=jobs,
               display=False)


def toy_record(tmp_path):
    return read_task_states(tmp_path / "state")["demo/toy"]


@pytest.fixture
def counted():
    calls.clear()
    return calls


def test_undeclared_shard_signature_is_unchanged(tmp_path, counted):
    """Without a scope: the fingerprint is in the signature, equality"""
    toy_run(tmp_path, {"a", "b"}, scoped=False)
    record = toy_record(tmp_path)
    src = tmp_path / "src.txt"
    stat = src.stat()
    blob = json.dumps({          # (the formula of before scopes)
        "version": 1, "inputs": [[str(src), stat.st_size,
                                  stat.st_mtime_ns]],
        "wanted": ["a", "b"]}, sort_keys=True, default=str)
    assert record["config_hash"] == hashlib.sha256(blob.encode()).hexdigest()
    assert "scopes" not in record and "seen" not in record
    toy_run(tmp_path, {"a", "b"}, scoped=False)
    assert len(counted) == 1
    toy_run(tmp_path, {"a"}, scoped=False)              # narrower: runs
    assert len(counted) == 2


def test_scoped_shard_signature_has_no_fingerprint(tmp_path):
    (tmp_path / "src.txt").write_text("input")
    shard = Shard(toy_shard, id="demo/toy", inputs=[tmp_path / "src.txt"],
                  scope={"toy": True})
    sign = engine_module._signature
    one, two = ToyContext(tmp_path, {"a"}), ToyContext(tmp_path, {"b"})
    scope = {"wanted": ["a"]}
    assert sign(one, ToyPolicy(), shard, scope) == \
        sign(two, ToyPolicy(), shard, scope)
    assert sign(one, ToyPolicy(), shard) != sign(two, ToyPolicy(), shard)
    assert sign(one, ToyPolicy(), shard, scope) != \
        sign(one, ToyPolicy(), shard)


@pytest.mark.parametrize("jobs", [1, 2])
def test_scoped_shard_skipped_when_covered(tmp_path, counted, jobs):
    units = ("a", "b", "c")
    toy_run(tmp_path, {"a", "b"}, units=units, jobs=jobs)
    record = toy_record(tmp_path)
    assert record["scopes"] == [{"wanted": ["a", "b"]}]
    assert record["seen"] == {"u": ["a", "b", "c"]}
    # narrower, or a unit that the shard does not hold: skipped
    report = toy_run(tmp_path, {"a"}, units=units, jobs=jobs)
    # (what it did last time is counted; its expected outputs reported)
    assert report.counts["run"] == {"skipped": 2}
    assert report.outputs == [tmp_path / "out" / "a.txt"]
    report = toy_run(tmp_path, {"a", "b", "z"}, units=units, jobs=jobs)
    assert report.counts["run"] == {"skipped": 2}
    assert toy_record(tmp_path)["scopes"] == [{"wanted": ["a", "b"]}]
    # a unit that it holds: runs (and extends its record)
    report = toy_run(tmp_path, {"a", "c"}, units=units, jobs=jobs)
    assert report.counts["run"] == {"done": 1, "skipped": 1}
    assert (tmp_path / "out" / "c.txt").exists()
    record = toy_record(tmp_path)
    assert record["scopes"] == [{"wanted": ["a", "b"]}, {"wanted": ["a", "c"]}]
    assert record["outputs"] == ["out/a.txt", "out/b.txt", "out/c.txt"]
    assert record["seen"] == {"u": ["a", "b", "c"]}
    if jobs == 1:
        assert len(counted) == 2


def test_scoped_shard_expects_some_outputs(tmp_path, counted):
    toy_run(tmp_path, {"a", "b"})
    # an output that the run does not expect is gone: skipped
    (tmp_path / "out" / "b.txt").unlink()
    toy_run(tmp_path, {"a"})
    assert len(counted) == 1
    # an expected one is gone: runs, writes it again
    toy_run(tmp_path, {"a", "b"})
    assert len(counted) == 2 and (tmp_path / "out" / "b.txt").exists()
    (tmp_path / "out" / "a.txt").unlink()
    toy_run(tmp_path, {"a"})
    assert len(counted) == 3 and (tmp_path / "out" / "a.txt").exists()


def test_scopes_are_carried(tmp_path, counted):
    """`a`, then `b`, then `a` again: the third run skips"""
    toy_run(tmp_path, {"a"})
    toy_run(tmp_path, {"b"})
    record = toy_record(tmp_path)
    assert record["scopes"] == [{"wanted": ["a"]}, {"wanted": ["b"]}]
    assert record["outputs"] == ["out/a.txt", "out/b.txt"]
    toy_run(tmp_path, {"a"})
    assert len(counted) == 2
    # the same scope again moves last
    toy_run(tmp_path, {"a", "b"})
    toy_run(tmp_path, {"a"}, config=1)                  # (re-signed)
    assert toy_record(tmp_path)["scopes"] == [{"wanted": ["a"]}]


def test_scopes_are_carried_only_if_all_outputs_exist(tmp_path):
    toy_run(tmp_path, {"a"})
    (tmp_path / "out" / "a.txt").unlink()
    toy_run(tmp_path, {"b"})
    record = toy_record(tmp_path)
    assert record["scopes"] == [{"wanted": ["b"]}]
    assert record["outputs"] == ["out/b.txt"]


def test_at_most_eight_scopes(tmp_path):
    units = [str(i) for i in range(10)]
    for unit in units:
        toy_run(tmp_path, {unit}, units=units)
    scopes = toy_record(tmp_path)["scopes"]
    assert scopes == [{"wanted": [u]} for u in units[2:]]
    assert len(toy_record(tmp_path)["outputs"]) == 10


@pytest.mark.parametrize("how", ["fail", "error"])
def test_no_scope_after_an_error(tmp_path, counted, how):
    kwargs = {"fail": "a"} if how == "fail" else {"error": True}
    report = toy_run(tmp_path, {"a", "b"}, **kwargs)
    assert report.errors
    record = toy_record(tmp_path)
    assert record["status"] == "failed"
    assert "scopes" not in record and "seen" not in record
    toy_run(tmp_path, {"a"}, **kwargs)
    assert len(counted) == 2


def test_a_failed_audit_is_never_skipped(tmp_path, counted, caplog):
    with caplog.at_level("WARNING"):
        toy_run(tmp_path, {"a", "b"}, unseen=("b",))
    assert "out/b.txt: unit not seen" in caplog.text
    record = toy_record(tmp_path)
    assert record["status"] == "done" and record["scopes"] == []
    toy_run(tmp_path, {"a", "b"}, unseen=("b",))
    toy_run(tmp_path, {"a"}, unseen=("b",))
    assert len(counted) == 3


def test_seen_without_a_task_does_nothing(tmp_path):
    ctx = Context(tmp_path)
    ctx.seen("sub", "01")               # (no reporter)

    def producer(ctx):
        ctx.seen("sub", "01")           # (no task)
        yield WriteText("x", ctx.root / "x.txt")

    assert not run(producer, ctx, display=False).errors


def test_legacy_record_is_upgraded(tmp_path, counted):
    """A record of before scopes: covered if made with this selection"""
    toy_run(tmp_path, {"a", "b"}, scoped=False)
    legacy = toy_record(tmp_path)
    report = toy_run(tmp_path, {"a", "b"})
    assert report.counts["run"] == {"skipped": 2} and len(counted) == 1
    record = toy_record(tmp_path)
    assert record["config_hash"] != legacy["config_hash"]
    assert record["scopes"] == [{"wanted": ["a", "b"]}]
    assert "seen" not in record
    assert record["outputs"] == legacy["outputs"]
    # (then by coverage, without `seen`: by inclusion)
    toy_run(tmp_path, {"a"})
    assert len(counted) == 1
    toy_run(tmp_path, {"a", "z"})
    assert len(counted) == 2


def test_legacy_record_of_another_selection_runs(tmp_path, counted):
    toy_run(tmp_path, {"a", "b"}, scoped=False)
    toy_run(tmp_path, {"a"})
    assert len(counted) == 2
    record = toy_record(tmp_path)
    assert record["scopes"] == [{"wanted": ["a"]}]


def interrupted(ctx):
    yield WriteText("x", ctx.root / "out" / "x.txt")
    raise KeyboardInterrupt


def test_interrupted_shard_is_not_done(tmp_path):
    (tmp_path / "src.txt").write_text("input")

    def producer(ctx):
        yield Shard(interrupted, id="demo/stop", inputs=[ctx.root / "src.txt"])

    ctx = Context(tmp_path, state=tmp_path / "state")
    with pytest.raises(KeyboardInterrupt):
        run(producer, ctx, display=False)
    assert read_task_states(tmp_path / "state")["demo/stop"]["status"] == \
        "failed"
