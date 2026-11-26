"""Stage-agnostic engine (`actions.engine`): context, policy, shards."""
import ast
import json
import os
import tempfile
import threading
import time
from pathlib import Path

import pytest

import brainspresso.actions as actions_pkg
import brainspresso.actions.engine as engine_module
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


@pytest.mark.parametrize("pause", [0, 0.4])
def test_crashed_shard_without_state_is_an_error(tmp_path, monkeypatch,
                                                 pause):
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
