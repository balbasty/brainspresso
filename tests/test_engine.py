"""Stage-agnostic engine (`actions.engine`): context, policy, shards."""
import ast
import json
from pathlib import Path

import pytest

import brainspresso.actions as actions_pkg
from brainspresso.actions import (
    Action, CopyJSON, Fail, IfExists, WriteJSON, WriteText, WriteTSV,
)
from brainspresso.actions.engine import (
    Context, Policy, Rejected, Shard, run,
)
from brainspresso.actions.state import read_task_states

ACTIONS = Path(actions_pkg.__file__).parent
# deprecated re-exports of the modules moved to `brainspresso.roast`
SHIMS = {"runner.py", "helpers.py"}


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
        if path.name in SHIMS:
            continue
        bad = {m for m in _imports(path)
               if m.startswith(("brainspresso.bids", "brainspresso.roast",
                                "brainspresso.datasets"))}
        if bad:
            offenders[path.name] = sorted(bad)
    assert offenders == {}


def test_actions_core_has_no_bids_constants():
    for path in sorted(ACTIONS.glob("*.py")):
        if path.name in SHIMS:
            continue
        source, name = path.read_text(), path.name
        for word in ("participants", "dataset_description", "phenotype",
                     "'sidecar'", '"sidecar"'):
            assert word not in source, (name, word)


def test_moved_symbols_keep_their_old_paths():
    from brainspresso.actions import helpers, runner
    from brainspresso.actions.action import default_kind
    from brainspresso import bids, roast
    import brainspresso.bids.kinds  # noqa: F401
    import brainspresso.bids.harvest as harvest
    import brainspresso.actions.state as state
    assert default_kind is bids.kinds.default_kind
    assert runner.RoastContext is roast.RoastContext
    assert runner.run_roast is roast.run_roast
    assert runner.roast_dataset is roast.roast_dataset
    assert runner.make_context is roast.make_context
    assert runner.as_list is roast.as_list
    assert runner.Shard is roast.Shard
    assert runner.LocalExecutor is roast.LocalExecutor
    assert runner.RoastReport is roast.RoastReport
    assert helpers.copy_templates is roast.copy_templates
    assert helpers.subject_shards is roast.subject_shards
    for name in ("task_path", "write_task_state", "read_task_states",
                 "now"):
        assert getattr(harvest, name) is getattr(state, name)


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
