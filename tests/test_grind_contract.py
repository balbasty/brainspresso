"""
Grind contract (step 4g): the stubs of `brainspresso.grind`.

Nothing is implemented yet; these tests pin the names, signatures and
layering that `docs/design/architecture.md` (§7) specifies, so that
step 7 fills the bodies without moving the contract.
"""
import ast
import dataclasses
import importlib
import inspect
import pkgutil
import re
import sys
import typing
from concurrent.futures import Future
from pathlib import Path

import pytest

import brainspresso.actions as actions_pkg
import brainspresso.grind as grind
from brainspresso.actions.engine import Context, LocalExecutor, Policy
from brainspresso.bids.layout import make_layout
from brainspresso.grind import (
    containers,
    context,
    execution,
    graph,
    index,
    model,
    provenance,
    run,
)

ROOT = Path(grind.__file__).parents[2]
GRIND = Path(grind.__file__).parent
ACTIONS = Path(actions_pkg.__file__).parent
MODULES = sorted(info.name for info in pkgutil.iter_modules(
    [str(GRIND)], "brainspresso.grind."))


def imports(path: Path) -> set[str]:
    """Modules that a file imports (relative ones: its own package)."""
    found = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            found.add(node.module)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 1, f"import from a parent in {path}"
            found.add(f"brainspresso.{path.parent.name}")
    return found


def params(fn) -> list[str]:
    return list(inspect.signature(fn).parameters)


# ----------------------------------------------------------------------
#   Package
# ----------------------------------------------------------------------

def test_modules():
    assert [m.rpartition(".")[2] for m in MODULES] == [
        "bidsapp", "containers", "context", "execution", "graph", "index",
        "model", "overrides", "provenance", "run",
    ]


@pytest.mark.parametrize("name", MODULES)
def test_public_names_are_documented(name):
    module = importlib.import_module(name)
    assert module.__doc__ and "Interfaces only" in module.__doc__
    for public in module.__all__:
        obj = getattr(module, public)
        if inspect.isclass(obj) or inspect.isfunction(obj):
            assert inspect.getdoc(obj), f"{name}.{public}"
            if obj.__module__ == name:
                assert public in grind.__all__ or public in (
                    "Level", "Need", "Pick", "StageFunction", "Verdict",
                    "IfStale"), public


def test_package_exports():
    assert sorted(grind.__all__) == sorted(set(grind.__all__))
    for name in grind.__all__:
        assert hasattr(grind, name), name


def test_layering():
    """`actions/` never imports grind; grind imports no optional module."""
    for path in ACTIONS.glob("*.py"):
        assert not any(m.startswith("brainspresso.grind")
                       for m in imports(path)), path
    allowed = ("brainspresso.actions", "brainspresso.bids",
               "brainspresso.grind", "brainspresso.errors")
    for path in GRIND.glob("*.py"):
        for module in imports(path):
            top = module.split(".")[0]
            if top == "brainspresso":
                assert module.startswith(allowed), (path.name, module)
            else:
                assert top in sys.stdlib_module_names, (path.name, module)


def test_docstrings_enforced():
    text = (ROOT / "pyproject.toml").read_text()
    pattern = next(line for line in text.splitlines()
                   if line.startswith('"!brainspresso/{'))
    assert "grind/**" in pattern and pattern.endswith('= ["D"]')


def test_design_document_names_the_stubs():
    text = (ROOT / "docs" / "design" / "architecture.md").read_text()
    section = text[text.index("## 7. Grind"):text.index("## 8. ")]
    for name in (
        "Pipeline", "Stage", "FunctionStage", "CommandStage", "BidsAppStage",
        "Requirement", "Query", "Output", "Unit", "LEVELS", "Config", "Work",
        "Resources", "format_output", "register_pipeline", "get_pipeline",
        "enumerate_units", "task_id", "signature", "Task", "Graph",
        "GrindContext", "GrindPolicy", "Executor", "Handle", "Profile",
        "BidsAppPipeline", "BidsAppConfig", "Container", "ContainerRuntime",
        "GrindOverrides", "DatasetRef", "build_index", "grind_dataset",
        "grind_collection", "GrindReport",
    ):
        assert hasattr(grind, name), name
        assert re.search(rf"\b{name}\b", section), name
    for field in graph.GRIND_FIELDS:
        assert f'"{field}"' in section, field


# ----------------------------------------------------------------------
#   Model
# ----------------------------------------------------------------------

def test_levels():
    assert dict(model.LEVELS) == {
        "session": ("sub", "ses"), "subject": ("sub",), "dataset": (),
        "group": (),
    }
    with pytest.raises(TypeError):
        model.LEVELS["run"] = ("sub", "ses", "run")


def fields(cls) -> dict:
    return {f.name: f.default for f in dataclasses.fields(cls)}


def test_dataclasses():
    assert list(fields(model.Unit)) == [
        "level", "dataset", "sub", "ses", "extra"]
    unit = model.Unit("session", "ixi", "01", "1")
    assert hash(unit) == hash(model.Unit("session", "ixi", "01", "1"))
    with pytest.raises(dataclasses.FrozenInstanceError):
        unit.sub = "02"

    req = fields(model.Requirement)
    assert list(req) == ["name", "query", "pipeline", "stage", "outputs",
                         "min", "max", "pick", "need"]
    assert (req["min"], req["max"], req["pick"], req["need"]) == (
        1, 1, "error", "any")
    assert list(fields(model.Query)) == [
        "datatypes", "suffixes", "extensions", "entities", "include",
        "exclude", "metadata"]
    assert list(fields(model.Output)) == [
        "name", "template", "source", "claim", "optional"]
    assert list(fields(model.Resources)) == [
        "cpus", "memory_gb", "gpus", "walltime_min", "scratch_gb", "extra"]
    assert list(fields(model.Work)) == [
        "unit", "config", "inputs", "outputs", "root", "workdir",
        "resources", "members", "failed"]
    assert list(fields(model.Stage)) == [
        "name", "level", "inputs", "outputs", "resources", "container"]
    assert list(fields(model.Pipeline)) == [
        "name", "version", "stages", "config", "requires", "container",
        "description", "code_url"]
    assert list(fields(grind.GrindOverrides)) == [
        "exclude_subs", "exclude", "inputs", "species", "template",
        "config", "resources"]
    assert grind.GrindOverrides() == grind.GrindOverrides()


def test_stage_kinds():
    assert inspect.isabstract(model.Stage)
    with pytest.raises(TypeError):
        model.Stage("x", "subject")
    assert params(model.Stage.run) == ["self", "ctx", "work"]
    assert params(model.Stage.fingerprint) == ["self", "config"]
    for kind in (model.FunctionStage, model.CommandStage, grind.BidsAppStage):
        assert issubclass(kind, model.Stage)
        assert params(kind.run) == ["self", "ctx", "work"]
    assert list(fields(model.FunctionStage))[-1] == "fn"
    assert list(fields(model.CommandStage))[-2:] == ["command", "env"]


def body(ctx, work):
    yield from ()


def test_declaration_example():
    """The example of `grind.model` builds: pipelines are plain data."""
    t1w = model.Requirement("t1w", model.Query(suffixes=("T1w",)))
    pipeline = model.Pipeline("fs-long", "7.4.1", stages=(
        model.FunctionStage("cross", "session", fn=body, inputs=(t1w,)),
        model.FunctionStage("base", "subject", fn=body, inputs=(
            model.Requirement("cross", stage="cross", max=None,
                              need="all"),)),
        model.FunctionStage("long", "session", fn=body, inputs=(
            model.Requirement("cross", stage="cross"),
            model.Requirement("base", stage="base"))),
    ), requires=())
    assert [s.name for s in pipeline.stages] == ["cross", "base", "long"]
    assert pipeline.stages[1].inputs[0].need == "all"
    with pytest.raises(NotImplementedError):
        pipeline.validate()
    with pytest.raises(NotImplementedError):
        next(pipeline.stages[0].run(None, model.Work(
            model.Unit("session", sub="01"))))


def test_config_protocol():
    pydantic = pytest.importorskip("pydantic")

    class Options(pydantic.BaseModel):
        fwhm: float = 0.0

    assert isinstance(Options(), model.Config)
    assert not isinstance(object(), model.Config)


def test_bids_app():
    assert issubclass(grind.BidsAppPipeline, model.Pipeline)
    app = grind.BidsAppPipeline(
        "fmriprep", "23.2.1",
        container=containers.Container("nipreps/fmriprep:23.2.1"))
    assert (app.group, app.session_flag, app.out_subdir) == (
        False, None, None)
    assert app.claims == ("sub-{sub}/", "sub-{sub}.html")
    assert list(fields(grind.BidsAppConfig)) == [
        "args", "bids_filter", "skip_bids_validation", "files"]
    assert params(grind.BidsAppStage.command) == ["self", "ctx", "work"]


# ----------------------------------------------------------------------
#   Signatures
# ----------------------------------------------------------------------

SIGNATURES = {
    model.format_output: ["template", "unit", "fields"],
    model.register_pipeline: ["pipeline", "name"],
    model.get_pipeline: ["name"],
    graph.task_id: ["pipeline", "stage", "unit"],
    graph.enumerate_units: ["layout", "level", "selection", "overrides",
                           "dataset"],
    graph.signature: ["pipeline", "stage", "config", "inputs", "root",
                     "upstream", "container"],
    graph.plan: ["pipeline", "scope", "configs", "redo", "if_stale"],
    containers.get_runtime: ["name"],
    containers.ContainerRuntime.command: [
        "self", "container", "argv", "mounts", "env", "workdir"],
    containers.ContainerRuntime.pull: ["self", "container"],
    execution.Executor.submit: ["self", "task", "resources", "after"],
    execution.Executor.submit_array: ["self", "tasks", "resources", "after"],
    execution.Executor.shutdown: ["self"],
    execution.Profile.resources_for: ["self", "pipeline", "stage", "default"],
    execution.load_profile: ["name", "path"],
    execution.make_executor: ["profile"],
    context.GrindContext.__init__: [
        "self", "layout", "pipeline", "config", "overrides", "profile",
        "dataset", "variant"],
    context.GrindContext.required: ["self", "pipeline"],
    context.GrindContext.scratch: ["self", "task"],
    index.DatasetRef.parse: ["text"],
    index.resolve_datasets: ["refs", "root", "selection"],
    index.parse_where: ["text"],
    index.build_index: ["datasets", "where"],
    index.Where.matches: ["self", "row"],
    provenance.generated_by: ["pipeline", "container"],
    provenance.source_datasets: ["derivative", "requires", "datasets"],
    provenance.describe_derivative: [
        "derivative", "pipeline", "container", "datasets"],
    run.grind_dataset: [
        "pipeline", "layout", "dataset", "selection", "overrides", "config",
        "profile", "jobs", "redo", "if_stale", "variant", "detach",
        "dry_run", "display", "executor"],
    run.grind_collection: [
        "pipeline", "datasets", "where", "root", "out", "selection",
        "config", "profile", "jobs", "redo", "if_stale", "variant",
        "detach", "dry_run", "display", "executor"],
    run.grind_status: ["pipeline", "layout", "variant"],
}


@pytest.mark.parametrize(
    "fn", SIGNATURES, ids=lambda fn: fn.__qualname__)
def test_signature(fn):
    assert params(fn) == SIGNATURES[fn]


def test_nothing_is_implemented(tmp_path):
    unit = model.Unit("subject", sub="01")
    layout = make_layout("study", tmp_path / "DS")
    pipeline = model.Pipeline("p", "1")
    calls = [
        lambda: unit.label,
        lambda: unit.entities,
        lambda: model.Query().selection(unit),
        lambda: model.Resources().as_dict(),
        lambda: model.format_output("sub-{sub}", unit),
        lambda: model.get_pipeline("p"),
        lambda: model.register_pipeline(pipeline),
        lambda: graph.task_id("p", "s", unit),
        lambda: graph.enumerate_units(layout, "subject"),
        lambda: graph.signature(pipeline, None, None),
        lambda: graph.plan(pipeline, graph.Scope((), layout)),
        lambda: graph.Graph().waves(),
        lambda: containers.get_runtime("docker"),
        lambda: execution.load_profile(),
        lambda: execution.make_executor(execution.Profile()),
        lambda: context.GrindContext(layout, pipeline),
        lambda: context.GrindPolicy().fingerprint(None),
        lambda: index.DatasetRef.parse("openneuro/ds000224"),
        lambda: index.parse_where("age > 60"),
        lambda: index.build_index(()),
        lambda: provenance.generated_by(pipeline),
        lambda: run.grind_dataset(pipeline, layout),
        lambda: run.grind_collection(pipeline, ["ixi"]),
        lambda: run.grind_status(pipeline, layout),
    ]
    for call in calls:
        with pytest.raises(NotImplementedError):
            call()
    assert not (tmp_path / "DS").exists()
    assert model.PIPELINES == {}


# ----------------------------------------------------------------------
#   Engine seams
# ----------------------------------------------------------------------

def test_engine_layer():
    assert issubclass(context.GrindContext, Context)
    assert context.GrindContext.stage == "grind"
    assert issubclass(context.GrindPolicy, Policy)
    for name in ("accept", "prepare", "fingerprint"):
        assert params(getattr(context.GrindPolicy, name)) == \
            params(getattr(Policy, name))


def test_submit_protocol_extends_the_local_seam():
    """`Executor.submit` is `LocalExecutor.submit` plus `after`."""
    local = inspect.signature(LocalExecutor.submit).parameters
    contract = inspect.signature(execution.Executor.submit).parameters
    assert list(contract)[:len(local)] == list(local)
    assert contract["resources"].default is None
    assert contract["after"].default == ()
    assert isinstance(Future(), execution.Handle)
    assert list(fields(execution.After)) == [
        "handles", "condition", "pairwise"]
    assert execution.BACKENDS == (
        "local", "slurm", "sge", "pbs", "dask", "ray")
    assert containers.RUNTIMES == ("docker", "apptainer", "none")
    profile = execution.Profile()
    assert (profile.backend, profile.runtime, profile.detach) == (
        "local", "docker", False)
    assert profile.runtime in containers.RUNTIMES


def test_task_state_fields():
    """Grind only adds fields to the task records of `actions.state`."""
    base = {"schema", "task", "stage", "pipeline", "unit", "status",
            "config_hash", "started", "finished", "error", "outputs"}
    assert not base & set(graph.GRIND_FIELDS)
    assert graph.GRIND_FIELDS == (
        "pipeline_stage", "dataset", "requires", "version", "container",
        "job")
    assert typing.get_args(graph.Verdict) == graph.VERDICTS
    assert typing.get_args(graph.IfStale) == ("error", "redo", "keep")
    assert list(fields(graph.Task)) == [
        "id", "pipeline", "stage", "unit", "state", "root", "after",
        "after_any", "resources", "signature", "verdict", "reason"]
    assert list(fields(graph.Scope)) == ["datasets", "home", "variant"]
    assert list(fields(graph.DatasetScope)) == [
        "ref", "layout", "selection", "overrides"]
    assert run.GRIND_LOCK == "grind"
    assert list(fields(run.GrindReport)) == [
        "graph", "status", "counts", "failures", "jobs", "engine"]
