"""
Grind and percolate contract (step 4g): the stubs of
`brainspresso.grind` and `brainspresso.percolate`.

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
from pathlib import Path

import pytest

import brainspresso.actions as actions_pkg
import brainspresso.grind as grind
import brainspresso.percolate as percolate
from brainspresso.actions.engine import Context, Policy
from brainspresso.bids.layout import make_layout
from brainspresso.grind import (
    containers,
    context,
    graph,
    index,
    model,
    provenance,
    run,
    staging,
)
from brainspresso.percolate import jobs, options
from brainspresso.percolate import plan as plans
from brainspresso.percolate import run as prun

ROOT = Path(__file__).resolve().parent.parent  # repo, not site-packages
SECTION = (ROOT / "docs" / "design" / "architecture.md").read_text()
SECTION = SECTION[SECTION.index("## 7. Grind"):SECTION.index("## 8. ")]
GRIND = Path(grind.__file__).parent
PERCOLATE = Path(percolate.__file__).parent
ACTIONS = Path(actions_pkg.__file__).parent
MODULES = sorted(
    info.name
    for folder, prefix in ((GRIND, "brainspresso.grind."),
                           (PERCOLATE, "brainspresso.percolate."))
    for info in pkgutil.iter_modules([str(folder)], prefix))
#: Type aliases: public in their module, not re-exported by the package
ALIASES = ("Level", "Need", "Pick", "StageFunction", "Verdict", "IfStale",
           "planner")


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


def flat(obj) -> str:
    """Docstring of an object, on one line."""
    return " ".join(inspect.getdoc(obj).split())


def fields(cls) -> dict:
    return {f.name: f.default for f in dataclasses.fields(cls)}


# ----------------------------------------------------------------------
#   Packages
# ----------------------------------------------------------------------

def test_modules():
    assert [m.split(".", 1)[1] for m in MODULES] == [
        "grind.bidsapp", "grind.containers", "grind.context", "grind.graph",
        "grind.index", "grind.model", "grind.overrides", "grind.provenance",
        "grind.run", "grind.staging",
        "percolate.jobs", "percolate.options", "percolate.plan",
        "percolate.run",
    ]


@pytest.mark.parametrize("name", MODULES)
def test_public_names_are_documented(name):
    module = importlib.import_module(name)
    package = grind if ".grind." in name else percolate
    assert module.__doc__ and "Interfaces only" in module.__doc__
    for public in module.__all__:
        obj = getattr(module, public)
        if inspect.isclass(obj) or inspect.isfunction(obj):
            assert inspect.getdoc(obj), f"{name}.{public}"
        if getattr(obj, "__module__", name) == name:
            assert public in package.__all__ or public in ALIASES, public


@pytest.mark.parametrize("package", [grind, percolate])
def test_package_exports(package):
    assert sorted(package.__all__) == sorted(set(package.__all__))
    for name in package.__all__:
        assert hasattr(package, name), name


def check_imports(folder: Path, allowed: tuple):
    for path in folder.glob("*.py"):
        for module in imports(path):
            top = module.split(".")[0]
            if top == "brainspresso":
                assert module.startswith(allowed), (path.name, module)
            else:
                assert top in sys.stdlib_module_names, (path.name, module)


def test_layering():
    """actions < percolate < grind; no optional module is imported."""
    for path in ACTIONS.glob("*.py"):
        assert not any(
            m.startswith(("brainspresso.grind", "brainspresso.percolate"))
            for m in imports(path)), path
    base = ("brainspresso.actions", "brainspresso.bids",
            "brainspresso.errors", "brainspresso.percolate")
    check_imports(PERCOLATE, base)
    check_imports(GRIND, base + ("brainspresso.grind",))
    assert prun.PLANNERS == {
        "roast": "brainspresso.roast.plan:planner",
        "grind": "brainspresso.grind.run:planner",
    }
    assert isinstance(run.planner, prun.StagePlanner)
    assert run.planner.stage == "grind"


def test_docstrings_enforced():
    text = (ROOT / "pyproject.toml").read_text()
    pattern = next(line for line in text.splitlines()
                   if line.startswith('"!brainspresso/{'))
    assert "grind/**" in pattern and "percolate/**" in pattern
    assert pattern.endswith('= ["D"]')


def test_design_document_names_the_stubs():
    for name in (
        "Pipeline", "Stage", "FunctionStage", "CommandStage", "BidsAppStage",
        "Requirement", "Query", "Output", "Unit", "LEVELS", "Config", "Work",
        "format_output", "register_pipeline", "get_pipeline",
        "enumerate_units", "task_id", "derivative_name", "signature",
        "GrindContext", "GrindPolicy", "GrindPlanner", "BidsAppPipeline",
        "BidsAppConfig", "Container", "ContainerRuntime", "Mount",
        "GrindOverrides", "DatasetRef", "build_index", "grind_dataset",
        "grind_collection", "GrindReport", "stage_in", "stage_out",
    ):
        assert hasattr(grind, name), name
        assert re.search(rf"\b{name}\b", SECTION), name
    for name in (
        "Plan", "PlanTask", "TaskScope", "PlanError", "PLAN_SCHEMA",
        "YAML_PITFALLS", "ambiguous_scalar", "check_strings", "read_plan",
        "write_plan", "resolve_roots", "ExecutionOptions", "Profile",
        "resolve_resources", "translate", "Scheduler", "make_jobs",
        "write_jobs", "submit_jobs", "StagePlanner", "PLANNERS", "percolate",
        "run_task", "plan_status", "cancel_plan", "list_plans",
    ):
        assert hasattr(percolate, name), name
        assert re.search(rf"\b{name}\b", SECTION), name
    for field in graph.GRIND_FIELDS:
        assert f'"{field}"' in SECTION, field
    # what the plan decisions removed
    for gone in ("Executor.attach", "NotReady", "TaskLost", "TaskArray",
                 "Task.status", "submit_array"):
        assert gone not in SECTION, gone
    assert "PlanTask.verdict" in SECTION
    assert "`pipeline`: the **name**" in SECTION


def test_command_options_are_in_the_design():
    for option in (
        "--plan", "--detach", "--retry failed", "--status", "--cancel",
        "--task", "--array", "--write-jobs", "--profile", "--backend",
        "--jobs", "--partition", "--queue", "--account", "--time", "--mem",
        "--cpus", "--gpus", "--max-array", "--max-running", "--directive",
        "--container-runtime", "--path-map", "--stage", "--scratch",
        "--keep-scratch-on-failure", "--with", "--variant", "--if-stale",
    ):
        assert option in SECTION, option
    assert "abide1/NYU" in SECTION and "openneuro/ds000224" in SECTION
    assert "[abide1.grind.fs]" in SECTION
    assert "~/.brainspresso/config.toml" in SECTION


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


def test_dataclasses():
    assert list(fields(model.Unit)) == [
        "level", "dataset", "sub", "ses", "extra"]
    unit = model.Unit("session", "IXI", "01", "1")
    assert hash(unit) == hash(model.Unit("session", "IXI", "01", "1"))
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
    assert model.Resources is plans.Resources
    assert list(fields(plans.Resources)) == [
        "cpus", "memory_gb", "gpus", "walltime_min", "scratch_gb"]
    assert list(fields(model.Work)) == [
        "unit", "config", "inputs", "outputs", "root", "stage", "workdir",
        "resources", "members", "failed"]
    assert fields(model.Work)["stage"] == "none"
    assert list(fields(model.Stage)) == [
        "name", "level", "inputs", "outputs", "resources", "container"]
    assert list(fields(model.Pipeline)) == [
        "name", "version", "stages", "config", "requires", "container",
        "description", "code_url"]
    assert list(fields(grind.GrindOverrides)) == [
        "exclude_subs", "exclude", "inputs", "species", "template",
        "config", "resources"]
    assert grind.GrindOverrides() == grind.GrindOverrides()
    assert list(fields(graph.Scope)) == [
        "datasets", "home", "variant", "variants"]
    assert list(fields(graph.DatasetScope)) == [
        "ref", "layout", "selection", "overrides"]


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
    # a task finds its stage again by name: callables are importable
    assert "module:qualname" in inspect.getdoc(model.FunctionStage)
    assert "module:qualname" in inspect.getdoc(model.CommandStage)
    assert "module:qualname" in inspect.getdoc(model.Pipeline.validate)


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
    fn = pipeline.stages[0].fn
    assert f"{fn.__module__}:{fn.__qualname__}".endswith(":body")
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
    assert app.claims == ()         # default claims depend on the level
    doc = inspect.getdoc(grind.BidsAppPipeline)
    assert "sub-{sub}/ses-{ses}/" in doc and "refuses" in doc
    assert list(fields(grind.BidsAppConfig)) == [
        "args", "bids_filter", "skip_bids_validation", "files"]
    assert params(grind.BidsAppStage.command) == ["self", "ctx", "work"]
    assert containers.RUNTIMES == ("docker", "apptainer", "none")


# ----------------------------------------------------------------------
#   Plans
# ----------------------------------------------------------------------

def test_plan_dataclasses():
    assert list(fields(plans.Plan)) == [
        "stage", "datasets", "home", "tasks", "pipeline", "variants",
        "if_stale", "images", "options", "command", "created",
        "brainspresso", "schema", "path"]
    assert fields(plans.Plan)["schema"] == plans.PLAN_SCHEMA == 1
    assert list(fields(plans.PlanTask)) == [
        "id", "dataset", "stage", "scope", "after", "after_any",
        "resources", "overrides", "signature", "verdict", "reason"]
    assert list(fields(plans.TaskScope)) == ["level", "sub", "ses", "extra"]
    assert list(fields(plans.PlanDataset)) == [
        "ref", "root", "uid", "layout", "config"]
    assert issubclass(plans.PlanError, ValueError)
    assert plans.PLANS_DIR == "plans"
    # a plan is plain data: no callable, no path but the roots
    task = plans.PlanTask(
        "grind/fs-long/cross/sub-01/ses-1", "IXI", "cross",
        plans.TaskScope("session", "sub-01", "ses-1"))
    plan = plans.Plan("grind", (plans.PlanDataset("IXI", "/data/IXI"),),
                      "/data/IXI", (task,))
    assert not any(callable(v) for v in dataclasses.asdict(plan).values())
    assert typing.get_args(typing.get_type_hints(plans.Plan)["stage"]) == (
        "roast", "grind")
    assert typing.get_args(graph.Verdict) == graph.VERDICTS
    assert fields(plans.PlanTask)["verdict"] in graph.VERDICTS
    assert typing.get_args(graph.IfStale) == ("error", "redo", "keep")
    assert fields(plans.Plan)["if_stale"] == "error"


def test_yaml_pitfalls():
    """Why plans prefix labels and check strings (PyYAML, YAML 1.1)."""
    for pitfall in ("01", "no", "on", "1e3"):
        assert pitfall in plans.YAML_PITFALLS
    doc = flat(plans)
    assert "yaml.safe_load" in doc and "yaml.safe_dump" in doc
    assert "never converts" in doc
    yaml = pytest.importorskip("yaml")
    # what safe_dump writes is read back unchanged by safe_load...
    for pitfall in plans.YAML_PITFALLS:
        text = yaml.safe_dump({"label": pitfall})
        assert yaml.safe_load(text) == {"label": pitfall}, pitfall
    # ...but a plan edited by hand is coerced: `read_plan` must check
    loaded = yaml.safe_load("sub: 01\nses: no\nrun: on\nacq: 1.0\n")
    assert loaded == {"sub": 1, "ses": False, "run": True, "acq": 1.0}
    assert not any(isinstance(v, str) for v in loaded.values())
    # `1e3` is written unquoted (YAML 1.2 readers take it for a
    # number): `write_plan` must refuse it as an identifier
    assert yaml.safe_dump({"variant": "1e3"}) == "variant: 1e3\n"
    # labels with their BIDS prefix are strings for every reader
    prefixed = "sub: sub-01\nses: ses-no\nrun: run-on\nacq: acq-1e3\n"
    assert all(isinstance(v, str)
               for v in yaml.safe_load(prefixed).values())
    scope = yaml.safe_dump(dataclasses.asdict(
        plans.TaskScope("session", "sub-01", "ses-1e3")))
    assert yaml.safe_load(scope)["sub"] == "sub-01"


def test_execution_options():
    assert list(fields(options.ExecutionOptions)) == [
        "profile", "backend", "jobs", "partition", "account", "time",
        "mem", "cpus", "gpus", "max_array", "max_running", "directives",
        "container_runtime", "path_map", "stage", "scratch",
        "keep_scratch_on_failure"]
    assert options.ExecutionOptions() == options.ExecutionOptions()
    assert list(fields(options.Profile)) == [
        "name", "backend", "container_runtime", "jobs", "partition",
        "account", "resources", "stages", "max_array", "max_running",
        "directives", "setup", "executable", "wait", "paths", "stage",
        "keep_scratch_on_failure", "scratch", "image_cache"]
    profile = options.Profile()
    assert (profile.backend, profile.container_runtime, profile.stage,
            profile.executable) == ("local", "docker", "none", "bx")
    assert options.BACKENDS == ("local", "slurm", "sge", "pbs")
    assert jobs.SCHEDULERS == options.BACKENDS[1:]
    assert options.STAGING == ("none", "inputs", "all")
    assert options.SCRATCH_VARIABLES == ("SLURM_TMPDIR", "TMPDIR", "TMP")
    assert options.PROFILES_SECTION == "profiles"
    assert profile.container_runtime in containers.RUNTIMES
    doc = inspect.getdoc(options.resolve_resources)
    order = [doc.index(word) for word in (
        "the stage declares", "the profile", "the dataset overrides",
        "the command line")]
    assert order == sorted(order)


def test_jobs():
    assert list(fields(jobs.Job)) == [
        "name", "tasks", "resources", "after", "after_any", "pairwise"]
    assert typing.get_args(jobs.JobState) == jobs.JOB_STATES
    hints = typing.get_type_hints(jobs.Scheduler)
    assert (hints["name"], hints["array_index"], hints["pairwise"]) == (
        str, str, bool)
    doc = flat(jobs)
    assert "bx percolate PLAN --task <id>" in doc
    assert "bx percolate PLAN --array <job> --index <n>" in doc
    assert "no submitit" in doc
    assert "equal lengths are not enough" in flat(jobs.make_jobs)
    assert list(fields(prun.PlanStatus)) == [
        "plan", "status", "counts", "failures", "jobs", "unknown"]


# ----------------------------------------------------------------------
#   Signatures
# ----------------------------------------------------------------------

SIGNATURES = {
    model.format_output: ["template", "unit", "fields"],
    model.register_pipeline: ["pipeline", "name"],
    model.get_pipeline: ["name"],
    model.Pipeline.terminal: ["self"],
    graph.task_id: ["pipeline", "stage", "unit", "variant"],
    graph.derivative_name: ["pipeline", "variant"],
    graph.enumerate_units: ["layout", "level", "selection", "overrides",
                            "dataset"],
    graph.signature: ["pipeline", "stage", "config", "inputs", "root",
                      "upstream", "container"],
    graph.plan: ["pipeline", "scope", "configs", "redo", "if_stale",
                 "runtime", "command", "options"],
    containers.get_runtime: ["name"],
    containers.ContainerRuntime.command: [
        "self", "container", "argv", "mounts", "env", "workdir"],
    containers.ContainerRuntime.pull: ["self", "container"],
    containers.ContainerRuntime.inspect: ["self", "container"],
    context.GrindContext.__init__: [
        "self", "layout", "pipeline", "config", "overrides", "profile",
        "dataset", "variant", "variants", "locks"],
    context.GrindContext.required: ["self", "pipeline"],
    context.GrindContext.scratch: ["self", "task"],
    staging.stage_in: ["ctx", "stage", "work"],
    staging.stage_out: ["ctx", "stage", "work"],
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
        "execution", "redo", "if_stale", "variant", "variants", "plan_path",
        "detach", "dry_run", "display"],
    run.grind_collection: [
        "pipeline", "datasets", "where", "root", "out", "selection",
        "config", "execution", "redo", "if_stale", "variant", "variants",
        "plan_path", "detach", "dry_run", "display"],
    run.grind_status: ["pipeline", "layout", "variant"],
    run.GrindPlanner.run: ["self", "plan", "task", "profile"],
    run.GrindPlanner.locks: ["self", "plan", "task"],
    prun.StagePlanner.run: ["self", "plan", "task", "profile"],
    prun.StagePlanner.locks: ["self", "plan", "task"],
    prun.get_planner: ["stage"],
    prun.percolate: ["plan", "options", "retry", "write_jobs", "detach",
                     "display"],
    prun.run_task: ["plan", "task", "options"],
    prun.plan_status: ["plan", "reconcile"],
    prun.cancel_plan: ["plan"],
    prun.list_plans: ["state"],
    plans.ambiguous_scalar: ["text"],
    plans.check_strings: ["document"],
    plans.plan_path: ["state", "plan"],
    plans.write_plan: ["plan", "path"],
    plans.read_plan: ["path"],
    plans.resolve_roots: ["plan", "paths"],
    plans.Plan.task: ["self", "id"],
    plans.Plan.todo: ["self"],
    options.load_profile: ["options", "config"],
    options.resolve_resources: ["task", "profile", "options"],
    options.translate: ["path", "paths"],
    jobs.get_scheduler: ["name"],
    jobs.make_jobs: ["plan", "profile", "options", "tasks"],
    jobs.write_jobs: ["plan", "jobs", "profile", "folder"],
    jobs.submit_jobs: ["plan", "jobs", "profile", "folder"],
    jobs.Scheduler.script: ["self", "job", "command", "profile"],
    jobs.Scheduler.submit_command: ["self", "script", "after"],
    jobs.Scheduler.query: ["self", "ids"],
    jobs.Scheduler.cancel_command: ["self", "ids"],
}


@pytest.mark.parametrize(
    "fn", SIGNATURES, ids=lambda fn: f"{fn.__module__}.{fn.__qualname__}")
def test_signature(fn):
    assert params(fn) == SIGNATURES[fn]


def test_nothing_is_implemented(tmp_path):
    unit = model.Unit("subject", sub="01")
    layout = make_layout("study", tmp_path / "DS")
    pipeline = model.Pipeline("p", "1")
    plan = plans.Plan("grind", tasks=(plans.PlanTask("grind/p/s/sub-01"),))
    profile = options.Profile()
    calls = [
        lambda: unit.label,
        lambda: unit.entities,
        lambda: model.Query().selection(unit),
        lambda: model.format_output("sub-{sub}", unit),
        lambda: model.get_pipeline("p"),
        lambda: model.register_pipeline(pipeline),
        lambda: pipeline.terminal(),
        lambda: graph.task_id("p", "s", unit, "hires"),
        lambda: graph.derivative_name("p", "hires"),
        lambda: graph.enumerate_units(layout, "subject"),
        lambda: graph.signature(pipeline, None, None),
        lambda: graph.plan(pipeline, graph.Scope((), layout)),
        lambda: containers.get_runtime("docker"),
        lambda: context.GrindContext(layout, pipeline),
        lambda: context.GrindPolicy().fingerprint(None),
        lambda: staging.stage_in(None, None, model.Work(unit)),
        lambda: index.DatasetRef.parse("openneuro/ds000224"),
        lambda: index.parse_where("age > 60"),
        lambda: index.build_index(()),
        lambda: provenance.generated_by(pipeline),
        lambda: run.grind_dataset(pipeline, layout),
        lambda: run.grind_collection(pipeline, ["abide1/NYU"]),
        lambda: run.grind_status(pipeline, layout),
        lambda: run.planner.run(plan, plan.tasks[0], profile),
        lambda: run.planner.locks(plan, plan.tasks[0]),
        lambda: plan.id,
        lambda: plan.todo(),
        lambda: plans.Resources().merged({"cpus": 2}),
        lambda: plans.ambiguous_scalar("01"),
        lambda: plans.check_strings({"schema": 1}),
        lambda: plans.write_plan(plan, tmp_path / "plan.yaml"),
        lambda: plans.read_plan(tmp_path / "plan.yaml"),
        lambda: plans.resolve_roots(plan),
        lambda: options.load_profile(),
        lambda: options.resolve_resources(plan.tasks[0], profile),
        lambda: options.translate("/data/x", (("/data", "/mnt"),)),
        lambda: jobs.get_scheduler("slurm"),
        lambda: jobs.make_jobs(plan, profile),
        lambda: jobs.write_jobs(plan, (), profile, tmp_path / "jobs"),
        lambda: jobs.submit_jobs(plan, (), profile),
        lambda: prun.get_planner("grind"),
        lambda: prun.percolate(plan),
        lambda: prun.run_task(plan, "grind/p/s/sub-01"),
        lambda: prun.plan_status(plan),
        lambda: prun.cancel_plan(plan),
        lambda: prun.list_plans(tmp_path),
    ]
    for call in calls:
        with pytest.raises(NotImplementedError):
            call()
    assert list(tmp_path.iterdir()) == []
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
    # task records store `ctx.pipeline` as it is: it must be a name
    doc = inspect.getdoc(context.GrindContext)
    assert "pipeline : str" in doc and "spec : Pipeline" in doc


def test_task_state_fields():
    """Grind only adds fields to the task records of `actions.state`."""
    base = {"schema", "task", "stage", "pipeline", "unit", "status",
            "config_hash", "started", "finished", "error", "outputs"}
    assert not base & set(graph.GRIND_FIELDS)
    assert graph.GRIND_FIELDS == (
        "pipeline_stage", "variant", "dataset", "requires", "version",
        "container", "plan", "job", "staged", "scratch")
    assert run.GRIND_LOCK == "grind"
    assert list(fields(run.GrindReport)) == [
        "plan", "status", "counts", "failures", "progress"]
