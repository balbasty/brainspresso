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
SECTION = (ROOT / "docs" / "design" / "architecture.md").read_text(
    encoding="utf-8")
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
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
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
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
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
        "YAML_PITFALLS", "plan_dumper", "check_strings", "read_plan",
        "write_plan", "watch_plan", "resolve_roots", "ExecutionOptions",
        "Profile",
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
                 "Task.status", "submit_array", "ambiguous_scalar",
                 "--follow` is used"):
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
        "--wait", "--status --watch",
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
    assert app.shared == ()
    doc = flat(grind.bidsapp)
    assert "its own `out_dir`" in doc and "never the derivative" in doc
    assert "`finalize` task reads these copies" in doc
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


def quoting_dumper(yaml):
    """What `plan_dumper` is specified to return (reference)."""
    class Dumper(yaml.SafeDumper):
        pass

    Dumper.add_representer(str, lambda dumper, value: dumper.
                           represent_scalar("tag:yaml.org,2002:str", value,
                                            style='"'))
    return Dumper


def test_yaml_strings_round_trip():
    """Plans quote every string: `1e3`, `01`, `no`, `on` stay strings."""
    for pitfall in ("1e3", "01", "no", "on"):
        assert pitfall in plans.YAML_PITFALLS
    doc = flat(plans)
    assert "yaml.safe_load" in doc and "yaml.SafeDumper" in doc
    assert "never converts" in doc and "nothing is refused" in doc
    assert "quotes every string scalar" in doc
    assert not hasattr(plans, "ambiguous_scalar")
    assert "SafeDumper" in flat(plans.plan_dumper)
    yaml = pytest.importorskip("yaml")
    dumper = quoting_dumper(yaml)
    assert issubclass(dumper, yaml.SafeDumper)
    for pitfall in plans.YAML_PITFALLS:
        document = {"variant": pitfall, "labels": [pitfall], pitfall: "k"}
        text = yaml.dump(document, Dumper=dumper)
        assert f'"{pitfall}"' in text, pitfall
        loaded = yaml.safe_load(text)
        assert loaded == document, pitfall
        assert isinstance(loaded["variant"], str)
        # no plain scalar is left for a YAML 1.2 reader to reinterpret
        assert not re.search(rf"(?m)[:-] {re.escape(pitfall)}$", text)
    # numbers, booleans and nulls stay what they are
    text = yaml.dump({"schema": 1, "gpu": False, "digest": None},
                     Dumper=dumper)
    assert yaml.safe_load(text) == {"schema": 1, "gpu": False,
                                    "digest": None}
    # plain `safe_dump` would write this one for a float (YAML 1.2)
    assert yaml.safe_dump({"variant": "1e3"}) == "variant: 1e3\n"
    # a plan edited by hand is coerced by the reader: `read_plan` checks
    loaded = yaml.safe_load("sub: 01\nses: no\nrun: on\nacq: 1.0\n")
    assert not any(isinstance(v, str) for v in loaded.values())
    # labels with their BIDS prefix are strings even unquoted
    prefixed = "sub: sub-01\nses: ses-no\nrun: run-on\nacq: acq-1e3\n"
    assert all(isinstance(v, str)
               for v in yaml.safe_load(prefixed).values())


def test_execution_options():
    assert list(fields(options.ExecutionOptions)) == [
        "profile", "backend", "jobs", "partition", "account", "time",
        "mem", "cpus", "gpus", "max_array", "max_running", "directives",
        "container_runtime", "path_map", "stage", "scratch",
        "keep_scratch_on_failure", "detach"]
    assert options.ExecutionOptions().detach is None
    assert options.ExecutionOptions() == options.ExecutionOptions()
    assert list(fields(options.Profile)) == [
        "name", "backend", "container_runtime", "jobs", "partition",
        "account", "resources", "stages", "max_array", "max_running",
        "directives", "setup", "executable", "detach", "lock_wait", "paths",
        "stage",
        "keep_scratch_on_failure", "scratch", "image_cache"]
    profile = options.Profile()
    assert (profile.backend, profile.container_runtime, profile.stage,
            profile.executable) == ("local", "docker", "none", "bx")
    assert profile.detach is None       # wait locally, detach on schedulers
    doc = flat(options.Profile)
    assert "wait on the local backend, detach on schedulers" in doc
    assert "The command line wins" in doc
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
    assert (hints["name"], hints["array_index"], hints["pairwise"],
            hints["must_succeed"]) == (str, str, bool, bool)
    doc = flat(jobs)
    # dependencies wait for the end; must-succeed only where it holds
    for word in ("not for their success", "afterany", "afterok",
                 "--kill-on-invalid-dep=yes", "aftercorr", "-hold_jid",
                 "-hold_jid_ad", "depend=afterany"):
        assert word in doc, word
    assert "the default for a dependency between jobs" in flat(jobs.Job)
    assert "bx percolate PLAN --task <key>" in doc
    assert "bx percolate PLAN --array <job> --index <n>" in doc
    assert "no submitit" in doc
    assert "equal lengths are not enough" in flat(jobs.make_jobs)
    assert list(fields(prun.PlanStatus)) == [
        "plan", "status", "counts", "failures", "jobs", "unknown"]
    assert '"incomplete"' in flat(prun.PlanStatus)


def test_plan_keys_and_waiting():
    """Task ids repeat across datasets; "waiting" is not "pending"."""
    one = plans.PlanTask("grind/p/s/sub-01", "ixi")
    two = plans.PlanTask("grind/p/s/sub-01", "abide1/NYU")
    assert one.id == two.id and one != two
    assert isinstance(plans.PlanTask.key, property)
    assert "<dataset>:<id>" in flat(plans.PlanTask.key)
    assert params(plans.Plan.task) == ["self", "key"]
    for doc in (flat(plans.PlanTask), flat(jobs.Job), flat(prun.run_task),
                flat(prun.PlanStatus)):
        assert "key" in doc.lower()
    assert prun.TASK_STATUSES == (
        "pending", "running", "waiting", "done", "failed", "skipped")
    assert '"status": "' + " | ".join(prun.TASK_STATUSES) + '"' in SECTION
    doc = flat(prun)
    assert "never turns it into \"failed\"" in doc
    assert "submits it again" in doc
    assert "more CPUs than `--jobs`" in doc
    assert '"waiting"' in flat(prun.StagePlanner.run)
    assert '"waiting"' in flat(run.GrindPlanner.run)
    assert "nothing is clamped" in flat(prun.percolate)
    assert "`<dataset ref>:<id>`" in SECTION


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
    graph.signature: ["pipeline", "stage", "config", "root", "inputs",
                      "upstream", "container"],
    graph.canonical_key: ["task_id", "dataset", "seen_from"],
    plans.write_images: ["plan", "digests"],
    plans.read_images: ["plan"],
    prun.StagePlanner.prepare: ["self", "plan", "profile"],
    run.GrindPlanner.prepare: ["self", "plan", "profile"],
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
        "dry_run", "display"],
    run.grind_collection: [
        "pipeline", "datasets", "where", "root", "out", "selection",
        "config", "execution", "redo", "if_stale", "variant", "variants",
        "plan_path", "dry_run", "display"],
    run.grind_status: ["pipeline", "layout", "variant"],
    run.GrindPlanner.run: ["self", "plan", "task", "profile"],
    run.GrindPlanner.locks: ["self", "plan", "task"],
    prun.StagePlanner.run: ["self", "plan", "task", "profile"],
    prun.StagePlanner.locks: ["self", "plan", "task"],
    prun.get_planner: ["stage"],
    prun.percolate: ["plan", "options", "retry", "write_jobs", "display"],
    prun.watch_plan: ["plan", "interval", "display"],
    prun.run_task: ["plan", "task", "options"],
    prun.plan_status: ["plan", "reconcile"],
    prun.cancel_plan: ["plan"],
    prun.list_plans: ["state"],
    plans.plan_dumper: [],
    plans.check_strings: ["document"],
    plans.plan_path: ["state", "plan"],
    plans.write_plan: ["plan", "path"],
    plans.read_plan: ["path"],
    plans.resolve_roots: ["plan", "paths"],
    plans.Plan.task: ["self", "key"],
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
        lambda: graph.signature(pipeline, None, None, root=tmp_path),
        lambda: graph.canonical_key("grind/p/s/sub-01", "ixi", "ixi"),
        lambda: plans.write_images(plan, {"p": "sha256:0"}),
        lambda: plans.read_images(plan),
        lambda: run.planner.prepare(plan, profile),
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
        lambda: plan.tasks[0].key,
        lambda: plan.todo(),
        lambda: plans.Resources().merged({"cpus": 2}),
        lambda: plans.plan_dumper(),
        lambda: prun.watch_plan(plan),
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


def test_final_review():
    """What the final review added to §7 (step 7 changes included)."""
    section = " ".join(SECTION.split())
    for word in (
        "`CollectionLayout`", "never guessed from the tree",
        "hashed on relative paths", "registry manifest digest",
        "the plan is never rewritten", "`run_lock(..., alive=)`",
        "**Breaking change**: roast gets no alias",
        "`bx <ds> roast --wait 600` is an error", "names `--lock-wait`",
        "**only** on harvest and compost",
        "`<state>/work/`", "`BidsAppPipeline.shared`",
        "refuses the plan before anything runs",
    ):
        assert word in section, word
    doc = flat(containers.Container)
    assert "Registry manifest digest" in doc and "never as its path" in doc
    assert "`image` is unchanged" in flat(containers.ContainerRuntime.pull)
    assert "plan again" not in section


def test_final_review_round_2():
    """What the second round of the final review settled."""
    section = " ".join(SECTION.split())
    doc = flat(jobs)
    # 1. pairwise-any never becomes a must-succeed form
    for kind in ("any", "ok", "pairwise_any", "pairwise_ok"):
        assert f'"{kind}"' in doc and f"`{kind}`" in section, kind
    assert "never to an element-wise must-succeed form" in flat(
        jobs.Scheduler.submit_command)
    assert "SLURM has no element-wise form that ignores failure" in section
    # 2. a waiting record is a todo verdict
    assert "`failed`/`skipped`/`waiting`" in section
    assert 'recorded "waiting" is "todo"' in flat(graph.plan)
    # 3. canonical keys in signatures and `requires`
    assert "`canonical_key`" in section
    assert "canonical key and signature" in flat(graph.signature)
    # 4. one image per plan
    assert "One image per plan" in section and "`read_images`" in section
    assert "one image per plan" in flat(prun.StagePlanner.prepare)
    # 5. the upstream records decide, not the scheduler
    assert "the upstream records decide" in section
    assert "the upstream records decide" in flat(prun.plan_status)
    # 6, 7. descriptions kept for finalize; links and empty folders
    doc = flat(grind.bidsapp)
    assert "descriptions/<unit label>.json" in doc and "`CommitTree`" in doc
    assert "empty folders are kept" in doc
    assert "`CommitTree`" in section and "`CommitTree`" in flat(staging)
    # 8. run records: never stale on a final task record alone
    assert "A final task record alone proves nothing" in section
    assert "one record per process" in section
    # 9. signatures on relative paths, version bumped
    assert "`_SIGNATURE_VERSION` is bumped" in section
    assert "keeps its absolute path" in section
    signature = inspect.signature(graph.signature).parameters["root"]
    assert signature.default is inspect.Parameter.empty
    # image files enter signatures by content
    assert "file:<SHA-256 of the file>" in section
    assert "file:<SHA-256 of the file>" in flat(containers.Container)
    # id -> key
    assert "--task KEY" in flat(prun.run_task)
    assert "Keys of the tasks to run" in flat(jobs.make_jobs)


def test_decisions_leave_no_question():
    """The owner's decisions are in the text; nothing is left open."""
    start = SECTION.index("### Decisions (owner) and open questions")
    decisions = " ".join(SECTION[start:].split())
    assert decisions.endswith("Open: none.")
    assert not re.search(r"(?m)^\d+\. ", SECTION[start:])
    for word in ("--if-stale", "PyYAML is a core", "UUID", "never adopted",
                 "--wait`/`--detach", "--status --watch",
                 "--lock-wait SECONDS", "deprecated alias", "(#24)"):
        assert word in decisions, word
    assert "sbatch --wait" in SECTION and "--follow" in SECTION
    assert fields(plans.Plan)["if_stale"] == "error"
    assert "--lock-wait SECONDS" in flat(options.Profile)
    assert "alias on harvest and compost" in flat(options.Profile)
    assert "`roast --wait 600` is an error" in flat(options.ExecutionOptions)
    assert "still read as `--lock-wait" not in " ".join(SECTION.split())
    assert "str | Path" not in str(
        inspect.signature(run.grind_dataset).parameters["config"])
